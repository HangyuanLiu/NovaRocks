// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original FE admission worker scalar evidence; no provider call or reset.
use super::*;
use anyhow::anyhow;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Completion {
    schema_version: u32,
    process_id: u32,
    invalid: bool,
    sequence: u64,
    row: Option<CompletionRow>,
}
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CompletionRow {
    catalog_name: String,
    attachment_id: String,
    projection_generation: u64,
    catalog_version: String,
    incarnation: String,
    queued: u64,
    started: u64,
    bound: u64,
    quarantined: u64,
    returned: u64,
    outcome: CompletionOutcome,
}
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
enum CompletionOutcome {
    Pending,
    QuarantinedUnsupported,
    ReturnedComplete,
    Failed,
}
impl Completion {
    fn identity(&self, journal: &Journal, frontend: u32) -> Result<()> {
        ensure!(
            self.schema_version == 1 && self.process_id == frontend && !self.invalid,
            "HMS original admission observation source failed or differs"
        );
        if let Some(row) = &self.row {
            ensure!(
                row.catalog_name == journal.catalog_name
                    && row.catalog_name == CATALOG
                    && uuid(&row.attachment_id)
                    && row.projection_generation > 0
                    && row.catalog_version == journal.catalog_version
                    && hash(&row.catalog_version, 64)
                    && row.incarnation == journal.incarnation
                    && uuid(&row.incarnation)
                    && row.queued > 0
                    && row.queued <= self.sequence,
                "HMS original admission generation differs from the captured SDK owner"
            );
        }
        Ok(())
    }
    fn complete(&self, journal: &Journal, frontend: u32) -> Result<()> {
        self.identity(journal, frontend)?;
        let row = self
            .row
            .as_ref()
            .ok_or_else(|| anyhow!("HMS original admission was not queued"))?;
        ensure!(
            row.queued < row.started
                && row.started < row.bound
                && row.bound < row.quarantined
                && row.quarantined < row.returned
                && row.returned <= self.sequence
                && row.outcome == CompletionOutcome::QuarantinedUnsupported,
            "HMS original admission worker has not returned after expected Unsupported quarantine"
        );
        Ok(())
    }
}

fn completion(port: u16, deadline: Instant) -> Result<Completion> {
    let raw = http_bytes(
        port,
        "/debug/hms-listing-observation",
        Some(json!({"operation":"admission_completion"})),
        deadline,
    )?;
    ensure!(
        raw.len() <= 2048,
        "HMS admission observation exceeds fixed scalar bound"
    );
    serde_json::from_slice(&raw).map_err(Into::into)
}

// At most one mutable evidence entry. Every replacement contains the entire
// original append-only SDK journal, not only the latest invocation or counts.
// No reset is callable here. The caller persists this evidence before reset.
pub(super) fn wait_original_admission(
    receipts: &mut Vec<Value>,
    port: u16,
    frontend: u32,
    deadline: Instant,
) -> Result<(Journal, Completion)> {
    let index = receipts.len();
    receipts.push(json!({"phase":"catalog-admission-sweep","samples":0}));
    let mut samples = 0_u64;
    let mut first_generation = None;
    loop {
        remaining(deadline)?;
        let completion = at_stage(InitialStage::CatalogAdmissionCompletion, || {
            completion(port, deadline)
        })?;
        let journal = at_stage(InitialStage::CatalogSnapshot, || snapshot(port, deadline))?;
        samples = samples
            .checked_add(1)
            .ok_or_else(|| anyhow!("HMS admission sample count overflow"))?;
        let generation = (
            journal.process_id,
            journal.catalog_name.clone(),
            journal.catalog_version.clone(),
            journal.incarnation.clone(),
            journal.domain.clone(),
            journal.phase,
        );
        if first_generation.is_none() {
            first_generation = Some(generation.clone());
        }
        receipts[index] = json!({"phase":"catalog-admission-sweep","samples":samples,
            "first_generation":first_generation,"completion":completion,"journal":journal});
        at_stage(InitialStage::CatalogAdmissionCompletion, || {
            ensure!(
                Some(&generation) == first_generation.as_ref(),
                "HMS admission journal changed generation or reset before persistence"
            );
            completion.identity(&journal, frontend)?;
            Ok(())
        })?;
        if completion.row.as_ref().is_some_and(|row| row.returned != 0) {
            record_and_validate_catalog_admission(receipts, &journal, &completion, frontend)?;
            // Recheck retirement/repeated notification after reading the full SDK journal.
            let after = at_stage(InitialStage::CatalogAdmissionCompletion, || {
                completion_snapshot(port, deadline)
            })?;
            receipts[index]["completion_after"] = serde_json::to_value(&after)?;
            at_stage(InitialStage::CatalogAdmissionCompletion, || {
                ensure!(
                    after == completion,
                    "HMS admission evidence changed before exact reset"
                );
                remaining(deadline)?;
                Ok(())
            })?;
            return Ok((journal, completion));
        }
        // This sleep consumes only the caller's existing absolute phase clock.
        std::thread::sleep(remaining(deadline)?.min(Duration::from_millis(10)));
    }
}
fn completion_snapshot(port: u16, deadline: Instant) -> Result<Completion> {
    completion(port, deadline)
}

pub(super) fn persist_before_reset(
    receipts: &[Value],
    journal: &Journal,
    completion: &Completion,
    frontend: u32,
    deadline: Instant,
    persist: impl FnOnce() -> Result<()>,
    reset: impl FnOnce() -> Result<()>,
) -> Result<()> {
    // Revalidate the original saved evidence before allowing any reset.
    validate_admission(journal, completion, frontend)?;
    let saved_journal = serde_json::to_value(journal)?;
    let saved_completion = serde_json::to_value(completion)?;
    ensure!(
        receipts.iter().any(|v| v["phase"] == "catalog-admission"
            && v["journal"] == saved_journal
            && v["completion"] == saved_completion),
        "HMS admission original journal was not deposited before persistence"
    );
    remaining(deadline)?;
    persist()?;
    remaining(deadline)?;
    reset()?;
    remaining(deadline)?;
    Ok(())
}

pub(super) fn record_and_validate_catalog_admission(
    receipts: &mut Vec<Value>,
    journal: &Journal,
    completion: &Completion,
    frontend: u32,
) -> Result<()> {
    receipts.push(json!({"phase":"catalog-admission","journal":journal,"completion":completion}));
    validate_admission(journal, completion, frontend)
}

fn validate_admission(journal: &Journal, completion: &Completion, frontend: u32) -> Result<()> {
    at_stage(InitialStage::CatalogJournalValidation, || {
        journal_idle(journal, frontend)
    })?;
    at_stage(InitialStage::CatalogAdmissionCompletion, || {
        completion.complete(journal, frontend)?;
        ensure!(
            journal.phase == 1 && journal.used == 1 && journal.records.len() == 1,
            "HMS original admission did not produce exactly its namespace discovery"
        );
        let record = &journal.records[0];
        ensure!(
            record.ordinal == 1
                && record.operation == Operation::Namespaces
                && record.target_sha256.is_none()
                && record.selection == Selection::ReadyOk
                && !record.stop_at_selection
                && !record.deadline_at_selection
                && !record.original_deadline_elapsed
                && record.sdk_ready > record.sdk_first_poll
                && record.sdk_dropped > record.sdk_ready,
            "HMS original admission namespace discovery did not complete successfully"
        );
        Ok(())
    })
}

#[cfg(test)]
pub(super) fn completed_for_test(journal: &Journal) -> Completion {
    Completion {
        schema_version: 1,
        process_id: journal.process_id,
        invalid: false,
        sequence: 5,
        row: Some(CompletionRow {
            catalog_name: journal.catalog_name.clone(),
            attachment_id: "01a12434-b125-7b41-b46d-0330420b9091".to_owned(),
            projection_generation: 3,
            catalog_version: journal.catalog_version.clone(),
            incarnation: journal.incarnation.clone(),
            queued: 1,
            started: 2,
            bound: 3,
            quarantined: 4,
            returned: 5,
            outcome: CompletionOutcome::QuarantinedUnsupported,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_completion_parser_refuses_duplicate_unknown_and_truncated_fields() {
        let duplicate = br#"{"schema_version":1,"schema_version":1,"process_id":1,"invalid":false,"sequence":0,"row":null}"#;
        let unknown = br#"{"schema_version":1,"process_id":1,"invalid":false,"sequence":0,"row":null,"other":0}"#;
        let truncated = br#"{"schema_version":1,"process_id":1}"#;
        for raw in [
            duplicate.as_slice(),
            unknown.as_slice(),
            truncated.as_slice(),
        ] {
            assert!(serde_json::from_slice::<Completion>(raw).is_err());
        }
    }
    #[test]
    fn journal_failure_missing_persistence_and_expiry_never_reset() {
        use std::cell::Cell;
        for mode in 0..6 {
            let mut journal = super::super::diagnostic_tests::initial_settled_journal();
            let completion = completed_for_test(&journal);
            let mut receipts = Vec::new();
            record_and_validate_catalog_admission(
                &mut receipts,
                &journal,
                &completion,
                journal.process_id,
            )
            .unwrap();
            let resets = Cell::new(0);
            let deadline = if mode == 4 {
                Instant::now()
            } else if mode == 3 {
                Instant::now() + Duration::from_millis(1)
            } else {
                Instant::now() + Duration::from_secs(1)
            };
            if mode == 0 {
                journal.sdk_objects_live = 1;
            }
            if mode == 1 {
                receipts.clear();
            }
            let original = std::sync::Arc::new(());
            struct Primary(std::sync::Arc<()>);
            impl std::fmt::Debug for Primary {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("OriginalPersistenceSource")
                }
            }
            impl std::fmt::Display for Primary {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("OriginalPersistenceSource")
                }
            }
            impl std::error::Error for Primary {}
            let result = persist_before_reset(
                &receipts,
                &journal,
                &completion,
                journal.process_id,
                deadline,
                || {
                    if mode == 2 {
                        Err(anyhow::Error::new(Primary(std::sync::Arc::clone(
                            &original,
                        ))))
                    } else {
                        if mode == 3 {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Ok(())
                    }
                },
                || {
                    resets.set(resets.get() + 1);
                    Ok(())
                },
            );
            if mode == 5 {
                assert!(result.is_ok());
                assert_eq!(resets.get(), 1);
            } else {
                assert!(result.is_err());
                assert_eq!(resets.get(), 0);
            }
            if mode == 2 {
                assert!(std::sync::Arc::ptr_eq(
                    &result.unwrap_err().downcast_ref::<Primary>().unwrap().0,
                    &original
                ));
            }
        }
    }

    #[test]
    fn source_generation_incomplete_quarantine_and_duplicate_invalid_refuse() {
        // Fixtures model scalar evidence only, not an actual FE worker/SDK run.
        let journal = super::super::diagnostic_tests::initial_settled_journal();
        for mode in 0..6 {
            let mut value = completed_for_test(&journal);
            match mode {
                0 => value.process_id += 1,
                1 => {
                    value.row.as_mut().unwrap().incarnation =
                        "01a12434-b125-7b41-b46d-0330420b9099".into()
                }
                2 => value.row.as_mut().unwrap().returned = 0,
                3 => value.row.as_mut().unwrap().outcome = CompletionOutcome::ReturnedComplete,
                4 => value.invalid = true,
                _ => value.row.as_mut().unwrap().projection_generation = 0,
            }
            assert!(value.complete(&journal, journal.process_id).is_err());
        }
    }
}
