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

//! An append-only file of MV management declarations.
//!
//! Each record is one line, written and synced before its declaration is
//! allowed to take effect. Appending is the whole durability story: nothing
//! rewrites earlier lines, nothing reads them back, and a reader only ever
//! needs to see what a human did and in what order.
//!
//! Rotation is deliberately not automatic. A rotating writer decides on its
//! own when older records stop existing, and these are the records that
//! explain irreversible operator actions; an operator or their log shipper
//! moves the file aside, and the next declaration recreates it.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use novarocks_mv_application::management::{ManagementAuditRecord, ManagementAuditSink};

/// The append-only sink the frontend installs when the deployment configures
/// one.
pub struct FileManagementAuditSink {
    path: PathBuf,
    /// One writer at a time, so two concurrent declarations cannot interleave
    /// halves of their lines into one record.
    writer: Mutex<()>,
}

impl FileManagementAuditSink {
    /// Open the sink, proving now rather than at the first declaration that
    /// the deployment can actually write it.
    ///
    /// A management command refuses when its record cannot be written, so a
    /// path that only fails later would turn a configuration mistake into an
    /// outage in the middle of a recovery.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "create the MV management audit directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        let sink = Self {
            path,
            writer: Mutex::new(()),
        };
        sink.append("")?;
        Ok(sink)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append(&self, line: &str) -> Result<(), String> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| "MV management audit writer is poisoned".to_string())?;
        let mut file: File = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| {
                format!(
                    "open the MV management audit file {}: {error}",
                    self.path.display()
                )
            })?;
        if !line.is_empty() {
            file.write_all(line.as_bytes()).map_err(|error| {
                format!(
                    "append to the MV management audit file {}: {error}",
                    self.path.display()
                )
            })?;
            file.flush().map_err(|error| {
                format!(
                    "flush the MV management audit file {}: {error}",
                    self.path.display()
                )
            })?;
        }
        // The caller treats success as permission to act, so the record has to
        // be on the device and not merely in the page cache.
        file.sync_data().map_err(|error| {
            format!(
                "sync the MV management audit file {}: {error}",
                self.path.display()
            )
        })
    }
}

impl ManagementAuditSink for FileManagementAuditSink {
    fn record(&self, record: &ManagementAuditRecord) -> Result<(), String> {
        record.validate()?;
        self.append(&render(record))
    }
}

/// One record as one line.
///
/// Every operator-supplied value is escaped, because an audit line that a
/// statement can break out of is an audit line a statement can forge.
fn render(record: &ManagementAuditRecord) -> String {
    let mut fields = vec![
        ("action".to_string(), record.action.as_str().to_string()),
        (
            "session_principal".to_string(),
            record.session_principal.clone(),
        ),
        (
            "operator_reference".to_string(),
            record.operator_reference.clone(),
        ),
        (
            "catalog".to_string(),
            record.table.instance_id.as_str().to_string(),
        ),
        ("database".to_string(), record.table.namespace.to_string()),
        ("name".to_string(), record.table.table.to_string()),
        (
            "local_owner".to_string(),
            record.local_owner.as_str().to_string(),
        ),
        (
            "local_incarnation".to_string(),
            record.local_incarnation.as_str().to_string(),
        ),
        (
            "challenge".to_string(),
            uuid::Uuid::from_bytes(record.challenge.to_bytes()).to_string(),
        ),
        (
            "evidence_reference".to_string(),
            record.evidence_reference.clone(),
        ),
        ("outcome".to_string(), record.outcome.to_string()),
    ];
    if let Some(incarnation) = &record.declared_old_incarnation {
        fields.push((
            "declared_old_incarnation".to_string(),
            incarnation.as_str().to_string(),
        ));
    }
    if let Some(owner) = &record.declared_new_owner {
        fields.push(("declared_new_owner".to_string(), owner.as_str().to_string()));
    }
    let rendered = fields
        .into_iter()
        .map(|(key, value)| format!("{key}={}", escape(&value)))
        .collect::<Vec<_>>()
        .join(" ");
    format!("{rendered}\n")
}

fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            other => escaped.push(other),
        }
    }
    escaped.push('"');
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::management::{
        DeploymentOwner, ManagementAuditAction, ManagementAuditOutcome, ProcessIncarnation,
        ReadmissionChallenge,
    };
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorTableIdentity};
    use std::sync::Arc;

    fn record(evidence: &str) -> ManagementAuditRecord {
        ManagementAuditRecord {
            action: ManagementAuditAction::ResumeManagement,
            session_principal: "root".to_string(),
            operator_reference: "operator@example".to_string(),
            table: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse("ice").expect("catalog"),
                namespace: Arc::from("db"),
                table: Arc::from("mv"),
            },
            local_owner: DeploymentOwner::parse("deployment-a").expect("owner"),
            local_incarnation: ProcessIncarnation::parse("inc-a").expect("incarnation"),
            declared_old_incarnation: Some(
                ProcessIncarnation::parse("inc-old").expect("incarnation"),
            ),
            declared_new_owner: None,
            challenge: ReadmissionChallenge::from_bytes([3; 16]),
            evidence_reference: evidence.to_string(),
            outcome: ManagementAuditOutcome::Applied,
        }
    }

    fn sink_path() -> PathBuf {
        std::env::temp_dir().join(format!("nr-mv-audit-{}.log", uuid::Uuid::now_v7()))
    }

    #[test]
    fn a_declaration_is_one_line_naming_both_the_principal_and_the_claim() {
        let path = sink_path();
        let sink = FileManagementAuditSink::open(&path).expect("open sink");

        sink.record(&record("controller confirmed the old FE exited"))
            .expect("record is written");

        let written = std::fs::read_to_string(&path).expect("read sink");
        assert_eq!(written.lines().count(), 1, "{written}");
        assert!(
            written.contains("action=\"resume_management\""),
            "{written}"
        );
        assert!(written.contains("session_principal=\"root\""), "{written}");
        assert!(
            written.contains("operator_reference=\"operator@example\""),
            "{written}"
        );
        assert!(
            written.contains("declared_old_incarnation=\"inc-old\""),
            "{written}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_operator_statement_cannot_forge_a_second_record() {
        let path = sink_path();
        let sink = FileManagementAuditSink::open(&path).expect("open sink");

        sink.record(&record(
            "real\nevidence_reference=\"forged\" outcome=\"applied\"",
        ))
        .expect("record is written");

        let written = std::fs::read_to_string(&path).expect("read sink");
        assert_eq!(
            written.lines().count(),
            1,
            "an embedded newline must not become a second line: {written}"
        );
        assert!(written.contains("\\n"), "{written}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_oversized_statement_is_refused_before_anything_is_written() {
        let path = sink_path();
        let sink = FileManagementAuditSink::open(&path).expect("open sink");

        let error = sink
            .record(&record(&"e".repeat(
                novarocks_mv_application::management::MAX_MANAGEMENT_AUDIT_FIELD_BYTES + 1,
            )))
            .expect_err("an oversized statement must not be recorded");

        assert!(error.contains("exceeds the"), "{error}");
        assert_eq!(
            std::fs::read_to_string(&path).expect("read sink"),
            "",
            "nothing may be written for a refused record"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_unwritable_path_fails_at_open_rather_than_mid_recovery() {
        let path = sink_path();
        std::fs::write(&path, "").expect("create a file to stand where a directory is needed");

        let error = match FileManagementAuditSink::open(path.join("audit.log")) {
            Ok(_) => panic!("a path under a regular file cannot be opened"),
            Err(error) => error,
        };

        assert!(error.contains("MV management audit"), "{error}");
        let _ = std::fs::remove_file(&path);
    }
}
