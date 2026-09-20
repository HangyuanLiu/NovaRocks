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

use std::collections::{BTreeMap, BTreeSet};

use crate::mv::domain::model::{AffectedTargetPartitions, MvStorageEngine};
use crate::mv::domain::refresh::planning::{
    RefreshBaseRelationOccurrence, RefreshPlanContract, RefreshStateBaseline,
};
use crate::mv::domain::refresh::snapshot::ExecutableRefreshDecision;
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) struct RefreshExecutionObservation<'a> {
    pub(crate) backend: MvStorageEngine,
    pub(crate) mv_id: Option<i64>,
    pub(crate) target: &'a MvTarget,
    pub(crate) base_refs: &'a [RefreshBaseRelationOccurrence],
    pub(crate) state_baseline: &'a RefreshStateBaseline,
    pub(crate) snapshot_pins: Option<&'a BTreeMap<SqlMvRelationOccurrenceId, Option<i64>>>,
}

#[derive(Debug)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) struct ValidatedRefreshExecution<'a> {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    contract: &'a RefreshPlanContract,
}

impl<'a> ValidatedRefreshExecution<'a> {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn decision(&self) -> ExecutableRefreshDecision {
        self.contract.decision
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn target(&self) -> &'a MvTarget {
        &self.contract.target
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn base_refs(&self) -> &'a [RefreshBaseRelationOccurrence] {
        &self.contract.base_refs
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn state_baseline(&self) -> &'a RefreshStateBaseline {
        &self.contract.state_baseline
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn affected_partitions(&self) -> &'a AffectedTargetPartitions {
        &self.contract.affected_partitions
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn validate_refresh_execution<'a>(
    contract: &'a RefreshPlanContract,
    observation: &RefreshExecutionObservation<'_>,
) -> Result<ValidatedRefreshExecution<'a>, String> {
    if contract.storage_engine != observation.backend {
        return Err(format!(
            "refresh execution backend mismatch: planned {}, observed {}",
            contract.storage_engine.backend_name(),
            observation.backend.backend_name()
        ));
    }
    if contract.mv_id != observation.mv_id {
        return Err(format!(
            "refresh execution mv id mismatch: planned {:?}, observed {:?}",
            contract.mv_id, observation.mv_id
        ));
    }
    if contract.target != *observation.target {
        return Err(format!(
            "refresh execution target mismatch: planned {}, observed {}",
            contract.target.display_name(),
            observation.target.display_name()
        ));
    }

    let contract_bases = base_occurrences("planned", &contract.base_refs)?;
    let observed_bases = base_occurrences("observed", observation.base_refs)?;
    if contract_bases != observed_bases {
        return Err(set_mismatch(
            "refresh execution base refs",
            &contract_bases,
            &observed_bases,
        ));
    }

    if contract.state_baseline != *observation.state_baseline {
        return Err(format!(
            "refresh execution state baseline mismatch: planned {:?}, observed {:?}",
            contract.state_baseline, observation.state_baseline
        ));
    }

    match &contract.state_baseline {
        RefreshStateBaseline::Pinless => {
            if !contract.snapshot_pins.is_empty() {
                return Err(
                    "pinless refresh execution contract must not contain planned snapshot pins"
                        .to_string(),
                );
            }
            if observation.snapshot_pins.is_some() {
                return Err(
                    "pinless refresh execution observation must not contain snapshot pins"
                        .to_string(),
                );
            }
        }
        RefreshStateBaseline::SnapshotBacked { .. } => {
            let planned_keys = contract
                .snapshot_pins
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            if planned_keys != contract_bases {
                return Err(set_mismatch(
                    "planned snapshot pin keys",
                    &contract_bases,
                    &planned_keys,
                ));
            }
            let observed_pins = observation.snapshot_pins.ok_or_else(|| {
                "snapshot-backed refresh execution observation is missing snapshot pins".to_string()
            })?;
            let observed_keys = observed_pins.keys().cloned().collect::<BTreeSet<_>>();
            if observed_keys != contract_bases {
                return Err(set_mismatch(
                    "observed snapshot pin keys",
                    &contract_bases,
                    &observed_keys,
                ));
            }
            for occurrence in &contract_bases {
                let planned = contract
                    .snapshot_pins
                    .get(occurrence)
                    .expect("validated key set");
                let observed = observed_pins.get(occurrence).expect("validated key set");
                if planned != observed {
                    return Err(format!(
                        "refresh execution snapshot pin mismatch for occurrence {}: planned {planned:?}, observed {observed:?}",
                        occurrence.get()
                    ));
                }
            }
        }
    }

    Ok(ValidatedRefreshExecution { contract })
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn dispatch_refresh_decision<T, E>(
    decision: ExecutableRefreshDecision,
    skip_empty: impl FnOnce() -> Result<T, E>,
    first_refresh: impl FnOnce() -> Result<T, E>,
    metadata_only: impl FnOnce() -> Result<T, E>,
    incremental: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    match decision {
        ExecutableRefreshDecision::SkipEmpty => skip_empty(),
        ExecutableRefreshDecision::FirstRefresh => first_refresh(),
        ExecutableRefreshDecision::MetadataOnly => metadata_only(),
        ExecutableRefreshDecision::Incremental => incremental(),
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
/// The set of occurrences this refresh reads.
///
/// Occurrences, not table names: one definition may read one table twice, and
/// those two mentions are two sources. What may not repeat is an occurrence —
/// each is one position in the definition, so a repeat means two different
/// facts are claiming to be the same source.
fn base_occurrences(
    source: &str,
    base_refs: &[RefreshBaseRelationOccurrence],
) -> Result<BTreeSet<SqlMvRelationOccurrenceId>, String> {
    let mut occurrences = BTreeSet::new();
    for base in base_refs {
        if !occurrences.insert(base.occurrence_id) {
            return Err(format!(
                "refresh execution {source} base refs name occurrence {} twice",
                base.occurrence_id.get()
            ));
        }
    }
    Ok(occurrences)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn set_mismatch(
    label: &str,
    expected: &BTreeSet<SqlMvRelationOccurrenceId>,
    observed: &BTreeSet<SqlMvRelationOccurrenceId>,
) -> String {
    let named = |set: &BTreeSet<SqlMvRelationOccurrenceId>| {
        set.iter().map(|id| id.get()).collect::<Vec<_>>()
    };
    let missing = named(&expected.difference(observed).copied().collect());
    let extra = named(&observed.difference(expected).copied().collect());
    format!("{label} mismatch: missing occurrences {missing:?}, extra {extra:?}")
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use super::*;
    use crate::mv::domain::model::{AffectedTargetPartitions, MvStorageEngine};
    use crate::mv::domain::refresh::planning::{
        RefreshPlanContract, RefreshStateBaseline, RefreshStateBaselineSource,
    };
    use crate::mv::domain::refresh::snapshot::ExecutableRefreshDecision;
    use novarocks_spi::connector::{
        ConnectorExactSemanticRevision, ConnectorProviderId, ConnectorTableObjectId,
    };
    use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
    use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
    use novarocks_types::naming::TableIdentity;

    fn object_id(value: &str) -> ConnectorTableObjectId {
        ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(value.as_bytes()))
            .expect("test object ID")
    }

    /// Provider-native data versions stay opaque here. The baseline is built
    /// the way a provider mints it and is only ever compared, never decoded.
    fn revision(
        object: &ConnectorTableObjectId,
        snapshot_id: i64,
    ) -> ConnectorExactSemanticRevision {
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").expect("test provider ID"),
            object,
            Some(snapshot_id),
        )
        .expect("test semantic revision")
    }

    fn baseline_source(
        occurrence_id: u32,
        name: &str,
        object: &ConnectorTableObjectId,
        snapshot_id: i64,
    ) -> RefreshStateBaselineSource {
        RefreshStateBaselineSource {
            occurrence_id: SqlMvRelationOccurrenceId::new(occurrence_id),
            table: table(name),
            semantic_revision: revision(object, snapshot_id),
        }
    }

    fn table(name: &str) -> TableIdentity {
        TableIdentity::new("ice", "db", name)
    }

    fn target(name: &str) -> MvTarget {
        MvTarget {
            catalog: Some("ice".to_string()),
            database: "db".to_string(),
            name: name.to_string(),
        }
    }

    fn snapshot_baseline() -> RefreshStateBaseline {
        RefreshStateBaseline::SnapshotBacked {
            previous_sources: vec![
                baseline_source(0, "left", &object_id("left-v1"), 1),
                baseline_source(1, "right", &object_id("right-v1"), 2),
            ],
            target_snapshot_id: Some(10),
            target_table_uuid: "target-v1".to_string(),
            definition_fingerprint: "definition-v1".to_string(),
        }
    }

    fn occ(index: u32) -> SqlMvRelationOccurrenceId {
        SqlMvRelationOccurrenceId::new(index)
    }

    fn base(index: u32, name: &str) -> RefreshBaseRelationOccurrence {
        RefreshBaseRelationOccurrence {
            occurrence_id: occ(index),
            table: table(name),
        }
    }

    fn contract() -> RefreshPlanContract {
        RefreshPlanContract {
            mv_id: Some(42),
            target: target("mv"),
            storage_engine: MvStorageEngine::Iceberg,
            decision: ExecutableRefreshDecision::Incremental,
            state_baseline: snapshot_baseline(),
            base_refs: vec![base(0, "left"), base(1, "right")],
            snapshot_pins: BTreeMap::from([(occ(0), Some(3)), (occ(1), Some(4))]),
            affected_partitions: AffectedTargetPartitions::not_derived("test"),
        }
    }

    fn validate<'a>(
        contract: &'a RefreshPlanContract,
        backend: MvStorageEngine,
        mv_id: Option<i64>,
        target: &MvTarget,
        base_refs: &[RefreshBaseRelationOccurrence],
        state_baseline: &RefreshStateBaseline,
        snapshot_pins: Option<&BTreeMap<SqlMvRelationOccurrenceId, Option<i64>>>,
    ) -> Result<ValidatedRefreshExecution<'a>, String> {
        validate_refresh_execution(
            contract,
            &RefreshExecutionObservation {
                backend,
                mv_id,
                target,
                base_refs,
                state_baseline,
                snapshot_pins,
            },
        )
    }

    #[test]
    fn rejects_snapshot_pin_drift() {
        let contract = contract();
        let mut observed_pins = contract.snapshot_pins.clone();
        observed_pins.insert(occ(0), Some(99));

        let error = validate(
            &contract,
            MvStorageEngine::Iceberg,
            Some(42),
            &contract.target,
            &contract.base_refs,
            &contract.state_baseline,
            Some(&observed_pins),
        )
        .unwrap_err();

        assert!(error.contains("snapshot pin"), "{error}");
        assert!(error.contains("occurrence 0"), "{error}");
    }

    #[test]
    fn accepts_base_ref_order_changes() {
        let contract = contract();
        let reordered = vec![base(1, "right"), base(0, "left")];

        validate(
            &contract,
            MvStorageEngine::Iceberg,
            Some(42),
            &contract.target,
            &reordered,
            &contract.state_baseline,
            Some(&contract.snapshot_pins),
        )
        .expect("base reference order must not affect identity validation");
    }

    #[test]
    fn rejects_identity_and_baseline_drift_fail_closed() {
        let contract = contract();
        let wrong_target = target("other_mv");
        // One occurrence claimed twice: two facts saying they are the same
        // source. Two occurrences of one table are fine and are covered by
        // `one_table_read_twice_is_two_sources`.
        let duplicate_bases = vec![base(0, "left"), base(0, "left")];
        let replacement_bases = vec![base(0, "left"), base(2, "replacement")];
        let pinless = RefreshStateBaseline::Pinless;

        let cases = [
            (
                "backend",
                validate(
                    &contract,
                    MvStorageEngine::StarRocks,
                    Some(42),
                    &contract.target,
                    &contract.base_refs,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "mv id",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(43),
                    &contract.target,
                    &contract.base_refs,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "target",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &wrong_target,
                    &contract.base_refs,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "occurrence 0 twice",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &contract.target,
                    &duplicate_bases,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "base refs",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &contract.target,
                    &replacement_bases,
                    &contract.state_baseline,
                    Some(&contract.snapshot_pins),
                ),
            ),
            (
                "state baseline",
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &contract.target,
                    &contract.base_refs,
                    &pinless,
                    Some(&contract.snapshot_pins),
                ),
            ),
        ];

        for (expected, result) in cases {
            let error = result.unwrap_err();
            assert!(
                error.contains(expected),
                "expected {expected:?} in {error:?}"
            );
        }
    }

    #[test]
    fn rejects_pin_key_and_option_value_drift() {
        let contract = contract();
        let mut missing = contract.snapshot_pins.clone();
        missing.remove(&occ(1));
        let mut extra = contract.snapshot_pins.clone();
        extra.insert(occ(7), Some(5));
        let mut some_to_none = contract.snapshot_pins.clone();
        some_to_none.insert(occ(0), None);

        for pins in [&missing, &extra, &some_to_none] {
            assert!(
                validate(
                    &contract,
                    MvStorageEngine::Iceberg,
                    Some(42),
                    &contract.target,
                    &contract.base_refs,
                    &contract.state_baseline,
                    Some(pins),
                )
                .is_err()
            );
        }

        let mut none_planned = contract.clone();
        none_planned.snapshot_pins.insert(occ(0), None);
        assert!(
            validate(
                &none_planned,
                MvStorageEngine::Iceberg,
                Some(42),
                &none_planned.target,
                &none_planned.base_refs,
                &none_planned.state_baseline,
                Some(&contract.snapshot_pins),
            )
            .unwrap_err()
            .contains("snapshot pin")
        );
    }

    #[test]
    fn rejects_planned_pin_keys_that_do_not_match_contract_bases() {
        let mut missing = contract();
        missing.snapshot_pins.remove(&occ(1));
        let mut extra = contract();
        extra.snapshot_pins.insert(occ(7), Some(5));

        for contract in [&missing, &extra] {
            let error = validate(
                contract,
                MvStorageEngine::Iceberg,
                Some(42),
                &contract.target,
                &contract.base_refs,
                &contract.state_baseline,
                Some(&contract.snapshot_pins),
            )
            .unwrap_err();
            assert!(error.contains("planned snapshot pin keys"), "{error}");
        }
    }

    #[test]
    fn rejects_an_occurrence_claimed_twice_on_either_side() {
        let mut duplicate_contract = contract();
        duplicate_contract.base_refs = vec![base(0, "left"), base(0, "left")];
        let error = validate(
            &duplicate_contract,
            MvStorageEngine::Iceberg,
            Some(42),
            &duplicate_contract.target,
            &duplicate_contract.base_refs,
            &duplicate_contract.state_baseline,
            Some(&duplicate_contract.snapshot_pins),
        )
        .unwrap_err();
        assert!(
            error.contains("planned base refs name occurrence 0 twice"),
            "{error}"
        );

        let contract = contract();
        let duplicate_observed = vec![base(0, "left"), base(0, "left")];
        let error = validate(
            &contract,
            MvStorageEngine::Iceberg,
            Some(42),
            &contract.target,
            &duplicate_observed,
            &contract.state_baseline,
            Some(&contract.snapshot_pins),
        )
        .unwrap_err();
        assert!(
            error.contains("observed base refs name occurrence 0 twice"),
            "{error}"
        );
    }

    /// Two mentions of one table are two sources, and the contract says so
    /// without complaint. This is the shape that used to be refused outright:
    /// keyed by table name, the second mention overwrote the first, so there
    /// was no way to state that they were pinned at different points.
    #[test]
    fn one_table_read_twice_is_two_sources() {
        let mut self_join = contract();
        self_join.base_refs = vec![base(0, "moves"), base(1, "moves")];
        self_join.snapshot_pins = BTreeMap::from([(occ(0), Some(3)), (occ(1), Some(4))]);

        validate(
            &self_join,
            MvStorageEngine::Iceberg,
            Some(42),
            &self_join.target,
            &self_join.base_refs,
            &self_join.state_baseline,
            Some(&self_join.snapshot_pins),
        )
        .expect("one table read twice is two occurrences, not a duplicate");
    }

    #[test]
    fn rejects_each_snapshot_backed_baseline_field_drift() {
        let contract = contract();
        let RefreshStateBaseline::SnapshotBacked {
            previous_sources,
            target_snapshot_id,
            target_table_uuid,
            definition_fingerprint,
        } = snapshot_baseline()
        else {
            unreachable!()
        };

        // Each case below changes exactly one baseline component. Validation
        // compares the whole baseline by value, so an ordered source list also
        // puts each source's occurrence id and the list's cardinality under
        // that comparison.
        let mut drifted_revision = previous_sources.clone();
        drifted_revision[0].semantic_revision = revision(&object_id("left-v1"), 99);
        let mut drifted_object_id = previous_sources.clone();
        drifted_object_id[0].semantic_revision = revision(&object_id("left-v2"), 9);
        let mut drifted_occurrence_id = previous_sources.clone();
        drifted_occurrence_id[0].occurrence_id = SqlMvRelationOccurrenceId::new(2);
        let mut dropped_source = previous_sources.clone();
        dropped_source.pop();

        let baseline = |previous_sources: Vec<RefreshStateBaselineSource>,
                        target_snapshot_id: Option<i64>,
                        target_table_uuid: &str,
                        definition_fingerprint: &str| {
            RefreshStateBaseline::SnapshotBacked {
                previous_sources,
                target_snapshot_id,
                target_table_uuid: target_table_uuid.to_string(),
                definition_fingerprint: definition_fingerprint.to_string(),
            }
        };

        let drifts = [
            (
                "previous source semantic revision",
                baseline(
                    drifted_revision,
                    target_snapshot_id,
                    &target_table_uuid,
                    &definition_fingerprint,
                ),
            ),
            (
                "previous source table object id",
                baseline(
                    drifted_object_id,
                    target_snapshot_id,
                    &target_table_uuid,
                    &definition_fingerprint,
                ),
            ),
            (
                "previous source occurrence id",
                baseline(
                    drifted_occurrence_id,
                    target_snapshot_id,
                    &target_table_uuid,
                    &definition_fingerprint,
                ),
            ),
            (
                "previous source cardinality",
                baseline(
                    dropped_source,
                    target_snapshot_id,
                    &target_table_uuid,
                    &definition_fingerprint,
                ),
            ),
            (
                "target snapshot id",
                baseline(
                    previous_sources.clone(),
                    Some(11),
                    &target_table_uuid,
                    &definition_fingerprint,
                ),
            ),
            (
                "target table uuid",
                baseline(
                    previous_sources.clone(),
                    target_snapshot_id,
                    "target-v2",
                    &definition_fingerprint,
                ),
            ),
            (
                "definition fingerprint",
                baseline(
                    previous_sources,
                    target_snapshot_id,
                    &target_table_uuid,
                    "definition-v2",
                ),
            ),
        ];

        for (component, drift) in &drifts {
            assert_ne!(
                *drift, contract.state_baseline,
                "{component} case must actually drift from the planned baseline"
            );
            let error = validate(
                &contract,
                MvStorageEngine::Iceberg,
                Some(42),
                &contract.target,
                &contract.base_refs,
                drift,
                Some(&contract.snapshot_pins),
            )
            .unwrap_err();
            assert!(error.contains("state baseline"), "{component}: {error}");
        }
    }

    #[test]
    fn validation_reports_identity_failures_in_contract_order() {
        let contract = contract();
        let wrong_target = target("wrong");
        let wrong_bases = vec![base(9, "wrong")];
        let pinless = RefreshStateBaseline::Pinless;
        let empty_pins = BTreeMap::new();

        let observations = [
            (
                MvStorageEngine::StarRocks,
                Some(99),
                &wrong_target,
                wrong_bases.as_slice(),
                &pinless,
                "backend",
            ),
            (
                MvStorageEngine::Iceberg,
                Some(99),
                &wrong_target,
                wrong_bases.as_slice(),
                &pinless,
                "mv id",
            ),
            (
                MvStorageEngine::Iceberg,
                Some(42),
                &wrong_target,
                wrong_bases.as_slice(),
                &pinless,
                "target",
            ),
            (
                MvStorageEngine::Iceberg,
                Some(42),
                &contract.target,
                wrong_bases.as_slice(),
                &pinless,
                "base refs",
            ),
            (
                MvStorageEngine::Iceberg,
                Some(42),
                &contract.target,
                contract.base_refs.as_slice(),
                &pinless,
                "state baseline",
            ),
        ];

        for (backend, mv_id, target, bases, baseline, expected) in observations {
            let error = validate(
                &contract,
                backend,
                mv_id,
                target,
                bases,
                baseline,
                Some(&empty_pins),
            )
            .unwrap_err();
            assert!(
                error.contains(expected),
                "expected {expected:?} in {error:?}"
            );
        }
    }

    #[test]
    fn pinless_contract_requires_empty_plans_and_no_observed_pin_map() {
        let mut contract = contract();
        contract.storage_engine = MvStorageEngine::StarRocks;
        contract.mv_id = None;
        contract.state_baseline = RefreshStateBaseline::Pinless;
        contract.snapshot_pins.clear();

        validate(
            &contract,
            MvStorageEngine::StarRocks,
            None,
            &contract.target,
            &contract.base_refs,
            &contract.state_baseline,
            None,
        )
        .expect("valid pinless contract should pass");

        let empty = BTreeMap::new();
        let error = validate(
            &contract,
            MvStorageEngine::StarRocks,
            None,
            &contract.target,
            &contract.base_refs,
            &contract.state_baseline,
            Some(&empty),
        )
        .unwrap_err();
        assert!(error.contains("pinless"), "{error}");
    }

    #[test]
    fn metadata_only_dispatches_only_metadata_closure() {
        let calls = RefCell::new(Vec::new());
        let result = dispatch_refresh_decision(
            ExecutableRefreshDecision::MetadataOnly,
            || {
                calls.borrow_mut().push("skip");
                Ok::<_, String>(0)
            },
            || {
                calls.borrow_mut().push("first");
                Ok(1)
            },
            || {
                calls.borrow_mut().push("metadata");
                Ok(2)
            },
            || {
                calls.borrow_mut().push("incremental");
                Ok(3)
            },
        )
        .unwrap();

        assert_eq!(result, 2);
        assert_eq!(*calls.borrow(), vec!["metadata"]);
    }

    #[test]
    fn dispatches_each_executable_decision_exactly_once() {
        for (decision, expected) in [
            (ExecutableRefreshDecision::SkipEmpty, "skip"),
            (ExecutableRefreshDecision::FirstRefresh, "first"),
            (ExecutableRefreshDecision::MetadataOnly, "metadata"),
            (ExecutableRefreshDecision::Incremental, "incremental"),
        ] {
            let calls = RefCell::new(Vec::new());
            let actual = dispatch_refresh_decision(
                decision,
                || {
                    calls.borrow_mut().push("skip");
                    Ok::<_, String>("skip")
                },
                || {
                    calls.borrow_mut().push("first");
                    Ok("first")
                },
                || {
                    calls.borrow_mut().push("metadata");
                    Ok("metadata")
                },
                || {
                    calls.borrow_mut().push("incremental");
                    Ok("incremental")
                },
            )
            .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(*calls.borrow(), vec![expected]);
        }
    }
}
