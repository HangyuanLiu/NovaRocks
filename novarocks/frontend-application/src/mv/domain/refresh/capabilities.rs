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

use crate::mv::domain::refresh::apply_key::ApplyKeyValueType;
use crate::mv::domain::refresh::snapshot::BaseSnapshotPolicy;
use novarocks_mv_application::persistence::codec::{ApplyKeyKind, InterpretationDocument};
use novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings;

/// What a NotDerivable partition derivation outcome means for the refresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartitionPruningPolicy {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    Required,
    BestEffort,
}

/// The compact row-identity discriminant needed by refresh execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefreshIdentity {
    VisibleTuple,
    GroupRowId,
    BranchScoped(Box<RefreshIdentity>),
}

/// Refresh-time capabilities reconstructed from a persisted MV schema contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshCapabilities {
    pub(crate) snapshot_policy: BaseSnapshotPolicy,
    pub has_agg_state: bool,
    pub(crate) identity: RefreshIdentity,
    pub apply_key_column: Option<String>,
    pub(crate) apply_key_value_type: Option<ApplyKeyValueType>,
    pub(crate) partition_pruning: PartitionPruningPolicy,
}

impl RefreshCapabilities {
    /// Reconstruct refresh capabilities from canonical D/L facts and the exact
    /// provider observation that L was validated against.
    ///
    /// `relation_occurrence_count` and `has_join` describe D's immutable
    /// relation shape: a join pair may start with one empty side, while a
    /// fan-in of independent relations may not. The physical apply-key column
    /// name is never persisted by the Accelerator; it comes from the same
    /// exact target observation that produced `bindings`.
    pub fn from_canonical_facts(
        interpretation: &InterpretationDocument,
        bindings: &MvRuntimeBindings,
        relation_occurrence_count: usize,
        has_join: bool,
    ) -> Result<RefreshCapabilities, String> {
        let has_agg = !interpretation.aggregates.is_empty();
        let has_branch = !interpretation.branches.is_empty();

        match (has_join, has_agg, has_branch) {
            (false, false, false)
            | (true, false, false)
            | (false, false, true)
            | (false, true, false)
            | (true, true, false)
            | (false, true, true)
            | (true, true, true) => {}
            _ => {
                return Err(format!(
                    "unsupported materialized-view interpretation shape \
                     (join={has_join}, agg={has_agg}, branch={has_branch})"
                ));
            }
        }

        let snapshot_policy = if has_branch {
            BaseSnapshotPolicy::AllBasesRequired
        } else if has_join {
            BaseSnapshotPolicy::JoinPairPartialInitialSkip
        } else if relation_occurrence_count > 1 {
            BaseSnapshotPolicy::AllBasesRequired
        } else {
            BaseSnapshotPolicy::SingleBase
        };

        let (identity, apply_key_column, apply_key_value_type) = if has_agg {
            if interpretation.apply_key.as_ref().map(|key| key.kind)
                != Some(ApplyKeyKind::GroupRowId)
            {
                return Err("aggregate MV requires the exact GroupRowId interpretation".into());
            }
            let [apply_key] = bindings.apply_key.as_slice() else {
                return Err("aggregate MV requires one exact physical state key".into());
            };
            (
                if has_branch {
                    RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::GroupRowId))
                } else {
                    RefreshIdentity::GroupRowId
                },
                Some(apply_key.name.clone()),
                Some(if has_branch {
                    ApplyKeyValueType::BranchUtf8
                } else {
                    ApplyKeyValueType::Utf8
                }),
            )
        } else {
            if interpretation.apply_key.is_some()
                || !bindings.apply_key.is_empty()
                || !bindings.branches.is_empty()
            {
                return Err("visible-tuple MV may not persist identity fields".into());
            }
            (RefreshIdentity::VisibleTuple, None, None)
        };

        Ok(RefreshCapabilities {
            snapshot_policy,
            has_agg_state: has_agg,
            identity,
            apply_key_column,
            apply_key_value_type,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::persistence::runtime_bindings::MvPhysicalFieldFacts;
    use novarocks_mv_application::persistence::test_support::ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;

    fn interpretation() -> InterpretationDocument {
        ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(11))
            .interpretation
    }

    fn visible_tuple_interpretation() -> InterpretationDocument {
        let mut interpretation = interpretation();
        interpretation.apply_key = None;
        interpretation.aggregates.clear();
        interpretation.state_slots.clear();
        interpretation.branches.clear();
        interpretation
    }

    fn physical(name: &str) -> MvPhysicalFieldFacts {
        MvPhysicalFieldFacts {
            field_id: novarocks_mv_application::persistence::identity::FieldIdentity::try_new(
                vec![0xfe, 1],
            )
            .expect("test field identity"),
            name: name.to_string(),
            ordinal: 0,
            data_type:
                novarocks_mv_application::persistence::codec::MvLogicalType::from_schema_type(
                    novarocks_types::logical_type::LogicalType::Int64,
                    bytes::Bytes::from_static(b"exact-provider-int64"),
                )
                .unwrap(),
            legacy_scalar_type: Some(novarocks_types::logical_type::LogicalType::Int64),
            nullable: false,
        }
    }

    fn bindings(apply_key: Vec<MvPhysicalFieldFacts>) -> MvRuntimeBindings {
        MvRuntimeBindings {
            outputs: Vec::new(),
            aggregates: Vec::new(),
            apply_key,
            branches: Vec::new(),
        }
    }

    #[test]
    fn single_relation_projection_uses_visible_tuple_without_identity_fields() {
        let interpretation = visible_tuple_interpretation();

        let capabilities = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(Vec::new()),
            1,
            false,
        )
        .expect("projection capabilities");

        assert_eq!(capabilities.snapshot_policy, BaseSnapshotPolicy::SingleBase);
        assert!(!capabilities.has_agg_state);
        assert_eq!(capabilities.identity, RefreshIdentity::VisibleTuple);
        assert_eq!(capabilities.apply_key_value_type, None);
        assert_eq!(capabilities.apply_key_column, None);
    }

    #[test]
    fn a_join_pair_and_a_fan_in_of_the_same_arity_choose_different_policies() {
        let interpretation = visible_tuple_interpretation();

        let join = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(Vec::new()),
            2,
            true,
        )
        .expect("join capabilities");
        assert_eq!(
            join.snapshot_policy,
            BaseSnapshotPolicy::JoinPairPartialInitialSkip
        );
        assert_eq!(join.identity, RefreshIdentity::VisibleTuple);
        assert_eq!(join.apply_key_value_type, None);
        assert_eq!(join.apply_key_column, None);

        let fan_in = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(Vec::new()),
            2,
            false,
        )
        .expect("fan-in capabilities");
        assert_eq!(
            fan_in.snapshot_policy,
            BaseSnapshotPolicy::AllBasesRequired,
            "a fan-in of independent relations may not start with one empty side"
        );
    }

    #[test]
    fn branch_union_aggregate_scopes_its_identity_and_requires_every_base() {
        let interpretation = interpretation();
        assert_eq!(
            interpretation.apply_key.as_ref().unwrap().kind,
            ApplyKeyKind::GroupRowId
        );
        assert!(!interpretation.aggregates.is_empty());
        assert!(!interpretation.branches.is_empty());

        let capabilities = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(vec![physical("__nova_group_row_id")]),
            2,
            false,
        )
        .expect("branch aggregate capabilities");

        assert_eq!(
            capabilities.snapshot_policy,
            BaseSnapshotPolicy::AllBasesRequired
        );
        assert!(capabilities.has_agg_state);
        assert_eq!(
            capabilities.identity,
            RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::GroupRowId))
        );
        assert_eq!(
            capabilities.apply_key_value_type,
            Some(ApplyKeyValueType::BranchUtf8)
        );
        assert_eq!(
            capabilities.apply_key_column.as_deref(),
            Some("__nova_group_row_id")
        );
    }

    #[test]
    fn join_over_branches_without_aggregate_state_is_rejected() {
        let mut interpretation = interpretation();
        interpretation.apply_key = None;
        interpretation.aggregates.clear();

        let error = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(Vec::new()),
            2,
            true,
        )
        .expect_err("join + branch without aggregate must be rejected");

        assert_eq!(
            error,
            "unsupported materialized-view interpretation shape (join=true, agg=false, branch=true)"
        );
    }

    #[test]
    fn aggregate_target_without_exactly_one_physical_state_key_is_rejected() {
        let interpretation = interpretation();

        assert_eq!(
            RefreshCapabilities::from_canonical_facts(
                &interpretation,
                &bindings(Vec::new()),
                1,
                false
            )
            .expect_err("no apply key"),
            "aggregate MV requires one exact physical state key"
        );
        assert!(
            RefreshCapabilities::from_canonical_facts(
                &interpretation,
                &bindings(vec![physical("a"), physical("b")]),
                1,
                false
            )
            .is_err(),
            "two physical apply-key columns must be rejected"
        );
    }

    #[test]
    fn visible_tuple_target_rejects_stale_persisted_identity_fields() {
        let interpretation = visible_tuple_interpretation();
        assert!(
            RefreshCapabilities::from_canonical_facts(
                &interpretation,
                &bindings(vec![physical("__nova_row_id")]),
                1,
                false,
            )
            .is_err()
        );
        let mut stale = interpretation;
        stale.apply_key = self::interpretation().apply_key;
        assert!(
            RefreshCapabilities::from_canonical_facts(&stale, &bindings(Vec::new()), 1, false,)
                .is_err()
        );
    }
}
