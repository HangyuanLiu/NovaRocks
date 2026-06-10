//! Unified partition-derivation library for Iceberg MV refresh.
//!
//! `AffectedTargetPartitions` is the single result type for every affected-
//! partition source (plan-time manifest planning and delta-chunk evaluation).
//! `NotDerived` carries an explicit reason; consumers decide via
//! `PartitionPruningPolicy` (BestEffort in v1, spec D5) whether that means
//! "no pruning" or "fail the refresh".

use std::collections::BTreeSet;

use crate::engine::mv::partition::MvPartitionKey;
use crate::meta::repository::mv_contract::{ExpressionKind, MvSchemaContract};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedTargetPartitions {
    Unpartitioned,
    Known { partitions: BTreeSet<MvPartitionKey> },
    NotDerived { reason: String },
}

impl AffectedTargetPartitions {
    pub(crate) fn known<I: IntoIterator<Item = MvPartitionKey>>(partitions: I) -> Self {
        Self::Known {
            partitions: partitions.into_iter().collect(),
        }
    }

    pub(crate) fn not_derived(reason: impl Into<String>) -> Self {
        Self::NotDerived {
            reason: reason.into(),
        }
    }

    pub(crate) fn not_derived_reason(&self) -> Option<&str> {
        match self {
            Self::NotDerived { reason } => Some(reason.as_str()),
            Self::Unpartitioned | Self::Known { .. } => None,
        }
    }

    pub(crate) fn is_not_derived(&self) -> bool {
        matches!(self, Self::NotDerived { .. })
    }

    pub(crate) fn partition_count(&self) -> usize {
        match self {
            Self::Unpartitioned | Self::NotDerived { .. } => 0,
            Self::Known { partitions } => partitions.len(),
        }
    }
}

/// Reasons aggregate-delta partition derivation can refuse a delta batch.
/// Every variant carries enough context for the refresh error message to
/// name the failing field and / or value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedPartitionError {
    /// The schema contract has no `target.partition` but the caller expected
    /// a partitioned MV. Only raised when the layout reports group-key columns
    /// but the contract is unpartitioned (callers should treat `partition =
    /// None` as Unpartitioned, not as an error — this variant is reserved
    /// for drift between layout and contract).
    ContractMissing(String),
    /// Transform listed in the contract has no first-class derivation rule.
    TransformUnsupported { field: String, transform: String },
    /// Output column referenced by the partition field is not a pure column
    /// expression, OR resolves to a non-group-key column in the layout.
    OutputLineageNotPureColumn { field: String },
    /// Partition field references a target visible column that does not
    /// exist in the contract, or whose backing visible column is missing
    /// from the layout / from the delta chunk schema.
    GroupKeyColumnMissing { field: String, reason: String },
    /// Group-key column in the delta chunk has an Arrow type that the
    /// Iceberg transform function refuses.
    GroupKeyTypeMismatch {
        field: String,
        want: String,
        got: String,
    },
    /// `iceberg::transform::create_transform_function(...).transform(array)`
    /// itself returned an error.
    TransformFailed { field: String, source: String },
}

impl std::fmt::Display for AffectedPartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ContractMissing(reason) => write!(
                f,
                "aggregate target partition contract missing or inconsistent: {reason}"
            ),
            Self::TransformUnsupported { field, transform } => write!(
                f,
                "aggregate target partition field {field} uses unsupported transform {transform}"
            ),
            Self::OutputLineageNotPureColumn { field } => write!(
                f,
                "aggregate target partition field {field} requires row-evaluation fallback"
            ),
            Self::GroupKeyColumnMissing { field, reason } => {
                write!(f, "aggregate target partition field {field}: {reason}")
            }
            Self::GroupKeyTypeMismatch { field, want, got } => write!(
                f,
                "aggregate target partition field {field} delta column type mismatch: want {want}, got {got}"
            ),
            Self::TransformFailed { field, source } => write!(
                f,
                "aggregate target partition field {field} transform failed: {source}"
            ),
        }
    }
}

impl std::error::Error for AffectedPartitionError {}

pub(crate) fn contract_transform_to_iceberg(
    transform: &crate::meta::repository::mv_contract::MvPartitionTransformContract,
    field: &str,
) -> Result<iceberg::spec::Transform, AffectedPartitionError> {
    use crate::meta::repository::mv_contract::MvPartitionTransformContract as C;
    match transform {
        C::Identity => Ok(iceberg::spec::Transform::Identity),
        C::Year => Ok(iceberg::spec::Transform::Year),
        C::Month => Ok(iceberg::spec::Transform::Month),
        C::Day => Ok(iceberg::spec::Transform::Day),
        C::Hour => Ok(iceberg::spec::Transform::Hour),
        C::Bucket { num_buckets } => Ok(iceberg::spec::Transform::Bucket(*num_buckets)),
        C::Truncate { width } => Ok(iceberg::spec::Transform::Truncate(*width)),
        C::Void => Err(AffectedPartitionError::TransformUnsupported {
            field: field.to_string(),
            transform: "void".to_string(),
        }),
    }
}

/// Plan-time resolution result: which delta output column feeds each target
/// partition field, and through which Iceberg transform. Resolved purely from
/// the persisted contract — no layout / chunk dependency (spec D5: binding to
/// physical chunk columns happens in the apply-side binder).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartitionDerivationSpec {
    pub target_spec_id: i32,
    pub fields: Vec<PartitionDerivationField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartitionDerivationField {
    pub partition_field_name: String,
    pub source_target_field_id: i32,
    /// Position in `contract.target.visible_columns` (== output column index).
    pub output_index: usize,
    pub transform: iceberg::spec::Transform,
}

/// Resolve the contract-level partition derivation spec.
///
/// Returns `Ok(None)` for unpartitioned contracts (no `target.partition`, or
/// an empty field list — mirroring `is_unpartitioned_mv_contract`). Errors are
/// the plan-time subset of [`AffectedPartitionError`]: `TransformUnsupported`,
/// `OutputLineageNotPureColumn`, `GroupKeyColumnMissing` (contract drift).
pub(crate) fn resolve_partition_derivation_spec(
    contract: &MvSchemaContract,
) -> Result<Option<PartitionDerivationSpec>, AffectedPartitionError> {
    let Some(partition) = contract.target.partition.as_ref() else {
        return Ok(None);
    };
    if partition.fields.is_empty() {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        let output_index = contract
            .target
            .visible_columns
            .iter()
            .position(|col| col.target_field_id == partition_field.source_target_field_id)
            .ok_or_else(|| AffectedPartitionError::GroupKeyColumnMissing {
                field: partition_field.partition_field_name.clone(),
                reason: format!(
                    "contract has no visible column for target field id {}",
                    partition_field.source_target_field_id
                ),
            })?;

        let lineage = contract.output.columns.get(output_index).ok_or_else(|| {
            AffectedPartitionError::OutputLineageNotPureColumn {
                field: partition_field.partition_field_name.clone(),
            }
        })?;
        let is_single_base_column = lineage.expression.kind == ExpressionKind::Column
            && lineage.expression.referenced_base_field_ids.len() == 1;
        let is_join_column = lineage.expression.kind == ExpressionKind::Column
            && lineage.expression.referenced_base_field_ids.is_empty()
            && lineage.expression.referenced_base_fields.len() == 1;
        if !is_single_base_column && !is_join_column {
            return Err(AffectedPartitionError::OutputLineageNotPureColumn {
                field: partition_field.partition_field_name.clone(),
            });
        }

        let transform = contract_transform_to_iceberg(
            &partition_field.transform,
            &partition_field.partition_field_name,
        )?;

        fields.push(PartitionDerivationField {
            partition_field_name: partition_field.partition_field_name.clone(),
            source_target_field_id: partition_field.source_target_field_id,
            output_index,
            transform,
        });
    }

    Ok(Some(PartitionDerivationSpec {
        target_spec_id: partition.target_spec_id,
        fields,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};

    fn key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn affected_target_partitions_known_dedupes_and_sorts() {
        let result = AffectedTargetPartitions::known([key("b"), key("a"), key("a")]);
        let AffectedTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert_eq!(
            partitions.into_iter().collect::<Vec<_>>(),
            vec![key("a"), key("b")]
        );
    }

    #[test]
    fn affected_target_partitions_not_derived_preserves_reason() {
        let result = AffectedTargetPartitions::not_derived("join MV planning not implemented");
        assert_eq!(
            result.not_derived_reason(),
            Some("join MV planning not implemented")
        );
        assert!(result.is_not_derived());
        assert_eq!(result.partition_count(), 0);
    }

    #[test]
    fn affected_target_partitions_unpartitioned_is_not_not_derived() {
        assert!(!AffectedTargetPartitions::Unpartitioned.is_not_derived());
        assert_eq!(AffectedTargetPartitions::Unpartitioned.partition_count(), 0);
    }

    // --- Moved from aggregate_delta.rs: AffectedPartitionError display test ---

    use crate::meta::repository::mv_contract::MvPartitionTransformContract;

    #[test]
    fn affected_partition_error_display_includes_field_and_reason() {
        let err = AffectedPartitionError::TransformUnsupported {
            field: "region".to_string(),
            transform: "void".to_string(),
        };
        let message = format!("{err}");
        assert!(message.contains("region"), "{message}");
        assert!(message.contains("void"), "{message}");
    }

    // --- Moved from aggregate_delta.rs: contract_transform_to_iceberg tests ---

    #[test]
    fn contract_transform_to_iceberg_handles_all_first_class_transforms() {
        for (input, expect) in [
            (
                MvPartitionTransformContract::Identity,
                iceberg::spec::Transform::Identity,
            ),
            (
                MvPartitionTransformContract::Year,
                iceberg::spec::Transform::Year,
            ),
            (
                MvPartitionTransformContract::Month,
                iceberg::spec::Transform::Month,
            ),
            (
                MvPartitionTransformContract::Day,
                iceberg::spec::Transform::Day,
            ),
            (
                MvPartitionTransformContract::Hour,
                iceberg::spec::Transform::Hour,
            ),
            (
                MvPartitionTransformContract::Bucket { num_buckets: 8 },
                iceberg::spec::Transform::Bucket(8),
            ),
            (
                MvPartitionTransformContract::Truncate { width: 16 },
                iceberg::spec::Transform::Truncate(16),
            ),
        ] {
            let result =
                contract_transform_to_iceberg(&input, "test_field").expect("transform conversion");
            assert_eq!(result, expect, "input={input:?}");
        }
    }

    #[test]
    fn contract_transform_to_iceberg_rejects_void() {
        let err = contract_transform_to_iceberg(&MvPartitionTransformContract::Void, "test_field")
            .unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::TransformUnsupported { ref field, ref transform }
                if field == "test_field" && transform == "void"
        ));
    }

    // --- Test fixture: copied verbatim from aggregate_delta.rs:720-799 ---

    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract, MvSchemaContract,
        OutputColumnLineage, OutputContract, TargetContract, TargetVisibleColumn,
    };

    fn count_contract_with_partition(
        partition_field_name: &str,
        transform: MvPartitionTransformContract,
        source_target_field_id: i32,
    ) -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.sales.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "region".to_string(),
                        type_signature: "string".to_string(),
                        required: true,
                    }],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: vec![
                    OutputColumnLineage {
                        expression: ExpressionLineage {
                            kind: ExpressionKind::Column,
                            referenced_base_field_ids: vec![1],
                            referenced_base_fields: Vec::new(),
                        },
                    },
                    OutputColumnLineage {
                        expression: ExpressionLineage {
                            kind: ExpressionKind::Column,
                            referenced_base_field_ids: Vec::new(),
                            referenced_base_fields: Vec::new(),
                        },
                    },
                ],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.analytics.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![
                    TargetVisibleColumn {
                        output_name: partition_field_name.to_string(),
                        target_field_id: source_target_field_id,
                        type_signature: "string".to_string(),
                        nullable: true,
                    },
                    TargetVisibleColumn {
                        output_name: "c".to_string(),
                        target_field_id: 12,
                        type_signature: "bigint".to_string(),
                        nullable: false,
                    },
                ],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__row_id__".to_string(),
                    target_field_id: 10,
                    source: ApplyKeySource::GroupRowId,
                },
                partition: Some(MvPartitionContract {
                    target_spec_id: 7,
                    fields: vec![MvPartitionFieldContract {
                        partition_field_id: 100,
                        partition_field_name: partition_field_name.to_string(),
                        source_target_field_id,
                        source_column_name: partition_field_name.to_string(),
                        transform,
                    }],
                }),
            },
        }
    }

    // --- New tests for resolve_partition_derivation_spec ---

    #[test]
    fn resolve_returns_none_for_unpartitioned_contract() {
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition = None;
        assert!(resolve_partition_derivation_spec(&contract).unwrap().is_none());
    }

    #[test]
    fn resolve_returns_none_for_empty_partition_fields() {
        // Mirrors is_unpartitioned_mv_contract: empty fields == unpartitioned.
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition.as_mut().unwrap().fields.clear();
        assert!(resolve_partition_derivation_spec(&contract).unwrap().is_none());
    }

    #[test]
    fn resolve_produces_spec_for_pure_column_identity_partition() {
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        let spec = resolve_partition_derivation_spec(&contract)
            .expect("resolve")
            .expect("partitioned");
        assert_eq!(spec.target_spec_id, 7);
        assert_eq!(spec.fields.len(), 1);
        assert_eq!(spec.fields[0].partition_field_name, "region");
        assert_eq!(spec.fields[0].source_target_field_id, 11);
        assert_eq!(spec.fields[0].output_index, 0);
        assert_eq!(spec.fields[0].transform, iceberg::spec::Transform::Identity);
    }

    #[test]
    fn resolve_rejects_void_transform() {
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Void, 11);
        let err = resolve_partition_derivation_spec(&contract).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::TransformUnsupported { ref field, ref transform }
                if field == "region" && transform == "void"
        ));
    }

    #[test]
    fn resolve_rejects_missing_target_field() {
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition.as_mut().unwrap().fields[0].source_target_field_id = 999;
        let err = resolve_partition_derivation_spec(&contract).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyColumnMissing { ref field, .. } if field == "region"
        ));
    }

    #[test]
    fn resolve_rejects_non_pure_output_lineage() {
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.output.columns[0].expression.kind = ExpressionKind::Func;
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = vec![1, 2];
        let err = resolve_partition_derivation_spec(&contract).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::OutputLineageNotPureColumn { ref field } if field == "region"
        ));
    }
}
