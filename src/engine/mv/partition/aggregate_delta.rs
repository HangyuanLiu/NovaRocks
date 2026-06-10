use std::collections::BTreeSet;

use crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout;
use crate::engine::mv::partition::MvPartitionKey;
use crate::engine::mv::partition::derivation::AffectedPartitionError;
use crate::exec::chunk::Chunk;
use crate::meta::repository::mv_contract::MvSchemaContract;

/// Set of MV target partitions affected by a signed aggregate delta batch.
///
/// `Unpartitioned` is the legitimate state for non-partitioned MV targets;
/// callers MUST NOT treat it as "no information available". A failed
/// derivation surfaces an [`AffectedPartitionError`] instead — the design
/// is strict fail-fast, no silent fallback (see spec §5 principle 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedAggregateTargetPartitions {
    Unpartitioned,
    Known {
        partitions: BTreeSet<MvPartitionKey>,
    },
}

impl AffectedAggregateTargetPartitions {
    pub(crate) fn known<I: IntoIterator<Item = MvPartitionKey>>(partitions: I) -> Self {
        Self::Known {
            partitions: partitions.into_iter().collect(),
        }
    }

    pub(crate) fn partitions(&self) -> Option<&BTreeSet<MvPartitionKey>> {
        match self {
            Self::Unpartitioned => None,
            Self::Known { partitions } => Some(partitions),
        }
    }
}

/// Inputs required to derive the affected target partitions from a signed
/// aggregate delta batch.
pub(crate) struct AggregateDeltaPartitionInput<'a> {
    pub(crate) layout: &'a AggregateMvLayout,
    pub(crate) schema_contract: &'a MvSchemaContract,
    pub(crate) delta_chunks: &'a [Chunk],
}

/// Derive the set of target partitions touched by a signed aggregate delta
/// batch given the MV schema contract and the aggregate layout.
///
/// Returns `Unpartitioned` when the contract has no `target.partition`.
/// Returns `Known { partitions }` with the deduplicated, sorted set of all
/// partition keys touched by the delta rows.
pub(crate) fn derive_from_aggregate_delta(
    input: &AggregateDeltaPartitionInput<'_>,
) -> Result<AffectedAggregateTargetPartitions, AffectedPartitionError> {
    use crate::engine::mv::partition::derivation::{
        bind_spec_to_aggregate_layout, evaluate_partition_spec, resolve_partition_derivation_spec,
    };

    let Some(spec) = resolve_partition_derivation_spec(input.schema_contract)? else {
        return Ok(AffectedAggregateTargetPartitions::Unpartitioned);
    };
    let bound = bind_spec_to_aggregate_layout(&spec, input.layout)?;
    let partitions = evaluate_partition_spec(spec.target_spec_id, &bound, input.delta_chunks)?;
    Ok(AffectedAggregateTargetPartitions::Known { partitions })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};

    fn sample_key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn affected_aggregate_target_partitions_known_dedupes_and_sorts() {
        let result = AffectedAggregateTargetPartitions::known([
            sample_key("b"),
            sample_key("a"),
            sample_key("a"),
        ]);
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert_eq!(
            partitions.into_iter().collect::<Vec<_>>(),
            vec![sample_key("a"), sample_key("b")]
        );
    }

    #[test]
    fn affected_aggregate_target_partitions_unpartitioned_has_no_partitions() {
        let result = AffectedAggregateTargetPartitions::Unpartitioned;
        assert!(result.partitions().is_none());
    }

    use crate::meta::repository::mv_contract::MvPartitionTransformContract;

    // --- derive_from_aggregate_delta tests ---

    use crate::connector::starrocks::table::ddl::starrocks_physical_column;
    use crate::connector::starrocks::table::mv_agg_state::{
        AggregateMvLayout, AggregateStateColumn, AggregateStateRole, AggregateVisibleColumn,
    };
    use crate::connector::starrocks::table::mv_shape::AggregateFunctionKind;
    use crate::exec::chunk::Chunk;
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvSchemaContract, OutputColumnLineage, OutputContract, TargetContract, TargetVisibleColumn,
    };
    use crate::sql::parser::ast::SqlType;
    use arrow::array::{Date32Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;

    fn count_layout_with_group_key(
        name: &str,
        data_type: DataType,
        sql_type: SqlType,
    ) -> AggregateMvLayout {
        let row_id = starrocks_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        );
        let group =
            starrocks_physical_column(name.to_string(), sql_type.clone(), true, true, false);
        let counter =
            starrocks_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
        let state = starrocks_physical_column(
            "__agg_state_c".to_string(),
            SqlType::BigInt,
            false,
            false,
            false,
        );
        AggregateMvLayout {
            row_id_column: row_id.clone(),
            visible_columns: vec![
                AggregateVisibleColumn {
                    name: name.to_string(),
                    data_type,
                    sql_type,
                    nullable: true,
                    source_index: 0,
                },
                AggregateVisibleColumn {
                    name: "c".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    source_index: 1,
                },
            ],
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_c".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                visible_source_index: 1,
                aggregate_index: 0,
                function: AggregateFunctionKind::Count,
                state_role: AggregateStateRole::Single,
                count_star: true,
            }],
            aggregate_input_types: vec![None],
            group_key_source_indexes: vec![0],
            physical_columns: vec![row_id, group, counter, state],
        }
    }

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

    fn batch_with_group_key(name: &str, dt: DataType, values: arrow::array::ArrayRef) -> Chunk {
        let n = values.len();
        let row_ids: Vec<String> = (0..n).map(|i| format!("rid-{i}")).collect();
        let row_id_arr: arrow::array::ArrayRef = StdArc::new(StringArray::from(row_ids));
        let counts: arrow::array::ArrayRef = StdArc::new(Int64Array::from(vec![1i64; n]));
        let states: arrow::array::ArrayRef = StdArc::new(Int64Array::from(vec![1i64; n]));
        let schema = StdArc::new(Schema::new(vec![
            Field::new("__row_id__", DataType::Utf8, false),
            Field::new(name, dt, true),
            Field::new("c", DataType::Int64, false),
            Field::new("__agg_state_c", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![row_id_arr, values, counts, states]).unwrap();
        crate::engine::record_batch_to_chunk(batch).unwrap()
    }

    #[test]
    fn derive_identity_returns_known_partition_per_unique_value() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")]))
                as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        let names: Vec<_> = partitions
            .iter()
            .map(|key| match &key.fields[0].value {
                MvPartitionValue::String(s) => s.clone(),
                MvPartitionValue::Null => "<NULL>".to_string(),
            })
            .collect();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
        for key in &partitions {
            assert_eq!(key.spec_id, 7);
            assert_eq!(key.fields[0].field_name, "region");
        }
    }

    #[test]
    fn derive_day_transform_normalizes_dates_to_day_buckets() {
        let layout = count_layout_with_group_key("ts", DataType::Date32, SqlType::Date);
        let contract = count_contract_with_partition("ts", MvPartitionTransformContract::Day, 11);
        // Two distinct days: 19500 and 19501.
        let chunk = batch_with_group_key(
            "ts",
            DataType::Date32,
            StdArc::new(Date32Array::from(vec![
                Some(19500),
                Some(19501),
                Some(19500),
            ])) as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        let values: Vec<_> = partitions
            .iter()
            .map(|key| match &key.fields[0].value {
                MvPartitionValue::String(s) => s.clone(),
                MvPartitionValue::Null => "<NULL>".to_string(),
            })
            .collect();
        // Day transform on a Date32 input should yield the integer day-since-epoch
        // for each distinct row. After dedup and sort: "19500", "19501".
        assert_eq!(values, vec!["19500".to_string(), "19501".to_string()]);
    }

    #[test]
    fn derive_bucket_transform_uses_iceberg_hash() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract = count_contract_with_partition(
            "region",
            MvPartitionTransformContract::Bucket { num_buckets: 8 },
            11,
        );
        // Build the chunk and run derivation.
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("east"), Some("west")]))
                as arrow::array::ArrayRef,
        );

        // Independently compute the expected bucket values via iceberg-rust
        // and assert the derivation produced exactly those.
        let arr: arrow::array::ArrayRef =
            StdArc::new(StringArray::from(vec![Some("east"), Some("west")]));
        let xform =
            iceberg::transform::create_transform_function(&iceberg::spec::Transform::Bucket(8))
                .expect("transform");
        let out = xform.transform(arr).expect("apply");
        let expected: Vec<String> = (0..out.len())
            .map(|i| {
                let arr = out.as_any().downcast_ref::<Int32Array>().expect("int32");
                arr.value(i).to_string()
            })
            .collect();

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        let got: std::collections::BTreeSet<String> = partitions
            .iter()
            .map(|key| match &key.fields[0].value {
                MvPartitionValue::String(s) => s.clone(),
                MvPartitionValue::Null => "<NULL>".to_string(),
            })
            .collect();
        let want: std::collections::BTreeSet<String> = expected.into_iter().collect();
        assert_eq!(got, want);
    }

    #[test]
    fn derive_unpartitioned_contract_returns_unpartitioned() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition = None;
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        assert!(matches!(
            derive_from_aggregate_delta(&input).unwrap(),
            AffectedAggregateTargetPartitions::Unpartitioned
        ));
    }

    #[test]
    fn derive_void_transform_returns_unsupported_error() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Void, 11);
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::TransformUnsupported { ref field, ref transform }
                if field == "region" && transform == "void"
        ));
    }

    #[test]
    fn derive_missing_target_field_returns_group_key_missing() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        contract.target.partition.as_mut().unwrap().fields[0].source_target_field_id = 999;
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyColumnMissing { ref field, .. } if field == "region"
        ));
    }

    #[test]
    fn derive_non_pure_output_lineage_returns_error() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        // Force the output column to look like a non-pure expression.
        contract.output.columns[0].expression.kind = ExpressionKind::Func;
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = vec![1, 2];

        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );
        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::OutputLineageNotPureColumn { ref field } if field == "region"
        ));
    }

    #[test]
    fn derive_missing_chunk_column_returns_group_key_missing() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        // Build a chunk whose group-key column name does NOT match the layout's.
        let row_ids: arrow::array::ArrayRef = StdArc::new(StringArray::from(vec![Some("rid-0")]));
        let other: arrow::array::ArrayRef = StdArc::new(StringArray::from(vec![Some("a")]));
        let counts: arrow::array::ArrayRef = StdArc::new(Int64Array::from(vec![1i64]));
        let states: arrow::array::ArrayRef = StdArc::new(Int64Array::from(vec![1i64]));
        let schema = StdArc::new(Schema::new(vec![
            Field::new("__row_id__", DataType::Utf8, false),
            Field::new("not_region", DataType::Utf8, true),
            Field::new("c", DataType::Int64, false),
            Field::new("__agg_state_c", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![row_ids, other, counts, states]).unwrap();
        let chunk = crate::engine::record_batch_to_chunk(batch).unwrap();

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::GroupKeyColumnMissing { ref field, .. } if field == "region"
        ));
    }

    #[test]
    fn derive_empty_chunks_returns_known_empty_set() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(Vec::<Option<&str>>::new())) as arrow::array::ArrayRef,
        );

        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert!(partitions.is_empty());
    }

    use crate::meta::repository::mv_contract::QualifiedFieldLineage;

    #[test]
    fn derive_accepts_join_aggregate_pure_column_lineage() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        // Swap the lineage from single-base form to join form: clear
        // referenced_base_field_ids and populate referenced_base_fields
        // with a single qualified ref. This simulates a join-aggregate MV
        // where the output column is backed by a qualified field reference
        // instead of a direct base field id.
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = Vec::new();
        contract.output.columns[0].expression.referenced_base_fields =
            vec![QualifiedFieldLineage {
                table_fqn: "ice.sales.orders".to_string(),
                qualifier_at_create: "base".to_string(),
                field_id: 1,
            }];

        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a"), Some("b")])) as arrow::array::ArrayRef,
        );
        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let result = derive_from_aggregate_delta(&input).expect("derive");
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn derive_rejects_join_aggregate_multi_base_field_lineage() {
        let layout = count_layout_with_group_key("region", DataType::Utf8, SqlType::String);
        let mut contract =
            count_contract_with_partition("region", MvPartitionTransformContract::Identity, 11);
        // Two base-field refs simulates a computed/joined expression, which
        // is NOT a pure passthrough and should be rejected. This represents
        // a scenario where the output column depends on multiple base fields
        // (e.g., a computed column in a join context).
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids = Vec::new();
        contract.output.columns[0].expression.referenced_base_fields = vec![
            QualifiedFieldLineage {
                table_fqn: "ice.sales.orders".to_string(),
                qualifier_at_create: "f".to_string(),
                field_id: 1,
            },
            QualifiedFieldLineage {
                table_fqn: "ice.sales.orders".to_string(),
                qualifier_at_create: "d".to_string(),
                field_id: 2,
            },
        ];

        let chunk = batch_with_group_key(
            "region",
            DataType::Utf8,
            StdArc::new(StringArray::from(vec![Some("a")])) as arrow::array::ArrayRef,
        );
        let input = AggregateDeltaPartitionInput {
            layout: &layout,
            schema_contract: &contract,
            delta_chunks: &[chunk],
        };
        let err = derive_from_aggregate_delta(&input).unwrap_err();
        assert!(matches!(
            err,
            AffectedPartitionError::OutputLineageNotPureColumn { ref field } if field == "region"
        ));
    }
}
