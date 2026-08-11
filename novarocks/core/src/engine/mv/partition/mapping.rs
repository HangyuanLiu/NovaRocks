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

#[cfg(test)]
use crate::sql::planner::vocabulary::ApplyKeySource;

use crate::mv::model::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
use crate::mv::persistence::schema::{
    ExpressionKind, MvPartitionTransformContract, MvSchemaContract,
};
use novarocks_connector_iceberg::delta::{ChangePartitionFieldValue, ChangePartitionValue};

pub(crate) fn map_connector_partition_to_mv_key(
    contract: &MvSchemaContract,
    observation: &crate::mv::storage_observation::MvSchemaValidationObservation,
    connector_partition: &novarocks_spi::connector::ConnectorChangePartition,
) -> Result<Option<MvPartitionKey>, String> {
    let Some(partition) = &contract.target.partition else {
        return Ok(None);
    };
    let base_contract = std::iter::once(&contract.base)
        .chain(contract.bases.iter())
        .find(|base| base.table_uuid == observation.table_uuid())
        .ok_or_else(|| {
            format!(
                "MV partition mapping has no stable base contract for observed table UUID {}",
                observation.table_uuid()
            )
        })?;

    let mut mapped_fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        let output_index = contract
            .target
            .visible_columns
            .iter()
            .position(|column| column.target_field_id == partition_field.source_target_field_id)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} references missing target field {}",
                    partition_field.partition_field_name, partition_field.source_target_field_id
                )
            })?;
        let output_lineage = contract.output.columns.get(output_index).ok_or_else(|| {
            format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            )
        })?;
        if output_lineage.expression.kind != ExpressionKind::Column
            || output_lineage.expression.referenced_base_field_ids.len() != 1
        {
            return Err(format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            ));
        }
        let stable_field_id = output_lineage.expression.referenced_base_field_ids[0];
        if !base_contract
            .schema_at_create
            .fields
            .iter()
            .any(|field| field.field_id == stable_field_id)
        {
            return Err(format!(
                "MV partition field {} references unknown stable base field {}",
                partition_field.partition_field_name, stable_field_id
            ));
        }
        let observed_field = observation
            .fields()
            .iter()
            .find(|field| field.field_id() == stable_field_id)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} cannot resolve stable base field {} in the exact schema observation",
                    partition_field.partition_field_name, stable_field_id
                )
            })?;
        let connector_field = connector_partition
            .fields()
            .iter()
            .find(|field| field.source_column().eq_ignore_ascii_case(observed_field.name()))
            .ok_or_else(|| {
                format!(
                    "MV partition field {} has no connector partition fact for exact source column {}",
                    partition_field.partition_field_name,
                    observed_field.name()
                )
            })?;
        if !connector_transform_matches_contract(
            connector_field.transform(),
            &partition_field.transform,
        ) {
            return Err(format!(
                "MV partition field {} connector transform does not match its persisted contract",
                partition_field.partition_field_name
            ));
        }
        let value = match connector_field.value() {
            novarocks_spi::connector::ConnectorChangePartitionValue::Null => MvPartitionValue::Null,
            novarocks_spi::connector::ConnectorChangePartitionValue::String(value) => {
                MvPartitionValue::String(value.to_string())
            }
        };
        mapped_fields.push(MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            value,
        ));
    }

    Ok(Some(MvPartitionKey::new(
        partition.target_spec_id,
        mapped_fields,
    )))
}

fn connector_transform_matches_contract(
    connector: novarocks_spi::connector::ConnectorChangePartitionTransform,
    contract: &MvPartitionTransformContract,
) -> bool {
    use novarocks_spi::connector::ConnectorChangePartitionTransform as Connector;

    match (connector, contract) {
        (Connector::Identity, MvPartitionTransformContract::Identity)
        | (Connector::Year, MvPartitionTransformContract::Year)
        | (Connector::Month, MvPartitionTransformContract::Month)
        | (Connector::Day, MvPartitionTransformContract::Day)
        | (Connector::Hour, MvPartitionTransformContract::Hour) => true,
        (Connector::Bucket { buckets }, MvPartitionTransformContract::Bucket { num_buckets }) => {
            buckets.get() == *num_buckets
        }
        (
            Connector::Truncate { width },
            MvPartitionTransformContract::Truncate { width: expected },
        ) => width.get() == *expected,
        _ => false,
    }
}

pub(crate) fn map_file_partition_to_mv_key(
    contract: &MvSchemaContract,
    file_spec_id: i32,
    file_partition_values: &[ChangePartitionFieldValue],
) -> Result<Option<MvPartitionKey>, String> {
    let Some(partition) = &contract.target.partition else {
        return Ok(None);
    };

    let mut mapped_fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        let expected_transform_text = contract_transform_manifest_text(&partition_field.transform)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} uses unsupported transform {}",
                    partition_field.partition_field_name,
                    partition_transform_name(&partition_field.transform)
                )
            })?;

        let output_index = contract
            .target
            .visible_columns
            .iter()
            .position(|column| column.target_field_id == partition_field.source_target_field_id)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} references missing target field {}",
                    partition_field.partition_field_name, partition_field.source_target_field_id
                )
            })?;
        let output_lineage = contract.output.columns.get(output_index).ok_or_else(|| {
            format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            )
        })?;

        if output_lineage.expression.kind != ExpressionKind::Column
            || output_lineage.expression.referenced_base_field_ids.len() != 1
        {
            return Err(format!(
                "MV partition field {} requires row-evaluation fallback",
                partition_field.partition_field_name
            ));
        }

        let base_field_id = output_lineage.expression.referenced_base_field_ids[0];

        let mut matched_by_id_count = 0;
        let mut transform_mismatch: Option<&str> = None;
        let file_partition_value = file_partition_values
            .iter()
            .find(|value| {
                if value.source_field_id != base_field_id {
                    return false;
                }
                matched_by_id_count += 1;
                if value.transform.eq_ignore_ascii_case(&expected_transform_text) {
                    true
                } else {
                    transform_mismatch = Some(value.transform.as_str());
                    false
                }
            })
            .ok_or_else(|| {
                if matched_by_id_count == 0 {
                    format!(
                        "MV partition field {} cannot be proven from Iceberg file partition metadata for file spec {}",
                        partition_field.partition_field_name, file_spec_id
                    )
                } else {
                    format!(
                        "MV partition field {} file metadata transform {} mismatches contract transform {}",
                        partition_field.partition_field_name,
                        transform_mismatch.unwrap_or("<unknown>"),
                        expected_transform_text
                    )
                }
            })?;

        let value = match &file_partition_value.value {
            ChangePartitionValue::Primitive(value) => MvPartitionValue::String(value.clone()),
            ChangePartitionValue::Null => MvPartitionValue::Null,
            ChangePartitionValue::Unsupported(reason) => {
                return Err(format!(
                    "MV partition field {} has unsupported partition value: {}",
                    partition_field.partition_field_name, reason
                ));
            }
        };
        mapped_fields.push(MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            value,
        ));
    }

    Ok(Some(MvPartitionKey::new(
        partition.target_spec_id,
        mapped_fields,
    )))
}

/// Convert a single `ChangePartitionValue` to an `MvPartitionValue`,
/// returning an error string describing the file context when the value is
/// `Unsupported`. Used by callers that bypass `map_file_partition_to_mv_key` —
/// notably the iceberg MV target locator, which derives the key field-by-field
/// rather than through the schema contract.
pub(crate) fn change_partition_value_to_mv_value(
    file_path: &str,
    value: &ChangePartitionValue,
) -> Result<MvPartitionValue, String> {
    match value {
        ChangePartitionValue::Primitive(v) => Ok(MvPartitionValue::String(v.clone())),
        ChangePartitionValue::Null => Ok(MvPartitionValue::Null),
        ChangePartitionValue::Unsupported(reason) => Err(format!(
            "iceberg file `{file_path}` has unsupported partition value: {reason}"
        )),
    }
}

/// Maps a contract transform to the manifest text produced by
/// `novarocks_connector_iceberg::delta::change_partition_transform_name`,
/// which uses `{:?}` (Rust Debug) on `novarocks_connector_iceberg::iceberg::spec::Transform` and lowercases
/// the result. This gives `bucket(8)` / `truncate(16)` with round parens,
/// NOT the `bucket[8]` form from Iceberg's `Display` impl. Returns `None` for
/// `Void` — void partitions carry no useful pruning information.
fn contract_transform_manifest_text(transform: &MvPartitionTransformContract) -> Option<String> {
    match transform {
        MvPartitionTransformContract::Identity => Some("identity".to_string()),
        MvPartitionTransformContract::Year => Some("year".to_string()),
        MvPartitionTransformContract::Month => Some("month".to_string()),
        MvPartitionTransformContract::Day => Some("day".to_string()),
        MvPartitionTransformContract::Hour => Some("hour".to_string()),
        MvPartitionTransformContract::Bucket { num_buckets } => {
            Some(format!("bucket({num_buckets})"))
        }
        MvPartitionTransformContract::Truncate { width } => Some(format!("truncate({width})")),
        MvPartitionTransformContract::Void => None,
    }
}

fn partition_transform_name(transform: &MvPartitionTransformContract) -> String {
    contract_transform_manifest_text(transform).unwrap_or_else(|| "Void".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::persistence::schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };

    fn contract_with_partition(transform: MvPartitionTransformContract) -> MvSchemaContract {
        let mut contract = contract_with_identity_partition();
        let partition = contract
            .target
            .partition
            .as_mut()
            .expect("identity helper always builds a partition");
        partition.fields[0].transform = transform;
        contract
    }

    fn partition_value(
        transform_text: &str,
        value: ChangePartitionValue,
    ) -> ChangePartitionFieldValue {
        ChangePartitionFieldValue {
            source_field_id: 1,
            source_column: Some("id".to_string()),
            field_name: "id".to_string(),
            transform: transform_text.to_string(),
            value,
        }
    }

    fn contract_with_identity_partition() -> MvSchemaContract {
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
                        name_at_create: "id".to_string(),
                        type_signature: "int".to_string(),
                        required: true,
                    }],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: Vec::new(),
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.analytics.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 10,
                    type_signature: "int".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 11,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: Some(MvPartitionContract {
                    target_spec_id: 7,
                    fields: vec![MvPartitionFieldContract {
                        partition_field_id: 100,
                        partition_field_name: "id".to_string(),
                        source_target_field_id: 10,
                        source_column_name: "id".to_string(),
                        transform: MvPartitionTransformContract::Identity,
                    }],
                }),
            },
        }
    }

    #[test]
    fn maps_identity_partition_value_to_mv_key() {
        let contract = contract_with_identity_partition();
        let file_partition_values = vec![ChangePartitionFieldValue {
            source_field_id: 1,
            source_column: None,
            field_name: "renamed_id".to_string(),
            transform: "identity".to_string(),
            value: ChangePartitionValue::Primitive("42".to_string()),
        }];

        let mapped = map_file_partition_to_mv_key(&contract, 5, &file_partition_values).unwrap();

        assert_eq!(
            mapped,
            Some(MvPartitionKey::new(
                7,
                vec![MvPartitionKeyField::new(
                    "id".to_string(),
                    MvPartitionValue::String("42".to_string()),
                )],
            ))
        );
    }

    #[test]
    fn returns_none_for_unpartitioned_contract() {
        let mut contract = contract_with_identity_partition();
        contract.target.partition = None;

        let mapped = map_file_partition_to_mv_key(&contract, 5, &[]).unwrap();

        assert_eq!(mapped, None);
    }

    #[test]
    fn unsupported_partition_value_requires_unknown_mapping() {
        let contract = contract_with_identity_partition();
        let file_partition_values = vec![ChangePartitionFieldValue {
            source_field_id: 1,
            source_column: Some("id".to_string()),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: ChangePartitionValue::Unsupported("binary partition value".to_string()),
        }];

        let err = map_file_partition_to_mv_key(&contract, 5, &file_partition_values).unwrap_err();

        assert!(err.contains("unsupported partition value"));
        assert!(err.contains("binary partition value"));
    }

    #[test]
    fn maps_year_transform_to_mv_key() {
        let contract = contract_with_partition(MvPartitionTransformContract::Year);
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "year",
                ChangePartitionValue::Primitive("55".to_string()),
            )],
        )
        .unwrap();

        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("55".to_string())
        );
    }

    #[test]
    fn maps_month_day_hour_transforms() {
        for (contract_transform, manifest_text, value) in [
            (MvPartitionTransformContract::Month, "month", "660"),
            (MvPartitionTransformContract::Day, "day", "20000"),
            (MvPartitionTransformContract::Hour, "hour", "480000"),
        ] {
            let contract = contract_with_partition(contract_transform.clone());
            let mapped = map_file_partition_to_mv_key(
                &contract,
                7,
                &[partition_value(
                    manifest_text,
                    ChangePartitionValue::Primitive(value.to_string()),
                )],
            )
            .unwrap();
            assert_eq!(
                mapped.unwrap().fields[0].value,
                MvPartitionValue::String(value.to_string()),
                "transform {contract_transform:?} did not round-trip"
            );
        }
    }

    #[test]
    fn maps_bucket_transform_with_matching_arity() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Bucket { num_buckets: 8 });
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "bucket(8)",
                ChangePartitionValue::Primitive("3".to_string()),
            )],
        )
        .unwrap();
        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("3".to_string())
        );
    }

    #[test]
    fn rejects_bucket_transform_arity_mismatch() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Bucket { num_buckets: 8 });
        let err = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "bucket(16)",
                ChangePartitionValue::Primitive("3".to_string()),
            )],
        )
        .unwrap_err();
        assert!(err.contains("file metadata transform"), "{err}");
        assert!(err.contains("bucket(16)"), "{err}");
        assert!(err.contains("bucket(8)"), "{err}");
    }

    #[test]
    fn rejects_truncate_transform_width_mismatch() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Truncate { width: 16 });
        let err = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "truncate(32)",
                ChangePartitionValue::Primitive("ho".to_string()),
            )],
        )
        .unwrap_err();
        assert!(err.contains("file metadata transform"), "{err}");
        assert!(err.contains("truncate(32)"), "{err}");
        assert!(err.contains("truncate(16)"), "{err}");
    }

    #[test]
    fn maps_truncate_transform_with_matching_width() {
        let contract =
            contract_with_partition(MvPartitionTransformContract::Truncate { width: 16 });
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value(
                "truncate(16)",
                ChangePartitionValue::Primitive("ho".to_string()),
            )],
        )
        .unwrap();
        assert_eq!(
            mapped.unwrap().fields[0].value,
            MvPartitionValue::String("ho".to_string())
        );
    }

    #[test]
    fn rejects_void_transform() {
        let contract = contract_with_partition(MvPartitionTransformContract::Void);
        let err = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value("void", ChangePartitionValue::Null)],
        )
        .unwrap_err();
        assert!(err.contains("Void"), "{err}");
    }

    #[test]
    fn null_partition_value_renders_as_mv_null() {
        let contract = contract_with_partition(MvPartitionTransformContract::Day);
        let mapped = map_file_partition_to_mv_key(
            &contract,
            7,
            &[partition_value("day", ChangePartitionValue::Null)],
        )
        .unwrap();
        assert_eq!(mapped.unwrap().fields[0].value, MvPartitionValue::Null);
    }

    #[test]
    fn change_partition_field_values_is_reachable_for_mv_partition_module() {
        use novarocks_connector_iceberg::delta::change_partition_field_values;
        // We do not need to drive Iceberg metadata in a unit test — just make
        // sure the symbol is visible at the call site. If this fn ever becomes
        // private again, this test will fail to compile.
        let _fn_ptr: fn(
            &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
            i32,
            &novarocks_connector_iceberg::iceberg::spec::Struct,
        ) -> Result<
            Vec<novarocks_connector_iceberg::delta::ChangePartitionFieldValue>,
            novarocks_connector_iceberg::delta::ChangeError,
        > = change_partition_field_values;
    }
}
