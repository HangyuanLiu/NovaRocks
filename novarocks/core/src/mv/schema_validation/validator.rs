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

//! Refresh-time Iceberg MV schema contract validator.
//!
//! Single entry point: `validate_schema_contract`. Three-stage check:
//!   1. identity guard (uuid + format-version + row-lineage)
//!   2. schema-id fast path + base referenced-field exact match
//!   3. target visible columns + hidden apply-key exact match
//!
//! Decisions are explicit. There is NO fallback path: incompatible
//! contracts result in fail-fast errors that propagate to the user.

use super::model::{ContractDecision, CurrentIcebergTableView, SchemaEvolutionError};
use crate::mv::analysis::rebind::RebindColumn;
use crate::mv::persistence::schema::{
    ApplyKeySource, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME,
    JOIN_APPLY_KEY_COLUMN_NAME, MvPartitionTransformContract, MvSchemaContract,
};

pub(crate) fn validate_schema_contract(
    contract: &MvSchemaContract,
    current_base: &CurrentIcebergTableView<'_>,
    current_target: &CurrentIcebergTableView<'_>,
) -> ContractDecision {
    // Stage 1: identity guard.
    if let Some(err) = validate_identity_guards(contract, current_base, current_target) {
        return ContractDecision::Incompatible(err);
    }
    if let Some(err) = check_target_partition_spec(contract, current_target.default_partition_spec)
    {
        return ContractDecision::Incompatible(err);
    }
    validate_schema_contract_after_identity(contract, current_base.schema, current_target.schema)
}

fn validate_schema_contract_after_identity(
    contract: &MvSchemaContract,
    base_schema: &iceberg::spec::Schema,
    target_schema: &iceberg::spec::Schema,
) -> ContractDecision {
    // Stage 2 fast path.
    if base_schema.schema_id() == contract.base.schema_id_at_create
        && target_schema.schema_id() == contract.target.schema_id_at_create
    {
        if contract.aggregate.is_some() {
            if let Some(err) = check_target_schema(contract, target_schema) {
                return ContractDecision::Incompatible(err);
            }
        }
        return ContractDecision::CompatibleSafe;
    }
    // Stage 2 precise base check.
    let rebound = match check_base_referenced_fields(contract, base_schema) {
        Err(err) => return ContractDecision::Incompatible(err),
        Ok(r) => r,
    };
    // Stage 3 target check.
    if let Some(err) = check_target_schema(contract, target_schema) {
        return ContractDecision::Incompatible(err);
    }
    if rebound.is_empty() {
        ContractDecision::CompatibleSafe
    } else {
        ContractDecision::CompatibleSafeWithRebind {
            rebound_columns: rebound,
        }
    }
}

fn validate_identity_guards(
    contract: &MvSchemaContract,
    base: &CurrentIcebergTableView<'_>,
    target: &CurrentIcebergTableView<'_>,
) -> Option<SchemaEvolutionError> {
    if base.table_uuid != contract.base.table_uuid {
        return Some(SchemaEvolutionError::BaseTableIdentityChanged {
            expected: contract.base.table_uuid.clone(),
            actual: base.table_uuid.clone(),
        });
    }
    if base.format_version != iceberg::spec::FormatVersion::V3 {
        return Some(SchemaEvolutionError::BaseRowLineageContractBroken {
            reason: format!(
                "base table must be Iceberg format v3, found {:?}",
                base.format_version
            ),
        });
    }
    if !base.row_lineage_enabled {
        return Some(SchemaEvolutionError::BaseRowLineageContractBroken {
            reason: "base table property write.row-lineage must be true".to_string(),
        });
    }

    if target.table_uuid != contract.target.table_uuid {
        return Some(SchemaEvolutionError::TargetTableIdentityChanged {
            expected: contract.target.table_uuid.clone(),
            actual: target.table_uuid.clone(),
        });
    }
    if target.format_version != iceberg::spec::FormatVersion::V3 {
        return Some(SchemaEvolutionError::TargetRowLineageContractBroken {
            reason: format!(
                "target table must be Iceberg format v3, found {:?}",
                target.format_version
            ),
        });
    }
    if !target.row_lineage_enabled {
        return Some(SchemaEvolutionError::TargetRowLineageContractBroken {
            reason: "target table property write.row-lineage must be true".to_string(),
        });
    }
    None
}

fn check_base_referenced_fields(
    contract: &MvSchemaContract,
    base_schema: &iceberg::spec::Schema,
) -> Result<Vec<RebindColumn>, SchemaEvolutionError> {
    let current = base_schema.as_struct();
    let mut rebound = Vec::new();
    for record in &contract.base.schema_at_create.fields {
        let Some(field) = current.fields().iter().find(|f| f.id == record.field_id) else {
            return Err(SchemaEvolutionError::BaseFieldDropped {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
            });
        };
        let current_signature = format!("{}", field.field_type);
        if current_signature != record.type_signature {
            return Err(SchemaEvolutionError::BaseFieldTypeChanged {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from: record.type_signature.clone(),
                to: current_signature,
            });
        }
        if field.required != record.required {
            return Err(SchemaEvolutionError::BaseFieldNullabilityChanged {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from_required: record.required,
                to_required: field.required,
            });
        }
        if !field.name.eq_ignore_ascii_case(&record.name_at_create) {
            rebound.push(RebindColumn {
                base_table_fqn: contract.base.table_fqn.clone(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                current_name: field.name.clone(),
            });
        }
    }
    Ok(rebound)
}

fn check_target_partition_spec(
    contract: &MvSchemaContract,
    current_spec: &iceberg::spec::PartitionSpec,
) -> Option<SchemaEvolutionError> {
    let Some(expected) = &contract.target.partition else {
        return None;
    };
    if current_spec.spec_id() != expected.target_spec_id {
        return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
            reason: format!(
                "expected default spec id {}, got {}",
                expected.target_spec_id,
                current_spec.spec_id()
            ),
        });
    }
    let fields = current_spec.fields();
    if fields.len() != expected.fields.len() {
        return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
            reason: format!(
                "expected {} partition fields, got {}",
                expected.fields.len(),
                fields.len()
            ),
        });
    }
    for (idx, (actual, expected)) in fields.iter().zip(expected.fields.iter()).enumerate() {
        if actual.field_id != expected.partition_field_id {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field #{idx} id expected {}, got {}",
                    expected.partition_field_id, actual.field_id
                ),
            });
        }
        if actual.source_id != expected.source_target_field_id {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field {} source id expected {}, got {}",
                    expected.partition_field_name,
                    expected.source_target_field_id,
                    actual.source_id
                ),
            });
        }
        if actual.name != expected.partition_field_name {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field #{idx} name expected {}, got {}",
                    expected.partition_field_name, actual.name
                ),
            });
        }
        let Some(actual_transform) = partition_transform_contract(&actual.transform) else {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field {} has unsupported transform {:?}",
                    actual.name, actual.transform
                ),
            });
        };
        if actual_transform != expected.transform {
            return Some(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: format!(
                    "partition field {} transform expected {:?}, got {:?}",
                    expected.partition_field_name, expected.transform, actual_transform
                ),
            });
        }
    }
    None
}

fn partition_transform_contract(
    transform: &iceberg::spec::Transform,
) -> Option<MvPartitionTransformContract> {
    match transform {
        iceberg::spec::Transform::Identity => Some(MvPartitionTransformContract::Identity),
        iceberg::spec::Transform::Year => Some(MvPartitionTransformContract::Year),
        iceberg::spec::Transform::Month => Some(MvPartitionTransformContract::Month),
        iceberg::spec::Transform::Day => Some(MvPartitionTransformContract::Day),
        iceberg::spec::Transform::Hour => Some(MvPartitionTransformContract::Hour),
        iceberg::spec::Transform::Bucket(num_buckets) => {
            Some(MvPartitionTransformContract::Bucket {
                num_buckets: *num_buckets,
            })
        }
        iceberg::spec::Transform::Truncate(width) => {
            Some(MvPartitionTransformContract::Truncate { width: *width })
        }
        iceberg::spec::Transform::Void => Some(MvPartitionTransformContract::Void),
        iceberg::spec::Transform::Unknown => None,
    }
}

fn check_target_schema(
    contract: &MvSchemaContract,
    target_schema: &iceberg::spec::Schema,
) -> Option<SchemaEvolutionError> {
    let current = target_schema.as_struct();
    for tv in &contract.target.visible_columns {
        let Some(field) = current.fields().iter().find(|f| f.id == tv.target_field_id) else {
            return Some(SchemaEvolutionError::TargetVisibleFieldDropped {
                output_name: tv.output_name.clone(),
                target_field_id: tv.target_field_id,
            });
        };
        let sig = format!("{}", field.field_type);
        if sig != tv.type_signature {
            return Some(SchemaEvolutionError::TargetVisibleFieldTypeChanged {
                target_field_id: tv.target_field_id,
                from: tv.type_signature.clone(),
                to: sig,
            });
        }
        if !field.name.eq_ignore_ascii_case(&tv.output_name) {
            return Some(SchemaEvolutionError::TargetVisibleFieldRenamed {
                target_field_id: tv.target_field_id,
                expected: tv.output_name.clone(),
                actual: field.name.clone(),
            });
        }
    }

    let expected = &contract.target.hidden_apply_key;
    let Some(field) = current
        .fields()
        .iter()
        .find(|f| f.id == expected.target_field_id)
    else {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!(
                "hidden apply-key field id {} not found",
                expected.target_field_id
            ),
        });
    };
    let expected_hidden_apply_key_column = match expected.source {
        ApplyKeySource::BaseRowId => HIDDEN_APPLY_KEY_COLUMN_NAME,
        ApplyKeySource::JoinRowKey => JOIN_APPLY_KEY_COLUMN_NAME,
        ApplyKeySource::GroupRowId => GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
    };
    if !field
        .name
        .eq_ignore_ascii_case(expected_hidden_apply_key_column)
    {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!("hidden apply-key column renamed to {}", field.name),
        });
    }
    if let Some(err) = check_aggregate_state_schema(contract, current) {
        return Some(err);
    }
    if !field.required {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: "hidden apply-key column must be required".to_string(),
        });
    }
    let expected_apply_key_type = match expected.source {
        ApplyKeySource::BaseRowId => iceberg::spec::PrimitiveType::Long,
        ApplyKeySource::JoinRowKey | ApplyKeySource::GroupRowId => {
            iceberg::spec::PrimitiveType::String
        }
    };
    match field.field_type.as_ref() {
        iceberg::spec::Type::Primitive(actual) if actual == &expected_apply_key_type => {}
        other => {
            return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
                reason: format!(
                    "hidden apply-key column must be {expected_apply_key_type:?}, got {other}"
                ),
            });
        }
    }
    None
}

fn check_aggregate_state_schema(
    contract: &MvSchemaContract,
    current: &iceberg::spec::StructType,
) -> Option<SchemaEvolutionError> {
    let aggregate = contract.aggregate.as_ref()?;
    if aggregate.state_layout_version != 1 {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate state layout version {} is unsupported; expected 1",
                aggregate.state_layout_version
            ),
        });
    }
    if aggregate.state_columns.is_empty() {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: "aggregate state columns must not be empty".to_string(),
        });
    }
    if aggregate.row_id_column_name != GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column name expected {}, got {}",
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, aggregate.row_id_column_name
            ),
        });
    }
    let mut row_id_matches = current.fields().iter().filter(|field| {
        field
            .name
            .eq_ignore_ascii_case(&aggregate.row_id_column_name)
    });
    let Some(row_id_field) = row_id_matches.next() else {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column {} not found",
                aggregate.row_id_column_name
            ),
        });
    };
    if row_id_matches.next().is_some() {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column {} is duplicated",
                aggregate.row_id_column_name
            ),
        });
    }
    if row_id_field.id != contract.target.hidden_apply_key.target_field_id {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id field id {} must match hidden apply-key field id {}",
                row_id_field.id, contract.target.hidden_apply_key.target_field_id
            ),
        });
    }
    if !row_id_field.required {
        return Some(SchemaEvolutionError::AggregateStateContractBroken {
            reason: format!(
                "aggregate row-id column {} must be required",
                aggregate.row_id_column_name
            ),
        });
    }
    match row_id_field.field_type.as_ref() {
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String) => {}
        other => {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate row-id column {} must be String, got {other}",
                    aggregate.row_id_column_name
                ),
            });
        }
    }

    for state_col in &aggregate.state_columns {
        let Some(field) = current
            .fields()
            .iter()
            .find(|field| field.id == state_col.target_field_id)
        else {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} not found",
                    state_col.column_name, state_col.target_field_id
                ),
            });
        };
        if !field.name.eq_ignore_ascii_case(&state_col.column_name) {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} renamed to {}",
                    state_col.column_name, state_col.target_field_id, field.name
                ),
            });
        }
        let sig = format!("{}", field.field_type);
        if sig != state_col.type_signature {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} changed type from {} to {}",
                    state_col.column_name, state_col.target_field_id, state_col.type_signature, sig
                ),
            });
        }
        let actual_nullable = !field.required;
        if actual_nullable != state_col.nullable {
            return Some(SchemaEvolutionError::AggregateStateContractBroken {
                reason: format!(
                    "aggregate state column {} field id {} nullable changed from {} to {}",
                    state_col.column_name,
                    state_col.target_field_id,
                    state_col.nullable,
                    actual_nullable
                ),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::persistence::schema::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, HiddenApplyKeyContract,
        JOIN_APPLY_KEY_COLUMN_NAME, JoinContract, JoinContractKind, JoinPredicateLineage,
        MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
        OutputColumnLineage, OutputContract, QualifiedFieldLineage, TargetContract,
        TargetVisibleColumn,
    };
    use std::sync::Arc;

    fn test_schema(
        schema_id: i32,
        fields: Vec<iceberg::spec::NestedField>,
    ) -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_schema_id(schema_id)
            .with_fields(fields.into_iter().map(Arc::new))
            .build()
            .expect("test schema")
    }

    #[derive(Clone)]
    struct TestCurrentIcebergTable {
        table_uuid: String,
        format_version: iceberg::spec::FormatVersion,
        row_lineage_enabled: bool,
        schema: iceberg::spec::Schema,
        default_partition_spec: iceberg::spec::PartitionSpec,
    }

    impl TestCurrentIcebergTable {
        fn view(&self) -> CurrentIcebergTableView<'_> {
            CurrentIcebergTableView {
                table_uuid: self.table_uuid.clone(),
                format_version: self.format_version,
                row_lineage_enabled: self.row_lineage_enabled,
                schema: &self.schema,
                default_partition_spec: &self.default_partition_spec,
            }
        }
    }

    fn identity_table(
        table_uuid: &str,
        format_version: iceberg::spec::FormatVersion,
        row_lineage_enabled: bool,
    ) -> TestCurrentIcebergTable {
        use iceberg::spec::{PartitionSpec, PrimitiveType, Type};

        TestCurrentIcebergTable {
            table_uuid: table_uuid.to_string(),
            format_version,
            row_lineage_enabled,
            schema: test_schema(
                1,
                vec![
                    iceberg::spec::NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    ),
                    iceberg::spec::NestedField::required(
                        2,
                        HIDDEN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::Long),
                    ),
                ],
            ),
            default_partition_spec: PartitionSpec::unpartition_spec(),
        }
    }

    fn identity_contract(
        base: &TestCurrentIcebergTable,
        target: &TestCurrentIcebergTable,
    ) -> MvSchemaContract {
        let mut contract = minimal_base_row_id_contract();
        contract.base.table_uuid = base.table_uuid.clone();
        contract.target.table_uuid = target.table_uuid.clone();
        contract
    }

    #[test]
    fn schema_evolution_error_messages_are_action_oriented() {
        let err = SchemaEvolutionError::BaseFieldDropped {
            field_id: 5,
            name_at_create: "amount".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("field id 5"));
        assert!(msg.contains("amount"));
        assert!(msg.contains("REFRESH FULL"));
    }

    #[test]
    fn schema_evolution_error_messages_are_exact() {
        let cases = vec![
            (
                SchemaEvolutionError::BaseTableIdentityChanged {
                    expected: "base-a".to_string(),
                    actual: "base-b".to_string(),
                },
                "iceberg MV refresh blocked: base table identity changed (uuid expected=base-a, actual=base-b); run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::BaseRowLineageContractBroken {
                    reason: "base reason".to_string(),
                },
                "iceberg MV refresh blocked: base table row-lineage contract broken (base reason); run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::BaseFieldDropped {
                    field_id: 7,
                    name_at_create: "amount".to_string(),
                },
                "iceberg MV refresh blocked: base column \"amount\" (field id 7) was dropped from base table; run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::BaseFieldTypeChanged {
                    field_id: 7,
                    name_at_create: "amount".to_string(),
                    from: "int".to_string(),
                    to: "long".to_string(),
                },
                "iceberg MV refresh blocked: base column \"amount\" (field id 7) changed type from int to long; run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::BaseFieldNullabilityChanged {
                    field_id: 7,
                    name_at_create: "amount".to_string(),
                    from_required: true,
                    to_required: false,
                },
                "iceberg MV refresh blocked: base column \"amount\" (field id 7) changed nullability from required=true to required=false; run REFRESH FULL or recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetTableIdentityChanged {
                    expected: "target-a".to_string(),
                    actual: "target-b".to_string(),
                },
                "iceberg MV refresh blocked: target table identity changed (uuid expected=target-a, actual=target-b); recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetRowLineageContractBroken {
                    reason: "target reason".to_string(),
                },
                "iceberg MV refresh blocked: target table row-lineage contract broken (target reason); recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetVisibleFieldDropped {
                    output_name: "amount".to_string(),
                    target_field_id: 8,
                },
                "iceberg MV refresh blocked: target visible column \"amount\" (field id 8) was dropped; recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetVisibleFieldRenamed {
                    target_field_id: 8,
                    expected: "amount".to_string(),
                    actual: "renamed_amount".to_string(),
                },
                "iceberg MV refresh blocked: target visible column (field id 8) renamed externally: expected \"amount\", actual \"renamed_amount\"; recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetVisibleFieldTypeChanged {
                    target_field_id: 8,
                    from: "int".to_string(),
                    to: "long".to_string(),
                },
                "iceberg MV refresh blocked: target visible column (field id 8) changed type from int to long; recreate the MV",
            ),
            (
                SchemaEvolutionError::HiddenApplyKeyContractBroken {
                    reason: "hidden reason".to_string(),
                },
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden reason); recreate the MV",
            ),
            (
                SchemaEvolutionError::TargetPartitionSpecChanged {
                    reason: "partition reason".to_string(),
                },
                "iceberg MV refresh blocked: target partition spec changed externally (partition reason); recreate the MV",
            ),
            (
                SchemaEvolutionError::AggregateStateContractBroken {
                    reason: "aggregate reason".to_string(),
                },
                "iceberg MV refresh blocked: target aggregate state contract broken (aggregate reason); recreate the MV",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }

        let good_base = identity_table("base-uuid", iceberg::spec::FormatVersion::V3, true);
        let good_target = identity_table("target-uuid", iceberg::spec::FormatVersion::V3, true);
        let base_v2 = identity_table("base-uuid", iceberg::spec::FormatVersion::V2, true);
        let base_missing = identity_table("base-uuid", iceberg::spec::FormatVersion::V3, false);
        let base_false = identity_table("base-uuid", iceberg::spec::FormatVersion::V3, false);
        let target_v2 = identity_table("target-uuid", iceberg::spec::FormatVersion::V2, true);
        let target_missing = identity_table("target-uuid", iceberg::spec::FormatVersion::V3, false);
        let target_false = identity_table("target-uuid", iceberg::spec::FormatVersion::V3, false);
        let identity_cases = [
            (
                &base_v2,
                &good_target,
                "iceberg MV refresh blocked: base table row-lineage contract broken (base table must be Iceberg format v3, found V2); run REFRESH FULL or recreate the MV",
            ),
            (
                &base_missing,
                &good_target,
                "iceberg MV refresh blocked: base table row-lineage contract broken (base table property write.row-lineage must be true); run REFRESH FULL or recreate the MV",
            ),
            (
                &base_false,
                &good_target,
                "iceberg MV refresh blocked: base table row-lineage contract broken (base table property write.row-lineage must be true); run REFRESH FULL or recreate the MV",
            ),
            (
                &good_base,
                &target_v2,
                "iceberg MV refresh blocked: target table row-lineage contract broken (target table must be Iceberg format v3, found V2); recreate the MV",
            ),
            (
                &good_base,
                &target_missing,
                "iceberg MV refresh blocked: target table row-lineage contract broken (target table property write.row-lineage must be true); recreate the MV",
            ),
            (
                &good_base,
                &target_false,
                "iceberg MV refresh blocked: target table row-lineage contract broken (target table property write.row-lineage must be true); recreate the MV",
            ),
        ];
        for (base, target, expected) in identity_cases {
            let contract = identity_contract(base, target);
            let error = validate_identity_guards(&contract, &base.view(), &target.view())
                .expect("identity case must be incompatible");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn identity_validation_preserves_first_error_order() {
        let good_base = identity_table("base-uuid", iceberg::spec::FormatVersion::V3, true);
        let good_target = identity_table("target-uuid", iceberg::spec::FormatVersion::V3, true);
        let contract = identity_contract(&good_base, &good_target);

        let mut base = good_base.clone();
        let mut target = good_target.clone();
        base.table_uuid = "BASE-UUID".to_string();
        base.format_version = iceberg::spec::FormatVersion::V2;
        base.row_lineage_enabled = false;
        target.table_uuid = "other-target".to_string();
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::BaseTableIdentityChanged {
                expected: "base-uuid".to_string(),
                actual: "BASE-UUID".to_string(),
            })
        );

        base.table_uuid = contract.base.table_uuid.clone();
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::BaseRowLineageContractBroken {
                reason: "base table must be Iceberg format v3, found V2".to_string(),
            })
        );

        base.format_version = iceberg::spec::FormatVersion::V3;
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::BaseRowLineageContractBroken {
                reason: "base table property write.row-lineage must be true".to_string(),
            })
        );

        base.row_lineage_enabled = true;
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetTableIdentityChanged {
                expected: "target-uuid".to_string(),
                actual: "other-target".to_string(),
            })
        );

        target.table_uuid = contract.target.table_uuid.clone();
        target.format_version = iceberg::spec::FormatVersion::V2;
        target.row_lineage_enabled = false;
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetRowLineageContractBroken {
                reason: "target table must be Iceberg format v3, found V2".to_string(),
            })
        );

        target.format_version = iceberg::spec::FormatVersion::V3;
        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetRowLineageContractBroken {
                reason: "target table property write.row-lineage must be true".to_string(),
            })
        );

        target.row_lineage_enabled = true;
        target.schema = test_schema(12, Vec::new());
        let mut partition_contract = contract.clone();
        partition_contract.target.partition = Some(MvPartitionContract {
            target_spec_id: 1,
            fields: Vec::new(),
        });
        assert_eq!(
            validate_schema_contract(&partition_contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetPartitionSpecChanged {
                reason: "expected default spec id 1, got 0".to_string(),
            })
        );

        assert_eq!(
            validate_schema_contract(&contract, &base.view(), &target.view()),
            ContractDecision::Incompatible(SchemaEvolutionError::TargetVisibleFieldDropped {
                output_name: "id".to_string(),
                target_field_id: 1,
            })
        );
    }

    #[test]
    fn schema_evolution_error_target_messages_recommend_recreate() {
        let err = SchemaEvolutionError::TargetTableIdentityChanged {
            expected: "A".into(),
            actual: "B".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("recreate the MV"));
    }

    #[test]
    fn schema_evolution_error_implements_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(SchemaEvolutionError::BaseFieldDropped {
            field_id: 5,
            name_at_create: "amount".into(),
        });
        let _ = err; // just ensure it compiles
    }

    #[test]
    fn target_partition_spec_guard_detects_external_transform_change() {
        use iceberg::spec::{
            NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
        };

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        HIDDEN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("schema"),
        );
        let matching_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "id_bucket_16", Transform::Bucket(16))
            .expect("partition field")
            .build()
            .bind(Arc::clone(&schema))
            .expect("bind spec");
        let changed_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "id_bucket_8", Transform::Bucket(8))
            .expect("partition field")
            .build()
            .bind(schema)
            .expect("bind spec");
        let mut contract = minimal_base_row_id_contract();
        contract.target.partition = Some(MvPartitionContract {
            target_spec_id: 0,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 1000,
                partition_field_name: "id_bucket_16".to_string(),
                source_target_field_id: 1,
                source_column_name: "id".to_string(),
                transform: MvPartitionTransformContract::Bucket { num_buckets: 16 },
            }],
        });

        assert_eq!(check_target_partition_spec(&contract, &matching_spec), None);
        assert!(matches!(
            check_target_partition_spec(&contract, &changed_spec),
            Some(SchemaEvolutionError::TargetPartitionSpecChanged { .. })
        ));
    }

    #[test]
    fn partition_compatibility_preserves_strict_field_order() {
        use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Transform, Type};

        let schema = Arc::new(test_schema(
            1,
            vec![
                NestedField::required(1, "identity_src", Type::Primitive(PrimitiveType::Int)),
                NestedField::required(2, "year_src", Type::Primitive(PrimitiveType::Date)),
                NestedField::required(3, "month_src", Type::Primitive(PrimitiveType::Date)),
                NestedField::required(4, "day_src", Type::Primitive(PrimitiveType::Date)),
                NestedField::required(5, "hour_src", Type::Primitive(PrimitiveType::Timestamp)),
                NestedField::required(6, "bucket_src", Type::Primitive(PrimitiveType::Int)),
                NestedField::required(7, "truncate_src", Type::Primitive(PrimitiveType::String)),
                NestedField::required(8, "void_src", Type::Primitive(PrimitiveType::Int)),
                NestedField::required(9, "unknown_src", Type::Primitive(PrimitiveType::Int)),
            ],
        ));
        let supported_spec = PartitionSpec::builder(Arc::clone(&schema))
            .with_spec_id(7)
            .add_partition_field("identity_src", "p_identity", Transform::Identity)
            .expect("identity partition")
            .add_partition_field("year_src", "p_year", Transform::Year)
            .expect("year partition")
            .add_partition_field("month_src", "p_month", Transform::Month)
            .expect("month partition")
            .add_partition_field("day_src", "p_day", Transform::Day)
            .expect("day partition")
            .add_partition_field("hour_src", "p_hour", Transform::Hour)
            .expect("hour partition")
            .add_partition_field("bucket_src", "p_bucket", Transform::Bucket(16))
            .expect("bucket partition")
            .add_partition_field("truncate_src", "p_truncate", Transform::Truncate(4))
            .expect("truncate partition")
            .add_partition_field("void_src", "p_void", Transform::Void)
            .expect("void partition")
            .build()
            .expect("supported partition spec");
        let expected_transforms = [
            MvPartitionTransformContract::Identity,
            MvPartitionTransformContract::Year,
            MvPartitionTransformContract::Month,
            MvPartitionTransformContract::Day,
            MvPartitionTransformContract::Hour,
            MvPartitionTransformContract::Bucket { num_buckets: 16 },
            MvPartitionTransformContract::Truncate { width: 4 },
            MvPartitionTransformContract::Void,
        ];
        let expected_partition = MvPartitionContract {
            target_spec_id: supported_spec.spec_id(),
            fields: supported_spec
                .fields()
                .iter()
                .zip(expected_transforms)
                .map(|(field, transform)| MvPartitionFieldContract {
                    partition_field_id: field.field_id,
                    partition_field_name: field.name.clone(),
                    source_target_field_id: field.source_id,
                    source_column_name: format!("source_{}", field.source_id),
                    transform,
                })
                .collect(),
        };
        let mut contract = minimal_base_row_id_contract();
        contract.target.partition = Some(expected_partition.clone());
        assert_eq!(
            check_target_partition_spec(&contract, &supported_spec),
            None,
            "all supported transforms must preserve their exact contracts"
        );

        let unknown_spec = PartitionSpec::builder(schema)
            .with_spec_id(7)
            .add_partition_field("unknown_src", "p_unknown", Transform::Unknown)
            .expect("unknown partition")
            .build()
            .expect("unknown partition spec");
        let mut no_partition_contract = minimal_base_row_id_contract();
        no_partition_contract.target.partition = None;
        assert_eq!(
            check_target_partition_spec(&no_partition_contract, &unknown_spec),
            None,
            "live partition state must be ignored when no partition contract was persisted"
        );

        let exact_error = |partition: MvPartitionContract,
                           current: &iceberg::spec::PartitionSpec| {
            let mut current_contract = minimal_base_row_id_contract();
            current_contract.target.partition = Some(partition);
            check_target_partition_spec(&current_contract, current)
                .expect("partition mismatch")
                .to_string()
        };
        let wrap = |reason: &str| {
            format!(
                "iceberg MV refresh blocked: target partition spec changed externally ({reason}); recreate the MV"
            )
        };

        let mut spec_id_changed = expected_partition.clone();
        spec_id_changed.target_spec_id = 8;
        assert_eq!(
            exact_error(spec_id_changed, &supported_spec),
            wrap("expected default spec id 8, got 7")
        );

        let mut count_changed = expected_partition.clone();
        count_changed.fields.pop();
        assert_eq!(
            exact_error(count_changed, &supported_spec),
            wrap("expected 7 partition fields, got 8")
        );

        let mut id_changed = expected_partition.clone();
        id_changed.fields[0].partition_field_id += 100;
        assert_eq!(
            exact_error(id_changed, &supported_spec),
            wrap("partition field #0 id expected 1100, got 1000")
        );

        let mut source_changed = expected_partition.clone();
        source_changed.fields[0].source_target_field_id = 99;
        assert_eq!(
            exact_error(source_changed, &supported_spec),
            wrap("partition field p_identity source id expected 99, got 1")
        );

        let mut name_changed = expected_partition.clone();
        name_changed.fields[0].partition_field_name = "P_IDENTITY".to_string();
        assert_eq!(
            exact_error(name_changed, &supported_spec),
            wrap("partition field #0 name expected P_IDENTITY, got p_identity")
        );

        let mut reordered = expected_partition.clone();
        reordered.fields.swap(0, 1);
        assert_eq!(
            exact_error(reordered, &supported_spec),
            wrap("partition field #0 id expected 1001, got 1000")
        );

        let mut transform_changed = expected_partition;
        transform_changed.fields[0].transform = MvPartitionTransformContract::Year;
        assert_eq!(
            exact_error(transform_changed, &supported_spec),
            wrap("partition field p_identity transform expected Year, got Identity")
        );

        let unknown_field = &unknown_spec.fields()[0];
        let unknown_contract = MvPartitionContract {
            target_spec_id: unknown_spec.spec_id(),
            fields: vec![MvPartitionFieldContract {
                partition_field_id: unknown_field.field_id,
                partition_field_name: unknown_field.name.clone(),
                source_target_field_id: unknown_field.source_id,
                source_column_name: "unknown_src".to_string(),
                transform: MvPartitionTransformContract::Identity,
            }],
        };
        assert_eq!(
            exact_error(unknown_contract, &unknown_spec),
            wrap("partition field p_unknown has unsupported transform Unknown")
        );
    }

    #[test]
    fn supplied_base_schema_drives_base_rebind_decision() {
        let base_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "renamed_id",
                base_type.clone(),
            ))])
            .build()
            .expect("base schema");
        let target_schema = iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    target_type.clone(),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    HIDDEN_APPLY_KEY_COLUMN_NAME,
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("target schema");
        let contract = MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.db.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 1,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: format!("{base_type}"),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 11,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: format!("{target_type}"),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: None,
            },
        };

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(
            decision,
            ContractDecision::CompatibleSafeWithRebind {
                rebound_columns: vec![RebindColumn {
                    base_table_fqn: "ice.db.orders".to_string(),
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    current_name: "renamed_id".to_string(),
                }],
            }
        );
    }

    #[test]
    fn supplied_base_schema_rejects_referenced_nullability_drift() {
        let base_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::optional(
                1,
                "id",
                base_type.clone(),
            ))])
            .build()
            .expect("base schema");
        let target_schema = iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    target_type.clone(),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    HIDDEN_APPLY_KEY_COLUMN_NAME,
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("target schema");
        let contract = minimal_base_row_id_contract();

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(SchemaEvolutionError::BaseFieldNullabilityChanged {
                field_id,
                name_at_create,
                from_required,
                to_required,
            }) => {
                assert_eq!(field_id, 1);
                assert_eq!(name_at_create, "id");
                assert!(from_required);
                assert!(!to_required);
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn supplied_base_schema_rebind_payload_includes_base_fqn() {
        let base_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "renamed_id",
                base_type.clone(),
            ))])
            .build()
            .expect("base schema");
        let target_schema = iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    target_type.clone(),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    HIDDEN_APPLY_KEY_COLUMN_NAME,
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("target schema");
        let contract = minimal_base_row_id_contract();

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(
            decision,
            ContractDecision::CompatibleSafeWithRebind {
                rebound_columns: vec![RebindColumn {
                    base_table_fqn: "ice.db.orders".to_string(),
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    current_name: "renamed_id".to_string(),
                }],
            }
        );
    }

    #[test]
    fn base_field_compatibility_preserves_tolerance_and_rebind_order() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let int_type = Type::Primitive(PrimitiveType::Int);
        let long_type = Type::Primitive(PrimitiveType::Long);
        let mut contract = minimal_base_row_id_contract();
        contract.base.schema_at_create.fields.push(BaseFieldRecord {
            field_id: 2,
            name_at_create: "amount".to_string(),
            type_signature: int_type.to_string(),
            required: false,
        });

        let dropped = test_schema(
            2,
            vec![NestedField::optional(2, "amount", int_type.clone())],
        );
        assert_eq!(
            check_base_referenced_fields(&contract, &dropped)
                .expect_err("referenced field drop must fail")
                .to_string(),
            "iceberg MV refresh blocked: base column \"id\" (field id 1) was dropped from base table; run REFRESH FULL or recreate the MV"
        );

        let type_changed = test_schema(
            2,
            vec![
                NestedField::required(1, "id", long_type),
                NestedField::optional(2, "amount", int_type.clone()),
            ],
        );
        assert_eq!(
            check_base_referenced_fields(&contract, &type_changed)
                .expect_err("referenced field type change must fail")
                .to_string(),
            "iceberg MV refresh blocked: base column \"id\" (field id 1) changed type from int to long; run REFRESH FULL or recreate the MV"
        );

        let unrelated_reordered = test_schema(
            2,
            vec![
                NestedField::optional(99, "unrelated", Type::Primitive(PrimitiveType::String)),
                NestedField::optional(2, "amount", int_type.clone()),
                NestedField::required(1, "id", int_type.clone()),
            ],
        );
        assert_eq!(
            check_base_referenced_fields(&contract, &unrelated_reordered),
            Ok(Vec::new())
        );

        let case_only = test_schema(
            2,
            vec![
                NestedField::required(1, "ID", int_type.clone()),
                NestedField::optional(2, "AMOUNT", int_type.clone()),
            ],
        );
        assert_eq!(
            check_base_referenced_fields(&contract, &case_only),
            Ok(Vec::new())
        );

        let renamed_in_physical_reverse_order = test_schema(
            2,
            vec![
                NestedField::optional(2, "current_amount", int_type.clone()),
                NestedField::required(1, "current_id", int_type),
            ],
        );
        assert_eq!(
            check_base_referenced_fields(&contract, &renamed_in_physical_reverse_order),
            Ok(vec![
                RebindColumn {
                    base_table_fqn: "ice.db.orders".to_string(),
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    current_name: "current_id".to_string(),
                },
                RebindColumn {
                    base_table_fqn: "ice.db.orders".to_string(),
                    field_id: 2,
                    name_at_create: "amount".to_string(),
                    current_name: "current_amount".to_string(),
                },
            ])
        );
    }

    #[test]
    fn target_field_compatibility_preserves_nullable_tolerance_and_failures() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let int_type = Type::Primitive(PrimitiveType::Int);
        let long_type = Type::Primitive(PrimitiveType::Long);
        let contract = minimal_base_row_id_contract();
        let schema = |visible: Option<NestedField>, hidden: Option<NestedField>| {
            test_schema(12, visible.into_iter().chain(hidden).collect())
        };
        let hidden = || {
            NestedField::required(
                2,
                HIDDEN_APPLY_KEY_COLUMN_NAME,
                Type::Primitive(PrimitiveType::Long),
            )
        };

        let visible_nullable = schema(
            Some(NestedField::optional(1, "ID", int_type.clone())),
            Some(hidden()),
        );
        assert_eq!(check_target_schema(&contract, &visible_nullable), None);

        let cases = vec![
            (
                schema(None, Some(hidden())),
                "iceberg MV refresh blocked: target visible column \"id\" (field id 1) was dropped; recreate the MV",
            ),
            (
                schema(
                    Some(NestedField::required(1, "renamed_id", int_type.clone())),
                    Some(hidden()),
                ),
                "iceberg MV refresh blocked: target visible column (field id 1) renamed externally: expected \"id\", actual \"renamed_id\"; recreate the MV",
            ),
            (
                schema(
                    Some(NestedField::required(1, "id", long_type.clone())),
                    Some(hidden()),
                ),
                "iceberg MV refresh blocked: target visible column (field id 1) changed type from int to long; recreate the MV",
            ),
            (
                schema(Some(NestedField::required(1, "id", int_type.clone())), None),
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden apply-key field id 2 not found); recreate the MV",
            ),
            (
                schema(
                    Some(NestedField::required(1, "id", int_type.clone())),
                    Some(NestedField::required(
                        2,
                        "renamed_key",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ),
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden apply-key column renamed to renamed_key); recreate the MV",
            ),
            (
                schema(
                    Some(NestedField::required(1, "id", int_type.clone())),
                    Some(NestedField::optional(
                        2,
                        HIDDEN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ),
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden apply-key column must be required); recreate the MV",
            ),
            (
                schema(
                    Some(NestedField::required(1, "id", int_type)),
                    Some(NestedField::required(
                        2,
                        HIDDEN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::String),
                    )),
                ),
                "iceberg MV refresh blocked: target hidden apply-key column contract broken (hidden apply-key column must be Long, got string); recreate the MV",
            ),
        ];
        for (current_schema, expected) in cases {
            let error = check_target_schema(&contract, &current_schema)
                .expect("target compatibility case must fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn ordinary_schema_id_fast_path_preserves_target_tolerance() {
        let ordinary_contract = minimal_base_row_id_contract();
        let ordinary_base = test_schema(ordinary_contract.base.schema_id_at_create, Vec::new());
        let ordinary_target = test_schema(ordinary_contract.target.schema_id_at_create, Vec::new());
        assert_eq!(
            validate_schema_contract_after_identity(
                &ordinary_contract,
                &ordinary_base,
                &ordinary_target,
            ),
            ContractDecision::CompatibleSafe
        );

        let aggregate_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let mut aggregate_contract = aggregate_schema_contract(aggregate_type.to_string());
        aggregate_contract.target.schema_id_at_create = 11;
        let aggregate_base = test_schema(aggregate_contract.base.schema_id_at_create, Vec::new());
        let aggregate_target =
            aggregate_target_schema("__agg_state_c", iceberg::spec::PrimitiveType::String, false);
        assert_eq!(
            validate_schema_contract_after_identity(
                &aggregate_contract,
                &aggregate_base,
                &aggregate_target,
            ),
            ContractDecision::Incompatible(SchemaEvolutionError::AggregateStateContractBroken {
                reason: "aggregate state column __agg_state_c field id 3 changed type from long to string"
                    .to_string(),
            })
        );
    }

    fn minimal_base_row_id_contract() -> MvSchemaContract {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.db.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 1,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: format!("{target_type}"),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 11,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: format!("{target_type}"),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: None,
            },
        }
    }

    #[test]
    fn join_row_key_target_hidden_column_is_accepted() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema = iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    target_type.clone(),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    JOIN_APPLY_KEY_COLUMN_NAME,
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                )),
            ])
            .build()
            .expect("target schema");
        let contract = MvSchemaContract {
            contract_version: 2,
            base: BaseContract {
                table_fqn: "ice.db.left".to_string(),
                table_uuid: "left-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot { fields: vec![] },
            },
            bases: vec![
                BaseContract {
                    table_fqn: "ice.db.left".to_string(),
                    table_uuid: "left-uuid".to_string(),
                    alias_at_create: Some("l".to_string()),
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![BaseFieldRecord {
                            field_id: 1,
                            name_at_create: "id".to_string(),
                            type_signature: format!("{target_type}"),
                            required: true,
                        }],
                    },
                },
                BaseContract {
                    table_fqn: "ice.db.right".to_string(),
                    table_uuid: "right-uuid".to_string(),
                    alias_at_create: Some("r".to_string()),
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot {
                        fields: vec![BaseFieldRecord {
                            field_id: 2,
                            name_at_create: "id".to_string(),
                            type_signature: format!("{target_type}"),
                            required: true,
                        }],
                    },
                },
            ],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![],
                        referenced_base_fields: vec![QualifiedFieldLineage {
                            table_fqn: "ice.db.left".to_string(),
                            qualifier_at_create: "l".to_string(),
                            field_id: 1,
                        }],
                    },
                }],
                filter: None,
            },
            join: Some(JoinContract {
                kind: JoinContractKind::InnerEquiJoin,
                predicates: vec![JoinPredicateLineage {
                    left: QualifiedFieldLineage {
                        table_fqn: "ice.db.left".to_string(),
                        qualifier_at_create: "l".to_string(),
                        field_id: 1,
                    },
                    right: QualifiedFieldLineage {
                        table_fqn: "ice.db.right".to_string(),
                        qualifier_at_create: "r".to_string(),
                        field_id: 2,
                    },
                }],
            }),
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_join".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: format!("{target_type}"),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::JoinRowKey,
                },
                partition: None,
            },
        };

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(decision, ContractDecision::CompatibleSafe);
    }

    #[test]
    fn aggregate_state_target_layout_is_accepted() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema =
            aggregate_target_schema("__agg_state_c", iceberg::spec::PrimitiveType::Long, false);
        let contract = aggregate_schema_contract(format!("{target_type}"));

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        assert_eq!(decision, ContractDecision::CompatibleSafe);
    }

    #[test]
    fn aggregate_state_target_layout_rejects_renamed_state_column() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema =
            aggregate_target_schema("renamed_state", iceberg::spec::PrimitiveType::Long, false);
        let contract = aggregate_schema_contract(format!("{target_type}"));

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("__agg_state_c"), "reason={reason}");
                assert!(reason.contains("renamed"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn aggregate_state_target_layout_rejects_type_changed_state_column() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema =
            aggregate_target_schema("__agg_state_c", iceberg::spec::PrimitiveType::String, false);
        let contract = aggregate_schema_contract(format!("{target_type}"));

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("__agg_state_c"), "reason={reason}");
                assert!(reason.contains("changed type"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn aggregate_state_validation_runs_on_schema_id_fast_path() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema =
            aggregate_target_schema("__agg_state_c", iceberg::spec::PrimitiveType::String, false);
        let mut contract = aggregate_schema_contract(format!("{target_type}"));
        contract.target.schema_id_at_create = 11;

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("__agg_state_c"), "reason={reason}");
                assert!(reason.contains("changed type"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn aggregate_state_target_layout_rejects_nullable_row_id_column() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema = aggregate_target_schema_with_row_id(
            "__agg_state_c",
            iceberg::spec::PrimitiveType::Long,
            false,
            2,
            true,
        );
        let contract = aggregate_schema_contract(format!("{target_type}"));

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("row-id"), "reason={reason}");
                assert!(reason.contains("required"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn aggregate_state_target_layout_rejects_row_id_that_is_not_hidden_apply_key() {
        let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
        let base_schema = iceberg::spec::Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![])
            .build()
            .expect("base schema");
        let target_schema = aggregate_target_schema_with_extra_string_column(
            "__agg_state_c",
            iceberg::spec::PrimitiveType::Long,
            "other_key",
            false,
        );
        let mut contract = aggregate_schema_contract(format!("{target_type}"));
        contract
            .aggregate
            .as_mut()
            .expect("aggregate")
            .row_id_column_name = "other_key".to_string();

        let decision =
            validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

        match decision {
            ContractDecision::Incompatible(
                SchemaEvolutionError::AggregateStateContractBroken { reason },
            ) => {
                assert!(reason.contains("row-id"), "reason={reason}");
                assert!(reason.contains("__row_id__"), "reason={reason}");
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    fn aggregate_target_schema_with_extra_string_column(
        state_column_name: &str,
        state_column_type: iceberg::spec::PrimitiveType,
        extra_column_name: &str,
        extra_nullable: bool,
    ) -> iceberg::spec::Schema {
        let extra_field = if extra_nullable {
            iceberg::spec::NestedField::optional(
                4,
                extra_column_name,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
        } else {
            iceberg::spec::NestedField::required(
                4,
                extra_column_name,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
        };
        let mut fields = aggregate_target_schema(state_column_name, state_column_type, false)
            .as_struct()
            .fields()
            .to_vec();
        fields.push(Arc::new(extra_field));
        iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(fields)
            .build()
            .expect("target schema")
    }

    fn aggregate_target_schema(
        state_column_name: &str,
        state_column_type: iceberg::spec::PrimitiveType,
        state_column_nullable: bool,
    ) -> iceberg::spec::Schema {
        aggregate_target_schema_with_row_id(
            state_column_name,
            state_column_type,
            state_column_nullable,
            2,
            false,
        )
    }

    fn aggregate_target_schema_with_row_id(
        state_column_name: &str,
        state_column_type: iceberg::spec::PrimitiveType,
        state_column_nullable: bool,
        row_id_field_id: i32,
        row_id_nullable: bool,
    ) -> iceberg::spec::Schema {
        let state_type = iceberg::spec::Type::Primitive(state_column_type);
        let state_field = if state_column_nullable {
            iceberg::spec::NestedField::optional(3, state_column_name, state_type)
        } else {
            iceberg::spec::NestedField::required(3, state_column_name, state_type)
        };
        let row_id_field = if row_id_nullable {
            iceberg::spec::NestedField::optional(
                row_id_field_id,
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
        } else {
            iceberg::spec::NestedField::required(
                row_id_field_id,
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
        };
        iceberg::spec::Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
                Arc::new(row_id_field),
                Arc::new(state_field),
            ])
            .build()
            .expect("target schema")
    }

    fn aggregate_schema_contract(state_type_signature: String) -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 3,
            base: BaseContract {
                table_fqn: "ice.db.orders".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot { fields: vec![] },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: Some(AggregateStateContract {
                state_layout_version: 1,
                row_id_column_name: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
                state_columns: vec![AggregateStateColumnContract {
                    column_name: "__agg_state_c".to_string(),
                    target_field_id: 3,
                    type_signature: state_type_signature,
                    nullable: false,
                    role: AggregateStateRoleContract::Single,
                }],
            }),
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv_agg".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::GroupRowId,
                },
                partition: None,
            },
        }
    }
}
