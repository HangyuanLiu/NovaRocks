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

//! Application projection for SQL terminal write contracts.
//!
//! The compiler receives the resulting [`DmlWritePlanInput`] only. Provider
//! metadata never crosses this boundary: the request-local binding retains a
//! sealed write preparation and SQL sees only its Arrow layout and field
//! tokens.

use arrow::datatypes::{Schema, SchemaRef};
use novarocks_types::schema::ColumnDef;
use std::sync::Arc;

use crate::catalog_application::query_bindings::{
    QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
    QueryWriteTargetAdmission,
};
use novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan;
use novarocks_spi::connector::{ConnectorControlPlanningLease, ConnectorWriteInputShape};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::dml::{
    DmlWritePlanInput, DmlWriteSinkMode, DmlWriteTarget, DmlWriteTargetField,
};
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, frozen_connector_write_target_resolved_analyzer_table,
};

/// Project an admitted write target into the opaque DML planning boundary.
///
/// This is intentionally separate from the legacy internal compiler helper:
/// new application entry points must not receive the private planner write
/// contract.  The request-local binding keeps the same exact planning lease
/// and provider preparation through terminal planning and fragment setup.
pub(crate) fn dml_write_plan_input_for_admitted_target(
    bindings: &QueryTableBindingStore,
    binding: SqlTableBindingId,
    mode: DmlWriteSinkMode,
    input: novarocks_sql::planning::dml::ConnectorWriteInputBinding,
) -> Result<DmlWritePlanInput, String> {
    let captured = bindings.binding(binding)?;
    captured.admission.exact_planning_lease().map_err(|_| {
        "SQL write target binding is missing its admission planning lease".to_string()
    })?;
    let admission = admitted_write_target(&captured)?;
    let write_input = &admission.input;
    validate_mode(mode, write_input)?;
    let identity =
        novarocks_sql::planning::catalog::materialization_identity_facts(&captured.resolved);
    DmlWritePlanInput::try_new(
        mode,
        DmlWriteTarget {
            binding,
            catalog: identity.catalog().to_string(),
            namespace: identity.namespace().to_string(),
            table: identity.table().to_string(),
            fields: write_input
                .fields()
                .into_iter()
                .map(|field| {
                    Ok(DmlWriteTargetField {
                        token: field.token(),
                        column: admitted_write_column(field.field())?,
                        is_hidden: false,
                    })
                })
                .collect::<Result<_, String>>()?,
        },
        admitted_write_input_columns(write_input)?,
        input,
    )
}

/// Reserve a SQL write token for one logical target of the query's write
/// session.
///
/// The session already admitted this target when it began: the provider signed
/// the whole target set at once and handed back an opaque writer recipe plus the
/// input shape SQL is allowed to project. So there is no per-target preparation
/// to sign here, and none is invented -- the binding carries the sealed input
/// shape and nothing else the provider owns. The exact planning lease is the
/// same one the session was begun on, which is what keeps the generation that
/// signed the recipe alive through planning.
pub(crate) fn admit_session_connector_write_target(
    bindings: &QueryTableBindingStore,
    identity: FrozenConnectorScanIdentity,
    target: &ConnectorWriteTargetPlan,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<SqlTableBindingId, String> {
    let key = QueryTableBindingKey::session_write_target(
        identity.catalog(),
        identity.namespace(),
        identity.table(),
        target.ordinal(),
    );
    let input = target.input().clone();
    bindings.resolve_or_insert_with_id(key, |binding| {
        Ok(QueryTableBinding {
            resolved: frozen_connector_write_target_resolved_analyzer_table(
                &identity,
                write_input_schema(&input),
                binding,
            ),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Exact(planning_lease),
            source_metadata: None,
            // A terminal write target, not a read source: see the note on the
            // prepared path below.
            scan_materialization: None,
            mv_target_read: None,
            write_target_admission: Some(QueryWriteTargetAdmission {
                input: input.clone(),
            }),
            frozen_cohort_read: None,
            frozen_snapshot_materializations: std::collections::BTreeMap::new(),
            admitted_change_scans: std::collections::BTreeMap::new(),
        })
    })
}

fn admitted_write_target(
    binding: &QueryTableBinding,
) -> Result<&crate::catalog_application::query_bindings::QueryWriteTargetAdmission, String> {
    binding
        .write_target_admission
        .as_ref()
        .ok_or_else(|| "SQL write target binding is missing admitted write facts".to_string())
}

fn admitted_write_input_columns(
    input: &ConnectorWriteInputShape,
) -> Result<Vec<ColumnDef>, String> {
    input
        .fields()
        .into_iter()
        .map(|field| admitted_write_column(field.field()))
        .collect()
}

/// Freeze one signed write field as the SQL column the planner writes.
///
/// The field's own value type authors the column, so a root logical identity
/// the provider signed (LARGEINT, HLL, BITMAP, UUID, VARIANT, ...) is declared
/// on the column instead of collapsing onto its Arrow carrier -- the writer
/// input is what the provider's statistics pins are checked against. A plain
/// carrier field yields the same undeclared column it always did.
fn admitted_write_column(field: &arrow::datatypes::Field) -> Result<ColumnDef, String> {
    let value_type = novarocks_type_contract::FunctionValueType::try_from_field(field)
        .map_err(|error| format!("admitted write field `{}`: {error}", field.name()))?;
    ColumnDef::from_value_type(field.name().to_string(), value_type, None)
        .map_err(|error| format!("admitted write field `{}`: {error}", field.name()))
}

fn write_input_schema(input: &ConnectorWriteInputShape) -> SchemaRef {
    Arc::new(Schema::new(
        input
            .fields()
            .into_iter()
            .map(|field| field.field().clone())
            .collect::<Vec<_>>(),
    ))
}

fn validate_mode(mode: DmlWriteSinkMode, input: &ConnectorWriteInputShape) -> Result<(), String> {
    let matches = matches!(
        (mode, input),
        (
            DmlWriteSinkMode::Data,
            ConnectorWriteInputShape::Data { .. }
        ) | (
            DmlWriteSinkMode::RowLineageData,
            ConnectorWriteInputShape::RowLineage { .. }
        ) | (
            DmlWriteSinkMode::PositionDeletes,
            ConnectorWriteInputShape::PositionDelete { .. }
        ) | (
            DmlWriteSinkMode::DeletionVectors,
            ConnectorWriteInputShape::DeletionVector { .. }
        ) | (
            DmlWriteSinkMode::EqualityDeletes,
            ConnectorWriteInputShape::EqualityDelete { .. }
        )
    );
    matches.then_some(()).ok_or_else(|| {
        "SQL write sink mode does not match its Provider-signed input shape".to_string()
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};
    use novarocks_spi::connector::{
        ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
    };
    use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType};
    use novarocks_types::schema::{ColumnDef, SqlType};

    use super::admitted_write_input_columns;

    fn signed(ordinal: u8, field: Field) -> ConnectorWriteFieldBinding {
        let field_id = (u32::from(ordinal) + 1).to_string();
        let mut metadata = field.metadata().clone();
        metadata.insert("PARQUET:field_id".into(), field_id);
        ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([ordinal + 1; 32]),
            field.with_metadata(metadata),
        )
    }

    fn annotated(name: &str, data_type: DataType, logical: &str) -> Field {
        Field::new(name, data_type, true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), logical.into())].into())
    }

    /// The planner's writer input columns carry each signed field's root
    /// logical identity, while a plain carrier field projects to exactly the
    /// undeclared column it always did.
    #[test]
    fn admitted_write_columns_declare_signed_root_logical_identities() {
        let input = ConnectorWriteInputShape::Data {
            fields: vec![
                signed(0, Field::new("id", DataType::Int64, false)),
                signed(
                    1,
                    annotated("big", DataType::FixedSizeBinary(16), "largeint"),
                ),
                signed(2, annotated("h", DataType::Binary, "hll")),
                signed(3, Field::new("name", DataType::Utf8, true)),
            ],
        };
        let columns = admitted_write_input_columns(&input).expect("admitted columns");
        assert_eq!(
            columns[0],
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }
        );
        assert_eq!(
            columns[3],
            ColumnDef {
                name: "name".into(),
                data_type: DataType::Utf8,
                nullable: true,
                write_default: None,
                logical_type: None,
            }
        );
        for (ordinal, declaration, logical) in [
            (1, SqlType::LargeInt, ValueLogicalType::LargeInt),
            (2, SqlType::Hll, ValueLogicalType::Hll),
        ] {
            let column = &columns[ordinal];
            assert_eq!(column.logical_type.as_ref(), Some(&declaration));
            assert_eq!(
                &column.data_type,
                input.fields()[ordinal].field().data_type()
            );
            let declared = column.declared_value_type().expect("declared value type");
            assert_eq!(declared.logical_type, logical);
            assert_eq!(
                declared,
                novarocks_type_contract::FunctionValueType::try_from_field(
                    input.fields()[ordinal].field()
                )
                .expect("signed value type")
            );
        }
    }

    /// A signed field whose annotation its carrier cannot hold is refused
    /// rather than projected as a plain carrier column.
    #[test]
    fn admitted_write_columns_refuse_a_contradictory_annotation() {
        let input = ConnectorWriteInputShape::Data {
            fields: vec![signed(0, annotated("big", DataType::Int64, "largeint"))],
        };
        let error = admitted_write_input_columns(&input).expect_err("contradiction is refused");
        assert!(error.contains("admitted write field `big`"), "{error}");
    }
}
