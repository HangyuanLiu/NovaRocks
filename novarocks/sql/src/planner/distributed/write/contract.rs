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

//! SQL-owned facts for a distributed table-write sink.
//!
//! A compiler may reason about a write target's identity, schema, partition
//! layout, and row-lineage shape.  It must not retain a provider table object,
//! serialized metadata, object-store properties, prepared writer, or connector
//! handle.  The application binding store owns those authorities and converts
//! this contract into a connector-specific writer only after placement is
//! frozen.

use std::collections::BTreeSet;

use arrow::datatypes::DataType;
use novarocks_catalog::schema::ColumnDef;
use novarocks_spi::connector::ConnectorWriteFieldToken;

use crate::analysis::TypedExpr;
use crate::binding::SqlTableBindingId;
use crate::planner::table::SqlTableIdentity;

/// Generic Arrow input selection for a connector batch writer. This is a SQL
/// physical-planning fact: it identifies output columns, never provider
/// metadata or a connector handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectorWriteInputBinding {
    RootOutputByOrdinal,
    OutputOrdinals(Vec<usize>),
}

/// SQL-owned terminal writer input. Application code converts the completed
/// contract into a connector-specific writer after it looks up the exact
/// request-local binding token.
#[derive(Clone, Debug)]
pub(crate) struct SqlWritePlanInput {
    pub(crate) contract: SqlWriteSinkContract,
    pub(crate) input: ConnectorWriteInputBinding,
    /// A root-only projection supplied by SQL when hidden or state columns
    /// must be materialized immediately before the terminal sink.
    pub(crate) root_output_exprs: Option<Vec<TypedExpr>>,
}

/// Logical operation performed by the SQL terminal write sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

/// A Provider-signed SQL-visible target field.  The token binds this Arrow
/// field to one sealed write preparation without exposing a table-format field
/// ID to SQL planning.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlWriteTargetField {
    pub(crate) token: ConnectorWriteFieldToken,
    pub(crate) column: ColumnDef,
    pub(crate) is_hidden: bool,
}

/// The compiler-visible write target. The binding token is the sole route back
/// to application-owned provider authority; it is intentionally not
/// serializable and cannot be reused by another request/store.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlWriteSinkTargetContract {
    pub(crate) binding: SqlTableBindingId,
    pub(crate) table: SqlTableIdentity,
    pub(crate) fields: Vec<SqlWriteTargetField>,
}

impl SqlWriteSinkTargetContract {
    pub(crate) fn try_new(
        binding: SqlTableBindingId,
        table: SqlTableIdentity,
        fields: Vec<SqlWriteTargetField>,
    ) -> Result<Self, String> {
        if table.catalog.is_empty() || table.namespace.is_empty() || table.table.is_empty() {
            return Err("SQL write target requires a canonical table identity".to_string());
        }
        if fields.is_empty() {
            return Err("SQL write target requires at least one target field".to_string());
        }

        let mut field_tokens = BTreeSet::new();
        let mut names = BTreeSet::new();
        for field in &fields {
            if !field_tokens.insert(field.token) {
                return Err("SQL write target contains duplicate provider field token".to_string());
            }
            if !names.insert(field.column.name.clone()) {
                return Err(format!(
                    "SQL write target contains duplicate field name {}",
                    field.column.name
                ));
            }
        }

        Ok(Self {
            binding,
            table,
            fields,
        })
    }
}

/// Complete compiler-owned terminal write contract. It intentionally has no
/// storage location, cloud property, serialized provider metadata, writer
/// handle, or prepared operation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlWriteSinkContract {
    pub(crate) mode: SqlWriteSinkMode,
    pub(crate) target: SqlWriteSinkTargetContract,
    pub(crate) input_columns: Vec<ColumnDef>,
}

impl SqlWriteSinkContract {
    pub(crate) fn try_new(
        mode: SqlWriteSinkMode,
        target: SqlWriteSinkTargetContract,
        input_columns: Vec<ColumnDef>,
    ) -> Result<Self, String> {
        if input_columns.is_empty() {
            return Err("SQL write sink requires at least one input column".to_string());
        }
        Ok(Self {
            mode,
            target,
            input_columns,
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod test_support {
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::binding::SqlTableBindingScopeId;

    pub(crate) fn simple_sql_write_plan_input(
        input: ConnectorWriteInputBinding,
    ) -> SqlWritePlanInput {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(92).expect("non-zero scope")),
            NonZeroU32::new(1).expect("non-zero ordinal"),
        );
        let column = ColumnDef {
            name: "order_id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        let target = SqlWriteSinkTargetContract::try_new(
            binding,
            SqlTableIdentity {
                catalog: "iceberg".to_string(),
                namespace: "analytics".to_string(),
                table: "orders".to_string(),
            },
            vec![SqlWriteTargetField {
                token: ConnectorWriteFieldToken::from_bytes([1; 32]),
                column: column.clone(),
                is_hidden: false,
            }],
        )
        .expect("valid SQL target");
        SqlWritePlanInput {
            contract: SqlWriteSinkContract::try_new(SqlWriteSinkMode::Data, target, vec![column])
                .expect("valid SQL write contract"),
            input,
            root_output_exprs: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::binding::SqlTableBindingScopeId;

    fn binding() -> SqlTableBindingId {
        SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(91).expect("non-zero scope")),
            NonZeroU32::new(1).expect("non-zero ordinal"),
        )
    }

    fn column(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn target() -> SqlWriteSinkTargetContract {
        SqlWriteSinkTargetContract::try_new(
            binding(),
            SqlTableIdentity {
                catalog: "iceberg".to_string(),
                namespace: "analytics".to_string(),
                table: "orders".to_string(),
            },
            vec![SqlWriteTargetField {
                token: ConnectorWriteFieldToken::from_bytes([1; 32]),
                column: column("order_id", DataType::Int64),
                is_hidden: false,
            }],
        )
        .expect("valid SQL target")
    }

    #[test]
    fn sqlx2_write_sink_contract_keeps_only_binding_and_sql_facts() {
        let target = target();
        let contract = SqlWriteSinkContract::try_new(
            SqlWriteSinkMode::Data,
            target.clone(),
            vec![column("order_id", DataType::Int64)],
        )
        .expect("valid write contract");

        assert_eq!(contract.target.binding, binding());
        assert_eq!(contract.target.table.table, "orders");
        assert_eq!(
            contract.target.fields[0].token,
            ConnectorWriteFieldToken::from_bytes([1; 32])
        );
    }

    #[test]
    fn sqlx2_write_sink_contract_rejects_duplicate_provider_token() {
        let error = SqlWriteSinkTargetContract::try_new(
            binding(),
            SqlTableIdentity {
                catalog: "iceberg".to_string(),
                namespace: "analytics".to_string(),
                table: "orders".to_string(),
            },
            vec![
                SqlWriteTargetField {
                    token: ConnectorWriteFieldToken::from_bytes([9; 32]),
                    column: column("order_id", DataType::Int64),
                    is_hidden: false,
                },
                SqlWriteTargetField {
                    token: ConnectorWriteFieldToken::from_bytes([9; 32]),
                    column: column("customer_id", DataType::Int64),
                    is_hidden: false,
                },
            ],
        )
        .expect_err("duplicate provider token must fail");

        assert!(error.contains("duplicate provider field token"));
    }
}
