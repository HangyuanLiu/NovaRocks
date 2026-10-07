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

//! SQL-owned physical layout for aggregate materialized-view state.
//!
//! This module turns SQL's one-shot analyzed aggregate facts into the target
//! table's physical columns and the execution-only runtime layout.  It keeps
//! semantic table columns and aggregate-function vocabulary in SQL;
//! the embedded [`MvAggregateRuntimeLayout`] contains only Arrow/runtime facts.

use std::collections::HashSet;

use arrow::datatypes::DataType;
use novarocks_types::logical_type::LogicalType;
use novarocks_types::mv_aggregate_layout::{
    MvAggregateRuntimeKind, MvAggregateRuntimeLayout, MvAggregateStateColumn, MvAggregateStateRole,
    MvAggregateVisibleColumn,
};
use novarocks_types::naming::normalize_identifier;

use crate::mv_refresh::AggregateFunctionKind;
use crate::planning::mv::SqlMvAggregateLayoutFacts;

pub const MV_AGGREGATE_ROW_ID_COLUMN: &str = "__row_id__";
pub const MV_AGGREGATE_STATE_PREFIX: &str = "__agg_state_";
pub const MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN: &str = "__agg_state___ivm_row_count";

/// Complete logical declaration for one aggregate target column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvAggregateColumnFacts {
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
}

/// One target-table column emitted by the aggregate MV physical-layout builder.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlMvAggregatePhysicalColumn {
    column: SqlMvAggregateColumnFacts,
    visible: bool,
    is_key: bool,
}

impl SqlMvAggregatePhysicalColumn {
    pub fn new(
        name: String,
        logical_type: LogicalType,
        nullable: bool,
        visible: bool,
        is_key: bool,
    ) -> Self {
        Self {
            column: SqlMvAggregateColumnFacts {
                name,
                data_type: logical_type,
                nullable,
            },
            visible,
            is_key,
        }
    }

    pub fn logical_type(&self) -> &LogicalType {
        &self.column.data_type
    }
    pub fn column(&self) -> &SqlMvAggregateColumnFacts {
        &self.column
    }
    pub fn visible(&self) -> bool {
        self.visible
    }
    pub fn is_key(&self) -> bool {
        self.is_key
    }
}

/// SQL-owned target DDL together with its execution-only aggregate layout.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlMvAggregatePhysicalLayout {
    row_id_column: SqlMvAggregatePhysicalColumn,
    physical_columns: Vec<SqlMvAggregatePhysicalColumn>,
    runtime_layout: MvAggregateRuntimeLayout,
}

impl SqlMvAggregatePhysicalLayout {
    pub fn row_id_column(&self) -> &SqlMvAggregatePhysicalColumn {
        &self.row_id_column
    }

    pub fn physical_columns(&self) -> &[SqlMvAggregatePhysicalColumn] {
        &self.physical_columns
    }

    pub fn runtime_layout(&self) -> &MvAggregateRuntimeLayout {
        &self.runtime_layout
    }
}

/// Build the aggregate MV target layout in one SQL-owned transaction.
///
/// Validation checks input counts, group-key indexes, each aggregate's visible
/// type, and state-column contracts. Visible declarations retain complete
/// logical facts without a weaker SQL syntax projection.  The hidden retraction-count state is appended
/// only after every explicit aggregate state has been accepted.
pub fn build_sql_mv_aggregate_physical_layout(
    facts: &SqlMvAggregateLayoutFacts,
) -> Result<SqlMvAggregatePhysicalLayout, String> {
    let calls = facts.calls();
    let output_columns = facts.output_columns();
    let aggregate_input_types = facts.aggregate_input_types();
    let group_key_source_indexes = facts.group_key_source_indexes();

    if aggregate_input_types.len() != calls.len() {
        return Err(format!(
            "aggregate MV input type metadata count mismatch: inputs={} aggregates={}",
            aggregate_input_types.len(),
            calls.len()
        ));
    }

    let row_id_column = physical_column(
        MV_AGGREGATE_ROW_ID_COLUMN.to_string(),
        LogicalType::Utf8,
        false,
        false,
        true,
    );
    let mut physical_columns = vec![row_id_column.clone()];
    for (group_key_index, source_index) in group_key_source_indexes.iter().enumerate() {
        if *source_index >= output_columns.len() {
            return Err(format!(
                "aggregate MV group key visible source index out of range: group_key_index={group_key_index} source_index={source_index} outputs={}",
                output_columns.len()
            ));
        }
    }

    let visible_columns = output_columns
        .iter()
        .enumerate()
        .map(|(source_index, column)| {
            physical_columns.push(SqlMvAggregatePhysicalColumn::new(
                column.name.clone(),
                column.logical_type().clone(),
                column.nullable,
                true,
                false,
            ));
            Ok(MvAggregateVisibleColumn::new(
                column.name.clone(),
                column.data_type.clone(),
                column.nullable,
                source_index,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let avg_count = calls
        .iter()
        .filter(|call| call.function() == AggregateFunctionKind::Avg)
        .count();
    let mut state_columns = Vec::with_capacity(calls.len() + avg_count + 1);
    for (aggregate_index, call) in calls.iter().enumerate() {
        let visible_source_index = call.visible_source_index();
        let visible = output_columns.get(visible_source_index).ok_or_else(|| {
            format!(
                "aggregate MV visible source index out of range: aggregate_index={aggregate_index} source_index={visible_source_index}"
            )
        })?;
        validate_aggregate_state_visible_type(
            call.function(),
            &visible.data_type,
            aggregate_input_types
                .get(aggregate_index)
                .and_then(Option::as_ref),
            call.output_name(),
        )?;

        let state_name = format!(
            "{MV_AGGREGATE_STATE_PREFIX}{}",
            sanitize_state_column_name(call.output_name())
        );
        let state_data_type = DataType::LargeBinary;
        if call.function() == AggregateFunctionKind::Avg {
            for (suffix, role) in [
                ("avg_sum", MvAggregateStateRole::AvgSum),
                ("avg_count", MvAggregateStateRole::AvgCount),
            ] {
                let state_name = format!("{state_name}_{suffix}");
                validate_state_column_type(call.function(), role, &state_data_type, &state_name)?;
                physical_columns.push(physical_column(
                    state_name.clone(),
                    LogicalType::Binary,
                    false,
                    false,
                    false,
                ));
                state_columns.push(MvAggregateStateColumn::new(
                    state_name,
                    state_data_type.clone(),
                    false,
                    visible_source_index,
                    aggregate_index,
                    runtime_kind(call.function()),
                    role,
                    false,
                ));
            }
        } else {
            validate_state_column_type(
                call.function(),
                MvAggregateStateRole::Single,
                &state_data_type,
                &state_name,
            )?;
            physical_columns.push(physical_column(
                state_name.clone(),
                LogicalType::Binary,
                false,
                false,
                false,
            ));
            state_columns.push(MvAggregateStateColumn::new(
                state_name,
                state_data_type,
                false,
                visible_source_index,
                aggregate_index,
                runtime_kind(call.function()),
                MvAggregateStateRole::Single,
                call.count_star(),
            ));
        }
    }

    if !calls
        .iter()
        .any(|call| call.function() == AggregateFunctionKind::Count && call.count_star())
    {
        validate_state_column_type(
            AggregateFunctionKind::Count,
            MvAggregateStateRole::RetractionCount,
            &DataType::Int64,
            MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN,
        )?;
        physical_columns.push(physical_column(
            MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN.to_string(),
            LogicalType::Int64,
            false,
            false,
            false,
        ));
        state_columns.push(MvAggregateStateColumn::new(
            MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN.to_string(),
            DataType::Int64,
            false,
            0,
            calls.len(),
            MvAggregateRuntimeKind::Count,
            MvAggregateStateRole::RetractionCount,
            true,
        ));
    }

    let runtime_layout = MvAggregateRuntimeLayout::try_new(
        MV_AGGREGATE_ROW_ID_COLUMN.to_string(),
        visible_columns,
        state_columns,
        aggregate_input_types.to_vec(),
        group_key_source_indexes.to_vec(),
    )?;
    Ok(SqlMvAggregatePhysicalLayout {
        row_id_column,
        physical_columns,
        runtime_layout,
    })
}

/// Reject duplicate physical names using StarRocks identifier normalization.
pub fn validate_unique_aggregate_physical_column_names(
    physical_columns: &[SqlMvAggregatePhysicalColumn],
) -> Result<(), String> {
    let mut names = HashSet::with_capacity(physical_columns.len());
    for column in physical_columns {
        let normalized = normalize_identifier(&column.column.name)?;
        if !names.insert(normalized.clone()) {
            return Err(format!(
                "aggregate MV physical column name collision: hidden column name collision or duplicate physical column `{normalized}`"
            ));
        }
    }
    Ok(())
}

fn physical_column(
    name: String,
    data_type: LogicalType,
    nullable: bool,
    visible: bool,
    is_key: bool,
) -> SqlMvAggregatePhysicalColumn {
    SqlMvAggregatePhysicalColumn::new(name, data_type, nullable, visible, is_key)
}

fn runtime_kind(function: AggregateFunctionKind) -> MvAggregateRuntimeKind {
    match function {
        AggregateFunctionKind::Count => MvAggregateRuntimeKind::Count,
        AggregateFunctionKind::Sum => MvAggregateRuntimeKind::Sum,
        AggregateFunctionKind::Avg => MvAggregateRuntimeKind::Avg,
        AggregateFunctionKind::Min => MvAggregateRuntimeKind::Min,
        AggregateFunctionKind::Max => MvAggregateRuntimeKind::Max,
        AggregateFunctionKind::BoolOr => MvAggregateRuntimeKind::BoolOr,
        AggregateFunctionKind::BoolAnd => MvAggregateRuntimeKind::BoolAnd,
        AggregateFunctionKind::CountDistinct => MvAggregateRuntimeKind::CountDistinct,
        AggregateFunctionKind::ApproxCountDistinct => MvAggregateRuntimeKind::ApproxCountDistinct,
    }
}

fn validate_state_column_type(
    function: AggregateFunctionKind,
    state_role: MvAggregateStateRole,
    data_type: &DataType,
    state_name: &str,
) -> Result<(), String> {
    match state_role {
        MvAggregateStateRole::RetractionCount => match data_type {
            DataType::Int64 => Ok(()),
            other => Err(format!(
                "aggregate MV retraction count state column `{state_name}` must be BIGINT, got {other:?}"
            )),
        },
        MvAggregateStateRole::Single => match data_type {
            DataType::Binary | DataType::LargeBinary => Ok(()),
            other => Err(format!(
                "expected VARBINARY state column type for `{state_name}` ({function:?}), got: {other:?}"
            )),
        },
        MvAggregateStateRole::AvgSum | MvAggregateStateRole::AvgCount => match data_type {
            DataType::Binary | DataType::LargeBinary => Ok(()),
            other => Err(format!(
                "expected VARBINARY AVG state column type for `{state_name}` ({function:?}), got: {other:?}"
            )),
        },
    }
}

fn validate_aggregate_state_visible_type(
    function: AggregateFunctionKind,
    visible_data_type: &DataType,
    input_data_type: Option<&DataType>,
    output_name: &str,
) -> Result<(), String> {
    match (function, visible_data_type) {
        (AggregateFunctionKind::Sum, DataType::Float32 | DataType::Float64) => Err(format!(
            "SUM state type is unsupported for aggregate `{output_name}` output: {visible_data_type:?}; FLOAT/DOUBLE inputs are not supported by SUM state"
        )),
        (AggregateFunctionKind::Avg, DataType::Decimal128(_, _)) => match input_data_type {
            Some(DataType::Decimal128(_, _)) => Ok(()),
            Some(other) => Err(format!(
                "AVG state type is unsupported for aggregate `{output_name}` input: {other:?}; DECIMAL AVG requires Decimal128 input scale metadata"
            )),
            None => Err(format!(
                "AVG state type is unsupported for aggregate `{output_name}` output: {visible_data_type:?}; DECIMAL AVG requires input scale metadata"
            )),
        },
        _ => Ok(()),
    }
}

fn sanitize_state_column_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "agg".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Field;

    use super::*;

    #[test]
    fn aggregate_physical_columns_preserve_exact_visible_logical_facts() {
        use novarocks_types::logical_type::LogicalType;
        let statements =
            novarocks_parser::parse("SELECT items, SUM(v) AS s FROM t GROUP BY items").unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query");
        };
        let calls = crate::planning::mv::extract_aggregate_sql_calls(query).unwrap();
        let field = Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
            true,
        );
        let nested =
            crate::planning::mv::SqlMvOutputColumnFacts::from_engine_field(&field).unwrap();
        let outputs = vec![
            nested.clone(),
            crate::planning::mv::SqlMvOutputColumnFacts::from_logical(
                "s".into(),
                LogicalType::Int64,
                true,
            )
            .unwrap(),
        ];
        let facts = SqlMvAggregateLayoutFacts::from_aggregate_calls_and_outputs(
            &calls,
            &outputs,
            &[Some(DataType::Int64)],
        )
        .unwrap();
        let layout = build_sql_mv_aggregate_physical_layout(&facts).unwrap();
        let visible = layout
            .physical_columns()
            .iter()
            .find(|column| column.column().name == "items")
            .unwrap();
        assert_eq!(visible.logical_type(), nested.logical_type());
        let LogicalType::Array { element, .. } = visible.logical_type() else {
            panic!("array");
        };
        assert!(!element.nullable);
        assert_eq!(layout.row_id_column().logical_type(), &LogicalType::Utf8);
        for state in layout
            .physical_columns()
            .iter()
            .filter(|column| column.column().name.starts_with(MV_AGGREGATE_STATE_PREFIX))
        {
            let expected = if state.column().name == MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN {
                LogicalType::Int64
            } else {
                LogicalType::Binary
            };
            assert_eq!(state.logical_type(), &expected);
        }
    }

    #[test]
    fn aggregate_physical_declaration_accepts_fixed_binary_without_sql_syntax_gate() {
        let statements =
            novarocks_parser::parse("SELECT k, SUM(v) AS s FROM t GROUP BY k").unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query");
        };
        let calls = crate::planning::mv::extract_aggregate_sql_calls(query).unwrap();
        let outputs = vec![
            crate::planning::mv::SqlMvOutputColumnFacts::from_logical(
                "k".into(),
                LogicalType::FixedSizeBinary(32),
                false,
            )
            .unwrap(),
            crate::planning::mv::SqlMvOutputColumnFacts::from_logical(
                "s".into(),
                LogicalType::Int64,
                true,
            )
            .unwrap(),
        ];
        let facts = SqlMvAggregateLayoutFacts::from_aggregate_calls_and_outputs(
            &calls,
            &outputs,
            &[Some(DataType::Int64)],
        )
        .unwrap();
        let layout = build_sql_mv_aggregate_physical_layout(&facts).unwrap();
        let key = layout
            .physical_columns()
            .iter()
            .find(|column| column.column().name == "k")
            .unwrap();
        assert_eq!(key.column().data_type, LogicalType::FixedSizeBinary(32));
        assert_eq!(
            layout.runtime_layout().visible_columns()[0].data_type(),
            &DataType::FixedSizeBinary(32)
        );
    }

    #[test]
    fn physical_column_validator_keeps_normalized_collision_error() {
        let columns = vec![
            physical_column(
                "Visible_Output".to_string(),
                LogicalType::Int64,
                false,
                true,
                false,
            ),
            physical_column(
                "`visible_output`".to_string(),
                LogicalType::Int64,
                false,
                true,
                false,
            ),
        ];
        assert_eq!(
            validate_unique_aggregate_physical_column_names(&columns),
            Err("aggregate MV physical column name collision: hidden column name collision or duplicate physical column `visible_output`".to_string())
        );
    }
}
