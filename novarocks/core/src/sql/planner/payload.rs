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

//! Stage-neutral leaf payloads shared by logical and physical planner IR.

use arrow::datatypes::DataType;

use crate::sql::analysis::{OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::ScanVariantColumn;
use crate::sql::planner::table::TableDef;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanScanNode {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    pub variant_columns: Vec<ScanVariantColumn>,
    pub mv_rewritten_from: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanFilterNode {
    pub predicate: TypedExpr,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanProjectNode {
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanSortNode {
    pub items: Vec<SortItem>,
    pub analytic_partition_by: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub offset: Option<i64>,
    pub partition_limit: Option<usize>,
    pub topn_type: Option<crate::exec::node::sort::SortTopNType>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanLimitNode {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanValuesNode {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanRepeatNode {
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub all_rollup_column_ids: Vec<ColumnId>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
    pub virtual_tuple_id: Option<i32>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanWindowNode {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanGenerateSeriesNode {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanTableFunctionNode {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanRowCountAssertion {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanAssertOneRowNode {
    pub subquery_text: String,
    pub desired_num_rows: Option<i64>,
    pub assertion: PlanRowCountAssertion,
    pub group_key_column_ids: Vec<ColumnId>,
    pub group_key_labels: Vec<String>,
    pub keyed_message_prefix: Option<String>,
}

impl PlanAssertOneRowNode {
    pub(crate) fn global_at_most_one(subquery_text: impl Into<String>) -> Self {
        Self {
            subquery_text: subquery_text.into(),
            desired_num_rows: Some(1),
            assertion: PlanRowCountAssertion::Le,
            group_key_column_ids: Vec::new(),
            group_key_labels: Vec::new(),
            keyed_message_prefix: None,
        }
    }

    pub(crate) fn per_key_at_most_one(
        subquery_text: impl Into<String>,
        group_key_column_ids: Vec<ColumnId>,
        group_key_labels: Vec<String>,
        keyed_message_prefix: impl Into<String>,
    ) -> Self {
        Self {
            subquery_text: subquery_text.into(),
            desired_num_rows: Some(1),
            assertion: PlanRowCountAssertion::Le,
            group_key_column_ids,
            group_key_labels,
            keyed_message_prefix: Some(keyed_message_prefix.into()),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanCTEAnchorNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanCTEProduceNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanCTEConsumeNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
    pub producer_column_ids: Vec<crate::sql::column_id::ColumnId>,
}

/// A single window function expression with its OVER specification.
#[derive(Clone, Debug)]
pub(crate) struct WindowExpr {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub partition_by: Vec<TypedExpr>,
    pub order_by: Vec<SortItem>,
    pub window_frame: Option<crate::sql::analysis::WindowFrame>,
    pub result_type: DataType,
    /// Display label only (EXPLAIN / output schema). Identity is now
    /// `output_column_id`. (G1: `output_name` downgraded from a binding key.)
    pub output_name: String,
    /// G1: globally-unique id of this window function's output column.
    /// TODO(G1 P2/P3): remove this allow once parent Project/window references
    /// are rebound by id and downstream binding consumes the populated field.
    #[allow(dead_code)]
    pub output_column_id: crate::sql::column_id::ColumnId,
    /// `IGNORE NULLS` modifier. Currently honored by first_value / last_value
    /// / lead / lag; ignored for other window functions.
    pub ignore_nulls: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateCall {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub result_type: DataType,
    pub order_by: Vec<SortItem>,
    /// G1: id of THIS aggregate's output column. Planner-created calls are
    /// minted by `collect_aggregates`; rewrite paths should preserve existing
    /// ids or allocate ids for newly-defined aggregate outputs. Fixtures and
    /// transient adapters may use `UNSET` until they become executable
    /// bindings.
    pub output_column_id: crate::sql::column_id::ColumnId,
}
