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

use crate::sql::analysis::cte::CTERegistry;
use crate::sql::analysis::*;
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::planner::logical::*;
use crate::sql::planner::payload::*;

use super::aggregate::{
    collect_non_agg_column_refs, dedup_group_by_exprs, expr_column_id, prepare_repeat_input,
    split_projection_for_aggregate,
};
use super::relation::plan_relation_scoped;
use super::subquery::{wrap_predicate_applies, wrap_scalar_applies};
use super::window::build_window_and_project;

// ---------------------------------------------------------------------------
// SELECT planning
// ---------------------------------------------------------------------------

pub(super) fn plan_select_scoped(
    mut select: ResolvedSelect,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    const REPEAT_GROUP_QUALIFIER: &str = "__repeat_group";

    // Take ownership of all apply specs up-front. The wrap points below consume
    // them clause by clause.
    let mut apply_specs = std::mem::take(&mut select.apply_specs);
    let mut predicate_apply_specs = std::mem::take(&mut select.predicate_apply_specs);

    let mut current = match select.from.take() {
        Some(relation) => plan_relation_scoped(relation, cte_registry, factory)?,
        None => LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![vec![]],
                columns: vec![],
            }),
            vec![],
            None,
        ),
    };

    // WHERE placement: Apply nodes for WHERE-clause scalar subqueries are
    // inserted between the FROM plan and the WHERE Filter so the output column
    // is visible when the filter expression evaluates.
    current = wrap_scalar_applies(
        current,
        &mut apply_specs,
        ApplyClause::Where,
        cte_registry,
        factory,
    )?;
    current = wrap_predicate_applies(
        current,
        &mut predicate_apply_specs,
        ApplyClause::Where,
        cte_registry,
        factory,
    )?;

    if let Some(predicate) = select.filter.take() {
        current = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: predicate,
            }),
            vec![current],
            None,
        );
    }

    if let Some(mut repeat_info) = select.repeat.take() {
        let grouping_key_aliases = prepare_repeat_input(
            &mut current,
            &mut select,
            &mut repeat_info,
            REPEAT_GROUP_QUALIFIER,
            factory,
        );
        current = LogicalPlanNode::new(
            LogicalPlanKind::Repeat(PlanRepeatNode {
                repeat_column_ref_list: repeat_info.repeat_column_ref_list,
                repeat_column_ref_ids: repeat_info.repeat_column_ref_ids,
                grouping_ids: repeat_info.grouping_ids,
                all_rollup_columns: repeat_info.all_rollup_columns,
                all_rollup_column_ids: repeat_info.all_rollup_column_ids,
                grouping_key_aliases: grouping_key_aliases,
                grouping_fn_args: repeat_info.grouping_fn_args,
                grouping_fn_arg_ids: repeat_info.grouping_fn_arg_ids,
                grouping_fn_ids: repeat_info.grouping_fn_ids,
                virtual_tuple_id: None,
            }),
            vec![current],
            None,
        );
    }

    if select.has_aggregation || !select.group_by.is_empty() {
        if let Some(ref having_expr) = select.having {
            // Collect the output column ids of HAVING apply specs so they are
            // not mistakenly promoted into the GROUP BY list. Those columns are
            // produced by Apply nodes that sit ABOVE the Aggregate, so the
            // Aggregate must not try to pass them through as group keys.
            let mut having_apply_col_ids: std::collections::HashSet<ColumnId> = apply_specs
                .iter()
                .filter(|s| s.clause == ApplyClause::Having)
                .map(|s| s.output_column.column_id)
                .collect();
            having_apply_col_ids.extend(
                predicate_apply_specs
                    .iter()
                    .filter(|s| s.clause == ApplyClause::Having)
                    .map(|s| s.output_column.column_id),
            );
            let mut extra_gb = Vec::new();
            collect_non_agg_column_refs(having_expr, &select.group_by, &mut extra_gb);
            for col in extra_gb {
                // Skip output columns of HAVING apply specs — they are
                // provided by the Apply node above the Aggregate, not below.
                if let ExprKind::ColumnRef { column_id, .. } = &col.kind
                    && having_apply_col_ids.contains(column_id)
                {
                    continue;
                }
                select.group_by.push(col);
            }
        }

        let aggregate_group_by = dedup_group_by_exprs(&select.group_by);
        let (project_items, agg_calls, output_columns, rewritten_having) =
            split_projection_for_aggregate(
                &select.projection,
                &aggregate_group_by,
                select.having.as_ref(),
                factory,
            );
        current = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: aggregate_group_by,
                aggregates: agg_calls,
                output_columns: output_columns,
                already_pushed: false,
            }),
            vec![current],
            None,
        );

        // HAVING placement: Apply nodes for HAVING-clause scalar subqueries
        // are inserted above the Aggregate and below the HAVING Filter.
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Having,
            cte_registry,
            factory,
        )?;
        current = wrap_predicate_applies(
            current,
            &mut predicate_apply_specs,
            ApplyClause::Having,
            cte_registry,
            factory,
        )?;

        if let Some(having) = rewritten_having {
            current = LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode { predicate: having }),
                vec![current],
                None,
            );
        }

        // Projection placement (aggregated branch): Apply nodes for
        // Projection-clause scalar subqueries are inserted before the window
        // and project so the output column is available for the SELECT list.
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Projection,
            cte_registry,
            factory,
        )?;

        current = build_window_and_project(current, project_items, factory)?;
    } else {
        // Projection placement (non-aggregated branch).
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Projection,
            cte_registry,
            factory,
        )?;

        current = build_window_and_project(current, select.projection.clone(), factory)?;
    }

    debug_assert!(
        apply_specs.is_empty() && predicate_apply_specs.is_empty(),
        "unplaced apply specs: scalar={:?} predicate={:?}",
        apply_specs.iter().map(|s| s.clause).collect::<Vec<_>>(),
        predicate_apply_specs
            .iter()
            .map(|s| s.clause)
            .collect::<Vec<_>>()
    );

    // SELECT DISTINCT → Aggregate on all output columns (deduplication)
    if select.distinct {
        current = build_distinct(current, &select.projection, factory);
    }

    Ok(current)
}

/// Build a deduplication Aggregate for SELECT DISTINCT.
/// Uses all projection columns as GROUP BY keys with no aggregate functions.
fn build_distinct(
    input: LogicalPlanNode,
    projection: &[ProjectItem],
    factory: &mut ColumnRefFactory,
) -> LogicalPlanNode {
    let mut group_by = Vec::new();
    let mut output_columns = Vec::new();
    for item in projection {
        // Prefer the pre-assigned output_column_id (e.g. synthetic __match_N
        // columns from IN/EXISTS subquery rewrites). Falling back to
        // expr_column_id would mint a fresh id, disconnecting the column from
        // any downstream reference that already holds the original id.
        let cid = if item.output_column_id != ColumnId::UNSET {
            item.output_column_id
        } else {
            expr_column_id(&item.expr, &item.output_name, factory)
        };
        group_by.push(TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: cid,
                qualifier: None,
                column: item.output_name.clone(),
            },
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
        });
        output_columns.push(OutputColumn {
            column_id: cid,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        });
    }
    LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by: group_by,
            aggregates: vec![],
            output_columns: output_columns,
            already_pushed: false,
        }),
        vec![input],
        None,
    )
}
