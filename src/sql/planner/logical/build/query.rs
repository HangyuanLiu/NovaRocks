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
use crate::sql::planner::plan::*;

use super::aggregate::{
    collect_aggregates, ensure_aggregate_output_columns, planner_aggregate_group_by_targets,
    planner_repeat_original_group_by_targets, rewrite_agg_calls_to_refs, rewrite_expr_children,
    rewrite_group_by_expr_refs,
};
use super::output::{adapt_plan_output, plan_output_columns};
use super::relation::{plan_set_operation_scoped, plan_values};
use super::select::plan_select_scoped;

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

/// Plan a resolved query into a single logical tree, wrapping CTE definitions
/// as nested anchor/produce pairs around the main query subtree.
pub(crate) fn plan_query(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    plan_scoped_query(resolved, &cte_registry, factory)
}

pub(super) fn plan_scoped_query(
    resolved: ResolvedQuery,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    let ResolvedQuery {
        body,
        order_by,
        limit,
        offset,
        output_columns,
        local_cte_ids,
    } = resolved;

    // Plan the query body first so we can stamp fresh set-op ColumnIds before
    // apply_query_modifiers consumes output_columns.
    let mut body_plan = plan_body_scoped(body, cte_registry, factory)?;

    // Strategy A: if the body produced a set-op node (Union/Intersect/Except),
    // overwrite its output_columns with the fresh ColumnIds that the analyzer
    // allocated for this query's output (stored in `output_columns`).  The
    // planner previously left branch-side ColumnIds in those fields, which
    // disagreed with the fresh IDs that the parent scope uses to reference
    // the set-op output.
    match &mut body_plan.kind {
        LogicalPlanKind::Union(node) => {
            node.output_columns = output_columns.clone();
        }
        LogicalPlanKind::Intersect(node) => {
            node.output_columns = output_columns.clone();
        }
        LogicalPlanKind::Except(node) => {
            node.output_columns = output_columns.clone();
        }
        _ => {}
    }

    let mut root =
        apply_query_modifiers(body_plan, order_by, output_columns, limit, offset, factory);

    for cte_id in local_cte_ids.into_iter().rev() {
        let entry = cte_registry
            .get(cte_id)
            .ok_or_else(|| format!("missing CTE entry for id {cte_id}"))?;
        let produce_input = plan_scoped_query(entry.resolved_query.clone(), cte_registry, factory)?;
        let produce_input = adapt_plan_output(produce_input, &entry.output_columns)?;
        let produce = LogicalPlanNode::new(
            LogicalPlanKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: entry.id,
                output_columns: entry.output_columns.clone(),
            }),
            vec![produce_input],
            None,
        );
        root = LogicalPlanNode::new(
            LogicalPlanKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: entry.id }),
            vec![produce, root],
            None,
        );
    }

    Ok(root)
}

fn apply_query_modifiers(
    mut body_plan: LogicalPlanNode,
    order_by: Vec<SortItem>,
    output_columns: Vec<OutputColumn>,
    limit: Option<i64>,
    offset: Option<i64>,
    factory: &mut ColumnRefFactory,
) -> LogicalPlanNode {
    let mut final_projection: Option<Vec<ProjectItem>> = None;

    // Wrap with Sort if ORDER BY is present.
    if !order_by.is_empty() {
        let body_output_columns =
            plan_output_columns(&body_plan).unwrap_or_else(|_| output_columns.clone());
        let mut extra_items = collect_extra_sort_items(&order_by, &body_output_columns, factory);
        let sort_items =
            rewrite_sort_items_to_projection_refs(&order_by, &extra_items, &body_output_columns);
        if !extra_items.is_empty() {
            // We're about to add extra sort-only columns to the inner Project
            // and then strip them with an outer Project after the sort. To
            // make that outer Project's column references unambiguous — even
            // when two SELECT items share an output name (e.g. `t1.c2,
            // t2.c2` both default to `c2`) — rename each inner Project
            // SELECT item to a unique synthetic name (`__nr_sel_<idx>`).
            // The outer strip-projection then references those synthetic
            // names and re-aliases each to the user-visible output name.
            //
            // Extras keep their display-name output_name because
            // `sort_items` (rewritten above by
            // `rewrite_sort_items_to_projection_refs`) references them
            // through that exact name.
            //
            // Sort items that didn't match an extra (and therefore still
            // hold their original ColumnRef into the SELECT projection)
            // would otherwise fail to resolve after the rename, so we
            // remap any `ColumnRef(<select_output_name>)` to the matching
            // `__nr_sel_<idx>` below.
            // Each tuple: (user-visible output name, data type, nullable, inner output_column_id).
            // The inner output_column_id is captured here so the outer strip-project can
            // reference the same ColumnId that the inner Project produces, preserving id
            // continuity through the double-Project barrier for the Phase-1 tagging pass.
            let user_select: Option<Vec<(String, arrow::datatypes::DataType, bool, ColumnId)>> =
                if let LogicalPlanNode {
                    kind: LogicalPlanKind::Project(proj),
                    children,
                    ..
                } = &mut body_plan
                {
                    let select_items_for_extra = proj.items.clone();
                    for extra in &mut extra_items {
                        extra.expr = rewrite_project_output_refs_to_item_expr(
                            &extra.expr,
                            &select_items_for_extra,
                        );
                    }

                    if let Some(child) = children.get_mut(0)
                        && matches!(child.kind, LogicalPlanKind::Aggregate(_))
                    {
                        if let LogicalPlanKind::Aggregate(agg) = &mut child.kind {
                            for extra in &extra_items {
                                collect_aggregates(&extra.expr, &mut agg.aggregates, factory);
                            }
                            ensure_aggregate_output_columns(agg);
                        }
                        // ORDER BY-only aggregates (e.g. `count(v2)` that does
                        // not appear in SELECT) were just folded into the
                        // aggregate node above. Their extra Project items still
                        // carry raw AggregateCall expressions; rewrite them to
                        // reference the aggregate's output columns, exactly as
                        // split_projection_for_aggregate does for SELECT/HAVING.
                        // Without this the post-aggregate Project keeps a
                        // ColumnRef to the aggregate's *input* column (the
                        // aggregate argument), which the id-binding verifier
                        // rejects as "not produced by child scope".
                        // ORDER BY-only group-by *expressions* (e.g. `substr(col, ...)`
                        // that appears in GROUP BY/SELECT but whose ORDER BY display
                        // name didn't match the SELECT output name — most commonly the
                        // `substr`/`substring` alias, where the SELECT output name keeps
                        // the SQL-text spelling but the analyzed expr canonicalizes the
                        // function name) keep a raw expression over the aggregate's
                        // *input* columns. Rewrite them to reference the planner
                        // aggregate's group-key layout, exactly as
                        // split_projection_for_aggregate does for SELECT/HAVING.
                        // Without this the post-aggregate Project re-derives the group
                        // key from a pre-aggregate column that the id-binding verifier
                        // rejects as "not produced by child scope".
                        let repeat_gb_targets = planner_repeat_original_group_by_targets(child);
                        if let LogicalPlanKind::Aggregate(agg) = &mut child.kind {
                            let mut gb_targets = planner_aggregate_group_by_targets(agg);
                            gb_targets.extend(repeat_gb_targets);
                            for extra in &mut extra_items {
                                extra.expr =
                                    rewrite_agg_calls_to_refs(&extra.expr, &agg.aggregates);
                                extra.expr = rewrite_group_by_expr_refs(&extra.expr, &gb_targets);
                            }
                        }
                    }
                    let user: Vec<(String, arrow::datatypes::DataType, bool, ColumnId)> = proj
                        .items
                        .iter()
                        .map(|it| {
                            (
                                it.output_name.clone(),
                                it.expr.data_type.clone(),
                                it.expr.nullable,
                                it.output_column_id,
                            )
                        })
                        .collect();
                    for (idx, item) in proj.items.iter_mut().enumerate() {
                        item.output_name = format!("__nr_sel_{idx}");
                    }
                    for extra in &extra_items {
                        proj.items.push(extra.clone());
                    }
                    Some(user)
                } else {
                    None
                };

            // After renaming, sort items that still hold ColumnRefs to
            // pre-rename SELECT output names must be remapped onto the
            // synthetic `__nr_sel_<idx>` slots. Without this, sort
            // references like `ORDER BY v1` (matching SELECT v1 → renamed
            // to `__nr_sel_1`) would fail to resolve at sort time.
            let sort_items = if let Some(ref user) = user_select {
                let name_to_output: std::collections::HashMap<String, (usize, ColumnId)> = user
                    .iter()
                    .enumerate()
                    .map(|(idx, (name, _, _, output_id))| (name.to_lowercase(), (idx, *output_id)))
                    .collect();
                let id_to_output: std::collections::HashMap<ColumnId, (usize, ColumnId)> = user
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, (_, _, _, output_id))| {
                        (*output_id != ColumnId::UNSET).then_some((*output_id, (idx, *output_id)))
                    })
                    .collect();
                sort_items
                    .into_iter()
                    .map(|item| remap_sort_to_synthetic(item, &id_to_output, &name_to_output))
                    .collect()
            } else {
                sort_items
            };

            // Sort with extended scope
            body_plan = LogicalPlanNode::new(
                LogicalPlanKind::Sort(LogicalSortNode {
                    items: sort_items,
                    // Top-level ORDER BY — no analytic partition.
                    analytic_partition_by: Vec::new(),
                    output_columns: vec![],
                    offset: None,
                    partition_limit: None,
                    topn_type: None,
                }),
                vec![body_plan],
                None,
            );

            // Strip synthetic sort-only columns after LIMIT/OFFSET so the
            // limit stays directly above Sort and can be rewritten to TopN.
            final_projection = Some(if let Some(user) = user_select {
                user.into_iter()
                    .enumerate()
                    .map(|(idx, (name, dt, nullable, inner_cid))| {
                        let syn_name = format!("__nr_sel_{idx}");
                        // Reuse the inner project item's existing ColumnId so
                        // that the Phase-1 tagging pass can thread required
                        // columns through the double-Project barrier without
                        // encountering an id discontinuity. Minting a fresh id
                        // here would make the outer Project's output invisible
                        // to the inner Project's pruning tag.
                        let cid = inner_cid;
                        ProjectItem {
                            expr: TypedExpr {
                                kind: ExprKind::ColumnRef {
                                    column_id: cid,
                                    qualifier: None,
                                    column: syn_name,
                                },
                                data_type: dt,
                                nullable,
                            },
                            output_name: name,
                            output_column_id: cid,
                        }
                    })
                    .collect()
            } else {
                output_columns
                    .iter()
                    .map(|col| ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: col.column_id,
                                qualifier: None,
                                column: col.name.clone(),
                            },
                            data_type: col.data_type.clone(),
                            nullable: col.nullable,
                        },
                        output_name: col.name.clone(),
                        output_column_id: col.column_id,
                    })
                    .collect()
            });
        } else {
            body_plan = LogicalPlanNode::new(
                LogicalPlanKind::Sort(LogicalSortNode {
                    items: sort_items,
                    // Top-level ORDER BY — no analytic partition.
                    analytic_partition_by: Vec::new(),
                    output_columns: vec![],
                    offset: None,
                    partition_limit: None,
                    topn_type: None,
                }),
                vec![body_plan],
                None,
            );
        }
    }

    // Wrap with Limit if LIMIT/OFFSET is present.
    if limit.is_some() || offset.is_some() {
        body_plan = LogicalPlanNode::new(
            LogicalPlanKind::Limit(LogicalLimitNode {
                limit: limit,
                offset: offset,
            }),
            vec![body_plan],
            None,
        );
    }

    if let Some(items) = final_projection {
        body_plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(LogicalProjectNode {
                items: items,
                output_qualifier: None,
            }),
            vec![body_plan],
            None,
        );
    }

    body_plan
}

fn collect_extra_sort_items(
    order_by: &[SortItem],
    output: &[OutputColumn],
    factory: &mut ColumnRefFactory,
) -> Vec<ProjectItem> {
    let output_names: std::collections::HashSet<String> =
        output.iter().map(|c| c.name.to_lowercase()).collect();
    let output_ids: std::collections::HashSet<ColumnId> = output
        .iter()
        .filter_map(|c| (c.column_id != ColumnId::UNSET).then_some(c.column_id))
        .collect();
    let mut added = std::collections::HashSet::new();
    let mut extra = Vec::new();
    for item in order_by {
        if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind
            && output_ids.contains(column_id)
        {
            continue;
        }
        let output_name = crate::sql::codegen::helpers::typed_expr_display_name(&item.expr);
        let output_name_lower = output_name.to_lowercase();
        if !output_names.contains(&output_name_lower) && added.insert(output_name_lower) {
            let output_column_id = if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind {
                *column_id
            } else {
                factory.create(
                    None,
                    output_name.clone(),
                    item.expr.data_type.clone(),
                    item.expr.nullable,
                )
            };
            extra.push(ProjectItem {
                expr: item.expr.clone(),
                output_name,
                output_column_id,
            });
        }
    }
    extra
}

/// Rewrite a sort item so any unqualified `ColumnRef` pointing at a
/// pre-rename SELECT output name is remapped to the matching
/// `__nr_sel_<idx>`. Used after the inner Project items have been renamed
/// for the sort-extras flow so that simple `ORDER BY <select_alias>`
/// references still resolve.
fn remap_sort_to_synthetic(
    item: SortItem,
    id_to_output: &std::collections::HashMap<ColumnId, (usize, ColumnId)>,
    name_to_output: &std::collections::HashMap<String, (usize, ColumnId)>,
) -> SortItem {
    let SortItem {
        expr,
        asc,
        nulls_first,
    } = item;
    SortItem {
        expr: remap_select_alias_refs(expr, id_to_output, name_to_output),
        asc,
        nulls_first,
    }
}

fn remap_select_alias_refs(
    expr: TypedExpr,
    id_to_output: &std::collections::HashMap<ColumnId, (usize, ColumnId)>,
    name_to_output: &std::collections::HashMap<String, (usize, ColumnId)>,
) -> TypedExpr {
    match expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier: None,
            ref column,
        } => {
            let target = if column_id != ColumnId::UNSET {
                id_to_output.get(&column_id)
            } else {
                None
            }
            .or_else(|| name_to_output.get(&column.to_lowercase()));
            if let Some((idx, output_id)) = target {
                TypedExpr {
                    data_type: expr.data_type,
                    nullable: expr.nullable,
                    kind: ExprKind::ColumnRef {
                        column_id: *output_id,
                        qualifier: None,
                        column: format!("__nr_sel_{idx}"),
                    },
                }
            } else {
                expr
            }
        }
        _ => expr,
    }
}

pub(super) fn rewrite_project_output_refs_to_item_expr(
    expr: &TypedExpr,
    project_items: &[ProjectItem],
) -> TypedExpr {
    if let ExprKind::ColumnRef {
        column_id,
        qualifier: None,
        column,
    } = &expr.kind
    {
        if *column_id != ColumnId::UNSET
            && let Some(item) = project_items
                .iter()
                .find(|item| item.output_column_id == *column_id)
        {
            return item.expr.clone();
        }
        if let Some(item) = project_items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case(column))
        {
            return item.expr.clone();
        }
    }

    rewrite_expr_children(expr, |child| {
        rewrite_project_output_refs_to_item_expr(child, project_items)
    })
}

fn rewrite_sort_items_to_projection_refs(
    order_by: &[SortItem],
    extra_items: &[ProjectItem],
    output: &[OutputColumn],
) -> Vec<SortItem> {
    let extra_names: std::collections::HashMap<String, &ProjectItem> = extra_items
        .iter()
        .map(|item| {
            (
                crate::sql::codegen::helpers::typed_expr_display_name(&item.expr).to_lowercase(),
                item,
            )
        })
        .collect();
    // SELECT output columns keyed by display name. A non-ColumnRef ORDER BY item
    // (e.g. `sum(x)`) whose display name matches a SELECT output already computed
    // by the aggregate/projection must reference that output column rather than
    // repeat the expression — repeating it keeps a raw AggregateCall whose
    // argument column lives below the aggregate and is not in the sort's input
    // scope ("not produced by child scope").
    let output_by_name: std::collections::HashMap<String, &OutputColumn> = output
        .iter()
        .filter(|c| c.column_id != ColumnId::UNSET)
        .map(|c| (c.name.to_lowercase(), c))
        .collect();
    let output_by_id: std::collections::HashMap<ColumnId, &OutputColumn> = output
        .iter()
        .filter(|c| c.column_id != ColumnId::UNSET)
        .map(|c| (c.column_id, c))
        .collect();

    order_by
        .iter()
        .map(|item| {
            let display =
                crate::sql::codegen::helpers::typed_expr_display_name(&item.expr).to_lowercase();
            if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind
                && *column_id != ColumnId::UNSET
                && output_by_id.contains_key(column_id)
            {
                // Positional ORDER BY is already resolved to the exact SELECT
                // output ColumnId. Keep that id instead of re-resolving by
                // display name; duplicate output names such as `s.a, t.a`
                // would otherwise collapse both sort keys onto the same slot.
                item.clone()
            } else if let Some(extra) = extra_names.get(&display) {
                // Preserve the extra item's output_column_id so that the
                // Phase-1 tagging pass (tag_sort → collect_column_id_refs)
                // can see this sort key's ColumnId and include it in the
                // child's required_output_columns.  Using UNSET here caused
                // tag_sort to silently omit the extra column from the inner
                // project's needed set, which then made PruneProjectColumns
                // drop the extra item → the sort's input no longer had the
                // column → "Column cannot be resolved" at codegen time.
                SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: extra.output_column_id,
                            qualifier: None,
                            column: extra.output_name.clone(),
                        },
                        data_type: item.expr.data_type.clone(),
                        nullable: item.expr.nullable,
                    },
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                }
            } else if let ExprKind::ColumnRef {
                qualifier: None,
                column,
                ..
            } = &item.expr.kind
                && let Some(col) = output_by_name.get(&column.to_lowercase())
            {
                SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: col.column_id,
                            qualifier: None,
                            column: col.name.clone(),
                        },
                        data_type: item.expr.data_type.clone(),
                        nullable: item.expr.nullable,
                    },
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                }
            } else if !matches!(item.expr.kind, ExprKind::ColumnRef { .. })
                && let Some(col) = output_by_name.get(&display)
            {
                // Non-ColumnRef ORDER BY item (e.g. an aggregate) that names a
                // SELECT output: reference that already-computed output column.
                SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: col.column_id,
                            qualifier: None,
                            column: col.name.clone(),
                        },
                        data_type: item.expr.data_type.clone(),
                        nullable: item.expr.nullable,
                    },
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                }
            } else {
                item.clone()
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Body planning
// ---------------------------------------------------------------------------

fn plan_body_scoped(
    body: QueryBody,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    match body {
        QueryBody::Select(select) => plan_select_scoped(select, cte_registry, factory),
        QueryBody::SetOperation(set_op) => plan_set_operation_scoped(set_op, cte_registry, factory),
        QueryBody::Values(values) => plan_values(values, factory),
    }
}
