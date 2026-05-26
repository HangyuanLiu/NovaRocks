//! Rewrite pass for `LowCardinalityDictionaryRewrite`.
//!
//! Walks the plan top-down. Per-node behavior:
//!
//! * `Scan`: attach `ScanDictionaryColumn` hints + an extra hidden
//!   `OutputColumn` so codegen materializes the dict-id slot. The
//!   string column itself is kept on the scan output — callers that
//!   only need the dict id read it from the dict slot, and any
//!   downstream `Decode` consumes the dict slot back to the original
//!   string slot.
//! * `Aggregate`: rewrite group-by string column refs to point at the
//!   dict slot; if the aggregate exposes a string group-by column
//!   upward, insert a `Decode` above the aggregate so consumers still
//!   see the string value.
//! * `Sort` / `TopN-via-Sort`: when a sort key has an order-preserving
//!   snapshot, rewrite the key to the dict slot; otherwise insert a
//!   `Decode` between the sort and its input so the sort still sees
//!   strings.
//! * `Project`, `Limit`: passthrough at Task 7 scope (Limit cannot
//!   change semantics; Project today simply consumes whatever the
//!   child surfaces).
//! * `Join` / `Union` / `Intersect` / `Except` / `Window` /
//!   `TableFunction` / set ops: conservative decode boundary — every
//!   dict column flowing through is decoded back to its string before
//!   the node. `TODO(task-8)` markers flag where Task 8 can refine.
//!
//! The rewriter is idempotent: a `Scan` whose `dict_columns` is
//! already populated is skipped on a second pass.

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::{
    AggregateNode, DecodeMapping, DecodeNode, FilterNode, LimitNode, LogicalPlan, ProjectNode,
    ScanDictionaryColumn, ScanNode, SortNode,
};

use super::context::DictionaryRewriteContext;
use super::expr::rewrite_column_ref;

pub(crate) fn rewrite(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    rewrite_node(plan, ctx)
}

fn rewrite_node(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    match plan {
        LogicalPlan::Scan(scan) => Ok(LogicalPlan::Scan(rewrite_scan(scan, ctx))),
        LogicalPlan::Filter(node) => Ok(LogicalPlan::Filter(FilterNode {
            input: Box::new(rewrite_node(*node.input, ctx)?),
            predicate: node.predicate,
        })),
        LogicalPlan::Project(node) => rewrite_project(node, ctx),
        LogicalPlan::Aggregate(node) => rewrite_aggregate(node, ctx),
        LogicalPlan::Sort(node) => rewrite_sort(node, ctx),
        LogicalPlan::Limit(node) => Ok(LogicalPlan::Limit(LimitNode {
            input: Box::new(rewrite_node(*node.input, ctx)?),
            limit: node.limit,
            offset: node.offset,
        })),
        // Conservative decode boundary for nodes Task 7 does not yet
        // analyse. `TODO(task-8)`: tighten these to keep dict columns
        // flowing where the snapshots line up.
        LogicalPlan::Join(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::Intersect(_)
        | LogicalPlan::Except(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::TableFunction(_)
        | LogicalPlan::Repeat(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::CTEAnchor(_)
        | LogicalPlan::CTEProduce(_) => decode_boundary(plan, ctx),
        // Leaves that produce no dict columns of their own.
        LogicalPlan::CTEConsume(_) | LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) => {
            Ok(plan)
        }
        // Decode is the rewrite's own output; do not recurse into it
        // again.
        LogicalPlan::Decode(_) => Ok(plan),
    }
}

fn rewrite_scan(mut scan: ScanNode, ctx: &mut DictionaryRewriteContext) -> ScanNode {
    // Idempotency guard: an already-populated `dict_columns` means a
    // previous application of this rule already rewrote the scan.
    if !scan.dict_columns.is_empty() {
        return scan;
    }
    let eligible = ctx.dict_eligible_columns_for_scan(&scan.database, &scan.table.name);
    if eligible.is_empty() {
        return scan;
    }
    for (col_name, snapshot) in eligible {
        // Locate the source column descriptor to preserve nullability.
        let (source_name, nullable) = match scan
            .columns
            .iter()
            .find(|c| c.name.to_ascii_lowercase() == col_name.to_ascii_lowercase())
        {
            Some(c) => (c.name.clone(), c.nullable),
            None => continue,
        };
        let dict_column = DictionaryRewriteContext::dict_column_name(&scan.table.name, &col_name);
        scan.columns.push(OutputColumn {
            column_id: ColumnId::UNSET,
            name: dict_column.clone(),
            data_type: DataType::Int32,
            nullable,
        });
        if let Some(required) = scan.required_columns.as_mut() {
            required.push(dict_column.clone());
        }
        scan.dict_columns.push(ScanDictionaryColumn {
            source_column: source_name,
            dict_column,
            dictionary: snapshot,
        });
        ctx.mark_changed();
    }
    scan
}

fn rewrite_project(
    node: ProjectNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    let input = rewrite_node(*node.input, ctx)?;
    // Task 7 scope: do not rewrite project items themselves. Projects
    // that re-emit a string column do so by carrying its dict alias up
    // implicitly; the parent boundary (or the final user-facing
    // projection) is what decides whether a Decode is needed.
    // TODO(task-8): rewrite project items so derived dict expressions
    // (`upper(s)`, `concat(s, '!')`, etc.) operate on the dict id.
    Ok(LogicalPlan::Project(ProjectNode {
        input: Box::new(input),
        items: node.items,
    }))
}

fn rewrite_aggregate(
    node: AggregateNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    let input = rewrite_node(*node.input, ctx)?;
    let mut group_by = Vec::with_capacity(node.group_by.len());
    let mut decoded_group_keys: Vec<(String, String)> = Vec::new();
    for expr in &node.group_by {
        if let crate::sql::analysis::ExprKind::ColumnRef { column, .. } = &expr.kind
            && let Some(dict_col) = ctx.dict_column_for(column)
        {
            group_by.push(rewrite_column_ref(expr, ctx));
            // The aggregate node was emitting the original string
            // column name to consumers; we must surface that name
            // through a Decode boundary above the aggregate.
            decoded_group_keys.push((dict_col.to_string(), column.clone()));
            continue;
        }
        group_by.push(expr.clone());
    }

    // Output columns: dict-encoded group-by columns are renamed to the
    // dict slot for the immediate aggregate scope; the decode above
    // restores the original string name for callers.
    let mut output_columns: Vec<OutputColumn> = node
        .output_columns
        .iter()
        .map(|out| {
            if let Some(dict) = ctx.dict_column_for(&out.name) {
                OutputColumn {
                    column_id: out.column_id,
                    name: dict.to_string(),
                    data_type: DataType::Int32,
                    nullable: out.nullable,
                }
            } else {
                out.clone()
            }
        })
        .collect();
    let aggregate = LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(input),
        group_by,
        aggregates: node.aggregates,
        output_columns: output_columns.clone(),
        already_pushed: node.already_pushed,
    });
    if decoded_group_keys.is_empty() {
        return Ok(aggregate);
    }

    let mappings: Vec<DecodeMapping> = decoded_group_keys
        .iter()
        .map(|(dict, string)| DecodeMapping {
            dict_column: dict.clone(),
            string_column: string.clone(),
        })
        .collect();
    // Restore the original string-column names on the post-decode
    // output_columns so consumers continue to bind to the string.
    for out in output_columns.iter_mut() {
        if let Some(original) = ctx.string_column_for(&out.name) {
            out.name = original.to_string();
            // The decoded column's logical type is the snapshot's
            // string DataType, which is always Utf8 / LargeUtf8 /
            // Binary / LargeBinary; use the snapshot to pick.
            if let Some(snap) = ctx.snapshot_for_string(original) {
                out.data_type = snap.data_type.clone();
            }
        }
    }
    ctx.mark_changed();
    Ok(LogicalPlan::Decode(DecodeNode {
        input: Box::new(aggregate),
        mappings,
        output_columns,
    }))
}

fn rewrite_sort(node: SortNode, ctx: &mut DictionaryRewriteContext) -> Result<LogicalPlan, String> {
    let input = rewrite_node(*node.input, ctx)?;
    // Determine whether all sort keys with dict snapshots are
    // order-preserving. Otherwise insert a Decode before the sort so
    // the sort still operates on strings.
    let mut needs_decode = false;
    let mut sort_items = Vec::with_capacity(node.items.len());
    for item in &node.items {
        if let crate::sql::analysis::ExprKind::ColumnRef { column, .. } = &item.expr.kind {
            if let Some(snap) = ctx.snapshot_for_string(column) {
                if snap.order_preserving {
                    let mut rewritten = item.clone();
                    rewritten.expr = rewrite_column_ref(&item.expr, ctx);
                    sort_items.push(rewritten);
                    ctx.mark_changed();
                    continue;
                } else {
                    needs_decode = true;
                }
            }
        }
        sort_items.push(item.clone());
    }
    let input = if needs_decode {
        wrap_with_decode(input, ctx)
    } else {
        input
    };
    Ok(LogicalPlan::Sort(SortNode {
        input: Box::new(input),
        items: sort_items,
        analytic_partition_by: node.analytic_partition_by,
    }))
}

fn decode_boundary(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    // For nodes Task 7 does not refine, recurse into their children to
    // pick up scan-side dict columns, then wrap each child with a
    // Decode so the node itself never has to know about dict ids.
    let rewritten = rewrite_children(plan, ctx)?;
    Ok(wrap_children_with_decode(rewritten, ctx))
}

fn rewrite_children(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    match plan {
        LogicalPlan::Join(mut node) => {
            node.left = Box::new(rewrite_node(*node.left, ctx)?);
            node.right = Box::new(rewrite_node(*node.right, ctx)?);
            Ok(LogicalPlan::Join(node))
        }
        LogicalPlan::Union(mut node) => {
            let mut new_inputs = Vec::with_capacity(node.inputs.len());
            for input in node.inputs.drain(..) {
                new_inputs.push(rewrite_node(input, ctx)?);
            }
            node.inputs = new_inputs;
            Ok(LogicalPlan::Union(node))
        }
        LogicalPlan::Intersect(mut node) => {
            let mut new_inputs = Vec::with_capacity(node.inputs.len());
            for input in node.inputs.drain(..) {
                new_inputs.push(rewrite_node(input, ctx)?);
            }
            node.inputs = new_inputs;
            Ok(LogicalPlan::Intersect(node))
        }
        LogicalPlan::Except(mut node) => {
            let mut new_inputs = Vec::with_capacity(node.inputs.len());
            for input in node.inputs.drain(..) {
                new_inputs.push(rewrite_node(input, ctx)?);
            }
            node.inputs = new_inputs;
            Ok(LogicalPlan::Except(node))
        }
        LogicalPlan::Window(mut node) => {
            node.input = Box::new(rewrite_node(*node.input, ctx)?);
            Ok(LogicalPlan::Window(node))
        }
        LogicalPlan::TableFunction(mut node) => {
            node.input = Box::new(rewrite_node(*node.input, ctx)?);
            Ok(LogicalPlan::TableFunction(node))
        }
        LogicalPlan::Repeat(mut node) => {
            node.input = Box::new(rewrite_node(*node.input, ctx)?);
            Ok(LogicalPlan::Repeat(node))
        }
        LogicalPlan::SubqueryAlias(mut node) => {
            node.input = Box::new(rewrite_node(*node.input, ctx)?);
            Ok(LogicalPlan::SubqueryAlias(node))
        }
        LogicalPlan::CTEAnchor(mut node) => {
            node.produce = Box::new(rewrite_node(*node.produce, ctx)?);
            node.consumer = Box::new(rewrite_node(*node.consumer, ctx)?);
            Ok(LogicalPlan::CTEAnchor(node))
        }
        LogicalPlan::CTEProduce(mut node) => {
            node.input = Box::new(rewrite_node(*node.input, ctx)?);
            Ok(LogicalPlan::CTEProduce(node))
        }
        other => Ok(other),
    }
}

fn wrap_children_with_decode(plan: LogicalPlan, ctx: &mut DictionaryRewriteContext) -> LogicalPlan {
    match plan {
        LogicalPlan::Join(mut node) => {
            node.left = Box::new(wrap_with_decode(*node.left, ctx));
            node.right = Box::new(wrap_with_decode(*node.right, ctx));
            LogicalPlan::Join(node)
        }
        LogicalPlan::Union(mut node) => {
            node.inputs = node
                .inputs
                .into_iter()
                .map(|input| wrap_with_decode(input, ctx))
                .collect();
            LogicalPlan::Union(node)
        }
        LogicalPlan::Intersect(mut node) => {
            node.inputs = node
                .inputs
                .into_iter()
                .map(|input| wrap_with_decode(input, ctx))
                .collect();
            LogicalPlan::Intersect(node)
        }
        LogicalPlan::Except(mut node) => {
            node.inputs = node
                .inputs
                .into_iter()
                .map(|input| wrap_with_decode(input, ctx))
                .collect();
            LogicalPlan::Except(node)
        }
        LogicalPlan::Window(mut node) => {
            node.input = Box::new(wrap_with_decode(*node.input, ctx));
            LogicalPlan::Window(node)
        }
        LogicalPlan::TableFunction(mut node) => {
            node.input = Box::new(wrap_with_decode(*node.input, ctx));
            LogicalPlan::TableFunction(node)
        }
        LogicalPlan::Repeat(mut node) => {
            node.input = Box::new(wrap_with_decode(*node.input, ctx));
            LogicalPlan::Repeat(node)
        }
        LogicalPlan::SubqueryAlias(mut node) => {
            node.input = Box::new(wrap_with_decode(*node.input, ctx));
            LogicalPlan::SubqueryAlias(node)
        }
        LogicalPlan::CTEAnchor(mut node) => {
            node.produce = Box::new(wrap_with_decode(*node.produce, ctx));
            node.consumer = Box::new(wrap_with_decode(*node.consumer, ctx));
            LogicalPlan::CTEAnchor(node)
        }
        LogicalPlan::CTEProduce(mut node) => {
            node.input = Box::new(wrap_with_decode(*node.input, ctx));
            LogicalPlan::CTEProduce(node)
        }
        other => other,
    }
}

/// Wrap `plan` with a `Decode` for every dict column in scope so that
/// the parent operator only sees string columns. No-op when no dict
/// columns are active.
pub(crate) fn wrap_with_decode(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> LogicalPlan {
    if !ctx.has_any_dict_column() {
        return plan;
    }
    // Avoid double-decoding when the plan is already a Decode.
    if matches!(plan, LogicalPlan::Decode(_)) {
        return plan;
    }
    // Gather every dict slot reachable through the rule context; the
    // codegen-side Decode emits one mapping per dict slot the parent
    // expects to consume as a string.
    let mut mappings: Vec<DecodeMapping> = Vec::new();
    let mut renamed_outputs: Vec<OutputColumn> = Vec::new();
    let mut wrapped_any = false;
    for col in plan_output_columns(&plan) {
        if let Some(dict) = ctx.dict_column_for(&col.name) {
            // The child surfaces both `col.name` (string) and a dict
            // slot. Build a mapping from the dict slot back to the
            // string column. The string column already exists on the
            // input so we do not need to rename in `renamed_outputs`.
            mappings.push(DecodeMapping {
                dict_column: dict.to_string(),
                string_column: col.name.clone(),
            });
            wrapped_any = true;
        }
        renamed_outputs.push(col);
    }
    if !wrapped_any {
        return plan;
    }
    ctx.mark_changed();
    LogicalPlan::Decode(DecodeNode {
        input: Box::new(plan),
        mappings,
        output_columns: renamed_outputs,
    })
}

/// Best-effort projection of a logical plan's output columns. Mirrors
/// the small subset of variants Task 7 actually manipulates;
/// downstream-of-decode boundaries do not need it.
fn plan_output_columns(plan: &LogicalPlan) -> Vec<OutputColumn> {
    match plan {
        LogicalPlan::Scan(scan) => scan.columns.clone(),
        LogicalPlan::Aggregate(node) => node.output_columns.clone(),
        LogicalPlan::Window(node) => node.output_columns.clone(),
        LogicalPlan::SubqueryAlias(node) => node.output_columns.clone(),
        LogicalPlan::TableFunction(node) => node.output_columns.clone(),
        LogicalPlan::CTEProduce(node) => node.output_columns.clone(),
        LogicalPlan::CTEConsume(node) => node.output_columns.clone(),
        LogicalPlan::Decode(node) => node.output_columns.clone(),
        LogicalPlan::Filter(node) => plan_output_columns(&node.input),
        LogicalPlan::Project(node) => node
            .items
            .iter()
            .map(|item| OutputColumn {
                column_id: ColumnId::UNSET,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
            })
            .collect(),
        LogicalPlan::Sort(node) => plan_output_columns(&node.input),
        LogicalPlan::Limit(node) => plan_output_columns(&node.input),
        LogicalPlan::Repeat(node) => plan_output_columns(&node.input),
        LogicalPlan::Join(node) => {
            let mut out = plan_output_columns(&node.left);
            out.extend(plan_output_columns(&node.right));
            out
        }
        LogicalPlan::Union(node) => node
            .inputs
            .first()
            .map(plan_output_columns)
            .unwrap_or_default(),
        LogicalPlan::Intersect(node) => node
            .inputs
            .first()
            .map(plan_output_columns)
            .unwrap_or_default(),
        LogicalPlan::Except(node) => node
            .inputs
            .first()
            .map(plan_output_columns)
            .unwrap_or_default(),
        LogicalPlan::Values(node) => node.columns.clone(),
        LogicalPlan::GenerateSeries(node) => vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: node.column_name.clone(),
            data_type: DataType::Int64,
            nullable: false,
        }],
        LogicalPlan::CTEAnchor(node) => plan_output_columns(&node.consumer),
    }
}
