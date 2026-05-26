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
//! * `Project`: passthrough at Task 7 scope, but plain column-alias
//!   items propagate the dict binding under the new name so a
//!   downstream Join / boundary still finds it.
//! * `Limit`: passthrough.
//! * `Join` / `Union` / `Intersect` / `Except` / `Window` /
//!   `TableFunction` / set ops: conservative decode boundary — every
//!   dict column flowing through is decoded back to its string before
//!   the node. `TODO(task-8)` markers flag where Task 8 can refine.
//!
//! The rewriter is idempotent: a `Scan` whose `dict_columns` is
//! already populated is skipped on a second pass.
//!
//! Per-subtree dict visibility lives in `DictScope`, returned alongside
//! the rewritten plan. The rule-global `DictionaryRewriteContext` does
//! NOT carry an output-name -> dict-column map: that map collides when
//! two scans share a column name. See `context.rs`.

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::{
    AggregateNode, DecodeMapping, DecodeNode, FilterNode, LimitNode, LogicalPlan, ProjectNode,
    ScanDictionaryColumn, ScanNode, SortNode,
};

use super::context::{DictBinding, DictScope, DictionaryRewriteContext};
use super::expr::rewrite_column_ref_with_scope;

pub(crate) fn rewrite(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    let (plan, _scope) = rewrite_node(plan, ctx)?;
    Ok(plan)
}

fn rewrite_node(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let (scan, scope) = rewrite_scan(scan, ctx);
            Ok((LogicalPlan::Scan(scan), scope))
        }
        LogicalPlan::Filter(node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            Ok((
                LogicalPlan::Filter(FilterNode {
                    input: Box::new(input),
                    predicate: node.predicate,
                }),
                scope,
            ))
        }
        LogicalPlan::Project(node) => rewrite_project(node, ctx),
        LogicalPlan::Aggregate(node) => rewrite_aggregate(node, ctx),
        LogicalPlan::Sort(node) => rewrite_sort(node, ctx),
        LogicalPlan::Limit(node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            Ok((
                LogicalPlan::Limit(LimitNode {
                    input: Box::new(input),
                    limit: node.limit,
                    offset: node.offset,
                }),
                scope,
            ))
        }
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
            Ok((plan, DictScope::new()))
        }
        // Decode is the rewrite's own output; do not recurse into it
        // again. The decoded output is all strings — no dict scope.
        LogicalPlan::Decode(_) => Ok((plan, DictScope::new())),
    }
}

fn rewrite_scan(mut scan: ScanNode, ctx: &mut DictionaryRewriteContext) -> (ScanNode, DictScope) {
    let mut scope = DictScope::new();
    // Idempotency guard: an already-populated `dict_columns` means a
    // previous application of this rule already rewrote the scan.
    // Rebuild the scope from the existing hints so callers above still
    // see the bindings.
    if !scan.dict_columns.is_empty() {
        for hint in &scan.dict_columns {
            scope.insert(
                hint.source_column.clone(),
                DictBinding {
                    dict_column: hint.dict_column.clone(),
                    snapshot: hint.dictionary.clone(),
                },
            );
        }
        return (scan, scope);
    }
    let eligible = ctx.dict_eligible_columns_for_scan(&scan.database, &scan.table.name);
    if eligible.is_empty() {
        return (scan, scope);
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
            source_column: source_name.clone(),
            dict_column: dict_column.clone(),
            dictionary: snapshot.clone(),
        });
        scope.insert(
            source_name,
            DictBinding {
                dict_column,
                snapshot,
            },
        );
        ctx.mark_changed();
    }
    (scan, scope)
}

fn rewrite_project(
    node: ProjectNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    let (input, input_scope) = rewrite_node(*node.input, ctx)?;
    // Task 7 scope: do not rewrite project items themselves. Projects
    // that re-emit a string column do so by carrying its dict alias up
    // implicitly; the parent boundary (or the final user-facing
    // projection) is what decides whether a Decode is needed.
    // TODO(task-8): rewrite project items so derived dict expressions
    // (`upper(s)`, `concat(s, '!')`, etc.) operate on the dict id.
    //
    // For plain column-alias items (`SELECT s AS t FROM ...`), propagate
    // the dict binding under the alias name so a downstream boundary
    // can still find the dict column to decode.
    let mut output_scope = DictScope::new();
    for item in &node.items {
        if let crate::sql::analysis::ExprKind::ColumnRef { column, .. } = &item.expr.kind
            && let Some(binding) = input_scope.get(column)
        {
            output_scope.insert(item.output_name.clone(), binding.clone());
        }
    }
    Ok((
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: node.items,
        }),
        output_scope,
    ))
}

fn rewrite_aggregate(
    node: AggregateNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    let (input, input_scope) = rewrite_node(*node.input, ctx)?;
    let mut group_by = Vec::with_capacity(node.group_by.len());
    let mut decoded_group_keys: Vec<(
        String,
        String,
        std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>,
    )> = Vec::new();
    for expr in &node.group_by {
        if let crate::sql::analysis::ExprKind::ColumnRef { column, .. } = &expr.kind
            && let Some(binding) = input_scope.get(column)
        {
            group_by.push(rewrite_column_ref_with_scope(expr, &input_scope));
            // The aggregate node was emitting the original string
            // column name to consumers; we must surface that name
            // through a Decode boundary above the aggregate.
            decoded_group_keys.push((
                binding.dict_column.clone(),
                column.clone(),
                binding.snapshot.clone(),
            ));
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
            if let Some(binding) = input_scope.get(&out.name) {
                OutputColumn {
                    column_id: out.column_id,
                    name: binding.dict_column.clone(),
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
        // Aggregate did not consume any dict columns from its input;
        // the aggregate's own output is all strings, so nothing
        // dict-typed is exposed upward.
        return Ok((aggregate, DictScope::new()));
    }

    // Build dict_column -> (string_column, snapshot) so we can restore
    // names and types on the decode's output_columns.
    let mut decoded_index: std::collections::BTreeMap<
        String,
        (
            String,
            std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>,
        ),
    > = std::collections::BTreeMap::new();
    for (dict, string, snap) in &decoded_group_keys {
        decoded_index.insert(dict.clone(), (string.clone(), snap.clone()));
    }
    let mappings: Vec<DecodeMapping> = decoded_group_keys
        .iter()
        .map(|(dict, string, _)| DecodeMapping {
            dict_column: dict.clone(),
            string_column: string.clone(),
        })
        .collect();
    // Restore the original string-column names on the post-decode
    // output_columns so consumers continue to bind to the string.
    for out in output_columns.iter_mut() {
        if let Some((original, snap)) = decoded_index.get(&out.name) {
            out.name = original.clone();
            out.data_type = snap.data_type.clone();
        }
    }
    ctx.mark_changed();
    // Decode is a terminator for dict bindings — its output is all
    // strings, so the returned scope is empty.
    Ok((
        LogicalPlan::Decode(DecodeNode {
            input: Box::new(aggregate),
            mappings,
            output_columns,
        }),
        DictScope::new(),
    ))
}

fn rewrite_sort(
    node: SortNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    let (input, input_scope) = rewrite_node(*node.input, ctx)?;
    // Determine whether all sort keys with dict snapshots are
    // order-preserving. Otherwise insert a Decode before the sort so
    // the sort still operates on strings.
    let mut needs_decode = false;
    let mut sort_items = Vec::with_capacity(node.items.len());
    for item in &node.items {
        if let crate::sql::analysis::ExprKind::ColumnRef { column, .. } = &item.expr.kind
            && let Some(binding) = input_scope.get(column)
        {
            if binding.snapshot.order_preserving {
                let mut rewritten = item.clone();
                rewritten.expr = rewrite_column_ref_with_scope(&item.expr, &input_scope);
                sort_items.push(rewritten);
                ctx.mark_changed();
                continue;
            } else {
                needs_decode = true;
            }
        }
        sort_items.push(item.clone());
    }
    let (input, output_scope) = if needs_decode {
        // Decode below the sort: the sort itself now sees strings and
        // surfaces strings; no dict columns leak upward.
        (wrap_with_decode(input, &input_scope, ctx), DictScope::new())
    } else {
        (input, input_scope)
    };
    Ok((
        LogicalPlan::Sort(SortNode {
            input: Box::new(input),
            items: sort_items,
            analytic_partition_by: node.analytic_partition_by,
        }),
        output_scope,
    ))
}

fn decode_boundary(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    // For nodes Task 7 does not refine, recurse into their children to
    // pick up scan-side dict columns, then wrap each child with a
    // Decode so the node itself never has to know about dict ids.
    let rewritten = rewrite_children_decoded(plan, ctx)?;
    // After wrapping every child with Decode, the parent boundary's
    // own output is all strings — no scope leaks upward.
    Ok((rewritten, DictScope::new()))
}

/// Recurse into each child, then wrap that child with `Decode` using
/// the child's scope. This is the conservative variant the rewriter
/// applies at every node it does not specifically handle (Join, Union,
/// Window, etc.).
fn rewrite_children_decoded(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    match plan {
        LogicalPlan::Join(mut node) => {
            let (left, left_scope) = rewrite_node(*node.left, ctx)?;
            let (right, right_scope) = rewrite_node(*node.right, ctx)?;
            node.left = Box::new(wrap_with_decode(left, &left_scope, ctx));
            node.right = Box::new(wrap_with_decode(right, &right_scope, ctx));
            Ok(LogicalPlan::Join(node))
        }
        LogicalPlan::Union(mut node) => {
            let mut new_inputs = Vec::with_capacity(node.inputs.len());
            for input in node.inputs.drain(..) {
                let (rewritten, scope) = rewrite_node(input, ctx)?;
                new_inputs.push(wrap_with_decode(rewritten, &scope, ctx));
            }
            node.inputs = new_inputs;
            Ok(LogicalPlan::Union(node))
        }
        LogicalPlan::Intersect(mut node) => {
            let mut new_inputs = Vec::with_capacity(node.inputs.len());
            for input in node.inputs.drain(..) {
                let (rewritten, scope) = rewrite_node(input, ctx)?;
                new_inputs.push(wrap_with_decode(rewritten, &scope, ctx));
            }
            node.inputs = new_inputs;
            Ok(LogicalPlan::Intersect(node))
        }
        LogicalPlan::Except(mut node) => {
            let mut new_inputs = Vec::with_capacity(node.inputs.len());
            for input in node.inputs.drain(..) {
                let (rewritten, scope) = rewrite_node(input, ctx)?;
                new_inputs.push(wrap_with_decode(rewritten, &scope, ctx));
            }
            node.inputs = new_inputs;
            Ok(LogicalPlan::Except(node))
        }
        LogicalPlan::Window(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::Window(node))
        }
        LogicalPlan::TableFunction(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::TableFunction(node))
        }
        LogicalPlan::Repeat(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::Repeat(node))
        }
        LogicalPlan::SubqueryAlias(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::SubqueryAlias(node))
        }
        LogicalPlan::CTEAnchor(mut node) => {
            let (produce, produce_scope) = rewrite_node(*node.produce, ctx)?;
            let (consumer, consumer_scope) = rewrite_node(*node.consumer, ctx)?;
            node.produce = Box::new(wrap_with_decode(produce, &produce_scope, ctx));
            node.consumer = Box::new(wrap_with_decode(consumer, &consumer_scope, ctx));
            Ok(LogicalPlan::CTEAnchor(node))
        }
        LogicalPlan::CTEProduce(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::CTEProduce(node))
        }
        other => Ok(other),
    }
}

/// Wrap `plan` with a `Decode` for every dict column in `scope` so the
/// parent operator only sees string columns. No-op when the scope is
/// empty or none of the plan's output columns are dict-encoded.
pub(crate) fn wrap_with_decode(
    plan: LogicalPlan,
    scope: &DictScope,
    ctx: &mut DictionaryRewriteContext,
) -> LogicalPlan {
    if scope.is_empty() {
        return plan;
    }
    // Avoid double-decoding when the plan is already a Decode.
    if matches!(plan, LogicalPlan::Decode(_)) {
        return plan;
    }
    let mut mappings: Vec<DecodeMapping> = Vec::new();
    let mut renamed_outputs: Vec<OutputColumn> = Vec::new();
    let mut wrapped_any = false;
    for col in plan_output_columns(&plan) {
        if let Some(binding) = scope.get(&col.name) {
            mappings.push(DecodeMapping {
                dict_column: binding.dict_column.clone(),
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
