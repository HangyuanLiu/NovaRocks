//! Phase 1 of column pruning: top-down tagging pass.
//!
//! Walks the logical plan tree and writes `required_output_columns:
//! Option<HashSet<ColumnId>>` on every operator node based on what the
//! *parent* operator needs.
//!
//! Semantics:
//! - `parent_needed = None` at the root means "all outputs required".
//! - `Some(set)` means "downstream needs exactly this ColumnId set".
//! - After this pass every node has `Some(_)` so Phase-2 pruning rules
//!   can read a local tag without recursing.
//!
//! This module does **not** prune anything.  Pruning (removing items /
//! output_columns entries) is done in Phase-2 `Prune*Columns` rules.
//!
//! Spec: `docs/design/specs/2026-05-28-oq-1-column-pruning-arch-refactor-design.md` §5.

use std::collections::{HashMap, HashSet};

use crate::sql::analysis::ExprKind;
use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs, collect_output_ids, collect_output_ids_ordered,
};
use crate::sql::planner::plan::{LogicalPlanNode, LogicalPlanNodeKind};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Walk `plan` top-down and stamp `required_output_columns` on every operator.
///
/// `parent_needed = None` means the root has no caller restriction (all outputs
/// required).  Each operator type computes its own child's needed set and
/// recurses.
pub(crate) fn tag_required_columns(
    plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    match &plan.kind {
        LogicalPlanNodeKind::Scan(_) => tag_scan(plan, parent_needed),
        LogicalPlanNodeKind::Values(_) => tag_values(plan, parent_needed),
        LogicalPlanNodeKind::GenerateSeries(_) => tag_generate_series(plan, parent_needed),
        LogicalPlanNodeKind::Project(_) => tag_project(plan, parent_needed),
        LogicalPlanNodeKind::Filter(_) => tag_filter(plan, parent_needed),
        LogicalPlanNodeKind::Sort(_) => tag_sort(plan, parent_needed),
        LogicalPlanNodeKind::Limit(_) => tag_limit(plan, parent_needed),
        LogicalPlanNodeKind::Aggregate(_) => tag_aggregate(plan, parent_needed),
        LogicalPlanNodeKind::Join(_) => tag_join(plan, parent_needed),
        LogicalPlanNodeKind::Union(_) => tag_union(plan, parent_needed),
        LogicalPlanNodeKind::Intersect(_) => tag_intersect(plan, parent_needed),
        LogicalPlanNodeKind::Except(_) => tag_except(plan, parent_needed),
        LogicalPlanNodeKind::CTEAnchor(_) => tag_cte_anchor(plan, parent_needed),
        LogicalPlanNodeKind::CTEConsume(_) => tag_cte_consume(plan, parent_needed),
        LogicalPlanNodeKind::CTEProduce(_) => tag_cte_produce(plan, parent_needed),
        LogicalPlanNodeKind::Window(_) => tag_window(plan, parent_needed),
        LogicalPlanNodeKind::Repeat(_) => tag_repeat(plan, parent_needed),
        LogicalPlanNodeKind::Decode(_) => tag_decode(plan, parent_needed),
        LogicalPlanNodeKind::AggregateStateMerge(_) => {
            tag_aggregate_state_merge(plan, parent_needed)
        }
        LogicalPlanNodeKind::TableFunction(_) => tag_table_function(plan, parent_needed),
        LogicalPlanNodeKind::Apply(_) => tag_apply(plan, parent_needed),
        LogicalPlanNodeKind::AssertOneRow(_) => tag_assert_one_row(plan, parent_needed),
        LogicalPlanNodeKind::ImvDelta(_) | LogicalPlanNodeKind::ImvVersion(_) => {
            panic!("imv marker should not appear in non-IMV column pruning")
        }
    }
}

fn tag_aggregate_state_merge(
    mut plan: LogicalPlanNode,
    _parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(
        plan.kind,
        LogicalPlanNodeKind::AggregateStateMerge(_)
    ));
    let (old_input, delta_input) = plan.take_two_children();
    plan.children = vec![
        tag_required_columns(old_input, None),
        tag_required_columns(delta_input, None),
    ];
    plan
}

/// Apply is eliminated by the SubqueryRewrite stage, which runs before column
/// pruning, so pruning never sees it in production plans. Tag conservatively:
/// require everything below, prune nothing.
fn tag_apply(
    mut plan: LogicalPlanNode,
    _parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::Apply(_)));
    plan.required_output_columns = None;
    let (left, right) = plan.take_two_children();
    plan.children = vec![
        tag_required_columns(left, None),
        tag_required_columns(right, None),
    ];
    plan
}

/// Conservative: no pruning through AssertOneRow in M0. Tighten when M1
/// starts producing this node in real plans.
fn tag_assert_one_row(
    mut plan: LogicalPlanNode,
    _parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::AssertOneRow(_)));
    plan.required_output_columns = None;
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, None)];
    plan
}

// ---------------------------------------------------------------------------
// Leaf handlers
// ---------------------------------------------------------------------------

fn tag_scan(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Scan(scan) = &plan.kind else {
        unreachable!()
    };
    let needed =
        parent_needed.unwrap_or_else(|| scan.columns.iter().map(|c| c.column_id).collect());
    plan.required_output_columns = Some(needed);
    plan
}

fn tag_values(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Values(node) = &plan.kind else {
        unreachable!()
    };
    let needed =
        parent_needed.unwrap_or_else(|| node.columns.iter().map(|c| c.column_id).collect());
    plan.required_output_columns = Some(needed);
    plan
}

/// GenerateSeries is a leaf with one output ColumnId.  Like Scan/Values, a
/// `None` parent means all leaf outputs are required.
fn tag_generate_series(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::GenerateSeries(node) = &plan.kind else {
        unreachable!()
    };
    let needed = parent_needed.unwrap_or_else(|| {
        if node.output_column_id == ColumnId::UNSET {
            HashSet::new()
        } else {
            HashSet::from([node.output_column_id])
        }
    });
    plan.required_output_columns = Some(needed);
    plan
}

// ---------------------------------------------------------------------------
// Unary handlers
// ---------------------------------------------------------------------------

fn tag_project(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Project(node) = &plan.kind else {
        unreachable!()
    };
    plan.required_output_columns = parent_needed.clone();
    // child_needed = union of ColumnRefs of items whose output_column_id is in
    // parent_needed (or all items when parent_needed is None).
    //
    // assert_true items are ALWAYS included in child_needed regardless of
    // parent_needed: they carry runtime correctness checks (e.g. the per-group
    // row-check from ScalarApplyToJoin) whose column refs (e.g. the count
    // column from the grouping aggregate) must remain available to the child.
    // This mirrors the StarRocks PruneProjectColumnsRule carve-out.
    let child_needed: HashSet<ColumnId> = node
        .items
        .iter()
        .filter(|item| match &parent_needed {
            None => true,
            Some(n) => {
                n.contains(&item.output_column_id)
                    || matches!(
                        &item.expr.kind,
                        ExprKind::FunctionCall { name, .. } if name == "assert_true"
                    )
            }
        })
        .flat_map(|item| collect_column_id_refs(&item.expr))
        .collect();
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, Some(child_needed))];
    plan
}

fn tag_filter(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Filter(node) = &plan.kind else {
        unreachable!()
    };
    plan.required_output_columns = parent_needed.clone();
    // Child needs everything the parent needs PLUS all columns referenced in
    // the predicate.  When parent_needed is None (keep all), propagate None so
    // the child also keeps all columns instead of collapsing to just the
    // predicate refs.
    let child_needed = parent_needed.map(|mut needed| {
        needed.extend(collect_column_id_refs(&node.predicate));
        needed
    });
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, child_needed)];
    plan
}

fn tag_sort(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Sort(node) = &plan.kind else {
        unreachable!()
    };
    plan.required_output_columns = parent_needed.clone();
    // When parent_needed is None (keep all), propagate None so the child also
    // keeps all columns instead of collapsing to just the sort-key refs.
    let child_needed = parent_needed.map(|mut needed| {
        for item in &node.items {
            needed.extend(collect_column_id_refs(&item.expr));
        }
        for expr in &node.analytic_partition_by {
            needed.extend(collect_column_id_refs(expr));
        }
        needed
    });
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, child_needed)];
    plan
}

fn tag_limit(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::Limit(_)));
    plan.required_output_columns = parent_needed.clone();
    // Limit is transparent: passes parent_needed straight through.
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, parent_needed)];
    plan
}

fn tag_aggregate(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Aggregate(node) = &plan.kind else {
        unreachable!()
    };
    plan.required_output_columns = parent_needed.clone();

    // Conservative keep-all-aggregate-inputs strategy.
    //
    // Aggregate output metadata starts with the group-by output prefix used by
    // the physical layout, while aggregate function identity lives on
    // AggregateCall.output_column_id.  Required input derivation should not
    // infer liveness from output positions; if the aggregate node is live at
    // all, every expression it consumes remains needed.
    //
    // Conservative fix: child always needs ALL group-by column refs PLUS ALL
    // aggregate args and order-by column refs, regardless of parent_needed.
    // This matches the semantics of the old name-based PruneColumns pass and
    // is correct: if the aggregate node is live at all, every input column it
    // consumes is required.  Per-aggregate output pruning (Gap 5) is a
    // follow-up that requires an explicit output_column_id on AggregateCall.
    //
    // None-propagation discipline: when parent_needed is None (root / keep-all),
    // pass None to the child so it also keeps all its columns.
    let child_needed: Option<HashSet<ColumnId>> = parent_needed.map(|_| {
        let mut needed: HashSet<ColumnId> = HashSet::new();
        for gb in &node.group_by {
            needed.extend(collect_column_id_refs(gb));
        }
        for agg in &node.aggregates {
            for arg in &agg.args {
                needed.extend(collect_column_id_refs(arg));
            }
            for item in &agg.order_by {
                needed.extend(collect_column_id_refs(&item.expr));
            }
        }
        needed
    });

    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, child_needed)];
    plan
}

fn tag_window(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::Window(_)));
    plan.required_output_columns = parent_needed;
    // Window output columns carry fresh ColumnIds (allocated by the planner)
    // that are distinct from the child's ids, so we cannot reliably map
    // parent_needed back to child column ids.  Pass None to the child so all
    // input columns are preserved and no column is spuriously dropped.
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, None)];
    plan
}

fn tag_repeat(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Repeat(node) = &plan.kind else {
        unreachable!()
    };
    plan.required_output_columns = parent_needed.clone();
    let child_needed = if parent_needed.is_none() {
        None
    } else if node.all_rollup_column_ids.len() == node.all_rollup_columns.len() {
        let grouping_output_ids: HashSet<ColumnId> = node
            .grouping_fn_ids
            .iter()
            .map(|(_, column_id)| *column_id)
            .collect();
        let mut needed = parent_needed.unwrap_or_default();
        needed.retain(|column_id| !grouping_output_ids.contains(column_id));
        needed.extend(node.all_rollup_column_ids.iter().copied());
        Some(needed)
    } else {
        None
    };
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, child_needed)];
    plan
}

/// Decode node translates `string_column` references (in parent_needed) to
/// `dict_column` references for the child.
///
/// `DecodeMapping` uses String names, not ColumnIds.  We look up which
/// output column in `node.output_columns` carries the `string_column` name
/// to find the ColumnId the parent is referencing, then substitute
/// the corresponding column id that the child exposes under `dict_column`.
///
/// If a parent-needed id does NOT correspond to any mapping's string_column,
/// it is passed through unchanged (the child still produces it).
/// Decode node: for ColumnId-based needed sets, the pass-through is
/// transparent.
///
/// Why: `DecodeMapping` uses String names (`dict_column` / `string_column`),
/// but the rewriter that inserts `Decode` keeps the **same `ColumnId`** on
/// both the child's dict-column output and the Decode node's string-column
/// output (see `low_cardinality_dict/rewriter.rs:209`).  So:
///
///   - Parent references string_column with ColumnId X.
///   - Decode.output_columns carries column_id=X, name=string_column.
///   - Child produces the same ColumnId X under name dict_column.
///
/// Therefore no id translation is needed; parent_needed can be passed to
/// the child unchanged.
fn tag_decode(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::Decode(_)));
    plan.required_output_columns = parent_needed.clone();
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, parent_needed)];
    plan
}

fn tag_table_function(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::TableFunction(_)));
    plan.required_output_columns = parent_needed;
    // The function's args reference INPUT columns that may not appear in
    // parent_needed (e.g. UNNEST(t.arr) where parent only sees the exploded
    // output).  Pass None to the child so no input column is spuriously dropped.
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, None)];
    plan
}

// ---------------------------------------------------------------------------
// Binary / n-ary handlers
// ---------------------------------------------------------------------------

fn tag_join(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Join(node) = &plan.kind else {
        unreachable!()
    };
    plan.required_output_columns = parent_needed.clone();

    // When parent_needed is None (keep all), propagate None to both children so
    // they also keep all columns.  When Some, compute combined = parent_needed ∪
    // condition refs, then split by which child produces each id.
    let (left_needed, right_needed) = match parent_needed {
        None => (None, None),
        Some(mut combined) => {
            if let Some(cond) = &node.condition {
                combined.extend(collect_column_id_refs(cond));
            }
            let left_outputs = collect_output_ids(plan.left());
            let right_outputs = collect_output_ids(plan.right());
            let left: HashSet<ColumnId> = combined
                .iter()
                .filter(|id| left_outputs.contains(id))
                .copied()
                .collect();
            let right: HashSet<ColumnId> = combined
                .iter()
                .filter(|id| right_outputs.contains(id))
                .copied()
                .collect();
            (Some(left), Some(right))
        }
    };

    let (left, right) = plan.take_two_children();
    plan.children = vec![
        tag_required_columns(left, left_needed),
        tag_required_columns(right, right_needed),
    ];
    plan
}

// ---------------------------------------------------------------------------
// Set operation handlers (Gap 4)
// ---------------------------------------------------------------------------

fn tag_union(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::Union(node) = &plan.kind else {
        unreachable!()
    };

    if !node.all {
        plan.required_output_columns = parent_needed;
        plan.children = plan
            .children
            .into_iter()
            .map(|child| tag_required_columns(child, None))
            .collect();
        return plan;
    }

    // Resolve which positions in the output schema are needed.
    let outputs: Vec<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    let needed_positions: Vec<usize> = match &parent_needed {
        None => (0..outputs.len()).collect(),
        Some(n) => outputs
            .iter()
            .enumerate()
            .filter_map(|(i, id)| n.contains(id).then_some(i))
            .collect(),
    };

    plan.required_output_columns = parent_needed;
    plan.children = plan
        .children
        .into_iter()
        .map(|child| {
            let child_outputs = collect_output_ids_ordered(&child);
            let child_needed: HashSet<ColumnId> = needed_positions
                .iter()
                .filter_map(|&i| child_outputs.get(i).copied())
                .collect();
            tag_required_columns(child, Some(child_needed))
        })
        .collect();
    plan
}

fn tag_intersect(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::Intersect(_)));

    plan.required_output_columns = parent_needed;
    plan.children = plan
        .children
        .into_iter()
        .map(|child| tag_required_columns(child, None))
        .collect();
    plan
}

fn tag_except(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::Except(_)));

    plan.required_output_columns = parent_needed;
    plan.children = plan
        .children
        .into_iter()
        .map(|child| tag_required_columns(child, None))
        .collect();
    plan
}

// ---------------------------------------------------------------------------
// CTE handlers (Gap 3 — two-walk pattern)
// ---------------------------------------------------------------------------

fn tag_cte_consume(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    let LogicalPlanNodeKind::CTEConsume(node) = &plan.kind else {
        unreachable!()
    };
    // Leaf in this walk — always store Some(_) so that subtree_untagged
    // returns false after tagging.  When parent_needed is None (no restriction
    // from above), default to keeping all of this node's own output ids, which
    // is the correct "keep-all" signal for the CTE two-walk.
    plan.required_output_columns = Some(
        parent_needed.unwrap_or_else(|| node.output_columns.iter().map(|c| c.column_id).collect()),
    );
    plan
}

fn tag_cte_produce(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::CTEProduce(_)));
    plan.required_output_columns = parent_needed.clone();
    // The produce-side needed ids are already in the producer's output id
    // space (translate_consume_to_produce_ids mapped them).  Pass them
    // straight through to the CTE body.
    let input = plan.take_single_child();
    plan.children = vec![tag_required_columns(input, parent_needed)];
    plan
}

fn tag_cte_anchor(
    mut plan: LogicalPlanNode,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlanNode {
    assert!(matches!(plan.kind, LogicalPlanNodeKind::CTEAnchor(_)));

    // --- Walk 1: tag the consumer subtree with parent_needed. ---
    // This stamps required_output_columns on every CTEConsume for this cte_id.
    let produce = plan.take_child(0);
    let consumer = plan.take_child(0);
    let consumer = tag_required_columns(consumer, parent_needed.clone());

    // --- Tag the producer subtree with None (keep all). ---
    //
    // Conservative choice: pass None to the CTEProduce body so all produce
    // columns survive, rather than computing a narrowed produce_needed set.
    //
    // Why conservative: PruneCTEProduceColumns and PruneCTEConsumeColumns are
    // no-ops (Gap-3 deferred).  The CTE multicast protocol sends ALL produce
    // columns to every consumer exchange node; each consumer reads them by
    // positional index.  A narrowed produce_needed set could prune leaf nodes
    // (e.g. Scan) below the produce correctly in isolation, but since the
    // produce's output_columns list is not pruned (no-op rule), the scan must
    // still produce ALL columns that the produce's output_columns list names.
    // Passing None ensures the scan's required_output_columns == all columns,
    // which is the safe invariant until Gap-3 is implemented.
    let produce = tag_required_columns(produce, None);

    plan.children = vec![produce, consumer];
    plan.required_output_columns = parent_needed;
    plan
}

// ---------------------------------------------------------------------------
// CTE helpers
// ---------------------------------------------------------------------------

/// Recursively traverse `plan` and union all `required_output_columns` sets
/// from `CTEConsume` nodes whose `cte_id` matches `target_id` into `acc`.
fn collect_cte_consumer_needs(
    plan: &LogicalPlanNode,
    target_id: CteId,
    acc: &mut HashSet<ColumnId>,
) {
    match &plan.kind {
        LogicalPlanNodeKind::CTEConsume(c) if c.cte_id == target_id => {
            if let Some(req) = &plan.required_output_columns {
                acc.extend(req.iter().copied());
            }
            // A CTEConsume is a leaf; do not recurse further.
        }
        LogicalPlanNodeKind::CTEConsume(_) => {
            // Different cte_id — skip.
        }
        LogicalPlanNodeKind::Scan(_)
        | LogicalPlanNodeKind::Values(_)
        | LogicalPlanNodeKind::GenerateSeries(_) => {}
        LogicalPlanNodeKind::Filter(_)
        | LogicalPlanNodeKind::Project(_)
        | LogicalPlanNodeKind::Aggregate(_)
        | LogicalPlanNodeKind::Sort(_)
        | LogicalPlanNodeKind::Limit(_)
        | LogicalPlanNodeKind::Window(_)
        | LogicalPlanNodeKind::TableFunction(_)
        | LogicalPlanNodeKind::Repeat(_)
        | LogicalPlanNodeKind::Decode(_)
        | LogicalPlanNodeKind::CTEProduce(_)
        | LogicalPlanNodeKind::AssertOneRow(_)
        | LogicalPlanNodeKind::ImvDelta(_)
        | LogicalPlanNodeKind::ImvVersion(_) => {
            for child in &plan.children {
                collect_cte_consumer_needs(child, target_id, acc);
            }
        }
        LogicalPlanNodeKind::Join(_)
        | LogicalPlanNodeKind::Union(_)
        | LogicalPlanNodeKind::Intersect(_)
        | LogicalPlanNodeKind::Except(_)
        | LogicalPlanNodeKind::AggregateStateMerge(_)
        | LogicalPlanNodeKind::Apply(_)
        | LogicalPlanNodeKind::CTEAnchor(_) => {
            for child in &plan.children {
                collect_cte_consumer_needs(child, target_id, acc);
            }
        }
    }
}

/// Build a map from `consume_side_column_id` → `position` for the first
/// matching `CTEConsume(target_id)` found in the subtree.
///
/// All consumers with the same `cte_id` share the same positional schema, so
/// we stop at the first match.  The position is the index into
/// `CTEConsume.output_columns`, which aligns with `CTEProduce.output_columns`.
fn find_consume_position_map(plan: &LogicalPlanNode, target_id: CteId) -> HashMap<ColumnId, usize> {
    let mut map = HashMap::new();
    walk_consume_position_map(plan, target_id, &mut map);
    map
}

fn walk_consume_position_map(
    plan: &LogicalPlanNode,
    target_id: CteId,
    map: &mut HashMap<ColumnId, usize>,
) {
    match &plan.kind {
        LogicalPlanNodeKind::CTEConsume(c) if c.cte_id == target_id => {
            // Record consume_side_column_id -> position for each output column.
            // Use `or_insert` so that if multiple consumers exist (multi-consumer
            // case), the first one wins — positions are identical across all
            // consumers with the same cte_id.
            for (i, col) in c.output_columns.iter().enumerate() {
                map.entry(col.column_id).or_insert(i);
            }
        }
        LogicalPlanNodeKind::CTEConsume(_)
        | LogicalPlanNodeKind::Scan(_)
        | LogicalPlanNodeKind::Values(_)
        | LogicalPlanNodeKind::GenerateSeries(_) => {}
        _ => {
            for child in &plan.children {
                walk_consume_position_map(child, target_id, map);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TagRequiredColumns rewrite rule
// ---------------------------------------------------------------------------

/// Returns `true` when the plan tree rooted at `plan` has not yet been tagged
/// by the Phase-1 tagging pass.
///
/// **Why we check first-child rather than the root node itself**:
/// `tag_required_columns(root, None)` stores `parent_needed = None` on the
/// root operator (semantics: "all outputs required, no restriction from the
/// parent"), but it ALWAYS stores `Some(_)` on every *leaf* node (Scan,
/// Values, GenerateSeries).  Non-leaf nodes at the root that received
/// `parent_needed = None` therefore still carry `required_output_columns = None`
/// after being tagged.  Using the root's own field as the guard would cause
/// the rule to re-fire on every fixed-point iteration.
///
/// The fix: for leaf nodes, check the node's own field (leaves always get
/// `Some(_)` after tagging).  For non-leaf nodes, check the first child's
/// field recursively — after tagging, the deepest leaf will have `Some(_)`.
///
/// `ImvDelta` / `ImvVersion` lack `required_output_columns` and must not
/// be subject to column pruning.  Return `false` so the rule never fires.
fn subtree_untagged(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        // Leaves: always get `Some(_)` after tagging.
        LogicalPlanNodeKind::Scan(_)
        | LogicalPlanNodeKind::Values(_)
        | LogicalPlanNodeKind::GenerateSeries(_)
        | LogicalPlanNodeKind::CTEConsume(_) => plan.required_output_columns.is_none(),
        // Non-leaves: check the first child (which will itself be a leaf or
        // recurse further until a leaf is reached).
        LogicalPlanNodeKind::Filter(_)
        | LogicalPlanNodeKind::Project(_)
        | LogicalPlanNodeKind::Aggregate(_)
        | LogicalPlanNodeKind::Join(_)
        | LogicalPlanNodeKind::Sort(_)
        | LogicalPlanNodeKind::Limit(_)
        | LogicalPlanNodeKind::Window(_)
        | LogicalPlanNodeKind::Repeat(_)
        | LogicalPlanNodeKind::CTEAnchor(_)
        | LogicalPlanNodeKind::CTEProduce(_)
        | LogicalPlanNodeKind::Decode(_)
        | LogicalPlanNodeKind::AggregateStateMerge(_)
        | LogicalPlanNodeKind::Apply(_)
        | LogicalPlanNodeKind::AssertOneRow(_)
        | LogicalPlanNodeKind::TableFunction(_)
        | LogicalPlanNodeKind::Union(_)
        | LogicalPlanNodeKind::Intersect(_)
        | LogicalPlanNodeKind::Except(_) => plan
            .children
            .first()
            .map_or(false, |child| subtree_untagged(child)),
        // ImvDelta and ImvVersion are not subject to column pruning.
        LogicalPlanNodeKind::ImvDelta(_) | LogicalPlanNodeKind::ImvVersion(_) => false,
    }
}

/// Phase-1 tagging rule: walks the plan top-down via [`tag_required_columns`]
/// and stamps `required_output_columns` on every operator node.
///
/// The rule fires once per subtree: `matches` uses [`subtree_untagged`] which
/// checks the first reachable leaf rather than the root node itself.  This is
/// necessary because `tag_required_columns(root, None)` stores `None` on the
/// root (semantics: "no parent restriction"), but always stores `Some(_)` on
/// leaf nodes.  After `apply` returns, all leaves carry `Some(_)`, so
/// `subtree_untagged` returns `false` and the rule does not re-fire.
///
/// TopDown driver post-`apply` child walk: after the root fires and tags the
/// whole tree, `rewrite_children` recurses into already-tagged children.
/// `matches` returns `false` for each (their leaves are `Some(_)`), so no
/// re-tagging occurs.
///
/// The pipeline's fixed-point loop re-runs the stage; on the second pass
/// `subtree_untagged == false` everywhere, `phase_changed == false`, and the
/// loop exits cleanly.
///
/// **No behavior change**: this pass only writes metadata.  Nothing reads
/// `required_output_columns` until the per-operator prune rules are registered
/// in a later task.
pub(crate) struct TagRequiredColumns;

impl LogicalRewriteRule for TagRequiredColumns {
    fn name(&self) -> &'static str {
        "TagRequiredColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
        subtree_untagged(plan)
    }

    fn apply(
        &self,
        plan: LogicalPlanNode,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        let tagged = tag_required_columns(plan, None);
        Ok(RewriteResult::Changed(tagged))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalCTEAnchorNode, LogicalCTEConsumeNode,
        LogicalCTEProduceNode, LogicalExceptNode, LogicalFilterNode, LogicalIntersectNode,
        LogicalJoinNode, LogicalLimitNode, LogicalPlanNodeKind, LogicalProjectNode,
        LogicalScanNode, LogicalSortNode, LogicalUnionNode, LogicalValuesNode, LogicalWindowNode,
        WindowExpr,
    };
    use arrow::datatypes::DataType;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref_expr(id: ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: format!("c{}", id.0),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn int_literal(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn make_scan_with_ids(id_a: u32, id_b: u32, id_c: u32) -> LogicalPlanNode {
        let table = TableDef {
            name: "t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "b".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "c".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "d".to_string(),
                table: table,
                alias: None,
                columns: vec![
                    make_output_column(ColumnId::new_for_test(id_a), "a"),
                    make_output_column(ColumnId::new_for_test(id_b), "b"),
                    make_output_column(ColumnId::new_for_test(id_c), "c"),
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
            }),
            vec![],
            None,
        )
    }

    fn scan_with_3_cols() -> LogicalPlanNode {
        make_scan_with_ids(1, 2, 3)
    }

    fn needed_set(ids: &[u32]) -> HashSet<ColumnId> {
        ids.iter().map(|&id| ColumnId::new_for_test(id)).collect()
    }

    fn required_columns(plan: &LogicalPlanNode) -> &HashSet<ColumnId> {
        plan.required_output_columns
            .as_ref()
            .expect("expected required_output_columns to be tagged")
    }

    fn scan_required_columns(plan: &LogicalPlanNode) -> &HashSet<ColumnId> {
        assert!(
            matches!(&plan.kind, LogicalPlanNodeKind::Scan(_)),
            "expected Scan node, got {:?}",
            plan.kind
        );
        required_columns(plan)
    }

    // -----------------------------------------------------------------------
    // Scan tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_scan_with_none_keeps_all_cols() {
        let tagged = tag_required_columns(scan_with_3_cols(), None);
        let LogicalPlanNodeKind::Scan(_) = &tagged.kind else {
            panic!()
        };
        let req = required_columns(&tagged);
        assert_eq!(req.len(), 3);
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_scan_with_subset_keeps_only_those() {
        let subset = needed_set(&[2]);
        let tagged = tag_required_columns(scan_with_3_cols(), Some(subset.clone()));
        let LogicalPlanNodeKind::Scan(_) = &tagged.kind else {
            panic!()
        };
        assert_eq!(required_columns(&tagged), &subset);
    }

    // -----------------------------------------------------------------------
    // Project tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_project_filters_child_needed_by_output_column_id() {
        // Project[a→101, b→102] <- Scan[a@1, b@2, c@3]
        // parent_needed = {102 (b)}
        // Expected: scan.required_output_columns = {2}  (only b from scan)
        let project = LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    ProjectItem {
                        output_column_id: ColumnId::new_for_test(101),
                        output_name: "a".to_string(),
                        expr: col_ref_expr(ColumnId::new_for_test(1)),
                    },
                    ProjectItem {
                        output_column_id: ColumnId::new_for_test(102),
                        output_name: "b".to_string(),
                        expr: col_ref_expr(ColumnId::new_for_test(2)),
                    },
                ],
                output_qualifier: None,
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let needed = needed_set(&[102]);
        let tagged = tag_required_columns(project, Some(needed.clone()));

        let LogicalPlanNodeKind::Project(_) = &tagged.kind else {
            panic!()
        };
        assert_eq!(tagged.required_output_columns.as_ref().unwrap(), &needed);

        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        let scan_req = required_columns(input);
        assert!(
            scan_req.contains(&ColumnId::new_for_test(2)),
            "scan should keep b"
        );
        assert!(
            !scan_req.contains(&ColumnId::new_for_test(1)),
            "scan should NOT keep a"
        );
    }

    #[test]
    fn tag_project_with_none_parent_includes_all_item_refs() {
        // parent_needed=None: child_needed = union of all items' column refs
        let project = LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    ProjectItem {
                        output_column_id: ColumnId::new_for_test(101),
                        output_name: "a".to_string(),
                        expr: col_ref_expr(ColumnId::new_for_test(1)),
                    },
                    ProjectItem {
                        output_column_id: ColumnId::new_for_test(102),
                        output_name: "b".to_string(),
                        expr: col_ref_expr(ColumnId::new_for_test(2)),
                    },
                ],
                output_qualifier: None,
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(project, None);
        let LogicalPlanNodeKind::Project(_) = &tagged.kind else {
            panic!()
        };
        // required_output_columns should be None (transparent)
        assert!(tagged.required_output_columns.is_none());
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        let scan_req = required_columns(input);
        // Both a(1) and b(2) referenced; c(3) not in any item expr
        assert!(scan_req.contains(&ColumnId::new_for_test(1)));
        assert!(scan_req.contains(&ColumnId::new_for_test(2)));
        assert!(!scan_req.contains(&ColumnId::new_for_test(3)));
    }

    // -----------------------------------------------------------------------
    // Filter test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_filter_adds_predicate_cols_to_child_needed() {
        // Filter(c@3 > 0) <- Scan[a@1, b@2, c@3]
        // parent_needed = {1}
        // Expected: child_needed = {1, 3}
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_ref_expr(ColumnId::new_for_test(3))),
                        op: BinOp::Gt,
                        right: Box::new(int_literal(0)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(filter, Some(needed_set(&[1])));
        let LogicalPlanNodeKind::Filter(_) = &tagged.kind else {
            panic!()
        };
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        let req = required_columns(input);
        assert!(
            req.contains(&ColumnId::new_for_test(1)),
            "a needed by parent"
        );
        assert!(
            req.contains(&ColumnId::new_for_test(3)),
            "c needed by predicate"
        );
        assert!(!req.contains(&ColumnId::new_for_test(2)), "b not needed");
    }

    // -----------------------------------------------------------------------
    // Aggregate test
    // -----------------------------------------------------------------------

    /// Bug A regression: tag_aggregate must use the conservative keep-all
    /// strategy for aggregate args.
    ///
    /// Required input derivation must not use output positions to decide which
    /// aggregate calls are live. Aggregate call identity is
    /// `AggregateCall.output_column_id`, and this pass conservatively keeps
    /// every aggregate input while the aggregate node itself is live.
    ///
    /// Conservative fix: child_needed always includes ALL group_by column refs
    /// PLUS ALL aggregate args and order_by column refs, regardless of
    /// parent_needed.  This matches the semantics of the old PruneColumns pass
    /// and prevents input columns from being spuriously dropped by PruneScan.
    #[test]
    fn tag_aggregate_conservative_keeps_all_aggregate_args_in_child_needed() {
        // Aggregate[group_by=[y@1], count(*)→301, sum(x@2)→302]
        // parent_needed = {301}  (only count needed)
        //
        // Expected (conservative fix):
        //   child_needed = {1, 10}  (group_by y@1 + ALL aggregate args: x@10)
        //   c@3 (not referenced by any group_by or agg arg) is NOT needed.
        //
        let agg = LogicalPlanNode::new(
            LogicalPlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref_expr(ColumnId::new_for_test(1))],
                aggregates: vec![
                    AggregateCall {
                        name: "count".to_string(),
                        args: vec![],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: ColumnId::UNSET,
                    },
                    AggregateCall {
                        name: "sum".to_string(),
                        args: vec![col_ref_expr(ColumnId::new_for_test(2))],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: ColumnId::UNSET,
                    },
                ],
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1), "y"),
                    make_output_column(ColumnId::new_for_test(301), "count"),
                    make_output_column(ColumnId::new_for_test(302), "sum_x"),
                ],
                already_pushed: false,
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(agg, Some(needed_set(&[301])));
        let LogicalPlanNodeKind::Aggregate(_) = &tagged.kind else {
            panic!()
        };
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        let req = required_columns(input);
        // group_by y@1 must always be in child_needed.
        assert!(req.contains(&ColumnId::new_for_test(1)), "group_by y@1");
        // sum(x) arg x@2 must be in child_needed even though parent only needs count.
        assert!(
            req.contains(&ColumnId::new_for_test(2)),
            "sum(x@2) arg must be kept (conservative keep-all)"
        );
        // c@3 is not referenced by group_by or any agg arg — may be absent.
        // (We do not assert it is absent; correctness only requires the above.)
    }

    /// tag_aggregate with parent_needed=None propagates None to the child
    /// (None-propagation discipline — child keeps all its columns).
    #[test]
    fn tag_aggregate_none_parent_propagates_none_to_child() {
        let agg = LogicalPlanNode::new(
            LogicalPlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref_expr(ColumnId::new_for_test(1))],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref_expr(ColumnId::new_for_test(2))],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                output_columns: vec![make_output_column(ColumnId::new_for_test(301), "sum_x")],
                already_pushed: false,
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(agg, None);
        let LogicalPlanNodeKind::Aggregate(_) = &tagged.kind else {
            panic!()
        };
        // Aggregate receives None → keeps None on itself.
        assert!(tagged.required_output_columns.is_none());
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        // Child got None → Scan expands to all columns.
        let req = required_columns(input);
        assert_eq!(req.len(), 3, "scan keeps all 3 columns");
    }

    // -----------------------------------------------------------------------
    // Join test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_join_splits_needed_by_child_outputs_and_adds_condition_cols() {
        // Join[INNER, on a@1=d@4] <- {Scan_l[a@1,b@2,c@3], Scan_r[d@4,e@5,f@6]}
        // parent_needed = {2, 6}
        // Expected:
        //   left_needed  = {1, 2}  (join cond a + parent b)
        //   right_needed = {4, 6}  (join cond d + parent f)
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_ref_expr(ColumnId::new_for_test(1))),
                        op: BinOp::Eq,
                        right: Box::new(col_ref_expr(ColumnId::new_for_test(4))),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
            }),
            vec![make_scan_with_ids(1, 2, 3), make_scan_with_ids(4, 5, 6)],
            None,
        );
        let tagged = tag_required_columns(join, Some(needed_set(&[2, 6])));
        let LogicalPlanNodeKind::Join(_) = &tagged.kind else {
            panic!()
        };
        let left = tagged.left();
        let LogicalPlanNodeKind::Scan(_) = &left.kind else {
            panic!()
        };
        let right = tagged.right();
        let LogicalPlanNodeKind::Scan(_) = &right.kind else {
            panic!()
        };
        let lreq = required_columns(left);
        let rreq = required_columns(right);
        assert_eq!(lreq.len(), 2);
        assert!(lreq.contains(&ColumnId::new_for_test(1)));
        assert!(lreq.contains(&ColumnId::new_for_test(2)));
        assert_eq!(rreq.len(), 2);
        assert!(rreq.contains(&ColumnId::new_for_test(4)));
        assert!(rreq.contains(&ColumnId::new_for_test(6)));
    }

    // -----------------------------------------------------------------------
    // Union position-aligned test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_union_position_aligned_propagation() {
        // Union[output: x@1001, y@1002, z@1003]
        //   <- Scan_a[a@1, b@2, c@3]
        //   <- Scan_b[d@4, e@5, f@6]
        // parent_needed = {1002}  (position 1 = y)
        // Expected:
        //   Scan_a: {2}  (position 1 = b@2)
        //   Scan_b: {5}  (position 1 = e@5)
        let union = LogicalPlanNode::new(
            LogicalPlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1001), "x"),
                    make_output_column(ColumnId::new_for_test(1002), "y"),
                    make_output_column(ColumnId::new_for_test(1003), "z"),
                ],
            }),
            vec![make_scan_with_ids(1, 2, 3), make_scan_with_ids(4, 5, 6)],
            None,
        );
        let tagged = tag_required_columns(union, Some(needed_set(&[1002])));
        let LogicalPlanNodeKind::Union(_) = &tagged.kind else {
            panic!()
        };
        let a_req = scan_required_columns(tagged.child(0));
        let b_req = scan_required_columns(tagged.child(1));
        assert_eq!(a_req.len(), 1);
        assert!(
            a_req.contains(&ColumnId::new_for_test(2)),
            "position 1 = b@2"
        );
        assert_eq!(b_req.len(), 1);
        assert!(
            b_req.contains(&ColumnId::new_for_test(5)),
            "position 1 = e@5"
        );
    }

    #[test]
    fn tag_union_distinct_preserves_all_child_columns() {
        let union = LogicalPlanNode::new(
            LogicalPlanNodeKind::Union(LogicalUnionNode {
                all: false,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1001), "x"),
                    make_output_column(ColumnId::new_for_test(1002), "y"),
                    make_output_column(ColumnId::new_for_test(1003), "z"),
                ],
            }),
            vec![make_scan_with_ids(1, 2, 3), make_scan_with_ids(4, 5, 6)],
            None,
        );
        let tagged = tag_required_columns(union, Some(needed_set(&[1002])));
        let LogicalPlanNodeKind::Union(_) = &tagged.kind else {
            panic!()
        };
        let a_req = scan_required_columns(tagged.child(0));
        let b_req = scan_required_columns(tagged.child(1));
        assert_eq!(a_req.len(), 3);
        assert_eq!(b_req.len(), 3);
        for id in [1, 2, 3] {
            assert!(a_req.contains(&ColumnId::new_for_test(id)));
        }
        for id in [4, 5, 6] {
            assert!(b_req.contains(&ColumnId::new_for_test(id)));
        }
    }

    #[test]
    fn tag_intersect_preserves_all_child_columns() {
        let intersect = LogicalPlanNode::new(
            LogicalPlanNodeKind::Intersect(LogicalIntersectNode {
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1001), "x"),
                    make_output_column(ColumnId::new_for_test(1002), "y"),
                    make_output_column(ColumnId::new_for_test(1003), "z"),
                ],
            }),
            vec![make_scan_with_ids(1, 2, 3), make_scan_with_ids(4, 5, 6)],
            None,
        );
        let tagged = tag_required_columns(intersect, Some(needed_set(&[1002])));
        let LogicalPlanNodeKind::Intersect(_) = &tagged.kind else {
            panic!()
        };
        assert_eq!(scan_required_columns(tagged.child(0)).len(), 3);
        assert_eq!(scan_required_columns(tagged.child(1)).len(), 3);
    }

    #[test]
    fn tag_except_preserves_all_child_columns() {
        let except = LogicalPlanNode::new(
            LogicalPlanNodeKind::Except(LogicalExceptNode {
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1001), "x"),
                    make_output_column(ColumnId::new_for_test(1002), "y"),
                    make_output_column(ColumnId::new_for_test(1003), "z"),
                ],
            }),
            vec![make_scan_with_ids(1, 2, 3), make_scan_with_ids(4, 5, 6)],
            None,
        );
        let tagged = tag_required_columns(except, Some(needed_set(&[1002])));
        let LogicalPlanNodeKind::Except(_) = &tagged.kind else {
            panic!()
        };
        assert_eq!(scan_required_columns(tagged.child(0)).len(), 3);
        assert_eq!(scan_required_columns(tagged.child(1)).len(), 3);
    }

    // -----------------------------------------------------------------------
    // CTEAnchor two-walk test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_cte_anchor_produce_body_gets_keep_all_none() {
        // tag_cte_anchor passes None to the produce body (keep-all) to avoid
        // mis-aligning consumer positional slot assignments (Gap-3 conservative).
        //
        // CTEProduce[cte=7, output: c0@10, c1@20, c2@30] <- Scan[a@10,b@20,c@30]
        // CTEConsume[cte=7, output: k0@101, k1@102, k2@103]
        // parent_needed of anchor = {102}  (k1 @ position 1)
        // Expected (conservative): produce scan gets ALL columns {10, 20, 30}
        // because tag_cte_anchor passes None to the produce body.
        let cte_id: CteId = 7;

        let scan = make_scan_with_ids(10, 20, 30);

        let produce = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: cte_id,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(10), "c0"),
                    make_output_column(ColumnId::new_for_test(20), "c1"),
                    make_output_column(ColumnId::new_for_test(30), "c2"),
                ],
            }),
            vec![scan],
            None,
        );

        let consume = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: "u1".to_string(),
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(101), "k0"),
                    make_output_column(ColumnId::new_for_test(102), "k1"),
                    make_output_column(ColumnId::new_for_test(103), "k2"),
                ],
            }),
            vec![],
            None,
        );

        let anchor = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: cte_id }),
            vec![produce, consume],
            None,
        );

        let tagged = tag_required_columns(anchor, Some(needed_set(&[102])));

        let LogicalPlanNodeKind::CTEAnchor(_) = &tagged.kind else {
            panic!()
        };
        let produce = tagged.child(0);
        let LogicalPlanNodeKind::CTEProduce(_) = &produce.kind else {
            panic!()
        };
        let produce_input = produce.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &produce_input.kind else {
            panic!()
        };
        let req = required_columns(produce_input);
        // Conservative keep-all: produce body scan keeps all 3 columns.
        assert_eq!(
            req.len(),
            3,
            "scan must keep all columns (keep-all for CTE produce body)"
        );
        assert!(req.contains(&ColumnId::new_for_test(10)), "a@10 kept");
        assert!(req.contains(&ColumnId::new_for_test(20)), "b@20 kept");
        assert!(req.contains(&ColumnId::new_for_test(30)), "c@30 kept");
    }

    #[test]
    fn tag_cte_anchor_multi_consumer_produce_body_gets_keep_all_none() {
        // Two CTEConsumers — tag_cte_anchor passes None to the produce body
        // (keep-all) in the conservative no-op approach (Gap-3 deferred).
        //
        // consumer1 needs k1@102 (position 1)
        // consumer2 needs m2@203 (position 2)
        // Expected (conservative): produce scan gets ALL columns {10, 20, 30}
        // because tag_cte_anchor passes None to the produce body.
        let cte_id: CteId = 42;

        let scan = make_scan_with_ids(10, 20, 30);
        let produce = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: cte_id,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(10), "c0"),
                    make_output_column(ColumnId::new_for_test(20), "c1"),
                    make_output_column(ColumnId::new_for_test(30), "c2"),
                ],
            }),
            vec![scan],
            None,
        );

        let consume1 = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: "u1".to_string(),
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(101), "k0"),
                    make_output_column(ColumnId::new_for_test(102), "k1"),
                    make_output_column(ColumnId::new_for_test(103), "k2"),
                ],
            }),
            vec![],
            None,
        );
        let consume2 = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: "u2".to_string(),
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(201), "m0"),
                    make_output_column(ColumnId::new_for_test(202), "m1"),
                    make_output_column(ColumnId::new_for_test(203), "m2"),
                ],
            }),
            vec![],
            None,
        );

        // Consumer subtree: Join of consume1 and consume2.
        // parent_needed for the anchor = {102, 203}
        let consumer = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![consume1, consume2],
            None,
        );

        let anchor = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: cte_id }),
            vec![produce, consumer],
            None,
        );

        let tagged = tag_required_columns(anchor, Some(needed_set(&[102, 203])));

        let LogicalPlanNodeKind::CTEAnchor(_) = &tagged.kind else {
            panic!()
        };
        let produce = tagged.child(0);
        let LogicalPlanNodeKind::CTEProduce(_) = &produce.kind else {
            panic!()
        };
        let produce_input = produce.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &produce_input.kind else {
            panic!()
        };
        let req = required_columns(produce_input);
        // Conservative keep-all: produce body scan keeps all 3 columns.
        assert_eq!(
            req.len(),
            3,
            "scan must keep all columns (keep-all for CTE produce body)"
        );
        assert!(req.contains(&ColumnId::new_for_test(10)), "a@10 kept");
        assert!(req.contains(&ColumnId::new_for_test(20)), "b@20 kept");
        assert!(req.contains(&ColumnId::new_for_test(30)), "c@30 kept");
    }

    // -----------------------------------------------------------------------
    // Window test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_window_passes_none_to_child_keeps_all_input_cols() {
        // Window node must pass None to its child because window output_columns
        // carry fresh ColumnIds distinct from the child's ids — any attempt to
        // remap them risks under-tagging.  The safe fallback is None (keep all).
        //
        // Window[passthrough: a@1, b@2; window: row_number()→301 over part(b@2) order(c@3)]
        //   output_columns = [a@1, b@2, row_number@301]
        // parent_needed = {1}  (only a needed)
        // Expected: window.required_output_columns = {1}
        //           child scan gets required_output_columns = all of {1,2,3}
        //           because the handler passes None to the child.
        let window = LogicalPlanNode::new(
            LogicalPlanNodeKind::Window(LogicalWindowNode {
                window_exprs: vec![WindowExpr {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    partition_by: vec![col_ref_expr(ColumnId::new_for_test(2))],
                    order_by: vec![SortItem {
                        expr: col_ref_expr(ColumnId::new_for_test(3)),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: None,
                    result_type: DataType::Int64,
                    output_name: "row_number".to_string(),
                    output_column_id: ColumnId::new_for_test(301),
                    ignore_nulls: false,
                }],
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1), "a"),
                    make_output_column(ColumnId::new_for_test(2), "b"),
                    make_output_column(ColumnId::new_for_test(301), "row_number"),
                ],
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(window, Some(needed_set(&[1])));
        let LogicalPlanNodeKind::Window(_) = &tagged.kind else {
            panic!()
        };
        // The window node itself records the parent's request.
        assert_eq!(
            tagged.required_output_columns.as_ref().unwrap(),
            &needed_set(&[1])
        );
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        // Child got None → Scan expands to all its columns.
        let req = required_columns(input);
        assert_eq!(req.len(), 3, "scan keeps all 3 input columns");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_window_with_none_parent_child_also_keeps_all() {
        // When parent_needed is None, Window propagates None to the child
        // (no-op: child keeps all columns too).
        let window = LogicalPlanNode::new(
            LogicalPlanNodeKind::Window(LogicalWindowNode {
                window_exprs: vec![WindowExpr {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    partition_by: vec![col_ref_expr(ColumnId::new_for_test(2))],
                    order_by: vec![SortItem {
                        expr: col_ref_expr(ColumnId::new_for_test(3)),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: None,
                    result_type: DataType::Int64,
                    output_name: "row_number".to_string(),
                    output_column_id: ColumnId::new_for_test(301),
                    ignore_nulls: false,
                }],
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1), "a"),
                    make_output_column(ColumnId::new_for_test(2), "b"),
                    make_output_column(ColumnId::new_for_test(301), "row_number"),
                ],
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(window, None);
        let LogicalPlanNodeKind::Window(_) = &tagged.kind else {
            panic!()
        };
        assert!(tagged.required_output_columns.is_none());
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        // None propagated → Scan keeps all columns.
        let req = required_columns(input);
        assert_eq!(req.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Sort / Limit passthrough tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_sort_adds_key_cols_to_child_needed() {
        let sort = LogicalPlanNode::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: vec![SortItem {
                    expr: col_ref_expr(ColumnId::new_for_test(3)),
                    asc: true,
                    nulls_first: false,
                }],
                analytic_partition_by: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(sort, Some(needed_set(&[1])));
        let LogicalPlanNodeKind::Sort(_) = &tagged.kind else {
            panic!()
        };
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        let req = required_columns(input);
        assert!(req.contains(&ColumnId::new_for_test(1)), "parent needed a");
        assert!(
            req.contains(&ColumnId::new_for_test(3)),
            "sort key c needed"
        );
        assert!(!req.contains(&ColumnId::new_for_test(2)));
    }

    #[test]
    fn tag_limit_passes_needed_through() {
        let limit = LogicalPlanNode::new(
            LogicalPlanNodeKind::Limit(LogicalLimitNode {
                limit: Some(10),
                offset: None,
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let needed = needed_set(&[2]);
        let tagged = tag_required_columns(limit, Some(needed.clone()));
        let LogicalPlanNodeKind::Limit(_) = &tagged.kind else {
            panic!()
        };
        assert_eq!(tagged.required_output_columns.as_ref().unwrap(), &needed);
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        // Exactly the parent needed set passed through.
        assert_eq!(required_columns(input), &needed_set(&[2]));
    }

    // -----------------------------------------------------------------------
    // Values leaf test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_values_with_none_stamps_all_ids() {
        let values = LogicalPlanNode::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![
                    make_output_column(ColumnId::new_for_test(5), "x"),
                    make_output_column(ColumnId::new_for_test(6), "y"),
                ],
            }),
            vec![],
            None,
        );
        let tagged = tag_required_columns(values, None);
        let LogicalPlanNodeKind::Values(_) = &tagged.kind else {
            panic!()
        };
        let req = required_columns(&tagged);
        assert_eq!(req.len(), 2);
        assert!(req.contains(&ColumnId::new_for_test(5)));
        assert!(req.contains(&ColumnId::new_for_test(6)));
    }

    // -----------------------------------------------------------------------
    // None-propagation tests (Fix 4/5/6: Filter/Sort/Join must not collapse
    // None to an empty set — they must pass None through to children).
    // -----------------------------------------------------------------------

    #[test]
    fn tag_filter_none_parent_propagates_none_to_child() {
        // Filter(pred on c@3) <- Scan[a@1, b@2, c@3]
        // parent_needed = None
        // BUG before fix: collapsed to Some({3}), losing a@1 and b@2.
        // Correct: child gets None → Scan keeps all {1,2,3}.
        let filter = LogicalPlanNode::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_ref_expr(ColumnId::new_for_test(3))),
                        op: BinOp::Gt,
                        right: Box::new(int_literal(0)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(filter, None);
        let LogicalPlanNodeKind::Filter(_) = &tagged.kind else {
            panic!()
        };
        assert!(
            tagged.required_output_columns.is_none(),
            "filter keeps None on itself"
        );
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        // None propagated → Scan expands to all columns.
        let req = required_columns(input);
        assert_eq!(
            req.len(),
            3,
            "scan must keep all 3 columns, not just predicate ref c"
        );
        assert!(req.contains(&ColumnId::new_for_test(1)), "a@1 kept");
        assert!(req.contains(&ColumnId::new_for_test(2)), "b@2 kept");
        assert!(req.contains(&ColumnId::new_for_test(3)), "c@3 kept");
    }

    #[test]
    fn tag_sort_none_parent_propagates_none_to_child() {
        // Sort(order by c@3) <- Scan[a@1, b@2, c@3]
        // parent_needed = None
        // BUG before fix: collapsed to Some({3}), losing a@1 and b@2.
        // Correct: child gets None → Scan keeps all {1,2,3}.
        let sort = LogicalPlanNode::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: vec![SortItem {
                    expr: col_ref_expr(ColumnId::new_for_test(3)),
                    asc: true,
                    nulls_first: false,
                }],
                analytic_partition_by: vec![col_ref_expr(ColumnId::new_for_test(2))],
                partition_limit: None,
                topn_type: None,
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(sort, None);
        let LogicalPlanNodeKind::Sort(_) = &tagged.kind else {
            panic!()
        };
        assert!(
            tagged.required_output_columns.is_none(),
            "sort keeps None on itself"
        );
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        // None propagated → Scan expands to all columns.
        let req = required_columns(input);
        assert_eq!(
            req.len(),
            3,
            "scan must keep all 3 columns, not just sort/partition refs"
        );
        assert!(req.contains(&ColumnId::new_for_test(1)), "a@1 kept");
        assert!(req.contains(&ColumnId::new_for_test(2)), "b@2 kept");
        assert!(req.contains(&ColumnId::new_for_test(3)), "c@3 kept");
    }

    #[test]
    fn tag_join_none_parent_propagates_none_to_both_children() {
        // Join[INNER, on a@1=d@4] <- {Scan_l[a@1,b@2,c@3], Scan_r[d@4,e@5,f@6]}
        // parent_needed = None
        // BUG before fix: collapsed to Some({1,4}), losing b,c and e,f.
        // Correct: both children get None → each Scan keeps all its columns.
        let join = LogicalPlanNode::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_ref_expr(ColumnId::new_for_test(1))),
                        op: BinOp::Eq,
                        right: Box::new(col_ref_expr(ColumnId::new_for_test(4))),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
            }),
            vec![make_scan_with_ids(1, 2, 3), make_scan_with_ids(4, 5, 6)],
            None,
        );
        let tagged = tag_required_columns(join, None);
        let LogicalPlanNodeKind::Join(_) = &tagged.kind else {
            panic!()
        };
        assert!(
            tagged.required_output_columns.is_none(),
            "join keeps None on itself"
        );
        let left = tagged.left();
        let LogicalPlanNodeKind::Scan(_) = &left.kind else {
            panic!()
        };
        let right = tagged.right();
        let LogicalPlanNodeKind::Scan(_) = &right.kind else {
            panic!()
        };
        // None propagated → each Scan expands to all its columns.
        let lreq = required_columns(left);
        let rreq = required_columns(right);
        assert_eq!(lreq.len(), 3, "left scan keeps all 3 columns");
        assert!(lreq.contains(&ColumnId::new_for_test(1)));
        assert!(lreq.contains(&ColumnId::new_for_test(2)));
        assert!(lreq.contains(&ColumnId::new_for_test(3)));
        assert_eq!(rreq.len(), 3, "right scan keeps all 3 columns");
        assert!(rreq.contains(&ColumnId::new_for_test(4)));
        assert!(rreq.contains(&ColumnId::new_for_test(5)));
        assert!(rreq.contains(&ColumnId::new_for_test(6)));
    }

    // -----------------------------------------------------------------------
    // Keep-all tests for Repeat and TableFunction (Fix 1/2):
    // even when parent_needed omits a column the operator needs from its input,
    // the child retains all columns because the handler passes None.
    // -----------------------------------------------------------------------

    #[test]
    fn tag_repeat_maps_parent_needed_and_rollup_keys_to_child_ids() {
        use crate::sql::planner::plan::LogicalRepeatNode;
        // Repeat node referencing rollup columns b@2 by ColumnId.
        // parent_needed = {1}  (only a — does NOT include the rollup column b@2).
        // The handler sends {1,2} to the child: parent output a@1 plus
        // rollup key b@2 needed by Repeat's nulling/grouping logic.
        let repeat = LogicalPlanNode::new(
            LogicalPlanNodeKind::Repeat(LogicalRepeatNode {
                repeat_column_ref_list: vec![vec!["b".to_string()]],
                repeat_column_ref_ids: vec![vec![ColumnId::new_for_test(2)]],
                grouping_ids: vec![1],
                all_rollup_columns: vec!["b".to_string()],
                all_rollup_column_ids: vec![ColumnId::new_for_test(2)],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![],
                grouping_fn_arg_ids: vec![],
                grouping_fn_ids: vec![],
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(repeat, Some(needed_set(&[1])));
        let LogicalPlanNodeKind::Repeat(_) = &tagged.kind else {
            panic!()
        };
        // Repeat records parent_needed on itself.
        assert_eq!(
            tagged.required_output_columns.as_ref().unwrap(),
            &needed_set(&[1])
        );
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        let req = required_columns(input);
        assert_eq!(req.len(), 2, "scan keeps parent-needed and rollup key ids");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(!req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_repeat_parent_none_preserves_all_child_outputs() {
        use crate::sql::planner::plan::LogicalRepeatNode;

        let repeat = LogicalPlanNode::new(
            LogicalPlanNodeKind::Repeat(LogicalRepeatNode {
                repeat_column_ref_list: vec![vec!["b".to_string()]],
                repeat_column_ref_ids: vec![vec![ColumnId::new_for_test(2)]],
                grouping_ids: vec![1],
                all_rollup_columns: vec!["b".to_string()],
                all_rollup_column_ids: vec![ColumnId::new_for_test(2)],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![],
                grouping_fn_arg_ids: vec![],
                grouping_fn_ids: vec![],
            }),
            vec![scan_with_3_cols()],
            None,
        );

        let tagged = tag_required_columns(repeat, None);
        let LogicalPlanNodeKind::Repeat(_) = &tagged.kind else {
            panic!()
        };
        assert!(
            tagged.required_output_columns.is_none(),
            "Repeat root should keep the all-required None marker"
        );
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        let req = required_columns(input);
        assert_eq!(req.len(), 3, "child scan must keep all outputs");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_generate_series_parent_none_requires_output_id() {
        use crate::sql::planner::plan::LogicalGenerateSeriesNode;

        let output_id = ColumnId::new_for_test(301);
        let tagged = tag_required_columns(
            LogicalPlanNode::new(
                LogicalPlanNodeKind::GenerateSeries(LogicalGenerateSeriesNode {
                    start: 1,
                    end: 3,
                    step: 1,
                    column_name: "x".to_string(),
                    alias: Some("gs".to_string()),
                    output_column_id: output_id,
                }),
                vec![],
                None,
            ),
            None,
        );

        let LogicalPlanNodeKind::GenerateSeries(_) = &tagged.kind else {
            panic!()
        };
        let req = required_columns(&tagged);
        assert_eq!(req.len(), 1);
        assert!(req.contains(&output_id));
    }

    #[test]
    fn tag_table_function_passes_none_to_child_even_when_parent_needed_is_narrow() {
        use crate::sql::planner::plan::LogicalTableFunctionNode;
        // TableFunction: UNNEST(arr@2) → exploded_col@401
        // parent_needed = {401}  (only the function output — does NOT include arr@2).
        // The handler must pass None to the child so arr@2 (the arg) is not dropped.
        let tf = LogicalPlanNode::new(
            LogicalPlanNodeKind::TableFunction(LogicalTableFunctionNode {
                function_name: "unnest".to_string(),
                args: vec![col_ref_expr(ColumnId::new_for_test(2))],
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1), "a"),
                    make_output_column(ColumnId::new_for_test(401), "unnested"),
                ],
                alias: None,
                is_left_join: false,
            }),
            vec![scan_with_3_cols()],
            None,
        );
        let tagged = tag_required_columns(tf, Some(needed_set(&[401])));
        let LogicalPlanNodeKind::TableFunction(_) = &tagged.kind else {
            panic!()
        };
        // TableFunction records parent_needed on itself.
        assert_eq!(
            tagged.required_output_columns.as_ref().unwrap(),
            &needed_set(&[401])
        );
        let input = tagged.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!()
        };
        // Child got None → Scan expands to all columns, including arr@2.
        let req = required_columns(input);
        assert_eq!(
            req.len(),
            3,
            "scan keeps all 3 columns, including arr@2 needed by function arg"
        );
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(
            req.contains(&ColumnId::new_for_test(2)),
            "arr@2 must be kept for UNNEST arg"
        );
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    // -----------------------------------------------------------------------
    // TagRequiredColumns rule end-to-end pipeline test
    // -----------------------------------------------------------------------

    /// Verify that `TagRequiredColumns` runs through the full
    /// `query_rewrite_pipeline` and stamps `required_output_columns = Some(_)`
    /// on both nodes of a Project → Scan plan.
    #[test]
    fn tag_required_columns_rule_runs_through_pipeline_and_stamps_nodes() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
        use std::collections::HashMap;

        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![crate::sql::analysis::ProjectItem {
                    output_column_id: ColumnId::new_for_test(101),
                    output_name: "a".to_string(),
                    expr: col_ref_expr(ColumnId::new_for_test(1)),
                }],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanNodeKind::Scan(LogicalScanNode {
                    database: "db".to_string(),
                    table: crate::sql::catalog::TableDef {
                        name: "t".to_string(),
                        columns: vec![crate::sql::catalog::ColumnDef {
                            name: "a".to_string(),
                            data_type: arrow::datatypes::DataType::Int32,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        }],
                        iceberg_row_lineage_metadata_columns: vec![],
                        source: crate::sql::catalog::ScanSource::StarRocks {
                            db_id: 0,
                            table_id: 0,
                        },
                    },
                    alias: None,
                    columns: vec![make_output_column(ColumnId::new_for_test(1), "a")],
                    predicates: vec![],
                    required_columns: None,
                    dict_columns: vec![],
                    variant_columns: vec![],
                }),
                vec![],
                None,
            )],
            None,
        );

        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let result = pipeline.rewrite(plan, &mut ctx).unwrap();

        // After the pipeline, the Scan leaf must have Some(_) on
        // required_output_columns — proof that TagRequiredColumns ran and
        // stamped the leaf.
        //
        // Note: the root Project carries `required_output_columns = None`
        // because it was called as the tree root (parent_needed = None), which
        // is the correct metadata: "no parent restriction on the root".
        // Only leaf nodes are guaranteed to hold `Some(_)` after tagging.
        let LogicalPlanNodeKind::Project(_) = &result.kind else {
            panic!("expected Project at root after pipeline rewrite");
        };

        let input = result.unary_input();
        let LogicalPlanNodeKind::Scan(_) = &input.kind else {
            panic!("expected Scan child after pipeline rewrite");
        };
        assert!(
            input.required_output_columns.is_some(),
            "Scan.required_output_columns must be Some(_) after TagRequiredColumns stage ran"
        );
    }

    // -----------------------------------------------------------------------
    // tag_cte_consume with None parent — must store Some(all output ids)
    // -----------------------------------------------------------------------

    #[test]
    fn tag_cte_consume_with_none_parent_stores_some_all_output_ids() {
        // When parent_needed is None (no parent restriction), tag_cte_consume
        // must store Some(all output ids) — not None.  This ensures
        // subtree_untagged returns false after the first tagging pass, so
        // TagRequiredColumns terminates in one iteration for CTE plans.
        let cte_id: CteId = 99;
        let consume = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: "c".to_string(),
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(10), "x"),
                    make_output_column(ColumnId::new_for_test(20), "y"),
                    make_output_column(ColumnId::new_for_test(30), "z"),
                ],
            }),
            vec![],
            None,
        );

        let tagged = tag_cte_consume(consume, None);

        let LogicalPlanNodeKind::CTEConsume(_) = &tagged.kind else {
            panic!("expected CTEConsume");
        };
        let req = tagged
            .required_output_columns
            .as_ref()
            .expect("required_output_columns must be Some(_) after tagging with None parent");
        assert!(
            req.contains(&ColumnId::new_for_test(10)),
            "x@10 must be kept"
        );
        assert!(
            req.contains(&ColumnId::new_for_test(20)),
            "y@20 must be kept"
        );
        assert!(
            req.contains(&ColumnId::new_for_test(30)),
            "z@30 must be kept"
        );
        assert_eq!(req.len(), 3, "all 3 output ids kept");
    }

    #[test]
    fn tag_cte_anchor_with_none_parent_consume_leaf_is_some() {
        // A CTEAnchor tagged with parent_needed=None must end up with the
        // CTEConsume leaf holding Some(_), proving subtree_untagged returns
        // false (clean single-pass termination for `WITH cte AS (...) SELECT *
        // FROM cte` style plans).
        let cte_id: CteId = 88;

        let scan = make_scan_with_ids(10, 20, 30);
        let produce = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: cte_id,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(10), "a"),
                    make_output_column(ColumnId::new_for_test(20), "b"),
                    make_output_column(ColumnId::new_for_test(30), "c"),
                ],
            }),
            vec![scan],
            None,
        );

        let consume = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: "u".to_string(),
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(101), "p"),
                    make_output_column(ColumnId::new_for_test(102), "q"),
                ],
            }),
            vec![],
            None,
        );

        let anchor = LogicalPlanNode::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: cte_id }),
            vec![produce, consume],
            None,
        );

        let tagged = tag_required_columns(anchor, None);

        let LogicalPlanNodeKind::CTEAnchor(_) = &tagged.kind else {
            panic!("expected CTEAnchor");
        };
        let consumer = tagged.child(1);
        let LogicalPlanNodeKind::CTEConsume(_) = &consumer.kind else {
            panic!("expected CTEConsume consumer");
        };
        // The leaf must be Some(_) — not None — so subtree_untagged is false.
        assert!(
            consumer.required_output_columns.is_some(),
            "CTEConsume.required_output_columns must be Some(_) after tagging with None parent"
        );
        // All output ids must be present (keep-all semantics).
        let req = required_columns(consumer);
        assert!(req.contains(&ColumnId::new_for_test(101)), "p@101 kept");
        assert!(req.contains(&ColumnId::new_for_test(102)), "q@102 kept");
        assert_eq!(req.len(), 2, "both output ids kept");
    }
}
