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
//! Spec: `docs/superpowers/specs/2026-05-28-oq-1-column-pruning-arch-refactor-design.md` §5.

use std::collections::{HashMap, HashSet};

use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs, collect_output_ids, collect_output_ids_ordered,
};
use crate::sql::planner::plan::LogicalPlan;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Walk `plan` top-down and stamp `required_output_columns` on every operator.
///
/// `parent_needed = None` means the root has no caller restriction (all outputs
/// required).  Each operator type computes its own child's needed set and
/// recurses.
pub(crate) fn tag_required_columns(
    plan: LogicalPlan,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(_) => tag_scan(plan, parent_needed),
        LogicalPlan::Values(_) => tag_values(plan, parent_needed),
        LogicalPlan::GenerateSeries(_) => tag_generate_series(plan, parent_needed),
        LogicalPlan::Project(_) => tag_project(plan, parent_needed),
        LogicalPlan::Filter(_) => tag_filter(plan, parent_needed),
        LogicalPlan::Sort(_) => tag_sort(plan, parent_needed),
        LogicalPlan::Limit(_) => tag_limit(plan, parent_needed),
        LogicalPlan::Aggregate(_) => tag_aggregate(plan, parent_needed),
        LogicalPlan::Join(_) => tag_join(plan, parent_needed),
        LogicalPlan::SubqueryAlias(_) => tag_subquery_alias(plan, parent_needed),
        LogicalPlan::Union(_) => tag_union(plan, parent_needed),
        LogicalPlan::Intersect(_) => tag_intersect(plan, parent_needed),
        LogicalPlan::Except(_) => tag_except(plan, parent_needed),
        LogicalPlan::CTEAnchor(_) => tag_cte_anchor(plan, parent_needed),
        LogicalPlan::CTEConsume(_) => tag_cte_consume(plan, parent_needed),
        LogicalPlan::CTEProduce(_) => tag_cte_produce(plan, parent_needed),
        LogicalPlan::Window(_) => tag_window(plan, parent_needed),
        LogicalPlan::Repeat(_) => tag_repeat(plan, parent_needed),
        LogicalPlan::Decode(_) => tag_decode(plan, parent_needed),
        LogicalPlan::TableFunction(_) => tag_table_function(plan, parent_needed),
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker should not appear in non-IMV column pruning")
        }
    }
}

// ---------------------------------------------------------------------------
// Leaf handlers
// ---------------------------------------------------------------------------

fn tag_scan(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Scan(mut scan) = plan else {
        unreachable!()
    };
    let needed = parent_needed
        .unwrap_or_else(|| scan.columns.iter().map(|c| c.column_id).collect());
    scan.required_output_columns = Some(needed);
    LogicalPlan::Scan(scan)
}

fn tag_values(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Values(mut node) = plan else {
        unreachable!()
    };
    let needed = parent_needed
        .unwrap_or_else(|| node.columns.iter().map(|c| c.column_id).collect());
    node.required_output_columns = Some(needed);
    LogicalPlan::Values(node)
}

/// GenerateSeries has no ColumnId on its output slot (only a `column_name:
/// String`).  We write `parent_needed` (or an empty set when None, meaning
/// all-required) onto the field so Phase-2 no-ops cleanly.
fn tag_generate_series(
    plan: LogicalPlan,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlan {
    let LogicalPlan::GenerateSeries(mut node) = plan else {
        unreachable!()
    };
    // Use an empty set as the "all required" sentinel: GenerateSeries is a
    // leaf with a single unnamed column and no column-id addressable outputs.
    // Phase-2 prune rule is a no-op for GenerateSeries anyway.
    node.required_output_columns = Some(parent_needed.unwrap_or_default());
    LogicalPlan::GenerateSeries(node)
}

// ---------------------------------------------------------------------------
// Unary handlers
// ---------------------------------------------------------------------------

fn tag_project(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Project(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();
    // child_needed = union of ColumnRefs of items whose output_column_id is in
    // parent_needed (or all items when parent_needed is None).
    let child_needed: HashSet<ColumnId> = node
        .items
        .iter()
        .filter(|item| match &parent_needed {
            None => true,
            Some(n) => n.contains(&item.output_column_id),
        })
        .flat_map(|item| collect_column_id_refs(&item.expr))
        .collect();
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Project(node)
}

fn tag_filter(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Filter(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();
    // Child needs everything the parent needs PLUS all columns referenced in
    // the predicate.  When parent_needed is None (keep all), propagate None so
    // the child also keeps all columns instead of collapsing to just the
    // predicate refs.
    let child_needed = parent_needed.map(|mut needed| {
        needed.extend(collect_column_id_refs(&node.predicate));
        needed
    });
    node.input = Box::new(tag_required_columns(*node.input, child_needed));
    LogicalPlan::Filter(node)
}

fn tag_sort(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Sort(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();
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
    node.input = Box::new(tag_required_columns(*node.input, child_needed));
    LogicalPlan::Sort(node)
}

fn tag_limit(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Limit(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();
    // Limit is transparent: passes parent_needed straight through.
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::Limit(node)
}

fn tag_aggregate(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Aggregate(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();

    // Always include group-by column refs in child_needed.
    let mut child_needed: HashSet<ColumnId> = HashSet::new();
    for gb in &node.group_by {
        child_needed.extend(collect_column_id_refs(gb));
    }

    // For each aggregate, include its input columns only if its output id is
    // in parent_needed (or always when parent_needed is None).
    let group_by_len = node.group_by.len();
    for (i, agg) in node.aggregates.iter().enumerate() {
        let agg_output_id = node.output_columns[group_by_len + i].column_id;
        let is_needed = match &parent_needed {
            None => true,
            Some(n) => n.contains(&agg_output_id),
        };
        if is_needed {
            for arg in &agg.args {
                child_needed.extend(collect_column_id_refs(arg));
            }
            for item in &agg.order_by {
                child_needed.extend(collect_column_id_refs(&item.expr));
            }
        }
    }

    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Aggregate(node)
}

fn tag_subquery_alias(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    // SubqueryAlias is transparent: ColumnId space is preserved across the
    // alias boundary (only the qualifier/name changes, not the id).
    // Spec §5.8: pass parent_needed unchanged to inner plan.
    let LogicalPlan::SubqueryAlias(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::SubqueryAlias(node)
}

fn tag_window(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Window(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed;
    // Window output columns carry fresh ColumnIds (allocated by the planner)
    // that are distinct from the child's ids, so we cannot reliably map
    // parent_needed back to child column ids.  Pass None to the child so all
    // input columns are preserved and no column is spuriously dropped.
    node.input = Box::new(tag_required_columns(*node.input, None));
    LogicalPlan::Window(node)
}

fn tag_repeat(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    // RepeatPlanNode.repeat_column_ref_list is Vec<Vec<String>> (column names),
    // not ColumnIds.  We cannot map names to ids here, so we cannot determine
    // which child columns the rollup groups reference.  Pass None to the child
    // (keep all input columns) to avoid under-tagging.
    let LogicalPlan::Repeat(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed;
    node.input = Box::new(tag_required_columns(*node.input, None));
    LogicalPlan::Repeat(node)
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
fn tag_decode(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Decode(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::Decode(node)
}

fn tag_table_function(
    plan: LogicalPlan,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlan {
    let LogicalPlan::TableFunction(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed;
    // The function's args reference INPUT columns that may not appear in
    // parent_needed (e.g. UNNEST(t.arr) where parent only sees the exploded
    // output).  Pass None to the child so no input column is spuriously dropped.
    node.input = Box::new(tag_required_columns(*node.input, None));
    LogicalPlan::TableFunction(node)
}

// ---------------------------------------------------------------------------
// Binary / n-ary handlers
// ---------------------------------------------------------------------------

fn tag_join(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Join(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();

    // When parent_needed is None (keep all), propagate None to both children so
    // they also keep all columns.  When Some, compute combined = parent_needed ∪
    // condition refs, then split by which child produces each id.
    let (left_needed, right_needed) = match parent_needed {
        None => (None, None),
        Some(mut combined) => {
            if let Some(cond) = &node.condition {
                combined.extend(collect_column_id_refs(cond));
            }
            let left_outputs = collect_output_ids(&node.left);
            let right_outputs = collect_output_ids(&node.right);
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

    node.left = Box::new(tag_required_columns(*node.left, left_needed));
    node.right = Box::new(tag_required_columns(*node.right, right_needed));
    LogicalPlan::Join(node)
}

// ---------------------------------------------------------------------------
// Set operation handlers (Gap 4)
// ---------------------------------------------------------------------------

fn tag_union(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Union(mut node) = plan else {
        unreachable!()
    };

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

    node.required_output_columns = parent_needed;
    node.inputs = node
        .inputs
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
    LogicalPlan::Union(node)
}

fn tag_intersect(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Intersect(mut node) = plan else {
        unreachable!()
    };

    let outputs: Vec<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    let needed_positions: Vec<usize> = match &parent_needed {
        None => (0..outputs.len()).collect(),
        Some(n) => outputs
            .iter()
            .enumerate()
            .filter_map(|(i, id)| n.contains(id).then_some(i))
            .collect(),
    };

    node.required_output_columns = parent_needed;
    node.inputs = node
        .inputs
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
    LogicalPlan::Intersect(node)
}

fn tag_except(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Except(mut node) = plan else {
        unreachable!()
    };

    let outputs: Vec<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    let needed_positions: Vec<usize> = match &parent_needed {
        None => (0..outputs.len()).collect(),
        Some(n) => outputs
            .iter()
            .enumerate()
            .filter_map(|(i, id)| n.contains(id).then_some(i))
            .collect(),
    };

    node.required_output_columns = parent_needed;
    node.inputs = node
        .inputs
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
    LogicalPlan::Except(node)
}

// ---------------------------------------------------------------------------
// CTE handlers (Gap 3 — two-walk pattern)
// ---------------------------------------------------------------------------

fn tag_cte_consume(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::CTEConsume(mut node) = plan else {
        unreachable!()
    };
    // Leaf in this walk — just stamp the needed set and return.
    node.required_output_columns = parent_needed;
    LogicalPlan::CTEConsume(node)
}

fn tag_cte_produce(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::CTEProduce(mut node) = plan else {
        unreachable!()
    };
    node.required_output_columns = parent_needed.clone();
    // The produce-side needed ids are already in the producer's output id
    // space (translate_consume_to_produce_ids mapped them).  Pass them
    // straight through to the CTE body.
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::CTEProduce(node)
}

fn tag_cte_anchor(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::CTEAnchor(mut node) = plan else {
        unreachable!()
    };
    let cte_id = node.cte_id;

    // --- Walk 1: tag the consumer subtree with parent_needed. ---
    // This stamps required_output_columns on every CTEConsume for this cte_id.
    let consumer = tag_required_columns(*node.consumer, parent_needed.clone());

    // --- Walk 2: collect all CTEConsume needed ids for this cte_id. ---
    let mut consume_needs: HashSet<ColumnId> = HashSet::new();
    collect_cte_consumer_needs(&consumer, cte_id, &mut consume_needs);

    // Build consume_id -> position map from any CTEConsume with matching cte_id.
    let consume_pos_map = find_consume_position_map(&consumer, cte_id);

    // Translate consume-side ids to produce-side output ids via position.
    let produce_output_ids: Vec<ColumnId> = match &*node.produce {
        LogicalPlan::CTEProduce(p) => p.output_columns.iter().map(|c| c.column_id).collect(),
        _ => panic!("CTEAnchor.produce must be a CTEProduce node"),
    };

    let produce_needed: HashSet<ColumnId> = consume_needs
        .iter()
        .filter_map(|cid| {
            consume_pos_map
                .get(cid)
                .and_then(|&pos| produce_output_ids.get(pos).copied())
        })
        .collect();

    // --- Tag the producer subtree with the translated needed set. ---
    let produce = tag_required_columns(*node.produce, Some(produce_needed));

    node.consumer = Box::new(consumer);
    node.produce = Box::new(produce);
    node.required_output_columns = parent_needed;
    LogicalPlan::CTEAnchor(node)
}

// ---------------------------------------------------------------------------
// CTE helpers
// ---------------------------------------------------------------------------

/// Recursively traverse `plan` and union all `required_output_columns` sets
/// from `CTEConsume` nodes whose `cte_id` matches `target_id` into `acc`.
fn collect_cte_consumer_needs(
    plan: &LogicalPlan,
    target_id: CteId,
    acc: &mut HashSet<ColumnId>,
) {
    match plan {
        LogicalPlan::CTEConsume(c) if c.cte_id == target_id => {
            if let Some(req) = &c.required_output_columns {
                acc.extend(req.iter().copied());
            }
            // A CTEConsume is a leaf; do not recurse further.
        }
        LogicalPlan::CTEConsume(_) => {
            // Different cte_id — skip.
        }
        LogicalPlan::Scan(_) | LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) => {}
        LogicalPlan::Filter(f) => collect_cte_consumer_needs(&f.input, target_id, acc),
        LogicalPlan::Project(p) => collect_cte_consumer_needs(&p.input, target_id, acc),
        LogicalPlan::Aggregate(a) => collect_cte_consumer_needs(&a.input, target_id, acc),
        LogicalPlan::Sort(s) => collect_cte_consumer_needs(&s.input, target_id, acc),
        LogicalPlan::Limit(l) => collect_cte_consumer_needs(&l.input, target_id, acc),
        LogicalPlan::Window(w) => collect_cte_consumer_needs(&w.input, target_id, acc),
        LogicalPlan::Join(j) => {
            collect_cte_consumer_needs(&j.left, target_id, acc);
            collect_cte_consumer_needs(&j.right, target_id, acc);
        }
        LogicalPlan::Union(u) => {
            for child in &u.inputs {
                collect_cte_consumer_needs(child, target_id, acc);
            }
        }
        LogicalPlan::Intersect(i) => {
            for child in &i.inputs {
                collect_cte_consumer_needs(child, target_id, acc);
            }
        }
        LogicalPlan::Except(e) => {
            for child in &e.inputs {
                collect_cte_consumer_needs(child, target_id, acc);
            }
        }
        LogicalPlan::SubqueryAlias(s) => collect_cte_consumer_needs(&s.input, target_id, acc),
        LogicalPlan::TableFunction(t) => collect_cte_consumer_needs(&t.input, target_id, acc),
        LogicalPlan::Repeat(r) => collect_cte_consumer_needs(&r.input, target_id, acc),
        LogicalPlan::Decode(d) => collect_cte_consumer_needs(&d.input, target_id, acc),
        LogicalPlan::CTEProduce(p) => collect_cte_consumer_needs(&p.input, target_id, acc),
        LogicalPlan::CTEAnchor(a) => {
            // Recurse into the consumer side of nested CTEAnchors to find any
            // matching CTEConsume nodes there.
            collect_cte_consumer_needs(&a.consumer, target_id, acc);
            // Also recurse into the nested produce side in case an inner CTE
            // body references the outer CTE.
            collect_cte_consumer_needs(&a.produce, target_id, acc);
        }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {}
    }
}

/// Build a map from `consume_side_column_id` → `position` for the first
/// matching `CTEConsume(target_id)` found in the subtree.
///
/// All consumers with the same `cte_id` share the same positional schema, so
/// we stop at the first match.  The position is the index into
/// `CTEConsume.output_columns`, which aligns with `CTEProduce.output_columns`.
fn find_consume_position_map(
    plan: &LogicalPlan,
    target_id: CteId,
) -> HashMap<ColumnId, usize> {
    let mut map = HashMap::new();
    walk_consume_position_map(plan, target_id, &mut map);
    map
}

fn walk_consume_position_map(
    plan: &LogicalPlan,
    target_id: CteId,
    map: &mut HashMap<ColumnId, usize>,
) {
    match plan {
        LogicalPlan::CTEConsume(c) if c.cte_id == target_id => {
            // Record consume_side_column_id -> position for each output column.
            // Use `or_insert` so that if multiple consumers exist (multi-consumer
            // case), the first one wins — positions are identical across all
            // consumers with the same cte_id.
            for (i, col) in c.output_columns.iter().enumerate() {
                map.entry(col.column_id).or_insert(i);
            }
        }
        LogicalPlan::CTEConsume(_) => {} // different cte_id
        LogicalPlan::Scan(_) | LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) => {}
        LogicalPlan::Filter(f) => walk_consume_position_map(&f.input, target_id, map),
        LogicalPlan::Project(p) => walk_consume_position_map(&p.input, target_id, map),
        LogicalPlan::Aggregate(a) => walk_consume_position_map(&a.input, target_id, map),
        LogicalPlan::Sort(s) => walk_consume_position_map(&s.input, target_id, map),
        LogicalPlan::Limit(l) => walk_consume_position_map(&l.input, target_id, map),
        LogicalPlan::Window(w) => walk_consume_position_map(&w.input, target_id, map),
        LogicalPlan::Join(j) => {
            walk_consume_position_map(&j.left, target_id, map);
            walk_consume_position_map(&j.right, target_id, map);
        }
        LogicalPlan::Union(u) => {
            for child in &u.inputs {
                walk_consume_position_map(child, target_id, map);
            }
        }
        LogicalPlan::Intersect(i) => {
            for child in &i.inputs {
                walk_consume_position_map(child, target_id, map);
            }
        }
        LogicalPlan::Except(e) => {
            for child in &e.inputs {
                walk_consume_position_map(child, target_id, map);
            }
        }
        LogicalPlan::SubqueryAlias(s) => walk_consume_position_map(&s.input, target_id, map),
        LogicalPlan::TableFunction(t) => walk_consume_position_map(&t.input, target_id, map),
        LogicalPlan::Repeat(r) => walk_consume_position_map(&r.input, target_id, map),
        LogicalPlan::Decode(d) => walk_consume_position_map(&d.input, target_id, map),
        LogicalPlan::CTEProduce(p) => walk_consume_position_map(&p.input, target_id, map),
        LogicalPlan::CTEAnchor(a) => {
            walk_consume_position_map(&a.consumer, target_id, map);
            walk_consume_position_map(&a.produce, target_id, map);
        }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {}
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
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, CTEAnchorNode, CTEConsumeNode, CTEProduceNode, FilterNode,
        JoinNode, LimitNode, ProjectNode, ScanNode, SortNode, SubqueryAliasNode, UnionNode,
        ValuesNode, WindowExpr, WindowNode,
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

    fn make_scan_with_ids(id_a: u32, id_b: u32, id_c: u32) -> LogicalPlan {
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
        LogicalPlan::Scan(ScanNode {
            database: "d".to_string(),
            table,
            alias: None,
            columns: vec![
                make_output_column(ColumnId::new_for_test(id_a), "a"),
                make_output_column(ColumnId::new_for_test(id_b), "b"),
                make_output_column(ColumnId::new_for_test(id_c), "c"),
            ],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn scan_with_3_cols() -> LogicalPlan {
        make_scan_with_ids(1, 2, 3)
    }

    fn needed_set(ids: &[u32]) -> HashSet<ColumnId> {
        ids.iter().map(|&id| ColumnId::new_for_test(id)).collect()
    }

    // -----------------------------------------------------------------------
    // Scan tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_scan_with_none_keeps_all_cols() {
        let tagged = tag_required_columns(scan_with_3_cols(), None);
        let LogicalPlan::Scan(s) = tagged else {
            panic!()
        };
        let req = s.required_output_columns.unwrap();
        assert_eq!(req.len(), 3);
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_scan_with_subset_keeps_only_those() {
        let subset = needed_set(&[2]);
        let tagged = tag_required_columns(scan_with_3_cols(), Some(subset.clone()));
        let LogicalPlan::Scan(s) = tagged else {
            panic!()
        };
        assert_eq!(s.required_output_columns.unwrap(), subset);
    }

    // -----------------------------------------------------------------------
    // Project tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_project_filters_child_needed_by_output_column_id() {
        // Project[a→101, b→102] <- Scan[a@1, b@2, c@3]
        // parent_needed = {102 (b)}
        // Expected: scan.required_output_columns = {2}  (only b from scan)
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan_with_3_cols()),
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
            required_output_columns: None,
        });
        let needed = needed_set(&[102]);
        let tagged = tag_required_columns(project, Some(needed.clone()));

        let LogicalPlan::Project(p) = tagged else {
            panic!()
        };
        assert_eq!(p.required_output_columns.unwrap(), needed);

        let LogicalPlan::Scan(s) = *p.input else {
            panic!()
        };
        let scan_req = s.required_output_columns.unwrap();
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
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan_with_3_cols()),
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
            required_output_columns: None,
        });
        let tagged = tag_required_columns(project, None);
        let LogicalPlan::Project(p) = tagged else {
            panic!()
        };
        // required_output_columns should be None (transparent)
        assert!(p.required_output_columns.is_none());
        let LogicalPlan::Scan(s) = *p.input else {
            panic!()
        };
        let scan_req = s.required_output_columns.unwrap();
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
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan_with_3_cols()),
            predicate: TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_ref_expr(ColumnId::new_for_test(3))),
                    op: BinOp::Gt,
                    right: Box::new(int_literal(0)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            required_output_columns: None,
        });
        let tagged = tag_required_columns(filter, Some(needed_set(&[1])));
        let LogicalPlan::Filter(f) = tagged else {
            panic!()
        };
        let LogicalPlan::Scan(s) = *f.input else {
            panic!()
        };
        let req = s.required_output_columns.unwrap();
        assert!(req.contains(&ColumnId::new_for_test(1)), "a needed by parent");
        assert!(
            req.contains(&ColumnId::new_for_test(3)),
            "c needed by predicate"
        );
        assert!(!req.contains(&ColumnId::new_for_test(2)), "b not needed");
    }

    // -----------------------------------------------------------------------
    // Aggregate test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_aggregate_only_requests_needed_aggregate_args() {
        // Aggregate[group_by=[a@1], sum(b@2)→201, avg(c@3)→202] <- Scan[a@1,b@2,c@3]
        // parent_needed = {201}  (only sum(b) needed, not avg(c))
        // Expected: child_needed = {1, 2}  (group_by a + sum arg b; NOT c)
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan_with_3_cols()),
            group_by: vec![col_ref_expr(ColumnId::new_for_test(1))],
            aggregates: vec![
                AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref_expr(ColumnId::new_for_test(2))],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                },
                AggregateCall {
                    name: "avg".to_string(),
                    args: vec![col_ref_expr(ColumnId::new_for_test(3))],
                    distinct: false,
                    result_type: DataType::Float64,
                    order_by: vec![],
                },
            ],
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(1), "a"),
                make_output_column(ColumnId::new_for_test(201), "sum_b"),
                make_output_column(ColumnId::new_for_test(202), "avg_c"),
            ],
            already_pushed: false,
            required_output_columns: None,
        });
        let tagged = tag_required_columns(agg, Some(needed_set(&[201])));
        let LogicalPlan::Aggregate(a) = tagged else {
            panic!()
        };
        let LogicalPlan::Scan(s) = *a.input else {
            panic!()
        };
        let req = s.required_output_columns.unwrap();
        assert!(req.contains(&ColumnId::new_for_test(1)), "group_by a");
        assert!(req.contains(&ColumnId::new_for_test(2)), "sum(b) arg");
        assert!(
            !req.contains(&ColumnId::new_for_test(3)),
            "avg(c) not needed"
        );
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
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(make_scan_with_ids(1, 2, 3)),
            right: Box::new(make_scan_with_ids(4, 5, 6)),
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
            required_output_columns: None,
        });
        let tagged = tag_required_columns(join, Some(needed_set(&[2, 6])));
        let LogicalPlan::Join(j) = tagged else {
            panic!()
        };
        let LogicalPlan::Scan(l) = *j.left else {
            panic!()
        };
        let LogicalPlan::Scan(r) = *j.right else {
            panic!()
        };
        let lreq = l.required_output_columns.unwrap();
        let rreq = r.required_output_columns.unwrap();
        assert_eq!(lreq.len(), 2);
        assert!(lreq.contains(&ColumnId::new_for_test(1)));
        assert!(lreq.contains(&ColumnId::new_for_test(2)));
        assert_eq!(rreq.len(), 2);
        assert!(rreq.contains(&ColumnId::new_for_test(4)));
        assert!(rreq.contains(&ColumnId::new_for_test(6)));
    }

    // -----------------------------------------------------------------------
    // SubqueryAlias test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_subquery_alias_transparently_propagates_needed() {
        // SubqueryAlias[t] <- Scan[a@1,b@2,c@3]
        // parent_needed = {2}
        // Expected: scan.required_output_columns = {2}
        let alias = LogicalPlan::SubqueryAlias(SubqueryAliasNode {
            input: Box::new(scan_with_3_cols()),
            alias: "t".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(1), "a"),
                make_output_column(ColumnId::new_for_test(2), "b"),
                make_output_column(ColumnId::new_for_test(3), "c"),
            ],
            required_output_columns: None,
        });
        let needed = needed_set(&[2]);
        let tagged = tag_required_columns(alias, Some(needed.clone()));
        let LogicalPlan::SubqueryAlias(s) = tagged else {
            panic!()
        };
        assert_eq!(s.required_output_columns.unwrap(), needed);
        let LogicalPlan::Scan(inner) = *s.input else {
            panic!()
        };
        let inner_req = inner.required_output_columns.unwrap();
        assert!(inner_req.contains(&ColumnId::new_for_test(2)));
        assert_eq!(inner_req.len(), 1, "only b propagated");
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
        let union = LogicalPlan::Union(UnionNode {
            inputs: vec![make_scan_with_ids(1, 2, 3), make_scan_with_ids(4, 5, 6)],
            all: true,
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(1001), "x"),
                make_output_column(ColumnId::new_for_test(1002), "y"),
                make_output_column(ColumnId::new_for_test(1003), "z"),
            ],
            required_output_columns: None,
        });
        let tagged = tag_required_columns(union, Some(needed_set(&[1002])));
        let LogicalPlan::Union(u) = tagged else {
            panic!()
        };
        let LogicalPlan::Scan(a) = &u.inputs[0] else {
            panic!()
        };
        let LogicalPlan::Scan(b) = &u.inputs[1] else {
            panic!()
        };
        let a_req = a.required_output_columns.as_ref().unwrap();
        let b_req = b.required_output_columns.as_ref().unwrap();
        assert_eq!(a_req.len(), 1);
        assert!(a_req.contains(&ColumnId::new_for_test(2)), "position 1 = b@2");
        assert_eq!(b_req.len(), 1);
        assert!(b_req.contains(&ColumnId::new_for_test(5)), "position 1 = e@5");
    }

    // -----------------------------------------------------------------------
    // CTEAnchor two-walk test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_cte_anchor_two_walk_translates_consumer_needs_to_producer() {
        // CTEProduce[cte=7, output: c0@10, c1@20, c2@30] <- Scan[a@10,b@20,c@30]
        // CTEConsume[cte=7, output: k0@101, k1@102, k2@103]
        // parent_needed of anchor = {102}  (k1 @ position 1)
        // Expected: produce scan gets {20}  (position 1 = b@20)
        let cte_id: CteId = 7;

        let scan = make_scan_with_ids(10, 20, 30);

        let produce = LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id,
            input: Box::new(scan),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(10), "c0"),
                make_output_column(ColumnId::new_for_test(20), "c1"),
                make_output_column(ColumnId::new_for_test(30), "c2"),
            ],
            required_output_columns: None,
        });

        let consume = LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias: "u1".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(101), "k0"),
                make_output_column(ColumnId::new_for_test(102), "k1"),
                make_output_column(ColumnId::new_for_test(103), "k2"),
            ],
            required_output_columns: None,
        });

        let anchor = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id,
            produce: Box::new(produce),
            consumer: Box::new(consume),
            required_output_columns: None,
        });

        let tagged = tag_required_columns(anchor, Some(needed_set(&[102])));

        let LogicalPlan::CTEAnchor(a) = tagged else {
            panic!()
        };
        let LogicalPlan::CTEProduce(p) = *a.produce else {
            panic!()
        };
        let LogicalPlan::Scan(s) = *p.input else {
            panic!()
        };
        let req = s.required_output_columns.unwrap();
        assert!(
            req.contains(&ColumnId::new_for_test(20)),
            "b@20 needed (position 1 of producer)"
        );
        assert!(
            !req.contains(&ColumnId::new_for_test(10)),
            "a@10 not needed"
        );
        assert!(
            !req.contains(&ColumnId::new_for_test(30)),
            "c@30 not needed"
        );
    }

    #[test]
    fn tag_cte_anchor_union_of_multi_consumer_needs() {
        // Two CTEConsumers each referencing a different column.
        // consumer1 needs k1@102 (position 1)
        // consumer2 needs m2@203 (position 2)
        // Expected: produce scan gets {20, 30}
        let cte_id: CteId = 42;

        let scan = make_scan_with_ids(10, 20, 30);
        let produce = LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id,
            input: Box::new(scan),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(10), "c0"),
                make_output_column(ColumnId::new_for_test(20), "c1"),
                make_output_column(ColumnId::new_for_test(30), "c2"),
            ],
            required_output_columns: None,
        });

        let consume1 = LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias: "u1".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(101), "k0"),
                make_output_column(ColumnId::new_for_test(102), "k1"),
                make_output_column(ColumnId::new_for_test(103), "k2"),
            ],
            required_output_columns: None,
        });
        let consume2 = LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias: "u2".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(201), "m0"),
                make_output_column(ColumnId::new_for_test(202), "m1"),
                make_output_column(ColumnId::new_for_test(203), "m2"),
            ],
            required_output_columns: None,
        });

        // Consumer subtree: Join of consume1 and consume2.
        // parent_needed for the anchor = {102, 203}
        let consumer = LogicalPlan::Join(JoinNode {
            left: Box::new(consume1),
            right: Box::new(consume2),
            join_type: JoinKind::Inner,
            condition: None,
            required_output_columns: None,
        });

        let anchor = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id,
            produce: Box::new(produce),
            consumer: Box::new(consumer),
            required_output_columns: None,
        });

        let tagged = tag_required_columns(anchor, Some(needed_set(&[102, 203])));

        let LogicalPlan::CTEAnchor(a) = tagged else {
            panic!()
        };
        let LogicalPlan::CTEProduce(p) = *a.produce else {
            panic!()
        };
        let LogicalPlan::Scan(s) = *p.input else {
            panic!()
        };
        let req = s.required_output_columns.unwrap();
        assert!(
            req.contains(&ColumnId::new_for_test(20)),
            "b@20 from k1 (position 1)"
        );
        assert!(
            req.contains(&ColumnId::new_for_test(30)),
            "c@30 from m2 (position 2)"
        );
        assert!(!req.contains(&ColumnId::new_for_test(10)), "a@10 not needed");
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
        let window = LogicalPlan::Window(WindowNode {
            input: Box::new(scan_with_3_cols()),
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
                ignore_nulls: false,
            }],
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(1), "a"),
                make_output_column(ColumnId::new_for_test(2), "b"),
                make_output_column(ColumnId::new_for_test(301), "row_number"),
            ],
            required_output_columns: None,
        });
        let tagged = tag_required_columns(window, Some(needed_set(&[1])));
        let LogicalPlan::Window(w) = tagged else {
            panic!()
        };
        // The window node itself records the parent's request.
        assert_eq!(w.required_output_columns.as_ref().unwrap(), &needed_set(&[1]));
        let LogicalPlan::Scan(s) = *w.input else {
            panic!()
        };
        // Child got None → Scan expands to all its columns.
        let req = s.required_output_columns.unwrap();
        assert_eq!(req.len(), 3, "scan keeps all 3 input columns");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_window_with_none_parent_child_also_keeps_all() {
        // When parent_needed is None, Window propagates None to the child
        // (no-op: child keeps all columns too).
        let window = LogicalPlan::Window(WindowNode {
            input: Box::new(scan_with_3_cols()),
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
                ignore_nulls: false,
            }],
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(1), "a"),
                make_output_column(ColumnId::new_for_test(2), "b"),
                make_output_column(ColumnId::new_for_test(301), "row_number"),
            ],
            required_output_columns: None,
        });
        let tagged = tag_required_columns(window, None);
        let LogicalPlan::Window(w) = tagged else {
            panic!()
        };
        assert!(w.required_output_columns.is_none());
        let LogicalPlan::Scan(s) = *w.input else {
            panic!()
        };
        // None propagated → Scan keeps all columns.
        let req = s.required_output_columns.unwrap();
        assert_eq!(req.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Sort / Limit passthrough tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_sort_adds_key_cols_to_child_needed() {
        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan_with_3_cols()),
            items: vec![SortItem {
                expr: col_ref_expr(ColumnId::new_for_test(3)),
                asc: true,
                nulls_first: false,
            }],
            analytic_partition_by: vec![],
            required_output_columns: None,
        });
        let tagged = tag_required_columns(sort, Some(needed_set(&[1])));
        let LogicalPlan::Sort(s) = tagged else {
            panic!()
        };
        let LogicalPlan::Scan(scan) = *s.input else {
            panic!()
        };
        let req = scan.required_output_columns.unwrap();
        assert!(req.contains(&ColumnId::new_for_test(1)), "parent needed a");
        assert!(
            req.contains(&ColumnId::new_for_test(3)),
            "sort key c needed"
        );
        assert!(!req.contains(&ColumnId::new_for_test(2)));
    }

    #[test]
    fn tag_limit_passes_needed_through() {
        let limit = LogicalPlan::Limit(LimitNode {
            input: Box::new(scan_with_3_cols()),
            limit: Some(10),
            offset: None,
            required_output_columns: None,
        });
        let needed = needed_set(&[2]);
        let tagged = tag_required_columns(limit, Some(needed.clone()));
        let LogicalPlan::Limit(l) = tagged else {
            panic!()
        };
        assert_eq!(l.required_output_columns.unwrap(), needed);
        let LogicalPlan::Scan(s) = *l.input else {
            panic!()
        };
        // Exactly the parent needed set passed through.
        assert_eq!(s.required_output_columns.unwrap(), needed_set(&[2]));
    }

    // -----------------------------------------------------------------------
    // Values leaf test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_values_with_none_stamps_all_ids() {
        let values = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![
                make_output_column(ColumnId::new_for_test(5), "x"),
                make_output_column(ColumnId::new_for_test(6), "y"),
            ],
            required_output_columns: None,
        });
        let tagged = tag_required_columns(values, None);
        let LogicalPlan::Values(v) = tagged else {
            panic!()
        };
        let req = v.required_output_columns.unwrap();
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
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan_with_3_cols()),
            predicate: TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_ref_expr(ColumnId::new_for_test(3))),
                    op: BinOp::Gt,
                    right: Box::new(int_literal(0)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            required_output_columns: None,
        });
        let tagged = tag_required_columns(filter, None);
        let LogicalPlan::Filter(f) = tagged else {
            panic!()
        };
        assert!(
            f.required_output_columns.is_none(),
            "filter keeps None on itself"
        );
        let LogicalPlan::Scan(s) = *f.input else {
            panic!()
        };
        // None propagated → Scan expands to all columns.
        let req = s.required_output_columns.unwrap();
        assert_eq!(req.len(), 3, "scan must keep all 3 columns, not just predicate ref c");
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
        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan_with_3_cols()),
            items: vec![SortItem {
                expr: col_ref_expr(ColumnId::new_for_test(3)),
                asc: true,
                nulls_first: false,
            }],
            analytic_partition_by: vec![col_ref_expr(ColumnId::new_for_test(2))],
            required_output_columns: None,
        });
        let tagged = tag_required_columns(sort, None);
        let LogicalPlan::Sort(s) = tagged else {
            panic!()
        };
        assert!(
            s.required_output_columns.is_none(),
            "sort keeps None on itself"
        );
        let LogicalPlan::Scan(scan) = *s.input else {
            panic!()
        };
        // None propagated → Scan expands to all columns.
        let req = scan.required_output_columns.unwrap();
        assert_eq!(req.len(), 3, "scan must keep all 3 columns, not just sort/partition refs");
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
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(make_scan_with_ids(1, 2, 3)),
            right: Box::new(make_scan_with_ids(4, 5, 6)),
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
            required_output_columns: None,
        });
        let tagged = tag_required_columns(join, None);
        let LogicalPlan::Join(j) = tagged else {
            panic!()
        };
        assert!(
            j.required_output_columns.is_none(),
            "join keeps None on itself"
        );
        let LogicalPlan::Scan(l) = *j.left else {
            panic!()
        };
        let LogicalPlan::Scan(r) = *j.right else {
            panic!()
        };
        // None propagated → each Scan expands to all its columns.
        let lreq = l.required_output_columns.unwrap();
        let rreq = r.required_output_columns.unwrap();
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
    fn tag_repeat_passes_none_to_child_even_when_parent_needed_is_narrow() {
        use crate::sql::planner::plan::RepeatPlanNode;
        // Repeat node referencing rollup columns b@2 (by name, not id).
        // parent_needed = {1}  (only a — does NOT include the rollup column b@2).
        // The handler cannot resolve the name→id for b, so it must pass None
        // to the child, keeping all {1,2,3}.
        let repeat = LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(scan_with_3_cols()),
            repeat_column_ref_list: vec![vec!["b".to_string()]],
            grouping_ids: vec![1],
            all_rollup_columns: vec!["b".to_string()],
            grouping_key_aliases: vec![],
            grouping_fn_args: vec![],
            required_output_columns: None,
        });
        let tagged = tag_required_columns(repeat, Some(needed_set(&[1])));
        let LogicalPlan::Repeat(r) = tagged else {
            panic!()
        };
        // Repeat records parent_needed on itself.
        assert_eq!(r.required_output_columns.as_ref().unwrap(), &needed_set(&[1]));
        let LogicalPlan::Scan(s) = *r.input else {
            panic!()
        };
        // Child got None → Scan expands to all columns.
        let req = s.required_output_columns.unwrap();
        assert_eq!(req.len(), 3, "scan keeps all 3 columns, including b@2 needed by rollup");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_table_function_passes_none_to_child_even_when_parent_needed_is_narrow() {
        use crate::sql::planner::plan::TableFunctionNode;
        // TableFunction: UNNEST(arr@2) → exploded_col@401
        // parent_needed = {401}  (only the function output — does NOT include arr@2).
        // The handler must pass None to the child so arr@2 (the arg) is not dropped.
        let tf = LogicalPlan::TableFunction(TableFunctionNode {
            input: Box::new(scan_with_3_cols()),
            function_name: "unnest".to_string(),
            args: vec![col_ref_expr(ColumnId::new_for_test(2))],
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(1), "a"),
                make_output_column(ColumnId::new_for_test(401), "unnested"),
            ],
            alias: None,
            is_left_join: false,
            required_output_columns: None,
        });
        let tagged = tag_required_columns(tf, Some(needed_set(&[401])));
        let LogicalPlan::TableFunction(t) = tagged else {
            panic!()
        };
        // TableFunction records parent_needed on itself.
        assert_eq!(t.required_output_columns.as_ref().unwrap(), &needed_set(&[401]));
        let LogicalPlan::Scan(s) = *t.input else {
            panic!()
        };
        // Child got None → Scan expands to all columns, including arr@2.
        let req = s.required_output_columns.unwrap();
        assert_eq!(req.len(), 3, "scan keeps all 3 columns, including arr@2 needed by function arg");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)), "arr@2 must be kept for UNNEST arg");
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }
}
