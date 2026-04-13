//! Logical plan optimizer.
//!
//! Applies optimization passes to the [`LogicalPlan`] tree before it is handed
//! to the Thrift emitter.  The cascades-based optimizer in
//! `src/sql/optimizer/cascades/` is the primary optimizer; this module exposes
//! shared utilities (e.g. `map_children`) used by individual optimizer rules.

pub(crate) mod cardinality;
pub(crate) mod cost;
pub(crate) mod expr_utils;
pub(crate) mod join_reorder;

use crate::sql::plan::*;

/// Apply a function to all direct children of a LogicalPlan node.
pub(super) fn map_children(plan: LogicalPlan, f: fn(LogicalPlan) -> LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => plan,
        LogicalPlan::CTEAnchor(n) => LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: n.cte_id,
            produce: Box::new(f(*n.produce)),
            consumer: Box::new(f(*n.consumer)),
        }),
        LogicalPlan::CTEProduce(n) => LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: n.cte_id,
            input: Box::new(f(*n.input)),
            output_columns: n.output_columns,
        }),
        LogicalPlan::Window(n) => LogicalPlan::Window(WindowNode {
            input: Box::new(f(*n.input)),
            ..n
        }),
        LogicalPlan::Filter(n) => LogicalPlan::Filter(FilterNode {
            input: Box::new(f(*n.input)),
            predicate: n.predicate,
        }),
        LogicalPlan::Project(n) => LogicalPlan::Project(ProjectNode {
            input: Box::new(f(*n.input)),
            items: n.items,
        }),
        LogicalPlan::Aggregate(n) => LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(f(*n.input)),
            ..n
        }),
        LogicalPlan::Join(n) => LogicalPlan::Join(JoinNode {
            left: Box::new(f(*n.left)),
            right: Box::new(f(*n.right)),
            join_type: n.join_type,
            condition: n.condition,
        }),
        LogicalPlan::Sort(n) => LogicalPlan::Sort(SortNode {
            input: Box::new(f(*n.input)),
            items: n.items,
        }),
        LogicalPlan::Limit(n) => LogicalPlan::Limit(LimitNode {
            input: Box::new(f(*n.input)),
            limit: n.limit,
            offset: n.offset,
        }),
        LogicalPlan::Union(n) => LogicalPlan::Union(UnionNode {
            inputs: n.inputs.into_iter().map(f).collect(),
            all: n.all,
        }),
        LogicalPlan::Intersect(n) => LogicalPlan::Intersect(IntersectNode {
            inputs: n.inputs.into_iter().map(f).collect(),
        }),
        LogicalPlan::Except(n) => LogicalPlan::Except(ExceptNode {
            inputs: n.inputs.into_iter().map(f).collect(),
        }),
        LogicalPlan::SubqueryAlias(n) => LogicalPlan::SubqueryAlias(SubqueryAliasNode {
            input: Box::new(f(*n.input)),
            alias: n.alias,
            output_columns: n.output_columns,
        }),
        LogicalPlan::Repeat(n) => LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(f(*n.input)),
            repeat_column_ref_list: n.repeat_column_ref_list,
            grouping_ids: n.grouping_ids,
            all_rollup_columns: n.all_rollup_columns,
            grouping_fn_args: n.grouping_fn_args,
        }),
    }
}

