//! Logical Plan — a tree of relational algebra operators.
//!
//! This is the layer where a future optimizer would operate.
//! Expressions use [`TypedExpr`] from [`crate::sql::analysis`].

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::catalog::TableDef;

use crate::sql::analysis::{JoinKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;

// ---------------------------------------------------------------------------
// Logical plan tree
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalPlanNode {
    pub kind: LogicalPlanNodeKind,
    pub children: Vec<LogicalPlanNode>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum LogicalPlanNodeKind {
    Scan(LogicalScanNode),
    Filter(LogicalFilterNode),
    Project(LogicalProjectNode),
    Aggregate(LogicalAggregateNode),
    Join(LogicalJoinNode),
    Sort(LogicalSortNode),
    Limit(LogicalLimitNode),
    Union(LogicalUnionNode),
    Intersect(LogicalIntersectNode),
    Except(LogicalExceptNode),
    Values(LogicalValuesNode),
    GenerateSeries(LogicalGenerateSeriesNode),
    TableFunction(LogicalTableFunctionNode),
    Window(LogicalWindowNode),
    /// Repeat node for ROLLUP/CUBE/GROUPING SETS.
    /// Replicates each input row N times with different null patterns.
    Repeat(LogicalRepeatNode),
    /// Defines the scope of one CTE. The left child is the producer subtree;
    /// the right child is the query subtree that may consume it.
    CTEAnchor(LogicalCTEAnchorNode),
    /// Produces the analyzed CTE definition.
    CTEProduce(LogicalCTEProduceNode),
    /// Reference to a CTE definition. Leaf node.
    CTEConsume(LogicalCTEConsumeNode),
    /// Low-cardinality dictionary decode: rewrites string columns to their
    /// dictionary-encoded form upstream and decodes back to strings before
    /// emission. Inserted by the dictionary-rewrite optimizer rule (Task 7);
    /// today no optimizer pass produces this variant — Task 5 only adds the
    /// type-system plumbing.
    Decode(LogicalDecodeNode),
    /// Logical IMV aggregate-state reconciliation over old target state and
    /// delta state. Execution lowering is added by later tasks.
    AggregateStateMerge(LogicalAggregateStateMergeNode),
    /// Subquery glue node (outer ⋈ subquery). Eliminated by the
    /// SubqueryRewrite stage; see ApplyNode.
    Apply(LogicalApplyNode),
    /// At-most-one-row runtime guard for scalar subqueries.
    AssertOneRow(LogicalAssertOneRowNode),
    /// IMV marker: "compute the incremental of input". Emitted by the
    /// `imv-delta-marker` stage; rejected by `imv-validation` if not
    /// consumed. Must never reach physical lowering. See
    /// `src/sql/optimizer/rewrite/imv/marker.rs`.
    ImvDelta(LogicalImvDeltaNode),
    /// IMV marker: "scan input over a snapshot window". Emitted by task 4
    /// scan-binding rules; consumed before lowering. Same panic-on-leak
    /// rule as `ImvDelta`.
    // PR-β scaffolding: task 4 constructs ImvVersion during scan-binding;
    // the variant exists here so the type is wired through the plan tree.
    #[allow(dead_code)]
    ImvVersion(LogicalImvVersionNode),
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum LogicalPlan {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    Sort(SortNode),
    Limit(LimitNode),
    Union(UnionNode),
    Intersect(IntersectNode),
    Except(ExceptNode),
    Values(ValuesNode),
    GenerateSeries(GenerateSeriesNode),
    TableFunction(TableFunctionNode),
    Window(WindowNode),
    /// Repeat node for ROLLUP/CUBE/GROUPING SETS.
    /// Replicates each input row N times with different null patterns.
    Repeat(RepeatPlanNode),
    /// Defines the scope of one CTE. The left child is the producer subtree;
    /// the right child is the query subtree that may consume it.
    CTEAnchor(CTEAnchorNode),
    /// Produces the analyzed CTE definition.
    CTEProduce(CTEProduceNode),
    /// Reference to a CTE definition. Leaf node.
    CTEConsume(CTEConsumeNode),
    /// Low-cardinality dictionary decode: rewrites string columns to their
    /// dictionary-encoded form upstream and decodes back to strings before
    /// emission. Inserted by the dictionary-rewrite optimizer rule (Task 7);
    /// today no optimizer pass produces this variant — Task 5 only adds the
    /// type-system plumbing.
    Decode(DecodeNode),
    /// Logical IMV aggregate-state reconciliation over old target state and
    /// delta state. Execution lowering is added by later tasks.
    AggregateStateMerge(AggregateStateMergeNode),
    /// Subquery glue node (outer ⋈ subquery). Eliminated by the
    /// SubqueryRewrite stage; see ApplyNode.
    Apply(ApplyNode),
    /// At-most-one-row runtime guard for scalar subqueries.
    AssertOneRow(AssertOneRowNode),
    /// IMV marker: "compute the incremental of input". Emitted by the
    /// `imv-delta-marker` stage; rejected by `imv-validation` if not
    /// consumed. Must never reach physical lowering. See
    /// `src/sql/optimizer/rewrite/imv/marker.rs`.
    ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode),
    /// IMV marker: "scan input over a snapshot window". Emitted by task 4
    /// scan-binding rules; consumed before lowering. Same panic-on-leak
    /// rule as `ImvDelta`.
    // PR-β scaffolding: task 4 constructs ImvVersion during scan-binding;
    // the variant exists here so the type is wired through the plan tree.
    #[allow(dead_code)]
    ImvVersion(crate::sql::optimizer::rewrite::imv::marker::ImvVersionNode),
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalDecodeNode {
    pub mappings: Vec<DecodeMapping>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalAggregateStateMergeNode {
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) change_op_column: String,
    pub(crate) output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalApplyNode {
    pub kind: ApplyKind,
    pub subquery_expr: TypedExpr,
    pub output_column: OutputColumn,
    pub inner_output_column_id: ColumnId,
    pub correlation_column_ids: Vec<ColumnId>,
    pub correlation_conjuncts: Vec<TypedExpr>,
    pub residual_predicate: Option<TypedExpr>,
    pub need_check_max_rows: bool,
    pub use_semi_anti: bool,
    pub uncorrelated_outer_predicate_columns: HashSet<ColumnId>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalAssertOneRowNode {
    pub subquery_text: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalRepeatNode {
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub all_rollup_column_ids: Vec<ColumnId>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEAnchorNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEProduceNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEConsumeNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalWindowNode {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalGenerateSeriesNode {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalTableFunctionNode {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalScanNode {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    pub dict_columns: Vec<ScanDictionaryColumn>,
    pub variant_columns: Vec<ScanVariantColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalValuesNode {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalFilterNode {
    pub predicate: TypedExpr,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalProjectNode {
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalAggregateNode {
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    pub already_pushed: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalSortNode {
    pub items: Vec<SortItem>,
    pub analytic_partition_by: Vec<TypedExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalLimitNode {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalJoinNode {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalUnionNode {
    pub all: bool,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalIntersectNode {
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalExceptNode {
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalImvDeltaNode {
    pub is_root: bool,
    pub action_column: Option<ColumnId>,
    pub branch_scope: Option<crate::sql::catalog::BranchScope>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalImvVersionNode {
    pub version_ref: crate::sql::optimizer::rewrite::imv::marker::ImvVersionRef,
}

#[derive(Clone, Debug)]
pub(crate) struct DecodeNode {
    pub input: Box<LogicalPlan>,
    pub mappings: Vec<DecodeMapping>,
    /// Output columns this Decode exposes upward. Mirrors the input's
    /// output columns with each `dict_column` swapped for its
    /// `string_column`. Populated by the rewrite rule that inserts
    /// Decode (Task 7) and preserved by every downstream pass. The
    /// optimizer's `derive_output_columns` returns this verbatim — without
    /// it the parent group would observe the child's `dict_column` name
    /// rather than `string_column`.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Per-column mapping from the dictionary-encoded slot back to the original
/// string slot. `dict_column` is the input column produced by the upstream
/// dict-encoded plan; `string_column` is the string output exposed to the
/// rest of the plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodeMapping {
    pub source_column_id: ColumnId,
    pub output_column_id: ColumnId,
    pub dict_column: String,
    pub string_column: String,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateStateMergeNode {
    pub(crate) old_input: Box<LogicalPlan>,
    pub(crate) delta_input: Box<LogicalPlan>,
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) change_op_column: String,
    pub(crate) output_columns: Vec<OutputColumn>,
}

/// What the subquery expression looks like to its enclosing clause.
/// M1 consumes the non-Scalar variants; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKind {
    Scalar,
    Exists { negated: bool },
    In { negated: bool },
}

/// Subquery glue node: left child = outer plan, right child = subquery plan.
/// Built by the planner from analyzer-collected subquery metadata (M1);
/// rewritten into join / aggregate / window shapes by the optimizer's
/// SubqueryRewrite stage. Must never survive past that stage — the
/// ApplyException rule and the optimize() backstop enforce this, and
/// memo conversion panics on a leaked Apply as defence in depth.
/// Field semantics mirror StarRocks LogicalApplyOperator; see the design doc
/// docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md §5.1.
/// M1 consumes the remaining fields; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ApplyNode {
    pub left: Box<LogicalPlan>,
    /// Subquery plan. May reference outer columns from
    /// `correlation_column_ids` while the Apply is alive.
    pub right: Box<LogicalPlan>,
    pub kind: ApplyKind,
    /// The expression the Apply was built from, written over the inner plan's
    /// output columns (`lhs IN (inner_col)`, `EXISTS(inner_col)`, or a bare
    /// `ColumnRef(inner_col)` for scalar subqueries).
    pub subquery_expr: TypedExpr,
    /// Fresh column standing in for the subquery's value in outer expressions.
    pub output_column: OutputColumn,
    /// The inner subquery's single scalar output column; the Apply's
    /// output_column is mapped to this after decorrelation. Captured at
    /// M1a emission time (before any pushdown), so it is stable across the
    /// M1b pushdown rules which may add group-by keys to the inner aggregate.
    pub inner_output_column_id: ColumnId,
    /// Outer-side columns referenced inside the subquery.
    pub correlation_column_ids: Vec<ColumnId>,
    /// Correlated conjuncts hoisted out of the inner plan by the
    /// SubqueryRewrite push-down rules (empty at construction).
    pub correlation_conjuncts: Vec<TypedExpr>,
    /// Uncorrelated residual predicate hoisted out of the inner plan.
    pub residual_predicate: Option<TypedExpr>,
    /// Scalar only: the subquery must still be runtime-checked to <= 1 row.
    pub need_check_max_rows: bool,
    /// True iff the subquery sits as a top-level AND conjunct of
    /// WHERE / HAVING / JOIN-ON, so it may collapse into a semi/anti join.
    pub use_semi_anti: bool,
    /// For uncorrelated scalar subqueries used inside a predicate: the outer
    /// sibling columns of that predicate (drives left-side Apply push-down).
    pub uncorrelated_outer_predicate_columns: HashSet<ColumnId>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Runtime guard asserting its input yields at most one row (SQL scalar
/// subquery cardinality rule). Lowered to thrift ASSERT_NUM_ROWS_NODE; the
/// exec operator and FE-compat lowering already exist. Must not be reordered
/// with Limit (a LIMIT above would mask the multi-row error).
/// M1 produces this node from ScalarApplyToJoin; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct AssertOneRowNode {
    pub input: Box<LogicalPlan>,
    /// Original subquery text used in the runtime error message.
    pub subquery_text: String,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Repeat node for ROLLUP/CUBE/GROUPING SETS.
/// Replicates each input row N times with different null patterns.
#[derive(Clone, Debug)]
pub(crate) struct RepeatPlanNode {
    pub input: Box<LogicalPlan>,
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub all_rollup_column_ids: Vec<ColumnId>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEAnchorNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub produce: Box<LogicalPlan>,
    pub consumer: Box<LogicalPlan>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEProduceNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub input: Box<LogicalPlan>,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEConsumeNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Analytic/window function evaluation node.
#[derive(Clone, Debug)]
pub(crate) struct WindowNode {
    pub input: Box<LogicalPlan>,
    pub window_exprs: Vec<WindowExpr>,
    /// All output columns: base columns from input + window function results.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
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

/// Inline table function: `TABLE(generate_series(start, end, step))`.
/// Emitted as a TABLE_FUNCTION_NODE over a one-row parameter input.
#[derive(Clone, Debug)]
pub(crate) struct GenerateSeriesNode {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Lateral table function evaluation over each input row.
#[derive(Clone, Debug)]
pub(crate) struct TableFunctionNode {
    pub input: Box<LogicalPlan>,
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// Leaf nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ScanNode {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    /// Predicates pushed down from Filter nodes by the optimizer.
    pub predicates: Vec<TypedExpr>,
    /// Columns actually required by upstream operators (set by column pruning).
    /// `None` means all columns are required (no pruning applied).
    pub required_columns: Option<Vec<String>>,
    /// Per-scan dictionary plan hints. Populated by the Task 7
    /// `LowCardinalityDictionaryRewrite` rule when a string column on
    /// this scan is eligible for low-cardinality rewriting. Empty
    /// everywhere else. Mirrored onto `LogicalScanOp` and
    /// `PhysicalScanOp` by memo conversion and the `ScanToPhysical`
    /// implementation rule.
    pub dict_columns: Vec<ScanDictionaryColumn>,
    /// Synthetic typed columns materialized from variant paths during scan.
    /// Populated by `VariantPathPushdownRule` and mirrored onto
    /// `PhysicalScanOp` by memo conversion and `ScanToPhysical`.
    pub variant_columns: Vec<ScanVariantColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Plan hint for a single dict-encoded string column on a scan.
/// `source_column` is the original string column name in the scan
/// output; `dict_column` is the synthetic `Int32` slot name introduced
/// by the rewrite rule; `dictionary` is the snapshot whose `(id, bytes)`
/// pairs become a `TGlobalDict` payload at codegen time.
#[derive(Clone, Debug)]
pub(crate) struct ScanDictionaryColumn {
    pub source_column: String,
    pub dict_column: String,
    pub dictionary: std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanVariantColumn {
    pub source_column_id: ColumnId,
    pub source_column: String,
    pub synthetic_column_id: ColumnId,
    pub synthetic_column: String,
    pub canonical_path: String,
    pub requested_type: DataType,
    pub strict: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ValuesNode {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// Unary nodes (single input)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicate: TypedExpr,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectNode {
    pub input: Box<LogicalPlan>,
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    /// Set to true by `AggregatePushdownRule`'s rewriter on the FINAL
    /// (top-level) aggregate after a partial aggregate has been spliced
    /// below. The collector treats `already_pushed = true` as a hard
    /// "skip" signal so the rule does not re-fire on its own output.
    /// Other rules (predicate pushdown, column pruning, cte rewrite,
    /// etc.) MUST preserve this flag when cloning `AggregateNode`.
    pub already_pushed: bool,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
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

#[derive(Clone, Debug)]
pub(crate) struct SortNode {
    pub input: Box<LogicalPlan>,
    pub items: Vec<SortItem>,
    /// Populated by `build_window_and_project` when this Sort was inserted
    /// as a precursor to a Window operator (PARTITION BY ...). Carries the
    /// window's partition_by columns, which become the analytic-partition
    /// tag on the downstream LogicalSortOp / PhysicalSortOp / TSortNode.
    /// Empty for top-level `ORDER BY` sorts.
    pub analytic_partition_by: Vec<TypedExpr>,
    /// Set by RankingWindowPredicatePushdown: per-partition rank cap + ranking
    /// kind. `None` ⇒ ordinary sort. See OQ-13 ranking-window design spec §4.
    pub partition_limit: Option<usize>,
    pub topn_type: Option<crate::exec::node::sort::SortTopNType>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct LimitNode {
    pub input: Box<LogicalPlan>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// Binary nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinKind,
    /// `None` for CROSS JOIN.
    pub condition: Option<TypedExpr>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// N-ary set operation nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct UnionNode {
    pub inputs: Vec<LogicalPlan>,
    /// `true` = UNION ALL, `false` = UNION DISTINCT.
    pub all: bool,
    /// Position-aligned output schema. Column at index `i` describes the
    /// union's output slot at position `i`, using the first branch's
    /// ColumnId. Populated at planner construction time so that future
    /// column-pruning passes (Gap 4) can map parent ColumnId requests to
    /// branch positions without descending into inputs.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct IntersectNode {
    pub inputs: Vec<LogicalPlan>,
    /// Position-aligned output schema. Same semantics as `UnionNode::output_columns`.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExceptNode {
    pub inputs: Vec<LogicalPlan>,
    /// Position-aligned output schema. Same semantics as `UnionNode::output_columns`.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[allow(non_snake_case)]
impl LogicalPlanNode {
    pub(crate) fn new(
        kind: LogicalPlanNodeKind,
        children: Vec<LogicalPlanNode>,
        required_output_columns: Option<HashSet<ColumnId>>,
    ) -> Self {
        Self {
            kind,
            children,
            required_output_columns,
        }
    }

    pub(crate) fn Scan(node: ScanNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: node.database,
                table: node.table,
                alias: node.alias,
                columns: node.columns,
                predicates: node.predicates,
                required_columns: node.required_columns,
                dict_columns: node.dict_columns,
                variant_columns: node.variant_columns,
            }),
            vec![],
            node.required_output_columns,
        )
    }

    pub(crate) fn Filter(node: FilterNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Filter(LogicalFilterNode {
                predicate: node.predicate,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn Project(node: ProjectNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: node.items,
                output_qualifier: node.output_qualifier,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn Aggregate(node: AggregateNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: node.group_by,
                aggregates: node.aggregates,
                output_columns: node.output_columns,
                already_pushed: node.already_pushed,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn Join(node: JoinNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Join(LogicalJoinNode {
                join_type: node.join_type,
                condition: node.condition,
            }),
            vec![(*node.left).into(), (*node.right).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn Sort(node: SortNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Sort(LogicalSortNode {
                items: node.items,
                analytic_partition_by: node.analytic_partition_by,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn Limit(node: LimitNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Limit(LogicalLimitNode {
                limit: node.limit,
                offset: node.offset,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn Union(node: UnionNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Union(LogicalUnionNode {
                all: node.all,
                output_columns: node.output_columns,
            }),
            node.inputs.into_iter().map(Into::into).collect(),
            node.required_output_columns,
        )
    }

    pub(crate) fn Intersect(node: IntersectNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Intersect(LogicalIntersectNode {
                output_columns: node.output_columns,
            }),
            node.inputs.into_iter().map(Into::into).collect(),
            node.required_output_columns,
        )
    }

    pub(crate) fn Except(node: ExceptNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Except(LogicalExceptNode {
                output_columns: node.output_columns,
            }),
            node.inputs.into_iter().map(Into::into).collect(),
            node.required_output_columns,
        )
    }

    pub(crate) fn Values(node: ValuesNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: node.rows,
                columns: node.columns,
            }),
            vec![],
            node.required_output_columns,
        )
    }

    pub(crate) fn GenerateSeries(node: GenerateSeriesNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::GenerateSeries(LogicalGenerateSeriesNode {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name,
                alias: node.alias,
                output_column_id: node.output_column_id,
            }),
            vec![],
            node.required_output_columns,
        )
    }

    pub(crate) fn TableFunction(node: TableFunctionNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::TableFunction(LogicalTableFunctionNode {
                function_name: node.function_name,
                args: node.args,
                output_columns: node.output_columns,
                alias: node.alias,
                is_left_join: node.is_left_join,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn Window(node: WindowNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Window(LogicalWindowNode {
                window_exprs: node.window_exprs,
                output_columns: node.output_columns,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn Repeat(node: RepeatPlanNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Repeat(LogicalRepeatNode {
                repeat_column_ref_list: node.repeat_column_ref_list,
                repeat_column_ref_ids: node.repeat_column_ref_ids,
                grouping_ids: node.grouping_ids,
                all_rollup_columns: node.all_rollup_columns,
                all_rollup_column_ids: node.all_rollup_column_ids,
                grouping_key_aliases: node.grouping_key_aliases,
                grouping_fn_args: node.grouping_fn_args,
                grouping_fn_arg_ids: node.grouping_fn_arg_ids,
                grouping_fn_ids: node.grouping_fn_ids,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn CTEAnchor(node: CTEAnchorNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::CTEAnchor(LogicalCTEAnchorNode {
                cte_id: node.cte_id,
            }),
            vec![(*node.produce).into(), (*node.consumer).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn CTEProduce(node: CTEProduceNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: node.cte_id,
                output_columns: node.output_columns,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn CTEConsume(node: CTEConsumeNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: node.cte_id,
                alias: node.alias,
                output_columns: node.output_columns,
            }),
            vec![],
            node.required_output_columns,
        )
    }

    pub(crate) fn Decode(node: DecodeNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Decode(LogicalDecodeNode {
                mappings: node.mappings,
                output_columns: node.output_columns,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn AggregateStateMerge(node: AggregateStateMergeNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
                group_key_names: node.group_key_names,
                aggregate_state_names: node.aggregate_state_names,
                change_op_column: node.change_op_column,
                output_columns: node.output_columns,
            }),
            vec![(*node.old_input).into(), (*node.delta_input).into()],
            None,
        )
    }

    pub(crate) fn Apply(node: ApplyNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::Apply(LogicalApplyNode {
                kind: node.kind,
                subquery_expr: node.subquery_expr,
                output_column: node.output_column,
                inner_output_column_id: node.inner_output_column_id,
                correlation_column_ids: node.correlation_column_ids,
                correlation_conjuncts: node.correlation_conjuncts,
                residual_predicate: node.residual_predicate,
                need_check_max_rows: node.need_check_max_rows,
                use_semi_anti: node.use_semi_anti,
                uncorrelated_outer_predicate_columns: node.uncorrelated_outer_predicate_columns,
            }),
            vec![(*node.left).into(), (*node.right).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn AssertOneRow(node: AssertOneRowNode) -> Self {
        Self::new(
            LogicalPlanNodeKind::AssertOneRow(LogicalAssertOneRowNode {
                subquery_text: node.subquery_text,
            }),
            vec![(*node.input).into()],
            node.required_output_columns,
        )
    }

    pub(crate) fn ImvDelta(
        node: crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode,
    ) -> Self {
        Self::new(
            LogicalPlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: node.is_root,
                action_column: node.action_column,
                branch_scope: node.branch_scope,
            }),
            vec![(*node.input).into()],
            None,
        )
    }

    pub(crate) fn ImvVersion(
        node: crate::sql::optimizer::rewrite::imv::marker::ImvVersionNode,
    ) -> Self {
        Self::new(
            LogicalPlanNodeKind::ImvVersion(LogicalImvVersionNode {
                version_ref: node.version_ref,
            }),
            vec![(*node.input).into()],
            None,
        )
    }
}

impl From<LogicalPlan> for LogicalPlanNode {
    fn from(plan: LogicalPlan) -> Self {
        match plan {
            LogicalPlan::Scan(node) => Self::Scan(node),
            LogicalPlan::Filter(node) => Self::Filter(node),
            LogicalPlan::Project(node) => Self::Project(node),
            LogicalPlan::Aggregate(node) => Self::Aggregate(node),
            LogicalPlan::Join(node) => Self::Join(node),
            LogicalPlan::Sort(node) => Self::Sort(node),
            LogicalPlan::Limit(node) => Self::Limit(node),
            LogicalPlan::Union(node) => Self::Union(node),
            LogicalPlan::Intersect(node) => Self::Intersect(node),
            LogicalPlan::Except(node) => Self::Except(node),
            LogicalPlan::Values(node) => Self::Values(node),
            LogicalPlan::GenerateSeries(node) => Self::GenerateSeries(node),
            LogicalPlan::TableFunction(node) => Self::TableFunction(node),
            LogicalPlan::Window(node) => Self::Window(node),
            LogicalPlan::Repeat(node) => Self::Repeat(node),
            LogicalPlan::CTEAnchor(node) => Self::CTEAnchor(node),
            LogicalPlan::CTEProduce(node) => Self::CTEProduce(node),
            LogicalPlan::CTEConsume(node) => Self::CTEConsume(node),
            LogicalPlan::Decode(node) => Self::Decode(node),
            LogicalPlan::AggregateStateMerge(node) => Self::AggregateStateMerge(node),
            LogicalPlan::Apply(node) => Self::Apply(node),
            LogicalPlan::AssertOneRow(node) => Self::AssertOneRow(node),
            LogicalPlan::ImvDelta(node) => Self::ImvDelta(node),
            LogicalPlan::ImvVersion(node) => Self::ImvVersion(node),
        }
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    #[test]
    fn logical_plan_node_exposes_kind_and_children_uniformly() {
        let child = LogicalPlanNode {
            kind: LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
            required_output_columns: None,
        };

        let node = LogicalPlanNode {
            kind: LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![],
                output_qualifier: None,
            }),
            children: vec![child],
            required_output_columns: None,
        };

        assert!(matches!(node.kind, LogicalPlanNodeKind::Project(_)));
        assert_eq!(node.children.len(), 1);
        assert!(node.required_output_columns.is_none());
    }

    #[test]
    fn legacy_logical_plan_conversion_moves_children_to_wrapper() {
        let legacy = LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            items: vec![],
            output_qualifier: None,
            required_output_columns: None,
        });

        let node = LogicalPlanNode::from(legacy);

        assert!(matches!(node.kind, LogicalPlanNodeKind::Project(_)));
        assert_eq!(node.children.len(), 1);
        assert!(matches!(
            node.children[0].kind,
            LogicalPlanNodeKind::Values(_)
        ));
    }

    #[test]
    fn legacy_imv_marker_conversion_moves_input_to_wrapper() {
        let legacy =
            LogicalPlan::ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode {
                input: Box::new(LogicalPlan::Values(ValuesNode {
                    rows: vec![],
                    columns: vec![],
                    required_output_columns: None,
                })),
                is_root: true,
                action_column: Some(ColumnId::new_for_test(7)),
                branch_scope: None,
            });

        let node = LogicalPlanNode::from(legacy);

        match node.kind {
            LogicalPlanNodeKind::ImvDelta(delta) => {
                assert!(delta.is_root);
                assert_eq!(delta.action_column, Some(ColumnId::new_for_test(7)));
            }
            other => panic!("expected ImvDelta, got {other:?}"),
        }
        assert_eq!(node.children.len(), 1);
        assert!(matches!(
            node.children[0].kind,
            LogicalPlanNodeKind::Values(_)
        ));
    }

    #[test]
    fn aggregate_node_already_pushed_defaults_false_via_construction() {
        let node = AggregateNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            already_pushed: false,
            required_output_columns: None,
        };
        assert!(!node.already_pushed);
    }

    #[test]
    fn project_node_required_output_columns_defaults_none() {
        // Construct a ProjectNode with a minimal Values input and assert
        // that required_output_columns is None on a freshly-built node.
        let node = ProjectNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            items: vec![],
            output_qualifier: None,
            required_output_columns: None,
        };
        assert!(node.required_output_columns.is_none());
    }

    #[test]
    fn union_node_carries_explicit_output_columns() {
        use crate::sql::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "x".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }];
        let node = UnionNode {
            inputs: vec![],
            all: true,
            output_columns: cols.clone(),
            required_output_columns: None,
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "x");
        assert_eq!(node.output_columns[0].data_type, DataType::Int32);
        assert!(!node.output_columns[0].nullable);
    }

    #[test]
    fn intersect_node_carries_explicit_output_columns() {
        use crate::sql::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "y".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        }];
        let node = IntersectNode {
            inputs: vec![],
            output_columns: cols,
            required_output_columns: None,
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "y");
    }

    #[test]
    fn except_node_carries_explicit_output_columns() {
        use crate::sql::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "z".to_string(),
            data_type: DataType::Boolean,
            nullable: false,
            is_internal: false,
        }];
        let node = ExceptNode {
            inputs: vec![],
            output_columns: cols,
            required_output_columns: None,
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "z");
    }

    #[test]
    fn aggregate_state_merge_node_preserves_inputs_and_output_columns() {
        use crate::sql::analysis::OutputColumn;
        use crate::sql::column_id::ColumnId;

        fn empty_values_for_test() -> LogicalPlan {
            LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })
        }

        let old_input = empty_values_for_test();
        let delta_input = empty_values_for_test();
        let node = AggregateStateMergeNode {
            old_input: Box::new(old_input),
            delta_input: Box::new(delta_input),
            group_key_names: vec!["region".to_string()],
            aggregate_state_names: vec!["c".to_string(), "s".to_string()],
            change_op_column: "__change_op".to_string(),
            output_columns: vec![
                OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "region".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::new_for_test(2),
                    name: "c".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
        };

        assert_eq!(node.group_key_names, vec!["region"]);
        assert_eq!(node.aggregate_state_names, vec!["c", "s"]);
        assert_eq!(node.change_op_column, "__change_op");
        assert_eq!(node.output_columns.len(), 2);
    }
}
