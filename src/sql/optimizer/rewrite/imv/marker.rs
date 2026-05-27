//! Logical marker operators for Incremental MV (IMV) rewrite. See
//! docs/superpowers/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md §8.
//!
//! These markers must never reach physical lowering. The `imv-delta-marker`
//! stage of the IMV pipeline wraps the root; the `imv-validation` stage
//! rejects any plan that still carries a marker afterwards.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

/// `Delta(plan)` — "compute the incremental of plan". Typically wraps the
/// root of an IMV refresh plan exactly once. `action_column` is the column
/// that will eventually carry the per-row INSERT / DELETE / UPDATE marker
/// once task 5 (Action column propagation) fills it; in PR-β it is always
/// `None`.
#[derive(Clone, Debug)]
pub(crate) struct ImvDeltaNode {
    pub input: Box<LogicalPlan>,
    pub is_root: bool,
    pub action_column: Option<ColumnId>,
}

/// `Version(plan, version_ref)` — "scan plan over the snapshot window
/// described by `version_ref`". Task 4 (Iceberg scan delta/version binding)
/// emits this from Scan-replacing rules; PR-β only needs the type to exist.
#[derive(Clone, Debug)]
pub(crate) struct ImvVersionNode {
    pub input: Box<LogicalPlan>,
    pub version_ref: ImvVersionRef,
}

/// Snapshot window descriptor used by `ImvVersionNode`. PR-β leaves the
/// concrete fields to task 4; we only need a constructible placeholder so
/// the type is reachable from tests.
#[derive(Clone, Debug, Default)]
pub(crate) struct ImvVersionRef {
    _private: (),
}

/// Wraps the root of an IMV refresh plan in `ImvDelta { is_root: true }`.
/// One-shot per pipeline run: the `wrapped` flag is set on the first
/// apply and short-circuits every subsequent `matches()` so the rule
/// fires exactly once even though TopDown traversal would otherwise
/// revisit the child of the new `ImvDelta`.
pub(crate) struct WrapRootInImvDeltaRule {
    wrapped: AtomicBool,
}

impl WrapRootInImvDeltaRule {
    pub(crate) fn new() -> Self {
        Self {
            wrapped: AtomicBool::new(false),
        }
    }
}

impl LogicalRewriteRule for WrapRootInImvDeltaRule {
    fn name(&self) -> &'static str {
        "WrapRootInImvDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        if self.wrapped.load(Ordering::SeqCst) {
            return false;
        }
        // Plan was already wrapped before the pipeline ran (e.g. re-entry
        // on a previously wrapped plan): record the fact and skip.
        if matches!(
            plan,
            LogicalPlan::ImvDelta(ImvDeltaNode { is_root: true, .. })
        ) {
            self.wrapped.store(true, Ordering::SeqCst);
            return false;
        }
        true
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.wrapped.store(true, Ordering::SeqCst);
        Ok(RewriteResult::Changed(LogicalPlan::ImvDelta(
            ImvDeltaNode {
                input: Box::new(plan),
                is_root: true,
                action_column: None,
            },
        )))
    }
}

/// Returns true if `plan` contains any `ImvDelta` or `ImvVersion` node at
/// any depth. The Validation stage uses this to detect unresolved markers.
pub(crate) fn plan_contains_imv_marker(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => true,
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => false,
        LogicalPlan::Filter(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Project(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Aggregate(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Sort(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Limit(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Window(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::TableFunction(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::SubqueryAlias(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Repeat(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::CTEProduce(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Decode(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Join(n) => {
            plan_contains_imv_marker(&n.left) || plan_contains_imv_marker(&n.right)
        }
        LogicalPlan::CTEAnchor(n) => {
            plan_contains_imv_marker(&n.produce) || plan_contains_imv_marker(&n.consumer)
        }
        LogicalPlan::Union(n) => n.inputs.iter().any(plan_contains_imv_marker),
        LogicalPlan::Intersect(n) => n.inputs.iter().any(plan_contains_imv_marker),
        LogicalPlan::Except(n) => n.inputs.iter().any(plan_contains_imv_marker),
    }
}

/// Returns the distinct kinds of marker present in `plan`, in stable
/// order. Used by the Validation stage's error message.
pub(crate) fn collect_marker_kinds(plan: &LogicalPlan) -> Vec<&'static str> {
    let mut found: Vec<&'static str> = Vec::new();
    collect_into(plan, &mut found);
    found.sort();
    found.dedup();
    found
}

fn collect_into(plan: &LogicalPlan, found: &mut Vec<&'static str>) {
    match plan {
        LogicalPlan::ImvDelta(n) => {
            found.push("ImvDelta");
            collect_into(&n.input, found);
        }
        LogicalPlan::ImvVersion(n) => {
            found.push("ImvVersion");
            collect_into(&n.input, found);
        }
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => {}
        LogicalPlan::Filter(n) => collect_into(&n.input, found),
        LogicalPlan::Project(n) => collect_into(&n.input, found),
        LogicalPlan::Aggregate(n) => collect_into(&n.input, found),
        LogicalPlan::Sort(n) => collect_into(&n.input, found),
        LogicalPlan::Limit(n) => collect_into(&n.input, found),
        LogicalPlan::Window(n) => collect_into(&n.input, found),
        LogicalPlan::TableFunction(n) => collect_into(&n.input, found),
        LogicalPlan::SubqueryAlias(n) => collect_into(&n.input, found),
        LogicalPlan::Repeat(n) => collect_into(&n.input, found),
        LogicalPlan::CTEProduce(n) => collect_into(&n.input, found),
        LogicalPlan::Decode(n) => collect_into(&n.input, found),
        LogicalPlan::Join(n) => {
            collect_into(&n.left, found);
            collect_into(&n.right, found);
        }
        LogicalPlan::CTEAnchor(n) => {
            collect_into(&n.produce, found);
            collect_into(&n.consumer, found);
        }
        LogicalPlan::Union(n) => n.inputs.iter().for_each(|p| collect_into(p, found)),
        LogicalPlan::Intersect(n) => n.inputs.iter().for_each(|p| collect_into(p, found)),
        LogicalPlan::Except(n) => n.inputs.iter().for_each(|p| collect_into(p, found)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    #[test]
    fn wrap_rule_wraps_plain_root_once() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let plan = empty_values_plan();
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            vec![Box::new(WrapRootInImvDeltaRule::new())],
        )]);

        let out = pipeline.rewrite(plan, &mut ctx).unwrap();

        let LogicalPlan::ImvDelta(delta) = out else {
            panic!("expected ImvDelta at root");
        };
        assert!(delta.is_root);
        assert!(delta.action_column.is_none());
        assert!(matches!(*delta.input, LogicalPlan::Values(_)));
    }

    #[test]
    fn wrap_rule_is_idempotent_on_already_wrapped_plan() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let already = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(empty_values_plan()),
            is_root: true,
            action_column: None,
        });
        let before = format!("{already:?}");

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            vec![Box::new(WrapRootInImvDeltaRule::new())],
        )]);

        let out = pipeline.rewrite(already, &mut ctx).unwrap();
        assert_eq!(format!("{out:?}"), before, "wrap must not double-wrap");
    }

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }

    #[test]
    fn imv_delta_node_constructs_with_none_action_column() {
        let node = ImvDeltaNode {
            input: Box::new(empty_values_plan()),
            is_root: true,
            action_column: None,
        };
        assert!(node.is_root);
        assert!(node.action_column.is_none());
    }

    #[test]
    fn imv_version_node_constructs_with_default_ref() {
        let node = ImvVersionNode {
            input: Box::new(empty_values_plan()),
            version_ref: ImvVersionRef::default(),
        };
        assert!(matches!(*node.input, LogicalPlan::Values(_)));
    }

    #[test]
    fn plan_contains_imv_marker_false_for_plain_plan() {
        let plan = empty_values_plan();
        assert!(!plan_contains_imv_marker(&plan));
    }

    #[test]
    fn plan_contains_imv_marker_true_for_root_delta() {
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(empty_values_plan()),
            is_root: true,
            action_column: None,
        });
        assert!(plan_contains_imv_marker(&plan));
    }

    #[test]
    fn plan_contains_imv_marker_true_for_nested_version() {
        use crate::sql::planner::plan::LimitNode;
        // Build Limit(Limit(ImvVersion(Values))). The marker is
        // deeply nested; the helper must recurse.
        let nested = LogicalPlan::ImvVersion(ImvVersionNode {
            input: Box::new(empty_values_plan()),
            version_ref: ImvVersionRef::default(),
        });
        let inner = LogicalPlan::Limit(LimitNode {
            input: Box::new(nested),
            limit: None,
            offset: None,
        });
        let outer = LogicalPlan::Limit(LimitNode {
            input: Box::new(inner),
            limit: None,
            offset: None,
        });
        assert!(plan_contains_imv_marker(&outer));
    }

    #[test]
    fn collect_marker_kinds_reports_each_distinct_kind() {
        let delta = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(LogicalPlan::ImvVersion(ImvVersionNode {
                input: Box::new(empty_values_plan()),
                version_ref: ImvVersionRef::default(),
            })),
            is_root: true,
            action_column: None,
        });
        assert_eq!(collect_marker_kinds(&delta), vec!["ImvDelta", "ImvVersion"]);
    }
}
