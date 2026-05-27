//! Logical marker operators for Incremental MV (IMV) rewrite. See
//! docs/superpowers/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md §8.
//!
//! These markers must never reach physical lowering. The `imv-delta-marker`
//! stage of the IMV pipeline wraps the root; the `imv-validation` stage
//! rejects any plan that still carries a marker afterwards.

use crate::sql::column_id::ColumnId;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

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
}
