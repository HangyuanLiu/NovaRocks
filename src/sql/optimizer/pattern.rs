//! Declarative match patterns for the Memo-side Binder (G5 A2).
//! Introduced for the Memo binder ONLY; RBO keeps imperative matches(&OptExpr).
//! A pattern matches on operator KIND + structural shape; all field predicates
//! (JoinKind::Inner, limit.is_some(), union.all, …) stay inside the rule's
//! apply_bound, exactly where the legacy code read them.

use crate::sql::optimizer::operator::Operator;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum OpKind {
    Join,
    Limit,
    Sort,
    TopN,
    Project,
    Scan,
    Union,
}

/// Structural match pattern. `Op` matches operator KIND only (not fields).
/// `Leaf` = opaque single child group (captured, not descended/enumerated).
/// `MultiLeaf` = variable-arity trailing tail of opaque child groups.
#[derive(Clone, Debug)]
pub(crate) enum Pattern {
    Op { kind: OpKind, children: Vec<Pattern> },
    Leaf,
    MultiLeaf,
}

/// Logical operator → its `OpKind`, or `None` for operators no A2 pattern uses.
pub(crate) fn op_kind(op: &Operator) -> Option<OpKind> {
    match op {
        Operator::LogicalJoin(_) => Some(OpKind::Join),
        Operator::LogicalLimit(_) => Some(OpKind::Limit),
        Operator::LogicalSort(_) => Some(OpKind::Sort),
        Operator::LogicalTopN(_) => Some(OpKind::TopN),
        Operator::LogicalProject(_) => Some(OpKind::Project),
        Operator::LogicalScan(_) => Some(OpKind::Scan),
        Operator::LogicalUnion(_) => Some(OpKind::Union),
        _ => None,
    }
}

/// Cheap root gate used by explore/implement before constructing a Binder:
/// equivalent to today's `rule.matches(&op)` on the root variant.
pub(crate) fn pattern_root_matches(p: &Pattern, op: &Operator) -> bool {
    match p {
        Pattern::Leaf | Pattern::MultiLeaf => true,
        Pattern::Op { kind, .. } => op_kind(op) == Some(*kind),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::operator::{LogicalJoinOp, Operator};
    use crate::sql::common::JoinKind;

    #[test]
    fn op_kind_maps_logical_join() {
        let op = Operator::LogicalJoin(LogicalJoinOp { join_type: JoinKind::Inner, condition: None });
        assert_eq!(op_kind(&op), Some(OpKind::Join));
    }

    #[test]
    fn pattern_root_matches_kind_only_not_fields() {
        let left = Operator::LogicalJoin(LogicalJoinOp { join_type: JoinKind::LeftOuter, condition: None });
        let p = Pattern::Op { kind: OpKind::Join, children: vec![Pattern::Leaf, Pattern::Leaf] };
        assert!(pattern_root_matches(&p, &left));
    }
}
