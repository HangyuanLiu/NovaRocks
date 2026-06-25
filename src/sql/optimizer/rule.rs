//! Rule trait for the Cascades optimizer.
//!
//! Rules transform or implement expressions in the Memo. Transformation rules
//! produce logically equivalent alternatives; implementation rules map logical
//! operators to their physical counterparts.

use super::memo::{GroupId, MExpr, Memo};
use super::operator::Operator;

// ---------------------------------------------------------------------------
// Rule types
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub(crate) enum RuleType {
    Transformation,
    Implementation,
}

/// A new expression to add to a group.
pub(crate) struct NewExpr {
    pub op: Operator,
    pub children: Vec<GroupId>,
}

// ---------------------------------------------------------------------------
// Rule trait
// ---------------------------------------------------------------------------

pub(crate) trait Rule: Send + Sync {
    fn name(&self) -> &str;
    #[allow(dead_code)]
    fn rule_type(&self) -> RuleType;
    /// Returns true if this rule can apply to the given operator.
    fn matches(&self, op: &Operator) -> bool;
    /// Produce alternative expressions for the given MExpr.
    ///
    /// Takes `&mut Memo` so that rules creating intermediate groups (e.g. two-phase
    /// aggregation) can allocate new groups for their internal structure.
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr>;

    /// Declarative match shape for the Memo binder. Default = root-only wildcard
    /// (`Pattern::Leaf`), so un-migrated rules behave exactly as today: the binder
    /// yields one root binding and `apply_bound` shims to legacy `apply`.
    fn pattern(&self) -> crate::sql::optimizer::pattern::Pattern {
        crate::sql::optimizer::pattern::Pattern::Leaf
    }

    /// If true, only the FIRST binding from the binder is applied (reproduces a
    /// legacy `.find`-style single-match rule). Default: apply all bindings.
    fn first_match_only(&self) -> bool {
        false
    }

    /// Apply against a fully-resolved binding. Default = shim to legacy `apply`
    /// on the bound root expr. Migrated rules override this and never call the default.
    fn apply_bound(
        &self,
        binding: &crate::sql::optimizer::binder::Binding,
        memo: &mut Memo,
    ) -> Vec<NewExpr> {
        let root = binding.root_mexpr(memo).clone();
        self.apply(&root, memo)
    }
}

#[cfg(test)]
mod trait_default_tests {
    use super::*;
    struct DummyRule;
    impl Rule for DummyRule {
        fn name(&self) -> &str {
            "Dummy"
        }
        fn rule_type(&self) -> RuleType {
            RuleType::Transformation
        }
        fn matches(&self, _op: &Operator) -> bool {
            true
        }
        fn apply(&self, _expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
            vec![]
        }
    }
    #[test]
    fn default_pattern_is_leaf() {
        assert!(matches!(
            DummyRule.pattern(),
            crate::sql::optimizer::pattern::Pattern::Leaf
        ));
    }
}
