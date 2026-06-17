//! `OptExpr` — the optimizer's concrete logical operator tree.
//!
//! Mirrors StarRocks `OptExpression`: an `Operator` payload plus child
//! `OptExpr`s. Scalars inside the operator are already interned `ScalarId`
//! handles into the owning `ScalarArena`. This is the tree the RBO rewrite
//! phase will operate on (A2); `convert::opt_expr_to_memo` copies it into the
//! Memo for CBO.

use super::operator::Operator;

#[derive(Clone, Debug)]
pub(crate) struct OptExpr {
    pub op: Operator,
    pub children: Vec<OptExpr>,
}

impl OptExpr {
    pub(crate) fn new(op: Operator, children: Vec<OptExpr>) -> Self {
        Self { op, children }
    }

    pub(crate) fn leaf(op: Operator) -> Self {
        Self {
            op,
            children: Vec::new(),
        }
    }
}
