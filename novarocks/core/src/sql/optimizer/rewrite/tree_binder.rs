// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Tree-side declarative Pattern matcher.
//!
//! The tree rewrite driver uses this matcher as a structural pre-gate before
//! calling each rule's field-level `matches` method. The migration invariants
//! are:
//! - driver traversal and fixed-point order stay unchanged;
//! - default `Pattern::Leaf` is a root wildcard, so holdout rules keep their
//!   legacy imperative matching behavior;
//! - for migrated rules, structural `pattern` matching plus field-level
//!   `matches` is equivalent to the old monolithic `matches` predicate;
//! - `apply` remains the semantic rewrite boundary and is not interpreted by
//!   the binder;
//! - `first_match_only` is degenerate on concrete trees because one node can
//!   produce at most one binding.
//!
//! Unlike the memo binder, an `OptExpr` child is one concrete subtree, so a
//! successful match produces at most one binding.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{Pattern, op_kind};

/// A successful tree pattern match.
///
/// `interiors` contains only nodes matched by `Pattern::Op`, stored in DFS
/// preorder. `Pattern::Leaf` and `Pattern::MultiLeaf` captures are not
/// represented here.
pub(crate) struct TreeBinding<'t> {
    root: &'t OptExpr,
    interiors: Vec<&'t OptExpr>,
}

impl<'t> TreeBinding<'t> {
    // These accessors support migrated rules and tests that need matched
    // interior nodes.
    /// Return the root expression matched by the pattern.
    #[allow(dead_code)]
    pub(crate) fn root(&self) -> &'t OptExpr {
        self.root
    }

    /// Return the operator for a matched `Pattern::Op` interior index.
    ///
    /// Panics if `i` is outside the matched interior list.
    #[allow(dead_code)]
    pub(crate) fn op(&self, i: usize) -> &'t Operator {
        &self.interiors[i].op
    }

    /// Return the node for a matched `Pattern::Op` interior index.
    ///
    /// Panics if `i` is outside the matched interior list.
    #[allow(dead_code)]
    pub(crate) fn node(&self, i: usize) -> &'t OptExpr {
        self.interiors[i]
    }

    /// Return the children for a matched `Pattern::Op` interior index.
    ///
    /// Panics if `i` is outside the matched interior list.
    #[allow(dead_code)]
    pub(crate) fn children(&self, i: usize) -> &'t [OptExpr] {
        &self.interiors[i].children
    }
}

#[allow(dead_code)]
pub(crate) fn bind_tree<'t>(pattern: &Pattern, expr: &'t OptExpr) -> Option<TreeBinding<'t>> {
    match_pattern(pattern, expr).map(|interiors| TreeBinding {
        root: expr,
        interiors,
    })
}

fn match_pattern<'t>(pattern: &Pattern, expr: &'t OptExpr) -> Option<Vec<&'t OptExpr>> {
    match pattern {
        Pattern::Leaf | Pattern::MultiLeaf => Some(Vec::new()),
        Pattern::Op { kind, children } => {
            if op_kind(&expr.op) != Some(*kind) {
                return None;
            }

            let mut interiors = vec![expr];
            interiors.extend(match_children(children, &expr.children)?);
            Some(interiors)
        }
    }
}

fn match_children<'t>(
    patterns: &[Pattern],
    child_exprs: &'t [OptExpr],
) -> Option<Vec<&'t OptExpr>> {
    let has_multi_leaf_tail = matches!(patterns.last(), Some(Pattern::MultiLeaf));
    let fixed_patterns = if has_multi_leaf_tail {
        &patterns[..patterns.len() - 1]
    } else {
        patterns
    };

    if fixed_patterns
        .iter()
        .any(|pattern| matches!(pattern, Pattern::MultiLeaf))
    {
        return None;
    }

    if has_multi_leaf_tail {
        if child_exprs.len() < fixed_patterns.len() {
            return None;
        }
    } else if child_exprs.len() != fixed_patterns.len() {
        return None;
    }

    let mut interiors = Vec::new();
    for (pattern, child_expr) in fixed_patterns.iter().zip(child_exprs.iter()) {
        interiors.extend(match_pattern(pattern, child_expr)?);
    }

    Some(interiors)
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::bind_tree;
    use crate::sql::common::{JoinKind, LiteralValue};
    use crate::sql::optimizer::operator::{FilterOp, LogicalJoinOp, Operator, ScanOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::pattern::{OpKind, Pattern};
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;

    fn bool_literal_scalar(arena: &mut ScalarArena) -> ScalarId {
        arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(true))),
            DataType::Boolean,
            false,
        )
    }

    fn mk_scan() -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::compiler::mv_rewrite::test_scan_source(
                    crate::sql::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn mk_filter(predicate: ScalarId, child: OptExpr) -> OptExpr {
        OptExpr::new(Operator::LogicalFilter(FilterOp { predicate }), vec![child])
    }

    fn mk_join(left: OptExpr, right: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
        )
    }

    #[test]
    fn leaf_root_matches_any_node() {
        let scan = mk_scan();

        let binding = bind_tree(&Pattern::Leaf, &scan).expect("leaf should match scan");

        assert!(ptr::eq(binding.root(), &scan));
    }

    #[test]
    fn root_multi_leaf_matches_any_node_without_interiors() {
        let scan = mk_scan();

        let binding = bind_tree(&Pattern::MultiLeaf, &scan).expect("multileaf should match scan");

        assert!(ptr::eq(binding.root(), &scan));
        assert!(binding.interiors.is_empty());
    }

    #[test]
    fn op_two_level_filter_scan_matches_and_exposes_interiors() {
        let mut arena = ScalarArena::new();
        let predicate = bool_literal_scalar(&mut arena);
        let filter = mk_filter(predicate, mk_scan());
        let pattern = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::Op {
                kind: OpKind::Scan,
                children: vec![Pattern::MultiLeaf],
            }],
        };

        let binding = bind_tree(&pattern, &filter).expect("filter scan pattern should match");

        assert!(matches!(binding.op(0), Operator::LogicalFilter(_)));
        assert!(matches!(binding.op(1), Operator::LogicalScan(_)));
        assert!(ptr::eq(binding.node(1), filter.child(0)));
    }

    #[test]
    fn non_matching_kind_returns_none() {
        let scan = mk_scan();
        let pattern = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::MultiLeaf],
        };

        assert!(bind_tree(&pattern, &scan).is_none());
    }

    #[test]
    fn exact_arity_mismatch_returns_none() {
        let mut arena = ScalarArena::new();
        let predicate = bool_literal_scalar(&mut arena);
        let filter = mk_filter(predicate, mk_scan());
        let pattern = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![],
        };

        assert!(bind_tree(&pattern, &filter).is_none());
    }

    #[test]
    fn single_node_any_arity_via_multileaf() {
        let scan = mk_scan();
        let pattern = Pattern::Op {
            kind: OpKind::Scan,
            children: vec![Pattern::MultiLeaf],
        };

        let binding = bind_tree(&pattern, &scan).expect("scan with tail multileaf should match");

        assert!(matches!(binding.op(0), Operator::LogicalScan(_)));
    }

    #[test]
    fn tail_multi_leaf_fixed_prefix_shortage_returns_none() {
        let scan = mk_scan();
        let pattern = Pattern::Op {
            kind: OpKind::Scan,
            children: vec![Pattern::Leaf, Pattern::MultiLeaf],
        };

        assert!(bind_tree(&pattern, &scan).is_none());
    }

    #[test]
    fn nested_child_failure_returns_none() {
        let mut arena = ScalarArena::new();
        let predicate = bool_literal_scalar(&mut arena);
        let filter = mk_filter(predicate, mk_scan());
        let pattern = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::Op {
                kind: OpKind::Join,
                children: vec![Pattern::MultiLeaf],
            }],
        };

        assert!(bind_tree(&pattern, &filter).is_none());
    }

    #[test]
    fn non_tail_multi_leaf_rejected() {
        let join = mk_join(mk_scan(), mk_scan());
        let pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::MultiLeaf, Pattern::Leaf],
        };

        assert!(bind_tree(&pattern, &join).is_none());
    }
}
