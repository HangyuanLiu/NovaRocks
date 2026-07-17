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

//! PruneUnionColumns — Phase 2 rule for Union nodes.
//!
//! Filters `UnionOp.output_columns` to only those whose `column_id`
//! is in `required_output_columns`. Keeps at least one column to preserve
//! a valid output schema (Gap 4).
//!
//! The set-op node's `output_columns` and `child_output_columns` metadata must
//! be pruned by the same output positions. Branch inputs are NOT modified here
//! — the Phase-1 tagging pass has already tagged each branch with the
//! position-restricted required set, and the branches' own prune rules handle
//! their pruning independently.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::column_pruning::keep_at_least_one;
use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids_ordered_opt;

pub(crate) struct PruneUnionColumns;

impl LogicalRewriteRule for PruneUnionColumns {
    fn name(&self) -> &'static str {
        "PruneUnionColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Union,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalUnion(mut node) = op else {
            unreachable!()
        };

        if !node.all {
            return Ok(RewriteResult::Unchanged);
        }

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_len = node.output_columns.len();

        // Determine which ids to keep.
        let filtered: HashSet<ColumnId> = node
            .output_columns
            .iter()
            .map(|c| c.column_id)
            .filter(|id| needed.contains(id))
            .collect();

        // Ensure at least one column survives.
        let fallback = node
            .output_columns
            .first()
            .map(|c| c.column_id)
            .unwrap_or(ColumnId::UNSET);
        let keep_ids = keep_at_least_one(filtered, fallback);

        let keep_positions: Vec<usize> = node
            .output_columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| keep_ids.contains(&column.column_id).then_some(idx))
            .collect();

        let new_output_columns: Vec<_> = keep_positions
            .iter()
            .map(|idx| node.output_columns[*idx].clone())
            .collect();

        let new_child_output_columns = if node.child_output_columns.is_empty() {
            Vec::new()
        } else {
            children
                .iter()
                .enumerate()
                .map(|(child_idx, child)| {
                    let existing = node
                        .child_output_columns
                        .get(child_idx)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]);
                    let current = child_output_columns_from_expr(child, existing);
                    keep_positions
                        .iter()
                        .filter_map(|idx| current.get(*idx).cloned())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let output_changed = new_output_columns.len() != original_len;
        let child_output_changed = !node.child_output_columns.is_empty()
            && !nested_output_column_ids_eq(&node.child_output_columns, &new_child_output_columns);

        if !output_changed && !child_output_changed {
            return Ok(RewriteResult::Unchanged);
        }

        if !node.child_output_columns.is_empty() {
            node.child_output_columns = new_child_output_columns;
        }
        node.output_columns = new_output_columns;
        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalUnion(node),
            children,
            required_output_columns,
        }))
    }
}

fn child_output_columns_from_expr(child: &OptExpr, existing: &[OutputColumn]) -> Vec<OutputColumn> {
    let ids = collect_output_ids_ordered_opt(child);
    if ids.is_empty() {
        return existing.to_vec();
    }
    ids.into_iter()
        .enumerate()
        .map(|(idx, id)| {
            existing
                .iter()
                .find(|column| column.column_id == id)
                .cloned()
                .or_else(|| {
                    existing.get(idx).cloned().map(|mut column| {
                        column.column_id = id;
                        column
                    })
                })
                .unwrap_or_else(|| OutputColumn {
                    column_id: id,
                    name: format!("col_{}", idx + 1),
                    data_type: DataType::Null,
                    nullable: true,
                    is_internal: false,
                })
        })
        .collect()
}

fn nested_output_column_ids_eq(left: &[Vec<OutputColumn>], right: &[Vec<OutputColumn>]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right.iter()).all(|(left, right)| {
            left.iter()
                .map(|column| column.column_id)
                .eq(right.iter().map(|column| column.column_id))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{Operator, UnionOp, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn dummy_input() -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }))
    }

    fn values_input(columns: Vec<OutputColumn>) -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns,
        }))
    }

    #[test]
    fn prune_union_filters_to_needed_subset() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_b);

        let mut expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_b, "b"),
                    make_output_column(id_c, "c"),
                ],
                child_output_columns: vec![],
            }),
            vec![dummy_input(), dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalUnion(pruned) = &changed.op else {
            panic!("expected Union");
        };

        assert_eq!(pruned.output_columns.len(), 1);
        assert_eq!(pruned.output_columns[0].column_id, id_b);
        // inputs are untouched
        assert_eq!(changed.children.len(), 2);
    }

    #[test]
    fn prune_union_filters_child_output_columns_by_position() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);
        let left_a = ColumnId::new_for_test(11);
        let left_b = ColumnId::new_for_test(12);
        let left_c = ColumnId::new_for_test(13);
        let right_a = ColumnId::new_for_test(21);
        let right_b = ColumnId::new_for_test(22);
        let right_c = ColumnId::new_for_test(23);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_c);

        let mut expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_b, "b"),
                    make_output_column(id_c, "c"),
                ],
                child_output_columns: vec![
                    vec![
                        make_output_column(left_a, "left_a"),
                        make_output_column(left_b, "left_b"),
                        make_output_column(left_c, "left_c"),
                    ],
                    vec![
                        make_output_column(right_a, "right_a"),
                        make_output_column(right_b, "right_b"),
                        make_output_column(right_c, "right_c"),
                    ],
                ],
            }),
            vec![dummy_input(), dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalUnion(pruned) = &changed.op else {
            panic!("expected Union");
        };

        assert_eq!(
            pruned
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![id_a, id_c]
        );
        assert_eq!(
            pruned.child_output_columns[0]
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![left_a, left_c]
        );
        assert_eq!(
            pruned.child_output_columns[1]
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![right_a, right_c]
        );
    }

    #[test]
    fn prune_union_syncs_child_output_columns_even_when_schema_width_is_unchanged() {
        let id_a = ColumnId::new_for_test(1);
        let id_row = ColumnId::new_for_test(6);
        let stale_left_row = ColumnId::new_for_test(7);
        let stale_right_row = ColumnId::new_for_test(8);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_row);

        let mut expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_row, "_row_id"),
                ],
                child_output_columns: vec![
                    vec![
                        make_output_column(id_a, "left_a"),
                        make_output_column(stale_left_row, "_row_id"),
                    ],
                    vec![
                        make_output_column(id_a, "right_a"),
                        make_output_column(stale_right_row, "_row_id"),
                    ],
                ],
            }),
            vec![
                values_input(vec![
                    make_output_column(id_a, "left_a"),
                    make_output_column(id_row, "_row_id"),
                ]),
                values_input(vec![
                    make_output_column(id_a, "right_a"),
                    make_output_column(id_row, "_row_id"),
                ]),
            ],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalUnion(pruned) = &changed.op else {
            panic!("expected Union");
        };
        assert_eq!(
            pruned
                .child_output_columns
                .iter()
                .map(|columns| columns[1].column_id)
                .collect::<Vec<_>>(),
            vec![id_row, id_row]
        );
    }

    #[test]
    fn prune_union_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);

        let expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: false,
                output_columns: vec![make_output_column(id_a, "a")],
                child_output_columns: vec![],
            }),
            vec![dummy_input()],
        );
        // required_output_columns = None (default), also all=false

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_union_keeps_at_least_one_when_needed_empty() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        // needed is empty — must keep first column.
        let mut expr = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
                child_output_columns: vec![],
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(HashSet::new());

        let rule = PruneUnionColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalUnion(pruned) = &changed.op else {
            panic!("expected Union");
        };

        assert_eq!(pruned.output_columns.len(), 1);
        assert_eq!(pruned.output_columns[0].column_id, id_a, "first col kept");
    }
}
