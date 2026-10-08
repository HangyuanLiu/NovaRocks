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

//! Prune unused literal VALUES columns and lift row-invariant literals.
//!
//! Nonliteral cells remain evaluated even when their output is unused. A
//! lifted literal retains the column's exact type and nullability, while one
//! source column always remains to preserve row multiplicity.

use crate::column_id::ColumnId;
use crate::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::optimizer::scalar::ScalarNode;

pub(crate) struct PruneValuesColumns;

impl LogicalRewriteRule for PruneValuesColumns {
    fn name(&self) -> &'static str {
        "PruneValuesColumns"
    }
    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }
    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Values,
            children: vec![],
        }
    }
    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }
    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalValues(mut node) = op else {
            unreachable!()
        };
        let Some(needed) = &required_output_columns else {
            return Ok(RewriteResult::Unchanged);
        };
        if node.columns.is_empty() || node.rows.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }
        if node.rows.iter().any(|row| row.len() != node.columns.len()) {
            return Err("VALUES column pruning requires exact row width".into());
        }
        let arena_rc = ctx.scalar_arena();
        let mut arena = arena_rc.borrow_mut();
        let mut keep = node
            .columns
            .iter()
            .enumerate()
            .map(|(i, column)| {
                column.column_id == ColumnId::UNSET
                    || needed.contains(&column.column_id)
                    || node
                        .rows
                        .iter()
                        .any(|row| !matches!(arena.node(row[i]), ScalarNode::Literal(_)))
            })
            .collect::<Vec<_>>();
        if !keep.iter().any(|keep| *keep) {
            keep[0] = true;
        }
        let mut lifted = keep
            .iter()
            .enumerate()
            .map(|(i, keep)| {
                *keep
                    && matches!(arena.node(node.rows[0][i]), ScalarNode::Literal(_))
                    && arena.data_type(node.rows[0][i]) == &node.columns[i].data_type
                    && node.rows.iter().all(|row| row[i] == node.rows[0][i])
            })
            .collect::<Vec<_>>();
        // Even an all-constant relation needs one cell per original row.
        if keep.iter().zip(&lifted).all(|(keep, lift)| !keep || *lift) {
            lifted[keep
                .iter()
                .position(|keep| *keep)
                .expect("one column remains")] = false;
        }
        if keep.iter().all(|keep| *keep) && !lifted.iter().any(|lift| *lift) {
            return Ok(RewriteResult::Unchanged);
        }
        let items = node
            .columns
            .iter()
            .enumerate()
            .filter(|(i, _)| keep[*i])
            .map(|(i, column)| {
                let scalar = if lifted[i] {
                    arena.node(node.rows[0][i]).clone()
                } else {
                    ScalarNode::ColumnRef(column.column_id)
                };
                ScalarProjectItem {
                    expr: arena.intern(scalar, column.data_type.clone(), column.nullable),
                    output_name: column.name.clone(),
                    output_column_id: column.column_id,
                    expr_display: None,
                }
            })
            .collect();
        drop(arena);
        let source = keep
            .iter()
            .zip(&lifted)
            .map(|(keep, lift)| *keep && !lift)
            .collect::<Vec<_>>();
        node.columns = node
            .columns
            .into_iter()
            .enumerate()
            .filter_map(|(i, column)| source[i].then_some(column))
            .collect();
        node.rows = node
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .filter_map(|(i, cell)| source[i].then_some(cell))
                    .collect()
            })
            .collect();
        let inner_required = Some(node.columns.iter().map(|column| column.column_id).collect());
        let inner = OptExpr {
            op: Operator::LogicalValues(node),
            children,
            required_output_columns: inner_required,
        };
        let rewritten = if lifted.iter().any(|lift| *lift) {
            OptExpr {
                op: Operator::LogicalProject(ProjectOp {
                    items,
                    output_qualifier: None,
                }),
                children: vec![inner],
                required_output_columns,
            }
        } else {
            OptExpr {
                required_output_columns,
                ..inner
            }
        };
        Ok(RewriteResult::Changed(rewritten))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::OutputColumn;
    use crate::common::LiteralValue;
    use crate::optimizer::operator::ValuesOp;
    use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId};
    use arrow::datatypes::DataType;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    fn column(id: u32) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: format!("c{id}"),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        }
    }
    fn literal(arena: &mut ScalarArena, text: &str) -> ScalarId {
        arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::String(text.into()))),
            DataType::Utf8,
            false,
        )
    }
    fn context(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }
    fn values(rows: Vec<Vec<ScalarId>>, needed: &[u32]) -> OptExpr {
        OptExpr {
            op: Operator::LogicalValues(ValuesOp {
                columns: (1..=rows[0].len() as u32).map(column).collect(),
                rows,
            }),
            children: vec![],
            required_output_columns: Some(
                needed
                    .iter()
                    .map(|id| ColumnId(*id))
                    .collect::<HashSet<_>>(),
            ),
        }
    }
    fn rewrite(expr: OptExpr, ctx: &mut RewriteContext) -> OptExpr {
        let RewriteResult::Changed(rewritten) = PruneValuesColumns.apply(expr, ctx).unwrap() else {
            panic!("expected compact VALUES")
        };
        rewritten
    }

    #[test]
    fn catalog_values_lift_only_identical_literals_and_preserve_duplicate_rows() {
        let mut arena = ScalarArena::new();
        let catalog = literal(&mut arena, "catalog");
        let schema_a = literal(&mut arena, "a");
        let schema_b = literal(&mut arena, "b");
        let table = literal(&mut arena, "table");
        let rows = vec![
            vec![catalog, schema_a, table],
            vec![catalog, schema_a, table],
            vec![catalog, schema_b, table],
        ];
        let mut ctx = context(arena);
        let rewritten = rewrite(values(rows, &[1, 2]), &mut ctx);
        let Operator::LogicalProject(project) = rewritten.op else {
            panic!("expected lifted constants")
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
        let arena_rc = ctx.scalar_arena();
        let arena = arena_rc.borrow();
        assert_eq!(arena.data_type(project.items[0].expr), &DataType::Utf8);
        assert!(arena.nullable(project.items[0].expr));
        assert!(matches!(
            arena.node(project.items[0].expr),
            ScalarNode::Literal(_)
        ));
        let Operator::LogicalValues(source) = &rewritten.children[0].op else {
            panic!("expected VALUES source")
        };
        assert_eq!(source.columns.len(), 1);
        assert_eq!(source.columns[0].column_id, ColumnId(2));
        assert_eq!(
            source.rows,
            vec![vec![schema_a], vec![schema_a], vec![schema_b]]
        );
    }

    #[test]
    fn count_only_keeps_one_cell_per_row_in_an_all_constant_relation() {
        let mut arena = ScalarArena::new();
        let cell = literal(&mut arena, "same");
        let mut ctx = context(arena);
        let rewritten = rewrite(values(vec![vec![cell, cell]; 3], &[]), &mut ctx);
        let Operator::LogicalValues(source) = rewritten.op else {
            panic!("expected VALUES source")
        };
        assert_eq!(source.columns.len(), 1);
        assert_eq!(source.rows, vec![vec![cell]; 3]);
    }

    #[test]
    fn unused_nonliteral_cells_are_preserved_for_runtime_validation() {
        let mut arena = ScalarArena::new();
        let a = literal(&mut arena, "a");
        let b = literal(&mut arena, "b");
        let bad_cast = arena.intern(
            ScalarNode::Cast {
                child: a,
                target: DataType::Int64,
                decimal_overflow_policy:
                    novarocks_type_contract::DecimalOverflowPolicy::ReportError,
            },
            DataType::Int64,
            true,
        );
        let mut ctx = context(arena);
        let mut input = values(vec![vec![a, a, bad_cast], vec![a, b, bad_cast]], &[2]);
        let Operator::LogicalValues(source) = &mut input.op else {
            unreachable!()
        };
        source.columns[2].data_type = DataType::Int64;
        let rewritten = rewrite(input, &mut ctx);
        let Operator::LogicalValues(source) = rewritten.op else {
            panic!("expected VALUES source")
        };
        assert_eq!(
            source
                .columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(2), ColumnId(3)]
        );
        assert_eq!(source.rows, vec![vec![a, bad_cast], vec![b, bad_cast]]);
    }
}
