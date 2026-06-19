//! PushDownPredicateProject — `Filter(Project)` rewrite.
//!
//! Pushes conjuncts that reference only pass-through (i.e. bare
//! `ColumnRef`) projection items below the Project, leaving conjuncts
//! that touch computed expressions as a residual Filter above. One step
//! only — the rewrite pipeline's bottom-up walker will push further at the
//! next round.
//!
//! Migrated to `OptExpr` / `LogicalRewriteRule`.

use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{FilterOp, Operator, ProjectOp};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{
    combine_and, split_and, wrap_remaining_filter_opt,
};
use crate::sql::optimizer::scalar::{self, ScalarArena};

pub(crate) struct PushDownPredicateProject;

impl LogicalRewriteRule for PushDownPredicateProject {
    fn name(&self) -> &'static str {
        "PushDownPredicateProject"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalFilter(_))
            && expr
                .children
                .first()
                .map(|c| matches!(&c.op, Operator::LogicalProject(_)))
                .unwrap_or(false)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            mut children,
            required_output_columns: _,
        } = expr;
        let Operator::LogicalFilter(filter) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let project_expr = children.remove(0);
        let OptExpr {
            op: project_op,
            children: mut project_children,
            required_output_columns,
        } = project_expr;
        let Operator::LogicalProject(proj) = project_op else {
            return Ok(RewriteResult::Unchanged);
        };
        if project_children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let project_input = project_children.remove(0);

        let arena_rc = ctx.scalar_arena();
        let predicate_typed = {
            let arena = arena_rc.borrow();
            scalar::materialize(&arena, filter.predicate)
        };

        let conjuncts = split_and(predicate_typed);
        let mut pushable: Vec<TypedExpr> = Vec::new();
        let mut remaining: Vec<TypedExpr> = Vec::new();
        for conj in conjuncts {
            match rewrite_predicate_through_project(&conj, &proj, &arena_rc.borrow()) {
                Some(rewritten) => pushable.push(rewritten),
                None => remaining.push(conj),
            }
        }

        if pushable.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        let pushed = combine_and(pushable);
        let pushed_id = scalar::intern_typed(&mut arena_rc.borrow_mut(), &pushed);
        let new_child = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![project_input],
        );
        let new_project = OptExpr {
            op: Operator::LogicalProject(ProjectOp {
                items: proj.items,
                output_qualifier: proj.output_qualifier,
            }),
            children: vec![new_child],
            required_output_columns,
        };
        let result = wrap_remaining_filter_opt(new_project, remaining, &mut arena_rc.borrow_mut());
        Ok(RewriteResult::Changed(result))
    }
}

/// Try to rewrite `expr` (a predicate fragment) so it references only the
/// Project's input columns rather than its output column names. Returns `None`
/// if any `ColumnRef` in `expr` maps to a computed (non-passthrough) item.
fn rewrite_predicate_through_project(
    expr: &TypedExpr,
    proj: &ProjectOp,
    arena: &ScalarArena,
) -> Option<TypedExpr> {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => lookup_passthrough_projection(*column_id, qualifier.as_deref(), column, proj, arena),
        ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => Some(expr.clone()),
        ExprKind::BinaryOp { left, op, right } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(rewrite_predicate_through_project(left, proj, arena)?),
                op: *op,
                right: Box::new(rewrite_predicate_through_project(right, proj, arena)?),
            },
        }),
        ExprKind::UnaryOp { op, expr: inner } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::UnaryOp {
                op: *op,
                expr: Box::new(rewrite_predicate_through_project(inner, proj, arena)?),
            },
        }),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::FunctionCall {
                name: name.clone(),
                args: rewrite_expr_list_through_project(args, proj, arena)?,
                distinct: *distinct,
            },
        }),
        ExprKind::LambdaFunction { params, body } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::LambdaFunction {
                params: params.clone(),
                body: Box::new(rewrite_predicate_through_project(body, proj, arena)?),
            },
        }),
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::AggregateCall {
                name: name.clone(),
                args: rewrite_expr_list_through_project(args, proj, arena)?,
                distinct: *distinct,
                order_by: order_by
                    .iter()
                    .map(|item| {
                        rewrite_predicate_through_project(&item.expr, proj, arena).map(|expr| {
                            SortItem {
                                expr,
                                asc: item.asc,
                                nulls_first: item.nulls_first,
                            }
                        })
                    })
                    .collect::<Option<Vec<_>>>()?,
            },
        }),
        ExprKind::Cast {
            expr: inner,
            target,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Cast {
                expr: Box::new(rewrite_predicate_through_project(inner, proj, arena)?),
                target: target.clone(),
            },
        }),
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::IsNull {
                expr: Box::new(rewrite_predicate_through_project(inner, proj, arena)?),
                negated: *negated,
            },
        }),
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::InList {
                expr: Box::new(rewrite_predicate_through_project(inner, proj, arena)?),
                list: rewrite_expr_list_through_project(list, proj, arena)?,
                negated: *negated,
            },
        }),
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Between {
                expr: Box::new(rewrite_predicate_through_project(inner, proj, arena)?),
                low: Box::new(rewrite_predicate_through_project(low, proj, arena)?),
                high: Box::new(rewrite_predicate_through_project(high, proj, arena)?),
                negated: *negated,
            },
        }),
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Like {
                expr: Box::new(rewrite_predicate_through_project(inner, proj, arena)?),
                pattern: Box::new(rewrite_predicate_through_project(pattern, proj, arena)?),
                negated: *negated,
            },
        }),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Case {
                operand: match operand {
                    Some(operand) => Some(Box::new(rewrite_predicate_through_project(
                        operand, proj, arena,
                    )?)),
                    None => None,
                },
                when_then: when_then
                    .iter()
                    .map(|(when, then)| {
                        Some((
                            rewrite_predicate_through_project(when, proj, arena)?,
                            rewrite_predicate_through_project(then, proj, arena)?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
                else_expr: match else_expr {
                    Some(else_expr) => Some(Box::new(rewrite_predicate_through_project(
                        else_expr, proj, arena,
                    )?)),
                    None => None,
                },
            },
        }),
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::IsTruthValue {
                expr: Box::new(rewrite_predicate_through_project(inner, proj, arena)?),
                value: *value,
                negated: *negated,
            },
        }),
        ExprKind::Nested(inner) => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Nested(Box::new(rewrite_predicate_through_project(
                inner, proj, arena,
            )?)),
        }),
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::WindowCall {
                name: name.clone(),
                args: rewrite_expr_list_through_project(args, proj, arena)?,
                distinct: *distinct,
                partition_by: rewrite_expr_list_through_project(partition_by, proj, arena)?,
                order_by: order_by
                    .iter()
                    .map(|item| {
                        rewrite_predicate_through_project(&item.expr, proj, arena).map(|expr| {
                            SortItem {
                                expr,
                                asc: item.asc,
                                nulls_first: item.nulls_first,
                            }
                        })
                    })
                    .collect::<Option<Vec<_>>>()?,
                window_frame: window_frame.clone(),
                ignore_nulls: *ignore_nulls,
            },
        }),
        ExprKind::SubqueryPlaceholder { .. } => Some(expr.clone()),
        ExprKind::Lambda { params, body } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Lambda {
                params: params.clone(),
                body: Box::new(rewrite_predicate_through_project(body, proj, arena)?),
            },
        }),
    }
}

fn rewrite_expr_list_through_project(
    exprs: &[TypedExpr],
    proj: &ProjectOp,
    arena: &ScalarArena,
) -> Option<Vec<TypedExpr>> {
    exprs
        .iter()
        .map(|expr| rewrite_predicate_through_project(expr, proj, arena))
        .collect()
}

/// Look up a ColumnRef in the project items. Returns the underlying input
/// expression only when the item is a passthrough (bare ColumnRef) item.
/// The item's `expr` is a `ScalarId` — we materialize it to check if it's a
/// ColumnRef, and if so return the materialized expression as the rewritten ref.
fn lookup_passthrough_projection(
    column_id: ColumnId,
    qualifier: Option<&str>,
    column: &str,
    proj: &ProjectOp,
    arena: &ScalarArena,
) -> Option<TypedExpr> {
    use crate::sql::optimizer::scalar::ScalarNode;
    for item in &proj.items {
        // Check directly whether the item is a bare ColumnRef without going
        // through the display-lookup path (which can be polluted by later
        // intern_typed calls that overwrite the qualifier for the same column_id).
        let ScalarNode::ColumnRef(input_col_id) = arena.node(item.expr) else {
            continue;
        };
        let input_col_id = *input_col_id;
        // The pushed predicate must reference the INPUT column without the
        // project's output_qualifier — the qualifier belongs to the project's
        // aliased output, not the underlying column.
        let stripped = TypedExpr {
            data_type: arena.data_type(item.expr).clone(),
            nullable: arena.nullable(item.expr),
            kind: ExprKind::ColumnRef {
                column_id: input_col_id,
                qualifier: None,
                column: item.output_name.clone(),
            },
        };
        if column_id != ColumnId::UNSET && item.output_column_id == column_id {
            return Some(stripped);
        }
        if let Some(ref output_qualifier) = proj.output_qualifier
            && !qualifier
                .map(|q| q.eq_ignore_ascii_case(output_qualifier))
                .unwrap_or(true)
        {
            continue;
        }
        if item.output_name.eq_ignore_ascii_case(column) {
            return Some(stripped);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        FilterOp, Operator, ProjectOp, ScalarProjectItem, ScanOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::{self, ScalarArena};
    use arrow::datatypes::DataType;

    // Helper: intern a TypedExpr that uses ColumnId::UNSET (used for
    // passthrough project tests where column ids are not resolved).
    // NOTE: intern_typed panics on UNSET column refs — so we build items
    // using real ColumnIds here. Use ColumnId::new_for_test to keep tests
    // simple with stable ids.

    fn col_id(n: u32) -> ColumnId {
        ColumnId::new_for_test(n)
    }

    fn col_ref(name: &str, id: ColumnId) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn qualified_col_ref(qualifier: &str, name: &str, id: ColumnId) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: Some(qualifier.into()),
                column: name.into(),
            },
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn is_not_null(expr: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(expr),
                negated: true,
            },
        }
    }

    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }

    fn and(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            },
        }
    }

    fn make_table_def(cols: &[(&str, ColumnId)]) -> TableDef {
        TableDef {
            name: "t".into(),
            columns: cols
                .iter()
                .map(|(n, _)| ColumnDef {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn scan_opt(arena: &mut ScalarArena, cols: &[(&str, ColumnId)]) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: make_table_def(cols),
            alias: None,
            columns: cols
                .iter()
                .map(|(n, id)| OutputColumn {
                    column_id: *id,
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    /// Build a passthrough project: each column in `cols` is a bare ColumnRef.
    fn passthrough_project_opt(
        arena: &mut ScalarArena,
        cols: &[(&str, ColumnId)],
        output_qualifier: Option<String>,
        input: OptExpr,
    ) -> OptExpr {
        let items: Vec<ScalarProjectItem> = cols
            .iter()
            .map(|(name, id)| {
                let expr_id = scalar::intern_typed(arena, &col_ref(name, *id));
                ScalarProjectItem {
                    expr: expr_id,
                    output_name: (*name).into(),
                    output_column_id: *id,
                    expr_display: None,
                }
            })
            .collect();
        OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items,
                output_qualifier,
            }),
            vec![input],
        )
    }

    fn filter_opt(arena: &mut ScalarArena, predicate: TypedExpr, child: OptExpr) -> OptExpr {
        let pred_id = scalar::intern_typed(arena, &predicate);
        OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred_id }),
            vec![child],
        )
    }

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(std::iter::empty::<String>());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    // Test 1: SELECT a, b FROM (SELECT a, b FROM t) WHERE a = 1
    // Expected: Project(Filter(Scan)) — the predicate is pushed below the project.
    #[test]
    fn pushes_through_passthrough_project() {
        let mut arena = ScalarArena::new();
        let a_id = col_id(1);
        let b_id = col_id(2);
        let scan = scan_opt(&mut arena, &[("a", a_id), ("b", b_id)]);
        let project = passthrough_project_opt(&mut arena, &[("a", a_id), ("b", b_id)], None, scan);
        let filter = filter_opt(&mut arena, eq(col_ref("a", a_id), int_lit(1)), project);

        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        assert!(rule.matches(&filter, &ctx));
        let result = rule.apply(filter, &mut ctx).unwrap();

        match result {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalProject(_) => match &out.children[0].op {
                    Operator::LogicalFilter(_) => match &out.children[0].children[0].op {
                        Operator::LogicalScan(_) => {}
                        other => panic!("expected Scan under Filter, got {:?}", other),
                    },
                    other => panic!("expected Filter under Project, got {:?}", other),
                },
                other => panic!("expected Project at top, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    #[test]
    fn rewrites_qualified_alias_predicate_before_pushdown() {
        let mut arena = ScalarArena::new();
        let item_sk_id = col_id(10);
        let scan = scan_opt(&mut arena, &[("item_sk", item_sk_id)]);
        let project = passthrough_project_opt(
            &mut arena,
            &[("item_sk", item_sk_id)],
            Some("asceding".into()),
            scan,
        );
        let filter = filter_opt(
            &mut arena,
            is_not_null(qualified_col_ref("asceding", "item_sk", item_sk_id)),
            project,
        );

        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(filter, &mut ctx).unwrap();

        match result {
            RewriteResult::Changed(out) => {
                assert!(matches!(out.op, Operator::LogicalProject(_)));
                let inner = &out.children[0];
                let Operator::LogicalFilter(inner_filter) = &inner.op else {
                    panic!("expected pushed Filter below Project");
                };
                let arena_ref = ctx.scalar_arena();
                let arena = arena_ref.borrow();
                let pred_expr = scalar::materialize(&arena, inner_filter.predicate);
                let ExprKind::IsNull { expr, negated } = &pred_expr.kind else {
                    panic!("expected pushed IS NOT NULL predicate");
                };
                assert!(*negated);
                let ExprKind::ColumnRef {
                    column_id: pushed_col_id,
                    column,
                    ..
                } = &expr.kind
                else {
                    panic!("expected pushed predicate to reference the Project input column");
                };
                // The pushed predicate must reference the underlying input column
                // (by column_id, which is the semantic identity in the arena model).
                // The qualifier is display-only and may reflect the intern_typed order,
                // so we do not assert on it here.
                assert_eq!(*pushed_col_id, item_sk_id);
                assert_eq!(column, "item_sk");
            }
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    // Test 2: SELECT a+1 AS x FROM t WHERE x = 5
    // No conjuncts are pushable; rule must return Unchanged.
    #[test]
    fn does_not_push_through_computed_projection() {
        let mut arena = ScalarArena::new();
        let a_id = col_id(1);
        let x_id = col_id(2);
        let scan = scan_opt(&mut arena, &[("a", a_id)]);
        // Build: Project(Scan) with computed item x = a + 1.
        let computed_expr = TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref("a", a_id)),
                op: BinOp::Add,
                right: Box::new(int_lit(1)),
            },
        };
        let computed_id = scalar::intern_typed(&mut arena, &computed_expr);
        let project = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: computed_id,
                    output_name: "x".into(),
                    output_column_id: x_id,
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            vec![scan],
        );
        let filter = filter_opt(&mut arena, eq(col_ref("x", x_id), int_lit(5)), project);

        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        assert!(rule.matches(&filter, &ctx));
        let result = rule.apply(filter, &mut ctx).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "should not push through a computed projection"
        );
    }

    // Test 4: WHERE 1=1 (constant predicate, no column refs).
    // Expected shape: Project(Filter(Scan))
    #[test]
    fn pushes_constant_predicate_through_project() {
        let mut arena = ScalarArena::new();
        let a_id = col_id(1);
        let scan = scan_opt(&mut arena, &[("a", a_id)]);
        let project = passthrough_project_opt(&mut arena, &[("a", a_id)], None, scan);
        let one_eq_one = eq(int_lit(1), int_lit(1));
        let filter = filter_opt(&mut arena, one_eq_one, project);
        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(filter, &mut ctx).unwrap();
        match result {
            RewriteResult::Changed(out) => {
                assert!(matches!(out.op, Operator::LogicalProject(_)));
                assert!(matches!(out.children[0].op, Operator::LogicalFilter(_)));
            }
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    // Test 3: AND of a pass-through ref (a = 1) and a computed-expr ref (x = 5)
    // Expected shape: Filter(Project(Filter(Scan)))
    #[test]
    fn partial_pushdown_through_project() {
        let mut arena = ScalarArena::new();
        let a_id = col_id(1);
        let x_id = col_id(2);
        let scan = scan_opt(&mut arena, &[("a", a_id)]);
        let computed_expr = TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref("a", a_id)),
                op: BinOp::Add,
                right: Box::new(int_lit(1)),
            },
        };
        let passthrough_id = scalar::intern_typed(&mut arena, &col_ref("a", a_id));
        let computed_id = scalar::intern_typed(&mut arena, &computed_expr);
        let project = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![
                    ScalarProjectItem {
                        expr: passthrough_id,
                        output_name: "a".into(),
                        output_column_id: a_id,
                        expr_display: None,
                    },
                    ScalarProjectItem {
                        expr: computed_id,
                        output_name: "x".into(),
                        output_column_id: x_id,
                        expr_display: None,
                    },
                ],
                output_qualifier: None,
            }),
            vec![scan],
        );
        let pred = and(
            eq(col_ref("a", a_id), int_lit(1)),
            eq(col_ref("x", x_id), int_lit(5)),
        );
        let filter = filter_opt(&mut arena, pred, project);

        let rule = PushDownPredicateProject;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(filter, &mut ctx).unwrap();

        // Expected: Filter(Project(Filter(Scan)))
        match result {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalFilter(_) => match &out.children[0].op {
                    Operator::LogicalProject(_) => match &out.children[0].children[0].op {
                        Operator::LogicalFilter(_) => {
                            match &out.children[0].children[0].children[0].op {
                                Operator::LogicalScan(_) => {}
                                other => panic!("expected Scan at bottom, got {:?}", other),
                            }
                        }
                        other => panic!("expected Filter under Project, got {:?}", other),
                    },
                    other => panic!("expected Project under outer Filter, got {:?}", other),
                },
                other => panic!("expected outer Filter at top, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }
}
