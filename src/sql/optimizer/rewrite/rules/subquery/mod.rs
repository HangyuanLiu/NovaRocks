//! SubqueryRewrite stage rules.
//!
//! M0 ships only the ApplyException terminal guard. The decorrelation rules
//! (push-down normalization, ApplyToWindow, *ApplyToJoin) land with M1+; see
//! docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md §6.

mod apply_exception;
mod apply_to_window;
mod decorrelate_util;
mod existential_apply_to_join;
mod predicate_apply_util;
mod push_down_apply_agg_filter;
mod push_down_apply_filter;
mod quantified_apply_to_join;
mod scalar_apply_to_join;
mod win_magic_util;

pub(crate) use apply_exception::ApplyException;
pub(crate) use apply_to_window::ApplyToWindow;
#[allow(unused_imports)]
pub(crate) use existential_apply_to_join::ExistentialApplyToJoin;
pub(crate) use push_down_apply_agg_filter::PushDownApplyAggFilter;
pub(crate) use push_down_apply_filter::PushDownApplyFilter;
#[allow(unused_imports)]
pub(crate) use quantified_apply_to_join::QuantifiedApplyToJoin;
pub(crate) use scalar_apply_to_join::ScalarApplyToJoin;

use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) fn subquery_rewrite_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![
        Box::new(PushDownApplyAggFilter),
        Box::new(PushDownApplyFilter),
        Box::new(ApplyToWindow), // to-window BEFORE to-join (StarRocks ordering)
        Box::new(ScalarApplyToJoin),
        Box::new(ExistentialApplyToJoin), // EXISTS / NOT EXISTS -> LeftSemi / LeftAnti
        Box::new(QuantifiedApplyToJoin),  // IN / NOT IN -> LeftSemi / NullAwareLeftAnti|LeftAnti
        Box::new(ApplyException),         // must stay LAST
    ]
}

/// Non-disableable backstop used by `optimize()`: returns the ApplyException
/// error if any Apply survived the rewrite pipeline (possible when the user
/// disabled the ApplyException rule via `disable_optimizer_rules`); a leaked
/// Apply must surface as a user-readable error, never as the memo-conversion
/// panic.
pub(crate) fn find_residual_apply(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Apply(node) => Some(apply_exception::apply_exception_message(node)),
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => None,
        LogicalPlan::Filter(n) => find_residual_apply(&n.input),
        LogicalPlan::Project(n) => find_residual_apply(&n.input),
        LogicalPlan::Aggregate(n) => find_residual_apply(&n.input),
        LogicalPlan::Join(n) => {
            find_residual_apply(&n.left).or_else(|| find_residual_apply(&n.right))
        }
        LogicalPlan::Sort(n) => find_residual_apply(&n.input),
        LogicalPlan::Limit(n) => find_residual_apply(&n.input),
        LogicalPlan::Union(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::Intersect(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::Except(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::TableFunction(n) => find_residual_apply(&n.input),
        LogicalPlan::Window(n) => find_residual_apply(&n.input),
        LogicalPlan::Repeat(n) => find_residual_apply(&n.input),
        LogicalPlan::CTEAnchor(n) => {
            find_residual_apply(&n.produce).or_else(|| find_residual_apply(&n.consumer))
        }
        LogicalPlan::CTEProduce(n) => find_residual_apply(&n.input),
        LogicalPlan::Decode(n) => find_residual_apply(&n.input),
        LogicalPlan::AggregateStateMerge(n) => {
            find_residual_apply(&n.old_input).or_else(|| find_residual_apply(&n.delta_input))
        }
        LogicalPlan::AssertOneRow(n) => find_residual_apply(&n.input),
        LogicalPlan::ImvDelta(n) => find_residual_apply(&n.input),
        LogicalPlan::ImvVersion(n) => find_residual_apply(&n.input),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::planner::plan::{ApplyKind, ApplyNode, LimitNode, ValuesNode};

    fn empty_values() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        })
    }

    fn apply_over_values() -> LogicalPlan {
        LogicalPlan::Apply(ApplyNode {
            left: Box::new(empty_values()),
            right: Box::new(empty_values()),
            kind: ApplyKind::Scalar,
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(5),
                    qualifier: None,
                    column: "sq".to_string(),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            output_column: OutputColumn {
                column_id: ColumnId(5),
                name: "sq".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: true,
            },
            inner_output_column_id: ColumnId(5),
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        })
    }

    fn exists_apply_over_values() -> LogicalPlan {
        LogicalPlan::Apply(ApplyNode {
            left: Box::new(empty_values()),
            right: Box::new(empty_values()),
            kind: ApplyKind::Exists { negated: false },
            subquery_expr: TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
                data_type: DataType::Boolean,
                nullable: false,
            },
            output_column: OutputColumn {
                column_id: ColumnId(6),
                name: "exists".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                is_internal: true,
            },
            inner_output_column_id: ColumnId(7),
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: true,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        })
    }

    fn int_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn int_column_ref(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn in_apply_over_values() -> LogicalPlan {
        let outer_col = int_output_column(ColumnId(8), "outer_v");
        let inner_col = int_output_column(ColumnId(9), "inner_v");

        LogicalPlan::Apply(ApplyNode {
            left: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![outer_col.clone()],
                required_output_columns: None,
            })),
            right: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![inner_col.clone()],
                required_output_columns: None,
            })),
            kind: ApplyKind::In { negated: false },
            subquery_expr: int_column_ref(outer_col.column_id, &outer_col.name),
            output_column: OutputColumn {
                column_id: ColumnId(10),
                name: "in_result".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                is_internal: true,
            },
            inner_output_column_id: inner_col.column_id,
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: true,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        })
    }

    #[test]
    fn apply_exception_never_fires_for_decorrelatable_apply() {
        // M1b: an uncorrelated scalar Apply over empty Values is handled by
        // ScalarApplyToJoin (CROSS JOIN + pass-through Project). ApplyException
        // is never reached; the pipeline succeeds.
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let result = query_rewrite_pipeline(&HashMap::new())
            .rewrite(apply_over_values(), &mut ctx)
            .expect("pipeline must succeed: ScalarApplyToJoin eliminates the Apply");
        // The Apply must be gone — rewritten to a Project wrapping a CrossJoin.
        assert!(
            find_residual_apply(&result).is_none(),
            "no Apply must survive after ScalarApplyToJoin fires"
        );
    }

    #[test]
    fn pipeline_eliminates_uncorrelated_exists_apply() {
        // EXISTS over an uncorrelated empty Values subtree should be rewritten
        // by ExistentialApplyToJoin into a LeftSemi join before ApplyException.
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let result = query_rewrite_pipeline(&HashMap::new())
            .rewrite(exists_apply_over_values(), &mut ctx)
            .expect("pipeline must succeed: ExistentialApplyToJoin eliminates the Apply");
        assert!(
            find_residual_apply(&result).is_none(),
            "no Apply must survive after ExistentialApplyToJoin fires"
        );
    }

    #[test]
    fn pipeline_eliminates_uncorrelated_in_apply() {
        // IN over uncorrelated Values should be rewritten by
        // QuantifiedApplyToJoin before ApplyException runs.
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let result = query_rewrite_pipeline(&HashMap::new())
            .rewrite(in_apply_over_values(), &mut ctx)
            .expect("pipeline must succeed: QuantifiedApplyToJoin eliminates the Apply");
        assert!(
            find_residual_apply(&result).is_none(),
            "no Apply must survive after QuantifiedApplyToJoin fires"
        );
    }

    #[test]
    fn disabled_apply_exception_is_caught_by_backstop() {
        // Disable rules that could eliminate this scalar Apply so it survives
        // to the backstop.
        let mut ctx = RewriteContext::for_query(vec![
            "ApplyException".to_string(),
            "ScalarApplyToJoin".to_string(),
            "PushDownApplyAggFilter".to_string(),
            "PushDownApplyFilter".to_string(),
        ]);
        let rewritten = query_rewrite_pipeline(&HashMap::new())
            .rewrite(apply_over_values(), &mut ctx)
            .expect("pipeline passes with scalar Apply-elimination rules disabled");
        let message = find_residual_apply(&rewritten).expect("backstop must detect the apply");
        assert!(message.contains("subquery decorrelation failed"));
    }

    #[test]
    fn find_residual_apply_ignores_plain_plans() {
        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        });
        assert!(find_residual_apply(&plan).is_none());
    }

    #[test]
    fn find_residual_apply_finds_apply_nested_under_unary() {
        // Apply one level below a unary container: exercises the walker's
        // recursive descent, not just the root case.
        let plan = LogicalPlan::Limit(LimitNode {
            input: Box::new(apply_over_values()),
            limit: Some(1),
            offset: None,
            required_output_columns: None,
        });
        let message = find_residual_apply(&plan).expect("walker must find the nested apply");
        assert!(
            message.contains("subquery decorrelation failed"),
            "unexpected message: {message}"
        );
    }
}
