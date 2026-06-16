use std::time::Instant;

use crate::sql::optimizer::rewrite::context::{RewriteContext, RewriteFailurePolicy};
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlanNode;

pub(crate) fn rewrite_with_rule(
    plan: LogicalPlanNode,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlanNode, bool), String> {
    match rule.traversal() {
        RewriteTraversal::TopDown => rewrite_top_down(plan, rule, ctx),
        RewriteTraversal::BottomUp => rewrite_bottom_up(plan, rule, ctx),
    }
}

fn rewrite_top_down(
    plan: LogicalPlanNode,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlanNode, bool), String> {
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx)?;
    let (plan, child_changed) = rewrite_children(plan, rule, ctx)?;
    Ok((plan, node_changed || child_changed))
}

fn rewrite_bottom_up(
    plan: LogicalPlanNode,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlanNode, bool), String> {
    let (plan, child_changed) = rewrite_children(plan, rule, ctx)?;
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx)?;
    Ok((plan, child_changed || node_changed))
}

fn apply_rule_to_node(
    plan: LogicalPlanNode,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlanNode, bool), String> {
    if !rule.matches(&plan, ctx) {
        return Ok((plan, false));
    }

    let original = plan.clone();
    let phase = rule.phase();
    let rule_name = rule.name();
    ctx.trace_mut().rule_matched(phase, rule_name);

    let start = Instant::now();
    match rule.apply(plan, ctx) {
        Ok(RewriteResult::Unchanged) => Ok((original, false)),
        Ok(RewriteResult::Changed(next)) => {
            ctx.trace_mut()
                .rule_changed(phase, rule_name, start.elapsed().as_micros());
            Ok((next, true))
        }
        Ok(RewriteResult::Rejected(diagnostic)) => {
            let message = diagnostic.message;
            ctx.trace_mut()
                .rule_rejected(phase, rule_name, message.clone());
            match ctx.policy().failure_policy {
                RewriteFailurePolicy::CollectDiagnostics => Ok((original, false)),
                RewriteFailurePolicy::FailFast => Err(message),
            }
        }
        Err(message) => {
            ctx.trace_mut()
                .rule_failed(phase, rule_name, message.clone());
            Err(message)
        }
    }
}

fn rewrite_children(
    mut plan: LogicalPlanNode,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlanNode, bool), String> {
    let (children, changed) = rewrite_plan_list(std::mem::take(&mut plan.children), rule, ctx)?;
    plan.children = children;
    Ok((plan, changed))
}

fn rewrite_plan_list(
    inputs: Vec<LogicalPlanNode>,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(Vec<LogicalPlanNode>, bool), String> {
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (input, input_changed) = rewrite_with_rule(input, rule, ctx)?;
        changed |= input_changed;
        rewritten.push(input);
    }
    Ok((rewritten, changed))
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    use super::rewrite_with_rule;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
    use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;
    use crate::sql::planner::plan::{
        LogicalPlanNode, LogicalPlanNodeKind, LogicalProjectNode, LogicalScanNode,
    };

    struct RenameScanRule;

    impl LogicalRewriteRule for RenameScanRule {
        fn name(&self) -> &'static str {
            "RenameScanRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
            matches!(&plan.kind, LogicalPlanNodeKind::Scan(node) if node.table.name == "before")
        }

        fn apply(
            &self,
            plan: LogicalPlanNode,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            let LogicalPlanNode {
                kind,
                required_output_columns,
                ..
            } = plan;
            let LogicalPlanNodeKind::Scan(mut node) = kind else {
                return Ok(RewriteResult::Unchanged);
            };
            node.table.name = "after".to_string();
            Ok(RewriteResult::Changed(LogicalPlanNode::new(
                LogicalPlanNodeKind::Scan(node),
                vec![],
                required_output_columns,
            )))
        }
    }

    struct RejectProjectRule;

    impl LogicalRewriteRule for RejectProjectRule {
        fn name(&self) -> &'static str {
            "RejectProjectRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn traversal(&self) -> RewriteTraversal {
            RewriteTraversal::TopDown
        }

        fn matches(&self, plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
            matches!(&plan.kind, LogicalPlanNodeKind::Project(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlanNode,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                self.name(),
                "project rejected",
            )))
        }
    }

    #[test]
    fn bottom_up_rewrite_rebuilds_project_child() {
        let plan = project_over_scan("before");
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed);
        let LogicalPlanNodeKind::Project(_) = &rewritten.kind else {
            panic!("expected project root");
        };
        let LogicalPlanNodeKind::Scan(scan) = &rewritten.unary_input().kind else {
            panic!("expected rewritten scan child");
        };
        assert_eq!(scan.table.name, "after");
    }

    #[test]
    fn rejected_rule_collects_diagnostic_without_changing_plan() {
        let plan = project_over_scan("before");
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let (rewritten, changed) = rewrite_with_rule(plan, &RejectProjectRule, &mut ctx).unwrap();

        assert!(!changed);
        assert_eq!(format!("{rewritten:?}"), before);
        assert!(ctx.trace().events().iter().any(|event| {
            matches!(
                event,
                RewriteTraceEvent::RuleRejected {
                    phase: RewritePhase::StructuralRewrite,
                    rule: "RejectProjectRule",
                    message
                } if message == "project rejected"
            )
        }));
    }

    fn project_over_scan(table_name: &str) -> LogicalPlanNode {
        let output = output_column("c1");
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref(output.column_id, "c1"),
                    output_name: "c1".to_string(),
                    output_column_id: output.column_id,
                }],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanNodeKind::Scan(LogicalScanNode {
                    database: "db".to_string(),
                    table: table_def(table_name),
                    alias: None,
                    columns: vec![output.clone()],
                    predicates: vec![],
                    required_columns: None,
                    dict_columns: vec![],
                    variant_columns: vec![],
                }),
                vec![],
                None,
            )],
            None,
        )
    }

    fn table_def(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "c1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(1),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref(column_id: ColumnId, column: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: column.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    #[test]
    fn rewrite_traverses_into_imv_delta_child() {
        use crate::sql::planner::plan::{
            LogicalImvDeltaNode, LogicalPlanNode, LogicalPlanNodeKind, LogicalScanNode,
        };

        let inner = LogicalPlanNode::new(
            LogicalPlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: table_def("before"),
                alias: None,
                columns: vec![output_column("c1")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
            }),
            vec![],
            None,
        );

        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: None,
            }),
            vec![inner],
            None,
        );

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed, "RenameScanRule should rewrite the wrapped Scan");
        let LogicalPlanNodeKind::ImvDelta(delta) = &rewritten.kind else {
            panic!("expected ImvDelta to remain at root after child rewrite");
        };
        let LogicalPlanNodeKind::Scan(scan) = &rewritten.children[0].kind else {
            panic!("expected Scan inside ImvDelta");
        };
        assert_eq!(scan.table.name, "after");
    }

    #[test]
    fn rewrite_visits_all_logical_plan_variants() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::result::RewriteResult;
        use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
        use crate::sql::planner::plan::*;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountVisitsRule {
            count: Arc<AtomicUsize>,
        }

        impl LogicalRewriteRule for CountVisitsRule {
            fn name(&self) -> &'static str {
                "CountVisitsRule"
            }
            fn phase(&self) -> RewritePhase {
                RewritePhase::LogicalNormalize
            }
            fn traversal(&self) -> RewriteTraversal {
                RewriteTraversal::TopDown
            }
            fn matches(&self, _plan: &LogicalPlanNode, _ctx: &RewriteContext) -> bool {
                self.count.fetch_add(1, Ordering::SeqCst);
                false
            }
            fn apply(
                &self,
                _plan: LogicalPlanNode,
                _ctx: &mut RewriteContext,
            ) -> Result<RewriteResult, String> {
                Ok(RewriteResult::Unchanged)
            }
        }

        let leaf = LogicalPlanNode::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        );

        // Exhaustive match on &LogicalPlanNode. This is the intentional trip-wire:
        // if a new variant lands in LogicalPlanNode, this test fails to compile.
        fn assert_variant_handled(variant: &LogicalPlanNode) {
            match &variant.kind {
                LogicalPlanNodeKind::Scan(_)
                | LogicalPlanNodeKind::Filter(_)
                | LogicalPlanNodeKind::Project(_)
                | LogicalPlanNodeKind::Aggregate(_)
                | LogicalPlanNodeKind::Join(_)
                | LogicalPlanNodeKind::Sort(_)
                | LogicalPlanNodeKind::Limit(_)
                | LogicalPlanNodeKind::Union(_)
                | LogicalPlanNodeKind::Intersect(_)
                | LogicalPlanNodeKind::Except(_)
                | LogicalPlanNodeKind::Values(_)
                | LogicalPlanNodeKind::GenerateSeries(_)
                | LogicalPlanNodeKind::TableFunction(_)
                | LogicalPlanNodeKind::Window(_)
                | LogicalPlanNodeKind::Repeat(_)
                | LogicalPlanNodeKind::CTEAnchor(_)
                | LogicalPlanNodeKind::CTEProduce(_)
                | LogicalPlanNodeKind::CTEConsume(_)
                | LogicalPlanNodeKind::Decode(_)
                | LogicalPlanNodeKind::AggregateStateMerge(_)
                | LogicalPlanNodeKind::Apply(_)
                | LogicalPlanNodeKind::AssertOneRow(_)
                | LogicalPlanNodeKind::ImvDelta(_)
                | LogicalPlanNodeKind::ImvVersion(_) => {}
            }
        }
        assert_variant_handled(&leaf);

        let count = Arc::new(AtomicUsize::new(0));
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let (_, _) = super::rewrite_with_rule(
            leaf,
            &CountVisitsRule {
                count: Arc::clone(&count),
            },
            &mut ctx,
        )
        .unwrap();

        assert!(count.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn bottom_up_rewrite_rebuilds_apply_children() {
        use std::collections::HashSet;

        use crate::sql::planner::plan::{ApplyKind, LogicalApplyNode, LogicalPlanNodeKind};

        let outer = project_over_scan("outer");
        let LogicalPlanNodeKind::Project(_) = &outer.kind else {
            panic!("helper returns project");
        };
        let inner = project_over_scan("before");
        let LogicalPlanNodeKind::Project(_) = &inner.kind else {
            panic!("helper returns project");
        };

        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: column_ref(ColumnId(7), "sq"),
                output_column: output_column("sq"),
                inner_output_column_id: ColumnId(7),
                correlation_column_ids: vec![],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![outer.into_single_child(), inner.into_single_child()],
            None,
        );

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed);
        let LogicalPlanNodeKind::Apply(_) = &rewritten.kind else {
            panic!("expected apply root");
        };
        let LogicalPlanNodeKind::Scan(right_scan) = &rewritten.right().kind else {
            panic!("expected scan on apply right side");
        };
        assert_eq!(right_scan.table.name, "after");
    }
}
