use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::tree::rewrite_with_rule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct RewritePipeline {
    phases: Vec<RewritePhase>,
    rules: Vec<Box<dyn LogicalRewriteRule>>,
}

impl RewritePipeline {
    pub(crate) fn new(phases: Vec<RewritePhase>, rules: Vec<Box<dyn LogicalRewriteRule>>) -> Self {
        Self { phases, rules }
    }

    pub(crate) fn rule_names(&self) -> Vec<&'static str> {
        self.rules.iter().map(|rule| rule.name()).collect()
    }

    pub(crate) fn rewrite(
        &self,
        plan: LogicalPlan,
        ctx: &mut RewriteContext,
    ) -> Result<LogicalPlan, String> {
        let mut current = plan;

        for &phase in &self.phases {
            ctx.trace_mut().phase_started(phase);

            for iteration in 1..=ctx.policy().max_iterations {
                ctx.trace_mut().iteration_started(phase, iteration);
                let mut phase_changed = false;

                for rule in &self.rules {
                    if rule.phase() != phase {
                        continue;
                    }

                    let rule_name = rule.name();
                    if !ctx.is_rule_enabled(rule_name) {
                        ctx.trace_mut().rule_skipped(phase, rule_name, "disabled");
                        continue;
                    }

                    match rewrite_with_rule(current, rule.as_ref(), ctx) {
                        Ok((rewritten, changed)) => {
                            current = rewritten;
                            phase_changed |= changed;
                        }
                        Err(message) => {
                            ctx.trace_mut().phase_ended(phase);
                            return Err(message);
                        }
                    }
                }

                if !phase_changed {
                    break;
                }
            }

            ctx.trace_mut().phase_ended(phase);
        }

        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::RewritePipeline;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    struct DisabledRule {
        matches_called: Arc<AtomicUsize>,
    }

    impl LogicalRewriteRule for DisabledRule {
        fn name(&self) -> &'static str {
            "DisabledRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            self.matches_called.fetch_add(1, Ordering::SeqCst);
            true
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    struct FailingRule;

    impl LogicalRewriteRule for FailingRule {
        fn name(&self) -> &'static str {
            "FailingRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Values(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Err("boom".to_string())
        }
    }

    struct RejectingRule;

    impl LogicalRewriteRule for RejectingRule {
        fn name(&self) -> &'static str {
            "RejectingRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Values(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                self.name(),
                "not supported",
            )))
        }
    }

    #[test]
    fn empty_pipeline_preserves_plan_and_records_phases() {
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize, RewritePhase::Validation],
            vec![],
        );
        let plan = empty_values_plan();
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let rewritten = pipeline.rewrite(plan, &mut ctx).unwrap();

        assert_eq!(format!("{rewritten:?}"), before);
        assert_eq!(
            ctx.trace().events(),
            &[
                RewriteTraceEvent::PhaseStarted {
                    phase: RewritePhase::LogicalNormalize,
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::LogicalNormalize,
                    iteration: 1,
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::LogicalNormalize,
                },
                RewriteTraceEvent::PhaseStarted {
                    phase: RewritePhase::Validation,
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::Validation,
                    iteration: 1,
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::Validation,
                },
            ]
        );
    }

    #[test]
    fn disabled_rule_is_skipped_before_match() {
        let matches_called = Arc::new(AtomicUsize::new(0));
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize],
            vec![Box::new(DisabledRule {
                matches_called: Arc::clone(&matches_called),
            })],
        );
        let plan = empty_values_plan();
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_query(vec!["DisabledRule".to_string()]);

        let rewritten = pipeline.rewrite(plan, &mut ctx).unwrap();

        assert_eq!(format!("{rewritten:?}"), before);
        assert_eq!(matches_called.load(Ordering::SeqCst), 0);
        assert_eq!(
            ctx.trace().events(),
            &[
                RewriteTraceEvent::PhaseStarted {
                    phase: RewritePhase::LogicalNormalize,
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::LogicalNormalize,
                    iteration: 1,
                },
                RewriteTraceEvent::RuleSkipped {
                    phase: RewritePhase::LogicalNormalize,
                    rule: "DisabledRule",
                    reason: "disabled".to_string(),
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::LogicalNormalize,
                },
            ]
        );
    }

    #[test]
    fn failed_rule_records_one_failed_event() {
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize],
            vec![Box::new(FailingRule)],
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let result = pipeline.rewrite(empty_values_plan(), &mut ctx);

        assert_eq!(result.unwrap_err(), "boom");
        assert_eq!(count_failed_events(&ctx, "FailingRule"), 1);
    }

    #[test]
    fn fail_fast_rejection_records_rejected_without_failed_event() {
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize],
            vec![Box::new(RejectingRule)],
        );
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());

        let result = pipeline.rewrite(empty_values_plan(), &mut ctx);

        assert_eq!(result.unwrap_err(), "not supported");
        assert_eq!(count_rejected_events(&ctx, "RejectingRule"), 1);
        assert_eq!(count_failed_events(&ctx, "RejectingRule"), 0);
    }

    fn count_failed_events(ctx: &RewriteContext, rule_name: &'static str) -> usize {
        ctx.trace()
            .events()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    RewriteTraceEvent::RuleFailed { rule, .. } if *rule == rule_name
                )
            })
            .count()
    }

    fn count_rejected_events(ctx: &RewriteContext, rule_name: &'static str) -> usize {
        ctx.trace()
            .events()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    RewriteTraceEvent::RuleRejected { rule, .. } if *rule == rule_name
                )
            })
            .count()
    }

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }
}
