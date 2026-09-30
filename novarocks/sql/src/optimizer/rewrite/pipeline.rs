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

use crate::compiler::SqlCompileError;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::optimizer::rewrite::tree::rewrite_with_rule;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};

pub(crate) struct RewriteStage {
    name: &'static str,
    phase: RewritePhase,
    rules: Vec<Box<dyn LogicalRewriteRule>>,
}

impl RewriteStage {
    pub(crate) fn new(
        name: &'static str,
        phase: RewritePhase,
        rules: Vec<Box<dyn LogicalRewriteRule>>,
    ) -> Self {
        Self { name, phase, rules }
    }

    #[cfg(test)]
    pub(crate) fn name(&self) -> &'static str {
        self.name
    }
}

pub(crate) struct RewritePipeline {
    stages: Vec<RewriteStage>,
}

impl RewritePipeline {
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) fn new(phases: Vec<RewritePhase>, rules: Vec<Box<dyn LogicalRewriteRule>>) -> Self {
        let mut stages: Vec<RewriteStage> = phases
            .into_iter()
            .map(|phase| RewriteStage::new(phase.as_str(), phase, Vec::new()))
            .collect();

        for rule in rules {
            let phase = rule.phase();
            if let Some(stage) = stages.iter_mut().find(|stage| stage.phase == phase) {
                stage.rules.push(rule);
            } else {
                stages.push(RewriteStage::new(phase.as_str(), phase, vec![rule]));
            }
        }

        Self { stages }
    }

    pub(crate) fn from_stages(stages: Vec<RewriteStage>) -> Self {
        Self { stages }
    }

    #[cfg(test)]
    pub(crate) fn stage_names(&self) -> Vec<&'static str> {
        self.stages.iter().map(|stage| stage.name()).collect()
    }

    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) fn rule_names(&self) -> Vec<&'static str> {
        self.stages
            .iter()
            .flat_map(|stage| stage.rules.iter().map(|rule| rule.name()))
            .collect()
    }

    pub(crate) fn rewrite(
        &self,
        plan: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<OptExpr, SqlCompileError> {
        let control = ctx.control_view();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)
            .map_err(crate::compiler::SqlCompileError::from)?;
        let mut current = plan;

        for stage in &self.stages {
            work.step()
                .map_err(crate::compiler::SqlCompileError::from)?;
            let phase = stage.phase;
            ctx.check_deadline(stage.name)?;
            ctx.trace_mut().phase_started_with_stage(phase, stage.name);

            for iteration in 1..=ctx.policy().max_iterations {
                work.step()
                    .map_err(crate::compiler::SqlCompileError::from)?;
                ctx.check_deadline(stage.name)?;
                ctx.trace_mut().iteration_started(phase, iteration);
                let mut phase_changed = false;

                for rule in &stage.rules {
                    work.step()
                        .map_err(crate::compiler::SqlCompileError::from)?;
                    ctx.check_deadline(rule.name())?;
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

        work.finish()
            .map_err(crate::compiler::SqlCompileError::from)?;
        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use crate::compiler::SqlCompileError;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{RewritePipeline, RewriteStage};
    use crate::optimizer::operator::{GenerateSeriesOp, Operator, ValuesOp};
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::optimizer::rewrite::phase::RewritePhase;
    use crate::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
    use crate::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::optimizer::rewrite::trace::RewriteTraceEvent;

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

        fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
            self.matches_called.fetch_add(1, Ordering::SeqCst);
            true
        }

        fn apply(
            &self,
            _expr: OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, SqlCompileError> {
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

        fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
            matches!(&expr.op, Operator::LogicalValues(_))
        }

        fn apply(
            &self,
            _expr: OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, SqlCompileError> {
            Err(SqlCompileError::Compilation("boom".to_string()))
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

        fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
            matches!(&expr.op, Operator::LogicalValues(_))
        }

        fn apply(
            &self,
            _expr: OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, SqlCompileError> {
            Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                self.name(),
                "not supported",
            )))
        }
    }

    struct ValuesToGenerateSeriesRule;

    impl LogicalRewriteRule for ValuesToGenerateSeriesRule {
        fn name(&self) -> &'static str {
            "ValuesToGenerateSeriesRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
            matches!(&expr.op, Operator::LogicalValues(_))
        }

        fn apply(
            &self,
            _expr: OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, SqlCompileError> {
            Ok(RewriteResult::Changed(OptExpr::new(
                Operator::LogicalGenerateSeries(GenerateSeriesOp {
                    start: 1,
                    end: 1,
                    step: 1,
                    column_name: "stage1".to_string(),
                    alias: None,
                    output_column_id: crate::column_id::ColumnId::UNSET,
                }),
                vec![],
            )))
        }
    }

    struct GenerateSeriesToValuesRule;

    impl LogicalRewriteRule for GenerateSeriesToValuesRule {
        fn name(&self) -> &'static str {
            "GenerateSeriesToValuesRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
            matches!(&expr.op, Operator::LogicalGenerateSeries(_))
        }

        fn apply(
            &self,
            _expr: OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, SqlCompileError> {
            Ok(RewriteResult::Changed(empty_values_plan()))
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
                    stage: "LogicalNormalize",
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
                    stage: "Validation",
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
                    stage: "LogicalNormalize",
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

        assert_eq!(
            result.unwrap_err(),
            crate::compiler::SqlCompileError::Compilation("boom".to_string())
        );
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

        assert_eq!(
            result.unwrap_err(),
            crate::compiler::SqlCompileError::Compilation("not supported".to_string())
        );
        assert_eq!(count_rejected_events(&ctx, "RejectingRule"), 1);
        assert_eq!(count_failed_events(&ctx, "RejectingRule"), 0);
    }

    #[test]
    fn duplicate_phase_stages_run_in_declared_order() {
        let pipeline = RewritePipeline::from_stages(vec![
            RewriteStage::new(
                "first-structural-stage",
                RewritePhase::StructuralRewrite,
                vec![Box::new(ValuesToGenerateSeriesRule)],
            ),
            RewriteStage::new(
                "second-structural-stage",
                RewritePhase::StructuralRewrite,
                vec![Box::new(GenerateSeriesToValuesRule)],
            ),
        ]);
        let stage_names: Vec<&'static str> =
            pipeline.stages.iter().map(|stage| stage.name()).collect();
        assert_eq!(
            stage_names,
            vec!["first-structural-stage", "second-structural-stage"]
        );

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let rewritten = pipeline.rewrite(empty_values_plan(), &mut ctx).unwrap();

        assert!(matches!(&rewritten.op, Operator::LogicalValues(_)));
        let changed_rules: Vec<&'static str> = ctx
            .trace()
            .events()
            .iter()
            .filter_map(|event| {
                if let RewriteTraceEvent::RuleChanged { rule, .. } = event {
                    Some(*rule)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            changed_rules,
            vec!["ValuesToGenerateSeriesRule", "GenerateSeriesToValuesRule"]
        );
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

    fn empty_values_plan() -> OptExpr {
        OptExpr::new(
            Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
        )
    }

    #[test]
    fn empty_pipeline_checks_entry_and_finish_without_rules() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Stop {
            calls: AtomicUsize,
            stop_at: usize,
            error: CompileControlError,
        }
        impl PureCompileControl for Stop {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                assert_eq!(units, 0);
                if self.calls.fetch_add(1, Ordering::SeqCst) == self.stop_at {
                    Err(self.error)
                } else {
                    Ok(())
                }
            }
        }
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop_at in [0, 1] {
                let control = Stop {
                    calls: AtomicUsize::new(0),
                    stop_at,
                    error,
                };
                let mut ctx = RewriteContext::for_query_with_settings(Default::default(), &control);
                let pipeline = RewritePipeline::from_stages(vec![]);
                assert_eq!(
                    pipeline.rewrite(empty_values_plan(), &mut ctx).unwrap_err(),
                    crate::compiler::SqlCompileError::from(error)
                );
                assert_eq!(control.calls.load(Ordering::SeqCst), stop_at + 1);
                assert!(ctx.trace().events().is_empty());
            }
        }
    }

    #[test]
    fn control_after_apply_never_enters_collect_diagnostics() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Owner {
            stopped: std::sync::atomic::AtomicBool,
            error: CompileControlError,
        }
        impl PureCompileControl for Owner {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                if self.stopped.load(Ordering::SeqCst) {
                    Err(self.error)
                } else {
                    Ok(())
                }
            }
        }
        struct StopAndReject<'a>(&'a Owner);
        impl LogicalRewriteRule for StopAndReject<'_> {
            fn name(&self) -> &'static str {
                "StopAndReject"
            }
            fn phase(&self) -> RewritePhase {
                RewritePhase::LogicalNormalize
            }
            fn matches(&self, _: &OptExpr, _: &RewriteContext) -> bool {
                true
            }
            fn apply(
                &self,
                _: OptExpr,
                _: &mut RewriteContext,
            ) -> Result<RewriteResult, SqlCompileError> {
                self.0.stopped.store(true, Ordering::SeqCst);
                Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                    self.name(),
                    "ordinary candidate rejection",
                )))
            }
        }
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let owner = Owner {
                stopped: Default::default(),
                error,
            };
            let mut ctx = RewriteContext::for_query_with_settings(Default::default(), &owner);
            // Direct invocation borrows the real rule owner; pipeline boxes are
            // static by contract and need no capability-bearing test shim.
            let failure = crate::optimizer::rewrite::tree::rewrite_with_rule(
                empty_values_plan(),
                &StopAndReject(&owner),
                &mut ctx,
            )
            .unwrap_err();
            assert_eq!(failure, crate::compiler::SqlCompileError::from(error));
            assert_eq!(count_rejected_events(&ctx, "StopAndReject"), 0);
            assert_eq!(count_failed_events(&ctx, "StopAndReject"), 0);
        }
    }
}
