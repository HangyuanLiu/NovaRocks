use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::pipeline::RewritePipeline;

pub(crate) fn default_rewrite_phases() -> Vec<RewritePhase> {
    vec![
        RewritePhase::LogicalNormalize,
        RewritePhase::StructuralRewrite,
        RewritePhase::SemanticRewrite,
        RewritePhase::Validation,
    ]
}

pub(crate) fn query_rewrite_pipeline() -> RewritePipeline {
    RewritePipeline::new(default_rewrite_phases(), Vec::new())
}

pub(crate) fn mv_rewrite_pipeline() -> RewritePipeline {
    RewritePipeline::new(default_rewrite_phases(), Vec::new())
}

pub(crate) fn is_known_rewrite_rule_name(name: &str) -> bool {
    let query_pipeline = query_rewrite_pipeline();
    let mv_pipeline = mv_rewrite_pipeline();

    query_pipeline
        .rule_names()
        .into_iter()
        .chain(mv_pipeline.rule_names())
        .any(|rule_name| rule_name == name)
}

#[cfg(test)]
mod tests {
    use super::{
        default_rewrite_phases, is_known_rewrite_rule_name, mv_rewrite_pipeline,
        query_rewrite_pipeline,
    };
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    #[derive(Debug, PartialEq, Eq)]
    struct TestMvExtension {
        marker: String,
    }

    #[test]
    fn query_pipeline_is_empty_and_noop_in_phase_one() {
        let pipeline = query_rewrite_pipeline();
        assert!(pipeline.rule_names().is_empty());

        let plan = empty_values_plan();
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let rewritten = pipeline.rewrite(plan, &mut ctx).unwrap();

        assert_eq!(format!("{rewritten:?}"), before);
        assert_default_phase_trace(&ctx);
    }

    #[test]
    fn mv_pipeline_is_empty_and_noop_in_phase_one() {
        let pipeline = mv_rewrite_pipeline();
        assert!(pipeline.rule_names().is_empty());

        let plan = empty_values_plan();
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension(TestMvExtension {
            marker: "mv-refresh".to_string(),
        });

        let rewritten = pipeline.rewrite(plan, &mut ctx).unwrap();

        assert_eq!(format!("{rewritten:?}"), before);
        assert_eq!(
            ctx.extension::<TestMvExtension>(),
            Some(&TestMvExtension {
                marker: "mv-refresh".to_string(),
            })
        );
        assert_default_phase_trace(&ctx);
    }

    #[test]
    fn rewrite_registry_has_no_rule_names_before_rules_are_migrated() {
        assert!(!is_known_rewrite_rule_name(""));
        assert!(!is_known_rewrite_rule_name("AggregatePushdown"));
        assert!(!is_known_rewrite_rule_name("PushFilterThroughProject"));
    }

    fn assert_default_phase_trace(ctx: &RewriteContext) {
        assert_eq!(
            default_rewrite_phases(),
            vec![
                RewritePhase::LogicalNormalize,
                RewritePhase::StructuralRewrite,
                RewritePhase::SemanticRewrite,
                RewritePhase::Validation,
            ]
        );
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
                    phase: RewritePhase::StructuralRewrite,
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::StructuralRewrite,
                    iteration: 1,
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::StructuralRewrite,
                },
                RewriteTraceEvent::PhaseStarted {
                    phase: RewritePhase::SemanticRewrite,
                },
                RewriteTraceEvent::IterationStarted {
                    phase: RewritePhase::SemanticRewrite,
                    iteration: 1,
                },
                RewriteTraceEvent::PhaseEnded {
                    phase: RewritePhase::SemanticRewrite,
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

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }
}
