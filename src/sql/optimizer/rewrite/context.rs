use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use crate::sql::optimizer::rewrite::trace::RewriteTrace;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteConsumer {
    Query,
    MaterializedViewRefresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteFailurePolicy {
    CollectDiagnostics,
    FailFast,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RewritePolicy {
    pub(crate) failure_policy: RewriteFailurePolicy,
    pub(crate) max_iterations: usize,
}

impl Default for RewritePolicy {
    fn default() -> Self {
        Self {
            failure_policy: RewriteFailurePolicy::CollectDiagnostics,
            max_iterations: 8,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RewriteContext {
    consumer: RewriteConsumer,
    disabled_rules: HashSet<String>,
    policy: RewritePolicy,
    trace: RewriteTrace,
    extension: Option<Arc<dyn Any + Send + Sync>>,
}

impl RewriteContext {
    pub(crate) fn new(consumer: RewriteConsumer) -> Self {
        Self {
            consumer,
            disabled_rules: HashSet::new(),
            policy: RewritePolicy::default(),
            trace: RewriteTrace::default(),
            extension: None,
        }
    }

    pub(crate) fn for_query(disabled_rules: impl IntoIterator<Item = String>) -> Self {
        let mut ctx = Self::new(RewriteConsumer::Query);
        ctx.disabled_rules = disabled_rules.into_iter().collect();
        ctx
    }

    pub(crate) fn for_mv_refresh(disabled_rules: impl IntoIterator<Item = String>) -> Self {
        let mut ctx = Self::new(RewriteConsumer::MaterializedViewRefresh);
        ctx.disabled_rules = disabled_rules.into_iter().collect();
        ctx.policy.failure_policy = RewriteFailurePolicy::FailFast;
        ctx
    }

    pub(crate) fn consumer(&self) -> RewriteConsumer {
        self.consumer
    }

    pub(crate) fn policy(&self) -> &RewritePolicy {
        &self.policy
    }

    pub(crate) fn policy_mut(&mut self) -> &mut RewritePolicy {
        &mut self.policy
    }

    pub(crate) fn is_rule_enabled(&self, rule_name: &str) -> bool {
        !self.disabled_rules.contains(rule_name)
    }

    pub(crate) fn trace(&self) -> &RewriteTrace {
        &self.trace
    }

    pub(crate) fn trace_mut(&mut self) -> &mut RewriteTrace {
        &mut self.trace
    }

    pub(crate) fn set_extension<T>(&mut self, extension: T)
    where
        T: Any + Send + Sync,
    {
        self.extension = Some(Arc::new(extension));
    }

    pub(crate) fn extension<T>(&self) -> Option<&T>
    where
        T: Any + Send + Sync,
    {
        self.extension.as_ref()?.downcast_ref::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;

    #[derive(Debug, PartialEq, Eq)]
    struct TestExtension {
        value: i32,
    }

    #[test]
    fn query_context_uses_disabled_rules() {
        let ctx = RewriteContext::for_query(vec!["RuleA".to_string()]);
        assert_eq!(ctx.consumer(), RewriteConsumer::Query);
        assert_eq!(
            ctx.policy().failure_policy,
            RewriteFailurePolicy::CollectDiagnostics
        );
        assert_eq!(ctx.policy().max_iterations, 8);
        assert!(!ctx.is_rule_enabled("RuleA"));
        assert!(ctx.is_rule_enabled("RuleB"));
    }

    #[test]
    fn context_exposes_mutable_policy_and_trace() {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.policy_mut().max_iterations = 3;
        ctx.trace_mut().phase_started(RewritePhase::Validation);

        assert_eq!(ctx.policy().max_iterations, 3);
        assert_eq!(ctx.trace().events().len(), 1);
    }

    #[test]
    fn mv_context_defaults_to_fail_fast() {
        let ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        assert_eq!(ctx.consumer(), RewriteConsumer::MaterializedViewRefresh);
        assert_eq!(ctx.policy().failure_policy, RewriteFailurePolicy::FailFast);
    }

    #[test]
    fn context_extension_round_trips() {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension(TestExtension { value: 7 });
        assert_eq!(
            ctx.extension::<TestExtension>(),
            Some(&TestExtension { value: 7 })
        );
        assert!(ctx.extension::<String>().is_none());
    }
}
