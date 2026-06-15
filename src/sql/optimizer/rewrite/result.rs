use crate::sql::planner::plan::LogicalPlanNode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RewriteDiagnosticKind {
    Rejected,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RewriteDiagnostic {
    pub(crate) rule: &'static str,
    pub(crate) message: String,
    pub(crate) kind: RewriteDiagnosticKind,
}

impl RewriteDiagnostic {
    pub(crate) fn rejected(rule: &'static str, message: impl Into<String>) -> Self {
        Self {
            rule,
            message: message.into(),
            kind: RewriteDiagnosticKind::Rejected,
        }
    }

    pub(crate) fn error(rule: &'static str, message: impl Into<String>) -> Self {
        Self {
            rule,
            message: message.into(),
            kind: RewriteDiagnosticKind::Error,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RewriteResult {
    Unchanged,
    Changed(LogicalPlanNode),
    Rejected(RewriteDiagnostic),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{LogicalPlanNode, LogicalPlanNodeKind, LogicalValuesNode};

    #[test]
    fn rejected_diagnostic_preserves_rule_and_message() {
        let diagnostic = RewriteDiagnostic::rejected("RuleA", "unsupported join shape");
        assert_eq!(diagnostic.rule, "RuleA");
        assert_eq!(diagnostic.message, "unsupported join shape");
        assert_eq!(diagnostic.kind, RewriteDiagnosticKind::Rejected);
    }

    #[test]
    fn error_diagnostic_preserves_rule_and_message() {
        let diagnostic = RewriteDiagnostic::error("RuleB", "rewrite failed");
        assert_eq!(diagnostic.rule, "RuleB");
        assert_eq!(diagnostic.message, "rewrite failed");
        assert_eq!(diagnostic.kind, RewriteDiagnosticKind::Error);
    }

    #[test]
    fn rewrite_result_variants_hold_payloads() {
        assert!(matches!(RewriteResult::Unchanged, RewriteResult::Unchanged));

        let changed = RewriteResult::Changed(LogicalPlanNode::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        ));
        assert!(matches!(
            changed,
            RewriteResult::Changed(LogicalPlanNode {
                kind: LogicalPlanNodeKind::Values(_),
                ..
            })
        ));

        let rejected =
            RewriteResult::Rejected(RewriteDiagnostic::rejected("RuleC", "not applicable"));
        let RewriteResult::Rejected(diagnostic) = rejected else {
            panic!("expected rejected result");
        };
        assert_eq!(diagnostic.rule, "RuleC");
        assert_eq!(diagnostic.message, "not applicable");
    }
}
