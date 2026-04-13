//! Per-optimize-call configuration shared by the RBO and CBO drivers.

use std::collections::HashSet;
use std::time::Duration;

/// Controls which rules fire and bounds resource use.
///
/// Constructed once per `optimize()` call. Held by both the RBO driver
/// (`rbo::driver::rewrite_to_fixed_point`) and the CBO search loop. Rule
/// names live in a single namespace shared across `RewriteRule` (RBO) and
/// `Rule` (CBO); names must be unique across both trait families.
pub(crate) struct OptimizerOptions {
    disabled_rules: HashSet<String>,
    /// Hard cap on the RBO driver's tree-level fixed-point loop.
    pub rbo_max_iterations: usize,
    /// Hard cap on the CBO Memo group count (existing constant; documented here).
    pub cbo_max_groups: usize,
    /// Wall-clock budget for the entire `optimize()` call (existing constant; documented here).
    pub optimize_timeout: Duration,
}

impl OptimizerOptions {
    pub(crate) fn default_settings() -> Self {
        Self {
            disabled_rules: HashSet::new(),
            rbo_max_iterations: 32,
            cbo_max_groups: 5000,
            optimize_timeout: Duration::from_secs(10),
        }
    }

    pub(crate) fn is_enabled(&self, rule_name: &str) -> bool {
        !self.disabled_rules.contains(rule_name)
    }

    pub(crate) fn disable(&mut self, rule_name: &str) {
        self.disabled_rules.insert(rule_name.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_enables_all_rules() {
        let opts = OptimizerOptions::default_settings();
        assert!(opts.is_enabled("AnyRuleName"));
        assert!(opts.is_enabled("PushDownPredicateScan"));
    }

    #[test]
    fn disable_blocks_named_rule_only() {
        let mut opts = OptimizerOptions::default_settings();
        opts.disable("PushDownPredicateScan");
        assert!(!opts.is_enabled("PushDownPredicateScan"));
        assert!(opts.is_enabled("PushDownPredicateProject"));
    }

    #[test]
    fn defaults_match_existing_optimizer_constants() {
        let opts = OptimizerOptions::default_settings();
        assert_eq!(opts.rbo_max_iterations, 32);
        assert_eq!(opts.cbo_max_groups, 5000);
        assert_eq!(opts.optimize_timeout, Duration::from_secs(10));
    }
}
