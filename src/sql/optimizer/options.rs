//! Per-optimize-call configuration shared by the RBO and CBO drivers.

use std::cell::RefCell;
use std::collections::HashSet;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SessionOptimizerSettings {
    pub enable_ukfk_opt: bool,
    pub enable_rbo_table_prune: bool,
    pub enable_cbo_table_prune: bool,
    pub enable_table_prune_on_update: bool,
    pub enable_eliminate_agg: bool,
    pub disabled_rules: Vec<String>,
    /// Master kill switch for MV query rewrite. Default true.
    pub enable_mv_rewrite: bool,
    /// When false, only fully-fresh MVs are eligible; partial-freshness
    /// rewrites (UNION ALL with stale-partition base scan) are disabled.
    /// Default true.
    pub enable_mv_union_rewrite: bool,
    /// Skip MV rewrite when fresh partitions cover less than this fraction.
    /// Default 0.2.
    pub mv_rewrite_min_fresh_ratio: f64,
    /// Hard cap on number of MV alternatives inserted per memo group.
    /// Default 3.
    pub mv_rewrite_max_candidates_per_group: usize,
}

impl Default for SessionOptimizerSettings {
    fn default() -> Self {
        Self {
            enable_ukfk_opt: false,
            enable_rbo_table_prune: false,
            enable_cbo_table_prune: false,
            enable_table_prune_on_update: false,
            enable_eliminate_agg: false,
            disabled_rules: Vec::new(),
            enable_mv_rewrite: true,
            enable_mv_union_rewrite: true,
            mv_rewrite_min_fresh_ratio: 0.2,
            mv_rewrite_max_candidates_per_group: 3,
        }
    }
}

thread_local! {
    static SESSION_OPTIMIZER_SETTINGS: RefCell<SessionOptimizerSettings> =
        RefCell::new(SessionOptimizerSettings::default());
}

pub(crate) fn with_session_optimizer_settings<T>(
    settings: SessionOptimizerSettings,
    f: impl FnOnce() -> T,
) -> T {
    SESSION_OPTIMIZER_SETTINGS.with(|cell| {
        let previous = cell.replace(settings);
        let result = f();
        cell.replace(previous);
        result
    })
}

pub(crate) fn current_session_optimizer_settings() -> SessionOptimizerSettings {
    SESSION_OPTIMIZER_SETTINGS.with(|cell| cell.borrow().clone())
}

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
    #[allow(dead_code)]
    pub cbo_max_groups: usize,
    /// Wall-clock budget for the entire `optimize()` call (existing constant; documented here).
    pub optimize_timeout: Duration,
    /// Master kill switch for MV query rewrite.
    pub enable_mv_rewrite: bool,
    /// Enables partial-freshness UNION ALL compensation rewrites.
    pub enable_mv_union_rewrite: bool,
    /// Skip MV rewrite when fresh partitions cover less than this fraction.
    pub mv_rewrite_min_fresh_ratio: f64,
    /// Hard cap on number of MV alternatives inserted per memo group.
    pub mv_rewrite_max_candidates_per_group: usize,
}

impl OptimizerOptions {
    pub(crate) fn default_settings() -> Self {
        Self {
            disabled_rules: HashSet::new(),
            rbo_max_iterations: 32,
            cbo_max_groups: 5000,
            optimize_timeout: Duration::from_secs(10),
            enable_mv_rewrite: true,
            enable_mv_union_rewrite: true,
            mv_rewrite_min_fresh_ratio: 0.2,
            mv_rewrite_max_candidates_per_group: 3,
        }
    }

    pub(crate) fn is_enabled(&self, rule_name: &str) -> bool {
        !self.disabled_rules.contains(rule_name)
    }

    pub(crate) fn disable(&mut self, rule_name: &str) {
        self.disabled_rules.insert(rule_name.to_string());
    }

    pub(crate) fn from_session(settings: &SessionOptimizerSettings) -> Self {
        let mut opts = Self::default_settings();
        for rule_name in &settings.disabled_rules {
            opts.disable(rule_name);
        }
        opts.enable_mv_rewrite = settings.enable_mv_rewrite;
        opts.enable_mv_union_rewrite = settings.enable_mv_union_rewrite;
        opts.mv_rewrite_min_fresh_ratio = settings.mv_rewrite_min_fresh_ratio;
        opts.mv_rewrite_max_candidates_per_group = settings.mv_rewrite_max_candidates_per_group;
        opts
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

    #[test]
    fn from_session_copies_disabled_rules() {
        let settings = SessionOptimizerSettings {
            disabled_rules: vec!["JoinCommutativity".to_string(), "FooRule".to_string()],
            ..Default::default()
        };
        let opts = OptimizerOptions::from_session(&settings);
        assert!(!opts.is_enabled("JoinCommutativity"));
        assert!(!opts.is_enabled("FooRule"));
        assert!(opts.is_enabled("UnrelatedRule"));
    }

    #[test]
    fn from_session_empty_disabled_rules_enables_everything() {
        let settings = SessionOptimizerSettings::default();
        let opts = OptimizerOptions::from_session(&settings);
        assert!(opts.is_enabled("JoinCommutativity"));
        assert!(opts.is_enabled("AnyRuleAtAll"));
    }

    #[test]
    fn default_enables_mv_rewrite() {
        let s = SessionOptimizerSettings::default();
        assert!(s.enable_mv_rewrite);
        assert!(s.enable_mv_union_rewrite);
        assert!((s.mv_rewrite_min_fresh_ratio - 0.2).abs() < 1e-9);
        assert_eq!(s.mv_rewrite_max_candidates_per_group, 3);
    }
}
