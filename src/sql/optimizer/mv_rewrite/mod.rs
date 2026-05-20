//! Materialized view query rewrite.
//!
//! See `docs/superpowers/specs/2026-05-21-mv-rewrite-ivm-design.md` for
//! design rationale. Reference: StarRocks `materialization/` rules.

pub(crate) mod column_id;
pub(crate) mod registry;
pub(crate) mod rules;
pub(crate) mod trace;

use std::sync::Arc;

use super::options::OptimizerOptions;

/// Context passed to MV-rewrite rules. Built once per `optimize()` call.
#[derive(Clone)]
pub(crate) struct MvRewriteCtx {
    inner: Arc<MvRewriteCtxInner>,
}

struct MvRewriteCtxInner {
    pub enable_mv_rewrite: bool,
    pub enable_mv_union_rewrite: bool,
    pub mv_rewrite_min_fresh_ratio: f64,
    pub mv_rewrite_max_candidates_per_group: usize,
    pub registry: registry::MvCandidateRegistry,
}

impl MvRewriteCtx {
    pub(crate) fn from_options(opts: &OptimizerOptions) -> Self {
        Self {
            inner: Arc::new(MvRewriteCtxInner {
                enable_mv_rewrite: opts.enable_mv_rewrite,
                enable_mv_union_rewrite: opts.enable_mv_union_rewrite,
                mv_rewrite_min_fresh_ratio: opts.mv_rewrite_min_fresh_ratio,
                mv_rewrite_max_candidates_per_group: opts.mv_rewrite_max_candidates_per_group,
                registry: registry::MvCandidateRegistry::new(),
            }),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.inner.enable_mv_rewrite
    }

    #[allow(dead_code)]
    pub(crate) fn union_enabled(&self) -> bool {
        self.inner.enable_mv_union_rewrite
    }

    #[allow(dead_code)]
    pub(crate) fn min_fresh_ratio(&self) -> f64 {
        self.inner.mv_rewrite_min_fresh_ratio
    }

    #[allow(dead_code)]
    pub(crate) fn max_candidates_per_group(&self) -> usize {
        self.inner.mv_rewrite_max_candidates_per_group
    }

    #[allow(dead_code)]
    pub(crate) fn registry(&self) -> &registry::MvCandidateRegistry {
        &self.inner.registry
    }
}
