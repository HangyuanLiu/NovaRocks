//! Observability trace records for MV rewrite attempts.

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum MvRewriteOutcome {
    Accepted { mv_name: String, fresh: usize, stale: usize },
    Rejected { mv_name: String, reason: String },
    Skipped { mv_name: String, reason: String },
}
