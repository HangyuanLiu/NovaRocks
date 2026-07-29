use std::fmt::Debug;

use crate::runtime::query_context::QueryId;

/// Protocol-neutral sink for per-query load diagnostics.
///
/// Role applications decide whether and where these logs are retained.
pub trait LoadTrackingLogSink: Send + Sync + Debug {
    fn append(&self, query_id: QueryId, logs: Vec<String>);
}
