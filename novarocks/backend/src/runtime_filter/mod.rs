//! Backend-owned runtime-filter participant state.

pub(crate) mod ingress;
pub(crate) mod participant;
#[cfg(test)]
pub(crate) mod test_support;
// The typed scan cannot bind this filter yet: `RuntimeFilterSession::subscribe`
// needs the fragment's decoded consumer contract, which reaches the scan node
// only after `lower_typed_connector_scan` has already frozen its source. The
// filter itself is complete and covered by tests; only that seam is missing.
#[allow(
    dead_code,
    reason = "Awaiting the fragment seam that carries decoded consumer contracts into typed scan lowering."
)]
pub(crate) mod typed_dynamic_filter;
