//! Shared runtime-filter lifecycle codec seam.
//!
//! Fragment-local native decoding is moving to the backend role. Lifecycle
//! request encoding remains a frontend/core concern during that migration, so
//! this module is the only core-facing entrypoint for the small set of
//! runtime-filter contract enum/value decoders it needs.

pub(in crate::protocol::native) use super::decode::{
    decode_runtime_filter_activation, decode_runtime_filter_capability,
    decode_runtime_filter_completion, decode_runtime_filter_contribution_kind,
    decode_runtime_filter_logical_domain_and_reduction,
};
