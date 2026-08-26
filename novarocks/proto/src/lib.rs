//! NovaRocks-native protobuf wire codecs.
//!
//! Generated DTOs, schema-ledger metadata, and the descriptor set belong to
//! `novarocks-proto-models`. This crate owns the codec and validation layer
//! derived from those artifacts. Transport, role-local state machines, and
//! FE/BE execution conversion remain outside this package.
// Design: ADR-0106 (docs/adr/ADR-0106-native-wire-layering-and-terminal-content-identity.md)
pub mod error;
pub use error::{FieldPath, FieldPathSegment, ProtocolError, ProtocolErrorKind};

/// Canonical descriptor-driven projection and digest utilities.
pub mod canonical;

/// Shared wire codecs for connector execution declarations and binding keys.
pub mod connector;

/// Validated connector execution-binding declaration and result values.
pub mod provider;

/// Validated neutral values used by the native query lifecycle.
pub mod lifecycle;

/// Validated membership and backend process wire values.
pub mod membership;
