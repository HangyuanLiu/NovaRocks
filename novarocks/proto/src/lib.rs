//! NovaRocks-native protobuf wire codecs.
//!
//! Generated DTOs, schema-ledger metadata, and the descriptor set belong to
//! `novarocks-proto-models`. This crate owns the codec and validation layer
//! derived from those artifacts. Transport, role-local state machines, and
//! FE/BE execution conversion remain outside this package.
// Design: ADR-0105 (docs/adr/ADR-0105-wire-authority-and-domain-carrier-separation.md)

// Design: ADR-0098 (docs/adr/ADR-0098-native-protocol-error-contract.md)
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
