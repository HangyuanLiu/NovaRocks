//! NovaRocks-native protobuf generated schema artifacts.
//!
//! This crate is the sole owner of repository-level generated DTOs, the
//! descriptor set, and the schema ledger. Wire codecs, transport, role-local
//! state, and FE/BE execution conversion are intentionally outside this crate.

pub const SCHEMA_LEDGER_VERSION: u32 = 1;

/// File descriptor set generated from the canonical repository-level IDL.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/novarocks_descriptor.bin"));

#[allow(clippy::len_without_is_empty)]
pub mod common {
    include!(concat!(env!("OUT_DIR"), "/novarocks.common.rs"));
}

#[allow(clippy::module_inception)]
pub mod expr {
    include!(concat!(env!("OUT_DIR"), "/novarocks.expr.rs"));
}

pub mod filter {
    include!(concat!(env!("OUT_DIR"), "/novarocks.filter.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod plan {
    include!(concat!(env!("OUT_DIR"), "/novarocks.plan.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod novarocks {
    use super::{common, filter, plan};

    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}
