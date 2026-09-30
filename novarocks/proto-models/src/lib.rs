//! NovaRocks-native protobuf generated schema artifacts.
//!
//! This crate is the sole owner of repository-level generated DTOs, the
//! descriptor set, and the schema ledger. Wire codecs, transport, role-local
//! state, and FE/BE execution conversion are intentionally outside this crate.

pub const SCHEMA_LEDGER_VERSION: u32 = 1;

pub mod resource_layout;

include!(concat!(env!("OUT_DIR"), "/resource_layout_registry.rs"));

/// File descriptor set generated from the canonical repository-level IDL.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/novarocks_descriptor.bin"));

pub mod catalog {
    include!(concat!(env!("OUT_DIR"), "/novarocks.catalog.rs"));
}

#[allow(clippy::len_without_is_empty)]
pub mod common {
    include!(concat!(env!("OUT_DIR"), "/novarocks.common.rs"));
}

pub mod connector_read {
    include!(concat!(env!("OUT_DIR"), "/novarocks.connector_read.rs"));
}

pub mod connector_common {
    include!(concat!(env!("OUT_DIR"), "/novarocks.connector_common.rs"));
}

pub mod connector_write {
    include!(concat!(env!("OUT_DIR"), "/novarocks.connector_write.rs"));
}

#[allow(clippy::module_inception)]
pub mod expr {
    include!(concat!(env!("OUT_DIR"), "/novarocks.expr.rs"));
}

pub mod filter {
    include!(concat!(env!("OUT_DIR"), "/novarocks.filter.rs"));
}

/// Flat type-table component of the v2 physical package vocabulary.
pub mod physical_type_v2 {
    include!(concat!(env!("OUT_DIR"), "/novarocks.physical_type_v2.rs"));
}

/// Flat invocation/control component of the v2 physical package vocabulary.
pub mod physical_control_v2 {
    include!(concat!(
        env!("OUT_DIR"),
        "/novarocks.physical_control_v2.rs"
    ));
}

/// Complete frozen call-effects and parameter component of the v2 vocabulary.
pub mod physical_semantics_v2 {
    include!(concat!(
        env!("OUT_DIR"),
        "/novarocks.physical_semantics_v2.rs"
    ));
}

#[allow(clippy::large_enum_variant)]
pub mod plan {
    include!(concat!(env!("OUT_DIR"), "/novarocks.plan.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod novarocks {
    pub use super::{
        catalog, common, connector_common, connector_read, connector_write, filter, plan,
    };

    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}
