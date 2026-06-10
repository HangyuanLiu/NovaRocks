//! Automatic maintenance for NovaRocks-owned Iceberg MV storage tables
//! (IV3-11): EXPIRE SNAPSHOTS / OPTIMIZE / DV compaction, driven by a
//! background coordinator. See
//! docs/superpowers/specs/2026-06-10-iceberg-mv-maintenance-scheduler-design.md.

pub(crate) mod policy;
pub(crate) mod stats;
