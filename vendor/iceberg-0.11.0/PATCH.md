# NovaRocks patches over apache/iceberg-rust 0.11.0-rc.2

Upstream tag: `v0.11.0-rc.2`
Upstream commit: `859048563bc71a3167d0545cb66756d26c64e4ca`
Upstream repository: https://github.com/apache/iceberg-rust
Normalized package: `iceberg-0.11.0.crate`
Package SHA-256: `83bd584247a59b6ce2d2f6ce08188640977568aff9b7240c4a5e45a1c6214142`

Generation command (Rust/Cargo 1.98.1, from the verified upstream checkout):

```sh
cargo +1.98.1 package --no-verify --exclude-lockfile --offline \
  -p iceberg-property-macro -p iceberg -p iceberg-catalog-rest -p iceberg-catalog-hms
```

`--exclude-lockfile` avoids resolving unrelated unpublished/workspace development
dependencies while preserving Cargo's normalized manifest and original published
source files. Each extracted file was compared byte-for-byte with this package.
The initial source commit is unmodified except for this inventory.

## Dependency policy

Retain upstream registry dependency versions and lints. The four Iceberg packages
are patched together at version `0.11.0`; `iceberg-property-macro` is vendored
without source patches. Do not vendor runtime/network/Arrow crates or add resource
accounting hooks. Only the approved protocol/API patch inventory may diverge.

## Validation

- Compare the initial source commit against the recorded package.
- Use workspace dependency patches for consumer checks and tests.
- Independently check this crate with sibling Iceberg patches when resolvable;
  record unavailable upstream development dependencies as a validation gap.

## Patch inventory

No patches applied in the initial source commit.

## U3 — snapshot reference map accessor

- Purpose: preserve the 0.9 `TableMetadata::refs()` read-only accessor.
- Files: `src/spec/table_metadata.rs`.
- Upstream status: no tracked upstream issue; rc.2 exposes only individual reference lookup.
- Production dependency: NovaRocks dependency validation, ref preparation, complete branch/table/call retention policy and reclamation protection observe the authoritative full branch/tag reference set.
- Exit: remove when upstream exposes the same complete reference-map accessor, or a reviewed consumer migration removes the need for it.
- Coverage: connector dependency/ref tests, fixed retention graphs and actual reclamation root/freshness tests; V2 standalone core check.

## P7a — missing v3 next-row-id read compatibility

- Purpose: preserve the 0.9 read default of `INITIAL_ROW_ID` when v3 metadata omits `next-row-id`; serialization continues emitting it.
- Files: `src/spec/table_metadata.rs`.
- Upstream status: no tracked upstream issue; rc.2 requires the field.
- Production dependency: reading existing Spark REST v3 metadata without the field.
- Exit: IRU-8 replaces the external semantic-identity compatibility boundary and removes this patch.
- Coverage: migrated `test_table_metadata_v3_missing_next_row_id_defaults_to_initial`; existing v3 metadata serde tests.

## U4 — direct child directory listing

- Purpose: preserve the 0.9 directory-listing capability on Storage and FileIO, with local-filesystem and in-memory implementations.
- Files: `src/io/storage/{mod,local_fs,memory}.rs`, `src/io/file_io.rs`.
- Upstream status: no tracked upstream issue; rc.2 has no directory-listing method.
- Production dependency: Hadoop catalog namespace/table discovery; unsupported storage backends return FeatureUnsupported.
- Exit: IRU-6c migrates the Hadoop storage discovery protocol and removes this patch when its consumer no longer needs the SDK directory extension. IRU-6a retains the existing Hadoop protocol.
- Coverage: existing connector Hadoop catalog discovery tests; V2 standalone core check. Old core has no dedicated directory-listing tests.
