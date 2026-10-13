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

## P2 — public terminal TableCommit builder

- Purpose: preserve the 0.9 patch that exposes `TableCommit::builder().build()`.
- Files: `src/catalog/mod.rs`.
- Upstream status: no tracked upstream issue; rc.2 keeps the builder crate-private.
- Production dependency: NovaRocks FrozenRequest converts one complete terminal request for `Catalog::update_table`; SDK actions do not own publication.
- Exit: IRU-6a removes this patch when its publication boundary no longer needs the SDK terminal builder.
- Coverage: existing connector FrozenRequest and commit tests; V2 standalone core check.

## U3 — snapshot reference map accessor

- Purpose: preserve the 0.9 `TableMetadata::refs()` read-only accessor.
- Files: `src/spec/table_metadata.rs`.
- Upstream status: no tracked upstream issue; rc.2 exposes only individual reference lookup.
- Production dependency: NovaRocks commit dependency validation observes the authoritative branch/tag reference set.
- Exit: remove when upstream exposes the same complete reference-map accessor, or a reviewed consumer migration removes the need for it.
- Coverage: existing connector commit dependency and ref tests; V2 standalone core check.

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
- Exit: IRU-6a removes this patch after the catalog storage boundary owns directory discovery without the SDK extension.
- Coverage: existing connector Hadoop catalog discovery tests; V2 standalone core check. Old core has no dedicated directory-listing tests.

## U2 — add-schema last-column-id

- Purpose: preserve the optional REST `TableUpdate::AddSchema.last_column_id` field, including omitted-field serde behavior.
- Files: `src/catalog/mod.rs`, `src/spec/table_metadata_builder.rs`, `src/transaction/update_schema.rs`.
- Upstream status: no tracked upstream issue; rc.2 omits the high-watermark field.
- Production dependency: FrozenRequest and authoritative staged-create initialization preserve catalog metadata high-watermarks.
- Exit: IRU-13b removes this patch when the connector owns the REST update vocabulary.
- Coverage: migrated `catalog::tests::test_add_schema` serde expectations for Some(3)/None; existing table metadata builder and update-schema tests adapted to the additive field.
- Port adaptation: the new 0.11 SDK update-schema action emits `last_column_id: None`; its schema-match tests ignore the new field. No SDK mutation semantics changed.
