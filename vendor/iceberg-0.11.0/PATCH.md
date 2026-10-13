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
