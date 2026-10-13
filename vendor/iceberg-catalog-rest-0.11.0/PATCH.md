# NovaRocks patches over apache/iceberg-rust 0.11.0-rc.2

Upstream tag: `v0.11.0-rc.2`
Upstream commit: `859048563bc71a3167d0545cb66756d26c64e4ca`
Upstream repository: https://github.com/apache/iceberg-rust
Normalized package: `iceberg-catalog-rest-0.11.0.crate`
Package SHA-256: `fde7674c80f19dc54ae2b0b3fb95f0715a4407df615fa63fe42801bc3ec2d2d4`

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

### R6: redacted storage-delegation handoff

- Files: `src/catalog/extension.rs`, `src/catalog.rs`.
- Purpose: retain `StorageCredentialDelegation` and `RestAccessDelegation` with
  exact-key lookup and credential-free Debug output; no raw map or serialization.
- Upstream status: no equivalent closed handoff surface at the pinned baseline.
- Production dependency: NovaRocks credential parsing in `loaded_table.rs`.
- Exit: IRU-13a replaces the vendored REST evidence and credential handoff.
- Coverage: `catalog::extension::tests::storage_delegation_exposes_exact_keys_without_debug_secrets`.
- Validation: standalone REST crate tests once the shared Cargo window is free.

### R7: deferred table materialization bound to the loading owner

- Files: `src/catalog/extension.rs`, `src/catalog.rs`.
- Purpose: keep deferred table/staged-create response facts opaque until the
  provider chooses an admitted FileIO; capture Runtime and KMS from the loading
  RestSessionCatalog and pass them to TableBuilder without ambient discovery.
- Upstream status: no equivalent deferred credential handoff at the baseline.
- Production dependency: NovaRocks `loaded_table.rs`, `catalog/rest.rs`, and
  execution credential acquisition materialize the response with their FileIO.
- Exit: IRU-13a replaces the vendored materialization and evidence surface.
- Coverage: `catalog::extension::tests::deferred_materialization_retains_loading_runtime_and_kms_without_ambient_runtime`;
  deferred load/stage component tests migrate with R5 and R3.
- Validation: standalone REST crate tests after the shared Cargo window is free.
