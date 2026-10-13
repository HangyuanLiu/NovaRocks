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

### R5: deferred delegated table load

- Files: `src/catalog/extension.rs`.
- Purpose: retain only `load_table_deferred_with_access_delegation`; issue one
  delegated GET, accept 200/304, retain TableNotFound on 404, defer FileIO.
  RestCatalog forwards its bound SessionContext to the session owner.
- Upstream status: no equivalent deferred delegated table load at the baseline.
- Production dependency: NovaRocks REST owner, loaded-table materialization and
  execution credential acquisition. Unused eager delegated load/create APIs and
  public header constants are intentionally not restored.
- Exit: IRU-13a replaces the vendored deferred handoff.
- Coverage: migrated `access_delegation_is_opt_in_and_preserves_redacted_facts`.
- Validation: standalone REST crate tests after the shared Cargo window is free.

### R8: authenticated credential renewal without table observation

- Files: `src/catalog/extension.rs`, `src/types.rs`.
- Purpose: retain the credential-only wire response and renewal GET through the
  initialized authenticated client, without table metadata or FileIO selection.
  Renewal is not gated by config endpoint declarations.
- Upstream status: no equivalent closed renewal handoff at the baseline.
- Production dependency: NovaRocks loaded-table and execution lease renewal.
- Exit: IRU-13a replaces the vendored credential renewal protocol surface.
- Coverage: migrated `credential_refresh_uses_catalog_authentication_and_redacts_values`.
- Validation: standalone REST crate tests after the shared Cargo window is free.

### R2: authoritative staged-create result

- Files: `src/catalog/extension.rs`, `src/catalog.rs`.
- Purpose: retain StagedTableCreate and its table/initialization-update accessors
  as the typed stage result; the unused untyped stage API is not restored.
- Upstream status: no equivalent authoritative staged result at the baseline.
- Production dependency: NovaRocks REST staged-create owner.
- Exit: IRU-13a replaces the vendored staged-create protocol surface.
- Coverage: authoritative staged-create component tests migrate with R3.
- Validation: standalone REST crate tests after the shared Cargo window is free.

### R3: typed staged-create and single publication dispatch

- Files: `src/catalog/extension.rs`, `src/catalog.rs`.
- Purpose: retain StagedCreateError/StagedCommitError and four typed public
  stage/commit entrypoints, implemented on RestSessionCatalog and forwarded
  from RestCatalog. Static stage still sends the access-delegation header;
  caller-FileIO commit does not select a catalog-global storage capability.
- Dispatch classification: client/request construction failure is undispatched;
  query_catalog failure is possibly dispatched, including buffered body-read
  failure (the deliberately accepted 0.11 difference); statuses preserve 0.9.
- Upstream status: no equivalent typed dispatch-certainty surface at baseline.
- Production dependency: NovaRocks REST CTAS staging and publication owner.
- Exit: IRU-13a replaces typed protocol/evidence and single-dispatch surfaces.
- Coverage: all retained typed stage/commit tests and authoritative staged
  snapshot request test migrated; P8 V2/V3 format, snapshot-state refusal and
  partition watermark tests preserved. Unused eager/extension helpers omitted.
- Validation: standalone REST crate check/tests after the shared window is free.

### R1: REST view protocol

- Files: `src/catalog.rs`, `src/catalog/extension.rs`, `src/types.rs`.
- Purpose: restore the six Catalog view methods, view endpoint URL helpers and
  four wire types over RestSessionCatalog using the bound facade context.
  view_exists always uses HEAD without endpoint-advertisement fallback;
  drop-view 404 text remains `Tried to drop a view that does not exist`.
- Upstream status: no equivalent view endpoints at the pinned baseline.
- Production dependency: NovaRocks view catalog-control and private delegate.
- Exit: IRU-13a replaces the vendored REST view protocol surface.
- Coverage: existing NovaRocks view connector/SQL tests; list-view protocol
  component test migrates with U1r (0.9 has no other view-specific unit test).
- Validation: standalone REST crate check/tests after the shared window is free.

### U1r: bounded single-page REST listings

- Files: `src/catalog/extension.rs`.
- Purpose: restore one-page tables/namespaces/views methods with unchanged
  facade signatures; query ordering remains pageSize, optional parent, then
  opaque pageToken. The connector owns accumulation and repeated-token refusal.
  Preserve the old 404 error kinds rather than inheriting new list API mapping.
- Upstream status: only accumulating list APIs exist at the pinned baseline.
- Production dependency: NovaRocks catalog listing-admission and discovery.
- Exit: IRU-13a replaces the vendored one-page protocol surface.
- Coverage: four migrated single-page/token/parent-filter tests from 0.9.
- Validation: standalone REST crate check/tests after all patches are ready.
