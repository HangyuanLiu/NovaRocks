# NovaRocks patches on top of crates.io iceberg-catalog-rest 0.9.0

- Implemented the Iceberg REST view endpoints on `RestCatalog`:
  create_view / load_view / update_view (commit) / drop_view /
  view_exists / list_views, plus `CreateViewRequest`, `LoadViewResult`,
  `CommitViewRequest` wire types. Upstream 0.9.0 has no view support.

- Added `RestCatalog::stage_create_table`, which uses the catalog's existing
  configured HTTP client, authentication, and FileIO construction while
  sending `stage-create: true` and accepting the specified null
  `metadata-location` response. The result carries initialization updates
  derived from authoritative staged metadata for a single subsequent
  `assert-create` commit.

- Added typed `stage_create_table_typed` and `commit_staged_table_typed`
  variants. They preserve `Conflict`, `KnownNotDispatched`,
  `PossiblyDispatched`, and committed-response-finalization states so a
  durable CTAS saga never classifies REST dispatch certainty from strings or
  the generic `Unexpected` error kind.

- Added the versioned catalog-native CTAS fenced staged-publication extension.
  `RestCatalog` installs it only when the real `/v1/config` response advertises
  `fenced-staged-publication=1`; a user property with that name cannot enable
  it. The extension reuses the resolved URI/prefix and the same authenticated,
  header-configured, redacting `HttpClient` as standard REST operations.

- Added typed `advance_fence`, `stage`, `inspect`, `publish`, and `abort`
  calls under
  `/v1/{prefix}/extensions/fenced-staged-publication/<operation>`. The bounded
  wire types carry stable saga/action identity, ordered generation, sealed
  digests, staged proof/locator, write completion, create policy, and terminal
  provenance. Errors preserve local `Unsupported`, typed stale/identity/digest
  conflicts, `KnownNotDispatched`, `PossiblyDispatched`,
  `CommittedResponseInvalid`, and `Ambiguous`; advertised endpoint 404s or
  invalid error bodies never become `NotCreated` or generic `Unsupported`.
  Only strict status-plus-kind protocol pairs produce typed `Unsupported` or
  `Conflict`; authentication, redirect, rate-limit, missing-endpoint, and
  mismatched status/kind responses fail closed as `Ambiguous`.

- The wire preserves the full lexicographically ordered generation triple
  (`control-plane-incarnation`, `resource-epoch`, `fence-generation`) rather
  than collapsing ownership into one counter. Stage and publish also carry the
  explicit create policy sealed by its digest. A successful stage response
  embeds the standard REST `LoadTableResult`; `RestCatalog` materializes that
  result with the same runtime context and FileIO configuration, without a
  second stage dispatch or client.

- Publication success is a tagged `Published` or `NoOp` disposition, so
  `IF NOT EXISTS` never collapses into a generic success. Historical `NoOp`
  may retain an unpublished staged locator for proof-bound cleanup, while an
  ambiguous observation carries a typed diagnostic and only optional opaque
  proof. Every response body,
  including 4xx and 5xx bodies, is content-length checked and incrementally
  limited to 64 KiB before decoding. Opaque locators, proofs, receipts,
  provenance, and provider payloads are redacted from `Debug` output.

## Validation

This extracted crate's manifest resolves `iceberg = "0.9.0"` from crates.io
unless the NovaRocks vendor patch is supplied explicitly. Run from this
directory (`vendor/iceberg-catalog-rest-0.9.0`); `CARGO_TARGET_DIR` is
deliberately unset, so Cargo uses this crate's default `target/` directory:

```bash
env -u CARGO_TARGET_DIR cargo tree \
  --config 'patch.crates-io.iceberg.path="../iceberg-0.9.0"' \
  -p iceberg-catalog-rest

env -u CARGO_TARGET_DIR cargo test \
  --config 'patch.crates-io.iceberg.path="../iceberg-0.9.0"' \
  --lib

source ../../docker/iceberg-rest/runtime/current/env.sh
env -u CARGO_TARGET_DIR cargo test \
  --config 'patch.crates-io.iceberg.path="../iceberg-0.9.0"' \
  --test staged_create_probe test_stage_create_local_fixture_probe -- \
  --ignored --exact --nocapture
```

The `cargo tree` output must identify both crates by their worktree paths,
not a registry source.
