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
