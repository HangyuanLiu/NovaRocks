# Task 6A Report: Restore EXPLAIN Iceberg UK/FK Metadata

## Status

DONE

## Change

- Added `TableLookupMode::{SchemaOnly, ExplainStats}` at the SQL catalog boundary.
- Kept named external `SchemaOnly` resolution on the existing catalog registry/cache path.
- Made named external `ExplainStats` resolution load the connector table and use the full
  `build_table_def` path, preserving Iceberg `serialized_metadata` for UK/FK rewrite planning.
- Routed only `EXPLAIN` and `EXPLAIN ANALYZE` through `ExplainStats`.
- Marked normal query, query preparation, MV, mutation, and view provider construction as
  `SchemaOnly`.
- Left default/local resolution, Iceberg metadata-table resolution, and schema-cache payload
  behavior unchanged.

## TDD Evidence

### RED

Command:

```bash
cargo test --lib sql::catalog::provider::tests::explain_stats_external_lookup_uses_full_metadata_builder -- --exact --test-threads=1
```

Result: expected compile failure before the production change.

- `could not find TableLookupMode in catalog` at the new provider test.
- `CatalogServiceProvider::new` accepted three arguments, so the fourth mode argument was
  rejected.

This demonstrated that the required mode-aware provider API was missing, rather than a test
fixture failure.

### GREEN

Command:

```bash
cargo test --lib sql::catalog::provider::tests::explain_stats_external_lookup_uses_full_metadata_builder -- --exact --test-threads=1
```

Result: `1 passed; 0 failed`.

The test verifies no registry lookup, one connector load, one full builder invocation, no schema
or metadata-row builder invocation, and retained `serialized_metadata == "full-metadata"`.

## Verification

| Command | Result |
|---|---|
| `cargo test --lib sql::catalog -- --test-threads=1` | Passed: 17 passed, 0 failed |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --all-targets` | Passed with existing repository warnings |
| `git diff --check` | Passed |

The SQL targeted/full CI was intentionally not run; it is owned by the main agent.

## Commit

`fix: restore full metadata for explain planning`
