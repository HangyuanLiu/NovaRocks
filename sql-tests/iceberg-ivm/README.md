Iceberg IVM SQL tests for NovaRocks.

This suite is for materialized views whose target storage engine is Iceberg:
`CREATE MATERIALIZED VIEW ... PROPERTIES('storage_engine' = 'iceberg')`.

Scope:
- Iceberg-backed MV target creation in the active Iceberg catalog/database
- manual refresh into a normal Iceberg target table
- append-only incremental refresh over an Iceberg base table
- row-lineage based incremental apply for base DELETE, UPDATE, and equality-delete changes
- metadata-only / no-op refresh behavior
- target catalog visibility and DROP cleanup

Recommended invocation:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --mode verify
```
