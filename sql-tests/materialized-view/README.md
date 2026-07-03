Materialized view SQL tests for NovaRocks.

The active C1 suite is intentionally lake-native only. It verifies that the
`materialized-view` runner suite uses its suite-level REST Iceberg catalog and
covers the basic Iceberg-target MV create / refresh / read / drop path.

The historical StarRocks-compatible MV cases from `dev/test/sql/test_materialized_view`
are parked under `legacy/`. They cover OLAP rewrite, refresh, status, privilege,
sync-MV, nested-MV, and partition-compensation behavior that is not the NIDL-C1
test-migration target and should be reintroduced only through a dedicated
lake-native rewrite or compatibility task.

Recommended invocation:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite materialized-view \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --mode verify
```

The runner expands `${uuid0}` style placeholders per run to avoid catalog,
database, and table name collisions.
