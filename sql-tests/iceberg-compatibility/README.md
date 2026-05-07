# Iceberg Compatibility SQL Suite

This suite validates cross-engine Iceberg compatibility.

The first case creates an Iceberg format-v3 table with Spark through the
workspace REST Catalog and MinIO object store, then reads the table through
NovaRocks.

Run it against the generated Codex environment:

```bash
source .codex/environments/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```
