# Iceberg Compatibility SQL Suite

This suite validates cross-engine Iceberg compatibility.

The first case creates an Iceberg format-v3 table with Spark through the
workspace REST Catalog and MinIO object store, then reads the table through
NovaRocks.

Run it against the generated local environment (see
[`docker/iceberg-rest/README.md`](../../docker/iceberg-rest/README.md) for
how to bring it up):

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```
