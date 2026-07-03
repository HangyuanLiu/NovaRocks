# SQL Tests

`sql-tests/` is the standalone SQL regression suite for NovaRocks.

## Object Store Prerequisite

Only Iceberg REST, managed-lake, and other object-store-backed suites require a
reachable MinIO-compatible object store at `http://127.0.0.1:9000`.

Default credentials (matching the standalone-server defaults):

- access key: `admin`
- secret key: `admin123`
- bucket: `novarocks`

If a selected suite declares an object-store warehouse and MinIO is not running,
the runner fails fast before executing that suite:

```
MinIO at http://127.0.0.1:9000 is unreachable.
hint: start it with:
  mkdir -p ~/minio-data && minio server ~/minio-data --console-address :9001 &
```

Example local startup:

```bash
mkdir -p ~/minio-data
minio server ~/minio-data --console-address :9001 &
```

## Default Standalone Flow

Start the standalone server on `9030`:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030
```

Then run a suite:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite filter --mode verify
```

The runner defaults to `tests/sql-test-runner/conf/sr.conf` when no explicit
`--config` is provided. Suites that need an Iceberg/managed-lake fixture should
pass the generated environment config or an explicit fixture config.

## Explicit Iceberg Config

For Docker-backed Iceberg suites, prefer the generated fixture config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite materialized-view --mode verify
```
