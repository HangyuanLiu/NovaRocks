# Iceberg Hive Metastore Test Environment

Standalone Hive Metastore fixture for NovaRocks Iceberg HMS catalog testing.

This directory is intentionally separate from `docker/iceberg-rest/`. The REST
fixture owns MinIO, REST Catalog, and Spark. The Hive fixture owns only HMS and
joins the REST fixture's Docker network so the metastore can reach MinIO at
`http://minio:9000`.

## Prepare Runtime Only

Generate this worktree's HMS runtime entry without starting Docker:

```bash
docker/iceberg-hive/up.sh --prepare-only
source docker/iceberg-hive/runtime/current/env.sh
```

## Start Docker

Start REST/MinIO first, then HMS:

```bash
docker/iceberg-rest/up.sh
docker/iceberg-hive/up.sh
source docker/iceberg-rest/runtime/current/env.sh
source docker/iceberg-hive/runtime/current/env.sh
```

The script writes generated state under:

```text
docker/iceberg-hive/runtime/<env-id>/
docker/iceberg-hive/runtime/current/
```

Important generated files:

- `env.sh` — shell exports for the HMS endpoint and warehouse.
- `manifest.json` — machine-readable HMS endpoint, compose project, and network.
- `README.md` — human-readable summary of the active HMS environment.
- `ice-hms-catalog.sql` — sample NovaRocks `CREATE EXTERNAL CATALOG` SQL.
- `spark-hms-defaults.conf` — extra Spark defaults for the `hms_catalog` catalog.

The SQL test runner picks up HMS placeholders from environment variables, so
source both runtime files before running HMS suites:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-hms --mode verify
```

After sourcing the HMS env, `docker/iceberg-rest/spark-sql.sh` automatically
loads the generated HMS Spark defaults through `NOVAROCKS_SPARK_EXTRA_DEFAULTS`.
Spark should use `hms_catalog` and the in-network endpoint `thrift://hms:9083`.

## Status

```bash
docker/iceberg-hive/status.sh
```

## Stop

```bash
docker/iceberg-hive/down.sh
```

In shared Docker mode this leaves the shared HMS Docker service running. Remove
only the current worktree runtime entry with:

```bash
docker/iceberg-hive/down.sh --runtime-only --purge
```

Stop the shared HMS service explicitly:

```bash
docker/iceberg-hive/down.sh --docker
```

## Required Base Image

The default image is built locally from this directory and tagged as
`novarocks/hive-metastore:4.0.0`. It uses `apache/hive:4.0.0` plus Hadoop S3A
support jars.

If the bundled Hadoop version in `apache/hive:4.0.0` changes, check it before
building and update `HADOOP_VERSION` in `Dockerfile`:

```bash
docker run --rm --entrypoint bash apache/hive:4.0.0 -lc 'ls /opt/hive/lib/hadoop-common-*.jar'
```

Build manually if needed:

```bash
docker build -t novarocks/hive-metastore:4.0.0 docker/iceberg-hive
```
