# NovaRocks Codex Environments

This directory contains workspace-scoped local development environment helpers
for Codex workspaces.

The scripts derive a stable environment id from the current workspace path.
Each workspace gets its own Docker Compose project, volumes, host ports, object
store warehouse prefix, Spark runtime, and generated NovaRocks test configs.

`environment.toml` points its setup script at `iceberg-rest-up.sh` and its
cleanup script at `iceberg-rest-down.sh --purge`, so a Codex workspace can
bootstrap and tear down its matching local services automatically.

## Start

```bash
.codex/environments/iceberg-rest-up.sh
```

The script writes generated files under a workspace-specific directory and
publishes a fixed discovery entry:

```text
.codex/environments/runtime/<env-id>/
.codex/environments/runtime/current/
```

Important generated files:

- `env.sh`: shell exports for this workspace.
- `manifest.json`: machine-readable ports, endpoints, compose project, and config paths.
- `README.md`: human-readable summary of the active environment.
- `standalone-managed-lake.toml`: NovaRocks standalone-server config.
- `sql-test.conf`: SQL test runner config.
- `ice-rest-catalog.sql`: REST catalog DDL for this workspace.
- `spark-defaults.conf`: Spark catalog config for REST Catalog + MinIO.
- `spark-iceberg-v3-smoke.sql`: Spark SQL that creates and writes a format-v3 Iceberg table.

Use the generated configs like this:

```bash
source .codex/environments/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```

Run the Spark Iceberg v3 smoke SQL:

```bash
.codex/environments/iceberg-rest-spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"
```

The Spark service talks to REST Catalog at `http://rest:8181` and MinIO at
`http://minio:9000` from inside the Docker network. NovaRocks talks to the same
services through the host-mapped endpoints recorded in `env.sh`.

The default REST Catalog image is `apache/iceberg-rest-fixture:1.8.1` because
`tabulario/iceberg-rest:1.6.0` rejects Iceberg format-version 3 tables.

If Docker Hub is unavailable, pull and tag the REST fixture image from a mirror
first:

```bash
docker pull --platform linux/arm64 dockerproxy.net/apache/iceberg-rest-fixture:1.8.1
docker tag dockerproxy.net/apache/iceberg-rest-fixture:1.8.1 apache/iceberg-rest-fixture:1.8.1
```

If Docker Hub is unavailable, pull and tag the Spark image from a mirror first:

```bash
docker pull docker.1panel.live/tabulario/spark-iceberg:3.5.5_1.8.1
docker tag docker.1panel.live/tabulario/spark-iceberg:3.5.5_1.8.1 tabulario/spark-iceberg:3.5.5_1.8.1
```

Use `SPARK_ICEBERG_IMAGE=<image>` before setup if you want a different Spark +
Iceberg runtime.

## Status

```bash
.codex/environments/iceberg-rest-status.sh
```

## Stop

```bash
.codex/environments/iceberg-rest-down.sh
```

Remove the workspace-specific Docker volume as well:

```bash
.codex/environments/iceberg-rest-down.sh --volumes
```

Codex workspace cleanup uses the stronger purge mode:

```bash
.codex/environments/iceberg-rest-down.sh --purge
```

That stops the workspace-specific Docker Compose project, removes its Docker
volume, and deletes `.codex/environments/runtime/<env-id>/`.

It also removes `.codex/environments/runtime/current` when that entry points at
the purged workspace environment.
