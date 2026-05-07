# Iceberg REST + MinIO + Spark Test Environment

Workspace-scoped local Iceberg REST Catalog + MinIO object store + Spark
runtime for NovaRocks development and CI.

This environment is the canonical test fixture for the
`iceberg-compatibility` SQL suite (cross-engine: Spark writes, NovaRocks
reads) and is also used by the standard `iceberg` suite. The Codex workspace
manifest at `.codex/environments/environment.toml` points its setup/cleanup
hooks at `up.sh` / `down.sh --purge` here.

The scripts derive a stable environment id from the current workspace path,
so each workspace (and each git worktree) gets its own Docker Compose
project, volumes, host ports, object-store prefix, Spark runtime, and
generated NovaRocks test configs without colliding with peers.

## Start

```bash
docker/iceberg-rest/up.sh
```

The script writes generated state under a workspace-specific directory and
publishes a fixed discovery entry:

```text
docker/iceberg-rest/runtime/<env-id>/
docker/iceberg-rest/runtime/current/
```

Important generated files (under the runtime entry):

- `env.sh` — shell exports for this workspace.
- `manifest.json` — machine-readable ports, endpoints, compose project, and config paths.
- `README.md` — human-readable summary of the active environment.
- `standalone-managed-lake.toml` — NovaRocks standalone-server config.
- `sql-test.conf` — SQL test runner config.
- `ice-rest-catalog.sql` — REST catalog DDL for this workspace.
- `spark-defaults.conf` — Spark catalog config for REST Catalog + MinIO.
- `spark-iceberg-v3-smoke.sql` — Spark SQL that creates and writes a format-v3 Iceberg table.

Use the generated configs:

```bash
source docker/iceberg-rest/runtime/current/env.sh

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
docker/iceberg-rest/spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"
```

The Spark service talks to REST Catalog at `http://rest:8181` and MinIO at
`http://minio:9000` from inside the Docker network. NovaRocks talks to the
same services through the host-mapped endpoints recorded in `env.sh`.

## Status

```bash
docker/iceberg-rest/status.sh
```

## Stop

```bash
docker/iceberg-rest/down.sh
```

Remove the workspace-specific Docker volume as well:

```bash
docker/iceberg-rest/down.sh --volumes
```

Codex workspace cleanup (and CI) uses the stronger purge mode:

```bash
docker/iceberg-rest/down.sh --purge
```

That stops the workspace-specific Docker Compose project, removes its
Docker volume, and deletes `docker/iceberg-rest/runtime/<env-id>/`. It also
removes `docker/iceberg-rest/runtime/current` when that entry points at
the purged workspace environment.

## Required Images

Pull these once before first use:

```bash
docker pull quay.io/minio/minio:latest
docker pull quay.io/minio/mc:latest
docker pull apache/iceberg-rest-fixture:1.8.1
docker pull tabulario/spark-iceberg:3.5.5_1.8.1
```

The default REST Catalog image is `apache/iceberg-rest-fixture:1.8.1`
because `tabulario/iceberg-rest:1.6.0` rejects Iceberg format-version 3
tables.

If Docker Hub is unavailable, pull and tag from a mirror first:

```bash
docker pull --platform linux/arm64 dockerproxy.net/apache/iceberg-rest-fixture:1.8.1
docker tag dockerproxy.net/apache/iceberg-rest-fixture:1.8.1 apache/iceberg-rest-fixture:1.8.1

docker pull docker.1panel.live/tabulario/spark-iceberg:3.5.5_1.8.1
docker tag docker.1panel.live/tabulario/spark-iceberg:3.5.5_1.8.1 tabulario/spark-iceberg:3.5.5_1.8.1
```

Override the images with `ICEBERG_REST_IMAGE=<image>` or
`SPARK_ICEBERG_IMAGE=<image>` before invoking `up.sh` if you want a
different runtime.

## CI Integration

`up.sh` and `down.sh --purge` are designed to be safe to call from CI:

- `up.sh` is idempotent — re-runs reuse the existing runtime entry and
  ports if `env.sh` already exists, otherwise allocate fresh free ports.
- `down.sh --purge` removes the compose project, MinIO volume, and the
  per-workspace runtime directory.
- All ports are auto-discovered to avoid collisions with peer workspaces
  on the same host.
- The runtime directory (`docker/iceberg-rest/runtime/`) is gitignored.

A typical CI step:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
trap "docker/iceberg-rest/down.sh --purge" EXIT

NO_PROXY=127.0.0.1,localhost \
cargo run --release -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" &
SERVER_PID=$!
trap "kill $SERVER_PID; docker/iceberg-rest/down.sh --purge" EXIT

# wait for MySQL port to be open, then run the suite
until nc -z 127.0.0.1 "$NOVA_ENV_MYSQL_PORT"; do sleep 1; done

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```
