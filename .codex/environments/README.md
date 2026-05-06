# NovaRocks Codex Environments

This directory contains workspace-scoped local development environment helpers
for Codex workspaces.

The scripts derive a stable environment id from the current workspace path.
Each workspace gets its own Docker Compose project, volumes, host ports, object
store warehouse prefix, and generated NovaRocks test configs.

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

Use the generated configs like this:

```bash
source .codex/environments/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
```

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
