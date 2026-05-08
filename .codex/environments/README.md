# NovaRocks Codex Environments

This directory holds only the Codex workspace setup manifest
(`environment.toml`).

The actual local Iceberg REST + MinIO + Spark test environment lives at
[`docker/iceberg-rest/`](../../docker/iceberg-rest/). The Docker services are
shared across worktrees by default; each Codex setup runs
`docker/iceberg-rest/up.sh --prepare-only`, which only generates this
worktree's runtime entry, NovaRocks server port, and config files. It does not
start Docker. See that README for usage, ports, and CI integration details.
