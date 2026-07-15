# FoundationDB 7.3.69 fixture

This fixture provides a worktree-scoped FoundationDB client and single-server
cluster for NovaRocks state-store tests. The server image and official native
client are pinned to FoundationDB 7.3.69. The Rust binding selects API version
730 separately at compile time. Starting the server requires Docker 29 or
newer with Docker Compose.

Supported client platforms:

- macOS arm64 developer machines:
  `FoundationDB-7.3.69_arm64.pkg`, SHA-256
  `6bfbd48ac21356de0baa0c1e84c6e33d15d95d0b9d022c35a7625e5d9293b71e`.
- Linux x86_64 production CI:
  `foundationdb-clients_7.3.69-1_amd64.deb`, SHA-256
  `ea59d1708519798c7bc4f514cd29af1ac8e41dccbec4371f22d86b713ea81cbf`.

The scripts reject other native-client platforms and reject assets whose hash
does not match the pinned value. Runtime files and downloaded binaries stay
under the ignored `runtime/` directory.

Prepare the client and generated environment without starting Docker:

```bash
docker/foundationdb/up.sh --prepare-only
source docker/foundationdb/runtime/current/env.sh
```

The generated environment exports the cluster file, keyspace UUID, client
library, `fdbcli`, runtime library path, Docker Compose project, and runtime
paths. The environment and keyspace UUID are derived from the canonical
worktree path, so separate worktrees do not share a Docker project or logical
keyspace.

Start the pinned Linux amd64 server and wait for `fdbcli status` readiness:

```bash
docker/foundationdb/up.sh
docker/foundationdb/status.sh
```

`status.sh` prints the cluster-file path, but never prints its contents. The
host cluster file points at the worktree-specific published port.

Remove only this worktree's generated runtime while leaving Docker untouched:

```bash
docker/foundationdb/down.sh
```

Explicitly stop only this worktree's Compose project before removing its
runtime:

```bash
docker/foundationdb/down.sh --docker
```

Production deployments must install and verify the official client before
NovaRocks starts. They must not use this fixture as a runtime downloader.
