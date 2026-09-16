<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# SQL Tests

SQL tests have two physical roots:

- `tests/sql/correctness/` contains the small-data correctness corpus used by development and full CI.
- `tests/sql/benchmarks/` contains fixed-data performance workloads. They are not selected by correctness CI.

## Object Store Prerequisite

Only Iceberg and other object-store-backed suites require a
reachable MinIO-compatible object store at `http://127.0.0.1:9000`.

Default credentials (matching the standalone defaults):

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

## Who Owns the Server

The runner's default `--cluster-mode all-in-one` does **not** start a server: it
connects to the host and port its config names, so a server must already be
running there. `--cluster-mode cross-process` is the opposite — the runner
launches and owns the FE/BE cluster itself, locating the binary through
`NOVAROCKS_BIN` or a build under `target/`.

## Default Standalone Flow

Start a server first. It takes one FE config and one BE config — copy
`novarocks-fe.toml.example` and `novarocks-be.toml.example` from the repo root,
or use the generated pair below. There is no `--port` flag:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone \
  --role all-in-one --fe-config ./novarocks-fe.toml --be-config ./novarocks-be.toml
```

Inside a worktree, do not assume a port — source the generated environment and
use its configs, so this worktree cannot collide with another:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone \
  --role all-in-one --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
```

When backgrounding the server, gate the first query on the `NOVAROCKS_READY`
marker it prints after binding — probing the port alone cannot tell a fresh
server from a leftover process that already owned it.

Then run a suite:

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml --bin novarocks-sql-test -- \
  --suite filter --mode verify
```

The runner defaults to `tests/sql/runner/conf/default.toml` (host `127.0.0.1`,
port `9030`) when no explicit `--config` is provided; pass
`--config "$NOVAROCKS_SQL_TEST_CONFIG"` to target the generated worktree
environment instead. Suites that need an Iceberg fixture should pass the
generated environment config or an explicit fixture config.

`tests/sql/correctness/README.md` carries the suite map — which engine area each
suite covers and what fixture or topology it needs. Choose suites from it rather
than running the whole corpus.

## Benchmark Flow

Benchmarks are deliberately separate from correctness suites. Use the local
wrapper, which builds the release server binary and defaults to SSB:

```bash
tools/benchmark/run-sql-benchmark.sh
```

Pass benchmark-runner options after the script name to choose a workload or an
output location. Benchmarks always use the cross-process FE/BE harness; choose
the number of BEs for this run with `--backend-count` (the default is one):

```bash
tools/benchmark/run-sql-benchmark.sh --suite tpc-ds --backend-count 2
tools/benchmark/run-sql-benchmark.sh --suite all --backend-count 4 --output-dir /tmp/novarocks-benchmarks
```

The benchmark runner resolves the fixed shared fixture before any suite hook.
It verifies results, performs one warmup pass, records five serial measured
passes, and captures a profile pass. Generated reports go to
`reports/sql-benchmarks/` and do not belong in correctness CI.

An external controller may attach an opaque environment JSON object and a
comparison key. The runner does not inspect host CPU, memory, or OS details;
it records these controller-provided values unchanged in both `run.json` and
`SUMMARY.md` for the controller's cross-machine comparison policy:

```bash
tools/benchmark/run-sql-benchmark.sh --backend-count 3 \
  --controller-environment '{"machine_pool":"nightly-a","storage":"local-minio"}' \
  --comparison-key nightly-a-release
```

## Explicit Iceberg Config

For Docker-backed Iceberg suites, prefer the generated fixture config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml --bin novarocks-sql-test -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite materialized-view --mode verify
```
