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

## Default Standalone Flow

Start the standalone server on `9030`:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone --port 9030
```

Then run a suite:

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml --bin novarocks-sql-test -- \
  --suite filter --mode verify
```

The runner defaults to `tests/sql/runner/conf/default.toml` when no explicit
`--config` is provided. Suites that need an Iceberg fixture should
pass the generated environment config or an explicit fixture config.

## Benchmark Flow

Benchmarks are deliberately separate from correctness suites. Use the local
wrapper, which builds the release server binary and defaults to SSB:

```bash
tools/benchmark/run-sql-benchmark.sh
```

Pass benchmark-runner options after the script name to choose a workload or an
output location:

```bash
tools/benchmark/run-sql-benchmark.sh --suite tpc-ds
tools/benchmark/run-sql-benchmark.sh --suite all --output-dir /tmp/novarocks-benchmarks
```

The benchmark runner resolves the fixed shared fixture before any suite hook.
It verifies results, performs one warmup pass, records five serial measured
passes, and captures a profile pass. Generated reports go to
`reports/sql-benchmarks/` and do not belong in correctness CI.

## Explicit Iceberg Config

For Docker-backed Iceberg suites, prefer the generated fixture config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml --bin novarocks-sql-test -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite materialized-view --mode verify
```
