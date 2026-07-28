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

# StateStore Tests

## Contract and Conformance Ownership

The canonical StateStore contract is `novarocks_spi::state_store`, and the
shared provider conformance suite is
`novarocks_spi::state_store::conformance`. The `state-store-conformance`
feature is provider-test-only: ordinary provider crates depend on
`novarocks-spi` normally and enable that feature only in dev-dependencies.
Both production provider gates run
`tools/ci/check-spi-dependency-boundary.py`. The check reads Cargo metadata and
the resolved default dependency DAG: required normal dependencies must be
exactly `async-trait`, `bytes`, `sha2`, and `uuid`; Tokio must remain the sole
optional normal dependency, owned only by `state-store-conformance`, and absent
from the default graph.

The SPI owns the provider factory, instance, descriptor, lifecycle, and open
request contracts. The state-store crate owns the typed provider registry,
`StateStoreHost`, configuration binding, and provider-private runtimes. Provider
identity is exposed by the descriptor or host and carried in metrics as a
`StateStoreProviderId`; it is not a `StateStore` method.

The current SPI-2 release gate is SQLite correctness, frontend lifecycle
coverage, and the real 1FE+3BE SQLite restart scenario. The MySQL and
FoundationDB commands below remain authoritative for later live-provider
runtime validation, but passing those external-service suites is not required
to merge SPI-2. Feature compilation still covers their complete factory and
instance abstractions. Test-hook provider harnesses are internal test tools,
not production composition APIs; production composition must use
`StateStoreHost`.

Focused contract checks:

```bash
cargo test -p novarocks-spi
tools/ci/check-spi-dependency-boundary.py --manifest-path Cargo.toml
cargo test -p novarocks-state-store --test state_store_sqlite -- --test-threads=1
cargo test -p novarocks-state-store \
  --features mysql-state-store-provider,state-store-test-hooks \
  --test state_store_mysql mysql_suite -- --exact --test-threads=1
cargo test -p novarocks-state-store \
  --features foundationdb-provider,state-store-test-hooks \
  --test state_store_foundationdb foundationdb_suite -- --exact --test-threads=1
```

## FoundationDB State Store

The FoundationDB provider is feature-gated and uses the official 7.3.69 native
client/server with Rust API 730. The Linux x86_64 production gate is:

```bash
docker/foundationdb/up.sh
tools/ci/foundationdb-provider.sh
```

The workflow calls `docker/foundationdb/up.sh`; that fixture's exact self-check
validates the pinned version, API, and platform-specific asset SHA. The gate
only consumes the generated environment and validates that it exists, targets
Linux x86_64, and contains the required client artifacts.

The pinned official client assets are
`FoundationDB-7.3.69_arm64.pkg` with SHA-256
`6bfbd48ac21356de0baa0c1e84c6e33d15d95d0b9d022c35a7625e5d9293b71e`
for macOS arm64 developer use, and
`foundationdb-clients_7.3.69-1_amd64.deb` with SHA-256
`ea59d1708519798c7bc4f514cd29af1ac8e41dccbec4371f22d86b713ea81cbf`
for Linux x86_64 production CI. macOS is auxiliary evidence only.

`state_store_foundationdb` runs all provider-specific scenarios and, through
`foundationdb_suite`, both the shared SPI StateStore conformance suite and the
provider-neutral coordination conformance in one explicit runtime lifecycle.
`state_store_foundationdb_cross_process` starts two independent helper
processes against the same generated cluster and keyspace. Those helpers are
FDB clients, not FEs, so this test must not be described as a real two-FE
deployment. `cross_process_three_be_state_store_baseline` remains the real
1FE+3BE query baseline, but it intentionally leaves FoundationDB disabled; its
role is regression and no-fallback evidence.

Commit-state native error logs may contain only the canonical UUID
`transaction_id`, `phase`, `native_error_code`, and `category` in addition to
the documented lifecycle/API/readiness/keyspace-hash fields. Never put
cluster-file contents, TLS passwords, private-key/certificate contents,
credentials, logical keys/values, secrets, or the raw keyspace UUID in logs or
goldens. Finish live-provider tests by dropping transaction and store handles,
then shut down the provider test harness. Only after successful harness
shutdown should the test fixture run `docker/foundationdb/down.sh --docker`.

## MySQL State Store

`mysql-state-store-provider` pins the optional Tokio-native client to
`mysql_async 0.37.0` with the minimal Rustls feature set. Default and
feature-off builds retain the MySQL configuration vocabulary but do not include
the async driver; selecting MySQL in those builds returns a typed
`ProviderNotCompiled` host error and never falls back to SQLite.

The production acceptance fixture is the pinned MySQL 8.4.10 container under
`docker/mysql-state-store/`. The Homebrew server is auxiliary developer evidence
only and does not replace the pinned fixture.

On Linux x86_64, run the same production gate as CI from a fresh fixture:

```bash
trap 'docker/mysql-state-store/down.sh --docker' EXIT
docker/mysql-state-store/up.sh
source docker/mysql-state-store/runtime/current/env.sh
tools/ci/mysql-state-store-provider.sh
```

The gate deliberately runs both `probes/contract.sh` and the public provider
3072/3073-byte exact test; neither is a substitute for the other. FoundationDB
coverage in this gate is feature-off and non-live, so it does not require
`libfdb_c`. The final 1FE+3BE baseline also leaves the MySQL provider disabled
and proves only additive/no-fallback behavior, not a two-FE failover.

`state_store_mysql` runs the provider scenarios and, through `mysql_suite`, both
the shared SPI StateStore conformance suite and the provider-neutral
coordination conformance. Each
conformance factory invocation uses a separately provisioned database while
sharing one explicit MySQL runtime. `state_store_mysql_cross_process` starts
two independent exec helper clients against an ordinary-provider credential and
shared schema. The parent test is the only database provisioner; helper
environments contain no provisioner credential or compose runtime material.
These helpers are MySQL clients, not FEs, so this test is not a real two-FE
deployment.

Run the MySQL cross-process suite with the pinned production fixture:

```bash
cargo test -p novarocks-state-store \
  --features mysql-state-store-provider,state-store-test-hooks \
  --test state_store_mysql_cross_process mysql_cross_process_suite \
  -- --exact --test-threads=1
```

The helper uses a strict, ordered JSONL protocol containing only
`Open`, `Begin`, `Get`, `Range`, `Put`, `Delete`, `Commit`, `Resolve`, `Poll`,
and `Shutdown`. Protocol errors are flushed before a deterministic nonzero
exit. Normal `Shutdown` aborts active transactions and waits for explicit
runtime disconnect. Diagnostics must never contain credentials, a DSN, a raw
database or cluster identifier, or logical keys and values.
