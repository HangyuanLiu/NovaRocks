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

# Tests Overview

This directory contains:

- Rust tests (`tests/*.rs` and unit tests under `src/**`)
- Shared Rust test helpers (`tests/common/**`)
- Test data (`tests/data/**`)

## Directory Layout

```text
tests/
├── README.md
├── *.rs                     # Rust integration tests (unit-like / fast path)
└── common/                  # Shared Rust test helpers
```

## Quick Entry

- Rust tests: `cargo test`
- SQL tests guide: `sql-tests/README.md`

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

`state_store_foundationdb` runs all provider-specific scenarios and all 13
shared conformance cases in one explicit runtime lifecycle.
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
goldens. Finish tests by dropping transaction and store handles, then call
`StateStoreRuntime::shutdown()`, and only after successful runtime shutdown run
`docker/foundationdb/down.sh --docker`.

## MySQL State Store Boundary

`mysql-state-store-provider` pins the optional Tokio-native client to
`mysql_async 0.37.0` with the minimal Rustls feature set. Default and
feature-off builds retain the MySQL configuration vocabulary but do not include
the async driver; selecting MySQL in those builds returns a typed
`InvalidConfiguration` error and never falls back to SQLite.

Task 1 covers only the dependency, nested `[state_store.mysql_client]`
configuration, non-secret static validation, 3072-byte provider-effective key
limit, and architecture ownership guard. Password values are not read during
configuration loading, and no MySQL schema, transaction, or runtime provider is
implemented in this task.

Focused contract checks:

```bash
cargo test --test state_store_contract mysql_
cargo test --lib state_store::limits::tests::mysql_
cargo test --test architecture_guard state_store_mysql
```

## About Rust Target Discovery

Cargo auto-discovers `tests/*.rs`.  
Data-dependent SSB checks are maintained as SQL+result cases under `sql-tests/ssb/sql/`.
