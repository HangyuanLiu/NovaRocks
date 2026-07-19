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

- StateStore provider and contract tests: `../../state-store/tests/README.md`

## About Rust Target Discovery

Cargo auto-discovers `tests/*.rs`.  
Data-dependent SSB checks are maintained as SQL+result cases under `sql-tests/ssb/sql/`.
