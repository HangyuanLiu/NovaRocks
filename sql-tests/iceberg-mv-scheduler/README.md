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

Iceberg MV scheduler SQL tests for NovaRocks.

This suite is for materialized views whose target storage engine is Iceberg and
whose refresh policy is driven by the standalone MV refresh scheduler:

- `REFRESH DEFERRED MANUAL`
- `REFRESH ASYNC ON CHANGE`
- `REFRESH ASYNC EVERY INTERVAL <n> <unit>`
- `ALTER MATERIALIZED VIEW ... PAUSE/RESUME REFRESH`

The suite requires a standalone-server process started with scheduler-enabled
configuration. In the generated Iceberg REST test environment, use:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost \
target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_SCHEDULER_CONFIG"

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-mv-scheduler \
  --mode verify
```

The normal `iceberg-ivm` suite remains the broad Iceberg-backed MV correctness
gate. This suite is intentionally smaller and focuses on whether the scheduler
actually fires refresh work.
