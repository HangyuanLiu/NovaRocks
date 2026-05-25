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
