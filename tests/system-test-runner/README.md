# NovaRocks system scenarios

`novarocks-system-tests` is the imperative, process-boundary test frontend.
It owns scenario discovery, actors, bounded deadlines, oracle composition and
failure reporting. `novarocks-cluster-harness` remains the only owner of
1FE+NBE configuration, spawn, readiness, topology, faults, restart, logs and
cleanup.

List registered scenarios:

```bash
cargo run -p novarocks-system-test-runner -- --list
```

Run one or all scenarios against the native 1FE+3BE default:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run -p novarocks-system-test-runner --profile dev-opt -- \
  --binary target/dev-opt/novarocks \
  --config "$NOVAROCKS_STANDALONE_CONFIG" \
  --artifact-root "$(mktemp -d)" \
  --cluster-size 3 \
  --timeout-secs 300 \
  --only query-lifecycle/mysql-disconnect
```

Scenarios run sequentially. On failure the runner prints action history,
process diagnostics, the retained runtime/log directory and an exact rerun
command. Successful scenarios explicitly stop their own process group and
remove the generated runtime directory.
