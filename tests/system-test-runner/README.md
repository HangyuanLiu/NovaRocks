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
cargo build --workspace --profile dev-opt
cargo run -p novarocks-system-test-runner --profile dev-opt -- \
  --binary target/dev-opt/novarocks \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$(mktemp -d)" \
  --cluster-size 3 \
  --timeout-secs 300 \
  --only query-lifecycle/mysql-disconnect
```

No Docker fixture is required. Every registered scenario builds its Iceberg
warehouse on the local filesystem under the harness runtime directory, so
`tools/ci/fixtures/system-scenarios-base.toml` — SQLite StateStore, no
`[connector.object_store]` — is enough. A worktree's generated
The generated `$NOVAROCKS_FE_CONFIG` / `$NOVAROCKS_BE_CONFIG` pair also works
when started through the exact `--role all-in-one --fe-config ... --be-config
...` command; it only adds object-store settings the scenarios never read.

Scenarios run sequentially. On failure the runner prints action history,
process diagnostics, the retained runtime/log directory and an exact rerun
command. Successful scenarios explicitly stop their own process group and
remove the generated runtime directory.

## In CI

`tools/ci/local-full-ci.sh` runs this registry as its own stable stage,
between the server binary smoke and the SQL suites. The stage discovers
scenarios through `--list` and runs each one with a single `--only`
invocation, so every scenario gets an independent summary row, log and
artifact directory. It selects and reports only — `tools/ci/lib/system_scenarios.sh`
holds no cluster lifecycle of its own.

The same `query-lifecycle/distributed-baseline` scenario is reused by the
FoundationDB and MySQL StateStore provider gates as a feature-binary
coexistence smoke. Read it narrowly: it proves a feature-enabled binary still
completes a standard native topology, query and cleanup, not that any query
used that provider.
