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

Most scenarios build their Iceberg warehouse on the local filesystem under
the harness runtime directory. The vended credential scenarios start an
isolated Iceberg REST and MinIO fixture and require the fixture image and
Docker service. The base config supplies SQLite StateStore and no shared
`[connector.object_store]` credentials. The generated
`$NOVAROCKS_FE_CONFIG` / `$NOVAROCKS_BE_CONFIG` pair also works
when started through the exact `--role all-in-one --fe-config ... --be-config
...` command; it adds object-store settings that local-filesystem scenarios
do not read.

The consumer credential acceptance cases are
`connector/vended-credential-refresh`,
`connector/vended-credential-targeted-deadline`,
`connector/vended-credential-late-close`, and
`connector/vended-credential-unreachable`. Run each with an exact `--only`
filter and `--cluster-size 3 --timeout-secs 180`. The fixture identifies the
BE principal and holds its refresh only after that request arrives. The
late-close case enables a debug-only, exact-authority trigger on BE[0].

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
