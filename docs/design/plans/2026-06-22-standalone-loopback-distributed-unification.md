# Standalone Loopback Distributed Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route ordinary standalone all-in-one SQL through the same loopback gRPC distributed execution path used by the NovaRocks standalone FE/BE cluster.

**Architecture:** `all-in-one` starts an embedded full-execution NovaRocksGrpc BE on loopback, registers it as the only live backend, then uses `ExecutionCoordinator + FragmentScheduler + RemoteDispatcher` for ordinary SQL. Root results return through `RESULT_SINK + typed result_buffer + FetchResult`, matching `standalone-server --role fe/be`.

**Tech Stack:** Rust, tonic gRPC, Thrift `TExecPlanFragmentParams`, Arrow `Chunk` / `RecordBatch`, NovaRocks standalone SQL engine, `cargo test`, `tests/cluster_mvp.rs`.

**Spec:** [docs/design/specs/2026-06-22-standalone-loopback-distributed-unification-design.md](../specs/2026-06-22-standalone-loopback-distributed-unification-design.md)

---

## Implementation Rules

1. Ordinary SELECT / write pipeline execution must not fallback to local `execute_plan` when loopback gRPC fails.
2. `ClusterRole::AllInOne` and `ClusterRole::Fe` should differ only by backend registry source: embedded loopback backend vs externally managed live backend registry.
3. Runtime-local handles (`terminal_sink`, `iceberg_catalogs`) remain explicit direct-execution exceptions until they can be serialized or replaced.
4. `EXPLAIN ANALYZE` must not silently return empty remote profiles. Either collect profiles through the remote report path or fail fast with a clear message.
5. Debug fault injection is a test contract. Use debug builds for tests that rely on `[debug] fault_inject_submit_fail_after`.

## Files

Modify:

- `src/engine/backend_ops.rs`
- `src/engine/mod.rs`
- `src/runtime/dispatcher.rs`
- `src/runtime/coordinator.rs`
- `tests/cluster_mvp.rs`

Possibly modify if implementation reveals stale comments only:

- `src/server/mod.rs`
- `src/runtime/scheduler.rs`

Do not change StarRocks FE-compatible thrift/brpc entrypoints.

## Phase 1: all-in-one gets a real live backend registry entry

**Purpose:** remove the implicit `SHOW BACKENDS` backend object and make all-in-one scheduling read the same registry surface as role=fe.

### Task 1.1: Add backend registry test coverage

Files:

- Modify: `src/engine/backend_ops.rs`

- [ ] Add tests under the existing `#[cfg(test)]` module, or create one at file bottom if none exists.

Use `replace_backend_registry_for_test(None)` at test start/end so tests do not leak global registry state.

```rust
#[test]
fn all_in_one_loopback_registry_installs_live_backend_zero() {
    use crate::runtime::backend_registry;

    backend_registry::replace_backend_registry_for_test(None);
    let endpoint: std::net::SocketAddr = "127.0.0.1:19070".parse().unwrap();

    let registry = install_all_in_one_backend_registry(endpoint, 3)
        .expect("install all-in-one loopback backend");
    let live = registry.live_endpoints();

    assert_eq!(live, vec![(0, endpoint)]);
    assert_eq!(
        live_backend_dispatch_entries().expect("dispatch entries"),
        vec![(0usize, endpoint)]
    );

    backend_registry::replace_backend_registry_for_test(None);
}
```

Expected first run:

```text
error[E0425]: cannot find function `install_all_in_one_backend_registry`
```

### Task 1.2: Implement the all-in-one registry installer

Files:

- Modify: `src/engine/backend_ops.rs`

- [ ] Add a helper that installs the loopback backend as `Live` with `be_id = 0`.

```rust
pub(crate) fn install_all_in_one_backend_registry(
    endpoint: SocketAddr,
    heartbeat_timeout_retries: u32,
) -> Result<Arc<BackendRegistry>, String> {
    if let Some(registry) = crate::runtime::backend_registry::backend_registry() {
        return Ok(registry);
    }

    let registry = Arc::new(BackendRegistry::new(heartbeat_timeout_retries));
    let be_id = registry.add_backend_with_state(endpoint, BackendState::Live);
    if be_id != 0 {
        return Err(format!(
            "all-in-one loopback backend must be backend 0, got {be_id}"
        ));
    }

    crate::runtime::backend_registry::install_backend_registry(Arc::clone(&registry));
    Ok(crate::runtime::backend_registry::backend_registry().unwrap_or(registry))
}
```

- [ ] Keep `ensure_backend_registry` unchanged for `role=fe`; do not start heartbeat manager for the embedded all-in-one backend.
- [ ] Update `execute_show_backends` so `ClusterRole::AllInOne` uses the installed registry snapshot instead of `implicit_all_in_one_backend`.

Target shape:

```rust
pub(crate) fn execute_show_backends(
    state: &Arc<StandaloneState>,
) -> Result<StatementResult, String> {
    match current_role()? {
        ClusterRole::Fe => {
            let registry = ensure_backend_registry(state)?;
            Ok(StatementResult::Query(show_backends_result(registry.snapshot())?))
        }
        ClusterRole::AllInOne => {
            let registry = crate::runtime::backend_registry::backend_registry()
                .ok_or_else(|| "all-in-one backend registry is not initialized".to_string())?;
            Ok(StatementResult::Query(show_backends_result(registry.snapshot())?))
        }
        ClusterRole::Be => Err("SHOW BACKENDS is not available in role=be".to_string()),
    }
}
```

- [ ] Delete `implicit_all_in_one_backend` once no call sites remain.

Verification:

```bash
cargo test --lib --package novarocks -- engine::backend_ops::tests::all_in_one_loopback_registry_installs_live_backend_zero
```

Expected:

```text
test engine::backend_ops::tests::all_in_one_loopback_registry_installs_live_backend_zero ... ok
```

### Task 1.3: Install the registry during all-in-one engine open

Files:

- Modify: `src/engine/mod.rs`

- [ ] Replace the role comment in `open_body` so it no longer says all-in-one uses `InProcessDispatcher`.
- [ ] After `ensure_standalone_exchange_server()` returns the actual bound port for `ClusterRole::AllInOne`, install the backend registry.

Target shape:

```rust
let role = crate::novarocks_config::config()
    .map(|c| c.cluster.role)
    .unwrap_or(crate::common::app_config::ClusterRole::AllInOne);
let exchange_port = if role == crate::common::app_config::ClusterRole::Fe {
    u16::MAX
} else {
    ensure_standalone_exchange_server()?
};
```

Change to:

```rust
let cfg = crate::novarocks_config::config()
    .map_err(|e| format!("read config failed: {e}"))?;
let role = cfg.cluster.role;
let exchange_port = match role {
    crate::common::app_config::ClusterRole::Fe => u16::MAX,
    crate::common::app_config::ClusterRole::Be
    | crate::common::app_config::ClusterRole::AllInOne => ensure_standalone_exchange_server()?,
};
if role == crate::common::app_config::ClusterRole::AllInOne {
    let endpoint: std::net::SocketAddr = format!("127.0.0.1:{exchange_port}")
        .parse()
        .map_err(|e| format!("parse all-in-one loopback backend endpoint failed: {e}"))?;
    backend_ops::install_all_in_one_backend_registry(
        endpoint,
        cfg.cluster.heartbeat_timeout_retries,
    )?;
}
```

- [ ] Keep `role=fe` calling `backend_ops::ensure_backend_registry(&inner)?` after `StandaloneState` is created.
- [ ] Do not modify `role=be` process dispatch here; `role=be` does not open MySQL query sessions.
- [ ] Preserve the current idempotent startup contract: `open_body` may start the full-execution gRPC server, and `serve_forever` may call `start_grpc_exchange_server` again. The registry must use the actual bound port returned by `ensure_standalone_exchange_server()`, not the configured port.

Verification:

```bash
cargo test --lib --package novarocks -- engine::backend_ops::tests::all_in_one_loopback_registry_installs_live_backend_zero
```

## Phase 2: all-in-one dispatcher and scheduler use the live registry

**Purpose:** make `ClusterRole::AllInOne` use the same remote dispatcher and scheduler backend list as `ClusterRole::Fe`.

### Task 2.1: Add unit coverage for dispatcher selection

Files:

- Modify: `src/engine/mod.rs`

- [ ] Add a test that installs a loopback registry and proves all-in-one dispatcher is a remote dispatcher by using remote-only fault injection behavior or by exposing a test-only discriminator.

Preferred test-only discriminator:

```rust
#[cfg(test)]
pub(crate) fn dispatcher_kind_for_test(
    dispatcher: &Arc<dyn crate::runtime::dispatcher::FragmentDispatcher>,
) -> &'static str {
    if dispatcher.as_any().is::<crate::runtime::dispatcher::RemoteDispatcher>() {
        "remote"
    } else if dispatcher
        .as_any()
        .is::<crate::runtime::dispatcher::InProcessDispatcher>()
    {
        "in-process"
    } else {
        "unknown"
    }
}
```

This requires adding a test-only `as_any` hook to `FragmentDispatcher` in Task 2.2.

Expected first run:

```text
error[E0599]: no method named `as_any` found for reference `&Arc<dyn FragmentDispatcher>`
```

### Task 2.2: Add a test-only dispatcher downcast hook

Files:

- Modify: `src/runtime/dispatcher.rs`

- [ ] Extend `FragmentDispatcher` with test-only `as_any`.

```rust
pub trait FragmentDispatcher: Send + Sync {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;

    fn submit_fragment(
        &self,
        backend_idx: usize,
        params: internal_service::TExecPlanFragmentParams,
    ) -> Result<(), String>;
    // existing methods stay unchanged
}
```

- [ ] Implement it for `InProcessDispatcher`, `RemoteDispatcher`, and all test dispatchers in this file.

```rust
#[cfg(test)]
fn as_any(&self) -> &dyn std::any::Any {
    self
}
```

Verification:

```bash
cargo test --lib --package novarocks -- runtime::dispatcher
```

### Task 2.3: Change all-in-one to `RemoteDispatcher`

Files:

- Modify: `src/engine/mod.rs`

- [ ] Update `coordinated_execution_services` so both `Fe` and `AllInOne` read scheduler endpoints from the live backend registry.

```rust
let backends: Vec<SocketAddr> = match role {
    ClusterRole::Fe | ClusterRole::AllInOne => {
        backend_ops::live_backend_scheduler_endpoints()?
    }
    ClusterRole::Be => {
        return Err("role=be must not enter standalone coordinator".into());
    }
};
```

- [ ] Update `dispatcher_for_role` so both `Fe` and `AllInOne` use `RemoteDispatcher::new_with_backend_ids`.

```rust
match role {
    ClusterRole::Fe | ClusterRole::AllInOne => {
        let entries = backend_ops::live_backend_dispatch_entries()?;
        Ok(Arc::new(
            crate::runtime::dispatcher::RemoteDispatcher::new_with_backend_ids(&entries)?,
        ))
    }
    ClusterRole::Be => Err("role=be must not enter standalone coordinator".to_string()),
}
```

- [ ] Delete the local `exchange_port` scheduler special case. If the function parameter becomes unused, remove it from `coordinated_execution_services` and update call sites.
- [ ] Update comments in `src/runtime/coordinator.rs` and `src/runtime/dispatcher.rs` that describe all-in-one as `InProcessDispatcher`.

Verification:

```bash
cargo test --lib --package novarocks -- engine::tests::all_in_one_dispatcher_uses_remote_registry
```

Expected:

```text
test engine::tests::all_in_one_dispatcher_uses_remote_registry ... ok
```

## Phase 3: remove ordinary single-fragment direct execution

**Purpose:** ordinary SQL should always enter `ExecutionCoordinator`, even when the distributed plan has one fragment.

### Task 3.1: Add a failing integration test proving `SELECT 1` uses gRPC submit

Files:

- Modify: `tests/cluster_mvp.rs`

- [ ] Add a helper for an all-in-one config.

```rust
fn start_all_in_one(extra: &str) -> (ProcessGuard, u16) {
    let mysql = ReservedPort::new();
    let http = ReservedPort::new();
    let grpc = ReservedPort::new();
    let mysql_port = mysql.port();
    let http_port = http.port();
    let grpc_port = grpc.port();
    let config = write_config(
        "all-in-one",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {http_port}
grpc_port = {grpc_port}

[standalone_server]
mysql_port = {mysql_port}

[cluster]
role = "all-in-one"

{extra}
"#
        ),
    );
    let _ = mysql.release();
    let _ = http.release();
    let _ = grpc.release();
    let mut process = ProcessGuard::spawn(config.path());
    process.wait_for_ready("NOVAROCKS_READY mysql_port=");
    (process, mysql_port)
}
```

- [ ] Add a fault-injection test. `fault_inject_submit_fail_after = 0` means the first `RemoteDispatcher::submit_fragment` call fails. Before this phase is implemented, `SELECT 1` will incorrectly succeed through direct execution.

```rust
#[test]
fn all_in_one_select_uses_loopback_submit() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let (_srv, mysql_port) = start_all_in_one(
        r#"
[debug]
fault_inject_submit_fail_after = 0
"#,
    );

    let mut conn = connect_mysql(mysql_port);
    let err = conn
        .query::<i64, _>("SELECT 1")
        .expect_err("SELECT 1 should hit RemoteDispatcher submit fault");
    let err = err.to_string();
    assert!(
        err.contains("debug submit fault injected"),
        "expected loopback submit fault, got: {err}"
    );
}
```

Expected first run:

```text
thread 'all_in_one_select_uses_loopback_submit' panicked at 'SELECT 1 should hit RemoteDispatcher submit fault'
```

### Task 3.2: Split direct execution exceptions from ordinary query execution

Files:

- Modify: `src/engine/mod.rs`

- [ ] Replace the implicit `force_single_fragment` boolean with an explicit reason enum.

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectExecutionReason {
    RuntimeLocalTerminalSink,
    RuntimeLocalIcebergRegistry,
    UnitTestNoExchangeBackend,
}

fn direct_execution_reason(
    has_terminal_sink: bool,
    has_iceberg_catalogs: bool,
    exchange_port: u16,
) -> Option<DirectExecutionReason> {
    if has_terminal_sink {
        return Some(DirectExecutionReason::RuntimeLocalTerminalSink);
    }
    if has_iceberg_catalogs {
        return Some(DirectExecutionReason::RuntimeLocalIcebergRegistry);
    }
    if exchange_port == 0 {
        return Some(DirectExecutionReason::UnitTestNoExchangeBackend);
    }
    None
}
```

- [ ] Add a direct helper with an explicit name.

```rust
fn execute_query_direct_for_runtime_local_handle(
    mut physical: crate::sql::optimizer::PhysicalPlanNode,
    codegen_catalog: &dyn crate::sql::catalog::CatalogProvider,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    query_opts: Option<QueryOptions>,
    terminal_sink: Option<Arc<crate::engine::mv_flow::TerminalSink>>,
    iceberg_catalogs: Option<Arc<crate::connector::iceberg::IcebergCatalogRegistry>>,
    mv_refresh_ctx: Option<&crate::sql::codegen::fragment_builder::MvRefreshCodegenContext>,
    reason: DirectExecutionReason,
) -> Result<QueryResult, String> {
    physical = collapse_distribution_enforcers_for_single_fragment(physical);
    let build_result = if let Some(mv_refresh_ctx) = mv_refresh_ctx {
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan_with_mv_refresh_ctx(
            &physical,
            codegen_catalog,
            connectors,
            current_database,
            Some(mv_refresh_ctx),
        )?
    } else {
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan(
            &physical,
            codegen_catalog,
            connectors,
            current_database,
        )?
    };
    let plan = single_fragment_plan(build_result).map_err(|_| {
        format!(
            "direct execution exception {reason:?} produced a multi-fragment plan"
        )
    })?;
    execute_plan(*plan, query_opts, terminal_sink, iceberg_catalogs, None)
}
```

Keep the helper in `src/engine/mod.rs` near `single_fragment_plan`; delete `choose_standalone_execution` after the ordinary path stops using it.

- [ ] In `execute_query_with_options_and_imv_validator_with_catalog_provider`, call the direct helper only when `direct_execution_reason(...)` returns `Some(reason)`.
- [ ] For all other queries, build the distributed plan and immediately call `ExecutionCoordinator`.

Target shape for the normal path:

```rust
let build_result = if let Some(mv_refresh_ctx) = mv_refresh_ctx {
    crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan_with_mv_refresh_ctx(
        &physical,
        codegen_catalog,
        connectors,
        current_database,
        Some(mv_refresh_ctx),
    )?
} else {
    crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_via_distributed_plan(
        &physical,
        codegen_catalog,
        connectors,
        current_database,
    )?
};

let (dispatcher, scheduler) = coordinated_execution_services()?;
crate::runtime::coordinator::ExecutionCoordinator::new(
    build_result,
    dispatcher,
    scheduler,
    query_opts,
)
.execute()
```

- [ ] Remove `StandaloneExecutionPlan` and `choose_standalone_execution` from the ordinary query path.

Verification:

```bash
cargo test --test cluster_mvp -- all_in_one_select_uses_loopback_submit --nocapture
```

Expected:

```text
test all_in_one_select_uses_loopback_submit ... ok
```

### Task 3.3: Add a success smoke for loopback all-in-one

Files:

- Modify: `tests/cluster_mvp.rs`

- [ ] Add a positive smoke without fault injection.

```rust
#[test]
fn all_in_one_loopback_select_succeeds() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let (_srv, mysql_port) = start_all_in_one("");
    let mut conn = connect_mysql(mysql_port);
    let rows: Vec<i64> = conn.query("SELECT 1").expect("SELECT 1");
    assert_eq!(rows, vec![1]);
}
```

Verification:

```bash
cargo test --test cluster_mvp -- all_in_one_loopback_select --nocapture
```

Expected:

```text
test all_in_one_loopback_select_succeeds ... ok
test all_in_one_select_uses_loopback_submit ... ok
```

## Phase 4: retire product use of `InProcessDispatcher`

**Purpose:** keep in-process dispatcher only as a temporary unit-test utility, or delete it once tests are converted.

### Task 4.1: Ensure no product call site constructs `InProcessDispatcher`

Files:

- Modify: `src/runtime/dispatcher.rs`
- Modify: tests in `src/runtime/dispatcher.rs` if needed

- [ ] Run:

```bash
rg -n "InProcessDispatcher::new|InProcessDispatcher::default|InProcessDispatcher" src tests --glob '!src/runtime/dispatcher.rs'
```

Expected after Phase 3:

```text
no non-comment matches outside src/runtime/dispatcher.rs
```

- [ ] Update stale comments in `src/runtime/coordinator.rs` and `src/runtime/dispatcher.rs`.
- [ ] If all remaining `InProcessDispatcher` references are tests inside `src/runtime/dispatcher.rs`, gate the type and helper state with `#[cfg(test)]`.

Minimal acceptable transitional shape:

```rust
#[cfg(test)]
pub struct InProcessDispatcher {
    state: Arc<InProcessState>,
}

#[cfg(test)]
impl FragmentDispatcher for InProcessDispatcher {
    // existing test-only implementation
}
```

- [ ] If non-test code still depends on `take_profiles` from `InProcessDispatcher`, finish Task 5 before adding `#[cfg(test)]`.

Verification:

```bash
cargo test --lib --package novarocks -- runtime::dispatcher
rg -n "InProcessDispatcher" src --glob '!src/runtime/dispatcher.rs'
```

Expected second command:

```text
no matches outside src/runtime/dispatcher.rs
```

Delete or rewrite the comment if it still says all-in-one uses in-process execution.

## Phase 5: handle EXPLAIN ANALYZE remote profile semantics

**Purpose:** avoid all-in-one having a hidden profile-only local execution path.

### Task 5.1: Make EXPLAIN ANALYZE treat all-in-one like remote coordinated execution

Files:

- Modify: `src/engine/mod.rs`

- [ ] Remove `choose_standalone_execution` from the EXPLAIN ANALYZE path.
- [ ] Build the distributed plan and always use `ExecutionCoordinator`.
- [ ] Keep the existing fail-fast check if the dispatcher cannot collect remote profiles.

Target shape:

```rust
let build_result = lower_distributed_plan(&dp, codegen_catalog, connectors, None)?;
let mut profiled_query_opts = query_opts.unwrap_or_default();
profiled_query_opts.enable_profile = Some(true);
let (dispatcher, scheduler) = coordinated_execution_services()?;
if !dispatcher.supports_profile_collection() {
    return Err(
        "EXPLAIN ANALYZE requires remote fragment profile collection; \
         RemoteDispatcher profiles are not available yet"
            .to_string(),
    );
}
let outcome = crate::runtime::coordinator::ExecutionCoordinator::new(
    build_result,
    dispatcher,
    scheduler,
    Some(profiled_query_opts),
)
.execute_with_write_outcome()?;
```

- [ ] If the project already has a remote profile report API by implementation time, replace the fail-fast branch with that API and add an assertion that profiles are non-empty.

Verification:

```bash
cargo test --lib --package novarocks -- engine::tests::explain_analyze
```

Expected until remote profile collection exists:

```text
EXPLAIN ANALYZE requires remote fragment profile collection
```

Any golden test that previously expected all-in-one EXPLAIN ANALYZE output must be updated to the new fail-fast behavior or moved behind a profile-capable dispatcher test.

## Phase 6: preserve FE/BE cluster behavior

**Purpose:** prove loopback unification did not break the existing standalone cluster distributed path.

### Task 6.1: Run existing cross-process smoke tests

Files:

- No code changes unless tests reveal regressions.

- [ ] Run the focused cluster tests:

```bash
cargo test --test cluster_mvp -- cross_process_remote_dispatcher_smoke --nocapture
cargo test --test cluster_mvp -- dynamic_add_drop_backend_persists_and_updates_metrics --nocapture
cargo test --test cluster_mvp -- submit_half_failure_cancels_submitted --nocapture
```

Expected:

```text
test cross_process_remote_dispatcher_smoke ... ok
test dynamic_add_drop_backend_persists_and_updates_metrics ... ok
test submit_half_failure_cancels_submitted ... ok
```

- [ ] If `submit_half_failure_cancels_submitted` starts failing because `REMOTE_SUBMIT_CALLS` is process-global and new tests run first, make the test fault threshold independent by isolating it in a fresh process or by adding a test-only reset function for remote dispatcher counters.

## Phase 7: final cleanup and verification

### Task 7.1: Remove dead single-fragment routing code

Files:

- Modify: `src/engine/mod.rs`

- [ ] Remove these if no longer referenced:

- `enum StandaloneExecutionPlan`
- `fn choose_standalone_execution`

- [ ] Keep `single_fragment_plan` only if the direct-exception helper still uses it. If retained, move it next to the helper and document that it is not an ordinary query fast path.

Search:

```bash
rg -n "StandaloneExecutionPlan|choose_standalone_execution|single_fragment_plan" src/engine/mod.rs
```

Expected:

```text
one match for the `single_fragment_plan` function definition, and no matches for `StandaloneExecutionPlan` or `choose_standalone_execution`
```

Only `single_fragment_plan` may remain, and only for explicit direct exceptions.

### Task 7.2: Full focused verification

Run:

```bash
cargo fmt --check
cargo test --lib --package novarocks -- engine::backend_ops
cargo test --lib --package novarocks -- runtime::dispatcher
cargo test --test cluster_mvp -- all_in_one_loopback_select --nocapture
cargo test --test cluster_mvp -- cross_process_remote_dispatcher_smoke --nocapture
cargo test --test cluster_mvp -- submit_half_failure_cancels_submitted --nocapture
```

Expected:

```text
cargo fmt --check exits 0
all focused cargo test commands exit 0
```

### Task 7.3: Static guardrails

Run:

```bash
rg -n "ClusterRole::AllInOne => Ok\\(Arc::new\\(\\s*crate::runtime::dispatcher::InProcessDispatcher" src
rg -n "force_single_fragment|choose_standalone_execution|StandaloneExecutionPlan" src/engine/mod.rs
rg -n "implicit_all_in_one_backend" src
```

Expected:

```text
no matches for all three commands, except `force_single_fragment` may appear only in historical docs
```

If the second command still finds `force_single_fragment` in live Rust code, rename or remove it so direct execution cannot be mistaken for ordinary query planning.

## Acceptance Criteria

- `standalone-server --role all-in-one` starts a full-execution gRPC endpoint before MySQL readiness and registers it as live backend 0.
- `SHOW BACKENDS` in all-in-one reads the real backend registry.
- Ordinary all-in-one `SELECT 1` fails under `[debug] fault_inject_submit_fail_after = 0`, proving it uses `RemoteDispatcher::submit_fragment`.
- Ordinary all-in-one `SELECT 1` succeeds without fault injection through loopback gRPC.
- `ClusterRole::AllInOne` and `ClusterRole::Fe` both use `RemoteDispatcher` and live backend scheduler endpoints.
- Product code no longer constructs `InProcessDispatcher`.
- Direct execution remains only for explicitly named runtime-local exceptions.
- Existing `role=fe/be` cluster smoke tests still pass.
