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

# NovaRocks - AI Agents Guide

This document is a quick operational index for agents working on NovaRocks.
It is designed to help you quickly:
- locate the right code paths
- understand the current execution architecture
- implement changes without semantic drift

This guide focuses on high-frequency implementation details and modification entry points.

---

## 1. Project Overview

NovaRocks is a **Rust-based, cloud-native, compute-storage decoupling friendly**
analytical query engine.

Its production runtime is the native NovaRocks FE/BE role model:

- **Native FE/BE roles**
  - `role=fe` owns the MySQL SQL entrypoint, statement admission, planning, and
    distributed coordination.
  - `role=be` owns local fragment execution and the native gRPC boundary.
  - `all-in-one` is a server-owned local supervisor that reads one normal FE
    config and one normal BE config. It preserves the FE/BE application
    boundary and is not an application role or separate production topology.

- **Sealed external Connector providers**
  - The active Server provider manifest contains exactly Iceberg and Paimon.
  - Paimon is read-only for append-only and `deduplicate` primary-key snapshot
    reads. Iceberg retains its existing read/write capabilities.
  - StarRocks is retired: its source remains for reference, but it has no active
    read capability and `[connector.starrocks]` is rejected at configuration
    parsing.

Columnar processing is centered on Arrow `RecordBatch` / `Chunk`.

---

## 2. Architecture Overview (Current Code)

### 2.1 Native FE/BE Roles

```text
SQL client
  `- MySQL protocol -----------> NovaRocks role=fe
                                      |
                                      v
                            Frontend SQL/coordinator application
                                      |
                                      v
                            Native gRPC fragment submission
                                      |
                                      v
                            NovaRocks role=be
                                      |
                            Pipeline / Runtime / Connectors
```

---

## 3. Non-Negotiable Rules (High Priority)

1. **Strictly follow native frozen contracts and type metadata**
   No fallback behavior, no guessed defaults, no implicit type downgrade.

2. **Fail fast on unsupported or ambiguous semantics**
   Return explicit errors in parsing/lowering stages instead of "best effort" execution.

3. **Keep protocol and execution responsibilities separated**
   Native gRPC is the FE/BE process boundary; execution semantics belong to
   their Rust application owners.

4. **Keep native role and Connector responsibilities explicit**
   FE owns SQL admission and global coordination; BE owns local execution.
   Connectors own external-system facts. Do not mix those owners through
   process-global state, direct calls, or transport fallback.

5. **Distributed deployment is the source of truth; all-in-one is test-only**
   The real, user-facing deployment is NovaRocks distributed (1 NovaRocks FE +
   N BE; CI baseline 1FE+3BE). The single-process all-in-one form is only a
   testing convenience. It starts normal FE and BE role runners with a pair of
   deployable configs. Do NOT model for, or add special-case branches for, the
   single-process form. Cluster-topology quantities (broadcast fanout,
   backend count, per-node budgets) must be read dynamically from the live BE
   registry, never hardcoded or defaulted to "single-process = 1 node". Tests
   must never pass only in standalone while failing under 1FE+3BE.

6. **External orchestration owns backend desired membership**
   In `role=fe`, `ClusterBackendService` is an in-memory, rebuildable runtime
   projection. A BE mints one UUIDv7 process identity per start and can enter
   new-query scheduling only after its authenticated announce and the FE-pull
   exact heartbeat agree on the immutable descriptor. StateStore is never a
   backend-membership source; `[cluster].backends` and SQL `ADD BACKEND` /
   `DROP BACKEND` are removed. Heartbeat/announce loss affects future admission,
   not an existing attempt; only lifecycle/control/transport evidence or exact
   process replacement decides that attempt's fate. Core consumes the read-only
   `BackendTopologyPort` only: do not add a metadata bridge, global registry,
   in-memory fallback, or direct-call production path. `role=be` never creates
   frontend membership or StateStore services.

7. **Language policy**
   - User interaction and design docs: Chinese
   - Code comments, logs, error messages, commit messages: English

---

## 4. Key Code Index (Validated Against Current Repository)

### 4.1 Entrypoints and Services

- `novarocks-server/src/main.rs`
  Native top-level command dispatch. The only server command is `standalone`;
  it chooses `fe`, `be`, or `all-in-one` before startup side effects.
  Server application composition and role runners live in
  `novarocks-server/library/src/**` (`novarocks-server-application`); the root
  `novarocks-server` package owns the thin CLI; the application library owns
  process allocator installation.

- `novarocks/frontend-application/src/**`
  FE application composition, SQL admission, catalog projection, native query
  execution adapter and distributed task coordination. Query-local semantic
  policy remains in `novarocks/query-application/src/**`; long-lived Catalog
  generation ownership remains in `novarocks/catalog-application/src/**`.
  `state_family/**` is the closed manifest of every frontend-local state
  family: it owns each family's classification (`ExternalProjection` /
  `ProcessRuntime` / `Accelerator`) and is the single definition point for
  persistent key prefixes and record versions, so a runtime family
  structurally cannot own durable state (ADR-0114).

- `novarocks/worker/src/**` and `novarocks/native-adapter/src/**`
  Worker owns BE-local Task/context admission, lifecycle, leases, convergence
  and connector execution policy. Native Adapter owns generated stubs/codec,
  listener composition, data-plane handlers and role-local wire projection.
  Keep generated DTO decode at the adapter boundary; do not move it into
  Worker or recreate a Backend facade.

- `novarocks/execution/src/**` and `novarocks/execution-contract/src/**`
  Carrier-neutral execution, query lifecycle contracts, and native runtime
  kernels shared by FE and BE application owners. `execution-contract` owns the
  identities and contracts both roles must agree on; `execution` owns the
  runtime that runs under them.

### 4.2 SQL Frontend: Parse, Plan, Admit

- `novarocks/parser/src/**`
  Hand-written lexer, parser and AST, including the StarRocks-compatible
  surface for catalogs, tables, materialized views, Iceberg refs and drops.

- `novarocks/sql/src/{analysis,analyzer,semantic}/**`
  Name and expression resolution, type rules and semantic validation.

- `novarocks/sql/src/optimizer/**`
  Logical/physical optimization: `rewrite/rules/**` and `cascades_rules/**` for
  rules, `cost.rs` and `estimate/**` for cost and statistics, `options.rs` for
  session-level rule disabling.

- `novarocks/sql/src/planner/**`
  Builds logical plans, materializes optimizer output into planner physical IR,
  and plans distributed fragment topology (`logical/`, `physical/`,
  `distributed/`, `pipeline/`, `runtime_filter/`).

- `novarocks/query-application/src/sql.rs`, `novarocks/query-application/src/sql/**`
  Query-application ownership of parser-admitted statement shape: statement
  admission and DDL/DML routing.

- `novarocks/frontend-application/src/query_execution/dml/**`
  DML flows — `insert.rs`, `delete/**`, `mutation.rs` / `mutation_flow.rs`,
  `truncate.rs`, `add_files.rs`, `ctas.rs` / `iceberg_ctas.rs`,
  `iceberg_writer.rs` (INSERT INTO / INSERT OVERWRITE).

- `novarocks/frontend-application/src/catalog_application/statement.rs`
  Catalog, database and table DDL against the projected catalog.

- `novarocks/mysql-adapter/src/**`
  MySQL protocol: `listener.rs` (connection serving and drain),
  `result_encoding.rs` (metadata projection), `relay_result_writer.rs` and
  `local_result_writer.rs` (governed bounded result delivery),
  `error_mapping/**`.

### 4.3 Native Compiled Package and BE Preparation

- `novarocks-server/library/src/{static_plan.rs,compiled_composition.rs}`
  Server 唯一组合 compiled package 解释器；FE/BE 共用冻结的 pure 目录与 codec 上限。
  `command.rs` 的 `--build-info` 输出 pure 摘要、封存定义数及确定排序的排除清单。
  没有解释器选择 CLI、candidate feature 或 v1 fallback。

- `novarocks/frontend-application/src/query_execution/physical_encoding.rs`
  从完成的 planner physical IR 经 `encode_completed_packages` 冻结 package。
  SQL 的精确结果声明、类型与函数准入快照必须随原始输入保留，不能从展示信息猜测。

- `novarocks/plan-codec/src/physical_package_v2/**`
  package 编解码与验证；`frozen_limits.rs` 拥有生产编码/解码上限和常量策略。
  `physical_type_v2.rs` 与 `physical_expression_v2.rs` 拥有对应 frozen vocabulary。

- `idl/novarocks/{service.proto,execution_shared.proto,physical_package_v2.proto}`
  `FrozenFragment` 仅携带 opaque package；旧计划树字段永久 reserved。
  `execution_shared.proto` 保留 RF 与类型共享消息的 `novarocks.plan` namespace；
  Rust `plan` 模块仍存在不代表存在 v1 decoder。

- `novarocks/task-codec/src/{creation.rs,resource_preflight.rs}` 与
  `novarocks/native-adapter/src/{task_protocol_ingress.rs,static_package_admission.rs}`
  在 Task 接管之前约束载体资源，准确保留 replay/identity 与 Worker 的 charge 所有权。
  接管后的 package 语义解码和局部编译仍由 BE preparation 执行。

- `novarocks/native-adapter/src/backend_task_execution/{compiled_package.rs,execution_host.rs}`
  解码 package、验证 provider recipes，经 LocalCompiler 产生 `LocalProgram`；
  再绑定本 Task 的 scan、writer、exchange、sink 与 RF endpoints，准备 compiled pipeline。
  `compiled_scan_binding.rs`、`compiled_writer_binding.rs` 和 `compiled_runtime_filter.rs`
  分别承接这些角色本地投影；`native_type_decode.rs` 是 RF 等消费者共用的类型解码 owner。

生产静态计划只有 package → LocalCompiler → compiled pipeline 一条路径。generated DTO
投影留在 adapter，纯降级留在 LocalCompiler，Task 生命周期留在 Worker。
修改前阅读 [ADR-0184](docs/adr/ADR-0184-compiled-packages-are-the-sole-static-plan-path.md)。

F0–F2 只交付翻转、删旧与完整 R/D/U 失败交接，不代表能力补齐或 CI 全绿。只有准确具名的
能力拒绝可临时登记，错误结果、崩溃、非预期超时与未覆盖项不能白名单化。整个 UEA-5E 的 Z
仍须清零修复账、相关登记及 pure 排除清单，并完成原生 1FE+3BE 与全部约定 CI 的最终验证。

### 4.4 Local Program and Operators

- `novarocks/local-compiler/src/**`
  只消费已冻结的 package、pure function/provider facts，产生纯 `LocalProgram`；
  不获取 Task、connector runtime 或 topology。能力拒绝必须与非法输入及控制失败保持 typed 区分。

- `novarocks/local-program/src/**`
  纯 Task-independent 节点、compiled expression、layout、sink 和 binding requirement facts；
  不拥有 live scan、writer、connector 或 async capability。

- `novarocks/execution/src/exec/node/**`
  compiled 与旧 evaluator 仍复用的值描述、scan/writer 等运行能力；
  `ExecPlan`、`ExecNode` 构造树和 lowering bridge 已删除，不得按目录名删除共享值类型。

- `novarocks/execution/src/exec/expr/**`
  `compiled_program/**` 执行已绑定的 pure 核心。旧 `ExprArena`、名字注册表、函数外壳和
  legacy aggregate adapter 暂留给 F3：FE `query_execution/constant_eval.rs`、
  `query_execution/row_mutation.rs` 的 COW cast 以及 compiled 的
  `exec/expr/compiled_program/guarded.rs` 共享 IN helper 仍有生产依赖。
  旧 evaluator 的私有 differential oracle 测试也保留；它不是生产计划树解释器。

- `novarocks/execution/src/exec/operators/**`
  compiled 算子与 factory；`compiled_expression.rs` 直接运行 compiled expression，
  不经旧 arena 或名字分派。

- `novarocks/execution/src/exec/chunk/{mod.rs,chunk_impl.rs}`
  `Chunk`（Arrow `RecordBatch` 包装）及 slot metadata mapping。

### 4.5 Pipeline Execution Framework

- `novarocks/execution/src/exec/pipeline/builder/compiled.rs`
  从 `LocalProgram` 和本 Task 准确绑定的能力建立 pipeline；`builder.rs` 拥有共享图值。
  `binding.rs` 定义 `ExchangeBindings`、`ScanBindings`，writer bindings 在 `runtime/fragment`。

- `novarocks/execution/src/exec/pipeline/executor.rs`
  `prepare_compiled_program_pipeline_execution_with_profiler` 准备 compiled pipeline；
  program 与 Task 能力分别传入，不能重建已删除的 construction plan bridge。

- `novarocks/execution/src/exec/pipeline/driver.rs`
  Driver execution logic.

- `novarocks/execution/src/exec/pipeline/global_driver_executor.rs`
  Global driver scheduling executor.

- `novarocks/execution/src/exec/pipeline/dependency.rs`
  Operator dependency management.

- `novarocks/execution/src/exec/pipeline/schedule/**`
  Scheduling and observable event mechanisms.

- `novarocks/execution/src/exec/operators/scan/stream_source.rs`
  Driver-owned scan source: the scan pipeline's one driver polls the
  connector page stream directly, refills its per-turn poll budget, and
  observes the stream's close as pending finish. No scan thread pool or scan
  chunk queue exists; see ADR-0159.

### 4.6 Exchange and Runtime

- `novarocks/execution/src/runtime/exchange.rs`
  Exchange receiver registry, chunk encode/decode, sender completion tracking.

- `novarocks/execution/src/runtime/fragment/io/**`
  Fragment I/O edges: `exchange_edge.rs` (the Installed/normal-close gated open
  barrier), `exchange_queue.rs` (outbound queue and backpressure),
  `exchange_receiver.rs` (application-hosted ingress for one receiver),
  `result.rs`, `commit.rs`.

- `novarocks/execution/src/exec/operators/exchange_source.rs`
  Source operator for an exchange node: reconstructs chunks and tracks sender
  completion.

- `novarocks/native-adapter/src/{exchange_data_plane.rs,exchange_transmitter.rs,fragment_exchange_receiver.rs}`
  Native exchange wire projection, route settlement and transmission.

- `novarocks/worker/src/result_buffer.rs`
  BE-local result buffering, retained budget and fetch behavior.

- `novarocks/worker/src/query_context.rs`
  BE-local query context manager, cleanup leases and resource snapshots.

- `novarocks/execution/src/runtime/runtime_state.rs`
  Runtime state for cache, runtime filters, and execution context.

### 4.7 Memory attribution and funding

- `novarocks/memory/src/lane/**` owns stable 64 B records, pointer-free 8 B
  `RecordRef`/`FactToken`, signed allocation count + unique slot pin lifetime,
  exact reclamation. Identity alone grants no access right.
- `novarocks/memory/src/membership.rs` owns direct account members used by
  lifecycle/control traversal; membership is not stored in allocation tails.
- `novarocks/memory/src/attribution/**` owns the 512 B size band, 8 B tail,
  destructor-free one-slot TLS (Q=1 MiB), synchronous `LaneHandle::run`,
  per-poll `AttributedFuture` and R1 `ExplicitOwner` helpers. Observation grants
  no capacity; publish one fact per actual block, never both manually and through
  wrapper/helper. R1 zero-sized requests publish no physical fact.
- `novarocks-server/library/src/memory_observation.rs` installs
  `AttributingAllocator<Jemalloc>` (System without default features). Process
  request counts, responsibility, allocator/cgroup/RSS and metadata estimates
  remain separate. Q×sampled pins excludes in-flight work and is not a physical
  upper bound. S1 production query/R1 integration remains follow-up work.
- `stop_producing` does not reclassify executing Work. U=ΣC_query; genuine Work
  teardown transfers residual responsibility without lowering root C or N by
  reclassification. Timeout/terminal never substitutes for actual exit.
- Read ADR-0167 and `docs/guides/development/memory-boundary.md` before changing
  these contracts. `tools/ci/memory-model-checks.sh` runs local Loom/Miri only;
  Miri uses Tree Borrows because Stacked Borrows rejects any in-band allocator
  tail read through the narrowed pointers std Box/Arc pass to `dealloc`. The
  当前阶段仅维护功能与数据正确性，内存测量载体已退出；保留 Loom/Miri
  和 unsafe 地址/生命周期要求。smoke 不证明性能或所有生产调用面已接线。

### 4.8 Connectors / Catalog Backends / Filesystem

- `novarocks/types/src/{naming,schema}.rs`
  Neutral catalog naming and schema vocabulary. It contains no catalog runtime
  or provider authority.

- `novarocks/sql/src/catalog/memory.rs`
  SQL-owned local `PlannerMemoryCatalog`; it stores private planner facts and
  materializes only SQL catalog-visible values.

- `novarocks/frontend-application/src/catalog_application/query_catalog/**`
  Frontend-owned query catalog registry, schema cache, and service composition.
  Connector admission and local-catalog snapshots remain application-owned.

- `novarocks/connector/starrocks/**`
  Retired reference implementation. It is not registered by the Server
  provider manifest and provides no active product read capability.

- `novarocks/connector/iceberg/**`
  Iceberg control/execution contracts, catalog integrations, and storage facts.

  Iceberg 写出类型边界见 [ADR-0178](docs/adr/ADR-0178-iceberg-write-type-boundary-and-variant-layering.md)：
  `commit/write_stack/` 冻结完整目标 schema，`commit/frozen_write.rs` 投影 SDK writer schema，
  `commit/write_adaptation.rs` 适配执行输入；Variant/uuid 数据写出准入与逻辑 Variant 读取分别属于 Provider。
  CREATE / ALTER ADD 在所有格式版本递归拒绝新 Variant 列；已有表的 DELETE、合法普通键 equality delete、
  TRUNCATE 与普通 metadata 修改保留原边界。schema ALTER 先在同一代 lease 做 Provider 纯准入，再执行原 MV / DROP 保护。

  Iceberg 提交契约见 [ADR-0171](docs/adr/ADR-0171-commit-operation-model.md)。修改前先读：
  - `novarocks/connector/iceberg/src/commit/model/**`：不可变意图、逻辑条目与物理对象身份、继承字段、冻结请求及发布/清理结果。
  - `novarocks/connector/iceberg/src/commit/dependency/**`：按类别惰性读取 metadata、目标 ref 存活集和连续历史窗口，验证操作依赖。
  - `novarocks/connector/iceberg/src/commit/staging/**`：updates 前缀事实、ID 规范化、requirements 折算及 manifest-list 的实际 row-ID 分配。
  - `novarocks/connector/iceberg/src/commit/attempt.rs`：加载、验证、准备、冻结、单次派发和确定冲突的重试。
  - `novarocks/connector/iceberg/src/commit/operation.rs`：操作内 I/O 并发、先登记后写入、实际退出监督和有界 owned-object 清理。

- `novarocks/connector/paimon/**`
  Read-only Paimon Filesystem Catalog, private wire codec, snapshot planning,
  and append-only / `deduplicate` primary-key table reads.

- `novarocks/fs/**`
  Connector-neutral authorized object-store access. Its process-local
  `StorageAuthority` acquires and renews vended material for the consumer that
  signs the object request; attempt access still verifies exact scope. See
  ADR-0151 before changing credential ownership or renewal.

---

## 5. Core Execution Flows

### 5.1 Native SQL and Connector Path

1. A MySQL client connects to a `role=fe` process through the native SQL
   entrypoint.
2. The frontend owns session admission, catalog resolution, planning, and
   coordinator lifecycle assembly.
3. Persistent tables belong to external providers. Iceberg owns its catalog and
   mutation truth; Paimon owns its external snapshots and exposes only its
   supported read capability.
4. Frontend 将完成的物理计划冻结成 compiled packages，连同准确的 Connector facts
   经 native gRPC 提交到一个或多个 `role=be` 进程。
5. BE hosts bind installed Connector execution instances and run Arrow batches
   through the pipeline/runtime stack.

### 5.2 Exchange Path

1. Sender-side operators encode chunks into
   `novarocks/execution/src/runtime/fragment/io/exchange_queue.rs`, which owns the
   outbound queue and backpressure, and transmit through
   `novarocks/native-adapter/src/exchange_transmitter.rs`.
2. An exchange edge starts Closed and opens only after every frozen destination
   has an Installed fact or has normally withdrawn its exact input demand
   (`novarocks/execution/src/runtime/fragment/io/exchange_edge.rs`), so no frame
   can precede the receiver that counts it.
3. Receiver side (`novarocks/native-adapter/src/exchange_data_plane.rs`) projects
   the wire payload and pushes into
   `novarocks/execution/src/runtime/fragment/io/exchange_receiver.rs` and the
   registry in `novarocks/execution/src/runtime/exchange.rs`.
4. The exchange source operator
   (`novarocks/execution/src/exec/operators/exchange_source.rs`) blocks until all
   senders reach EOS.
5. On cancellation, `exchange::cancel_fragment` / `cancel_exchange_key` clear
   exchange keys and wake blocked waiters.

### 5.3 Native Distributed Task Execution Path

1. `novarocks/frontend-application/src/query_execution/native_execution_adapter.rs` freezes a
   `QueryExecutionId` and, from one live backend snapshot, a task graph:
   `novarocks/frontend-application/src/task_execution/graph.rs` mints a `TaskIdentity`
   ({QueryExecutionId, StageId, TaskId, BackendProcessId}) per placement and a
   `QueryContextRef` ({QueryExecutionId, FrontendProcessId, BackendProcessId})
   per participating backend. A context exists only where the scheduler placed
   a task, so the number of participants is a property of the plan and the
   splits, never of the cluster size.
2. `execute_round_on_task_protocol` establishes every context and creates every
   task through a bounded, fair dispatcher
   (`novarocks/frontend-application/src/task_execution/{round,dispatch,context_owner}.rs`).
   Each operation gets one verdict per domain -- Apply / Idempotent / Older /
   Conflict -- and a domain never rolls back, which is why replaying the exact
   request is the prescribed recovery for an unknown outcome and a conflicting
   answer is fatal. `Accepted` proves Worker ownership before expensive
   preparation; `Installed` separately proves that local runtime installation
   completed. FE retains a per-backend deployment window W across Accepted or
   unknown RPC outcomes until Installed, terminal evidence or permanent
   stand-down. W must not exceed that exact backend descriptor's advertised
   per-context preparation positions P.
   FE 只发送 compiled package。BE 的 `CompiledPackageInterpreter` 解码并局部编译，
   `NativeTaskExecutionHost` 绑定 Task 能力后准备 compiled pipeline；没有 v1 解码或树构造桥。
   BE 的 typed 能力拒绝沿 `TaskFailureCategory::Unsupported` 与
   `AttemptFailureClass::Unsupported` 传至 `[UnsupportedCompiledShape]`，不重试或重新规划。
   SQL 分析期缺 pure 实现保留 `[sql.analyze.unavailable_implementation]`。
3. Push exchange edges start Closed and open once every frozen destination has
   an Installed fact or has normally withdrawn its exact input demand
   (`novarocks/execution/src/runtime/fragment/io/exchange_edge.rs`), so no
   frame can precede the receiver that counts it.
4. `novarocks/worker/src/task_registry.rs` owns the BE-local
   context and task state, exact admission, lease renewal and expiry, bounded
   tombstones, and one termination latch per task. The latch is first-wins with
   a single exception: a derived cause (`Aborted(PeerTaskFailed)`) is a
   placeholder that an originating cause (`Failed(TaskFailure)`) replaces
   exactly once. Accepted jobs enter a bounded fair FIFO; count, byte and
   per-context position charges remain until the preparation job actually exits,
   including after Installed or terminal publication. See
   `novarocks/worker/src/{task_registry.rs,task_registry_config.rs}`.
   Native status observation uses covered subscriptions with a mandatory
   nonzero generation (`novarocks/native-adapter/src/task_protocol.rs`);
   cursor-only generation-zero requests are refused.
5. Runtime-filter contributions and an operator's own counters ride
   `ReleaseQueryContextAck`: release is the backend's own statement that every
   local task is a terminal record, and the Frontend Application drives it for every
   intent. The task protocol mints no participant proof or attestation -- a
   task's terminal *is* its own status.
6. Client cancellation is `KILL QUERY` through the Query Application query-control
   owner, delivered as `AbortQueryContext`. The Frontend Application withholds the
   interrupt until its coordinator worker unwinds, so the next statement on
   that connection cannot race the statement generation.

## 6. Core Data Structures (Current Implementation)

- `Chunk`: `novarocks/execution/src/exec/chunk/chunk_impl.rs`
  Arrow `RecordBatch` wrapper with `slot_id -> column_index` mapping and memory accounting.

- `LocalProgram`: `novarocks/local-program/src/**`
  纯 compiled program；Task 的 exchange、scan、writer 与 sink 能力在 BE preparation 另行绑定。

- `ExprArena` / `ExprNode`: `novarocks/execution/src/exec/expr/mod.rs`
  F3 待迁移的旧表达式求值器，供 FE 折叠等保留调用方与私有 oracle 使用；不构造生产 fragment 树。

- `RuntimeState`: `novarocks/execution/src/runtime/runtime_state.rs`
  Runtime context for cache and runtime filter behavior.

- `ExchangeKey`: `novarocks/execution/src/runtime/exchange.rs`
  Exchange routing key (`finst_id_hi` + `finst_id_lo` + `node_id`).

- `QueryResult`: `novarocks/query-application/src/api/result.rs`
  Query-application result type consumed by the MySQL adapter's result encoding.

- `QueryExecutionId`: `novarocks/types/src/identity.rs`
  Immutable native query-attempt identity, shared across the process boundary.

- `TaskIdentity` / `QueryContextRef`:
  `novarocks/execution-contract/src/identity.rs`
  The indivisible {QueryExecutionId, StageId, TaskId, BackendProcessId} a task
  is addressed by, and the {QueryExecutionId, FrontendProcessId,
  BackendProcessId} a backend holds on an attempt's behalf. Any component
  mismatching is fatal; neither is ever partially matched.

- `TaskExecutionRegistry`:
  `novarocks/worker/src/task_registry.rs`
  BE-owned context and task state, exact admission, per-domain progression,
  lease renewal and expiry, bounded tombstones, and one termination latch per
  task.

---

## 7. Configuration and Runtime

### 7.1 Config File

- Default config file: `./novarocks.toml`
- Environment override: `NOVAROCKS_CONFIG=/path/to/file.toml`
- CLI override: `--config <path>`

### 7.2 Common Config Sections

- `[server]`
  `host`, `priority_networks`, `http_port`, `grpc_port`, and native advertise
  identity.

- `[native_trust]`
  Required in each deployable FE/BE config: a common `deployment_id` and
  Server-resolved `shared_secret`. Every Native RPC requires its deployment
  JWT. An absent `[native_trust.transport]` deliberately means authenticated
  plaintext h2c; `automatic` and `pem` add optional TLS 1.3/h2 without
  removing JWT. This does not protect MySQL or management HTTP. Read
  `docs/guides/deployment/native-trust.md` and ADR-0110 before changing this
  boundary; NWT-4 protocol slimming remains separate follow-up work.

- `[runtime]`
  `exchange_wait_ms`, `exchange_io_threads`, `exchange_io_max_inflight_bytes`,
  `pipeline_exec_thread_pool_thread_num`, `scan_io_*`, `scan_range_*`, `prefetch_*`, `cache.*`

- `[iceberg]`
  Embedded-JVM toggle for Iceberg metadata-table and remote metadata planning.

- `[standalone_server]`
  `mysql_port`, `user`, MV scheduler settings, and Iceberg maintenance settings.

- `[connector.object_store]`
  Process-local object-store credentials for Iceberg and Paimon connector
  execution; this does not define a native internal table store.

- `[debug]`
  `exec_node_output`, `exec_batch_plan_json`

### 7.3 Local Test Environment (Iceberg REST + MinIO + Spark)

The canonical local test fixture lives at `docker/iceberg-rest/` and is also
the CI fixture for the `iceberg`, `iceberg-compatibility`, and `iceberg-rest`
SQL suites. The Codex workspace manifest at
`.codex/environments/environment.toml` points setup at this directory.

共享 Docker fixture 由版本化 runtime owner 管理：一个对象存储实例可以供多个 catalog 实例使用，相同输入附着同一实例，输入变化创建并存版本。控制目录默认是 `${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/fixture-runtime`，可由 `NOVA_FIXTURE_RUNTIME_DIR` 指定；资源身份同时包含控制目录与本机 Docker daemon。服务端口由 owner 预留，默认范围 `28000..28999`，不能假设 `9000/8181/4040` 或固定 Compose 项目。

Codex setup 的 `docker/iceberg-rest/up.sh --prepare-only` 不调用 Docker。它按入口已保存的 owner locator 和记录发布配置；`ready=true` 只表示保存的记录为 ready，不证明当前健康。首次、记录缺失或 deleting 时发布 unbound，清除端点和镜像变量；正常 `up.sh` 先验证本机 BOM，再恢复或附着实例。

`runtime/current` 定位本 worktree 的稳定入口，入口的 `published` 是唯一绑定提交点。消费者在启动时解析一次 publication，再 source 其中的 `env.sh`；所有配置路径指向同一不可变 publication。`NOVA_ENV_RUNTIME_DIR` 保存 SQLite 等稳定运行数据，不能拼接成 `NOVA_ENV_RUNTIME_DIR/env.sh`。推荐：

```bash
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
```

Do not guess the NovaRocks server port. Always discover the active worktree
environment from the fixed generated entry:

```bash
source docker/iceberg-rest/runtime/current/env.sh
```

Important generated locations:

- `docker/iceberg-rest/runtime/current/env.sh`
  Shell exports for the active worktree. Prefer this for commands.
- `docker/iceberg-rest/runtime/current/manifest.json`
  Machine-readable endpoints, ports, Docker Compose project, warehouses, and config paths.
- `docker/iceberg-rest/runtime/current/README.md`
  Human-readable summary of the active worktree environment.

Important environment variables after sourcing `env.sh`:

- `NOVA_ENV_SHARED_DOCKER`, `NOVA_ENV_COMPOSE_PROJECT`, `NOVA_ENV_CONFIG_FILE`
- `NOVA_ENV_OBJECT_STORE_RUNTIME`, `NOVA_ENV_CATALOG_RUNTIME`, `NOVA_ENV_OBJECT_STORE_CONTAINER`
- `NOVA_ENV_REST_ENV_FILE`（本次 publication 的不可变 `env.sh` 路径）
- `NOVA_ENV_MINIO_PORT`, `NOVA_ENV_REST_PORT`, `NOVA_ENV_MYSQL_PORT`
- `NOVA_ENV_REST_MV_PORT`, `NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI`
- `NOVA_ENV_SPARK_UI_PORT`
- `AWS_S3_ENDPOINT`, `AWS_S3_ACCESS_KEY_ID`, `AWS_S3_SECRET_ACCESS_KEY`
- `NOVAROCKS_ICEBERG_REST_URI`
- `NOVAROCKS_ICEBERG_REST_WAREHOUSE`
- `NOVAROCKS_ICEBERG_REST_MV_URI`, `NOVAROCKS_ICEBERG_REST_MV_WAREHOUSE`
- `NOVAROCKS_FE_CONFIG`, `NOVAROCKS_BE_CONFIG`
- `NOVAROCKS_SQL_TEST_CONFIG`
- `NOVAROCKS_ICE_REST_CATALOG_SQL`
- `NOVAROCKS_SPARK_DEFAULTS`
- `NOVAROCKS_SPARK_REST_MV_URI`
- `NOVAROCKS_SPARK_V3_SMOKE_SQL`
- `NOVAROCKS_SPARK_SQL`

If the fixed entry is missing, initialize or inspect the environment with:

```bash
docker/iceberg-rest/up.sh --prepare-only
docker/iceberg-rest/status.sh
```

Start standalone against the generated config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
```

When backgrounding the server (e.g. inside an automated test driver), wait
for the readiness marker before issuing the first query — probing the mysql
port alone cannot distinguish a freshly-bound server from a leftover
process that already owned the port:

```bash
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
# Wait up to 60 s for the server to bind. If bind fails the line never
# appears, the process exits with code 1, and `wait` surfaces the failure.
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timed out waiting for NOVAROCKS_READY" >&2; kill -9 "$SRV_PID"; exit 1; }
```

The marker line is emitted on stdout immediately after a successful bind:
`NOVAROCKS_READY mysql_port=23223 pid=<pid>`. Any orchestration that
backgrounds the server **must** gate its first connection on this line.

Run SQL tests with the generated runner config:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
```

Run cross-engine Iceberg compatibility tests where Spark writes through REST
Catalog + MinIO and NovaRocks reads the table:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```

Run NovaRocks-only Iceberg REST end-to-end smoke (no Spark, NovaRocks both
writes and reads):

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Generate an Iceberg format-v3 table through Spark against the same REST Catalog
and MinIO services:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"
```

Inside the Docker network, Spark must use `http://rest:8181` for REST Catalog
and `http://minio:9000` for object storage. NovaRocks should use the host
endpoints from `env.sh`. Do not mix container endpoints into NovaRocks catalog
SQL.

`docker/iceberg-rest/down.sh --runtime-only` 显式解绑，发布 unbound 并回收旧输出，保留全部历史 `data_locations`。加 `--purge` 才清理全部已记录对象存储位置的私有前缀；只有全部成功才释放数据引用，失败保留原发布供重试。共享 SSB/TPC 正确性 READY 数据不属于 worktree 清理范围；首次新对象存储需由输入 owner 显式准备并校验。

共享服务的停止与删除使用 `docker/iceberg-rest/fixture-runtime.sh stop/delete <exact-runtime-id>`，全局 `--root/--daemon` 放在子命令前并取自 manifest 的 owner locator。catalog 普通删除在有绑定时拒绝；force 只条件解绑仍指向目标的 worktree，保留数据引用。对象存储默认停止和任何删除都受 catalog/数据引用保护；force-stop 可以中断消费者但保留记录、卷和端口。HMS 或其他外部 endpoint 未退出时普通/force catalog 删除均在破坏操作前拒绝；先按 HMS 自身入口退出。共享模式拒绝 `down.sh --docker/--volumes`。旧 `nr-iceberg-rest`、`nr-iceberg-hive` 项目和卷不自动接管或删除，退出由用户另行安排。

真实验收基准仍为 1FE+3BE；all-in-one 只作 smoke。fixture verify 和测试不自行下载或构建缺失输入，BOM 前置条件失败为 BLOCKED（75），端口/身份/生命周期失败为 VERIFY FAILED。

---

## 8. Development and Testing Standards

Use [test governance](docs/guides/development/test-governance.md) for new or
materially expanded test assets, approval snapshots, attempt materials and the
boundaries of existing evidence. The common workflow contract owns the rules;
the guide supplies NovaRocks locations and operational responsibilities.

### 8.1 Language Standard

- User communication and design docs: Chinese
- Code comments/logs/errors/commit messages: English

### 8.2 Build Mode

日常构建使用唯一 `dev` 配置和当前 worktree 的 `target/`。规范检查点先执行
`tools/ci/dev-test.sh prepare logs/<attempt>/artifacts.json`，同次准备 workspace tests 和真实 bins；
常规 Rust 由 nextest 逐测试进程执行；定向选择用
`tools/ci/dev-test.sh test -E 'package(novarocks-memory) & test(=attribution_reconcile)'`，
原生列举用 `tools/ci/dev-test.sh list -E 'package(novarocks-memory)'`。
最终按影响面运行 `tools/ci/dev-test.sh test`；doctest 单独 `tools/ci/dev-test.sh doc`。
默认 4 slots、无重试、非 fail-fast；原生资源组约束重型进程测试，不机械传全局串行。
nextest 日常最低 0.9.148，正式工具固定 0.9.148；缺工具、缺必要输入或选择为空时明确失败。
日常准备后前台 exec nextest 保留 TTY；`local-full-ci.sh --cargo-only` 独立调用批处理，
保存原始 nextest/JUnit 与 doctest 结果、全部监护记录及 `execution-index.json`。
release 用于必要的产品接线/发布检查，不再维护第二套日常功能矩阵。

窄 `cargo test -p <owner>` 是允许的快速内循环；它可能改变 feature 统一和重链接集合，不能当完整规范图。
新 worktree 的首次依赖编译、底层库编辑后多个测试/程序的必要链接、IDE 私有 check 缓存都是实际成本。
旧 18s/32s 仅是历史观察；当前 opt 取舍须引用本轮原始构建材料，不承诺提速。
IDE 测试按钮与私有 target 配置、清理责任见 [测试治理](docs/guides/development/test-governance.md#ide-与缓存维护)。

### 8.3 Code Quality

- `cargo fmt`
- `cargo clippy`
- `cargo build`
- 按影响面执行 nextest；独立 Cargo doctest，局部调试可用 `cargo test -p <owner>`

共享受管启动从 `ManagedCommand::new(payload)` 构造，monitor 持真实 Child 并负责失联回收；
成功路径检查 stop/wait 与所需原始日志断言后才释放诊断。普通 nextest 首次受管调用懒建材料租约，
纯测试不建 root/guard；嵌套 runner 由外层先 `ensure_run_retention(deadline)`。
完整 UTF-8 行实时诊断上限 1 MiB，原始双流日志和完整 capture 不因此截断。
容器的运行状态/正式材料根必须显式挂载支持的本地文件系统；overlay 不作锁语义 fallback。
直接 Cargo/IDE 获得相同库级回收，不自动获得 nextest 隔离或正式汇总。
范围、保留规则和 Linux 验证状态见 [test-support README](tests/test-support/README.md)，
原始结果接口见 [CI README](tools/ci/README.md)，修改这些边界前读 ADR-0184。

### 8.4 SQL Regression Tests

The SQL runner is under `tests/sql/runner`; its only suite root is `tests/sql/correctness`.
It requires a running NovaRocks
MySQL-compatible standalone server. Do not assume a fixed port in Codex
workspaces; source `docker/iceberg-rest/runtime/current/env.sh` when that
entry exists.

**Start standalone (no external FE needed):**

The two role configs are yours to create: copy `novarocks-fe.toml.example` and
`novarocks-be.toml.example`, or use the generated pair described below.

```bash
# Debug: fast compile, slow query (for fix verification)
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config ./novarocks-fe.toml --be-config ./novarocks-be.toml

# Release: check a release artifact; daily suite runs use dev
NO_PROXY=127.0.0.1,localhost cargo run --release -p novarocks-server -- standalone --role all-in-one \
  --fe-config ./novarocks-fe.toml --be-config ./novarocks-be.toml
```

When the local test environment is active:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
```

When starting a server manually inside a Codex worktree, prefer the generated
config so its configured MySQL port cannot collide with another worktree.

**Run test suites:**

Non-isolated runs require an explicit runner config with fixture endpoints and
the exact publication env file. The default config does not supply them.

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite <suite> --mode <verify|record|diff> [--query-timeout 60] [-j 4]
```

With a generated runner config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
```

Suites are discovered from `tests/sql/correctness/`; ask the runner for the
current list with `--list-suites`. `tests/sql/correctness/README.md` carries the
suite map — which engine area each suite covers, its typical change entry, and
whether it needs the REST Catalog fixture or a cross-process topology — and is
the reference for choosing suites. `ssb`, `tpc-h` and `tpc-ds` are explicit-only
dataset correctness suites in this same root. They require prepared READY inputs,
`--cluster-mode cross-process --cluster-size 3 -j 1` and explicit
`--cluster-launch-profile performance`. That launch profile selects the clean
harness configuration; it does not measure performance. The consumer only checks
inputs, runs suite init/cases/cleanup and owns actual FE/BE shutdown.
Cluster mode and backend count come from runner CLI; no suite owns an alternate runtime.

**Run specific cases:**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite join --only join_cross_join_small,join_array_type --mode verify
```

### 8.5 Test Scope Selection

Run the tests a change can actually break. Targeted verification is the default;
a full run is a deliberate step with a stated reason, not a safety reflex. Full
runs belong at milestone convergence, at the final verification before claiming
completion, and where the blast radius genuinely is the whole repository.

**Rust: choose the affected owner for the narrow edit loop.** Every first-party crate lives
in the single root workspace. Narrow `cargo test -p <crate>` may change feature
unification; a missing narrow-graph prerequisite must stay visible and cannot
be treated as evidence about the complete graph. Use the standard workspace
nextest entry with native filters for formal graph coverage. The packages
under `vendor/` own separate workspaces and locks and are out of scope
unless you edited them.

| Change lands in | Run at least |
|---|---|
| `novarocks/sql/**` (parser, analyzer, optimizer, planner, function registry) | `-p novarocks-sql` |
| `novarocks/execution/**` (operators, expressions, pipeline, exchange, runtime filter) | `-p novarocks-execution` |
| `novarocks/frontend-application/**`, `novarocks/query-application/**`, `novarocks/catalog-application/**` | that crate, plus `-p novarocks-worker -p novarocks-native-adapter` when the change crosses the FE/BE boundary |
| `novarocks/worker/**`, `novarocks/native-adapter/**` | `-p novarocks-worker -p novarocks-native-adapter` |
| `novarocks/connector/<name>/**` | that connector crate, plus `-p novarocks-fs` when authorized object-store access changes |
| `novarocks/state-store/**` | the touched backend crate plus `-p novarocks-state-store-api` |
| `novarocks/types/**`, `novarocks/spi/**`, `novarocks/execution-contract/**`, `novarocks/*-codec/**`, `novarocks/proto-models/**` | the whole workspace: a shared vocabulary or wire format has no bounded blast radius |

**SQL: choose suites from the suite map** in
`tests/sql/correctness/README.md`, and narrow further with `--only <case>` when
a single case covers the change. A change to array function declarations needs
`complex-type` and `function`, not the corpus; outer-join nullability needs
`join`; CUBE needs `aggregate`.

**统计范围与成本。** 测试目标随 workspace 变化；用 `cargo metadata --locked --no-deps --format-version 1`
统计当前成员和声明目标，用规范准备的 Cargo artifact 统计实际启用的测试与程序，二者不混为 case 数。
SQL 用已准备 runner 的 `--list-suites` 与所选 suite manifest；成本记录实际拓扑、fixture 与退出，
不沿用固定二进制、suite 或 case 数量作为当前事实。

**Investigate timing failures with evidence.** Credential-expiration checks
such as `provider_pool_evicts_vended_provider_at_credential_expiration`
(`novarocks/fs/src/access.rs`) and the `managed_process` family
(`tests/test-support/src/managed_process.rs`) can warrant investigating load,
readiness and signal timing. Their names or time constants do not establish a
cause. Preserve the original failure and actual load/process observations;
record isolated reruns separately. A pass when run alone means the failure was
not reproduced in that attempt. Concurrent load, even when observed, does not
by itself exclude a product race or justify calling the failure noise.

Neither family is in `tools/ci/baselines/known-failures.toml`, which covers
accepted SQL failures only. Keep an existing approved disposition separate from
this run's raw failures; targeted passes must not overwrite a full run's raw
result. See [failure and evidence boundaries](docs/guides/development/test-governance.md#失败与结论边界).

For SQL suites, read the first failure rather than the failure count: one
failing statement can leave the shared server in a state where later cases fail
for reasons of their own, so the count overstates the problem. Re-run a
suspected case against a clean server before attributing it to the change.

## 9. Suggested Starting Points for Typical Changes

- **SQL / parser / planner changes**: start with `novarocks/parser/src/**`,
  `novarocks/sql/src/{analysis,analyzer,semantic}/**`,
  `novarocks/sql/src/optimizer/**` and `novarocks/sql/src/planner/**`.
- **Package decode / compiled preparation**：从 `novarocks/plan-codec/src/physical_package_v2/**`、
  `novarocks/native-adapter/src/backend_task_execution/{compiled_package.rs,execution_host.rs}`
  与 `novarocks/local-compiler/src/**` 开始；Task 能力投影见 `compiled_scan_binding.rs`、
  `compiled_writer_binding.rs`、`compiled_runtime_filter.rs`，pipeline 见 Execution
  `exec/pipeline/builder/compiled.rs`。先读 ADR-0184，禁止恢复 v1 decoder/构造桥。
- **MySQL protocol behavior**: inspect `novarocks/mysql-adapter/src/listener.rs`,
  `result_encoding.rs`, `relay_result_writer.rs` and `local_result_writer.rs`.
- **DDL/DML behavior**: inspect `novarocks/query-application/src/sql/**` for
  admission and routing, then the specific flow under
  `novarocks/frontend-application/src/query_execution/dml/**` (`insert.rs`,
  `delete/**`, `mutation_flow.rs`, `truncate.rs`, `add_files.rs`,
  `iceberg_writer.rs`); catalog/table DDL is in
  `novarocks/frontend-application/src/catalog_application/statement.rs`.
- **Iceberg 提交、冲突与清理**：从 `novarocks/connector/iceberg/src/commit/{model,dependency,staging}/**`、
  `commit/attempt.rs`、`commit/operation.rs` 和 `commit/write_stack/control/publication.rs` 开始。
  单次派发结论由 `commit/attempt/observation.rs` 的 journal 拥有，prepared owner 在 `catalog/publication_dispatch.rs`；冻结恢复载荷与桥接证据在 `commit/recovery.rs`、
  `commit/attempt/observation.rs`；CTAS 完整 CREATE 请求在 `catalog_control/staged_create/publication.rs`。
  读 [ADR-0171](docs/adr/ADR-0171-commit-operation-model.md) 与 [ADR-0179](docs/adr/ADR-0179-iceberg-publication-lifecycle-and-evidence.md) 后再改身份、字段继承、派发生命周期或清理授权。
- **Execution semantics/operator behavior**：先核对 `novarocks/local-compiler/src/**` 和
  `novarocks/local-program/src/**`，再看 Execution `exec/pipeline/builder/compiled.rs`、
  `exec/operators/**`；`exec/node/**` 仍有共享值类型和运行能力，不能当旧树整片删除。
- **Scheduling/parallelism**: inspect `novarocks/execution/src/exec/pipeline/**`.
- **Exchange behavior**: inspect `novarocks/execution/src/runtime/exchange.rs`,
  `novarocks/execution/src/runtime/fragment/io/**`,
  `novarocks/execution/src/exec/operators/exchange_source.rs`, and
  `novarocks/native-adapter/src/{exchange_data_plane,exchange_transmitter,fragment_exchange_receiver}.rs`.
- **Materialized views**: inspect `novarocks/mv-application/src/**` and
  `novarocks/frontend-application/src/mv/**`; MV query rewrite is an optimizer
  concern under `novarocks/sql/src/optimizer/**`.
- **Native distributed task execution**: inspect
  `novarocks/frontend-application/src/query_execution/native_execution_adapter.rs`,
  `novarocks/frontend-application/src/task_execution/**`,
  `novarocks/worker/src/{task_registry,ingress,convergence}.rs`,
  `novarocks/native-adapter/src/{backend_application,task_protocol_ingress}.rs`, and
  `novarocks/execution/src/task_execution/**`. Preserve the Installed/normal-close
  gated edge-open barrier and the per-domain Apply / Idempotent / Older /
  Conflict verdict; do not add a protocol shim, a standalone direct-call path,
  or a no-runtime-filter retry inside an attempt. See ADR-0161.
- **Connector behavior**: inspect `novarocks/connector/**` and
  `novarocks/fs/**`. The active sealed providers are Iceberg and Paimon;
  StarRocks is retired and must not be restored through a local-binding config
  or runtime fallback.
- **Connector scan execution**: inspect
  `novarocks/spi/src/connector/read_stack/page_stream.rs` (the page stream
  contract and poll budget), `novarocks/worker/src/typed_connector_runtime/stream.rs`
  (split claims, successor window and per-split streams as one stream) and
  `novarocks/execution/src/exec/operators/scan/stream_source.rs`. Keep one
  stream per scan driver, the fan-out inside the builder's scan branch, and
  close as observation of physical exit; see ADR-0159.
- **FE/BE interface behavior**: inspect `novarocks/frontend-application/src/**`,
  `novarocks/query-application/src/**`, `novarocks/worker/src/**`,
  `novarocks/native-adapter/src/**`, and the neutral contracts under `novarocks/spi/**`
  (Connector) and `novarocks/state-store/api/**` (StateStore). Those two
  provider domains are separate compilation units: the StateStore contract does
  not depend on Arrow or on the Connector contract, and its shared test
  mechanics live in `novarocks/state-store/testkit/**`, which no production
  crate may depend on. See `docs/guides/development/state-store-boundary.md`.
- **Optimizer observability / plan-shape regression**: see
  `novarocks/sql/src/explain/**` for the EXPLAIN formatter (Normal/Verbose/Costs/
  Analyze). `EXPLAIN ANALYZE` returns a query-level Planning/Execution/
  Rows header above the Verbose plan; per-operator runtime stats are a
  follow-up. Verbose/Costs/Analyze append a stable `stats={rows=N}`
  trailer to each physical node. `SET disable_optimizer_rules = 'RuleA,RuleB'`
  (alias `cbo_disabled_rules`) bisects optimizer rules at session level;
  see `novarocks/sql/src/optimizer/options.rs`. Use the `tests/sql/correctness/optimizer/` suite
  for plan-golden cases, and `-- @explain_contains=<substr>` /
  `-- @normalize_explain_timing` in any sql-test case to assert plan-shape
  facts alongside the result golden.
- **Aggregate pushdown rule (OPT-1)**: see
  `novarocks/sql/src/optimizer/rewrite/rules/aggregate_pushdown/`. Pushes
  `LogicalAggregate` past inner/outer joins toward leaves when NDV
  bucketing predicts a real row-count reduction. White-list functions
  are SUM/MIN/MAX/COUNT(col). Disable via
  `SET disable_optimizer_rules = 'AggregatePushdown'`. Plan-shape
  cases live under `tests/sql/correctness/optimizer/sql/aggregate_pushdown_*.sql`.
  The idempotency guard is `LogicalAggregateNode::already_pushed`
  (`novarocks/sql/src/planner/logical/node.rs`) — other rules must preserve the
  flag when cloning.

---

## 10. StarRocks Reference Code Location

For StarRocks side-by-side reference implementation, use: `~/project/starrocks`

---

## 11. Architecture Decision Records (ADR)

Durable design decisions, philosophies, and their honestly-recorded trade-offs live in
`docs/adr/` (index and authoring rules: `docs/adr/README.md`).
Before changing architecture-level behavior, check the index for the affected domain.
Any PR that embodies a new design decision or accepts a compromise must add or
supersede an ADR — use `$ops-capture` from the `workbench` plugin
(`.agents/skills/workbench/`, exposed directly to Codex and to Claude Code via the
`.claude/skills` → `.agents/skills` symlink). Its contract embeds the template,
numbering, supersede, and collision-renumbering rules. `docs/adr/README.md` remains
authoritative for this repository: where it and the skill contract disagree, the
README wins.

Before writing one, apply the scope test: an ADR records a **long-lived design in
the code**, not one fix's ruling. If the entry would lose its value once the
incident that prompted it is forgotten, it belongs in the knowledge base as a
case, not here. Case narrative — field quotes, logs, the investigation path,
wrong turns, which fix was chosen — stays out of the ADR body; the ADR reaches it
through `provenance` and `related` only.

---

## 12. Project Development Workflow and Knowledge Base

Before execution, follow [test governance](docs/guides/development/test-governance.md)
for the project approval Git snapshot, a new attempt location and retention.
Before cleanup or finish, verify the recorded evidence handoff and recoverability.
These conventions do not authorize push, PR, upload or automatic deletion.

For NovaRocks feature, architecture, roadmap, and refactor work, resolve the
project documentation root from memory. Use the newest directly applicable,
existing path; when memory has no usable project documentation path, use
`<repo-root>/docs/workbench`.

The documentation root holds two side-by-side halves: `workflow/` for staged
development artifacts (spec, plan, umbrella, archive) and `ops/` for the durable
engineering knowledge base (scenarios, cases, and — for projects without an
in-repo ADR directory — ADRs). Resolve the root itself, never either half; if a
memory entry points straight at `.../workflow`, take its parent.

Use the generic skills-only plugin under `.agents/skills/workbench/`:

- `workbench`: identify and route the current stage or knowledge operation;
- `dev-workflow-explain-technical-concept`: explain technical concepts and
  current mechanisms in Chinese from first principles, with one concrete
  running example and explicit concept/design/implementation/impact layers;
- `dev-workflow-discuss-design`: settle the problem and major design decisions;
- `dev-workflow-write-spec`: write the accepted design into project docs;
- `dev-workflow-plan`: write and iterate the plan directly in project docs,
  then mark the persisted version approved after explicit user approval;
- `dev-workflow-execute`: create a goal and execute continuously through
  verification;
- `dev-workflow-finish`: publish and archive only when authorized;
- `ops-capture`: record a reproducible scenario, a first-hand debugging case, or
  an ADR, with its symptoms written both the way the field describes them and the
  way they appear in logs; for an ADR it first applies a scope test that keeps
  case narrative out of the record and makes the ruling produce named, reusable
  rules;
- `ops-lookup`: check our own records for whether a symptom has been hit before,
  and hand off to external-source search when it has not.

Codex discovers the project-local skill sources directly under `.agents/skills/`.

Knowledge operations are a side route available from every stage. `ops-lookup` is
read-only; `ops-capture` writes only knowledge entries. Neither changes spec, plan,
goal, or publication state.

Technical explanation is a read-only side route available from every stage. It
does not accept a design, approve a plan, authorize implementation, or modify
workflow state. Deliver the requested explanation before proposing a spec,
plan, code change, commit, push, or PR.

Do not skip the accepted-design and approved-plan gates. Sub-agents are allowed
in every stage when they provide useful parallel investigation, independent
verification, isolated implementation, or risk review. The main agent retains
ownership and verifies their results.

The bundle carries two contracts under
`.agents/skills/workbench/skills/workbench/references/`: `workflow-contract.md` is
the only development-workflow contract, and `ops-contract.md` is the only
knowledge-base contract. Do not depend on an external workflow document. The plan stage runs in the current editable mode and persists its
draft immediately; do not require Codex Plan mode. Produce a task DAG with hard
dependencies, parallel waves, non-overlapping file ownership, sub-agent
scheduling labels, independent validation, integration gates, and local commit
checkpoints. Explicit user approval promotes the persisted plan from `draft` to
`approved` before execution.

Plan and execute both resolve test scope through section 8.5: a plan task names
the tests its own change can break, and execution runs those, not the whole
repository. A full run needs one of the reasons section 8.5 lists.

Once execution starts, routine implementation difficulties are not reasons to
stop; pause only for the major decision conditions defined by
`dev-workflow-execute`. On a task-local development branch, checkpoint commits
are allowed after a coherent plan section completes or before risky changes.
The execute stage must never push or open a PR; those actions require explicit
authorization in `dev-workflow-finish`.

### 当前测试资产与维护频率

当前阶段只维护功能与数据正确性；不新增或维护吞吐、延迟分位数、RSS 比较门及持续测量协议。历史报告和 frozen receipt 仅是来源证据，不能成为新采集准入。一次性研究保存在仓库外独占尝试目录；晋升资产前先确定性质、最低充分 owner、独立 oracle、输入和真实退出。先承接性质再删除旧载体。

相关 Rust owner/helper/input 改动沿既有单测链路；结果慢端/并发与 range/delete/planning/DOP 显式功能组由领域维护者每周手工运行，相关 owner/fixture 改动必跑。三套 SSB/TPC correctness 成本较高，runner/输入绑定改动必跑三套，每周手工完整轮次，不扩默认 CI。MV/OPTIMIZE 沿既有 suite。周期调度与共享证据服务尚未交付，G7a/G7b 已取消，不作未来交付承诺。详见 [测试治理](docs/guides/development/test-governance.md)。

FE preparation 只保留带独立 secret 鉴权的 `/statement/{arm,peek,drain}` 功能观测；运行级 `/arm`、`/drain` 与专用计时钩子退出。内部 statement 观测由实际开发 cfg 编入，还需本次实际角色会话的独立观察授权；普通产品没有该内部路由。

### 主测试产物与有限授权

实际 `debug_assertions` 决定 `novarocks_dev_controls`，标准 dev 编入控制能力、标准 release 排除。
编入能力不授予任何动作：普通及有限控制场景使用同一个显式主 Server，授权仍绑定本次角色会话和准确动作。
SQL 仅传 `NOVAROCKS_BIN`，System 传 `--binary`；缺能力或授权明确失败，不能跳过断言或回退旧程序。

[规范开发入口](tools/ci/dev-test.sh) 与 CI 共用 workspace/tests/bins/dev 主图和 worktree target；
真实 Cargo JSON 绑定路径、自报与 hash，最后一次相关 Cargo 后刷新，消费前核对。
Server CLI 保留 `novarocks-server`，SQL CLI 保留原 manifest/package；System CLI 为
`novarocks-system-test-cli`，原 `novarocks-system-test-runner` 为库。来源仅薄入口冻结，库不监听 Git。
不要用源码 HEAD、路径、identity 或 runner profile 代替实际 FE/BE 自报。

默认功能 CI 排除兼容专项且不准备变体；独立 [兼容入口](tools/ci/native-compatibility-ci.sh)
按原生所选集合的完整生命周期需求准备 Compatible/OtherIsland，每种一次，然后恢复主输入并刷新产物。
选择策略、IDE 与保留/回收见 [测试治理](docs/guides/development/test-governance.md#兼容专项的选择)，
场景性质和 fixture 需求见 [System README](tests/system-test-runner/README.md#兼容专项与程序需求)。
HMS/MySQL 私有完整矩阵缺输入仍记 `NOT_RUN`，局部单测不替代原输入与真实退出。

现有 LNP-3A/LNP-3D、MV storage-contract 与 system MV 资产保留，旧指令/CALL 禁止扩展。7B3 承接非聚合冷恢复、配置/paused、Unknown 与管理接续；7B4 承接 SUM/COUNT、聚合状态和聚合冷恢复；7B5 承接 V/S 身份、发现/CREATE/DROP、路由与 materialized S/fallback；7B11 承接文档格式、独立 graph 与 shared-document-revisions。每个唯一产品 owner 在替代保护实际通过后逐项退役对应旧载体，不在 G2 新增启动期缓存缺失或通用擦除接口。
