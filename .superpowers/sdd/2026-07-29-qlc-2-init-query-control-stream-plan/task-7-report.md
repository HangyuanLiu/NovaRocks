# QLC-2 Task 7 实施报告：control-ready submission cutover

## 结论

Task 7 已完成。Frontend production coordinator 现在必须先用同一 live backend
snapshot 完成全 participant `InitQuery -> Attach -> ControlReady` barrier，随后才可
assembly 与提交 fragment；Backend native submit 必须携带同一
`QueryExecutionId`，并通过共享 `QueryLifecycleRegistry` 的 exact manifest admission。

旧 runtime-filter 独立 install/abort unary RPC、client、Frontend owner、Core legacy
typestate/lease 与 Backend adapter 已删除。QLC-1A cancellation、backend loss、报告失败、
超时与本地 fragment failure 都通过 attempt lifecycle stream first-wins 终止，不再从
Frontend registry 直接调用 `cancel_fragments`。

本任务没有实现 Task 8/9，没有 push、开 PR 或归档设计文档。

## Production cutover

### Frontend

- 每次 query 分配 `QueryExecutionId(attempt=1)` 后只解析一次 live topology；
  scheduler、gRPC lifecycle transport 与 gRPC fragment dispatcher 都从该冻结 snapshot
  构造。All-in-one 同样走 `BackendServicesSource::Live`，没有 local shortcut。
- schedule bind 后构造 `QueryInitOptions`，调用
  `initialize_query(...).assemble()`；schedule-bound 类型没有绕过 ControlReady 的
  production assembly 路径。
- `QueryLifecycleLease` 覆盖 submit、fetch、write/profile report aggregation 与最终
  outcome。成功显式 `finalize`；取消、LocalFailure、submit unknown outcome、fetch/report
  失败、超时或 assembly 失败均 `abort_preserving`，保留 primary error。
- `FrontendQueryRegistry` 只通知 active attempt control；不再保存 fragment dispatcher，
  也不再直接 fanout `cancel_fragments`。service-only participant 因来自 Init plan 的完整
  participant 集合，同样收到 Abort。

### Native wire 与 Backend admission

- `SubmitFragmentRequest` append-only 增加 required `execution_id = 3`；dispatcher 编码，
  gRPC server 在 native payload decode/ingress 前严格拒绝缺失、零 attempt、query id
  不匹配或非法 identity。
- `NativeFragmentRequest` 必须拥有 `QueryExecutionId`。
- Backend application composition 创建一份 `FragmentControlRegistry` 和一份
  `QueryLifecycleRegistry`；local lifecycle runtime、lifecycle ingress 与
  `NativeFragmentService` 共享它们。
- submit admission 顺序为：lifecycle permit、query runtime prepare、fragment prepare、
  control reserve、query register、pending control publish、worker spawn、report register、
  permit commit、start signal。
- permit commit 在 entry lock 内重新验证 ControlAttached/未终止，并把 finst 原子映射到
  execution id；Abort 与 submit race 时，late commit 及后续 admission 都 fail-closed。
  pending control 会锁存 start 前到达的 Abort。
- fragment failed/cancelled terminal fact 通过 finst mapping 发出 `LocalFailure`，并以
  first-wins 终止该 attempt；成功 terminal 只移除 mapping。
- Fresh review 额外修复 worker spawn 失败的 pre-start registration 回滚：registration
  lease 只在 worker 成功 spawn 后转入 running，避免 route/query mapping 泄漏。

### 旧 RF owner 删除

- 从 `NovaRocksGrpc` 删除 `InstallRuntimeFilterDeployment` /
  `AbortRuntimeFilterDeployment`。
- 删除 Core gRPC deployment adapter、client methods、fragment-dispatcher legacy
  implementation，以及 Frontend `runtime_filter.rs` owner。
- 删除 legacy artifact typestate、独立 epoch/options/install plan/barrier/lease/compiler。
- `filter.proto` 的 install/abort request envelope 仅保留为 lifecycle manifest 与 local
  runtime rollback 的 canonical codec value；它们不再是 RPC method 或 production
  transport caller。

## TDD 证据

RED 阶段先建立以下失败：

- native submit 无 `execution_id` 时，旧 wire/ingress 仍会继续进入 payload decode；
- Backend 在没有 ControlReady attempt 时仍能进入 fragment prepare/admission；
- registry Abort 与 in-flight submit permit 之间没有 commit-time 线性化；
- fragment failure 只有 FE report，没有 query-control `LocalFailure`；
- Frontend cancellation 仍直接调用 dispatcher，且 service-only participant 不在该集合；
- production coordinator 仍使用旧 RF barrier 后直接 assembly。

GREEN 聚焦回归：

```text
cargo test -p novarocks-frontend query_control_barrier_precedes_submission --lib -- --nocapture
1 passed; 0 failed

cargo test -p novarocks-frontend query_cancel_aborts_all_participants --lib -- --nocapture
1 passed; 0 failed

cargo test -p novarocks-backend fragment_requires_query_control_ready --lib -- --nocapture
1 passed; 0 failed

cargo test -p novarocks-backend query_abort_submit_race --lib -- --nocapture
1 passed; 0 failed

cargo test -p novarocks-backend fragment_failure_emits_query_local_failure --lib -- --nocapture
1 passed; 0 failed
```

Fresh review 进一步执行完整可编译 library suites：

```text
cargo test -p novarocks-frontend --lib -- --nocapture
119 passed; 0 failed

cargo test -p novarocks-backend --lib -- --nocapture
50 passed; 0 failed
```

其中 Frontend 旧 contract/backend-event tests 已从 direct dispatcher cancellation
断言迁移到 lifecycle-control-only 语义；all-in-one 测试通过真实 generated gRPC lifecycle
barrier 后才抵达预期的 fragment ingress rejection。

## Compile gates 与静态审计

最终真实树：

```text
cargo check -p novarocks
exit 0

cargo check -p novarocks --features compat
exit 0

cargo check -p novarocks-frontend
exit 0

cargo check -p novarocks-backend
exit 0

cargo check -p novarocks-compat
exit 0

cargo fmt --all -- --check
exit 0

git diff --check
exit 0
```

`rg` 审计为零：

- legacy typestate/epoch/options/install plan/barrier/lease/compiler；
- unary RF deployment RPC、adapter 与 client callers；
- `query_registry.rs` / `execution.rs` 中的 direct `cancel_fragments`；
- `FrontendRuntimeFilterDeployment` 与旧 barrier counter。

生产 constructor audit 确认 gRPC fragment dispatcher 总是编码 execution id；server
先 decode identity，Backend ingress 再按同一 id admission。Compat target 独立通过，
未添加 fake Init 或 standalone-only 分支。

## 基线阻塞与精确归因

计划中的两个 Core filtered `--lib` 命令仍在运行目标测试前被仓库既有
Iceberg optimize test helper API 漂移阻塞：

```text
cargo test -p novarocks submit_fragment_execution_id --lib -- --nocapture
cargo test -p novarocks grpc_query_lifecycle --lib -- --nocapture
```

共同错误为缺少 `IcebergOptimizeJobOutcome`、
`CreateIcebergOptimizeJobRequest`，以及 `JobMetaRepository` 的
create/claim/record/finish Iceberg optimize job 方法。本任务没有修改这些路径；
production Core default/compat compile gates 均通过。

计划原样、不带 `--lib` 的 Frontend filtered test 还会编译所有 integration targets，
被既有 StateStore API drift 阻塞：缺少 `FeDeploymentView`、`StateStoreRuntime`、
`open_state_store`，且旧 fake 仍实现已不属于 `StateStore` 的 `provider_name`。
本任务目标测试在 `--lib` 下真实运行，且完整 Frontend lib suite 全绿。

## Fresh review 回答

1. **是否存在未经过 Init/ControlReady 的 native submit？**

   否。Frontend production assembly 只能来自 ControlReady typestate；wire 强制
   execution id；Backend admission 强制 exact active manifest。
2. **是否还有 production old RF-only caller？**

   否。RPC、adapter、client、owner、legacy typestate/lease 均删除；RF contribution
   由 Init manifest 安装并由 query lifecycle cleanup。
3. **KILL 是否覆盖 service-only participant？**

   是。active attempt control 拥有 Init plan 的完整 participant set；三 participant
   回归包含一个没有 fragment 的 service-only backend，并断言三个 stream Abort、
   direct fragment cancellation 为零。
4. **Abort/submit permit race 是否可漏过？**

   否。permit commit 重验 termination/phase；control 在 commit 前已发布且可锁存 Abort；
   race 回归断言 late commit 与所有后续 admission 都拒绝。
5. **All-in-one 是否偷偷绕过 gRPC lifecycle？**

   否。Live source 同时构建 gRPC lifecycle transport 与 fragment dispatcher；
   完整 Frontend suite 的 all-in-one generated-service 回归已通过该 barrier。
6. **Compat 是否伪造 Init？**

   否。compat target 独立编译通过，未新增 compat-specific fake lifecycle 分支。

## Fix round 1/5：review finding closure

### Proto baseline 与 tag discipline

`current_schema_matches_baseline` 的初始 RED 同时报告了 QLC-2 新 lifecycle
messages/RPC、`SubmitFragmentRequest.execution_id = 3`、已删除的旧 RF unary
RPC/response DTO，以及本 arc 当前 native plan/filter schema 尚未进入 ledger 的变更。

本仓库当前明确不承诺 mixed-version wire compatibility，NovaRocks cluster 整体升级，
且该迁移没有 released client/history。恢复旧 unary RPC/response 会重新引入已经删除的
legacy RF owner，与本任务的 single query-lifecycle owner 冲突。因此本轮采用受审查的
pre-release no-history baseline 原子重置，而不是保留兼容 shim、跳过测试或放宽
comparator：

- `idl/novarocks/README.md` 记录该 reset 仅适用于首个 released wire contract 之前；
- ordinary comparator 与 additive writer 的拒绝规则保持原样；
- reset 后的 ledger 立即成为新的 append-only baseline；
- `RuntimeFilterProducerRole` 没有复用旧 tag 3：显式 reserve tag/name，新
  `join_build_key` / `aggregate_topn_key` 使用 tag 4/5；
- 最终在没有 write env、没有 skip 的情况下，focused baseline test 真实通过。

### Production all-ready ordering boundary

旧 `query_control_barrier_precedes_submission` 只直接调用 barrier。本轮将 production
ordering 证据放到真实 `FrontendDistributedQueryCoordinator::execute` 边界：

- 使用包含 fragment executor 与 RF service 的 multi-participant contract fixture；
- transport 在每个 session 真正返回 `ControlReady` 时记录 participant；
- 第一次实际 `FragmentDispatcher::submit_fragment` 入口确定性暂停；
- 在该入口比较 materialized Init participant set 与 ControlReady set，要求二者完全相等
  且 participant 数至少为 2；
- 释放 submit 后继续走真实 result fetch/finalize 并验证 non-empty result。

因此如果 coordinator 在最后一个 participant ready 前开始 dispatch，该测试会在实际
submit boundary 直接失败；它不再只是 barrier fake 的内部断言。

### Abort/Submit service race

新增真实 `NativeFragmentService` race：在 lifecycle permit 发放后暂停 submit，
并发从 query control 执行 Abort，再恢复 fragment prepare、query registration 与
pending control publication。

RED 证据是 lifecycle events 只有：

```text
[Prepared, Registered]
```

也就是首次 termination 发生时资源还不存在，旧实现的 late permit commit 虽拒绝 start，
但没有让 local termination 观察随后发布的真实 control。修复后，late commit rejection
在 resource publication 之后幂等重放 local termination；测试观察真实
`Registered -> Cancelled`，断言永远没有 `Started`，并等待 control route/query mapping
清理完成。

### Spawn failure rollback

第二次 worker spawn failure 后不再只检查 route cleanup；测试会用完全相同的 finst
再次 submit 并要求进入 worker start。这同时证明：

- query registration/control reservation 已释放；
- lifecycle in-flight permit 已由 RAII drop 回滚；
- first running fragment 的 route 没有被错误回滚。

### Fix-round verification

```text
cargo test -p novarocks --test proto_schema_compatibility \
  current_schema_matches_baseline -- --exact --nocapture
1 passed; 0 failed

cargo test -p novarocks-frontend --lib \
  query_control_barrier_precedes_submission -- --nocapture
1 passed; 0 failed

cargo test -p novarocks-backend query_abort_submit_race -- --nocapture
1 passed; 0 failed

cargo test -p novarocks-backend \
  second_worker_spawn_failure_rolls_back_only_its_pre_start_registration -- --nocapture
1 passed; 0 failed

cargo test -p novarocks-frontend --lib -- --nocapture
120 passed; 0 failed

cargo test -p novarocks-backend --lib -- --nocapture
51 passed; 0 failed

cargo check -p novarocks --features compat
exit 0

cargo check -p novarocks-compat
exit 0

cargo fmt --all -- --check
exit 0

git diff --check
exit 0
```

不带 `--lib` 的 Frontend filtered 命令仍会在运行目标测试前编译所有 integration
targets，并复现上文已归因的 StateStore API drift；本轮没有修改该独立阻塞路径。
