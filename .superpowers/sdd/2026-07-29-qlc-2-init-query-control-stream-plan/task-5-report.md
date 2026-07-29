# QLC-2 Task 5 实施报告：Frontend lifecycle barrier 与 lease

## 结论

Task 5 已完成。Frontend 现在具备消费 canonical `QueryInitPlan` 的 lifecycle
orchestration seam：冻结 participant 集合，并发完成全体 Init/Attach，
只有所有 participant 都返回 ready 后才交付 lifecycle lease；任一失败、取消、
backend generation 失效、LocalFailure、control stream 丢失或 heartbeat timeout
都会 fail-close 并清理完整 attempted set。

本任务刻意没有修改 `coordinator/execution.rs` 的 production submission 路径：
Task 6 尚未提供生产 gRPC transport，Task 7 才负责 scheduler/submission cutover。
因此当前提交证明 Frontend orchestration contract，但不会偷偷把旧 fragment
submission 切到未完成的新协议。

提交：由本报告对应的
`feat(frontend): coordinate query lifecycle barrier` 提交记录。

## 实现内容

### canonical manifest materialization 与全体 barrier

- 新增 `coordinator/query_lifecycle/{mod,manifest,barrier,lease}.rs`。
- `manifest.rs` 只消费 Task 2 已冻结的 `QueryInitPlan`，把同一份 manifest 与 digest
  编入 `QueryInitRequest`；target 保存同一 live snapshot 的 backend index、
  endpoint 和 start epoch，不重新推导 topology、runtime filter 或 query options。
- `InitQuery` 对全 participant 使用 scoped workers 并发 fanout；仅
  `DeadlineExceeded`/`StreamClosed` 这种 transport-unknown outcome 使用完全相同的
  execution id、manifest 和 digest 重试一次。业务拒绝不重试。
- Init 全 ready 后并发 Attach，并逐 session 等待 `ControlReady`。partial attach
  session 也会进入 cleanup ownership；任一失败都不会产生可提交 lease。
- 失败清理先保留 primary error，再并发向完整 attempted set 发 unary
  `AbortQuery`；rollback error 只追加上下文。termination ACK 还会校验 execution
  identity 与 `CoordinatorAbort` reason。

### lifecycle lease、heartbeat 与 fail-close

- lease guard 持有完整 attempted participant、全部 active session、唯一 attempt
  supervisor、registry binding 和 bounded join ownership。
- 每个 attempt 只有一个 supervisor；每轮先向全部 session 发送相同 sequence 的
  heartbeat，再收取严格匹配的 `HeartbeatAck`。
- `LocalFailure`、错误 event、stream send/receive 失败和 heartbeat timeout 都通过
  first-wins registry failure 触发 attempt abort；不重连、不 takeover。
- 显式 `finalize` 向全部 session 并发发送 Finalize，并等待
  `TerminationAccepted(CoordinatorFinalize)`；失败后对所有 participant 做 unary
  cleanup。
- abort 优先复用 active stream，session 不可用时 fallback unary AbortQuery；
  Drop 未 finalize 时 fail-close。supervisor stop 使用 condvar 唤醒，guard join
  等待有界于 heartbeat timeout + attach timeout。

### FrontendQueryRegistry binding

- 新增计划要求的 `ActiveQueryAttemptControl`，barrier 在第一条 Init 前绑定共享
  control；binding 由 lease RAII 持有并只清理同一 execution id。
- bind 前会检查 registration 到 barrier 之间已锁存的 first failure/cancellation，
  因而 pre-init cancellation 不会发出 Init。
- registry 把 service-only participant 的 backend generation 合并进 attempt
  ownership；backend unavailable/restarted 因此能终止只承担 runtime-filter 等服务
  的 participant，而不只依赖 fragment map。
- QLC-1A first failure、report failure、backend unavailable/restarted 和显式
  cancellation 都会同时通知 active lifecycle control；旧 fragment cancellation
  仍保留，供 Task 7 cutover 前兼容。

### 日志与 metrics

- 结构化日志覆盖 attempt 创建、participant 分类、Init/Attach outcome 与 latency、
  abort/finalize reason；participant 只记录 digest，不记录 SQL/options。
- FE lifecycle 状态变化发布 `FrontendQueryLifecycleMetricsSnapshot`，Prometheus
  暴露 active attempts、Init outcomes、control outcomes 与 Init/Attach latency
  totals/samples。

## TDD 证据

### RED

先加入计划要求的 barrier、unknown ACK retry、lease cleanup、LocalFailure 和
service-only fake transport 测试，再运行：

```text
cargo test -p novarocks-frontend frontend_query_lifecycle -- --nocapture
cargo test -p novarocks-frontend frontend_query_lifecycle_lease -- --nocapture
```

初次均按预期失败：`query_lifecycle/{manifest,barrier,lease}.rs` 尚不存在，
Frontend 也没有 barrier/lease 类型。随后实现最小 contract seam、并发 fanout、
registry binding、lease 与 supervisor，再逐步转绿。

Fresh review 又补充了：

- business rejection 不重试；
- rollback error 不覆盖 primary error；
- unary termination ACK identity/reason 校验；
- pre-init cancellation 与 service-only backend loss 的 registry 回归。

### GREEN 与最终验证

```text
cargo test -p novarocks-frontend --lib frontend_query_lifecycle -- --nocapture
test result: ok. 12 passed; 0 failed

cargo test -p novarocks-frontend --lib frontend_query_lifecycle_lease -- --nocapture
test result: ok. 5 passed; 0 failed

cargo test -p novarocks-frontend --lib query_registry -- --nocapture
test result: ok. 2 passed; 0 failed

cargo check -p novarocks-frontend
Finished `dev` profile; exit 0

cargo fmt --all -- --check
exit 0

git diff --check
exit 0
```

覆盖的关键行为包括：

- 3 participant 的 2 ready + 1 attach failure 会 abort 全部 3 个 attempted target；
- unknown InitAck 只对该 target 以同一 request/digest 重试一次；
- `RejectedCapacity` 不重试；
- partial failure 的 primary error 保持在首位，rollback failure 只追加；
- Drop abort、Finalize exactly once、duplicate abort 幂等；
- LocalFailure 与 heartbeat timeout fail-close；
- service-only participant 同样进入 Init/Attach、heartbeat、backend loss 与 cleanup；
- registration 后、第一条 Init 前的 cancellation 阻止 fanout。

## 基线阻塞与归因

计划原命令不带 `--lib`，会先编译 `novarocks-frontend/tests` 全部 integration
targets；当前树仍被 Task 5 之前已存在的 state-store 测试 API 漂移阻塞：

- `table_maintenance_repository.rs` 缺少 `FeDeploymentView`、
  `StateStoreRuntime`、`open_state_store`；
- 旧 fake 仍实现已不属于 `StateStore` 的 `provider_name`。

因此最终 focused acceptance 使用 `--lib` 精确运行新增单元测试；production
frontend library 的 `cargo check` 已通过。

Core metrics 单元 filter 同样被既有 Iceberg optimize job 测试编译错误阻塞：
缺少 `IcebergOptimizeJobOutcome`、`CreateIcebergOptimizeJobRequest` 以及
`JobMetaRepository` 的 create/claim/record/finish optimize job API。Task 5 没有
修改这些路径；metrics production code 已随 frontend check 完整编译。

## Fresh review

1. **participant 是否从别处重新推导？**
   否。只消费 frozen `QueryInitPlan`，wire request 与 target identity 都来自同一
   participant snapshot。
2. **是否可能 partial ready 后提交 fragment？**
   不会。lease 只在全部 InitAck ready 且全部 ControlReady 后构造；本任务也没有
   production submission cutover。
3. **unknown ACK 是否会换 digest 或扩大重试？**
   不会。只 clone 同一个 request 重试一次；业务拒绝立即进入全体 cleanup。
4. **失败清理是否遗漏 service-only participant？**
   不会。attempted set 来自 plan 全 participant，与 fragment ownership 无关。
5. **primary failure 是否可能被 rollback 覆盖？**
   不会。primary first-wins；cleanup error 以
   `query lifecycle rollback failed` 追加。
6. **heartbeat 是否允许 reconnect/takeover？**
   不允许。一个 lease、一个 supervisor、每 participant 一个 active session；
   任何 stream failure 都 fail-close。
7. **是否越过 Task 6/7 边界？**
   没有。这里只定义 transport seam 和 fake acceptance；生产 Tonic client 属于
   Task 6，scheduler/submit cutover 属于 Task 7。

## 剩余关注

- `frontend_owner_epoch` 当前使用 attempt id 作为本 attempt 内稳定且非零的 owner
  token；Task 6 production transport 接入时应复核是否需要由 FE process term
  提供更强的跨进程 epoch。QLC-2 明确禁止 reconnect/takeover，因此本任务没有
  引入续租语义。
- Task 6 必须把 production Tonic transport 的 inflight 限制落在
  `QueryLifecycleTransport` 实现中；Task 5 的并发 worker 数严格等于 frozen
  participant 数，不创建额外 fanout。

## Review fix round 1/5

本轮关闭五项 P1/P2 finding，提交信息为
`fix(frontend): harden lifecycle cleanup and metrics`。

### 1. pre-ready unwind-safe ownership

- registry bind 成功后、第一条 Init 之前立即构造 `PreReadyAttemptGuard`；guard 已持有
  完整 attempted participant、`AttemptControl` 和 registry binding。
- 只有 supervisor 成功创建并把所有权转入正式 lease 后才 disarm。Init/Attach
  期间任何 unwind、worker 外异常或 lease 构造前中断都会由 Drop 并发 unary abort
  attempted set，并释放 registry binding。
- 回归先让一个 participant 的真实 fake `InitQuery` 成功，再 deterministic panic；
  `catch_unwind` 后断言三个 attempted target 全部收到 AbortQuery，且同 execution id
  可以重新绑定 registry。

### 2. first-wins unary termination ACK

- unary fallback 仍严格校验 ACK execution identity，但接受 Backend 已锁存的任意合法
  `QueryTerminationReason`，包括 `CoordinatorStreamLost`、
  `CoordinatorHeartbeatTimeout` 和 `LocalFailure`。
- stream 与 unary acceptance 都记录实际 accepted reason，以及 query/attempt/backend
  generation/digest。
- fake 让三个 stream Abort 分别失败，再让 unary 返回三种已锁存 reason；primary
  error 保持不变，不再制造 rollback failure。

### 3. Drop cleanup failure 可观测

- cleanup failure 改为 typed participant failure，保留 target、digest、transport error
  kind 与 detail。
- 每个失败 participant 单独发布结构化 error 日志，字段包含 query id、attempt id、
  backend id、start epoch、digest、error kind/detail，并逐项增加
  `cleanup_failures` metric。
- Drop 仍 best effort，但 enriched failure 不再丢弃；存在不完整 cleanup 时额外记录
  attempt-level error。
- 回归让三个 stream 与 unary fallback 全部失败，断言三个 unary target 均被尝试且
  `cleanup_failures == 3`。

### 4. FE metrics 分类与维度

- `LocalFailure`、heartbeat timeout、control stream lost 现在是互斥分类；
  LocalFailure 不再增加 `coordinator_lost`。
- FE snapshot/Prometheus 新增：
  `init_uncertain_cleanup`、`manifest_conflicts`、`attach_failed`、
  `local_failures`、`backend_epoch_mismatches`、`cleanup_failures`。
- unknown Init retry 仍无法确定 outcome 时增加 uncertain cleanup；Attach 失败、
  frozen topology epoch mismatch、Init stale-backend rejection 和 manifest conflict
  分别进入对应维度。
- fake tests 分别断言 LocalFailure、heartbeat timeout、stream loss、unknown Init、
  attach failure、manifest conflict、epoch mismatch 和 cleanup failure 的 snapshot
  分类。

### 5. heartbeat ratio 的双层校验

- Task 5 `FrontendQueryLifecycleConfig` 与全局 config 使用同一约束：
  heartbeat timeout 必须至少为 interval 的 3 倍，并对乘法 overflow fail closed。
- RED 证明 50 ms / 100 ms 原先会被错误接受；GREEN 证明该组合被拒绝，而
  50 ms / 150 ms 合法。

### Review RED/GREEN

关键 RED：

```text
frontend_query_lifecycle_config_requires_three_heartbeat_intervals
FAILED: 50/100 must violate the 3x bound

frontend_query_lifecycle_pre_ready_guard_unwind_aborts_and_unbinds
E0432: no PreReadyAttemptGuard

frontend_query_lifecycle_unary_fallback_accepts_first_wins_terminal_reasons
FAILED: three valid first-wins reasons were appended as rollback failures

frontend_query_lifecycle_drop_cleanup_failure_is_observable
E0599: no metrics_snapshot and no cleanup failure dimension
```

最终 focused gate：

```text
cargo test -p novarocks-frontend --lib frontend_query_lifecycle -- --nocapture
test result: ok. 20 passed; 0 failed

cargo check -p novarocks-frontend
Finished `dev` profile; exit 0

cargo fmt --all -- --check
exit 0

git diff --check
exit 0
```

本轮仍未创建 production gRPC client，也未修改
`coordinator/execution.rs` 的 submit/cancel 路径；Task 6/7 stop rule 保持。
