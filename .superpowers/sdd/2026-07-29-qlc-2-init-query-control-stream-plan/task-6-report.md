# QLC-2 Task 6 实施报告：production gRPC lifecycle client

## 结论

Task 6 已完成。Frontend 与 Backend 现在可以通过真实 generated Tonic service
完成 `InitQuery`、`QueryControlStream` attach、`ControlReady`、heartbeat、
Finalize 与 unary Abort；production transport 冻结并严格校验同一 live snapshot
中的 backend id、endpoint 与 start epoch。

本任务没有把 production query execution 强制切到 lifecycle barrier，也没有实现
reconnect、takeover 或隐藏重试；这些边界继续由 Task 7 负责。

## 实现内容

### production unary 与 target binding

- 将 Task 5 的 `QueryLifecycleTransport`、`QueryControlSession`、target 与 typed
  transport error 下沉到 Core lifecycle contract，Frontend 只复用中立接口。
- 新增 `new_grpc_query_lifecycle_transport`，拒绝空 topology 与重复 backend id，
  并为每个 frozen target 创建 `NovaRocksGrpcRemoteClient`。
- Init/Abort 使用 global async data runtime 与调用方 deadline；adapter 本身不重试。
  request/response decode、unknown enum、execution id 或 digest 不一致均返回
  `InvalidResponse`。
- channel acquisition timeout/failure 与 RPC submit 前已耗尽的 deadline 归类为
  definite `Unavailable`，不会触发 unknown-outcome retry。Init 已提交后收到
  timeout、`Unavailable`、`Cancelled` 或 `Unknown` status 时才归类为
  `DeadlineExceeded`/`StreamClosed` unknown outcome，让 Task 5 只以完全相同
  request/digest 决定一次幂等重试。

### bounded bidirectional session bridge

- command/event channel 均固定容量 32；有界 pending-command FIFO 确保 server
  暂停读取或 HTTP/2 预取时，第 33 条未 ACK command 仍确定返回 typed
  `Backpressure`。只有与 FIFO front 类型和 heartbeat sequence 精确匹配的 ACK
  才核销一个 permit。
- bridge 在 global data runtime 上按序 decode response；重复、unsolicited、
  wrong-sequence 或 wrong-terminal ACK 返回 `InvalidResponse`，不释放 capacity。
  LocalFailure、TerminationAccepted、wire error 或 stream reset first-wins 保存
  typed terminal error，并在 event 对调用者可见前先关闭 command side。
- `recv_timeout` 区分本地 deadline 与 event channel/remote stream closure；
  session Drop 关闭 command sender 并 abort owned join handle，不留下 detached task。
- attach timeout 只限制 channel acquisition 与 stream 建立，不把短期 attach deadline
  写成长期 bidi stream 的 gRPC timeout header。

### BackendServicesSource

- `QueryBackendServices` 只包含 scheduler、fragment dispatcher 与 lifecycle
  transport；三者由一次 immutable live topology snapshot 构建。
- Fixed/Sequence test constructors 现在由调用者显式注入 lifecycle fake。
- Task 6 Gate 与“删除旧 RF owner”存在计划中间态约束：Task 7 前旧 production
  execution 仍需 runtime-filter deployment。为避免第二次 topology snapshot，
  coordinator 只保留一个明确的 legacy test override；production legacy adapter
  临时从本次 scheduler 的 frozen backend entries 派生，并用注释标明 Task 7 删除。

## TDD 与真实服务证据

RED 阶段先添加 production factory、loopback、backpressure/close 测试；Core filter
按预期报告 factory/module 尚不存在，同时也暴露了当前树既有的 Iceberg optimize
测试 API 漂移。

GREEN 覆盖：

- generated Tonic server 上真实 Init -> Attach -> ControlReady -> HeartbeatAck ->
  Finalize；
- 同一 generated service 上真实 unary Abort；
- server 暂停 heartbeat 时 32 条 pending command 可发送，第 33 条返回
  `Backpressure`；
- server reset 后 `recv_timeout` 返回 `StreamClosed`，不伪造 timeout；
- backend id、endpoint、start epoch 与 digest 未被 adapter 改写；
- post-submission Init status 的 unknown-outcome 分类；
- 一次 snapshot 同时构建 scheduler、dispatcher 与 lifecycle transport。

## 最终验证

```text
cargo test -p novarocks-frontend --lib frontend_query_lifecycle_live_transport -- --nocapture
test result: ok. 3 passed; 0 failed

cargo test -p novarocks-frontend --lib frontend_query_lifecycle -- --nocapture
test result: ok. 23 passed; 0 failed

cargo test -p novarocks-frontend --lib -- --nocapture
test result: ok. 118 passed; 0 failed

cargo check -p novarocks
exit 0

cargo check -p novarocks-frontend
exit 0

cargo check -p novarocks --features compat
exit 0

cargo fmt --all -- --check
exit 0

git diff --check
exit 0
```

计划中的 Core live-test 命令：

```text
cargo test -p novarocks grpc_query_lifecycle_client --lib -- --nocapture
```

仍在编译测试 crate 时被 Task 6 之前已存在的 7 个 Iceberg optimize job API
错误阻塞：缺少 `IcebergOptimizeJobOutcome`、
`CreateIcebergOptimizeJobRequest`，以及 `JobMetaRepository` 的
create/claim/record/finish optimize job 方法。该次编译没有 Task 6 新错误。
相同的 production client/server 行为已在可编译的 Frontend lib target 上通过真实
generated service 验证。

## Fresh review

Fresh review 首轮发现并已关闭：

1. Init post-submission transport status 必须保留 unknown-outcome 语义；
2. Fixed/Sequence 必须允许调用方显式注入 lifecycle fake；
3. 真实服务测试必须覆盖 unary Abort；
4. backend-service bundle 不应继续把独立 RF dispatcher 当作第四项。

最终结构没有越过 Task 7：production barrier、fragment execution id/admission、
QLC-1A cancellation cutover与旧 RF owner 删除均未实现。

## 剩余关注

- Core unit filter 需要基线 Iceberg optimize 测试 API 修复后才能直接运行本文件内
  的 Core tests；目前由 Frontend generated-service 测试提供等价 live 证据。
- Task 7 必须删除 `legacy_runtime_filter_dispatcher_override` 与临时派生逻辑，
  并让 production submit 强制消费本任务注入的 lifecycle transport。

## Review fix round 1/5

本轮关闭三个并发与 transport classification finding，且没有修改 Task 7 的
submission/cancellation cutover。

### Terminal publish 的线性化

- `QueryControlCommandState` 在一个 mutex 内同时拥有 sender、pending FIFO 和
  first-wins terminal error。
- bridge 对 LocalFailure、TerminationAccepted、wire/status error 先在锁内 drop
  sender 并保存 terminal error，再释放锁并向 event channel publish。因此 caller
  一旦观察到 terminal，后续 `send` 必然返回保存的 typed terminal error。
- Core 确定性单元回归直接调用 terminal preparation 边界，并在任何 event
  publication 之前断言 sender 已关闭且保存了 `StreamClosed`；Frontend live
  回归再从公开 session API 断言观察到 terminal 后不可继续 send。
- mutex 不跨 event channel 的 async send；Drop 同样先关闭 command state，再
  abort owned bridge task。

### command-specific pending permit

- scalar inflight counter 改为容量 32 的 typed FIFO：
  `Heartbeat(sequence)`、`Abort`、`Finalize`。
- command 只有 wire `try_send` 成功后才入 FIFO；ACK 只有匹配 FIFO front 才 pop。
  一条合法 HeartbeatAck 只释放一个 slot。
- duplicate、unsolicited、wrong-sequence、wrong-order ACK 都变成 terminal
  `InvalidResponse`，不释放任何 permit。Finalize 只接受
  `CoordinatorFinalize`；Abort 保留 Task 5 first-wins termination reason 语义。

### pre/post submission deadline

- `QueryLifecycleRpcError` 明确拆成 `PreSubmission`、
  `PostSubmissionDeadlineExceeded` 与 `PostSubmissionStatus`。
- channel acquisition timeout/failure 和 submit 前 deadline 耗尽是 definite
  `Unavailable`，`is_unknown_init_outcome == false`。
- 已提交 Init 的本地/remote timeout，以及 post-submit
  `Unavailable`/`Cancelled`/`Unknown` 是 retryable unknown outcome。

### RED/GREEN

真实 generated-service RED：

```text
frontend_query_lifecycle_live_transport_ack_releases_only_its_pending_command
FAILED: duplicate ACK was returned as HeartbeatAck

frontend_query_lifecycle_live_transport_rejects_mismatched_terminal_ack
FAILED: Finalize accepted CoordinatorAbort

frontend_query_lifecycle_live_transport_pre_submission_timeout_is_definite
FAILED: got DeadlineExceeded, expected Unavailable
```

GREEN 与最终验证：

```text
cargo test -p novarocks-frontend --lib frontend_query_lifecycle_live_transport -- --nocapture
test result: ok. 8 passed; 0 failed

cargo test -p novarocks-frontend --lib -- --nocapture
test result: ok. 123 passed; 0 failed

cargo check -p novarocks
exit 0

cargo check -p novarocks-frontend
exit 0

cargo check -p novarocks --features compat
exit 0
```

Core filtered test target 仍被同一组 7 个既有 Iceberg optimize API 编译错误阻塞；
没有 Task 6 新错误。独立 fix-round review 未发现 Critical、Important 或 Minor，
并确认 terminal close 不持锁跨 await、FIFO capacity 严格为 32、Abort first-wins
语义保留、Drop 无 detached task。
