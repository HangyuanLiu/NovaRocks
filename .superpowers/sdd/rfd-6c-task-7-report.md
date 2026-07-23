# RFD-6C Task 7 实现报告

## 状态

- 状态：完成
- 范围：production-shaped loopback 与 remote live TopN runtime-filter conformance
- 提交：`Verify live TopN runtime filters across participants`

## 实现摘要

Task 7 新增了三条端到端 conformance 路径，并只修复 harness 暴露出的生产接线问题：

1. loopback 路径直接构造 already-bound `RuntimeFilterGraph`，经过 coordinator prepare/binding、native encode/decode、`DeploymentCompiler`、install/epoch、fragment `NativeRuntimeFilterExecutionContext`、aggregate producer、OrderedBound adapter、Service loopback 和 live scan consumer；
2. remote 路径使用 3 个真实 participant 与 gRPC transport，生产者、聚合者和消费者跨 participant 分布；
3. remote transport failure 路径通过 test fixture 重写编译后 remote peer endpoint 注入不可达端点，验证 typed reliable transport fail-open，未向生产代码添加 test-only 分支。

fixture 的物理计划为 Iceberg/Parquet scan、grouped hash aggregate、gather exchange、sort 和 limit。TopN runtime filter 只使用：

- `AggregateTopNKey` producer；
- `OrderedBoundUpdate` contribution；
- `OrderedBound` artifact；
- `ProducerClosed` terminal；
- `NonBlockingLive` scan consumer。

没有生成或传输 `TopKSummary`，没有直接调用 Channel reducer，也没有通过 legacy Hub 注册、发布或获取 artifact。

## Loopback 证据

`live_topn_loopback_executes_aggregate_service_and_live_scan_chain` 覆盖：

- scan 在首个 artifact version 发布前已有进度；
- 首批候选产生 v1，更优候选产生 v2；
- 同一个 live scan consumer 观察到 v1 和 v2；
- 受 `connector_io_tasks_per_scan_operator = 1` 约束的 unopened morsel 被 late ordered pruning；
- runtime filter 开启和关闭时查询结果一致；
- Service 事件链包含 install、producer contribution、artifact availability 和 terminal；
- legacy Hub snapshot 为空。

## Remote 证据

`live_topn_remote_uses_contribution_ack_and_artifact_delivery` 覆盖：

- compiler 生成跨 participant 的 Producer -> Aggregator routing；
- 至少一个 sound remote producer 通过 RFD-4 transport 发送非零字节 `Contribution`，并在同一条 compiled route 上观察到匹配字节数的 `Sent` 与 `Accepted` ACK；
- aggregator 通过 remote artifact route 向 consumer 发送非零字节 `Artifact` / `FinalArtifact`，并在同一条 compiled route 上观察到匹配字节数的 `Sent` 与 `Accepted` ACK；
- consumer 与 loopback 使用相同实现；
- availability 可由一个 sound producer 触发，且发生在至少一个剩余 producer close 之前；
- install contract 要求 3 个 producer instance 全部 terminal；
- 两个 remote producer 的零字节 `ProducerClosed` frame 均经过 compiled route、发送并收到 `Accepted` ACK；
- aggregate owner 依次观察到 3 个 `ProducerInstanceClosed`，随后才观察到 `ChannelCompleted`；
- transport 中没有 `TopKSummary` contribution。

`live_topn_remote_timeout_fails_open_with_correct_results` 覆盖：

- 被 fixture 重写的 compiler-produced remote route 出现精确的 `FailedOpen(Deadline)`；
- scan 不因 runtime filter transport failure 阻塞；
- runtime filter 开启结果与关闭结果相同。

## TDD 证据

### RED

完整 loopback chain 首先暴露出 late file pruning 没有发生。随后用聚焦 decoder 测试固定三个生产问题：

1. native `has_null` / `all_null` 只有布尔证据，HDFS exact late pruning 需要显式 null-state 或真实 manifest count 才能安全判定；
2. native HDFS scan decoder 没有把 `QueryOptions.connector_io_tasks_per_scan_operator` 写入 `ScanNode`，测试无法可靠保留 unopened morsel；
3. native file-pruning assignment 的整数值统一编码为 8 字节，而 Iceberg `Int32` 边界必须是 4 字节。聚焦测试的 RED 为：

```text
left:  Some([10, 0, 0, 0, 0, 0, 0, 0])
right: Some([10, 0, 0, 0])
```

remote chain 的 RED 还验证了：部署 deadline 不能早于 fragment install，以及 terminal 不能仅按 ingress event 数量判断，必须结合 compiled producer cardinality、可靠 transport terminal ACK 和 aggregate-owner 生命周期顺序。

### GREEN

生产修复为：

- 将 wire `has_null` / `all_null` 保存在独立的 `IcebergFileNullState` 中，不再伪造 manifest `value_count` / `null_count`；
- 将 query option 中的 connector I/O task 数传入 native decoded `ScanNode`；
- 使用 `ScanNode.table.columns` 的权威 Arrow type 恢复 integer/float pruning bound 宽度；
- type 缺失、不兼容、越界、Int8 或 Float32 非精确收窄时省略可选 pruning stats，使文件保持 `Keep`，不猜测 wire payload 宽度；
- ordered producer 从 Pending 进入 Satisfied 时补发 `ProducerInstanceClosed`，并保证该事件早于同一 action 中的 `ChannelCompleted`。

## Review 修复

controller review 的 3 条 P1 与 2 条 P2 均以聚焦 RED/GREEN 闭环：

1. P1 ACK 证据：旧断言可能由零字节 terminal ACK 满足。新增 route/kind/nonzero-bytes 相关 helper，分别证明 `Contribution` 与 `Artifact` / `FinalArtifact` 的非零 frame 生命周期；
2. P1 成功顺序：旧断言只证明 availability 与 completion 各自存在。新增严格顺序断言，要求 availability 后仍有 producer close，3 个 `ProducerInstanceClosed` 全部位于 `ChannelCompleted` 前；
3. P1 失败证据：旧断言接受任意 typed terminal。现在只接受 fixture 实际重写的 compiler-produced route 上的 `FailedOpen(Deadline)`；
4. P2 null-state：删除由布尔值伪造的 `(value_count, null_count)`，以独立枚举携带 `NoNulls` / `HasNulls` / `AllNull`；真实 count 不完整时保持 `Keep`；
5. P2 Int8：shared untyped pruning decoder 会把单字节整数解释为 `u8`，因此在 typed consumption 支持前省略 Int8 file bounds，负值测试固定该边界。

因果顺序 RED 进一步暴露出 production observability 缺口：direct ordered / TopK 路径的 `refresh_ordered_instance_progress` 会把实例标记为 Satisfied，却没有生成 membership 路径已有的 `ProducerInstanceClosed`。修复后四个 ordered progress 入口都在 transition 时生成该事件，`refresh_after_ordered_progress` 再追加 completion。

### Remaining P1：未知 null count 的 wire 语义

re-review 指出的两条编码路径都曾用 `null_count.unwrap_or(0)` 生成 `has_null = false`，把 Iceberg manifest 中缺失的 null count 错误编码成显式 `NoNulls`。修复后：

- native protobuf encoder 与 Thrift encoder 都要求真实 `null_count`；缺失时省略该列的可选 file-pruning 元数据；
- 不再从缺失值推导 `NoNulls`，HDFS late pruning 因证据不足保守返回 `Keep`；
- native 回归经过真实 scan-range planner、instance protobuf encode、native decode、morsel 构造和 HDFS ordered late prune；
- Thrift 回归经过真实 Iceberg metadata encoder、`THdfsScanRange` decoder、morsel 构造和 HDFS ordered late prune。

两条回归在修复前均稳定得到：

```text
left: Skip
right: Keep
```

修复后均通过。

## 最终验证

```text
cargo test -p novarocks native_missing_null_count_keeps_ordered_late_pruning_conservative_after_wire_roundtrip --lib
PASS: 1 passed, 0 failed

cargo test -p novarocks --features compat thrift_missing_null_count_keeps_ordered_late_pruning_conservative_after_wire_roundtrip --lib
PASS: 1 passed, 0 failed

cargo test -p novarocks lowers_iceberg_data_file_scan --lib
PASS: 4 passed, 0 failed

cargo test -p novarocks native_scan_ordered_live_hdfs_skips_only_exact_file_range_evidence --lib
PASS: 1 passed, 0 failed

cargo test -p novarocks omits_negative_int8_file_pruning_bounds --lib
PASS: 1 passed, 0 failed

cargo test -p novarocks ordered_close_emits_instance_closed_before_channel_completion --lib
PASS: 1 passed, 0 failed

cargo test -p novarocks live_topn_loopback --lib
PASS: 1 passed, 0 failed

cargo test -p novarocks live_topn_remote --lib
PASS: 5 passed, 0 failed

cargo test -p novarocks runtime_filter::service::live_deployment_conformance_tests --lib
PASS: 10 passed, 0 failed
```

提交前还执行：

```text
cargo fmt --all -- --check
git diff --check
```

## 变更文件

- `novarocks/core/src/runtime_filter/service/live_deployment_conformance_tests.rs`
- `novarocks/core/src/runtime_filter/core/channel.rs`
- `novarocks/core/src/connector/hdfs.rs`
- `novarocks/core/src/connector/iceberg/file_pruning.rs`
- `novarocks/core/src/connector/iceberg/file_pruning_wire.rs`
- `novarocks/core/src/fs/scan_context.rs`
- `novarocks/core/src/protocol/native/decode/scan/file_range.rs`
- `novarocks/core/src/protocol/native/decode/scan/iceberg_data.rs`
- `novarocks/core/src/protocol/native/decode/scan/mod.rs`
- `.superpowers/sdd/rfd-6c-task-7-report.md`

## 边界与后续

- 未增加 ordinary SQL planner runtime-filter generation；
- 未增加 local/remote bool、Hub fallback、producer payload limit 或 operator-owned RPC；
- graph 在 fixture 中按 Task 7 要求直接构造，再走生产 prepare/native/compiler/install/execution 链；
- ordinary SQL 自动生成该 graph 以及 1FE+3BE SQL-level 验收属于后续 B4 / RFD-8，不在 Task 7 范围内。
