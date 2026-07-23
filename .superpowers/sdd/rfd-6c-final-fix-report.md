# RFD-6C 最终 review 修复报告

## 状态

- 状态：完成
- 分支：`codex/rfd-6c-topn-service-adapter`
- 基线提交：`69b52e4ba`（`Adapt RFD-6C tests after fragment binding rebase`）
- 修复提交主题：`Harden Aggregate TopN and Iceberg pruning boundaries`
- 推送目标：`fork/codex/rfd-6c-topn-service-adapter`
- 未修改 vault、spec、plan、umbrella、roadmap 或 PR metadata。

## 修复 1：Boolean Aggregate TopN fail-fast

根因是通用 `RuntimeOrderContract` 接受 `Boolean`，但 aggregate-owned TopN
boundary extractor 只实现以下单 key 类型：

```text
Int8 / Int16 / Int32 / Int64
Utf8
Date32
Timestamp
Decimal128
FixedSizeBinary(LARGEINT_BYTE_WIDTH)
```

此前 boundary constructor 只校验 key arity，因此 typed Boolean plan 可以通过
factory/build，直到第一组输入被观察时才失败。

本次修复：

- 增加唯一的 boundary contract validator，精确白名单与 extractor 保持一致；
- `AggregateTopNBoundaryState::try_new` 复用该 validator，作为防御性边界；
- normal aggregate 与 streaming aggregate 两个 native factory 在创建 Service session
  前校验全部 TopN producer spec；
- 不扩大通用 ordered contract 的限制，也不改变现有支持类型。

TDD RED：

```text
topn_boundary_constructor_rejects_ordered_boolean_before_observation
RED: constructor unexpectedly returned Ok

native_aggregate_topn_boolean_target_fails_at_factory_build_before_first_input
RED: pipeline graph unexpectedly built successfully
```

GREEN：

```text
cargo test -p novarocks topn_boundary --lib
15 passed

cargo test -p novarocks \
  native_aggregate_topn_boolean_target_fails_at_factory_build_before_first_input --lib
1 passed

cargo test -p novarocks aggregate_topn_producer --lib
12 passed
```

`topn_boundary_supports_all_frozen_single_key_scalar_types` 继续覆盖并通过全部已支持类型。

## 修复 2：native Iceberg invalid null-count wire stats

根因是 native Iceberg wire encoder 只要求 `null_count` 存在，然后以
`null_count > 0` 生成 null-state；因此负数会被错误编码为 `NoNulls`，
`null_count > value_count` 也会生成不可信 metadata。HDFS 精确 ordered pruning
路径本身已经拒绝这些计数，但 native wire 编码破坏了该保守边界。

本次修复要求 `value_count` 与 `null_count` 同时存在，并在任一为负数或
`null_count > value_count` 时完全省略该列 pruning metadata；没有修复、夹取或猜测
统计值。

新增测试经过真实 native encode/decode wire roundtrip，再 materialize HDFS scan op，
证明：

- missing `null_count`：`Keep`；
- negative `null_count`：`Keep`；
- `null_count > value_count`：`Keep`。

TDD RED：

```text
native_negative_null_count_keeps_ordered_late_pruning_conservative_after_wire_roundtrip
RED: Skip != Keep

native_null_count_exceeding_value_count_keeps_ordered_late_pruning_conservative_after_wire_roundtrip
RED: Skip != Keep
```

GREEN：

```text
cargo test -p novarocks \
  keeps_ordered_late_pruning_conservative_after_wire_roundtrip --lib
3 passed

cargo test -p novarocks \
  native_scan_ordered_live_hdfs_skips_only_exact_file_range_evidence --lib
1 passed
```

## Loopback / remote / conformance

```text
cargo test -p novarocks live_topn_loopback --lib
1 passed

cargo test -p novarocks live_topn_remote --lib
5 passed

cargo test -p novarocks \
  runtime_filter::service::live_deployment_conformance_tests --lib
10 passed

cargo test -p novarocks \
  live_topn_remote_uses_contribution_ack_and_artifact_delivery --lib
1 passed
```

## Serial broad filters 与 baseline audit

Native broad：

```text
cargo test -p novarocks protocol::native --lib -- --test-threads=1
318 passed, 1 failed
```

唯一失败仍是已知 baseline fixture：

```text
protocol::native::runtime_filter_install::tests::
runtime_filter_install_round_trips_direct_aggregate_and_relay
```

它仍使用旧 producer-route allowed kinds
`{Contribution, ProducerClosed, Unavailable}`，而 canonical route 要求
`ProducerUnavailable`。相较先前 `316 passed, 1 failed`，新增两个 invalid-count
wire roundtrip 测试均通过。

Service broad 连续两次均为：

```text
cargo test -p novarocks runtime_filter::service --lib -- --test-threads=1
363 passed, 24 failed, 2 ignored
```

其中 23 个是已知的 stale `consumer_ingress` allowed-kinds fixture；第 24 个在两次运行中
分别表现为 loopback 或 remote conformance 生命周期证据的时序波动。为排除本任务状态泄漏，
完成了以下审计：

```text
# 跳过 23 个 stale consumer-ingress fixture
cargo test -p novarocks runtime_filter::service --lib -- \
  --test-threads=1 --skip runtime_filter::service::consumer_ingress
353 passed, 0 failed, 2 ignored

# 改动前 69b52e4ba 临时 worktree，同一串行 broad 命令
363 passed, 24 failed, 2 ignored
```

改动前基线的第 24 个失败同样是
`live_topn_remote_uses_contribution_ack_and_artifact_delivery`。本次改动只加入纯
contract/stats 校验，不接触 Service registry、线程或 lifecycle event；stale fixture
使用的局部 Service 在 panic 展开时也通过 `Drop::shutdown` 清理。因此该第 24 个波动失败
不是本次修复引入的问题。

## 变更文件

- `novarocks/core/src/exec/operators/aggregate/topn_boundary.rs`
- `novarocks/core/src/exec/operators/aggregate/mod.rs`
- `novarocks/core/src/exec/operators/aggregate/streaming_sink.rs`
- `novarocks/core/src/exec/pipeline/builder.rs`
- `novarocks/core/src/connector/iceberg/scan_range.rs`
- `novarocks/core/src/protocol/native/decode/scan/mod.rs`

## Blockers

无 RFD-6C 最终修复 blocker。上述 23 个 Service fixture、1 个 native fixture 与 Service
conformance broad-filter 时序波动均已证明属于改动前基线，本任务不越界修改。
