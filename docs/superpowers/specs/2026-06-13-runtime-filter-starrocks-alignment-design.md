# Runtime Filter StarRocks 对齐设计

- 日期: 2026-06-13
- 状态: 草案，待评审后执行
- 范围标签: runtime-filter, optimizer, distributed-execution, starrocks-alignment

## 1. 背景与问题

NovaRocks standalone 已经具备 runtime filter 的主链路：

```text
runtime_filter_pass
  -> fragment_builder lower RuntimeFilterDesc / RuntimeFilterProbe
  -> ExecutionCoordinator / FragmentScheduler 填 TRuntimeFilterParams
  -> HashJoinBuildSink 生成 local / remote filter
  -> RuntimeFilterWorker merge partial filters
  -> RuntimeFilterHub 在 scan / exchange probe 侧等待和应用
```

用户本次目标是把这条链路对齐 StarRocks，并且重点修复分布式下的问题。验收环境必须是
NovaRocks standalone 独立分布式部署：

```text
1 NovaRocks FE process + 3 NovaRocks BE processes
```

不使用 `starrocks-fe-on-novarocks`，不依赖 StarRocks FE 生成 plan。

当前主要风险集中在两类：

1. optimizer probe placement 与 StarRocks `ExchangeNode.canCrossExchangeNode` 不完全一致，可能导致可安全下推的
   partitioned runtime filter 停在 exchange 边界，尤其影响 TPC-DS q72 这类多 join / 多 fragment 查询。
2. distributed runtime filter coordinator 参数与 StarRocks `DefaultCoordinator.setGlobalRuntimeFilterParams`
   语义不完全一致，尤其是 broadcast filter 与 partitioned filter 的 builder 数量不同，算错会导致 merge
   node 过早或过晚广播，进而带来 probe 等待超时、filter 缺失或性能退化。

本文只定义目标语义和验收标准；具体实现按
`docs/superpowers/plans/2026-06-13-runtime-filter-starrocks-alignment.md` 执行。

## 2. StarRocks 对齐目标

### 2.1 Join 侧 RF 生成

NovaRocks standalone optimizer 生成 RF 时应遵循 StarRocks 的安全边界：

- 只为可安全过滤 probe side 的 hash join equi-condition 生成 RF。
- outer / anti / null-preserving 语义边界不能把 RF 推到会改变结果的位置。
- build/probe size、selectivity、RF 数量上限仍通过 NovaRocks `OptimizerOptions` 控制，但默认行为要与
  StarRocks 的 conservative gating 等价：小 build、大 probe、有效过滤收益时生成。
- 不支持或表达不完整的 join key 类型必须 fail safe：不生成 RF，而不是生成会错误过滤的 RF。

### 2.2 Exchange crossing

NovaRocks probe RF 跨 `PhysicalDistribution` 的规则对齐 StarRocks `ExchangeNode.canCrossExchangeNode`：

- broadcast RF 可以跨 exchange。
- 单 equi-condition RF 可以跨 exchange。
- 多 equi-condition 的 partitioned RF 只有在 probe expr 等于唯一 hash partition column 时可以跨 exchange。
- 多列 global runtime filter 暂不启用；当 exchange hash partition expr 有多列时，不跨 exchange。
- `allow_cross_exchange_rf = false` 时，任何 RF 都不跨 exchange。
- RF 不能因为跨 exchange 而跳过 projection、outer join、anti join、null-aware anti join 等语义边界。

### 2.3 Fragment / thrift descriptor

每个 lowered runtime filter descriptor 必须完整携带执行语义：

- `filter_id`
- `build_expr`
- `plan_node_id_to_target_expr`
- `has_remote_targets`
- `build_join_mode`
- `layout`
- `runtime_filter_merge_nodes`，当存在 remote targets 时由 coordinator 注入

`build_join_mode` 是 distributed runtime filter 参数计算的权威来源。不能从 fragment 数量、scan 数量或
join 节点位置反推 join mode。

### 2.4 Distributed coordinator 参数

`TRuntimeFilterParams.runtime_filter_builder_number` 必须按 join mode 计算：

- broadcast RF：builder number = 1。每个 builder 产生的 filter 已是完整 build-side filter；merge node 收到
  一个即可广播 final filter。
- partitioned RF：builder number = build fragment instance count。merge node 必须等齐每个 build instance 的
  partial filter 后再广播 final filter。
- colocate / bucket shuffle：在 NovaRocks 当前执行模型中按 build fragment instance count 处理，后续如果引入
  bucket layout mapping，再单独细化。

所有 probe fragment instance 都必须收到 `TRuntimeFilterProberParams`。remote final filter 到达 query context
之前可以进入 pending 队列；query context 创建后必须 replay，不能丢失。

### 2.5 Runtime delivery

远端 RF 发送路径的最终语义：

```text
HashJoinBuildSink
  -> local RuntimeFilterHub publish for same-fragment probes
  -> remote partial filter to merge node when has_remote_targets
  -> RuntimeFilterWorker waits expected builder number
  -> merge_and_encode_filters
  -> broadcast final filter to all probe backend hosts
  -> RuntimeFilterHub receive_remote_filter
```

要求：

- partitioned RF 缺失 merge node 时 fail safe：记录 warning 并不发送错误 final filter。
- duplicate partial filter 以 `build_be_number` 去重。
- merge node 完成后后续 duplicate / late partial 不应再次广播。
- probe 侧等待必须受 timeout 控制，不能形成分布式死等。

## 3. 非目标

- 不实现 StarRocks 多列 global runtime filter session switch；当前明确保持关闭。
- 不在本任务实现 topN runtime filter、min/max sort filter 或 aggregate runtime filter。
- 不修改 SQL golden，除非后续用户明确要求。
- 不把 StarRocks FE-compatible BE 模式作为本次验收路径；本次验收只看 NovaRocks standalone 1FE+3BE。
- 不为 runtime filter 性能做 benchmark 结论；本次只验证 plan shape、正确性和无分布式等待问题。

## 4. 验收标准

### 4.1 Rust 单元测试

必须新增或更新以下测试：

- `sql::optimizer::runtime_filter_pass::tests::partitioned_rf_crosses_hash_exchange_for_single_key_when_flag_enabled`
- `sql::optimizer::runtime_filter_pass::tests::partitioned_rf_crosses_exchange_only_for_matching_partition_key`
- `runtime::coordinator::tests::runtime_filter_builder_number_follows_join_distribution`
- `runtime::runtime_filter_worker::tests::expected_builders_defaults_to_one_and_respects_params`

运行命令：

```bash
cargo test -q sql::optimizer::runtime_filter_pass::tests::
cargo test -q runtime::coordinator::tests::runtime_filter_builder_number_follows_join_distribution
cargo test -q runtime::runtime_filter_worker::tests::expected_builders_defaults_to_one_and_respects_params
```

### 4.2 独立 1FE+3BE SQL 验收

使用 SQL runner 的 cross-process 模式启动独立 NovaRocks FE/BE：

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo build --profile dev-opt

NO_PROXY=127.0.0.1,localhost \
NOVAROCKS_BIN="$PWD/target/dev-opt/novarocks" \
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --profile dev-opt -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q72 \
  --mode verify \
  --query-timeout 180 \
  --cluster-mode cross-process \
  --cluster-size 3 \
  --fail-fast \
  -j 1
```

然后按 fast-fail 跑 benchmark SQL suites：

```bash
for suite in ssb tpc-h tpc-ds; do
  NO_PROXY=127.0.0.1,localhost \
  NOVAROCKS_BIN="$PWD/target/dev-opt/novarocks" \
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --profile dev-opt -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite "$suite" \
    --mode verify \
    --query-timeout 180 \
    --cluster-mode cross-process \
    --cluster-size 3 \
    --fail-fast \
    -j 1
done
```

验收通过条件：

- cross-process runner 日志明确出现 3 个 BE 和 1 个 FE：
  - `started cross-process BE[0]`
  - `started cross-process BE[1]`
  - `started cross-process BE[2]`
  - `started cross-process FE`
- `tpc-ds q72` verify 通过。
- `ssb`、`tpc-h`、`tpc-ds` 在 1FE+3BE 下 verify 通过，遇到首个失败即停止排查。
- 测试结束后没有 runner 本次启动的残留 NovaRocks FE/BE 进程。

## 5. 当前工作区备注

本文生成时，当前工作区已经存在一次探索性 runtime filter diff。正式执行前需要由用户确认：

- 继续沿用该 diff 并按 plan 补齐剩余测试；
- 或先回滚探索性 diff，再严格按本文 spec/plan 从红测开始重做。

无论选择哪种方式，最终验收都必须以本 spec 的 1FE+3BE cross-process 命令为准。
