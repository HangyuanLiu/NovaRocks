# NovaRocks TPC Plan 差距与执行热点分析

Artifact 根目录：`/Users/harbor/.codex/worktrees/4f87/NovaRocks/logs/baseline/20260622-231416-tpc-baseline`

## 数据来源与边界

- 当前 NovaRocks 基线：`1 FE + 3 BE`，`dev-opt` binary，`EXPLAIN ANALYZE`，SSB / TPC-H / TPC-DS 全部通过。
- 当前 NovaRocks plan 与执行时间来自：`plans/analyze/{ssb,tpc-h,tpc-ds}/*.result`。
- StarRocks FE plan 对比来自历史采集：`/Users/harbor/project/NovaRocks/logs/plan-compare/20260613-201518`。
- 这份 StarRocks FE plan 对比没有在本轮重新采集，因此适合作为 plan shape 参照，不适合作为严格的“当前代码同一时刻”对照。
- `act={time=...}` 是当前 `EXPLAIN ANALYZE` 输出里的节点运行时计数器，适合找算子热点，但不能完整解释端到端耗时；exchange wait、driver 调度、fragment 同步等开销现在还没有完整归属到单个节点。

## 总结判断

SSB 的 plan 基本与 StarRocks FE 对齐，join / scan / aggregate / topn 数量一致，慢节点也都在 500ms 内。SSB 当前不是优先优化对象。

TPC-H 分成两类问题：`q18` 是明确的执行层热点，两个 partitioned hash join 分别耗时 44.8s 和 32.0s；`q9` 是更明显的 plan shape 差距，当前 NovaRocks 把 StarRocks FE 的部分 partitioned join 变成了 broadcast join，并带来 1.7GB 的 broadcast join 峰值内存。

TPC-DS 是系统性 plan 差距最明显的 suite。当前 NovaRocks 相比 StarRocks FE 倾向于更多 broadcast join、更少 partitioned join / shuffle exchange，并且 runtime filter probe 数量明显偏少。这类差距集中出现在 `q64`、`q23`、`q14`、`q4`、`q57`、`q47` 等 case。

## Suite 级 Plan 差距

| Suite | StarRocks FE broadcast join | NovaRocks broadcast join | StarRocks FE partitioned join | NovaRocks partitioned join | StarRocks FE shuffle exchange | NovaRocks shuffle exchange | StarRocks FE RF probe | NovaRocks RF probe |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SSB | 36 | 36 | 0 | 0 | 20 | 10 | 36 | 36 |
| TPC-H | 38 | 43 | 20 | 19 | 68 | 49 | 89 | 34 |
| TPC-DS | 484 | 574 | 140 | 71 | 564 | 275 | 947 | 476 |

需要谨慎看待 exchange 的绝对计数，因为 StarRocks FE 和当前 NovaRocks 的 formatter 对 broadcast / gather / hash exchange 的文本表达并不完全一致；更稳定的信号是 join distribution 和 runtime filter probe 的组合趋势。

## 慢 Case 与 Plan 差距

| Suite | Case | Elapsed | 主要 plan 差距 |
|---|---|---:|---|
| TPC-H | q18 | 91.26s | RF probe 少 4，RF build 少 1；join distribution 不像 q9 那样明显偏 broadcast |
| TPC-DS | q14.s1 | 39.85s | shuffle 少 2，RF probe 少 12，RF build 少 3，aggregate 少 3 |
| TPC-DS | q14.s2 | 39.85s | RF probe 少 14，RF build 少 5，aggregate 少 2 |
| TPC-DS | q23.s1 | 32.14s | broadcast join 多 2，partitioned join 少 1，shuffle 少 8，RF probe 少 2 |
| TPC-DS | q23.s2 | 32.14s | broadcast join 多 3，partitioned join 少 3，shuffle 少 11，RF probe 少 7 |
| TPC-DS | q64 | 31.64s | broadcast join 多 2，partitioned join 少 2，shuffle 少 7，RF probe 少 33 |
| TPC-H | q9 | 24.40s | broadcast join 多 2，partitioned join 少 2，shuffle 少 5，RF probe 少 9 |
| TPC-DS | q4 | 9.15s | broadcast join 多 8，partitioned join 少 8，shuffle 少 20，RF probe 少 25 |

`q64` 是 TPC-DS 中最适合作为 plan 对齐样例的 case：StarRocks FE 顶层 `i_item_sk / store_name / store_zip` join 是 partitioned join，并在多个输入侧保留 shuffle 与 runtime filter probe；当前 NovaRocks 顶层是 broadcast join，且底层 store_sales 分支也连续使用多个 broadcast join，其中 `ss_customer_sk = c_customer_sk` 节点峰值内存达到 1.4GB。

`q9` 是 TPC-H 中最适合作为 join distribution 对齐样例的 case：StarRocks FE 是 3 个 broadcast join + 2 个 partitioned join，当前 NovaRocks 是 5 个 broadcast join + 0 个 partitioned join。当前 `o_orderkey = l_orderkey` broadcast join 峰值内存达到 1.7GB。

`q18` 不应简单归因于“broadcast 过多”。当前两个最慢节点都是 partitioned hash join，说明更可能是执行层 hash join、runtime filter 生效、exchange fan-in 或 driver 调度问题。

## 执行层慢节点

| Suite | Case | Node | Operator | Time | Rows | Peak | 判断 |
|---|---|---:|---|---:|---:|---:|---|
| TPC-H | q18 | 9 | HASH JOIN PARTITIONED INNER `l_orderkey = o_orderkey` | 44.8s | 13,502,430 | 607.7MB | 第一优先级执行层热点 |
| TPC-H | q18 | 18 | HASH JOIN PARTITIONED LEFT SEMI `o_orderkey = lineitem.l_orderkey` | 32.0s | 6,001,287 | 82.5KB | 第一优先级执行层热点 |
| TPC-H | q18 | 7 | HASH JOIN PARTITIONED INNER `o_custkey = c_custkey` | 3.2s | 3,150,000 | 57.6MB | 次级 join 热点 |
| TPC-H | q9 | 8 | HASH JOIN BROADCAST `l_suppkey/l_partkey = ps_suppkey/ps_partkey` | 2.0s | 455,759 | 85.1MB | 受 plan distribution 影响 |
| TPC-H | q9 | 17 | PROJECT amount 表达式 | 1.8s | 325,847 | 13.0KB | 表达式执行热点 |
| TPC-H | q9 | 10 | HASH JOIN BROADCAST `o_orderkey = l_orderkey` | 1.4s | 1,303,388 | 1.7GB | broadcast 内存风险 |
| TPC-DS | q64 | 24 | HASH JOIN BROADCAST `c_first_shipto_date_sk = d3.d_date_sk` | 2.0s | 312,601 | 53.1MB | q64 可见 CPU 热点 |
| TPC-DS | q64 | 26 | HASH JOIN BROADCAST `ss_customer_sk = c_customer_sk` | 909.8ms | 701,577 | 1.4GB | broadcast 内存风险 |
| TPC-DS | q75 | 27 | UNION ALL | 2.1s | 819,316 | 6.1MB | union/project pipeline 开销 |
| TPC-DS | q75 | 25 | PROJECT sales_cnt / sales_amt | 2.1s | 269,449 | 12.3MB | 表达式执行热点 |
| TPC-DS | q81 | 49 | NEST LOOP JOIN CROSS | 1.8s | 67,906,697 | 1018.8KB | cross join 算子热点 |

TPC-DS 的端到端最慢 case 和单节点 `act` 最慢节点并不完全重合。`q14`、`q23`、`q64` 的 case wall time 是 31s 到 40s，但单个节点 `act` 最大通常只有 1s 到 2s。这说明当前 profile 还缺少足够的 wait / exchange / schedule 归因，不能只沿着单节点 CPU 时间优化。

## 优化方向

### P0：TPC-H q18 的 partitioned hash join 执行层优化

`q18` 的 44.8s 和 32.0s 都落在 partitioned hash join，应该优先检查：

- hash join build/probe 的向量化路径、hash table layout、null/equality check 热点。
- partitioned exchange 后的数据分布是否倾斜，是否有单 driver 或单 fragment 长尾。
- runtime filter 是否被正确下发并在 scan / exchange 侧生效；当前 plan 对比显示 RF probe 数量比 StarRocks FE 少。
- 大输入 join 是否存在过多 row materialization、projection copy 或 chunk 重组。

### P0：TPC-DS q64 的 plan distribution 与 runtime filter 对齐

`q64` 同时具备端到端慢、plan 差距大、broadcast join 峰值内存高三个特征。建议把它作为 TPC-DS 第一条 plan 对齐样例：

- 对齐 StarRocks FE 顶层 partitioned join，而不是在大结果侧继续 broadcast。
- 检查 join reorder / distribution selection 的 cost 估计，尤其是 `store_sales`、`customer`、`catalog_sales` 分支的 cardinality。
- 检查 runtime filter 经过 exchange、project、aggregate 后是否还能保留 probe 侧表达。

### P1：TPC-H q9 的 join distribution 与 cardinality 估计

`q9` 当前 plan 中 `stats={rows=41364136 conf=fallback}` 出现在上层 broadcast join，StarRocks FE 对应 plan 的最大基数低很多。这里更像统计信息或 fallback cardinality 影响了 distribution 选择：

- 检查 TPC-H 表统计信息是否完整进入 optimizer。
- 检查多列 join `l_suppkey/l_partkey = ps_suppkey/ps_partkey` 的选择率估计。
- 对大输入 join 增加 broadcast 风险阈值或更接近 StarRocks FE 的 partitioned 选择。

### P1：TPC-DS q14 / q23 / q4 的 runtime filter 和 shuffle 缺口

这些 case 的共同点是 RF probe 明显少，shuffle 明显少，部分 case broadcast join 明显更多：

- `q14`：RF build/probe 与 aggregate 节点数量都有差异，先看 filter 穿透聚合和多段 query 的 plan 结构。
- `q23`：broadcast join 更多、partitioned join 更少，适合跟 `q64` 一起验证 distribution 规则。
- `q4`：broadcast join 多 8、partitioned join 少 8、shuffle 少 20、RF probe 少 25，是系统性 plan drift 的高信号 case。

### P2：表达式、UNION、NLJ 的执行微优化

这类不是当前最大端到端瓶颈，但有明确可优化节点：

- `q9` node 17、`q75` node 25 / 12、`q13` node 14：project 表达式耗时偏高，检查 arithmetic / coalesce / between / date extract 的向量化实现。
- `q75` node 27：UNION ALL 2.1s，检查 union pipeline 是否有不必要 copy 或 batch 拼接开销。
- `q81` node 49：CROSS NEST LOOP JOIN 产出 67.9M rows，先确认 StarRocks FE 是否也是同形态；如果 plan 形态一致，再优化 NLJ 输出路径。

## 下一步建议顺序

1. 先补强 `EXPLAIN ANALYZE` 的分布式 profile 归因：至少拆出 operator CPU、exchange wait、driver blocked、sink/source wait、fragment wall time。否则 TPC-DS q14/q23 这类 case 很难靠现有 `act` 精确定位。
2. 用 `q64` 做 TPC-DS plan 对齐样例，重点对齐 join distribution 与 runtime filter probe。
3. 用 `q9` 做 TPC-H plan 对齐样例，重点修正 broadcast 过多和 cardinality fallback。
4. 并行分析 `q18` 的 partitioned hash join 执行路径，因为它是当前所有 benchmark 中最明确、最重的单点执行层热点。

## 相关产物

- Plan 差距明细：`reports/plan_gap_node_heading_current_nr_vs_starrocks_fe.csv`
- Plan 差距汇总：`reports/plan_gap_node_heading_aggregate_current_nr_vs_starrocks_fe.json`
- 慢节点明细：`reports/slow_nodes.csv`
- 每个 query 内部 top 5 慢节点：`reports/slow_nodes_top5_per_query.csv`
- 完整当前 plan：`plans/analyze/{ssb,tpc-h,tpc-ds}/*.result`
