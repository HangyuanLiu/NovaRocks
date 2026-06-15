# StarRocks FE Benchmark Plan Gap Analysis

Date: 2026-06-13
Status: Analysis
Scope: SSB, TPC-H, TPC-DS Iceberg benchmark plans

## 1. 背景

之前的 optimizer / statistics / distributed execution 相关 PR 已经合入后,
这次目标是用最新 StarRocks FE 作为参照, 对比 NovaRocks standalone planner
在 SSB, TPC-H, TPC-DS 三套 Iceberg benchmark 上的 plan 形态, 找出下一阶段
最值得投入的差距。

这份文档只分析 plan 形态, 不等价于 runtime benchmark。所有结论以
`EXPLAIN VERBOSE` 输出为依据, 用来指导后续 optimizer 和可观测性工作。

## 2. 对比环境

- StarRocks FE source: `origin/main`
  `b1a890eb58247498fff94385dfb13f5f01fd3a6e`
  (`[Refactor] Move query profile controls to QueryRuntimeState (#74756)`)
- NovaRocks source: `88ad35bd5a5b6d060892006b7975cdcdd76b3f5b`
- StarRocks FE runtime: `/Users/harbor/starrocks-on-novarocks/fe`
- NovaRocks compatible BE runtime:
  `/Users/harbor/starrocks-on-novarocks/novarocks`
- Iceberg warehouse:
  `s3://novarocks/novarocks-7fc1732a/iceberg-catalog`
- StarRocks FE catalog: `plan_cmp_sr`
- NovaRocks standalone catalog: `plan_cmp_nr`
- Plan artifact:
  `/Users/harbor/project/NovaRocks/logs/plan-compare/20260613-201518`

采集覆盖:

| Suite | Files | Statements | StarRocks FE | NovaRocks |
| --- | ---: | ---: | ---: | ---: |
| SSB | 13 | 13 | 13 ok / 0 fail | 13 ok / 0 fail |
| TPC-H | 22 | 22 | 22 ok / 0 fail | 22 ok / 0 fail |
| TPC-DS | 99 | 103 | 103 ok / 0 fail | 103 ok / 0 fail |

结论: 两边都能为三套 benchmark 全量生成 `EXPLAIN VERBOSE` plan。

## 3. 总体结论

NovaRocks 目前已经不再是 "benchmark query 无法规划" 的阶段。SSB,
TPC-H, TPC-DS 的核心 physical operator 都已经覆盖:

- scan, filter, project
- broadcast hash join
- partitioned hash join
- local/global aggregate
- top-n / sort
- exchange
- runtime filter
- CTE consume / scalar subquery 相关结构

和 StarRocks FE 相比, 最大差距集中在四类:

1. 复杂 CTE / 自连接 / scalar subquery 下的 cardinality 鲁棒性。
2. 分布式 cost model 和 distribution property, 当前 NovaRocks 明显偏向
   broadcast。
3. runtime filter 的 probe 传播和 explain 可审计性。
4. EXPLAIN 的工程可观测性, 特别是 fragment, cost, Iceberg scan metrics。

## 4. 已经接近 StarRocks FE 的部分

### 4.1 SSB: 基本 join/aggregate 形态已经对齐

以 `ssb/q4.1` 为例:

- StarRocks FE 和 NovaRocks 都选择 lineorder fact table 作为主输入,
  customer / supplier / part / date 维表 broadcast join。
- 两边都有 local aggregate + global aggregate。
- 两边都有最终 sort/top-n 形态。

差异主要在估算值。NovaRocks 利用当前 Iceberg min/max/NDV 信息后,
一些过滤后行数比 StarRocks FE 更激进, 例如 customer filter 和最终 group
行数明显更小。这不是当前最危险的问题, 但后续需要用 runtime profile
校准是否过度乐观。

### 4.2 TPC-H: 大表 partitioned join 已经出现

以 `tpc-h/q9` 为例:

- StarRocks FE 在 `lineitem` 和 `partsupp` 的大表 join 上选择 partitioned
  join。
- NovaRocks 也能在同一类事实表 join 上生成 partitioned join, 说明
  distributed physical plan 已经具备正确基础。

差异是 join order 和中间行数估算仍不一致。NovaRocks 当前最终估算偏小,
同时 explain 表达式渲染中出现了类似
`l_extendedprice * 1 - l_discount - ps_supplycost * l_quantity` 的可读性问题,
看起来丢失了原始括号。这个问题可能只是 EXPLAIN display, 但会降低
plan review 的可信度, 应该单独修正。

### 4.3 TPC-DS q72: 已具备分布式形态, 但仍是 pruning/cost 校准样例

`tpc-ds/q72` 是前一阶段重点问题。现在 NovaRocks plan 已经具备:

- catalog_sales 和 inventory 的大表 partitioned join。
- 多个 dimension table 的 broadcast join。
- local/global aggregate。
- top-n。
- 多个 runtime filter。

这说明 q72 已经不再是 "无法生成分布式 plan" 的问题。但 1FE + 3BE 环境
下的 targeted rerun 仍显示 q72 可能在 180s 内超时; 同时禁用
`RuntimeFilterPushDown` 后主 plan 形态基本不变, 也会超时。因此 q72 当前
不应简单归因为 runtime filter 正确性回归。

更值得关注的执行区域是 `catalog_sales` 和 `inventory` 的大表 partitioned
join。当前 plan 会先形成较大的 fact/fact 中间结果, 再通过多个 broadcast
dimension join 继续过滤。后续 q72 应作为 star-schema pruning, dynamic
pruning 和 join-reorder cost feedback 的校准样例, 而不是继续做单 query
特判。

## 5. 主要差距

### P0: CTE / 自连接 / scalar subquery 的 cardinality 爆炸

TPC-DS 中最明显的问题不是 plan 生成失败, 而是若干复杂 query 的估算值
出现数量级爆炸。典型样例:

| Query | NovaRocks 最大估算 | StarRocks FE 参照 | 现象 |
| --- | ---: | ---: | --- |
| `tpc-ds/q4` | `250000000000000` | 约 `2592364` | CTE `year_total` 多次消费和自连接后放大 |
| `tpc-ds/q31` | `92720750319171` | 约 `2513323` | 多分支 CTE / join selectivity 不稳定 |
| `tpc-ds/q11` | `176776695296637` | 约 `2592364` | 重复 CTE 消费导致行数上界失控 |
| `tpc-ds/q74` | `176776695296637` | 约 `2592364` | 和 q11 类似的 customer/year 聚合模式 |
| `tpc-ds/q59` | `2660963222367` | 约 `2513323` | 多层 join + group 后仍持续放大 |
| `tpc-ds/q2` | `7676602377121` | 约 `1944839` | scalar/aggregate 组合下估算失真 |

这类问题会污染后续所有代价决策:

- broadcast 是否超过内存上限会被误判。
- shuffle/broadcast 的网络成本比较会失真。
- join reorder 会被错误的中间行数驱动。
- runtime filter 的收益判断也会不稳定。

长期修复方向:

1. 给每个 logical operator 增加清晰的 row-count upper bound 语义。
   Aggregate, semi join, scalar aggregate, CTE consume 不能简单继承或倍乘
   输入估算。
2. CTE consume 需要携带 producer statistics, 并在多次消费时避免把同一批
   producer cardinality 当成独立随机变量无限相乘。
3. Join estimator 需要同时使用 join key NDV, null fraction, row-count cap,
   以及外键/主键或唯一性推导。至少要保证 `rows(left) * rows(right)` 的
   cross fallback 不会越过已知 domain 上界。
4. 对复杂谓词引入 damped independence。多个相关谓词不能长期按完全独立
   相乘, 多个相关 join 也不能长期按完全独立放大。
5. 对 scalar subquery 和 single-row aggregate 明确输出上界为 1, 并让后续
   cross join / nest loop join 继承这个上界。

建议第一个 milestone 先以 `q4`, `q31`, `q11`, `q74`, `q72` 建立 plan
regression, 只锁关键行数数量级和关键 distribution, 不要一次性锁完整
plan golden。

### P0/P1: 分布式 distribution choice 仍然偏 broadcast

从全量 plan 的方向性统计看, NovaRocks 在 TPC-DS 上比 StarRocks FE 更偏向
broadcast:

| Suite | Engine | Broadcast join | Partitioned join | Shuffle exchange |
| --- | --- | ---: | ---: | ---: |
| SSB | StarRocks FE | 36 | 0 | 10 |
| SSB | NovaRocks | 36 | 0 | 10 |
| TPC-H | StarRocks FE | 38 | 15 | 54 |
| TPC-H | NovaRocks | 45 | 13 | 45 |
| TPC-DS | StarRocks FE | 484 | 105 | 507 |
| TPC-DS | NovaRocks | 605 | 18 | 215 |

这些数字是基于 EXPLAIN 文本的启发式统计, 不能替代逐个 query 的语义
review, 但趋势很明确: SSB 基本一致, TPC-H 有小差距, TPC-DS 复杂 query
里 NovaRocks partitioned join 和 shuffle 选择明显不足。

长期修复方向:

1. 把 distribution property 变成 physical property, 让 join, aggregate,
   exchange 都显式声明 input/output distribution。
2. Join physical implementation 不应只基于规则倾向选择 broadcast。需要同时
   估算 build side row count, row width, memory cost, network cost, probe
   side parallelism 和已有 partition property。
3. 增加 broadcast memory guardrail。即使 cardinality 偏差存在, 也要通过
   conservative cap 避免把大中间结果错误 broadcast。
4. 对 colocated/compatible partition 的场景保留不必要 shuffle 的消除能力,
   但不要因此压制必要的 partitioned join。
5. EXPLAIN 需要输出 join distribution 的选择理由, 例如
   `distribution=BROADCAST reason=small_build estimated_build_bytes=...`。

建议从 TPC-DS 的 `q4`, `q31`, `q64`, `q83`, `q95` 选 5 个 query 做
distribution-focused regression。

### P1: Runtime filter probe 传播不足

NovaRocks 已经能 build runtime filter, 但 probe 侧可见数量少于 StarRocks FE。
方向性统计如下:

| Suite | Engine | Runtime filter build | Runtime filter probe |
| --- | --- | ---: | ---: |
| SSB | StarRocks FE | 36 | 13 |
| SSB | NovaRocks | 36 | 15 |
| TPC-H | StarRocks FE | 56 | 68 |
| TPC-H | NovaRocks | 45 | 35 |
| TPC-DS | StarRocks FE | 603 | 478 |
| TPC-DS | NovaRocks | 582 | 268 |

SSB 已经没有明显问题。TPC-H 和 TPC-DS 的差距说明 runtime filter 可能还没有
稳定穿过 project, exchange, CTE consume 或复杂 join subtree。

长期修复方向:

1. 为 runtime filter 增加从 build expression 到 scan column 的 lineage
   trace, 能解释每个 filter 为什么能或不能下推。
2. 允许 runtime filter 穿过安全的 project / alias / exchange / CTE consume。
3. 对不能下推的 filter 在 EXPLAIN 中输出 concise reason, 例如
   `blocked_by=non_deterministic_expr` 或 `blocked_by=missing_lineage`。
4. 建立 runtime filter plan regression, 先覆盖 `q72`, `q9`, `q49`, `q95`。

### P1: Semi-join reduction / dynamic pruning / cost feedback 缺失

目前 NovaRocks 已经有子查询语义上的 semi/anti join: `EXISTS`, `IN`,
`NOT IN` 可以改写成 `LeftSemi`, `LeftAnti` 或 `NullAwareLeftAnti`, 基数
估计也认识这些 join 类型。但 SSB/TPC-H/TPC-DS 中更关键的不是显式
semi-join, 而是 star-schema 下的维表过滤。

以 q72 为例, SQL 本身是普通 inner join, 不是 `IN/EXISTS` 子查询。当前优化器
不会把过滤后的 `date_dim`, `item`, `warehouse`, `customer_demographics`,
`household_demographics` 等小维表转成对 `catalog_sales` 或 `inventory` 的
早期 key-domain 过滤。因此大事实表 join 前缺少 semijoin reduction, 容易
先产生较大的 fact/fact 中间结果。

runtime filter 当前也主要是 physical plan 生成后的 annotation: probe 侧可以
做 chunk-level 过滤, 部分 Parquet 路径可以把 runtime min/max 转成 row group
或 page 级 predicate。但它还没有形成更早的 dynamic pruning 能力:

- 不参与 join reorder 成本选择。
- 不在 Iceberg manifest / file / split planning 阶段裁剪扫描范围。
- 不会等待 build-side domain 后再调度 probe-side split。
- 不会把过滤后维表 key-domain 作为 fact scan 的 logical semi-filter。
- 不会把 runtime filter 或 semijoin reduction 的预期收益反馈给 cost model。

这意味着当前 runtime filter 可以减少一部分已经调度/读取后的数据, 但还不能
系统性阻止大事实表 join 前的数据膨胀。下一阶段要补的是
"filtered dimension -> fact scan pruning -> join order cost feedback" 这条闭环。

### P1: Scalar subquery / multi-distinct / CTE reuse 仍有优化空间

`tpc-ds/q9` 和 `tpc-ds/q28` 展示出大量 scalar aggregate 以及
`NEST LOOP JOIN (CROSS)` 的 plan 结构。这里不一定是错误, 因为 scalar
aggregate 输出一行后 cross join 是合法形态; 但重复扫描同一事实表,
重复构建类似 aggregate 子计划, 会对执行效率和计划复杂度产生明显压力。

长期修复方向:

1. 对相同 base table + 相近谓词的 scalar aggregate 子查询做 common subplan
   识别, 或在逻辑层改写成一次 scan 多个 aggregate bucket。
2. CTE reuse 不只复用语法节点, 还需要复用 producer statistics 和 distribution
   property。
3. multi-distinct aggregate 的 local/global/single 分布策略要统一纳入 cost
   框架, 避免某些分支退化成单点瓶颈。

### P1/P2: EXPLAIN 可观测性落后于 StarRocks FE

StarRocks FE 的 `EXPLAIN VERBOSE` 目前能提供:

- `PLAN COST`
- fragment cost
- `PLAN FRAGMENT`
- input/output partition
- join distribution type
- table version
- Iceberg scan metrics, 包括 data/manifests/files 等信息

NovaRocks 当前 plan 更紧凑, 有 `stats={rows=...}`, exchange, runtime filter,
min/max stats 等信息, 但缺少:

- plan-level cost header
- fragment-level cost
- fragment id 和 input/output partition
- join distribution 选择原因
- Iceberg snapshot / manifest / file-level scan metrics
- concise schema display

另外 NovaRocks `EXPLAIN VERBOSE` 末尾的 `Boundary Schemas` 对复杂 TPC-DS
query 非常长, 对 plan review 的信噪比不高。建议默认 concise, 需要时再打开
full schema/debug 模式。

## 6. 下一步工作建议

### Workstream A: Statistics and cardinality guardrails

目标: 先让复杂 TPC-DS 的估算值进入合理数量级, 避免后续 cost model 在错误
输入上做精细优化。

建议任务:

1. 建立 plan regression suite, 覆盖 `q4`, `q31`, `q11`, `q74`, `q72`。
2. 增加 CTE producer/consumer statistics 传播。
3. 增加 scalar aggregate row-count upper bound。
4. 增加 join estimator 的 NDV-aware cap 和 damped independence。
5. 用 `EXPLAIN COSTS` 或 `EXPLAIN VERBOSE` 锁关键节点的行数数量级。

验收标准:

- `q4`, `q31`, `q11`, `q74` 不再出现 `1e12+` 级别的中间估算。
- q72 保持当前 partitioned large fact join 形态。
- SSB 不出现明显 plan 退化。

### Workstream B: Distribution property and costed join implementation

目标: 让 broadcast / partitioned / shuffle 的选择成为可解释的 cost decision,
而不是局部规则结果。

建议任务:

1. 给 physical plan 增加 distribution property 结构。
2. Join physical implementation 同时生成 broadcast 和 partitioned 候选。
3. 增加 row-width-aware broadcast cost 和 network shuffle cost。
4. 增加 broadcast memory guardrail。
5. 在 EXPLAIN 中输出 distribution choice reason。

验收标准:

- TPC-DS 中明显大中间结果不再被 broadcast。
- `q4`, `q31`, `q64`, `q83`, `q95` 的 shuffle/partitioned join 数量接近
  StarRocks FE 方向。
- TPC-H q9 保持大表 partitioned join。

### Workstream C: Runtime filter lineage, semi-join reduction, and dynamic pruning

目标: 不只缩小 TPC-H/TPC-DS 中 runtime filter probe 数量与 StarRocks FE 的
差距, 还要把 filtered dimension 对 fact scan 的裁剪能力前移到大事实表 join
之前。

建议任务:

1. 先补最小可用的 observability: 在 `EXPLAIN ANALYZE` 或 profile 中输出
   scan input/output rows, join input/output rows, runtime filter wait time,
   runtime filter accepted/rejected rows, 以及 scan pruning 指标。
2. 建立 runtime filter lineage trace, 解释每个 filter 从 build expression
   到 scan column 的映射链路。
3. 支持 runtime filter 穿过安全的 project / alias / exchange / CTE consume,
   并对不能下推的 filter 输出 concise reason。
4. 增加 star-schema semijoin reduction 识别: 对 filtered dimension + fact
   FK join 生成 key-domain 或 reduction descriptor。
5. 把 reduction descriptor 下推到 fact scan。第一阶段可以支持 in-list,
   min/max, bloom/domain summary; 第二阶段接入 Iceberg manifest/file/split
   pruning。
6. 把 semijoin reduction 和 runtime filter 的预期收益反馈给 join-reorder cost,
   让优化器知道 "先构造小维表 domain 再扫事实表" 可能比先做 fact/fact join
   更便宜。
7. 添加 `q72`, `q9`, `q49`, `q95` 的 plan-shape regression, 并补 q72
   profile regression 用来验证大 fact/fact join 前的输入规模下降。

验收标准:

- TPC-H runtime filter probe 数量明显接近 StarRocks FE。
- TPC-DS 复杂 join subtree 的 scan probe 覆盖率提升。
- EXPLAIN 能解释每个 runtime filter 的 build/probe 映射。
- q72 的 `catalog_sales` / `inventory` fact scan 或其上游 join 前出现可解释的
  dimension-domain pruning。
- join reorder 的 cost explain 能展示 semijoin/dynamic pruning 对候选计划的
  成本影响。

### Workstream D: EXPLAIN observability cleanup

目标: 让 benchmark plan review 不依赖人工 grep 大段文本。

建议任务:

1. 增加 plan-level cost header。
2. 增加 fragment id, fragment cost, input/output partition。
3. 增加 Iceberg scan snapshot / manifest / file metrics。
4. 把 `Boundary Schemas` 改成默认 concise, full schema 放到 debug explain。
5. 修复表达式渲染括号问题, 让 EXPLAIN display 不产生语义误读。

验收标准:

- 单个 TPC-DS query 的 plan 可以直接看出 cost, distribution, scan metrics。
- q9 的表达式渲染保留必要括号。
- 复杂 plan 的 schema 输出不再淹没核心 operator 信息。

## 7. 风险和限制

- 这次对比的是 plan, 不是端到端 benchmark runtime。
- StarRocks FE plan 来自本地构建的最新 `origin/main`。
- NovaRocks plan 来自 standalone `EXPLAIN VERBOSE`; compatible BE runtime
  同时完成部署并保持 alive, 但本次 gap 判断主要基于 standalone planner。
- 文本统计是启发式指标。涉及具体 query 的结论应以对应 `.plan` 文件人工
  review 为准。
- TPC-DS 中 `NEST LOOP JOIN (CROSS)` 不一定都是错误。scalar aggregate
  输出一行时 cross join 合法, 需要结合 row-count upper bound 和重复扫描
  成本判断。

## 8. 建议优先级

下一阶段不建议先做零散的单 query 特判。更有长期收益的顺序是:

1. 先做 cardinality guardrails, 让复杂 TPC-DS 的估算值不爆炸。
2. 同步补最小 profile / `EXPLAIN ANALYZE` 可观测性, 确认 scan/join/RF 的真实
   input/output 和等待成本。
3. 再做 distribution property 和 costed join implementation, 让 broadcast
   和 shuffle 的选择可解释。
4. 补 runtime filter lineage, semijoin reduction 和 dynamic pruning, 避免大表
   scan 在事实表 join 前失去维表过滤机会。
5. 最后完善完整 EXPLAIN observability, 把 plan review 变成稳定回归能力。

这条路线能把 q72 已经改善的成果沉淀成系统性能力, 而不是继续依赖单点
规则或 query-specific 修补。
