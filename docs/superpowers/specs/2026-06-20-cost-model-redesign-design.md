# NovaRocks CBO Cost Model Redesign

## 背景

NovaRocks 当前 cost 计算已经具备 property-aware search 的雏形，但整体仍偏简单：memo search 使用单一 `f64` 作为 winner cost，`CostEstimate { cpu_cost, memory_cost, network_cost }` 还没有成为主路径，许多物理节点仍落到固定比例或 fallback cost。已有 OQ-8 / OQ-12 / join reorder 设计为分布式属性、统计估计和多 join 枚举打好了基础，这一轮的目标是把 cost 模型补齐成一个可解释、可调参、可测试的 CBO 框架。

StarRocks 的实现提供了主要参考方向：cost 拆成 CPU / memory / network 维度，最终通过权重合成；hash join cost 明确区分 broadcast / shuffle，考虑 build/probe/output、backend 并行度、key size、memory fanout；distribution cost 单独建模 ANY / BROADCAST / SHUFFLE / GATHER / ROUND_ROBIN；统计信息需要能够提供输出大小、列宽、NDV 和不确定性标记。

## 目标

1. 建立统一的 `CostEstimate` 主路径，让物理节点、enforcer 和 join reorder proxy 使用一致的维度和因子命名。
2. 补齐主要物理节点的 cost matrix，避免大面积 fallback 到 `rows * constant`。
3. 让 join、aggregate、sort、topn、exchange、distribution enforcer 的代价能反映数据规模、输出宽度、表达式复杂度、并行度和网络/内存风险。
4. 通过 `EXPLAIN COSTS` 暴露足够信息，能够定位 plan 选择错误来自统计、公式还是 property/enforcer。
5. 用单测、plan golden 和 StarRocks sanity case 验证方向正确，不要求和 StarRocks 数值一致。

## 非目标

1. 不在这一轮改写统计推导的语义边界。`stats.rs` 仍负责 row count、NDV、列统计和 confidence；cost 模型只消费统计结果。
2. 不要求一次实现 StarRocks 所有高级统计能力，例如 histogram、multi-column combined stats、UK/FK 识别和 correlated predicate 精确估计。
3. 不把 join reorder proxy 提升为最终权威 cost。memo search 仍是最终 winner 的唯一权威。
4. 不引入外部成本校准系统或基准自动调参系统。

## 总体架构

Cost 模型分成四层：

1. `Statistics` / `ColumnStatistic`：提供 row count、输出列宽、NDV、confidence、fallback reason 等统计输入。
2. `CostInput`：把 operator、own stats、child stats、child output property、required property、alternative kind 和 `CostOptions` 汇总成 cost 计算的唯一入口。
3. `CostEstimate`：作为 CPU / memory / network 三维 cost 的主类型，并提供 total cost 合成。
4. Search / reorder / explain consumers：memo search 用 total cost 排 winner；join reorder proxy 用轻量输入做剪枝；explain 输出分解后的 cost 和关键决策。

推荐边界如下：

```rust
pub(crate) struct CostInput<'a> {
    pub op: &'a Operator,
    pub own_stats: &'a Statistics,
    pub child_stats: &'a [&'a Statistics],
    pub child_outputs: &'a [&'a PhysicalPropertySet],
    pub required_output: &'a PhysicalPropertySet,
    pub alt_kind: &'a PropertyAlternativeKind,
    pub options: &'a CostOptions,
}
```

`compute_cost_with_properties(...)` 最终收敛到 `compute_cost(input: &CostInput) -> CostEstimate`。短期可以保留 `Cost = f64` 的 public/search 接口，但内部必须先算 `CostEstimate`，再通过权重合成 `total`。这样可以控制改造范围，同时让 explain 和测试看到完整维度。

`CostOptions` 扩展为集中调参入口，至少包含：

- CPU / memory / network 权重。
- backend factor、parallelism factor、broadcast row/byte gate。
- predicate、projection、hash、sort、topn、aggregate、exchange、startup、fallback 等因子。
- memory spill risk 或 memory pressure 相关惩罚因子。

## 节点级 Cost Matrix

### Scan

Scan cost 以读取行数和输出字节为核心。CPU 取决于行数、列数、谓词复杂度和格式解码成本；network 取决于远端对象存储或 connector 是否需要跨节点传输；memory 主要是 batch materialization 和 projection buffer。没有精确 remote/local 信息时先用保守默认，后续由 connector metadata 细化。

### Filter

Filter cost 不只看输出行数，还要看输入行数和 predicate 复杂度。CPU 应按输入行评估表达式，输出 row count 只影响后续节点。复杂谓词、函数调用和 residual predicate 应比简单比较更贵。unknown selectivity 需要通过 confidence 或 fallback reason 体现。

### Project

Project cost 按输入行数和表达式复杂度计算。纯列引用接近零成本，简单 cast / arithmetic 中等，函数调用和复杂 scalar 更高。输出宽度影响 memory cost。

### Hash Join

Hash join 是本轮重点。cost 拆成：

- build cost：build rows、build bytes、join key count/key width、hash table entry overhead。
- probe cost：probe rows、probe key width、predicate residual complexity。
- output cost：output rows、output bytes。
- distribution cost：broadcast / shuffle / colocated / already satisfied 的网络和内存差异。
- memory risk：build side bytes 超过阈值时加 spill 或 pressure penalty。

Broadcast 的 memory cost 需要乘 backend fanout，network cost 需要反映 build side bytes 复制到多个 backend。Shuffle 的 network cost 按两边 shuffle bytes 估计，CPU 加 hash partition 成本。build/probe side 的代价应该依据 child output property 和 join distribution decision，而不是只看 operator 枚举。

### Nested Loop Join

Nested loop join 以 `left_rows * right_rows` 为基础，并对 cross join、non-equi join、missing predicate 和 large input 加强惩罚。它应该只在小表或无法 hash join 的语义场景下胜出。

### Aggregate

Aggregate cost 需要区分 local/global、group key 数量、NDV、输入行数、输出行数和 aggregate function 复杂度。Hash aggregate 的 memory cost 由 group cardinality、group key width 和 aggregate state width 决定。Split aggregate 的 local 阶段应体现行数削减收益，global 阶段体现 merge 成本。

### Sort / TopN / Limit

Sort 使用 `N log N` CPU 模型，memory 按输入 bytes 估算，并在超阈值时加 spill risk。TopN 使用 `N log K` 或 heap 模型，K 来自 limit，明显低于 full sort。Limit 主要是轻量 passthrough，但如果能减少上游 required property 或 exchange 输出，收益由 search/property 层体现。

### Exchange / Distribution Enforcer

Exchange 和 distribution enforcer 必须通过同一套 cost kernel。GATHER、SHUFFLE、BROADCAST、ROUND_ROBIN 分别建模 network bytes、CPU serialize/hash partition cost、memory buffer cost 和 backend fanout。已经满足 required property 的 alternative 不应重复收 enforcer cost。

### Fallback

仍需要 fallback，但 fallback 必须显式可见：节点类型、fallback reason、confidence 和使用的默认因子要能在 verbose/cost explain 或 debug path 中定位。fallback cost 应偏保守，避免 unknown stats 把极端 plan 选成 winner。

## 统计输入和边界

`stats.rs` 继续负责统计语义：row count、column NDV、column width、join/filter/aggregate cardinality、confidence。`cost.rs` 不重新推导 cardinality，只消费 `own_stats` 和 `child_stats`。

需要补齐的统计消费能力：

- 按输出列集合估算 bytes，例如 `compute_size_for_columns(&[ColumnId])` 或同等 helper。
- 从 scalar expression 估算表达式复杂度，包括 column ref、literal、cast、binary op、function call、case/compound predicate。
- 暴露 confidence 和 fallback reason，让 cost explain 能说明不确定性。
- 对 unknown row count、unknown NDV、zero/negative/NaN 等异常输入做统一 clamp。

模块边界：

- `stats.rs`：统计推导和 confidence。
- `cost.rs`：CPU / memory / network 公式、dimension weights、operator cost matrix。
- `search.rs`：枚举 alternative、调用 cost、比较 total，不内联公式。
- `derive/` property 相关模块：只负责 property 推导和 enforcer 插入，不持有 cost 公式。
- multi-join reorder：保留轻量 proxy cost，用一致因子做剪枝，不替代最终 search winner。

## Join Reorder Proxy

Multi-join reorder 的 proxy cost 继续只用于枚举剪枝。它和主 cost 模型共享维度名称和关键因子，但输入保持轻量：

- left/right rows。
- join output rows。
- join key NDV 和 key width。
- output width。
- join type。
- predicate complexity。

proxy 不读取 `child_outputs` / `required_output`，也不决定 broadcast/shuffle 的最终 winner。这样可以保持 reorder 阶段足够快，并避免提前复制 property-aware search 的逻辑。

## EXPLAIN 可观测性

`EXPLAIN COSTS` 是本轮主要验收入口。每个物理节点至少展示：

- `rows`
- `cpu`
- `memory`
- `network`
- `total`
- `confidence`

对关键节点额外展示 decision 字段：

- hash join：`distribution`、`build_rows`、`probe_rows`、`build_bytes`、`probe_bytes`、`spill_risk`。
- aggregate：`group_rows`、`group_keys`、`state_bytes`、`phase`。
- sort/topn：`input_rows`、`limit`、`spill_risk`。
- exchange/enforcer：`distribution`、`network_bytes`、`fanout`。
- fallback：`fallback_reason`。

输出目标是稳定、可测、能解释方向，不要求暴露完整公式。

## 验证策略

### Unit Tests

在 `cost.rs` 附近加入 focused unit tests，用合成 `Statistics` 覆盖：

- scan/filter/project。
- hash join broadcast vs shuffle。
- nested loop join large-input penalty。
- aggregate local/global 和 high-NDV memory cost。
- sort vs topn vs limit。
- exchange/enforcer distribution cost。
- unknown stats fallback。

### Plan Golden

在 optimizer SQL golden 中增加 plan-shape case：

- 小 build side 选择 broadcast，大 build side 避免 broadcast。
- shuffle 在双大表 join 中胜出。
- TopN 比 full sort 更便宜。
- high-cardinality aggregate 体现 memory pressure。
- unknown stats 不选择明显极端的 plan。

### StarRocks Sanity 对照

选择少量 StarRocks 参考 case 做方向对照，不比较数值绝对值：

- 大表 broadcast 被惩罚。
- nested loop 不应在普通 equi join 上胜出。
- TopN 明显低于 full sort。
- exchange distribution cost 会影响 join distribution 选择。
- unknown stats 使用保守 fallback。

## 分阶段落地

### Phase 1: CostEstimate 主路径

引入 `CostInput`，让 `cost.rs` 内部返回 `CostEstimate`，search 暂时继续用 total `f64` 比较 winner。补齐 `CostOptions` 的权重和核心因子。保证现有 plan 不发生大面积无意变化。

### Phase 2: 节点公式补齐

按 matrix 补齐 scan/filter/project/join/aggregate/sort/topn/limit/exchange/enforcer。优先处理 join、exchange、aggregate、sort/topn，因为它们最直接影响分布式计划质量。

### Phase 3: 可观测性和测试

扩展 `EXPLAIN COSTS`，加入 cost unit tests 和 optimizer golden。对 unknown stats/fallback 做稳定输出。

### Phase 4: Join Reorder Proxy 对齐

把 multi-join reorder proxy cost 改成同维度、同因子命名的轻量估计，并增加剪枝方向测试。最终 winner 仍由 memo search 决定。

## 风险和缓解

1. 公式调整导致 plan golden 大面积变化。缓解：Phase 1 先接入维度但保守保持 total，Phase 2 分节点启用并逐步更新 golden。
2. 统计不准导致 cost 看似错误。缓解：EXPLAIN 输出 confidence 和 fallback reason，区分 stats 问题和 cost 公式问题。
3. property-aware join cost 和 enforcer cost 双重计费。缓解：`CostInput` 显式传入 child output 和 required property，cost kernel 只对实际 alternative/enforcer 计费。
4. join reorder proxy 过重影响优化耗时。缓解：proxy 只消费轻量统计，不引入 property search 输入。
5. 调参常量分散。缓解：所有因子集中到 `CostOptions`，测试使用显式 options。

## 成功标准

1. 主要物理节点不再大面积落到 generic fallback。
2. memo search winner 使用 `CostEstimate::total_cost`，并能在 explain 中看到维度分解。
3. broadcast / shuffle / gather / topn / full sort / aggregate memory pressure 的方向符合直觉和 StarRocks sanity。
4. cost 公式有 focused unit tests，关键 plan 选择有 optimizer golden。
5. 当前已有的 property-aware search、stats derivation 和 join reorder 设计边界保持清晰，没有把公式散落到 search 或 property derive 层。
