# 优化器统计模型架构路线图(per-group + 多维度 cost + 可信度坍缩)

- 日期:2026-06-16
- 状态:架构决策已收敛,待按阶段实施
- 范围:`src/sql/optimizer/`(standalone Cascades 优化器)的统计推导、坍缩策略与 cost model
- 目标:把统计模型从「per-expr 现算」演进到「per-group 单一统计 + 多维度 cost + 按可信度选代表的坍缩」,对齐业界 Cascades 主流(StarRocks / CockroachDB / Calcite / GPORCA / Presto),以**长期可扩展性**为首要目标。

> 本文档是一条跨多个 PR 的 arc 的总纲。每个阶段(Phase)各自走独立的 spec→plan→实现;本文只固化方向、依赖顺序、关键决策与证据。第 0 节是给不熟悉优化器的读者的背景铺垫。

---

## 0. 背景(给不熟悉优化器内部的读者)

### 0.1 优化器为什么要"估数据量"

一条 SQL 有很多种执行方式,结果相同但快慢差很多。优化器从中挑最快的,靠**估算每一步要处理多少数据**——尤其**基数估计**:每一步产出多少行。

贯穿例子:
> `SELECT 城市, COUNT(*) FROM 用户 GROUP BY 城市`,"用户"表 200 行、100 个不同城市 → 分组产出 100 行。

### 0.2 Memo 与等价类(Group)

优化器探索方案时生成大量等价、共享子结构的写法,用 **Memo** 装。Memo 的核心是**等价类**(代码里叫 Group):**把"逻辑等价、产出相同结果"的若干写法归到同一个 Group。** 如 `A 连 B` 与 `B 连 A` 同组;"直接分组"与"两阶段分组"同组。

### 0.3 关键矛盾:一个 Group 为什么会冒出多个统计

一个 Group 所有成员产出相同结果,输出行数真实世界唯一确定。但优化器是"估"的:估算完美则成员都估同一个数;现实估算不完美,不同写法走不同路径、误差不同,估出的数可能不一样。**NovaRocks 现在是 per-expr 模型**(每个成员各保留各自估的数),这个不一致就暴露;业界用 **per-group 模型**(结构上只存一份),矛盾不会出现。

(讨论里的 200/100/75 是讲概念的例子;75 是一个具体估算 bug——见 §1.1。注:NovaRocks 当前"单阶段 vs 两阶段聚合"其实不在同一**逻辑** Group 竞争——聚合拆分是物理层规则。)

---

## 1. 三个相互耦合的问题

### 1.1 估计器对多阶段聚合二次打折(stage 非幂等)

`agg_group_rows`([estimate/ndv.rs:79-104](../../../src/sql/optimizer/estimate/ndv.rs))把输出 cap 在 `min(combined_ndv, child_rows × 0.75)`(`UNKNOWN_GROUP_BY_CORRELATION`,[statistics.rs:398](../../../src/sql/optimizer/statistics.rs))。`× 0.75` 被当成上界,但 agg 输出物理上界是 `child_rows`,不是它的 75%。两阶段聚合的 Global(child=已聚合的 100 行)被 `min(100, 75)=75` 错砍。**业界对照**:StarRocks `StatisticsCalculator.java:1163-1179` cap 是 `min(rowCount, inputRowCount)`,0.75 只作多列 NDV 组合的指数底数。

### 1.2 agg cost 缺失整个 memory 维度

`PhysicalHashAggregate` cost([cost.rs:125-137](../../../src/sql/optimizer/cost.rs))只算 `input_size × mode_factor`,**没有 memory/output 维度**;而 `PhysicalHashJoin` 算了 build 侧内存([cost.rs:265](../../../src/sql/optimizer/cost.rs))——不对称。**业界对照**:StarRocks `CostModel.java:265-289` agg cost = `cpuCost=input、memoryCost=output`,memory 权重(×2)是 cpu(×0.5)的 4 倍。

### 1.3 per-expr 是 outlier;为什么业界都用 per-group

每个参考 Cascades 优化器都把输出统计挂在 **Group/等价类**(StarRocks `Group.statistics`、CockroachDB `props.Statistics`、Calcite `RelSubset`、GPORCA `CGroup`)。类比:班级人数是"班级"的属性,不该让每个学生各报一个。根本原因:(1)**一致性**:输出行数是结果的属性,必须 Group 级唯一,per-expr 是范畴错误;(2)**代价可比**:Group 内比不同实现谁快,前提是同一起跑线,per-expr 混入估算噪声;(3)**向上一致**:上层引用下层那一份;(4)**性能**:O(成员数) 重复推导很贵。

---

## 2. 目标架构

**一个 Group 一份输出统计(per-group),由多维度 cost model 重度消费(cpu/memory/network),统计的代表由"按可信度选最可信成员"的坍缩策略选出(可信度 = 来源 + 可推导性的字典序,结构最简兜底)。**

---

## 3. 业界对照

| 引擎 | 统计挂在 | 坍缩/代表策略 | 多维度 cost | 可信度模型 |
| --- | --- | --- | --- | --- |
| **StarRocks** | Group | min-computeSize(自标 `@Todo`)+ MV override | cpu/memory/network | ad-hoc 2 档(MV-实测 > 估算) |
| **CockroachDB** | Group | `FirstExpr`(规范化形式) | 有 | 探索前彻底规范化 |
| **Calcite** | RelSubset | best member(CALCITE-2018 失效+重算) | 有 | metadata confidence |
| **GPORCA** | CGroup | `PgexprBestPromise`:argmax `EStatPromise`(4 档),`FFewerConj` inner-join tie-break | 有 | **promise 按算子形状可推导性分级** |
| **Presto** | — | — | 有 | `ConfidenceLevel{LOW<HIGH<FACT}` + `SourceInfo`(实测 > 估算) |

**关键模式**:provenance/promise-ranked 代表选择 + 结构 tie-break,是 GPORCA / Presto / StarRocks 各自独立重新发明的。GPORCA `CGroup::PgexprBestPromise` 字面就是"argmax promise,条件少者并列优先"。

**可信度是一个三层字典序(§4.5 详述)**:来源轴(主) → 可推导性轴(次) → 结构最简(兜底)。来源主导跨等级(实测压估算),可推导性在同来源内细分(简单算子 > 复杂算子)。

---

## 4. 关键设计决策

### 4.1 统计模型:per-group(已定)

search 的 own_stats 改为读 group 的 `logical_props`(经 `stats_for_group` 重建为 `Statistics`,该路径已存在),删 per-expr 现算路径。注意:删的是 **logical** 层 per-expr own_stats;`PhysicalHashAggregate` cost 仍须 walk 自己的 physical child chain,使每个 stage 的 memory 维度反映真实中间 size。

### 4.2 坍缩策略:按可信度的字典序 argmax(本期实做)

坍缩选择点 `derive_group_statistics_for`([stats.rs:733-764](../../../src/sql/optimizer/stats.rs))从 `logical_exprs.first()` 改成**对组内成员做字典序 argmax**:

```
key(member) = (source_confidence, derive_promise)        # 字典序:来源主、可推导性次
argmax key;  平局先比 FFewerConj(inner-join only);  最终平局取最低 index(canonical-first)
```

实现镜像 GPORCA `PgexprBestPromise` + `FBetterPromise`(`espFst > espSnd || (espFst == espSnd && FFewerConj)`)。从 `best=none` 开始、**严格大于**才替换 → 全平局保留最低 index 成员 → **all-High + all-equal-source 精确退化成今天的 `first()`,是严格 refinement**(零回归基线)。

**本期就实做(而非退化留未来)**,因为有两个 live 场景立即受益(见 §4.5 的来源轴/可推导性轴)。canonical-first 不是被替代,而是这个字典序的**最终兜底层**。

**为什么不是 min-computeSize(StarRocks)**:系统性乐观 → 低估 agg memory + under-fire broadcast gate([cost.rs:214-235](../../../src/sql/optimizer/cost.rs))→ 过度 broadcast;破坏 write-once;StarRocks 自标 `@Todo`;它只看一个标量,既不按来源也不按可推导性。

### 4.3 cost model:多维度,agg 加 memory 维度

对齐 StarRocks `CostEstimate{cpu, memory, network}`:agg 的 `memoryCost = output_size × factor`。权重沿用 StarRocks 量级(memory ≫ cpu)起步,落地实测调。

### 4.4 估计器:stage-idempotent(P3,前置)

`agg_group_rows` cap 从 `child_rows × 0.75` 改为 `child_rows`。多列衰减保留在 `combined_ndv` 的 damped product,0.75 不再作 cap。

### 4.5 可信度模型 v2(本期实做的核心)

**可信度 = 三层字典序**,坍缩用它做 argmax:

| 层 | 轴 | 比较 | NovaRocks 现状 | v2 本期 |
| --- | --- | --- | --- | --- |
| 主序 | **来源轴** | 实测 > 真实元数据 > 估算 > 默认 | 有但粗:`Exact`/`Estimated`/`Fallback`,缺顶档 | 加 `Measured` 顶档(type 就位,producer stub);argmax 本期已用此轴 |
| 次序 | **可推导性轴** | 同来源下,简单算子 > 复杂算子 | **完全没有**(derive 把公式结果全压成 Estimated) | 新增 `DerivePromise{Low,Medium,High}`,promise(op) on-the-fly 算 |
| 兜底 | **结构最简** | 取最原始成员 | 取第一个 | 不变(argmax 的最终 tie-break) |

为什么字典序而非加权:来源差异是"质"的(实测是真值,估算再可靠也是估),所以来源主导;可推导性在同来源内细分;结构最简平局兜底。来源与可推导性是**正交的两个量,不可 fold 成一维**(否则 Measured 低 promise 成员会输给 Estimated 高 promise 成员,违反"实测优先")。

#### 4.5.1 来源轴:`Confidence` 加 `Measured` 档

```rust
pub enum Confidence { Fallback, Estimated, Exact, Measured }  // Measured 新增顶档,derive(Ord) 自动定序
```

- **Measured 无 producer,本期 stub**(NovaRocks 暂无 MV 物化行数/runtime-feedback/采样)——variant + 注释命名未来消费者,inert。
- **但来源轴 argmax 本期就有当下价值,不靠 Measured**:`MvRewriteRule`(mod.rs:155)把 MV 重写候选注入与基础聚合**同一个 group**,MV 候选带的统计来源是 MV 目标表的真实目录/Iceberg 元数据(`Exact`),高于基础聚合的 `Estimated` → argmax 让 MV 的 Exact 统计赢。**今天 `first()` 做不到**(它忽略来源)。(实现时需核实 MvRewrite 确实注入同组,见 §7 失效重算义务。)
- **两处强制 consumer 编辑**(加档即必须,否则编译/逻辑错):
  - `aggregate_pushdown/cost.rs:40` 的 `match cs.confidence` 加 `Measured` 臂(否则 exhaustiveness 编译错)。
  - `cost.rs:227` `!= Confidence::Exact` 改 `< Confidence::Exact`(否则未来 `Measured` build 被当 Fallback 不信任,过不了 broadcast trust gate)。
- 加单元测试断言 `Measured > Exact > Estimated > Fallback`(防未来 enum 重排静默破坏 `< Exact` 比较)。

#### 4.5.2 可推导性轴:`DerivePromise` + `promise(op)`(本期实做最小真实版)

```rust
pub(crate) enum DerivePromise { Low, Medium, High }   // 与 Confidence 分开,不 fold
fn promise(op: &Operator, memo: &Memo) -> DerivePromise   // 坍缩时 on-the-fly 算,不存 MExpr(对齐 GPORCA EspDerive 无缓存)
```

算子→promise 映射(对齐 GPORCA 的 ~5 条规则,不是 per-op 大表):
- **默认 `High`**(GPORCA 的 Get/GbAgg/Limit/UnionAll/多数 Unary 都 = EspHigh)。
- **Join(Logical/PhysicalHashJoin/PhysicalNestLoopJoin):reorder 展开形状 → `Medium`,否则 `High`。** 这是**本期唯一 fire 的 live 规则**:`multi_join_reorder`(mod.rs:140)把多个 join 顺序物化进**同一个 group**(`copy_in_join_tree`,stats.rs:771);left-deep 2-way over base scans 与 n-ary 展开的 bushy join 同组,bushy 的行数估计累积更多误差(正是 GPORCA EspMedium n-ary 场景)。Medium 的 proxy:MExpr 无 `ExfidOrigin`,用 child 形状代理(**join 的 child 本身是 join = reorder 展开 = Medium;join over 两个 leaf/base = High**),无需改算子结构;更 faithful 的升级 = 在 `LogicalJoinOp` stamp 一个 bool(像 `AggregateNode::already_pushed`)于 copy_in_join_tree。
- **`Low` 档 typed-but-unproduced + 注释**:GPORCA 的 Low 触发(join 谓词含子查询、Unary 含 scalar 子查询/outer-ref/Apply)**在 NovaRocks 进不了 memo**——子查询/Apply 在 `convert::logical_plan_to_memo`(mod.rs:127)**之前**就已 decorrelate 并 `find_residual_apply`(mod.rs:99)hard-gate。所以保留 variant + comparator,注释命名第一个未来消费者(in-memo decorrelation / Apply 候选)。
- **不 port**:subquery→Low 臂(子查询不进 memo,会是 untested dead code,违反 CLAUDE.md rule 2 "fail fast")、EspDerive 的 outer-ref→Low 横切 override(MExpr 无 outer-ref 追踪)、EspNone 档(无 forbidden-source producer)。

> **诚实校准**:GPORCA promise 的主场景是 subquery,那个在 NovaRocks 已 pre-memo decorrelate、不发生。NovaRocks 同组成员可推导性分歧的**唯一 live 来源是 join-order 形状(bushy vs left-deep)**。所以可推导性轴本期有真实场景(reorder-heavy 的 TPC-DS/SSB)但**比 GPORCA 窄**;不做精细分类器、不数 FFewerConj 之外的东西。

**promise 选取判据(什么属性能进 promise)**:promise 是**组内坍缩**的 argmax——在一个 Group 的成员之间挑代表。所以一个属性要对 promise 有用,**必须在同 Group 的成员之间变化**;组内常量的属性(整组成员都相同)无法区分成员、对 argmax 永远平局,**不进 promise 轴**。据此三个属性的归属:
> - **join-order 形状**:成员间**变化**(reorder 产出 bushy vs left-deep)→ NovaRocks **唯一**进 promise 的属性。
> - **`join_type`(inner/semi/anti/null-aware)**:同组等价成员的 join 类型**相同**(组内常量)→ **不进 promise**。其"anti/semi 比 inner 基数估计不可靠"的信号走**来源轴 Confidence**(`estimate_cardinality` 已对 anti/semi 给低 confidence),影响整组可信度与向上传播,而非组内选代表。
> - **`from_subquery`(出身标记)**:组内常量(同组成员同源)→ **不进 promise**;即便非常量,出身也是错信号——GPORCA 用 **live `DeriveHasSubquery`**(decorrelate 后翻 false),不是 sticky provenance,出身标记会错误地持续 demote 一个已完全 decorrelate、完全可估的 join。**故不为 promise 加 `from_subquery` 标记。** 结构性佐证:NovaRocks decorrelate 产出的 join(Cross/LeftOuter/LeftSemi/LeftAnti/NullAwareLeftAnti)不进多成员 reorder 组(`collect_chain` 只对 Inner/Cross 链递归、其余当不透明原子),子查询来的 join 组通常只有单成员,无可 argmax。

#### 4.5.3 argmax 与 combine/derive 的关系(正交,后者零改动)

`combine()=min`、`derive()` 的 Estimated cap 是**单棵树内统计沿树传播的可信度衰减**;字典序 argmax 是**跨成员选信任谁**——两件正交的事。组合:`derive_statistics(member)` 仍 cap+min 出该成员的 confidence,argmax 再选 (confidence, promise) 最高的成员。`Measured.min(Estimated)=Estimated`,所以加档后 combine/derive 的每个 min 结果数值不变,**`statistics.rs:22-39` 零改动**。

#### 4.5.4 一致性要求(member-consistency)

`derive_group_statistics_for` 喂统计给 `logical_props::derive_for_group`([logical_props.rs:22](../../../src/sql/optimizer/logical_props.rs)),后者独立 re-pick `first()` 算结构属性(output cols、equivalence classes)。argmax 选了成员 i 后,**同一个成员 i 必须 thread 进 `derive_for_group`**(加 chosen-expr 参数),否则统计来自 argmax winner、结构属性来自另一成员 → silently 不一致。**真实 edit,非可选。**

#### 4.5.5 本期实做 vs stub 边界

**本期实做(real code)**:Confidence 加 `Measured` + 两处强制 consumer 编辑;坍缩三处(`stats.rs:739`、`search.rs:299`、`logical_props.rs:22`)改字典序 argmax 并**共享一个 helper**(否则 diverge);`DerivePromise` enum + `promise()` fn(High 默认、Medium 由 reorder 形状)。
**stub(typed-but-inert + 注释)**:`Measured` 无 producer;`DerivePromise::Low` 无 producer;不 port 的 GPORCA override/EspNone。

### 4.6 已排除的方案

- **min-computeSize 作通用坍缩**:见 §4.2。
- **search 读 group 缓存以"消除现算"(早期 B 的设想)**:在当前 per-expr cost 下会改 plan;在目标 per-group 架构下它本就是设计的一部分。
- **subquery→Low / EspNone / outer-ref override / 精细 per-op promise 表**:NovaRocks 无场景或无 MExpr 支持,见 §4.5.2。
- **`from_subquery` 出身标记进 promise**:组内常量(对 argmax 无区分力)、decorrelate 产出的 join 类型不进多成员 reorder 组、且出身是错信号(应用 live property 而非 provenance);join 类型可靠性走 Confidence。见 §4.5.2 的 promise 选取判据。
- **存 promise 到 MExpr**:会要求每次 explore/implement append 时失效重算,违反 append-only 记忆化;on-the-fly 算(GPORCA 即如此)。

---

## 5. 依赖顺序与阶段

```
(依赖) 标量表达式 IR(scalar arena)— per-group 建议排在其 M1 之后
   │
Phase 0  估计器修复(stage-idempotent)            ← 严格前置
   │   否则 per-group 把乐观的 75 锁成权威值
   ▼
Phase 1  per-group 切换 + 可信度模型 v2(本期核心)
   │   • search own_stats 改读 group logical_props;删 per-expr derive 路径(保留 physical-stage cost 推导)
   │   • 坍缩三处改字典序 argmax(共享 helper)+ member-consistency 线程化 + append-only/失效重算 debug_assert
   │   • Confidence 加 Measured 档(stub)+ 两处强制 consumer 编辑;来源轴 argmax 让 MV Exact 赢
   │   • DerivePromise{Low,Medium,High} + promise()(High 默认、Medium=reorder 形状);Low typed-unproduced
   ▼
Phase 2  agg cost 加 memory 维度(output × weight)
   │   此后 per-group 输出统计变 cost-active
   ▼
Phase 3 (future)  实测 producer 落地:启用 Measured;放宽 derive() 的 Estimated cap(Presto per-operator);
                  若 in-memo decorrelation 落地,启用 DerivePromise::Low 的 producer
```

**对标量表达式 IR(scalar arena)的依赖**(`docs/design/specs/2026-06-16-optimizer-scalar-expr-ir.md`):它给标量表达式做 hash-consing(结构相同⟺同 id + 交换律规范化),**直接强化本 arc**——Group 成员去重从"`Debug` 串判等"换 id 比较([该 spec §6](../../../../stupefied-hamilton-9f6eeb/docs/design/specs/2026-06-16-optimizer-scalar-expr-ir.md)),更可靠、伪重复成员减少。但它规范化的是标量层,关系层"哪个是第一个"仍靠 append-only(本期 debug_assert 守关系层,与 scalar IR 互补)。**建议 per-group 排在 scalar IR M1 之后。**

**Phase 0 为什么必须最先**:现有 idempotency 测试 fixture NDV=100 远低于 cap(7500),cap 从不 binding——没真正证明 idempotency。Phase 0 必须补 cap 真正 binding 的高 NDV / 多 key 测试。

---

## 6. 每阶段的 spec→plan 入口

| Phase | 一句话 | 改动面 | golden 预期 | 依赖 |
| --- | --- | --- | --- | --- |
| **0** 估计器 | `agg_group_rows` cap `×0.75 → input_rows` + cap-binding 测试 | `estimate/ndv.rs` | **会变**(agg-heavy plan);record/diff 重录 | 独立,可先合 |
| **1** per-group + 可信度 v2 | search 读 `logical_props`;三处坍缩点改字典序 argmax(共享 helper + member-consistency);Confidence 加 Measured + 2 处 consumer 编辑;DerivePromise + promise();删 per-expr | `search.rs`、`stats.rs`、`statistics.rs`、`logical_props.rs`、`memo.rs`、`cost.rs`、`aggregate_pushdown/cost.rs` | **会变**(MV 组偏好 Exact、reorder 组偏好 High-promise);session var gate + 重录 | Phase 0;**scalar IR M1 之后** |
| **2** agg memory cost | agg cost 加 `output_size × weight` | `cost.rs`(+ CostEstimate 多维化) | **会变**;重录 + 权重实测 | Phase 1 |
| **3** 实测 producer | 启用 Measured / DerivePromise::Low producer;放宽 derive cap | `stats.rs` 等 | 取决于 producer | Phase 1 扩展点 + producer |

---

## 7. 风险

- **Phase 0 排序 load-bearing**:per-group 先于估计器修复会把 75 锁成权威值。
- **plan-golden churn**:字典序 argmax 在两个 live case 改变统计选择——(a) MV rewrite 组(来源:Exact MV vs Estimated 聚合)、(b) multi-join reorder 组(promise:bushy Medium vs 2-way High)。两者都 shift `sql-tests/optimizer`/SSB/TPC-DS goldens。**session var(options.rs/disable_optimizer_rules)gate 便于 bisect;验证后重录。**
- **失效重算新义务(argmax 引入)**:`first()` 时代,后追加成员被忽略所以安全;改 argmax 后,**后追加"更高 source/promise 成员"到已 derived group 的 rule(如 MvRewrite 在初次 derive 之后注入)必须 reset 该组 `logical_props=None`**,否则 argmax 看不到更优成员。更新 append-only 不变量注释([stats.rs:708-718](../../../src/sql/optimizer/stats.rs)),并审计 `MvRewriteRule` 确认它注入后 reset/re-derive。
- **member-consistency bug**:`logical_props::derive_for_group` 若保留自己的 `first()` pick,统计来自 argmax winner、结构属性来自另一成员 → silently 不一致。chosen member 必须线程化(§4.5.4)。
- **三坍缩点必须共享一个 helper**(`stats.rs:739`、`search.rs:299`、`logical_props.rs:22`),否则 diverge。
- **Medium-promise proxy 是推断非 faithful**(GPORCA 用 `ExfidOrigin` NovaRocks 没有)——child-shape proxy 或 stamped bool 是近似;misfire 只选稍差成员的统计(never plan-incorrect,only plan-quality)。
- **FFewerConj 必须 inner-join-only**(`CLogicalInnerJoin::FFewerConj` 对非 inner pair 返回 false);应用到 outer join 会 diverge。
- **typed-but-unproduced 防被当 dead code**:`Measured`、`DerivePromise::Low` 必须有注释命名第一个未来消费者(CLAUDE.md rule 2)。
- **P2 勿 over-delete**:删的是 logical per-expr own_stats;`PhysicalHashAggregate` cost 必须仍 walk physical child chain。加 regression assertion。

---

## 8. 已收敛的决策记录

| 决策 | 结论 |
| --- | --- |
| 统计模型 | per-group(目标),per-expr 是 outlier |
| 坍缩策略 | 按可信度的**字典序 argmax**(来源, 可推导性)+ FFewerConj(inner-only)+ canonical-first 兜底;**本期实做**;min-computeSize 排除 |
| 可信度模型 | 三层字典序(来源/可推导性/结构最简);来源 = Confidence 加 `Measured`(stub);可推导性 = `DerivePromise{Low,Medium,High}` + `promise()`(本期实做最小真实版) |
| 来源轴本期价值 | 不靠 Measured——MvRewrite 注入同组的 Exact 统计经 argmax 赢过 Estimated(今天 first() 做不到) |
| 可推导性轴本期场景 | **仅 join-order 形状**(reorder bushy=Medium vs left-deep=High);subquery 已 pre-memo decorrelate 不进 memo,故 GPORCA 主场景在 NovaRocks 不发生、Low 档 stub |
| promise 选取判据 | 只用**组内成员间变化**的属性;组内常量(`join_type`、`from_subquery`)不进 promise——`join_type` 可靠性走 Confidence(estimate_cardinality 已对 anti/semi 低 confidence),`from_subquery` 不加(出身是错信号,应用 live property 而非 provenance) |
| 与 combine/derive 关系 | 正交;combine=min、derive cap Estimated 零改动(Measured.min(Estimated)=Estimated) |
| member-consistency | argmax 选的成员必须线程进 `derive_for_group`,统计与结构属性同源 |
| cost model | 多维度;agg 加 memory(output)维度,权重对齐 StarRocks 量级 |
| 估计器 | stage-idempotent(cap 去 0.75);Phase 0 前置 + cap-binding 测试 |
| per-expr 删除 | cutover 时删,不留 escape-hatch(保留 physical-stage cost 推导) |
| 标量 IR 依赖 | per-group 排在 scalar IR M1 之后,依赖其 id 判等让成员去重可靠 |

---

## 9. 关键代码引用与业界来源

**NovaRocks**:
- `estimate/ndv.rs:79-104` `agg_group_rows`(P3 改);`statistics.rs:398` `UNKNOWN_GROUP_BY_CORRELATION`
- `statistics.rs:12-18` `Confidence`(加 Measured + DerivePromise enum + 字典序比较器);`statistics.rs:22-39` `combine`/`derive`(零改动)
- `stats.rs:733-764` `derive_group_statistics_for`(坍缩点改 argmax);`stats.rs:1237` conjunct walk(FFewerConj 复用);`stats.rs:771-808` `copy_in_join_tree`(reorder 多形状同组 + Medium 的 stamp 点);`stats.rs:654-670` `best_join_key_ndv`(按可信度选的雏形)
- `search.rs:299` `group_statistics`(坍缩点之二,共享 helper);`logical_props.rs:22` `derive_for_group`(坍缩点之三 + member-consistency)
- `cost.rs:125-137` `PhysicalHashAggregate`(P2 加 memory);`cost.rs:227` `!= Exact`→`< Exact`;`cost.rs:265` HashJoin memory(不对称对照);`cost.rs:214-235` `broadcast_gate_passes`
- `rewrite/rules/aggregate_pushdown/cost.rs:40` `match cs.confidence`(加 Measured 臂)
- `memo.rs:93-100` `add_expr_to_group`(push-only 不变量);`mod.rs:99` `find_residual_apply`(子查询 pre-memo gate);`mod.rs:140` multi_join_reorder;`mod.rs:155` `MvRewriteRule`(同组注入,来源轴 live 场景)
- 标量 IR 依赖:`docs/design/specs/2026-06-16-optimizer-scalar-expr-ir.md`

**业界**:
- StarRocks:`ExpressionContext.java:70`、`CostModel.java:265-289`、`DeriveStatsTask.java:116-129`、`StatisticsCalculator.java:1163-1179`
- CockroachDB:`statistics_builder.go`(`FirstExpr`)
- Calcite:`RelSubset` RowCount、CALCITE-2018
- GPORCA(greenplum-db/gporca):`CLogical.h`(`EStatPromise` 4 档,`Esp` pure virtual)、`CLogicalJoin.h`(subquery→Low / ExfExpandNAryJoin→Medium / else High)、`CLogicalUnary.cpp`(subquery/outer-ref/Apply→Low)、`CGroup.cpp`(`PgexprBestPromise`/`FBetterPromise`/`EspDerive` outer-ref override)、`CLogicalInnerJoin.cpp`(`FFewerConj`)
- Presto:`SourceInfo.java`(`ConfidenceLevel{LOW,HIGH,FACT}`)、`HistoryBasedSourceInfo`(实测 > 估算)
