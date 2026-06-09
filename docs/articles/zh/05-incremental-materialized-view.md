# 增量物化视图：IMV 的 property framework

> NovaRocks 技术分析系列 · 第 5 篇（压轴）

物化视图（MV）不难建——把一个 `SELECT` 的结果物化下来就是了。难的是**刷新**：当基表只变了一小部分，怎么只刷新"受影响的那部分"，而不是把整个 MV 推倒重算？这就是增量物化视图（IMV），也是 NovaRocks 这套引擎能力的天花板，配得上做整个系列的压轴。

增量刷新难在两个子问题：其一，这次刷新到底**该不该**增量、增量到什么程度；其二，对一个**任意形状**的 MV 查询（带 join？带聚合？带 UNION ALL？还是它们的嵌套？），怎么自动推导出一个"只算 delta"的执行计划，并且保证结果正确。NovaRocks 对这两个问题的回答，分别是一个刷新决策器和一套 **property framework**。

```mermaid
flowchart TD
    SNAP["基表快照对比"] --> DEC{"RefreshDecision"}
    DEC -->|"FirstRefresh"| FULL["全量重算"]
    DEC -->|"MetadataOnly / SkipEmpty"| NOOP["无操作"]
    DEC -->|"Incremental"| PIPE
    Q["MV 查询（全量 SELECT）"] --> P["derive_fragment_property<br/>合成能力属性"]
    P --> ID["TargetIdentity<br/>BaseRowId / JoinRowKey / GroupRowId / BranchScoped"]
    P --> CT{"into_refresh_contract"}
    CT -->|"可执行子集"| RC["ImvRefreshContract"]
    CT -->|"超出"| FF["fail-fast 拒绝"]
    Q --> PIPE["build_imv_pipeline<br/>delta 改写流水线"]
    PIPE --> DELTA["只算 delta 的执行计划"]
```

## 第一步：这次该怎么刷？

刷新的第一个判断不是"怎么算"，而是"要不要算、算多少"。NovaRocks 把它收敛成一个枚举，再按基表数量的策略分派：

```rust
// src/engine/mv/refresh_driver.rs:31
pub(crate) enum RefreshDecision {
    SkipEmpty, FirstRefresh, MetadataOnly, Incremental, FailFast { reason: String },
}

// src/engine/mv/refresh_driver.rs:59
pub(crate) fn decide_refresh(policy: BaseSnapshotPolicy, statuses: &[BaseSnapshotStatus], label: &str)
    -> RefreshDecision {
    match policy {
        BaseSnapshotPolicy::SingleBase => decide_single_base_refresh(statuses, label),
        BaseSnapshotPolicy::AllBasesRequired => decide_all_bases_required_refresh(statuses, label),
        BaseSnapshotPolicy::JoinPairPartialInitialSkip =>
            decide_join_pair_partial_initial_skip_refresh(statuses, label),
    }
}
```

单基表的决策逻辑非常干净——它只比较"上次刷到哪个快照"和"现在基表在哪个快照"：

```rust
// src/engine/mv/refresh_driver.rs:81
match (status.previous_snapshot_id, status.current_snapshot_id_before_pin) {
    (None, None) => RefreshDecision::SkipEmpty,
    (None, Some(_)) => RefreshDecision::FirstRefresh,
    (Some(_), None) => fail_fast(/* 之前刷过的快照已不可达 */),
    (Some(previous), Some(current)) if previous == current => RefreshDecision::MetadataOnly,
    (Some(_), Some(_)) => RefreshDecision::Incremental,
}
```

第一次刷新（之前没刷过、现在有数据）必须全量；快照没变就只更新元数据；两个快照都在、且不同，才走真正的增量。注意那条 `(Some(_), None)` 的 fail-fast——之前刷过的基准快照如果已经被回收得不可达了，增量没法继续，于是明确报错而不是悄悄全量兜底。多基表（join）则用不同策略，因为"哪些基表必须都有可用快照才能增量"的规则不一样。

## 核心思想：用"能力属性"取代"形状分类"

真正的难点在第二个子问题。一个朴素的做法是：枚举 MV 查询的所有"形状"——纯投影/过滤、单表聚合、join、join+聚合、UNION ALL……每种写一套增量逻辑。但这条路会组合爆炸：`UNION ALL of (聚合 over join)` 这种嵌套，要么落不进任何一类，要么逼你为每种组合写 special case。

NovaRocks 换了个抽象层级——不问"这个查询长什么形状"，而问"**这个 MV 的一行输出，由什么来标识、又携带什么状态**"。前半部分是 `TargetIdentity`：

```rust
// src/engine/mv/refresh_property.rs:54
pub(crate) enum TargetIdentity {
    /// 单个基表行（直接扫描）
    BaseRowId,
    /// join 出来的行，由两个输入身份的组合标识
    JoinRowKey(Box<TargetIdentity>, Box<TargetIdentity>),
    /// 聚合分组行，由 group-key 列标识
    GroupRowId(Vec<String>),
    /// 分支作用域身份（UNION ALL）：每个分支的身份再打上分支判别
    BranchScoped(Box<TargetIdentity>),
}
```

关键在于它是一个**可组合的代数**：join 的身份是两个子身份的组合，UNION ALL 的身份是 `BranchScoped(子身份)`，而且构造时会把嵌套的 `BranchScoped` 拍平（`BranchScoped(BranchScoped(x)) == BranchScoped(x)`）。一个"聚合 over join 的 UNION ALL"，就自然表达成 `BranchScoped(GroupRowId)`——不需要为这个组合单独开一类。身份再配上后半部分 `StateContract`（这一行携带的是无状态投影、还是可增量合并的聚合状态），就构成了完整的"能力属性"（`RefreshFragmentProperty`）。UNION ALL 的同质性检查则靠 `kind_label`——所有分支必须是"同一种"身份，才允许并到一个分支作用域里。

属性合成出来后，`into_refresh_contract` 把它映射成一个可执行的刷新契约 `ImvRefreshContract`。这里有一个诚实的设计：**属性代数能表达的形状，比实际可执行的形状更多**；超出可执行集合的，明确拒绝：

```rust
// src/engine/mv/refresh_property.rs:625
// Every other property shape (e.g. UNION ALL of joins) is outside
// the legacy-supported set.
_ => Err(format!(
    "Iceberg IMV refresh contract does not support the synthesized property shape \
     (identity={identity:?}, state={state:?})"
)),
```

`UNION ALL of joins` 这种属性能合成出来，但映射成可执行契约时被 fail-fast 挡掉。把"能表达"和"能执行"分开、用一道显式的边界收口——这是在最复杂的地方仍然坚持 fail-fast。配套的还有结构性守卫，比如自连接（`T JOIN T` 在 join 结构上看着是两侧，实则只有一个 distinct 基表）会被显式拒绝：

```rust
// src/engine/mv/refresh_property.rs:710
fn validate_distinct_base_ref_arity(base_refs: &[IcebergTableRef], expected: usize)
    -> Result<(), String> {
    // ... 去重后要求 distinct 数等于 expected ...
    // "Iceberg IMV refresh contract requires {expected} distinct Iceberg base table refs, got {}"
}
```

## 把"全量查询"改写成"只算 delta"

光有属性还不够，还得把 MV 的全量 `SELECT` 真正改写成一个"只算变化量"的执行计划。这件事被做成了一条可组合的重写流水线——而且这条流水线是**真实注册、跑在优化器里的**，不是设计稿：

```rust
// src/sql/optimizer/rewrite/imv/pipeline.rs:29
pub(crate) fn build_imv_pipeline() -> RewritePipeline {
    RewritePipeline::from_stages(vec![
        // imv-delta-marker：把 root 包成 ImvDelta 标记
        RewriteStage::new("imv-delta-marker", /* ... */ vec![Box::new(WrapRootInImvDeltaRule::new())]),
        // imv-branch-union：给 UNION ALL 的每个分支打上分支作用域
        RewriteStage::new("imv-branch-union", /* ... */ vec![Box::new(RewriteBranchUnionRule)]),
        // imv-union-delta / imv-aggregate-state：聚合的带符号增量状态
        RewriteStage::new("imv-union-delta", /* ... */
            vec![Box::new(RewriteUnionAggregateDeltaRule), Box::new(RewriteTopLevelUnionDeltaRule)]),
        RewriteStage::new("imv-aggregate-state", /* ... */ vec![Box::new(RewriteAggregateStateRule)]),
        // imv-delta-pushdown：把 delta 推过一元算子、推过 join
        RewriteStage::new("imv-delta-pushdown", /* ... */
            vec![Box::new(PushDeltaThroughUnaryRule), Box::new(RewriteJoinDeltaRule)]),
        // ... scan 绑定、action/row-id 注入、apply-key、校验 ...
    ])
}
```

读这条流水线，就读懂了"增量正确性"是怎么被拆解的：

- **聚合的增量靠"带符号状态"**。`SUM`/`COUNT` 这类聚合无法直接做"减"，于是 `ivm_delta_aggregate` 把投影改写成带符号的增量状态——给每一行带一个变更操作标记（`CHANGE_OP_COLUMN`：插入为 +、删除为 −），聚合在这个符号上累加，于是删除就变成了"加一个负的状态"。`rewrite_select_sql_for_signed_delta_state` 干的就是这件事。
- **join 的增量靠 delta 传播**。`RewriteJoinDeltaRule` 把 `Δ(A⋈B)` 展开成"`ΔA⋈B` 并上 `A⋈ΔB`"这类形式——只有变化的那侧参与重算。
- **UNION ALL 靠分支作用域**。`RewriteBranchUnionRule` 给每个分支打上判别，让上面的聚合/join delta 规则能分支内独立作用。

每一种代数结构（聚合、join、union）对应一条规则，组合起来就能处理它们的嵌套——这正是"可组合属性"在执行侧的兑现：不是一个能处理所有形状的上帝函数，而是一组能拼装的规则。

而驱动这一切的，是合成出来的属性本身，不再是形状分类器。CREATE 路径上的注释把这个设计说得很直白：

> Drive CREATE off the synthesized capability property + identity instead of the legacy flat shape classifier.（`src/engine/mv/iceberg_refresh.rs:148`）

刷新时直接 `match property.identity` 来决定目标列、gating 和契约构建。第 3 篇里那个"物理写进 Parquet 的 `_row_id`"在这里兑现了价值——它正是 apply key（"把算出来的 delta 应用回 MV 的哪一行"）得以稳定对应的基础。

## 取舍与对照

- **从"形状分类"到"能力属性"，是这篇最核心的设计跃迁**。shape-dispatch 的复杂度随形状种类组合爆炸；property 是可组合代数，嵌套结构（join 套在 union 里、聚合套在 join 上）能自然表达，对应的增量改写也由可组合的规则拼出，而不是一个巨型 special-case 函数。
- **表达力 > 可执行性，用 fail-fast 收口**。属性代数刻意做得比可执行集合更宽，再用 `into_refresh_contract` 一道显式边界把"还不能正确增量"的形状挡在外面。在系统最复杂的角落，仍然是"宁可明确拒绝，不可悄悄算错"。
- **删除即"加负状态"**。聚合增量用带符号的 `CHANGE_OP` 把 DELETE 变成可累加的负贡献，绕开了"聚合不可逆"的难题——这是增量物化视图里很优雅的一招。
- **诚实的边界与中间态**。大量 fail-fast 守卫（不支持 DISTINCT/HAVING/子查询、自连接被拒绝、`UNION ALL of joins` 暂不可执行）划清了能力边界。另外，面向 StarRocks-table 后端的旧 `IncrementalMvShape` 形状分类器仍然并存——Iceberg 路径已完全切到 property framework，两条后端路径处于迁移的中间态。这一点要说清楚：property framework 不是"计划中"，它是 Iceberg IMV 路径**当前实际的驱动**；但全项目尚未完全统一到它之下。

## 小结：最后，凭什么相信它是对的？

到这里，系列把 NovaRocks 从入口、执行内核、SQL 大脑、数据湖一路讲到了能力天花板——增量物化视图。一个用可组合的能力属性来描述"如何增量"、用一串重写规则来兑现"正确增量"的系统，确实是这套引擎里最见功力的一块。

但一个近 55 万行、3.5 个月、大量由 AI 协作写出来的引擎，凭什么敢说这些都是对的？增量刷新这种地方，错一个符号、漏一条 delta 路径，结果就静默地错了。最后一篇收尾，我们就讲这件事的另一面——**正确性是怎么炼成的**：把 SQL 回归测试当作 ground truth 的工程闭环，以及它如何成为高速 AI 协作迭代不至于失控的护栏。
