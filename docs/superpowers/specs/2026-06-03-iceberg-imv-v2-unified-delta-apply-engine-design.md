# Iceberg IMV-v2:统一 delta-apply 引擎设计(RFC)

日期：2026-06-03
状态：Draft / 待团队评审
范围：Iceberg-backed 增量物化视图(IMV)的**物理 refresh 编排层**重构方向

---

## 0. TL;DR

NovaRocks 的 Iceberg IMV **逻辑层**已经是与 StarRocks 最新 IVM 同构的 *marker-pushdown* 框架(`ImvDelta` / `ImvVersion` marker + 每算子一条 delta rule，递归组合），并且在 **base DELETE / signed-state retraction** 上比 StarRocks 当前的 IVM 更靠前。真正的 case-by-case 重复发生在**物理 refresh 编排层**：每新增一种算子组合（projection / aggregate / join / join+aggregate / UNION ALL …）就要写一个新的 `refresh_*_mv` 函数。

本 RFC 提议把物理编排收敛为一个**统一的多-base delta-apply 引擎**：
1. 把"apply key 的类型与定位语义"抽象成 **ApplyKeyContract**，由 plan 自底向上派生，取代按 shape dispatch；
2. 把各 `refresh_*_mv` 里重复的 base-load / pin / contract-validate / changes 构造 / first-metadata-incremental 决策提取成**一个公共编排骨架**；
3. （可选）引入 **PCT-style 全量重刷 fallback** 作为 operator-agnostic 安全网 + **CREATE-time trial rewrite** 防"缺 rule 写错数据"。

目标：**新增算子组合的边际成本，从"一个新 refresh 函数"降到"一条 rewrite rule + 一个 apply-key 契约"**；通用性由 fallback 兜底，细粒度增量只在能证明正确的形态上做。

非目标：不改变"只物化 target 表、中间无 state"的存储模型（不转向 differential-dataflow 式的全算子物化）。

---

## 1. 背景与动机

当前对 `UNION ALL of aggregate branches`（roadmap 任务 9，下称 B 族）的支持，按现有模式又需要新增一个 `refresh_union_aggregate_mv` + `plan_iceberg_union_aggregate_mv_refresh`。这触发了一个根本问题：

> 这种按 SQL 形态逐个支持的方式，更像 case-by-case，而不是通用 planner。未来每出现新的算子组合（`union+join`、多层嵌套、window…），是不是都要单独写一遍？

本 RFC 回答这个问题，并给出收敛方向。结论先行：**逻辑层不需要推倒重来（方向已正确），需要收敛的是物理编排层。**

---

## 2. 现状剖析：两层架构

### 2.1 逻辑层 —— 已经通用、可组合

IMV rewrite pipeline（`src/sql/optimizer/rewrite/imv/pipeline.rs`）：

```
delta-marker → [join-delta] → [union-delta] → [aggregate-state] → delta-pushdown
            → scan-binding → action-propagation → apply-key → marker-cleanup → validation
```

- 两个 marker 算子：`ImvDelta`（change-stream，携带 `__change_op` 动作列）+ `ImvVersion`（snapshot-as-of）。
- 每个"难算子"一条 structural rule：`RewriteJoinAggregateDelta`、`RewriteUnionAggregateDelta`(A 族)、`RewriteTopLevelUnionDelta`(B 族投影)、`RewriteAggregateState`；unary（project/filter）由通用的 `PushDeltaThroughUnary` 处理。
- **rule 之间通过 marker 契约组合**：A 族 = `union-delta`（把 `δ(Union)=Union(δ(branch))`）+ `aggregate-state`（消费"已带 marker 的 union"）两个独立 rule 串联，**不是单独 case**。join-delta 内部也是造一个 union 再交给 aggregate-state。
- `scan-binding`(`BindIcebergScanRule`)的 `resolve_snapshot_window` 按 `mv_ctx.base_refs` + `pin` 为**每个** scan 独立绑定 delta 窗口——天然多-base，与算子组合无关。

这一层背后是代数化的 delta 规则：`δ(Filter)=Filter(δ)`、`δ(Union)=Union(δ)`、`δ(Join)=Join(δ,old)∪Join(new,δ)`、`δ(Agg)=signed-state-merge`。**已经接近"每算子一条规则、递归组合"的通用形态。**

### 2.2 物理层 —— 半统一，外层编排重复

入口与 dispatch（`src/engine/mv/iceberg_refresh.rs`）：

- 执行入口：`refresh_iceberg_mv` → `refresh_iceberg_mv_with_planned_partitions`；生产路径 `execute_iceberg_mv_refresh` 也汇入 `refresh_iceberg_mv_with_planned_partitions`。
- 按 shape dispatch 到：`refresh_single_aggregate_iceberg_mv` / `refresh_fan_in_aggregate_iceberg_mv`(A 族，本次新增) / `refresh_join_aggregate_iceberg_mv` / `refresh_join_mv` / `refresh_iceberg_union_projection_mv` / 单 base projection / fail-fast(B 族 aggregate)。

**核心 apply 已经统一**：上述各路径最终都调同一个
`incremental_refresh_iceberg_mv_with_changes(state, ctx, &[RewriteMergeBaseChange], options)`，
只是 `RewriteMergeRefreshOptions` 不同：

| shape | apply_key_column | apply_key_value_type | rewrite_evidence |
| --- | --- | --- | --- |
| projection/filter | `__nova_base_row_id` | `Int64` | `None` |
| aggregate / join-aggregate / A 族 | `__row_id__` | `Utf8` | `Aggregate` / `JoinAggregate` |
| B 族 projection | `__nova_base_row_id` | `BranchInt64` | `None` |

apply 定位器也已按类型分好：`locate_target_rows_by_apply_key`(Int64) / `locate_target_rows_by_string_apply_key`(Utf8) / `locate_target_rows_by_branch_apply_key`(BranchInt64)。

**真正重复的是外层编排**：每个 `refresh_*_mv` 都各自做一遍——
base 表 load、多-base pin(`RefreshSnapshotPin::capture`）、per-shape 的 schema-contract 校验、`has_previous/all_current` 的 first/metadata/incremental 决策、构造 `Vec<RewriteMergeBaseChange>`、staging-branch first-refresh、metadata-only finalize。本次实现 A 族时，`refresh_fan_in_aggregate_iceberg_mv` 几乎是 `refresh_iceberg_union_projection_mv` 的骨架镜像——这正说明该骨架可提取。

### 2.3 为什么物理层比逻辑层更"厚"

NovaRocks 把结果物化进 **Iceberg target 表**（无原生 PK upsert），增量 apply 必须：按 apply key 在 target 文件里**定位**命中行 → 写 **position delete** → 追加 **insert** → 单次 commit。这比"输出到原生 by-PK upsert 的 OLAP 表"重。这是物理编排更复杂的客观原因，不是设计失误（见 §3.2 对比）。

---

## 3. StarRocks 参考（`~/project/starrocks`，fe/fe-core）

StarRocks 维护 MV 数据有**两套机制**，恰好对应"通用但粗"和"细但窄"两端。

### 3.1 PCT（Partition Change Tracking）—— 生产默认，operator-agnostic

`MVPCTRefreshProcessor`：检测哪些 base 分区变了 → 经 `MVTimelinessRangePartitionArbiter` 做**分区范围映射**（完全不看 join/aggregate/projection 形态）→ **重跑 MV 定义 SQL 的那个分区子集**（`INSERT OVERWRITE`）。

- 优点：零 per-operator 逻辑，任意 SQL 都能刷，**用"重算"彻底回避组合爆炸**。
- 代价：粒度粗——1 行变化重刷整个分区；任何**非分区** base 表变化 → **全量重建**（`MVTimelinessRangePartitionArbiter.java:68`）。I/O 正比于分区大小，不是 delta 大小。

### 3.2 IVM —— 与 NovaRocks 同构的 marker-pushdown 框架（新，默认关闭）

`sql/optimizer/rule/ivm/`（2026-04 起，`enable_ivm_refresh` 默认 `false`）：

- 两个 marker：`LogicalDeltaOperator`(`__ACTION__`) + `LogicalVersionOperator`；`IvmRewriter` 把它们下推到 scan，**未收敛即硬报错**（不静默回退）。
- **每算子一对 rule**（6 Delta + 6 Version，共 12 个）：`IvmDeltaJoinRule` 实现 `δ(A⋈B)=(ΔA⋈B_from)∪(A_to⋈ΔB)`；`IvmDeltaAggregateRule` 用 `state_union` 对 `__ROW_ID__=encode_row_id(group_keys)` 做 signed-state merge；`IvmDeltaUnionRule` 把 Delta 推进每个 union child。
- 成本 **O(算子数)，不是 O(组合数)**：新组合零代码，新算子 +2 rule。
- 输出到 **PK OLAP 表**（`INSERT ... __op`，引擎原生 by-PK upsert），所以 refresh processor 很薄。

**与 NovaRocks 的同构对照**：

| 维度 | StarRocks IVM | NovaRocks IMV |
| --- | --- | --- |
| change-stream marker | `LogicalDeltaOperator` + `__ACTION__` | `ImvDelta` + `__change_op` |
| snapshot marker | `LogicalVersionOperator` | `ImvVersion` |
| 每算子 delta rule | `IvmDelta{Join,Aggregate,Union}Rule` | `join_delta` / `aggregate_state` / `union_delta` |
| apply key | `encode_row_id(group_keys) → __ROW_ID__` | `__row_id__ = hex\|join(gk)` |
| signed merge | `state_union` over `_combine` | `*_state_signed` + `AggregateStateMerge` |
| change op | `__ACTION__` (UPSERT/DELETE) | `__change_op` |
| 输出目标 | PK OLAP 表（原生 upsert） | Iceberg（locate + position-delete + insert） |

### 3.3 对 NovaRocks 的启示（含诚实校准）

1. **方向已对**：NovaRocks 逻辑层 = StarRocks 最新押注的通用 IVM 框架，不是 case-by-case。
2. **NovaRocks 在最难处更超前**：StarRocks IVM 当前 **append-only Iceberg + inner/cross join only，base DELETE 硬拒绝**（`MVIVMRefreshProcessor.java:290`，"Drop and recreate to recover"），最难的 delete-retraction + 不可逆聚合（MIN/MAX）正是其止步处。而 NovaRocks 已支持 base INSERT/DELETE/UPDATE + signed-state retraction（join-aggregate / A 族测试覆盖）。
3. **统一性来自输出模型**：StarRocks IVM 的薄 processor 得益于输出 PK OLAP 表。NovaRocks 输出 Iceberg，apply 必然更重——但**仍可统一**：rewrite 产出统一 change-stream，一个引擎按 apply-key 契约消费。
4. **安全网思路**：StarRocks 用 PCT 兜底任意 IVM 不支持的 SQL，并用 CREATE-time `IvmTrialRewriter` 防"缺 rule = 错数据"。值得借鉴。
5. **通用 query-rewrite 也有边界**：StarRocks SPJG（Goldstein–Larson）查询改写很通用，但明确不支持 grouping sets/cube、outer-join 受限——印证"通用 planner 也不能适配任意 SQL 都高效增量"，有理论边界。

---

## 4. 提议设计：统一 delta-apply 引擎

核心思想：**逻辑层保持 marker-pushdown（已通用）；物理层把"按 shape dispatch 到专门 refresh 函数"换成"按算子派生 apply-key 契约 + 一个统一引擎消费 change-stream"。**

### 4.1 ApplyKeyContract —— 抽象 apply key 的类型与定位语义

把当前散落在 `RewriteMergeRefreshOptions{apply_key_column, apply_key_value_type}` 的信息提升为一个一等契约：

```text
ApplyKeyContract {
    column: &str,                 // __nova_base_row_id / __row_id__ / ...
    kind: ApplyKeyKind,           // Int64 | Utf8 | Composite{ prefix: BranchId, inner: Box<ApplyKeyKind> }
    // 定位语义：如何在 target 表里按该 key 命中物理行
}
```

- 由 plan **自底向上派生**：projection/filter → `BaseRowId`；aggregate → `GroupRowId`；带 `__branch_id__` 的 union → 在 inner key 外包一层 `Composite`（或采用 §5 的 row_id 编码方案，使 inner key 自带 branch）。
- 取代"shape → 选哪个 `locate_target_rows_by_*`"的 dispatch：locator 成为契约 `kind` 的函数。

### 4.2 统一编排骨架（MultiBaseRefreshDriver）

把各 `refresh_*_mv` 的公共流程提取为一个骨架，shape-specific 的部分变成注入参数：

```text
unified_refresh(bases, contract_validator, apply_key_contract, first_refresh_sql_builder):
  1. load 所有 base 表
  2. RefreshSnapshotPin::capture(all bases)  + uuid 校验
  3. 对每个 base 跑 contract_validator（shape 提供的校验闭包）
  4. has_previous / all_current → 决策 First / MetadataOnly / Incremental
  5. First       → first_refresh_sql_builder 产 state/physical SQL → run → write → commit
     MetadataOnly→ finalize_iceberg_mv_metadata_only_refresh
     Incremental → 构造 Vec<RewriteMergeBaseChange> → incremental_refresh_iceberg_mv_with_changes(apply_key_contract)
```

现有的 `refresh_single_aggregate` / `refresh_fan_in_aggregate` / `refresh_join_aggregate` / `refresh_union_projection` 退化成**薄 wrapper**：各自只提供 (a) contract validator、(b) apply-key 契约、(c) first-refresh SQL 构造器。`incremental_refresh_iceberg_mv_with_changes` 已是统一 apply，无需大改。

### 4.3 统一 first-refresh

当前 first-refresh 有两套：`first_refresh_iceberg_aggregate_mv`（state-shaped 单 SELECT，要求 outer 是 `SetExpr::Select`）+ `rewrite_union_projection_full_refresh_select_with_pin`（union 投影）。统一为一个 **first-refresh SQL builder**：给定 plan，产出 state-shaped / row-id-stamped 的 full-refresh SQL（含 union 分支拼接 + branch 标记），交给现有 `run_mv_full_select_chunks` + write/commit。

### 4.4（可选）PCT-style 全量 fallback + CREATE-time trial

- **Fallback**：当 rewrite 判定 plan 不可增量（出现未支持算子/组合），退回"全量/分区重刷 defining SQL"。这让**通用性由 fallback 保证**，IMV 引擎只需在能证明正确的形态上做细粒度——与 StarRocks PCT+IVM 并存的策略一致。
- **CREATE-time trial**：建 MV 时就跑一遍 delta rewrite（对齐 StarRocks `IvmTrialRewriter`），缺 rule 立即在 CREATE 报错，杜绝"运行期才发现写错数据"。NovaRocks 现有 `ActionColumnValidationRule` / `UnresolvedMarkerCheckRule` 已是雏形。

---

## 5. B 族在 IMV-v2 下的落地（首个验证场景）

B 族（`Union(Aggregate(b₁)..Aggregate(bₙ))`，UNION 在聚合之上，bag semantics）在统一引擎下应当只需要：

- **逻辑层**：新增一条 `RewriteBranchUnionAggregateDelta` rule（`δ(Union(Agg))=Union(δ(Agg) per branch + 注入 branch 身份)`），复用现有 `aggregate-state` 的 signed-merge 构造。这就是 StarRocks `IvmDeltaUnionRule` 的对应物。
- **apply-key 契约**：采用 **A2 方案** —— 让 branch i 的 `__row_id__` 编码 `branch_id`（在 `build_row_id_array`，`src/connector/starrocks/table/mv_agg_state.rs:743`，及 first-refresh SQL 两处一致地 prepend `branch_id`）。这样两个 branch 的同 group key 天然得到不同 row_id，`AggregateStateMerge` 按 row_id 过滤即**天然 branch-isolated**，**复用现有 `Utf8` string locator，不需要 composite locator、不需要 branch-scoped scan、不动 merge 核心**。
- **first-refresh**：统一 first-refresh builder 的 union 变体（每 branch state-shaped SELECT + branch 编码 row_id + `__branch_id__` 列，UNION ALL 拼接）。
- **物理 orchestration**：**无新 `refresh_*_mv` 函数**——只通过 unified_refresh + 上述契约落地。

> 关键正确性背景（必须保留为验收）：`AggregateStateMergeOp` 当前纯 `__row_id__`-keyed，零 branch 感知；若不让 row_id 含 branch，跨 branch 同 group key 会被错误折叠（违反 bag semantics）。A2 用"row_id 编码 branch"在不动 merge 核心的前提下解决它。

---

## 6. 迁移路径（分阶段，每阶段保持全绿）

- **Phase 0（已完成）**：A 族 `Aggregate(UNION ALL)` fan-in execute 已实现（`refresh_fan_in_aggregate_iceberg_mv`，含首刷/增量/跨-branch 合并/INSERT/DELETE 端到端测试，`iceberg_refresh` 模块 78 测试无回归）。它镜像了 join/union-projection 的多-base 编排——是 Phase 1 的提取素材。
- **Phase 1**：提取 `unified_refresh` 多-base 编排骨架；现有 `refresh_*` 改薄 wrapper（注入 contract validator + apply-key 契约 + first-refresh builder）。**验收：iceberg-ivm 全 suite 行为不变。**
- **Phase 2**：落地 `ApplyKeyContract`（参数化 `RewriteMergeRefreshOptions`）+ 统一 first-refresh builder。
- **Phase 3**：B 族 aggregate 作为"在统一引擎上新增 shape"的首个验证——只加 rule + 契约，无新 refresh fn（§5）。**验收：B 族集成测试绿（同 group key 跨 branch 不合并、删一个 branch 不影响另一个）。**
- **Phase 4（可选，按 §8 决策）**：PCT-style 全量 fallback + CREATE-time trial rewrite。

---

## 7. 风险、边界、非目标

- **不改 state 模型**：保持"只物化 target 表、中间算子无 state"。这与 differential-dataflow（每中间算子物化 arrangement）不同，是 NovaRocks 资源友好的有意选择；代价是某些算子无法做到 delta-sized 增量，由 fallback 兜底。
- **理论边界**：无界 window、`DISTINCT` 聚合、不可逆聚合在删数下的某些场景，本质需要全量/更多 state；IMV 引擎应 fail-fast 到 fallback，而非给错结果。
- **重构回归风险**：现有 projection/aggregate/join shape 必须行为不变——分阶段 + 每阶段全 suite 绿 + 强回归。
- **保持现有优势**：NovaRocks 已有的 base DELETE / signed-state retraction 正确性必须在统一引擎下保留（这是相对 StarRocks IVM 的领先点）。

---

## 8. 待决策问题（供团队评审）

- **D1**：是否引入 PCT-style 全量重刷 fallback？（operator-agnostic 安全网，显著提升"任意 SQL 可刷"，但需分区/重刷基础设施）
- **D2**：`ApplyKeyContract` 的抽象边界——Iceberg-specific 还是预留通用后端？
- **D3**：`unified_refresh` 的提取形态——driver struct + 注入闭包，还是 per-shape trait 实现？
- **D4**：是否引入 CREATE-time trial rewrite（防缺 rule 写错数据）？
- **D5**：B 族 apply-key——**A2（row_id 编码 branch，推荐：最简、复用现有 locator/merge）** vs 设计文档原方案（composite `(branch_id, group_row_id)` locator + branch-scoped scan，触及更深 codegen）。本 RFC 推荐 A2。

---

## 9. 验收标准

- 现有 shape（projection / aggregate / join / join-aggregate / A 族）在统一引擎上**行为不变**（iceberg-ivm 全 suite 绿）。
- **新增算子或组合的边际成本 = O(1) rule + 一个 apply-key 契约**，不再需要新的 `refresh_*_mv` orchestration 函数。
- B 族 aggregate 通过统一引擎落地（无专门 refresh fn），且满足 bag-semantics 正确性（同 group key 跨 branch 不合并；删/增一个 branch 不影响其它 branch）。
- 可选 fallback 落地后：rewrite 不支持的 MV 定义在 CREATE 期明确报错或在 refresh 期走全量重刷，无静默错误结果。

---

## 10. 附：本 RFC 依据的关键代码位置

NovaRocks：
- 逻辑层 pipeline：`src/sql/optimizer/rewrite/imv/pipeline.rs`；marker/rules 同目录（`join_delta.rs` / `union_delta.rs` / `aggregate_rewrite.rs` / `delta_pushdown.rs` / `scan_binding.rs`）。
- 物理编排 + 核心 apply：`src/engine/mv/iceberg_refresh.rs`（`refresh_*_mv`、`incremental_refresh_iceberg_mv_with_changes`、`RewriteMergeRefreshOptions`、first-refresh）。
- apply 定位器 / merge：`src/engine/mv/iceberg_target_apply.rs`、`src/engine/mv/iceberg_aggregate_state.rs`、`src/exec/operators/aggregate_state_merge.rs`；row_id 编码 `src/connector/starrocks/table/mv_agg_state.rs:743`。

StarRocks（`~/project/starrocks/fe/fe-core/src/main/java/com/starrocks/`）：
- 三模式 dispatch：`scheduler/mv/MVRefreshProcessorFactory.java`；PCT：`scheduler/mv/pct/MVPCTRefreshProcessor.java`；分区映射：`catalog/mv/MVTimelinessRangePartitionArbiter.java`。
- IVM：`sql/optimizer/rule/ivm/IvmRewriter.java` + `sql/optimizer/rule/ivm/`（`IvmDelta{Join,Aggregate,Union}Rule.java`）；marker：`sql/optimizer/operator/logical/Logical{Delta,Version}Operator.java`；契约：`sql/analyzer/mv/IVMAnalyzer.java`、`sql/optimizer/rule/ivm/common/IvmOpUtils.java`。
- SPJG 查询改写：`sql/optimizer/rule/transformation/materialization/`。
