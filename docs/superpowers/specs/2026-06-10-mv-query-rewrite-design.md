# MV 查询透明改写（单表 SPJG + 聚合上卷）— 设计

- 日期: 2026-06-10
- 状态: 设计已评审，待写实施计划
- 范围标签: optimizer, materialized-view, cascades, iceberg, rewrite

## 1. 背景与问题

NovaRocks 已有完整的 MV 生命周期（CREATE / REFRESH / DROP，含增量刷新基础设施），
但**用户查询不会被透明改写到 MV 上**：查询写的是 base table 就扫 base table，MV
只能被显式点名查询。StarRocks 的核心竞争力之一正是优化器侧的 MV 透明改写
（`MaterializedViewRewriter` / `AggregatedMaterializedViewRewriter`，基于
"Optimizing Queries Using Materialized Views: A Practical, Scalable Solution"
的 SPJG 统一匹配框架）。

NovaRocks 现状盘点（已验证）：

- **已有**：MV 元数据持久化（`src/meta/repository/mv.rs` 的 `StoredMvDefinition`，
  含 `select_sql`、`base_table_refs`、`last_refresh_snapshots` 每 base 表 snapshot
  pin、`last_refresh_ms`、`max_staleness_ms`）；Cascades 优化器（memo 支持同
  group 多逻辑表达式 `Memo::add_expr_to_group`，变换规则框架
  `src/sql/optimizer/rule.rs` 的 `Rule` trait，规则每次 `optimize()` 实例化于
  `cascades_rules::all_transformation_rules()`）；analyzer 可把任意 SQL 重新分析为
  `LogicalPlan`；统计基建（Iceberg manifest 行数 + Puffin NDV）。
- **缺失**：优化器侧拿不到 MV 元数据；没有查询计划 ↔ MV 定义计划的匹配器；没有
  改写规则；没有新鲜度判定；会话变量 `enable_materialized_view_rewrite` 未接线。
- **注意**：现有 `src/sql/optimizer/rewrite/imv/` 管线是**刷新时**改写 MV 自身定义
  查询用的（增量化），与**用户查询改写**是两套机制，本设计不复用其改写规则，仅
  共享 MV 元数据。

## 2. 目标与范围

### 目标（MVP，第一个可合并里程碑）

- 单 base table 的 SPJG 查询（Scan → [Filter] → [Project] → [Aggregate]）能被透明
  改写为扫描 Iceberg MV 的目标表，覆盖：
  - 精确匹配（OnlyScan 等价物）：select/filter/project 查询命中 SPJ MV；
  - 谓词补偿：查询数据范围 ⊂ MV 数据范围时，差额谓词加回 MV scan 之上；
  - 聚合上卷（AggregateScan 等价物）：查询 group-by ⊆ MV group-by 时上卷
    SUM/MIN/MAX/COUNT。
- 改写以 **memo 替代表达式**形式注入，由现有 CBO 代价模型在原计划 / 各 MV 候选
  之间择优。
- 仅当**所有 base table 当前 snapshot 与 MV 上次刷新 pin 完全一致**才允许改写
  （严格一致语义，结果与扫 base table 完全等价）。
- 会话级开关 `enable_materialized_view_rewrite`（默认开）+
  `disable_optimizer_rules = 'MvRewrite'` 两条关闭通道。

### 非目标（明确排除，留作后续 roadmap）

- 多表 join 匹配（StarRocks 的 COMPLETE/QUERY_DELTA/VIEW_DELTA 模式）。
- 分区级新鲜度补偿与 UNION ALL 改写（stale 分区回读 base table）。
- staleness 宽容模式（`max_staleness_ms` 在改写路径暂不生效；字段保留给刷新调度）。
- 文本/AST 精确匹配改写（`TextMatchBasedRewriteRule` 等价物）。
- 函数等价类（date_trunc 蕴含、IN 列表蕴含、OR-range 合并简化）。
- managed-lake / 本地 parquet base table 或 MV 存储。
- AVG 及 `agg_state` 类高级上卷（AVG 的 SUM/COUNT 分解后续做）。
- BestMvSelector 式启发式预选（候选少，CBO 直接全量比较即可）。

## 3. 需求决策记录

| 决策点 | 结论 | 理由 |
|---|---|---|
| MVP 能力层级 | 单表 SPJG + 聚合上卷 | StarRocks 框架 20% 代码覆盖 80% 价值的子集；为多表匹配沉淀谓词分类/列映射地基 |
| 新鲜度语义 | 仅严格 snapshot 一致 | 语义最干净，改写结果永远正确；宽容模式后续以会话变量加入 |
| 存储后端 | 纯 Iceberg 端到端（base 与 MV 都是 Iceberg） | snapshot pin 机制最成熟，`last_refresh_snapshots` 即为此设计 |
| 优化器挂载点 | memo 注入替代表达式，CBO 择优 | 对齐 StarRocks；多候选竞争时能选最优；不确定性收益场景不误伤 |
| 谓词匹配强度 | 三分类（等值/范围/残余）+ 区间蕴含 + 谓词补偿 | 实用性与实现量平衡点；多表匹配的必要地基 |

## 4. 总体架构

```
用户查询
  ↓ analyze → plan_query（不变）
  ↓
[新增] MV 候选准备（engine 层，对应 StarRocks MvRewritePreprocessor）
  ├─ meta repository 反查：base_table_refs 与查询表相交的 Iceberg MV
  ├─ 严格新鲜度：base 当前 snapshot == last_refresh_snapshots pin
  ├─ analyze(select_sql) → LogicalPlan，验证单表 SPJG 形状
  ├─ 构造 MV 目标表 TableDef（无需全局注册）+ 统计信息并入 table_stats
  └─ 产出 Vec<MaterializationContext>
  ↓
optimize(plan, table_stats, factory, dict_provider, mv_candidates)  ← 签名扩展
  ↓
[新增] Cascades 变换规则 MvRewriteRule（对应 OnlyScanRule + AggregateScanRule）
  ├─ 匹配 memo 中 Aggregate→[Project]→[Filter]→Scan 与 [Filter]→Scan 子模式
  ├─ 逐候选统一匹配：谓词蕴含 + 列映射 + group-by 子集 + 上卷映射
  └─ 成功 → Memo::add_expr_to_group 注入 MV-scan 替代表达式
  ↓
CBO 代价搜索择优（MV 行数小 → 代价低 → 胜出；原计划保留，绝不强制替换）
```

分层原则：**候选准备在 engine 层**（需要访问 meta repository、Iceberg catalog、
统计加载，优化器层拿不到这些句柄）；**匹配与注入在优化器层**（纯逻辑，可单测）。
两层之间以 `MaterializationContext` 为唯一契约。

改写永远是可选优化：任何候选在任何阶段失败 = 跳过该候选（warn 日志），查询
绝不因改写报错。

## 5. 候选准备（engine 层）

新增 `src/engine/mv_rewrite_prep.rs`，在 analyze/plan 之后、`optimize()` 之前调用
（`enable_materialized_view_rewrite=false` 时整体跳过，零开销）：

1. **发现**：列出 meta repository 中 `storage_engine == "iceberg"` 且
   `base_table_refs` 与查询引用表集合相交的 MV。候选上限 **16**（对齐 StarRocks
   `cbo_materialized_view_rewrite_related_mvs_limit` 默认值，
   `SessionVariable.java:2954`），超出截断并记日志；实现为模块级命名常量
   （遵循 `EXPLORE_MAX_GROUPS` 先例，`src/sql/optimizer/mod.rs:53`）。
2. **严格新鲜度**：逐 base table 取当前 Iceberg snapshot id 与
   `last_refresh_snapshots` 中的 pin 比对，任一不一致或 MV 从未刷新（无 pin）
   即丢弃。在途刷新不影响判定：pin 永远指向已提交快照。
   **竞态说明**：snapshot 可能在「检查通过」与「执行」之间推进（base 新写入，
   或 MV 刷新提交导致 MV scan 以 CurrentSnapshot 绑定读到更新数据）。该窗口
   等价于查询提早/推迟了几毫秒被接收，且 MVP 中被替换的是单一完整子树、无
   混合版本 join 风险——接受此竞态，不做额外 pin。
3. **MV 计划分析**：用查询同一个 `CatalogProvider` 对 `select_sql` 做
   analyze + plan。ColumnId 不冲突需要新增一个小 API：现有 `analyze()` 硬编码
   新建 factory（`src/sql/analyzer/mod.rs:61-63`），而 `AnalyzerContext` 本就持有
   `Rc<RefCell<ColumnRefFactory>>`，因此加一个 `analyze_with_factory(query,
   catalog, db, factory)` 变体即可（低风险；engine 在 plan_query 之后、
   `optimize()` 移交 factory 之前调用它分析各候选）。验证形状严格为
   `Scan → [Filter] → [Project] → [Aggregate]`（单 Scan、无 join/CTE/窗口/
   HAVING/ORDER BY/LIMIT/DISTINCT），不符即丢弃。
4. **目标表 TableDef 构造**：普通 SELECT 的 Iceberg 表如今是经
   `CatalogMgrProvider` 在 analyze 期间惰性解析的（`query_prep.rs:1-3` 模块注释），
   不存在「SELECT 注册路径」。MV 目标表用 iceberg connector backend 对直接构造：
   `catalog_backend("iceberg").load_table` + `table_source("iceberg").build_table_def`
   （`src/connector/iceberg/catalog/backend.rs:195-241`，产出
   `ScanSource::IcebergDataFiles { table, files: [], cloud_properties,
   binding: CurrentSnapshot }`，files 留空、扫描时解析 splits）。先例：
   `register_external_table_by_name`（`query_prep.rs:430`，ANALYZE 与 IMV 刷新
   已这样注册 SQL 未点名的表）。由于 `LogicalScanOp` 内嵌完整 `TableDef`，
   **无需任何全局 catalog 注册**——TableDef 随 `MaterializationContext` 传入
   优化器即可被 codegen 经 `ScanSource` 分派到 Iceberg connector
   （`src/sql/codegen/fragment_builder.rs:451-482`）。
5. **统计注入**：加载目标表统计（snapshot 行数 + Puffin NDV，复用
   `build_table_statistics_with_ndv`）并入 `table_stats`。键格式以现有
   `build_table_stats_from_plan` / `collect_scan_stats`
   （`src/engine/mod.rs:3033/3042`）为准：**裸表名**（无库限定），且查找侧
   会 lowercase（`src/sql/optimizer/stats.rs:1045-1050`），故以
   `target_table.name` 的小写形式插入。裸名键的跨库同名碰撞是既有限制，
   不在本期处理（MV 目标表与查询中其他表同名时放弃该候选并记日志）。
6. **产出**：

```rust
pub(crate) struct MaterializationContext {
    pub mv_name: String,              // 诊断与 EXPLAIN 注记用
    pub mv_plan: LogicalPlan,         // MV 定义的分析结果（已验证 SPJG 形）
    pub target_table: TableDef,       // MV 目标表（改写后扫它）
    pub target_database: String,      // LogicalScanOp 所需的库限定
    // MV select list 第 i 项 ↔ 目标表第 i 个「可见列」。注意物理布局含
    // 内部列：聚合 MV 前置 __row_id__（mv_agg_state.rs:182-189）、尾部
    // __agg_state_* 状态列；非聚合 MV 尾部 apply-key / __branch_id__。
    // 实现以 visible_output_order()（mv_agg_state.rs:55-86）或持久化的
    // schema contract 为权威来源做可见列映射，不做裸下标对应；
    // MV scan 只投影可见列，内部列一律排除。
}
```

## 6. 统一匹配算法（优化器层）

新增 `src/sql/optimizer/cascades_rules/mv_rewrite/`，规则名 `MvRewrite`，
`RuleType::Transformation`。规则实例在 `optimize()` 时以候选列表构造：候选
非空时**单独追加**一个 `MvRewriteRule` 实例到变换规则列表，不改
`all_transformation_rules()` 的签名（规则名经 `name()` 自动进入
`is_known_rule_name` 的枚举范围）。

匹配的顶层模式：规则 `matches` 命中 `LogicalAggregate` 或 `LogicalFilter` 或
`LogicalScan`；`apply` 从该 MExpr 出发沿子 group 取**首个逻辑表达式**还原
`Aggregate→[Project]→[Filter]→Scan` / `[Filter]→Scan` 子树视图。为避免同一
group 重复注入，注入的替代表达式带 `mv_rewritten` 标记（或以 group 内已存在
同形 MV scan 判重）。

形状组合矩阵：

| 查询 \ MV | SPJ MV（无聚合） | SPJG MV（有聚合） |
|---|---|---|
| SPJ 查询 | ✓ 直接改写 + 谓词补偿 | ✗（MV 已丢明细行） |
| SPJG 查询 | ✓ 查询聚合保留在 MV scan 之上 | ✓ 需 group-by ⊆ 且函数可上卷 |

### 6.1 谓词拆分与蕴含（对应 StarRocks `PredicateSplit`）— `predicate_split.rs`

- 查询侧谓词 = scan 上的 predicates + Filter 节点谓词的合取范式拆分；MV 侧同理。
- 三分类：**等值**（col = 常量）、**范围**（col op 常量，op ∈ {<,<=,>,>=,=}，
  含 BETWEEN 展开）、**残余**（其余一切：`!=`、LIKE、IN、`IS [NOT] NULL`、
  函数调用、OR 顶层等）。`!=` 刻意归入残余：穿孔区间的蕴含判断收益低、易错，
  MVP 只做精确匹配。
- 命中条件（MV 数据 ⊇ 查询数据）：
  - 等值/范围：按列归并为区间集后做**区间蕴含**判断（查询区间 ⊆ MV 区间）；
  - 残余：表达式规范化（常量折叠、交换律排序）后要求 **MV 残余集合 ⊆ 查询残余
    集合**（逐条结构化精确匹配）。
- NULL 语义：WHERE 谓词按三值逻辑过滤掉求值为 NULL 的行，因此区间蕴含只需
  在非 NULL 域上成立即可；`IS NULL` / `IS NOT NULL` 不参与区间逻辑（残余类
  精确匹配），可空列不需要特殊处理。
- **补偿谓词** = 查询谓词中 MV 未保证的差额（查询独有的范围收紧、查询独有的
  残余项），改写后作为 Filter 加在 MV scan 之上。
- 补偿谓词引用的列必须能通过 6.2 的列映射改写为 MV 输出列，否则匹配失败
  （MV 没物化该列就无法过滤）。

### 6.2 列映射（StarRocks `EquationRewriter`/`ColumnRewriter` 的单表简化版）— `column_mapping.rs`

- 从 `mv_plan` 提取「MV 输出位置 i → 定义表达式 e_i（base 列上的表达式树）」。
- 反向构建重写表：查询中的表达式自顶向下尝试与某个 e_i **结构化精确匹配**
  （表达式树规范化后相等：列引用按 base 列身份比对、常量折叠、可交换运算符
  排序），命中则整棵替换为 MV 目标表第 i 个可见列的列引用（新 ColumnId）。
- 查询输出列、补偿谓词、查询侧保留的聚合/分组表达式全部经此重写；任何
  无法重写的 base 列引用残留 → 匹配失败。
- MVP 不做函数等价（如 `date_trunc('month', d)` 蕴含 `date_trunc('day', d)`）。

### 6.3 聚合上卷（对应 `AggregatedMaterializedViewRewriter`）— `aggregate_rollup.rs`

前提：查询与 MV 均为 SPJG 形且 6.1/6.2 通过。

- **group-by 完全相等**（重写后集合相等）：免上卷。查询聚合输出一对一改写为
  MV 对应聚合列的引用；查询聚合函数与 MV 聚合函数必须逐个结构化相等。
- **查询 group-by ⊂ MV group-by**：需上卷，白名单：

| 查询聚合 | MV 物化列 | 上卷函数 |
|---|---|---|
| SUM(e) | SUM(e) | SUM |
| MIN(e) | MIN(e) | MIN |
| MAX(e) | MAX(e) | MAX |
| COUNT(*) | COUNT(*) | SUM（标量聚合时包 COALESCE，见下） |
| COUNT(e)（e 不可为 NULL 时等价 COUNT(*)，否则需 MV 物化 COUNT(e)） | COUNT(e) | SUM（同上） |

- **空输入边界（正确性约束）**：查询 group-by 为空（标量聚合）且 MV scan
  经补偿过滤后无行时，原查询 `COUNT(*)` 必须返回一行 `0`，而 `SUM(cnt)` 对
  空输入返回 `NULL`。因此 COUNT 类上卷在**标量聚合**场景必须生成
  `COALESCE(SUM(cnt), 0)`。SUM/MIN/MAX 标量聚合对空输入本就返回 NULL，
  与上卷后行为一致，无需处理。有 group-by 的上卷不受影响：MV 中的组都来自
  真实数据，过滤后留下的组内行数 ≥ 1。

- 拒绝条件（匹配失败，不注入）：查询含 DISTINCT 聚合且 MV 为 SPJG；查询聚合
  函数不在白名单（含 AVG）；MV 聚合输出中找不到对应物化列；查询 group-by
  含无法映射到 MV 输出的表达式。
- MV 定义含 AVG：仅 group-by 完全相等路径可用（直接映射），上卷路径不可用。
- 有补偿谓词时：补偿 Filter 位于 MV scan 之上、上卷 Aggregate 之下。
  - **SPJG MV 的通用约束**：SPJG MV 输出只有 group-by 列与聚合列，对聚合列
    做行过滤会改变语义，因此**补偿谓词仅允许引用映射后为 MV group-by 列的
    列**（group-by 相等与上卷两条路径同样适用），否则匹配失败。SPJ MV 无此
    约束（任何可见列都可补偿过滤）。
  - group-by 完全相等 + 补偿谓词：无需保留查询侧 Aggregate——MV 每行对应
    一个完整 group，按 group-by 列过滤后每组仍恰好一行，直接映射安全。
- HAVING（查询侧聚合上方的 Filter）：保留在改写后计划顶部，经 6.2 重写。

### 6.4 替代表达式构造与 ColumnId 约束

构造 `MV scan（target_table，新 ColumnIds，自 memo.factory 分配） →
[补偿 Filter] → [上卷 Aggregate | Project]`。MV scan 需补全
`LogicalScanOp` 全部七个字段（`src/sql/optimizer/operator.rs:74-85`）：
`database`/`table`/`columns`/`predicates` 之外，`alias = None`、
`required_columns = None`、`dict_columns = vec![]`。可执行性由
`TableDef.source = ScanSource::IcebergDataFiles` 保证（codegen 按
`ScanSource` 分派 connector，与表名无关）。

- **memo group 等价硬约束**：注入表达式的输出列必须与原 group 的
  `LogicalProperties.output_columns` 在 ColumnId 层面一致。实现方式：顶层节点
  （Project 或上卷 Aggregate）的输出绑定**复用原子树顶层节点的输出 ColumnId**，
  内部引用全部指向 MV scan 的新 ColumnId。
- SPJ 改写顶层是 Project（旧输出 ColumnId := MV 列引用/重写表达式）；SPJG 上卷
  顶层是 Aggregate（聚合输出复用原 Aggregate 的输出 ColumnId）。
- 统计：MV scan 的行数/NDV 来自候选准备阶段注入的 `table_stats`，logical props
  推导与普通 scan 完全一致，CBO 无需特判。

## 7. 会话控制与可观测性

- 新会话变量 `enable_materialized_view_rewrite`（BOOL，默认 `true`）：
  - `false` 时 engine 层跳过候选准备（优化器自然无候选）；
  - 名称对齐 StarRocks（现有 sql-tests 中已出现该名字但未接线）。
- `SET disable_optimizer_rules = 'MvRewrite'` 可在优化器层关闭注入。机制说明：
  规则加入 `all_transformation_rules()` 后其 `name()` 自动被
  `sql::optimizer::is_known_rule_name`（`mod.rs:175-184`）枚举到；SET 时未知名
  仅 warn 不拒绝（`src/server/mod.rs:993-1003`），真正的强制点是 `explore()` /
  `implement()` 中逐规则检查的 `OptimizerOptions::is_enabled`
  （`mod.rs:230/286`）。
- EXPLAIN：改写命中后计划中可见 MV 目标表的 scan（表名即证据）；在 Verbose
  输出的 scan 节点追加 `rewritten with mv: <mv_name>` 注记（nice-to-have，
  实现于 `src/sql/explain.rs`）。

## 8. 测试策略

### 单元测试（优化器层，无服务器）

- 谓词三分类、区间蕴含、补偿谓词计算（含 BETWEEN、边界开闭、`!=`/IS NULL
  归入残余）。
- 列映射：直接列、表达式树匹配、映射失败残留检测。
- 上卷映射表、拒绝条件矩阵（DISTINCT、AVG、白名单外函数）。
- 标量聚合空输入：COUNT 上卷生成 COALESCE、SUM/MIN/MAX 保持 NULL。
- 替代表达式 ColumnId 绑定正确性（输出列与原 group 一致）。

### sql-tests 新 suite `mv-rewrite`（Iceberg 环境，`@explain_contains` + 结果 golden 双校验）

- **命中类**：精确匹配；谓词补偿命中（范围收紧 / 查询多残余谓词）；group-by
  相等直接映射；group-by 子集上卷（SUM/MIN/MAX/COUNT(*)）；标量聚合上卷且
  补偿过滤后无匹配行（COUNT 返回 0 而非 NULL）；SPJ MV 上的聚合查询；
  多 MV 候选 CBO 选行数更小者。
- **不命中类**：列不覆盖；范围不蕴含（查询范围更宽）；MV 残余谓词查询不含；
  DISTINCT 聚合；AVG 上卷；补偿谓词引用聚合列；MV 为 SPJG 查询为 SPJ。
- **新鲜度生命周期**：建 MV → REFRESH → 命中；INSERT base table → 不再命中
  （EXPLAIN 断言扫 base 表）；再 REFRESH → 恢复命中。
- **开关类**：`enable_materialized_view_rewrite=false` 不改写；
  `disable_optimizer_rules='MvRewrite'` 不改写。
- 既有 suite（`optimizer`、`materialized-view`、`iceberg` 等）全量回归无 diff
  （默认开下，无匹配 MV 的查询行为不变）。

## 9. 错误处理

- 候选准备各步骤失败（MV SQL 解析失败、目标表缺失、统计加载失败、形状不符）：
  warn 日志 + 跳过该候选。
- 规则 `apply` 内任何匹配失败：静默跳过（这是常态路径，不记日志；可加 trace）。
- 不引入任何 panic 路径；MV 候选准备的错误不得升级为查询错误。

## 10. StarRocks 对应物速查（实现时参考 `~/project/starrocks`）

| NovaRocks 模块 | StarRocks 类（fe-core optimizer） |
|---|---|
| `engine/mv_rewrite_prep.rs` | `MvRewritePreprocessor` |
| `MaterializationContext` | `MaterializationContext` |
| `cascades_rules/mv_rewrite/predicate_split.rs` | `PredicateSplit` / `PredicateExtractor` |
| `cascades_rules/mv_rewrite/column_mapping.rs` | `EquationRewriter` / `ColumnRewriter` |
| `cascades_rules/mv_rewrite/aggregate_rollup.rs` | `AggregatedMaterializedViewRewriter` / `AggregateFunctionRollupUtils` |
| `MvRewriteRule`（规则本体） | `OnlyScanRule` + `AggregateScanRule` |
| 严格 snapshot 一致检查 | `query_rewrite_consistency=strict` 的简化形 |

## 11. 后续 roadmap（非本期）

1. 多表 join 匹配（COMPLETE → QUERY_DELTA → VIEW_DELTA 渐进）。
2. 分区级新鲜度补偿 + UNION ALL 改写。
3. staleness 宽容模式（利用 `max_staleness_ms`，会话变量控制）。
4. AVG 的 SUM/COUNT 分解上卷；COUNT DISTINCT → bitmap/HLL 类等价。
5. 函数等价类（date_trunc 蕴含、IN 蕴含、OR-range 简化）。
6. 文本匹配快速通道。
7. managed-lake 后端接入（事务版本号充当 snapshot pin）。
