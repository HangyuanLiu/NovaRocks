# 预聚合 / 中间聚合态 TTypeDesc 确定化(pillar P5 收尾)

状态:设计 / 待实施。本文是给后续逐阶段实施用的设计文档,不是已落地记录。

## 背景与定位

这是 type_relation 收口线(PR #302 四处 check/materialize 收口 + List↔Struct 桥接移除、
PR #303 array_agg/group_concat order/distinct 结构化)之后的**根问题**,也是代码里多处注释
反复引用的 "pillar P5"。

它是若干 latent 症状的**共同根因**:执行层某些聚合状态 slot 的**声明类型(TTypeDesc)与运行时
实际产出的类型不一致**,于是散落着一批 "tolerance"(容忍/兜底)逻辑去吞掉这个不一致。把声明类型
做成确定性的(与运行时一致),这批 tolerance 就能整体删除,并解锁 exchange merge 家族的消除。

**验收标准(全程):NovaRocks coordinator + 3 个 NovaRocks BE 的 cross-process 集群。不需要
考虑 StarRocks-FE-compatible 模式。**

## 问题的精确机制

聚合输出 slot 的定型逻辑(standalone,`src/sql/codegen/fragment_builder.rs:2724-2763`):
- `need_finalize`(最终阶段,出结果)→ 用 `agg_call.result_type`(**返回类型**)。✅ 确定。
- 非 finalize(partial / 中间阶段)→ 用 `agg_fn.intermediate_type`。

对 avg / ndv / hll / percentile 这类"序列化状态"函数,`intermediate_type` 是 **VARBINARY**
(opaque 序列化 blob;`is_opaque_binary_primitive` = HLL/OBJECT/PERCENTILE,见
`src/common/util.rs:51`)。

问题:在 **partition-topN preAgg / streaming preAgg 的"透传"pre-agg** 这类路径上,运行时
实际携带的是**裸 numeric**(pre-agg 没有真正序列化,而是把原值透传),于是
「slot 声明 VARBINARY ≠ 实际 numeric」。

> 已核实:**普通 analytic/window 输出不在此列**。standalone `visit_window`
> (`fragment_builder.rs:3354-3363`)已用 `win_expr.result_type` 给 window 中间/输出 slot
> 定型;FE-compat 的 `src/lower/node/analytic.rs:315/352` 也用 `return_type` 构建
> `WindowFunctionKind`。这与 StarRocks 一致,无需改。漂移集中在 **pre-agg 透传 / partition-topN**。

## 权威目标模型(StarRocks,已调研)

聚合中间/序列化类型**按 (函数, 参数类型) 签名确定**(avg→VARBINARY、ndv→VARBINARY、
hll_union_agg→HLL、bitmap_union→BITMAP、percentile→VARBINARY、array_agg/group_concat→STRUCT)。
关键区分:

| | GROUP BY 聚合节点 | analytic/window 节点 |
|---|---|---|
| 中间类型 | **物化** VARBINARY(多阶段 serialize/merge) | **完全跳过**:`intermediate_tuple_id` 不设 |
| 输出 slot | partial=VARBINARY,final=返回类型 | = **返回类型** |
| 输入 | 上游 partial 状态(VARBINARY) | 普通:**裸 arg**;merge 特例(preAgg 上游已产 partial state):序列化状态,`is_merge_agg` 显式标记 |
| BE 计算 | update / serialize / merge | 裸值上跑 native state,出返回类型 |

参考位置(StarRocks):
- `fe/.../catalog/FunctionSet.java`:avg 1833-1867、ndv 1371-1379、hll 1484-1496、
  bitmap 1499-1521、percentile 1910-1971、array_agg/group_concat 1303-1309。
- `fe/.../catalog/AggregateFunction.java`:`getIntermediateType`(355-364)、
  `toThrift`(404-407,intermediate==ret 时存 null)。
- `fe/.../sql/plan/PlanFragmentBuilder.java`:agg 物化中间 2476-2479;analytic 3436-3523
  (`intermediate_tuple_id` unset、输出 = 返回类型)。
- `be/src/exec/analytor.cpp`:按裸 arg 选 kernel(199-281)、native state(303-305)、
  update vs merge(1085-1102,merge 仅在上游 preAgg 已产 partial state 时)。

要点:中间类型确定;但 **window 不该用中间类型**(用返回类型 + 裸输入),只有 partition-topN 的
merge 特例才吃序列化状态,且用 `is_merge_agg` 在 plan 里区分。

## 要被删除的 tolerance 机制(确定化后)

确定化到位后,以下兜底逻辑应能整体删除(~9 处):
- `src/runtime/exchange.rs:1290-1309` —— PARTITION-TOP-N opaque-binary passthrough
  (numeric 流入声明为 VARBINARY 的 slot 时不 materialize)。
- `src/lower/node/sort.rs:468` 附近 —— 对 opaque binary 类型跳过 cast。
- `src/exec/operators/analytic_shared.rs:262-291` —— `needs_schema_adjustment`:实际列类型/
  nullability 与预声明 schema 不符时按实际重建 output schema。
- 解锁 `src/exec/chunk/type_relation.rs:327` 的 array_agg "actual-wins nullability" outlier
  收口。
- 解锁 #3:exchange `merged_exchange_schema` / `merge_exchange_field_type`、
  `reconcile_chunk_data_type`、array_agg `reconcile_data_type` 这一族「合并取 actual」改为
  「源头 conform 到 descriptor」,merge 家族消除(见下方 Phase 3)。

## 分阶段计划

### Phase 0 — 把 pre-agg 透传行为钉实(必做的前置调研)

不改代码,只调研确认,产出"哪些函数 × 哪些路径,声明 VARBINARY 但运行时产 raw"的精确清单:
1. 在 standalone 跨进程下,普通 split-agg 的 partial 阶段(非 finalize),avg/ndv/hll/percentile
   的运行时 `build_array`(serialize 路径)到底产出什么 arrow 类型?是否是 `Binary`(与声明的
   VARBINARY 一致)?
   - 看 `src/exec/expr/agg/functions/{avg,*}.rs` 的 serialize/build_array 实现 + state_combinators。
2. partition-topN preAgg(及 streaming preAgg)是否对这些函数做"透传"(不序列化、直接传裸值)?
   透传时它声明的 slot 类型是什么(走 `fragment_builder.rs:2724-2763` 的非 finalize 分支 → VARBINARY)?
   - 看 `src/exec/operators/aggregate/streaming_sink.rs` / `streaming_source.rs`、partition-topN 相关算子、`fragment_builder.rs` 里 partition-topN / preAgg 的 codegen。
3. `is_merge_agg` 在 NovaRocks 里如何流转 + 是否被 analytic/preAgg 路径正确设置/消费?
   - 看 `src/lower/node/aggregate.rs`(`is_merge`)、`src/exec/node/aggregate.rs::AggFunction.input_is_intermediate`、analytic 路径。

**Phase 0 出口**:一张表,列出每个 (函数, 路径) 的「声明类型 / 运行时实际类型 / 是否一致」,据此确定
Phase 1 到底要把哪些 slot 改成 raw、哪些保持 VARBINARY 并让运行时真正序列化。

### Phase 1 — 让 pre-agg / 透传 slot 的声明类型与运行时一致

目标:消除「声明 VARBINARY、实际 raw」的漂移。按 Phase 0 的清单,二选一(每条路径各自定):
- **(A) 透传路径**:slot 声明改为运行时实际的 raw/返回类型(对齐 StarRocks「window 不用中间类型」);或
- **(B) 真序列化路径**:保持声明 VARBINARY,但让运行时在该 phase 真正序列化成 `Binary`,并用
  `is_merge_agg` 标记下游按 merge 消费。

主要改动点:
- `src/sql/codegen/fragment_builder.rs:2724-2763`(标准 codegen 非 finalize slot 定型)+ partition-topN / preAgg codegen。
- `src/lower/node/aggregate.rs`(FE-compat lowering 的对应路径,若共享)。
- 相关 preAgg/topN 算子的运行时产出(若选 B)。

验收:在 NovaRocks coord + 3 BE cross-process 跑 `aggregate analytic`(尤其 partition-topN /
window 聚合 / 含 avg、ndv、hll、percentile 的用例),pass 数不低于基线、无新增失败;**且此时即便
临时注释掉某个 tolerance 也不再触发**(用来证明漂移真的没了)。

### Phase 2 — 删除 tolerance 机制

Phase 1 的确定性成立后,逐个删除上文"要被删除的 tolerance"列表里的前三项(exchange passthrough、
sort skip-cast、analytic schema-adjustment),每删一处都跑 cross-process 验收确认不回归。

### Phase 3 — 消除 exchange merge 家族(原 #3)+ 收 nullability outlier

确定性到位后,把「合并取 actual」改成「源头 conform 到 descriptor」:
- exchange 发送端:用 chunk 的 `type_desc`(descriptor,已确认发送端可得且经 `align_chunk_schema_to_batch` 保留)直接做 wire schema + `retag_column` 各 chunk,删除 `merged_exchange_schema` / `merge_exchange_field_type` 的跨 chunk merge。
- `reconcile_chunk_data_type`(`align_chunk_schema_to_columns`)改为 conform-to-contract。
- array_agg `reconcile_data_type`:折叠到 retag / 删除;同时收 `type_relation.rs:327` 的
  actual-wins nullability outlier 进 `merge_fields_nullability`。

验收同上(cross-process);此阶段是真正的减法终点(执行层不再有「产出合并类型」的操作,只剩
relate 检查 + retag_column 物化 + merge_fields_nullability)。

## 风险 / 排序

- 这是真 pillar,比之前的收口块大:跨 FE-compat lowering + standalone codegen + 三条纠缠路径
  (analytic / streaming-preagg / partition-topN),核心难点是把 raw vs serialized 的状态表示
  在 descriptor 里按 phase / `is_merge_agg` 精确建模。blast radius 高(聚合 + 窗口 + 分布式)。
- 但全程有 StarRocks 对照,方向明确。**务必先做 Phase 0**(不调研清楚 pre-agg 透传的真实行为就动手,
  极易在 avg/hll/percentile 的某条路径上引入静默错误)。
- 不要一把梭;按 Phase 0→1→2→3 推进,每阶段 cross-process 验收。若 Phase 0 发现「透传 vs 序列化」
  的选择对某函数没有干净答案,停下来把权衡讲清楚再决定。

## 开放问题(Phase 0 解决)

1. avg/ndv/hll/percentile 的 partial 运行时产出到底是 `Binary` 还是 raw?(决定哪些是真漂移)
2. partition-topN / streaming preAgg 是否对这些函数透传裸值?透传是设计使然还是 NovaRocks 的捷径?
3. `is_merge_agg` 在 analytic/preAgg 路径是否被正确设置?StarRocks 用它区分 update vs merge,NovaRocks 是否同构?
4. 选 (A) 透传声明 raw 还是 (B) 真序列化?可能因函数而异(hll/percentile 天然 opaque,倾向 B;
   avg 在 window 下倾向 A=裸 numeric)。

## 约束(实施时)

- 三角 git:推 fork(`fork` = HangyuanLiu/NovaRocks),PR `gh pr create --repo NovaRocks/NovaRocks
  --base main --head HangyuanLiu:<branch>`;不推 origin、不开 origin-based PR。
- 提交信息英文;不加 `Co-Authored-By: Claude` trailer;与用户交流中文。
- 复杂功能先看 StarRocks 再动手(本 task 已附 StarRocks 参考位置)。
- 修 NovaRocks 自身缺口,不靠改 StarRocks FE 绕过。
- 环境:`source docker/iceberg-rest/runtime/current/env.sh`;cross-process:
  `--cluster-mode cross-process --cluster-size 3`。

## 参考
- 上游线:PR #302(type_relation 收口 + descriptor 权威 exchange)、#303(聚合 order/distinct 结构化)。
- 同目录:`2026-06-12-p2-p3-joint-impl-plan.md`、`../2026-06-12-distributed-execution-target-architecture.md`。
