---
id: ADR-0178
title: "Iceberg writes freeze storage types and adapt execution carriers at the provider boundary"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0174
    scope: "Core U8 Arrow writer schema override retention; Variant read capability deferral; all unrelated baseline and patch exit rules remain active"
    link: "ADR-0174-vendored-iceberg-baseline-and-patch-policy.md"
  - id: ADR-0055
    scope: "Decision 5 and compromise 3: preserving divergent Variant write target carriers; provider ownership of strategy and source facts remains active"
    link: "ADR-0055-row-dml-strategy-consumer-closeout.md"
date: 2026-10-15
provenance:
  - "discussion: 2026-10-15 frozen Iceberg target schema, closed input adaptation and Variant read/write boundaries"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/write_stack/domain.rs (IcebergDataBranchRecipe, IcebergEqualityDeleteRecipe)"
  - "novarocks/connector/iceberg/src/commit/frozen_write.rs (prepare_schema_core)"
  - "novarocks/connector/iceberg/src/commit/write_adaptation.rs (adapt_batch)"
  - "novarocks/connector/iceberg/src/commit/validation.rs (deny_unwritable_data_files, row_mutation_strategy_from_metadata)"
  - "novarocks/connector/iceberg/src/typed_read/schema_binding.rs (physical_adaptation, annotated_read_schema)"
  - "novarocks/connector/iceberg/src/manifest.rs (with_variant_statistics_hint)"
---

## 问题

当执行层的 Arrow 载体不足以区分外部存储类型时，谁决定 Iceberg 文件的逻辑类型、字段身份和可写能力，读写边界如何保持一致？

## 背景与执行事实

Iceberg schema 同时拥有 schema ID、嵌套 field ID、逻辑类型、required 和 default。Arrow 执行载体只承担计算：timestamp 与 timestamptz 可以使用同样的整数数组，Variant 使用 LargeBinary，uuid 与 FIXED 可以具有同样宽度。由执行载体反推存储类型会丢失这些区别。

| 层 | 权威输入 | 职责 |
| --- | --- | --- |
| FE Provider | 当前表或已冻结的目标 Iceberg schema | 签发完整数据分支 schema、等值字段投影与准入判定 |
| Provider 私有 recipe | 规范 Iceberg schema JSON 与原有分区、格式事实 | 跨角色传递存储真相，不扩展中立 SPI 或 native plan |
| BE Provider | recipe 中的目标 schema 与输入字段选择 | 按名字投影并保留字段顺序，生成 SDK Arrow writer schema |
| 写入适配 | 执行批次、目标 SDK Arrow schema | 值与 NULL 不变地适配载体，按目标重建嵌套字段 |
| 读取适配 | 冻结 Iceberg 逻辑类型、物理文件与投影 | 只有 Type::Variant 合并 Variant group；普通 metadata/value STRUCT 保持结构语义 |

`prepare_schema_core` 不再从 Arrow 类型恢复 Iceberg schema。数据和 equality delete 两条写出路径共用 `adapt_batch`；SDK Parquet writer 使用上游 schema 投影，vendor U8 override 及消费者退出。Provider 私有契约 revision 同步演进。

## 考虑过的选项

1. **从执行 Arrow schema 反推类型：设计否决。** 调用面简单，但丢失 timezone、uuid、Variant 和 storage field identity，无法证明输出类型忠实于目标。
2. **由 SPI 再建一套存储类型词汇：设计否决。** 把 Iceberg 格式语义复制进中立执行契约，制造第二份类型权威，违反 Provider 对外部事实的所有权。
3. **保留 vendor writer schema override：设计否决。** 绕过 SDK 的规范映射，使 batch、统计和 Parquet 标注可能认不同类型。SDK 能接收 Arrow 数组不代表它能正确写出相应存储类型。
4. **冻结完整目标 schema，在 Provider 内闭合输入适配：采用。** 保留 SDK 的存储语义，执行层无需恢复 field ID/default 或第三方类型。
5. **同时实现 Variant 与 uuid writer：待评估。** 需要正式 logical annotation、Variant extension、统计与独立实现互读；当前不以移除 override 为由猜测恢复写入。

## 裁决

1. **目标 schema 唯一权威。** FE Provider 冻结完整 Iceberg schema；BE 只投影这份 schema。等值删除冻结同一 schema 的字段投影。输入缺失、重复、无法绑定或非法保留字段立即报错，不回退按 ordinal 或载体推类型。
2. **适配集合封闭。** 顶层 Int8/Int16 到 Int32；Binary 到 LargeBinary；相同单位、整数值不变的 UTC timezone 标签；Int32 字符串字典展开；按目标重建 STRUCT/LIST/MAP 字段。LargeUtf8 字符串转 Utf8 检查 offset 范围。其他类型差异、嵌套可空性差异和 required 顶层 NULL 明确拒绝；不静默截断、转 NULL 或降级。展开与重建留在原 writer 资源归属内。
3. **数据写出准入与删除分离。** 目标任意层级含 Variant 或 uuid 的普通数据写出在副作用之前返回 Unsupported。UPDATE、MERGE 在只插入快捷分支之前判定。DV/position DELETE、TRUNCATE、位置删除重写与不含非法等值键的 equality delete 保留；ADD FILES 只拒绝 Variant，允许 uuid。
4. **Variant 的逻辑判定。** 执行值格式继续由共享 types 层拥有；Connector 拥有 Iceberg Type::Variant、Parquet group 与执行值之间的转换。顶层读取支持 unshredded 与 shredded；普通同形 STRUCT 不合并。嵌套 Variant 投影拒绝；未投影列不阻止其他列读取。
5. **新定义准入与统计诚实。** CREATE / ALTER ADD 在默认、v2、v3 均递归拒绝新增 Variant 列，含 IF NOT EXISTS；CREATE LIKE 保留源类型后拒绝目标定义，CTAS/MV 的 Variant 与 shredding 属性拒绝早于 namespace 请求。规则由 Provider 纯准入拥有；FE schema ALTER 先通过同一代 mutation lease 准入，再通过对应 planning lease 执行原 MV / DROP 依赖保护，并保留同一 operation id 执行与协调。已有表的普通 ADD、rename、comment、drop、reorder 和 Variant 同类型 MODIFY 沿原规则处理，DELETE / 合法普通键 equality delete / TRUNCATE 边界保留。Variant manifest bounds 尚不可读；历史 schema 出现 Variant 时，在 manifest 加载错误后附加可能相关的提示，保留类别、重试、清理与绑定事实，不吞统计、不跳 manifest。

本条部分替代 ADR-0174 的 core U8 保留项及 Variant 读取暂缓条款，其余 vendor 来源与退出规则不变。ADR-0055 的 Provider 策略所有权继续有效；其裁决 5、妥协 3 保留 Variant 写入载体分歧的临时做法退出，改为提前拒绝未支持的写出。消费 [ADR-0176](ADR-0176-recursive-execution-types-and-independent-storage-ownership.md) 的执行类型与独立存储所有权；共享 Variant 值格式的归属保持 [ADR-0040](ADR-0040-sql-compiler-dependency-inversion.md) 的边界。

## 接受的妥协（诚实记录）

- SDK 的 Variant/uuid writer 能力不足由显式拒绝体现，放弃已有短期 Variant 写入示例；顶层读取恢复不自动恢复数据写入。
- 字典展开与嵌套重建可能分配新数组，保留在已有 writer 资源域，不新增进程级容量或全局字典。
- Variant 历史提示只表示可能相关，可能伴随纯网络或权限错误；它不改变原始错误事实。

## 何时重新评估

- SDK 或正式 Provider writer 能正确输出 UUID/Variant logical annotation、extension、统计并由独立引擎读回时，单独评估恢复数据写入；shredding 写入另需属性、typed_value 与统计合同。
- SDK 支持读取 Variant bounds 时，补充真实历史 schema 与 manifest 互读后移除当前统计限制及相应提示。
- 嵌套 Variant 的逐字段读取、NULL 与重建语义闭合并通过跨引擎验证时，评估解除嵌套投影限制。
