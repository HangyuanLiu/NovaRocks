---
id: ADR-0182
title: "Iceberg nominal domains persist by stable recursive source field IDs"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-10-17
provenance:
  - "discussion: 2026-10 accepted Provider-owned recursive nominal declarations, stable field identity and exact historical validation"
  - "discussion: 2026-10-17 accepted one five-domain document, lawful schema cleanup and exact replacement storage"
code-anchors:
  - "novarocks/connector/iceberg/src/schema_mapping/nominal_declaration.rs (TableNominalDocument, from_columns, append_definition, validate_frozen_schema)"
  - "novarocks/connector/iceberg/src/catalog_control/catalog_mutation.rs (prepare_table_creation, scalar_integer_schema_updates)"
  - "novarocks/connector/iceberg/src/catalog_control/type_mapping.rs (schema_fields)"
  - "novarocks/connector/iceberg/src/schema_mapping/typed_value.rs (from_field)"
  - "novarocks/connector/iceberg/src/typed_read/codec/program_fields/typed_compare.rs (value)"
---

## 问题

Iceberg 原生类型不能区分 NovaRocks 的 HLL、Bitmap、LARGEINT、TINYINT 和 SMALLINT 时，持久化的名义领域由谁声明，如何在嵌套投影、字段改名及历史读取之间保持同一身份？

## 背景与执行事实

[ADR-0176](ADR-0176-recursive-execution-types-and-independent-storage-ownership.md) 分离执行语义、值布局与来源事实；[ADR-0178](ADR-0178-iceberg-write-type-boundary-and-variant-layering.md) 规定 Iceberg schema 是存储权威。本条记录该分层下的 Provider 持久声明，不改变 Arrow 字节或执行类型所有权。

| 事实 | 作者与表示 |
| --- | --- |
| HLL、Bitmap 存储 | Iceberg Binary；仅显式 Provider 声明赋予对应领域 |
| LARGEINT 存储 | Iceberg Fixed(16)；普通同宽 FIXED 不自动成为 LARGEINT |
| TINYINT、SMALLINT 存储 | Iceberg Int；声明绑定精确存储类型，作用范围保持顶层 |
| 嵌套身份 | Iceberg 正整数 field ID，包括 Struct 字段、List element、Map key/value |
| 持久声明 | 表属性 `novarocks.nominal-types.v1`，内容为 `{"version":1,"fields":[{"id":3,"domain":"largeint"}]}` |
| 冻结与运行时 | Provider 私有 recipe 的精确声明和所选 schema；执行字段 metadata 不成为权威 |

领域词汇封闭为小写 `hll`、`bitmap`、`largeint`、`tinyint`、`smallint`。前三者递归支持，后两者保持顶层作用范围。文档拒绝未知字段、重复或非正 ID、未知领域和版本；现有硬上限为 1 MiB、16,384 个声明，递归定义沿统一类型深度上限。持久文档的 v1 与 Provider 私有 codec revision 是独立版本；后者因冻结语义变化升级为 8，不将旧冻结内容自动猜测为新合同。

## 考虑过的选项

- **从 Binary/FIXED 字节、宽度或 Arrow annotation 推断领域：设计否决。** 同载体有不同合法语义，来源 annotation 也不能代替已加载 metadata 的声明。
- **按嵌套名字或文本路径持久化：设计否决。** 字段改名、删除后同名新增和匿名容器角色不能保持准确历史身份。
- **保留按名字别名或独立整数属性：设计否决。** 多份持久表示增加冲突并留下不同读取入口；五域只由同一稳定 ID 文档声明。
- **Provider 以稳定 field ID 声明并在完整 schema 上校验：采用。** 持久化与冻结都由来源 owner 负责，消费端不另建类型权威。

## 裁决

1. **定义作者拥有声明。** CREATE / ALTER ADD 从已准入的完整列定义生成声明，按实际发布 schema 的字段 ID 绑定；HLL、Bitmap、LARGEINT 递归，tinyint/smallint 仅顶层。ID 来源保留当前 SDK 的实际递归编号。普通属性 SET/UNSET 不得写入或删除 `novarocks.nominal-types.*`。
2. **稳定 ID 不随名字迁移。** rename 保持 field ID，文档不随名字变化；drop 保留 ID 声明服务历史 schema；同名新增使用新 ID，不继承旧领域。MODIFY 不得在未重写存储值时改换已有名义领域，包括已声明 tinyint/smallint 的 INT→LONG。普通未声明 INT→LONG 保留原有准入。
3. **只有一份持久真相。** `novarocks.nominal-types.v1` 统一承载五域。已加载 metadata 解码一次，再投影出既有递归名义声明和顶层整数声明两个冻结视图；规划的 Bitmap/HLL semantic kind 按顶层源 field ID 投影。`novarocks.logical_type.*` 与 `novarocks.scalar_integer_domains.v1` 不再读取、生成或保护；作为普通属性保存的文本不授予领域。普通 Binary/Fixed(16)/Int 不自动获得名义领域。
4. **历史与选择都严格验证。** 每个声明 ID 在每份包含它的保留 schema 中必须匹配精确存储类型，冲突以带 field ID 的 CorruptData 拒绝。不在任何保留 schema 中的 ID，若不超过 last-column-id，则属于合法历史清理：读取忽略，下一次写该文档时删除；超过 last-column-id 则以带 ID 的 CorruptData 拒绝。冻结所选 schema 的准确 ID 子集；解码拒绝声明指向不存在的所选源字段。Struct/List/Map 投影消费同一来源声明。
5. **外部 REPLACE 保持同一规则。** 保留同 ID、同存储类型的替换继续保留领域；同 ID 更换存储类型明确拒绝。移除声明或以新 ID 替换不会从名字、载体或历史字节修复领域。
6. **Provider 私有闭合。** 外部 Arrow annotation 只是准确声明的向外投影；冲突或无声明的名义 annotation 拒绝，不能反向签发领域。Native 只承载原 Provider opaque recipe；两个冻结视图与私有 codec revision 8 保持一致，不新增 revision。所有结构创建和晚到引用继续遵循 ADR-0176 与 [ADR-0167](ADR-0167-size-banded-allocation-attribution.md) 的真实 owner、控制和最后引用退出合同。检查点继续借用调用方控制，取消保留原始类型化失败。

## 接受的妥协（诚实记录）

- 标准 Iceberg 引擎看到 Binary/Fixed(16)/Int 存储，不自动理解 NovaRocks 名义领域；本条不承诺 HLL/Bitmap 计算或 LARGEINT 名义解释的跨引擎互操作。
- 外部工具更换已声明同 ID 的存储类型时，读取明确拒绝；同 ID、同存储类型不能仅凭 metadata 证明值语义仍相同。缺失声明不会从现存字节修复。
- 合法历史清理留下的孤立声明仅在下一次实际写文档时回收，不为读取增加 metadata 写入。

## 何时重新评估

- Iceberg 提供这些名义领域的正式存储类型或标准扩展，并有独立实现互读证据时，评估迁移表示；迁移不得靠载体宽度推断旧领域。
- 需要扩展 tinyint/smallint 的嵌套作用范围，或外部工具需要更换名义领域并重写值时，先定义完整迁移与冲突规则。
- 新名义领域或 SDK 字段编号规则变化时，显式升级/扩展 Provider 词汇并补齐 CREATE/ADD、rename/drop/name reuse、所选及历史 schema 与冻结重放验证。
