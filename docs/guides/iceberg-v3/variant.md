<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Variant 类型（V3）

> Iceberg v3 的 `variant` 用 metadata/value 表示半结构化数据。切到 iceberg-rust 0.11 后，本阶段只迁移类型与保证编译，**不承诺 Iceberg Variant 列的读取、写入或统计功能**。DDL 仍映射为上游 `Type::Variant`；含 Variant 列的写出失败于派发之前，确定未提交的产物由原 owner 清理。旧 INSERT/shredding 转换已删除，上游 writer 的不支持分支保持，待 Provider 物理布局与写出接缝恢复。
>
> 下文保留表达式和物理机制说明；历史 lake-table 示例标为待恢复，不是当前可运行能力。来源与补丁政策见 [ADR-0174](../../adr/ADR-0174-vendored-iceberg-baseline-and-patch-policy.md)。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| Variant 列读 | 🚧 待恢复 | 0.11 切换仅保证编译，无 Iceberg 读取功能承诺 |
| Variant 表达式函数（`variant_get` / `variant_extract` / `variant_typeof` / `get_json_string` 等） | 实现保留 | 表达式机制不等于 Iceberg 列读取能力 |
| `INSERT INTO ... VALUES (parse_json(...))` 写 variant 列 | ❌ 暂不可用 | 旧转换已退出；上游 writer 拒绝 Variant |
| `INSERT INTO ... SELECT` 写 variant 列 | ❌ 暂不可用 | 同上 |
| `INSERT OVERWRITE` / `DELETE` / `UPDATE` / `MERGE INTO` / `ADD EQUALITY DELETE` 在 variant-bearing 表 | ❌ 暂不可用 | 当前不支持 Variant 数据路径 |
| Variant *shredding* 读（`typed_value` 子树） | 🚧 待恢复 | 内核机制保留；无 Iceberg 读取功能承诺 |
| Variant *shredding* 写 | ❌ 暂不可用 | 属性解析和写出转换已删除 |
| Variant default value（`initial-default` / `write-default`） | ❌ | |
| Variant 在 partition spec / sort order / equality_ids | ❌ | spec 禁止；NovaRocks reject |
| Variant predicate pushdown 到 parquet | ❌ | |

---

## Variant 读路径（待恢复）

```sql
-- 待恢复后的用法示例；当前不保证 Iceberg Variant 列读取
SELECT id, variant_get(payload, '$.user.id') AS uid
  FROM t
 WHERE id < 1000;
```

待恢复并验证的读取能力：

- 直接 SELECT variant 列（按 binary 返回）
- 用 variant 表达式函数提取字段（`$.path.to.field`）
- variant 字段在投影裁剪 / 列裁剪 / runtime filter 中正常参与

## variant_get / try_variant_get 表达式机制（Spark 对齐）

- `variant_get/try_variant_get(v, path[, type])`：`path` 与 `type` 必须是字符串字面量。
  2 参返回 variant；3 参返回指定类型，`type` ∈ {boolean, int, bigint, float, double, string,
  date, datetime}。
- 语义：missing path → NULL；variant null → NULL；cast 失败 → `variant_get` 报错、
  `try_variant_get` 返回 NULL。
- 求值经由上游 parquet-variant-compute kernel/语义（与 scan 层 shredded 快路径同属
  parquet-variant-compute，语义保持一致，IV3-6 决策 B）；旧 `get_variant_*` 家族保持原有
  宽松强转语义不变，不参与下推优化。
- 第一个参数也接受 JSON 字符串 / VARCHAR（如 `variant_get(s, '$.a', 'bigint')`，其中 `s`
  是 JSON 文本）；`parse_json(s)` 先转成 variant 后也可作为第一个参数。坏 JSON 在 raw
  JSON/VARCHAR 与 `parse_json(s)` 模式下都是查询错误。

## 历史 Variant INSERT 示例（待恢复）

```sql
-- 历史写出示例；当前 INSERT 将失败，不能作为 smoke 或验证入口
CREATE TABLE t (id INT, v VARIANT) USING iceberg
  TBLPROPERTIES ("format-version" = "3");

INSERT INTO t VALUES
  (1, parse_json('{"a":1,"b":"x"}')),
  (2, parse_json('[10, 20, 30]')),
  (3, parse_json('null')),
  (4, NULL);

SELECT id, variant_typeof(v) FROM t ORDER BY id;
-- 1 Object / 2 Array / 3 Null / 4 NULL

SELECT id, get_json_string(v, '$.b') FROM t WHERE id = 1;  -- "x"
```

恢复写出时需要承接的物理合同：

- 文件落到 object storage 时是 spec-compliant parquet：variant group 标 `LogicalType::Variant`，使用 required Binary `metadata` 与 nullable Binary `value`，并保持外层字段的 Variant extension
- 默认非 shredding；shredding 属性接入与 `typed_value` 写出待重新实现和验证
- `INSERT INTO t SELECT ...` 也须沿正式布局接缝恢复，不留临时转换

## 其它 DML

当前 Variant-bearing Iceberg 表不提供 INSERT/OVERWRITE/DELETE/UPDATE/MERGE/equality-delete 的支持承诺。写出路径不会绕过 SDK 的不支持限制，不能通过旧 shredding 属性重新启用。其它引擎写入 Variant 表也不自动意味着 NovaRocks 已能读取它。

## ❌ Variant in partition spec / sort order / equality_ids

Spec 禁止；NovaRocks 在 CREATE TABLE / ALTER PARTITION 阶段 reject，错误信息明确说明。

## Variant shredding 机制（待恢复）

Spec optional 能力：在 parquet 物理结构里把"已知 schema 部分"提到 `typed_value` 子树以加速点查 / pushdown。

- **读：待恢复验证。** 原 `unshred_variant` 内核机制仍存在，但必须把上游 Variant Struct 表示、执行层逻辑载体和真实文件读路径接起来，才能重新声明 Iceberg 数据正确性。
- **写：暂不可用。** 旧 `write.parquet.variant-shredding.<col>` 解析及转换已删除。该属性不再是启用能力的入口；不能用 schema override 绕过上游 writer 的拒绝分支。

## ❌ Variant default value

Spec：variant 列允许 declare DEFAULT。NovaRocks 当前 CREATE TABLE / ALTER ADD COLUMN 在 variant 列上拒绝 DEFAULT。

## ❌ Variant predicate pushdown

Spec：variant 字段 path 提取（`payload:user.id > 1000`）应该可以下推到 parquet 层做剪枝（依赖 parquet variant page index）。NovaRocks 当前在 BE 层 evaluate variant predicate，扫描成本与全表扫描相当。

---

## 恢复条件

Provider 的 Variant 物理布局/writer owner 需要统一上游 `Type::Variant`、Struct metadata/value 表示与执行层逻辑载体，再交付读取、写入、统计和相应数据正确性覆盖。被删除、忽略和 SQL 跳过的性质覆盖须逐项重新承接；恢复后更新本指南与 [支持矩阵](reference/support-matrix.md#43-v3-新类型)。
