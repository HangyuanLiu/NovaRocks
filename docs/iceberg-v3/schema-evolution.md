# Schema 演进 DDL

> Iceberg 通过 field-id 维护 schema 历史，支持 ADD / DROP / RENAME / type widening / reorder / required↔optional 等"安全"变更。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| ADD COLUMN（含嵌套路径） | ✅ | |
| DROP COLUMN | ✅ | |
| RENAME COLUMN | ✅ | |
| 类型 widening（int→long、float→double、decimal precision↑） | ✅ | |
| Column reorder | ✅ | |
| required ↔ optional（沿 spec 安全方向） | ✅ | |
| STRUCT 内嵌 add / drop / rename / widen | ❌ | |
| ARRAY / MAP 元素类型 widening | ❌ | |
| DDL 失败原子回滚（schema commit conflict） | ❌ | |
| `ALTER TABLE … SET TBLPROPERTIES` 全量审计 | 🚧 | 部分 props 已支持 |

实现入口：`src/connector/iceberg/catalog/schema_update.rs`

---

## ✅ ADD COLUMN

```sql
ALTER TABLE t ADD COLUMN c STRING;
ALTER TABLE t ADD COLUMN c STRING DEFAULT 'pending';     -- v3 default value，详见 default-values.md
ALTER TABLE t ADD COLUMN profile.nick STRING;            -- 嵌套字段路径
```

新加列默认 nullable（`optional` in Iceberg 术语）。带 DEFAULT 时写入 `initial-default` + `write-default`。

## ✅ DROP COLUMN

```sql
ALTER TABLE t DROP COLUMN c;
```

DROP 是逻辑删除，只在新 schema 中移除字段；老 data file 中的对应列被读端忽略。

## ✅ RENAME COLUMN

```sql
ALTER TABLE t RENAME COLUMN old_name TO new_name;
```

按 field-id 维系，老 data file 不需要重写。

## ✅ 类型 widening

按 spec 允许的方向：

| 原类型 | 可 widen 到 |
| --- | --- |
| `int` | `long` |
| `float` | `double` |
| `decimal(P, S)` | `decimal(P', S)` 其中 P' ≥ P |
| `date` | `timestamp` 仅在显式 widen 时（spec 允许，但 NovaRocks 暂未端到端测试） |

```sql
ALTER TABLE t MODIFY COLUMN c BIGINT;   -- int → long
```

## ✅ Column reorder

```sql
ALTER TABLE t MODIFY COLUMN c BIGINT FIRST;
ALTER TABLE t MODIFY COLUMN c BIGINT AFTER other_col;
```

## ✅ required ↔ optional

按 spec，**只允许从 required 变 optional**（向更宽松方向）；反向（optional→required）会被拒绝，因为可能有历史 NULL 行违反约束。

## ❌ STRUCT 内嵌 add / drop / rename / widen

Spec：嵌套结构的字段也支持完整 schema evolution，按 field-id 路径定位。

**TODO**：未实现 / 未端到端验证。当前只支持 top-level 列的演进；`profile.nick` 这种嵌套路径**只在 ADD 时**被解析，DROP / RENAME / type widen 嵌套字段尚未走通。

## ❌ ARRAY / MAP 元素类型 widening

Spec：`array<int>` → `array<long>`、`map<string,int>` → `map<string,long>` 等，按 element field-id 演进。

**TODO**：未实现。

## ❌ DDL 失败原子回滚

Spec：DDL 是单 snapshot 事务，commit 冲突或失败需要回滚到上一状态。

**TODO**：当前实现下，commit 冲突可能导致部分元数据写入但 version-hint 没更新，需要手动清理。建议在生产环境串行化 DDL 调用。

## 🚧 `ALTER TABLE … SET TBLPROPERTIES`

部分 properties 已支持（`format-version`、`write.row-lineage`、`write.delete.mode` 等），但 spec 中列出的全集尚未审计。

**TODO**：列出 NovaRocks 当前能正确解析 + 生效的 properties，标注哪些被忽略。
