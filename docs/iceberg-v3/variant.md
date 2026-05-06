# Variant 类型（V3）

> Iceberg v3 引入 `variant` 类型，物理上由 metadata + value 两个 binary stream 组成（参考 Parquet variant proposal），逻辑上承载 schema-less 半结构化数据。NovaRocks 已实现读路径与表达式函数族；**写路径 IcebergSink 仍会拒绝 variant 列**。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| Variant 列读 | ✅ | `src/exec/variant.rs` |
| Variant 表达式函数（`variant_get` / `variant_extract` 等） | ✅ | `src/exec/expr/function/variant/*` |
| Variant 列 INSERT / CTAS 写 | ❌ | IcebergSink reject |
| Variant predicate pushdown 到 parquet | ❌ | |

---

## ✅ Variant 读路径

```sql
-- 假设 t.payload 是一个 variant 列（例如由 Spark / Iceberg-cli 写入）
SELECT id, variant_get(payload, '$.user.id') AS uid
  FROM t
 WHERE id < 1000;
```

支持：

- 直接 SELECT variant 列（按 binary 返回）
- 用 variant 表达式函数提取字段（`$.path.to.field`）
- variant 字段在投影裁剪 / 列裁剪 / runtime filter 中正常参与

## ❌ Variant 写路径

```sql
-- 暂未实现
CREATE TABLE t (id BIGINT, payload VARIANT) TBLPROPERTIES ("format-version" = "3");
INSERT INTO t VALUES (1, parse_json('{"a": 1}'));
-- ERROR: variant column write is not supported
```

**TODO**：

- 让 IcebergSink 接受 variant 列：序列化 metadata + value 两个 binary stream，写出到 parquet
- 对接 SQL `parse_json` / `variant_build` 等构造函数
- CTAS 在 variant 列上下文需要把 source 的 variant 透传

## ❌ Variant predicate pushdown

Spec：variant 字段的 path 提取（`payload:user.id > 1000`）应该可以下推到 parquet 层做剪枝（依赖 parquet variant page index）。

**TODO**：未实现。当前 variant predicate 在 BE 层 evaluate，扫描成本与全表扫描相当。

---

## 路线图

按 [完成度清单](../iceberg-v3/reference/support-matrix.md#43-v3-新类型) 的优先级，variant 写路径排在 P3（"v3 类型 tail"），优先级低于 catalog 生态、cross-engine 互通、MV 自动改写。
