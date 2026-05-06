# 维护 / 治理

> Iceberg 的运维操作：压缩小文件、清理历史 snapshot、回收孤儿文件、重写 manifest 等。NovaRocks 当前只有 OPTIMIZE TABLE 的 whole-table 路径，其他治理操作仍待补。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `OPTIMIZE TABLE`（whole-table 文件压缩） | ✅ | `src/connector/iceberg/compact.rs` |
| `OPTIMIZE TABLE` 增量（仅小文件 / 仅 partition） | ❌ | |
| `EXPIRE SNAPSHOTS` | ❌ | |
| `REMOVE ORPHAN FILES` | ❌ | |
| `REWRITE MANIFESTS` | ❌ | |
| `REWRITE POSITION DELETES`（v2→DV） | ❌ | |
| `REWRITE DATA FILES BY SORT ORDER` | ❌ | |
| 自动 maintenance 调度器 | ❌ | |

---

## ✅ OPTIMIZE TABLE

```sql
ALTER TABLE orders OPTIMIZE;
```

行为：

- 把当前 snapshot 的所有 data file 重写到一组新 file（按当前 partition spec）
- 同时合并 V2 position-delete / V3 DV：被删行不出现在新 file 中，DV blob 在重写完成后失效
- 跨历史 partition spec：所有老文件按其 spec 解释，重写到当前 spec
- 写出新 manifest，老文件由 EXPIRE 路径回收（待 EXPIRE 实现）

> ⚠️ 当前 OPTIMIZE 重写后**会重新分配 `_row_id`**（详见 [row-lineage](row-lineage.md)）。spec 允许保留 / 重新分配，NovaRocks 还没决定最终策略。

## ❌ OPTIMIZE 增量

Spec：

```sql
-- 暂未实现
ALTER TABLE orders OPTIMIZE WHERE size_in_bytes < 16777216;     -- 仅压小文件
ALTER TABLE orders OPTIMIZE WHERE country = 'CN';               -- 仅压指定 partition
```

**TODO**：未实现。当前只能 whole-table 压缩，对大表代价高。

## ❌ EXPIRE SNAPSHOTS

Spec：

```sql
-- 暂未实现
ALTER TABLE orders EXPIRE SNAPSHOTS OLDER THAN '2026-04-01 00:00:00';
ALTER TABLE orders EXPIRE SNAPSHOTS RETAIN LAST 50;
```

**TODO**：未实现。当前 snapshot 历史会无限增长，需要外部脚本（Spark / iceberg-cli）做清理。

## ❌ REMOVE ORPHAN FILES

Spec：扫描 warehouse 路径，找到没有被任何 manifest 引用的孤儿 data / delete file，清理。

```sql
-- 暂未实现
ALTER TABLE orders REMOVE ORPHAN FILES OLDER THAN '2026-04-01';
```

**TODO**：未实现。

## ❌ REWRITE MANIFESTS

Spec：把多个小 manifest 合并成大 manifest，加速 plan 阶段。

```sql
-- 暂未实现
ALTER TABLE orders REWRITE MANIFESTS;
```

**TODO**：未实现。

## ❌ REWRITE POSITION DELETES

Spec：把 V2 position-delete 重写成 V3 deletion vector（升级路径）。

**TODO**：未实现。如果你要从 V2 迁移到 V3，目前需要 DELETE 全部数据再重写，或借助外部工具。

## ❌ REWRITE DATA FILES BY SORT ORDER

Spec：按 Iceberg sort order 物理重排数据文件，让后续 sort-merge join / range scan 更高效。

```sql
-- 暂未实现
ALTER TABLE orders REWRITE DATA FILES BY SORT ORDER (user_id, ts DESC);
```

**TODO**：未实现。当前 OPTIMIZE 不感知 sort order。

## ❌ 自动 maintenance 调度器

Spec / 工程实践：基于 schedule 自动跑 OPTIMIZE / EXPIRE / ORPHAN，类似 Snowflake auto-clustering 或 Databricks `OPTIMIZE` cron。

**TODO**：未实现。运维方需要自己用外部 cron / Airflow 调度。
