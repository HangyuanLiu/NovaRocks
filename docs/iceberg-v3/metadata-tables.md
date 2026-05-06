# Metadata Tables

> Spec 定义的虚拟表，用来查表内部状态（snapshot 历史、refs、文件统计、partition 统计 ...）。NovaRocks 已经实现 BE 侧的首批四张（snapshots / history / refs / partitions），但 standalone-server 的 SQL parser 还没有接受 `t$tabletype` 路由语法，**端到端 SQL 入口暂未开放**。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `$snapshots` | 🚧 | BE 已落 PR #81，缺 SQL 入口 |
| `$history` | 🚧 | 同上 |
| `$refs`（branches + tags） | 🚧 | 同上 |
| `$partitions` | 🚧 | 同上 |
| `$manifests` | ❌ | |
| `$files` / `$all_data_files` | ❌ | |
| `$delete_files` / `$all_delete_files` | ❌ | |
| `$entries`（manifest entries） | ❌ | |
| `$metadata_log_entries` | ❌ | |
| `$position_deletes` | ❌ | |
| `$all_files`（V3 统一视图） | ❌ | |

实现入口：`src/connector/iceberg/metadata.rs`、`src/connector/iceberg/IcebergMetadataBridge.java`

---

## 🚧 当前限制：SQL 入口缺失

下列 BE 已经能返回正确数据：

| 元数据表 | 列 |
| --- | --- |
| `$snapshots` | `committed_at` (TIMESTAMP)、`snapshot_id` (BIGINT)、`parent_id` (BIGINT)、`operation` (STRING)、`manifest_list` (STRING)、`summary` (MAP<STRING, STRING>) |
| `$history` | `made_current_at` (TIMESTAMP)、`snapshot_id` (BIGINT)、`parent_id` (BIGINT)、`is_current_ancestor` (BOOLEAN) |
| `$refs` | `name` (STRING)、`type` (STRING `BRANCH` / `TAG`)、`snapshot_id` (BIGINT)、三个 retention 字段（min-snapshots-to-keep / max-snapshot-age-ms / max-ref-age-ms） |
| `$partitions` | partition struct（按表当前 spec）+ `record_count` / `file_count` / `position_delete_file_count` / `equality_delete_file_count`（均 BIGINT） |

但 standalone-server 的 SQL parser 不识别 `t$snapshots` 形式（参见 `src/engine/mod.rs:4538` 的 TODO），所以下列 SQL 暂时跑不通：

```sql
-- 暂未实现：parser 不接受 $tabletype
SELECT * FROM orders$snapshots;
SELECT * FROM orders$history;
SELECT * FROM orders$refs;
SELECT * FROM orders$partitions;
```

**TODO**：补 parser 端的 `<table>$<metadata_table>` 路由，把它解析成对应的 metadata table scan plan，再下推到已实现的 BE 路径。

## ❌ 其他 metadata table

下列 spec 要求的 metadata table 在 BE 和 SQL 两端都未实现：

- `$manifests`：列出 current snapshot 的所有 manifest 文件（path / length / partition spec / added/existing/deleted file counts ...）
- `$files`：current snapshot 的 live data file 列表（path / file_format / record_count / file_size_in_bytes / column stats / partition）
- `$all_data_files`：所有 snapshot 的并集
- `$delete_files`：current snapshot 的 delete file 列表（V2 position-delete + equality-delete + V3 DV）
- `$all_delete_files`：所有 snapshot 的并集
- `$entries`：manifest entries（包含 added / existing / deleted 状态、sequence number ...）
- `$metadata_log_entries`：每次 metadata.json 切换的日志
- `$position_deletes`：所有 V2 position-delete 行展开
- `$all_files`（V3）：data file + delete file 的统一视图

**TODO**：路线图上集中在解锁 SQL 入口之后再补 BE bridge。

---

## 临时绕过方案

在 SQL 入口落地前，如果你要查 metadata：

- 通过 Iceberg CLI / iceberg-rust：直接读 `metadata.json`
- 通过 `tbl.<branch_or_tag>` SELECT：`SELECT * FROM t FOR VERSION AS OF <id>` 可以验证某个 snapshot 是否还能读
- 通过外部 Spark / Trino：在共享 warehouse 上跑 `SELECT * FROM t.snapshots`
