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

# Lake publication：不确定提交的人工核对

一次 Iceberg 写入的可见性由 Catalog 提交决定。网络断开、进程退出或 Catalog 已提交但响应丢失时，
客户端可能收到 `CommitUnknown`。这不表示回滚，也不表示可以安全重试：**不要自动重试、不要调用
abort/cleanup、不要删除 staging ref 或对象。**

NovaRocks 为每次 mutating statement 分配 `LakePublicationId`，并将它写入可公开读取的 snapshot
summary 或表属性。人工核对只做读操作，并按下面三项共同判断：

| 核对项 | Published 的必要条件 | 任一项缺失或漂移时 |
| --- | --- | --- |
| marker | 找到同一 `LakePublicationId` 的 canonical marker | `Unknown` |
| identity | marker 中 target UUID 与当前表 UUID 完全相同 | `Unknown` |
| ancestry | marker snapshot 仍是目标 ref/main 的可达祖先 | `Unknown` |

只有三项同时满足，才可以报告 **Published**。marker 缺失、格式损坏、目标表被 drop/recreate、ref head
变化或无法读取 metadata 都必须保留 **Unknown**；它们不能被解释为未提交。

## 核对配方

先从 SQL 错误、statement log 或平台 audit 记录中取得 `LakePublicationId`，然后在同一 external
catalog 中运行只读查询（替换表名和 ID）：

```sql
-- 1. 找到声明该 publication 的 snapshot marker。
SELECT snapshot_id, parent_id, committed_at, summary
FROM catalog_name.db_name.table_name$snapshots
WHERE CAST(summary AS STRING) LIKE '%<LakePublicationId>%';

-- 2. 读取当前 ref/main 和 table identity，确认 snapshot 仍被当前历史引用。
SELECT name, type, snapshot_id
FROM catalog_name.db_name.table_name$refs;

-- 3. CTAS 还要读取新表的公开属性；表不存在时没有正向锚，仍是 Unknown。
SHOW CREATE TABLE catalog_name.db_name.created_table;
```

不同 Catalog 对 metadata-table 的 `summary` 展示格式可能不同；可以改用 Spark、Trino 或 REST
`loadTable` 读取同一 metadata JSON。关键是不改变判断标准：读取目标 UUID、snapshot marker 和
ancestor chain，而不是匹配 NovaRocks 的错误字符串。

对于 data-producing MV，额外读取 `$refs` 中的 `main` 与 NovaRocks-owned staging branch。历史 staging
branch 只能由配置了安全年龄窗的 GC 退休；人工核对和应用进程不得抢先删除它。对于还未注册目标表的
CTAS，只有其 deterministic warehouse-owned staging prefix 的 GC 可以在年龄窗后回收残留。

## 如何结束 Unknown

将上述三项的原始读结果、target identity、`LakePublicationId` 和 statement tag 交给平台运维或人工
处理流程。若最终证明 Published，客户端可以按已提交处理；若仍 Unknown，应保持 Unknown 并让 GC
在安全年龄窗后处理残留。不要以再次执行原 SQL 作为“恢复”。

## 验收环境

仓库的 `lake-publication` SQL suite 使用真实 Iceberg REST Catalog、MinIO 和 runner-owned 1FE+3BE
拓扑。它的透明代理只对标准 REST `stage-create` 或 table commit 注入一次故障；代理没有私有 Catalog
endpoint、SQLite ledger 或 publication authority。
