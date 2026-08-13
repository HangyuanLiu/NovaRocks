---
id: ADR-0066
title: "StateStore-owned catalog attachment authority with a derived per-FE runtime projection"
domain: [catalog-attachment]
status: active
supersedes: []
superseded-by: null
date: 2026-08-13
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/881"
  - "discussion: 2026-07-15 / 2026-08-13, CP-2 catalog active-active control plane"
code-anchors:
  - "novarocks/frontend/src/catalog_attachment/repository.rs (CatalogAttachmentRepository)"
  - "novarocks/frontend/src/catalog_application.rs (FrontendCatalogApplicationPort)"
  - "novarocks/frontend/src/catalog_controller.rs (FrontendCatalogController)"
  - "novarocks/core/src/catalog_application.rs (CatalogApplicationPort, CatalogRuntimeProjection)"
---

## 问题

多个 FE 如何对「有哪些外部 catalog」达成一致，同时让查询热路径不必每次去问共享存储？

## 背景与执行事实

外部 catalog 的 attachment 曾由聚合 Core 里的同步 `MetaStoreProvider` 记录持久化，`CREATE CATALOG`
先改进程内的 concrete registry、注册 control、发布 query catalog，最后才写元数据；启动恢复反向从本机
元数据全量重建。这条路径只能让一个 FE 在本机恢复 catalog：记录里没有 lifecycle identity、没有 provider
ID，也没有任何跨 FE 可解释的版本，进程在「已改内存、未写元数据」之间崩溃会留下无法从共享事实解释的半状态。

Connector control 侧已经具备可复用的运行期契约：`ConnectorControlFactoryRequest` 以
`provider_id + instance_id + properties` 请求本地 control，`ConnectorControlCreation` 只回传允许持久化的
非敏感 durable properties，`ConnectorControlRegistry` 的 planning lease 持有精确的本地 generation、
`retire_current` 立即停止新 lease 并等待既有 lease 释放。也就是说进程内的 fencing 已经解决了，缺的只是
「谁是集群权威」。

`StateStore` 提供 snapshot 读、serializable 写事务、`Committed/Conflict/CommitUnknown` 分类、
`resolve_commit`，以及带 `high_watermark`/`next_cursor`/`resync_required` 的 change page，且 change hint
只含 revision 与 key、不含业务状态。

## 考虑过的选项

**A. 保留 Core 的本机元数据记录，另加一层跨 FE 同步。** 不必改所有权边界，改动最小。代价是同时存在两份
可写的 catalog 真相（本机元数据 + 集群同步层），任何一次崩溃或分区都要靠对账规则决定谁赢；而这类规则一旦
写下来就会长期存在，且没有任何一层能独立解释「当前集群到底挂了哪些 catalog」。

**B. 查询路径每次到共享存储核对 attachment。** 语义最强：admission 永不陈旧。代价是每条 SQL 的
catalog 解析都变成一次共享存储读，把分析型查询的规划延迟绑到 StateStore provider 的延迟与可用性上；对
FDB/MySQL 这类远端 provider 是不可接受的热路径成本。

**C. StateStore 为唯一权威 + 每个 FE 派生只读运行时投影（本决策）。** DDL 走 StateStore 事务，查询只读
本进程内存投影，change hint 只当唤醒、拿到提示后重读权威记录。代价是 DROP 不是全局同步撤销：远端 FE 在有界
reconcile 窗口内仍可能用旧 attachment 接新查询。

对照 StarRocks：其 FE catalog 元数据由 leader 通过 edit log 复制到 follower，读路径同样只读内存，跨 FE
一致性由 leader 单点写入保证。本决策与之在「读内存、写共享真相」上同构，区别是不引入 FE leader/edit log，
而是把仲裁下沉到 StateStore 的 precondition 与 exact-version 事务——因为 backend membership（ADR-0013）
已经用同样的方式把 durable desired state 交给了 StateStore，再引入第二套 leader 仲裁会形成重复权威。

## 裁决

StateStore 的 `CatalogAttachmentRecord` 是唯一集群权威，Frontend 是它唯一的 owner。

- 主记录按规范化后的 `ConnectorInstanceId` 单键寻址，名字唯一性直接由该键上的 `Precondition::Absent`
  保证，不建第二套 `catalog_id -> name` 索引。记录携带 UUIDv7 `attachment_id` 作为一次 durable attachment
  的生命周期身份：同名 DROP 后重建必须换新身份，用来拒绝迟到的本地安装结果与 ABA。
- `CREATE` 以 durable commit 为线性化点：factory 在事务外做 provider 预检并回传可持久化 properties，
  `Absent` 事务提交成功后才注册本地 control generation，最后才把名字发布进 query catalog registry。
  提交后本地安装失败不回滚集群事实——该 FE 把投影标为 `Unavailable` 并有界重试，DDL 返回「已提交但本 FE
  未就绪」的确定分类。
- `DROP` 以 exact-version delete 为线性化点，且与该 catalog 的 MV target/upstream 依赖扫描处于同一个
  serializable 事务；反向地，所有会新增外部 catalog 引用的 durable MV writer 必须在写 definition/index 的
  同一事务里断言 attachment 仍存在且 `attachment_id + version` 未变。两个方向合起来形成竞态闭包：谁先提交，
  另一方都会 conflict 后重判，不可能留下引用已删除或同名重建 attachment 的 durable MV 定义。
- 内存投影是派生运行时，不是第二权威。`Ready/Unavailable` 只描述「本 FE 能否从同一 durable record 构造出
  本地 runtime」；不同 FE 对同一 attachment 可以有不同的本地 incarnation。**发布本地 runtime generation 才
  是让 SQL catalog 名可解析的动作**，撤销发布先于退役本地 generation——因此查询永远看不到没有 control
  binding 的 catalog 名。
- Core 只消费 `CatalogApplicationPort`（命令 + admission）与运行时发布 sink。`Absent` 映射 `NotFound`、
  `Unavailable` 映射 `Unavailable`，两者始终分开：被删掉的 catalog 报「未知」，本机没物化成功的报「不可用」。
  没安装该 port 的组合无法创建、删除、恢复或准入任何外部 catalog，直接 fail closed。
- 启动与通知消费无丢失窗口：先取 change poll 的 `high_watermark` 作为 anchor，再有界分页扫描 attachment
  keyspace 构造目标集合，然后从 anchor 开始 poll。hint、`resync_required`、cursor 失效与 store identity 变化
  一律触发同样的有界全量重建；cursor 与重试队列只在内存，不建第二份 durable consumer 状态。
- watcher freshness gate 防止无限期陈旧：短暂 StateStore 或 poll 故障期间已 Ready 的 catalog 可在配置预算内
  继续服务，超预算即关闭这些 catalog 的新 admission 并返回 typed `Unavailable`。DDL 必须访问 StateStore，
  store 不可用时立即失败，绝不用内存执行 `CREATE`/`DROP`。

legacy Core attachment owner 在同一次改动中删除：record codec、repository、restore/persist/delete 路径与
Core 侧 control binding 构造全部移除，不留双写、旧 reader 或 compatibility shim。

## 接受的妥协（诚实记录）

**DROP 不是全局同步撤销。** `DROP CATALOG` 返回成功只表示 durable attachment 已删除、且接收请求的那个 FE
已停止新 admission；健康的远端 FE 在有界 reconcile 窗口内仍可能把旧 attachment 用于新查询。换来的是查询热
路径零共享存储读，以及不必新增一套 durable FE-ack membership 协议。freshness gate 只保证陈旧有上界，不保证
线性一致。需要真正 cluster-wide 线性撤销时应另立决策，不要在本机制上打补丁。

**durable CREATE 与本地可用性分离。** 用户可能收到「已提交但本 FE 不可用」。这是共享真相与进程本地环境分离
的诚实结果；用回滚 durable truth 来掩盖会让其他健康 FE 无端失去一个已经成立的集群事实。

**没有在线 legacy reader。** 原子切换简化了长期正确性，但已部署的 legacy SQLite attachment 数据不会被
steady-state runtime 自动读取。这是权衡后的取舍而非遗漏：保留一个只在升级窗口有用的 reader 会长期污染
steady-state 路径。确有部署兼容需求时，应做一次性、可删除、显式执行的迁移工具。

**跨领域 DROP 事务增加了协调面。** 同步 `MvRepository` 无法表达共享事务，因此由 Frontend 侧的
transaction-scoped domain ports 编排 attachment 与 MV index 的同事务读写。代价是 Frontend 内多了一层事务作用域
契约；换来的是可由 StateStore serializable conflict 证明的「无悬空引用」，而不是提交后 best-effort 重查冒充
原子性。

**真实多 FE 生产证据尚未取得。** 当前 Server 生产组合只有 SQLite 单 FE deployment source，因此本决策的领域
并发与收敛由「共享同一 StateStore 的两个独立 Frontend host/controller fixture」证明，真实 Connector/BE 集成由
1FE+3BE 证明。这不等于独立进程 FE failover 证据——那属于多 FE StateStore 收敛工作，不能把这里的 focused
evidence 提升为 arc 级生产验收。

## 何时重新评估

- 出现需要 cluster-wide 线性 DROP 语义的用户场景（例如权限撤销必须立即全局生效），届时需要在 attachment
  之上补一层同步 revoke barrier 或 FE ack 协议。
- 引入 `ALTER CATALOG` / rename / credential rotation：需要单独设计 immutable identity、replace 与 runtime
  rotation，不要在现有记录上预埋 optional `generation` 字段。
- attachment 数量增长到有界分页全量重建的成本不可接受（例如上万个 catalog、或 reconcile 明显拖长启动），
  届时需要增量 diff 而不是每次全量重建目标集合。
- 生产出现真正的多 FE deployment source（FDB/MySQL 独立进程 2FE+3BE），此时应用该拓扑重跑同一批领域场景并
  补 failover/restore 矩阵，再决定 freshness budget 的默认值是否仍然合适。
- StateStore provider 的 change page retention 或 `resync_required` 语义发生变化，会直接影响「hint 只当唤醒」
  这条设计能否继续成立。
