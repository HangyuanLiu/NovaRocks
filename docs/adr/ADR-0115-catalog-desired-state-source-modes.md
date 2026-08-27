---
id: ADR-0115
title: "Catalog desired state as one typed snapshot from mutually exclusive source modes"
domain: [catalog-attachment]
status: active
supersedes: [ADR-0066]
superseded-by: null
date: 2026-08-26
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/984"
  - "discussion: 2026-08-25, catalog desired state under a lake-single-source-of-truth frontend"
code-anchors:
  - "novarocks/frontend/src/catalog_application/desired_state.rs (CatalogDesiredStateSnapshot, CatalogDesiredStateSource)"
  - "novarocks/frontend/src/catalog_application/frontend_port.rs (FrontendCatalogApplicationPort)"
---

## 问题

「这个集群挂了哪些外部 catalog」这份期望态从哪里来？在 serverless 形态下用户希望把它随镜像/配置一起复制，
在自管集群里用户希望继续用 SQL `CREATE CATALOG` 并跨重启恢复，未来托管平台还希望由 controller 下发版本化
快照。这三种来源能不能同时生效？枚举失败与单个 catalog 装载失败是不是同一种失败？

## 背景与执行事实

- ADR-0066 把 StateStore 定为 catalog attachment 的**唯一**持久权威。它的机制（durable commit 线性化点、
  exact-version delete、change hint 只作唤醒、丢通知退化为有界全量重建、`Absent` 与 `Unavailable` 分离）
  在实现上是正确的，本 ADR 全部继承。被取代的只有「唯一来源」这一条。
- 湖成为唯一共享真源之后，FE 的 durable store 从「真源」降级为「可选载体」。catalog 期望态是唯一仍然合理
  留在 store 里的用户可写事实，因此它是一个**窄化的例外**，而不是通用能力。
- 两种失败范围此前只由「哪条代码路径恰好把错误抛上去」区分：attachment 全量扫描失败会上抛并让 FE 打开失败，
  单个 catalog 的 provider 装载失败只把该 catalog 标 `Unavailable`。两种行为都正确，但都不是契约，也没有测试
  锁定，重构时极易翻转。
- 枚举完整性是有真实后果的：snapshot 是全量真相，reconcile 会退役任何枚举没有返回的 catalog。把「读成功的
  那几页」当作 snapshot 会删掉没人要求删除的 catalog。

## 考虑过的选项

1. **保持单一 StateStore 来源，serverless 用启动脚本预先执行 SQL**。不需要新契约，但 catalog 期望态变成
   一串命令式副作用，「从文件里删掉一个 catalog」无法表达，蓝绿复制也无从谈起。
2. **允许多来源合并（配置文件 + StateStore 叠加）**。看似灵活，实际制造双写权威：同一个 catalog 名可以有两个
   不一致的定义，谁赢取决于时序。
3. **一个 typed snapshot + 互斥 source mode**（采纳）。

## 裁决

1. 期望态收敛为一个 typed `CatalogDesiredStateSnapshot`：全部 catalog 的**精确全量**逻辑配置，加一个
   snapshot identity。逻辑配置只含 catalog 名、provider/type、显示名、durable connector 属性、credential
   引用与配置格式版本；**不含** resolved secret、attachment identity、CAS version、runtime generation、
   Ready/Unavailable 状态或后台 cursor。
2. source mode 是闭合三选一且**互斥**，在产生任何启动副作用之前选定一次：`StaticFile`（serverless 默认，
   文件即全量快照，SQL mutation typed reject）、`DynamicStateStore`（自管集群，SQL 可用且跨重启恢复）、
   `ManagedController`（未来平台下发）。不同 mode 的 catalog 永不合并为双权威，也不存在静默回落。
3. SQL `CREATE`/`DROP CATALOG` 的准入是所选 mode 的函数。
4. 两种失败范围由**类型**表达而非路径巧合：
   - 枚举不完整或不可信 → typed 全局失败，FE 不进入可服务状态。snapshot 的唯一构造器是校验构造器，因此
     「部分扫描变成 snapshot」在类型上不可达，尤其不会退化成「零个 catalog 的合法 snapshot」。
   - 单个 catalog 的 materialization 失败 → 只把该 catalog 标 `Unavailable`；per-catalog 物化函数**不返回
     `Result`**，所以一个 provider 的故障在结构上到不了 reconcile 的返回值。
5. clone 采用 `SemanticRebind`：只导出/导入逻辑快照，attachment id、CAS version、runtime generation、
   resolved secret 与 readiness 全部在目标侧重铸。snapshot identity 的摘要只覆盖逻辑配置，因此跨 clone 稳定。

## 接受的妥协（诚实记录）

- **只有 `DynamicStateStore` 真正实现了**。`StaticFile` 与 `ManagedController` 目前是 typed 未支持：选中即在
  副作用前失败。这是有意的未完成，不是 shim——但必须承认，一个只有单一实现的三变体枚举，其抽象是否恰当要等
  第二个实现落地才能验证。如果文件配置无法承载这个 snapshot 契约，那是设计变更，应退回讨论而不是加分支。
- **snapshot 里装的是「已定位条目」（逻辑配置 + 来源侧 identity），不是裸逻辑配置**。物化需要 attachment id
  来给本地投影做键。§ 的排除清单约束的是逻辑配置本身，identity 严格放在它旁边且不进摘要，所以导出仍不携带
  physical identity；但这确实比「snapshot 只有逻辑配置」更宽松。
- **catalog 是 StateStore 上唯一保留的用户可写持久事实**。这个例外很容易被误读成「StateStore 仍可承载任何
  重要状态」。它被绑定在 source mode 上，不得复用为 DML/job/view/topology 的权威。
- **`StaticFile` 语义会让「文件里删掉 catalog」变成一次真实删除**。这是全量快照的必然代价：期望态就是全量，
  不存在「只增不减的 seed」。
- 保留了 ADR-0066 的 change-hint/重读机制，也就保留了它的成本：提示丢失或 retention gap 会触发有界全量重建。

## 何时重新评估

- `StaticFile` 的配置解析与启动组合落地时：那是第一次用第二个实现检验这个契约，snapshot 字段集很可能需要
  调整。
- `ManagedController` 真正下发版本化快照时：需要重新审视 snapshot identity 是否足以表达 controller 的版本。
- 出现「必须同时从两个来源取 catalog」的真实需求时：本 ADR 的互斥前提就不再成立，必须重新讨论双权威的冲突
  裁决，而不是偷偷合并。
- readiness / drain 信号面建立后：per-catalog readiness 与更强的「所有 required catalog Ready 才切流」barrier
  的分工需要重新划线。
