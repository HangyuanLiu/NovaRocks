---
id: ADR-0054
title: "Frontend DML uses operation-scoped StateStore authority without claiming external commit fencing"
domain: [frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-08-10
provenance:
  - "discussion: 2026-08-10 frontend DML operation authority and recovery scope"
code-anchors:
  - "novarocks/frontend/src/dml/coordination.rs (DmlCoordinator)"
  - "novarocks/frontend/src/dml/state_store_journal.rs (StateStoreOperationJournal::mutate_operation_authorized_async)"
---

## 问题

Frontend 的七类持久化 DML 应如何在 control-plane restore、lease renew、并发接管与进程重启下约束 durable operation 写入，同时不把 StateStore authority 误称为外部系统 commit fencing？

## 背景与执行事实

Frontend 已拥有 INSERT、DELETE、UPDATE、MERGE、CTAS、TRUNCATE 与 ADD FILES 的 application flow，并以 StateStore journal 保存 typed lifecycle、opaque provider receipt/evidence、unfinished index 与 statement-family ownership facts。旧的进程内 admission 只在普通 write runner 开头执行一次，既没有与 intent create 放进同一个 StateStore transaction，也不能覆盖 CTAS、TRUNCATE、ADD FILES；journal mutation 同样没有验证当前 lease fence。

StateStore coordination 已提供 incarnation gate、transaction-scoped write admission、operation lease、exact-version fence 以及 commit-unknown recovery。Lease renew 会改变 lease record version，因此 acquire 时捕获的一次性 fence 会在第一次 renew 后失效；durable mutation 必须动态读取 live guard 的最新 fence，并让该 fence 的读取与 renew 串行。

Provider 仍拥有外部 writer、metadata publication、marker inspection、cleanup 与 reconcile truth。StateStore transaction 能拒绝旧 Frontend 写回 journal，却不能撤销已经发往外部系统的请求，也不能单独证明迟到的 provider commit 不会生效。

## 考虑过的选项

第一种选择是只把现有进程内 admission 换成 incarnation gate。改动最小，但 intent create 与 restore 仍可竞态，三类 statement flow 继续绕过，takeover 后的旧 owner 仍可写 journal，因此不能形成 durable authority。

第二种选择是给全部 DML 使用一个全局或 table-scoped lease。它容易理解，也会把不相关 operation 串行化；同表冲突本来应由 provider base state、CAS 与 canonical source ownership裁决，全局 lease 还会把 recovery controller 变成新的单点 leader。

第三种选择是为每个 durable operation 建立独立 lease：intent transaction 验证 write admission，claim 持久化 holder、attempt 与 fencing token，之后每次 journal mutation在同一 transaction验证 live latest fence、persisted attempt 与 expected revision。Recovery 按分片 due index有界扫描并先 claim operation；没有 family historical profile时只延后 due，不改业务 lifecycle。

第四种选择是同时把 fencing token扩展到全部 provider commit协议。它能形成更强的端到端承诺，但会改变 provider SPI、外部提交语义与历史 inspection能力，超出当前可验证的共同契约；在没有真实 provider实现前加入字段只会制造纸面 fencing。

## 裁决

采用第三种选择。Frontend host 在 StateStore open 后创建一个进程级 coordination runtime，统一拥有 incarnation gate、UUIDv7 holder、clock 与 LeaseManager；domain service只能消费它，不能自行切换 restore/write-open mode。

每个 DML operation 使用版本化 resource key `novarocks/frontend/dml/operation/v1/{uuid}`。创建 intent时在同一 transaction验证 current WriteAdmission，并原子写 operation、unfinished 与 recovery-due index；取得 lease后以 expected revision做 fenced claim，持久化完整 holder UUIDv7、coordination attempt UUIDv7、canonical fencing-token v1 bytes与 acquired time，但不持久化 LeaseFence 的 exact record version。

Live authority以共享 async mutex持有 LeaseGuard。Renew、dispatch前current-fence check以及每个 journal transaction validator都从该 guard读取最新 fence；renew commit outcome不确定时必须用同一个 operation id恢复，不能生成新 id重试。所有 lifecycle、fact、terminal、due 与 ADD FILES ownership mutation同时验证 current fence、persisted attempt和 expected revision。

Recovery 使用 16 个 shard的有序 due index、每页最多 128 条、每轮最多 claim 4 个 operation。未安装 statement-family historical recovery时，controller取得 operation authority后只把 due延后，不继续旧 provider call、不改 external evidence。Journal open保持只读，不执行启动扫描或修复。

这一裁决只建立 StateStore durable authority：旧 owner的迟到 journal写回会被拒绝，但不宣称 external commit已被 fencing。端到端外部安全必须由对应 provider的 historical inspection、guarded cleanup或可验证的外部 fencing能力另行完成。

## 接受的妥协（诚实记录）

我们接受 transaction validator使每次 journal mutation多读取 control与lease record，也接受 operation authority、renew task和due index扩大 Frontend DML代码面。这是为获得 restore与journal mutation之间的线性冲突、以及 renew后的 exact fence正确性付出的成本，不是性能上更优的选择。

我们接受当前 production recovery在没有 family profile时只会持续延后 operation，可能让 ambiguous evidence长期保留并需要人工观察。这样做是因为“保持未知”比用新 generation重放旧 commit或猜测 marker更安全；它不是完整恢复能力。

我们还接受当前验收只对一个 Frontend进程内的多个 logical holder与 SQLite StateStore提供运行时证据，其他 provider只做抽象编译与单元/conformance检查。因此本决策不构成双 Frontend部署已验证的声明，也不构成 FoundationDB/MySQL live readiness声明。

## 何时重新评估

- Provider为 distributed write、data mutation或 staged publication提供可跨 generation验证的 historical inspection与 guarded cleanup时，评估把对应 family profile接入 recovery controller。
- 外部系统提供可持久化、可比较且由 provider强制执行的 fencing token时，评估把 control-plane provenance纳入 provider commit协议并升级端到端承诺。
- Operation lease或每次 dispatch前current-fence read成为可测的吞吐瓶颈时，基于冲突率与延迟数据评估批处理或缓存；不得以弱化 transaction validation作为默认优化。
- 产品正式支持多 Frontend同时服务写请求时，必须补真实多进程 takeover、网络分区与 provider迟到提交矩阵，再决定是否需要更强的 holder lifecycle或全局恢复调度。
