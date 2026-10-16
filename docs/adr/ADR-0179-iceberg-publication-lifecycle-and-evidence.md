---
id: ADR-0179
title: "Iceberg publication has one frozen request, dispatch journal and independently bounded evidence owner"
domain: [provider-spi, distributed-query-lifecycle, crate-boundary, table-maintenance]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0118
    scope: "Unified Transaction and late SDK TableCommit carrier; provider-private catalog ownership and operation admission remain active"
    link: "ADR-0118-iceberg-provider-private-catalog-owner.md"
  - id: ADR-0171
    scope: "Late SDK TableCommit conversion, independent transaction outcome and prohibition of operation-owned reusable rewrite manifests; immutable intent, dependency, inheritance and artifact ownership contracts remain active"
    link: "ADR-0171-commit-operation-model.md"
  - id: ADR-0175
    scope: "SDK session and generic mutation facade ownership, caller deadline after dispatch and FE staged-result projection; authentication, header provenance, frozen routing and redaction rules remain active"
    link: "ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md"
  - id: ADR-0174
    scope: "P2 public terminal TableCommit builder exit and U4 directory discovery exit routing; all unrelated baseline and patch policies remain active"
    link: "ADR-0174-vendored-iceberg-baseline-and-patch-policy.md"
date: 2026-10-16
provenance:
  - "discussion: approved IRU-6a spec revision 15 and implementation plan revision 4"
code-anchors:
  - "novarocks/connector/iceberg/src/rest_protocol/attempt.rs (SessionGate, ensure_session)"
  - "novarocks/connector/iceberg/src/catalog/publication.rs (CatalogEvidencePolicy, PublicationGeneration)"
  - "novarocks/connector/iceberg/src/commit/attempt/observation.rs (PublicationJournal)"
  - "novarocks/connector/iceberg/src/commit/attempt/observation/dispatch.rs (publish_once)"
  - "novarocks/connector/iceberg/src/commit/model/request.rs (FrozenRequest)"
  - "novarocks/connector/iceberg/src/commit/snapshot_lifecycle_helpers/retention.rs"
  - "novarocks/connector/iceberg/src/commit/snapshot_lifecycle_helpers/reclamation.rs"
  - "tools/ci/check-iceberg-provider-dependency-boundary.py"
---

## 问题

外部变更已经发出后，语句取消、同步桥接错误、慢响应、缓存安装或对象构造失败都不能证明提交没有发生。多个适配器各自推断结果，会让同一请求同时成为“可以清理”和“可能已发布”，也会使维护重试、跨 generation 的 ref 保护及物理回收依赖不同的事实。

## 裁决

1. **一个会话权威。** REST generation 的 Provider 自有 HTTP/auth/session owner 成功建立一份配置 binding 并冻结路由、认证来源和能力；失败的初始化 attempt 退出后可开启新的初始化，不残留失败 binding。普通读取、受控发布、凭证取得共用该会话事实；生产 Provider 和 Server 的 normal/build 依赖闭包禁止 REST SDK，dev 互操作对照允许。HMS 保留 SDK 读取且始终拒绝变更。ADR-0175 的认证头唯一写者、公共路由上下文、服务端属性不覆盖认证和脱敏规则继续有效。
2. **冻结请求先于外发。** namespace/table/view 和数据、ref、维护发布均先完成准入、语义验证、请求编码及归属计划。已有表冻结原 incarnation UUID 和必要 CAS，不能通过重新加载填补缺失身份。每次 attempt 的幂等 key、请求摘要和基线准确绑定；重准备保留固定操作意图、评价时间和 plan digest，但使用新的 attempt、请求和 key。
3. **journal 唯一拥有结论。** 准备、claim、派发、证据和收尾属于一个条目。claim 在任何 await 前排除重复调用；只有屏障成功才开启外发。适配器返回后先同步记录其观察，再执行收尾。NotDispatched、明确拒绝、Applied、Unknown 来自实际观察阶段，不能从 SDK 错误类别、文本或重试标志反推已派发变更的结果。NoOp 明确记录其效果。
4. **独立期限与真实退出。** 准备受语句 deadline/stop 和维护总预算约束；外发区间从新起点计算独立 evidence 期限。finalization 和 reclamation 各有完整独立期限。语句取消禁止新工作，不能丢弃已经开始的非可取消 I/O。所有权覆盖到实际退出，超时 ACK 不等于任务退出。Applied 后物化、cache 更新或清理失败仅进入收尾事实，不降级提交结论。Hadoop conditional-create 在不替换安装后把 hint 更新作为收尾；旧 update 必须 metadata 与 hint 两次写入均确认成功才有 Applied，之前失败仍按实际阶段保留 Unknown。
5. **正面核查只追加事实。** 只有与准确请求绑定的归属标记、版本、UUID、快照及必要内容匹配才可从 Unknown 追加 positive proof。旧状态、缺失、偶然相同的当前后置条件都保持 Unknown。证明与 owner 收尾事实在同一个 journal 锁内可见，核查不伪称未观察的 Hadoop cache/hint 安装已完成；原始 Unknown 仍保留。进程内证据保留原 journal owner，wire-only 证据不能重建一次新的派发机会。
6. **跨 generation 的本地协调。** 同一 catalog scope 的表锁由真实 service/factory 注入，覆盖重叠 G1/G2 lease；不使用全局 registry。锁后读取当前权威 metadata，holder/waiter 到真实退出才回收。确定结案的 expiry 与本地 ref 修改串行；Unknown 释放锁后迟到 expiry 仍可能覆盖后来的 ref，本地锁不提供外部 CAS。
7. **retention 与回收分开。** 候选使用完整 branch/tag 图、table/call/ref 的明确优先级及固定评价时间；main 永不过期。确定冲突允许政策内有界重准备，Unknown、UUID replacement、总预算耗尽停止。Applied 后先定位实际提交版本和前驱，独立审计丢失 refs；异常 ref 的文件保护且诊断明确。回收保护当前完整 snapshot roots，包括无 ref 的历史；Puffin 任一存活 blob 保护整路径。不可变路径缓存不做 HEAD；新根读取失败、长度矛盾或前驱不可用均停止删除。每次 DELETE 发出前 age≤T，排队或扫描过窗重新复核；失败不续期。
8. **传输无关的结果。** 中立 SPI 区分 mutation effect、receipt/evidence 与 finalization；FE 只投影 Provider 事实，MySQL 只编码用户结果。lake publication 的 crash-only 结果、target OCC 与年龄窗 GC 沿 ADR-0110；Provider 私有 catalog 所有权沿 ADR-0118；操作、物理对象、继承字段和清理模型沿 ADR-0171。

## 考虑过的选项

- **保留 SDK mutation facade 和错误推断：否决。** 会保留第二个可派发面、第二份结果状态及 P2 API 补丁，不能证明实际外发与收尾的顺序。
- **在 bridge 外超时并丢弃 Future：否决。** 外部请求仍可能生效，本地却失去 actual-exit 与证据 owner。
- **核查旧状态后自动重发：否决。** 旧状态不证明原请求已退出或永远不会生效；6a 不发 recovery mutation。
- **每批文件完整扫描或复核：否决。** 保护视图按权威变化增量更新，新鲜度由真实单调时间约束，与批数无关。
- **永久依赖图边界：采用。** 检查真实 Cargo normal/build/dev 边的不同政策，保留原跨 owner 依赖限制；一次迁移搜索只留执行材料，不成为按旧符号限制未来设计的 guard。

## 接受的妥协与交接

REST 服务端没有完整 ref 保护集 CAS；本地锁不涵盖跨 catalog 别名及外部写者。独立 evidence 到期仍可能有迟到成功，6a 不把 Unknown 当作安全删除依据。Unknown 产物的持久隔离、跨重启恢复和单调守卫交 IRU-6b，不能用当前进程 journal 冒充 durable recovery。

Hadoop conditional-create 保留不替换安装，旧 update 保留原 metadata+hint 双写确认及编号、发现协议；cache 收尾与发布证据分离，conditional-create 的 hint 收尾不扩张到 update。该协议的剩余并发风险和目录发现 U4 的退出交 IRU-6c；本次没有引入新的 Hadoop CAS 协议。U3 完整 refs accessor 仍由 retention、依赖验证与回收的真实读取需求保留。

同步 bridge 的线程并发、按 catalog 的资源隔离和长期异步贯通由 Query Application / workload-control 后续治理；6a 交付阶段与 actual-exit 观察，内存限额归 MEM-1。本 ADR 描述已接受的契约；最终通过与未运行项以同候选 SHA 的执行证据为准，单测和设施测试不替代原生 1FE+3BE、Java 与 SQL 验收。

## 验证与重新评估

journal/adapter 单测验证屏障、一次派发、迟到与正面核查、独立期限和收尾；真实 service/factory 测试验证 generation/表协调；手工 snapshot 图、对象请求记录和虚拟时间验证 retention/回收。显式 Java/SQL/system 场景验证服务端阶段与实际退出，跨进程生产验收使用 1FE+3BE。依赖守卫的直接、传递、build、target 与 dev-only 正反样例检查真实 Cargo 图。

当标准 REST 提供完整原子保护语义、Hadoop 协议重设计、持久恢复或完整异步 owner 落地时，重新审查相应妥协，不能在适配层增加 fallback。
