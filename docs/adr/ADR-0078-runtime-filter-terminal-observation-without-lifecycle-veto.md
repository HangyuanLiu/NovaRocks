---
id: ADR-0078
title: "Runtime filter terminal observation has no lifecycle veto"
domain: [runtime-filter, distributed-query-lifecycle]
status: active
supersedes: [ADR-0076]
superseded-by: null
date: 2026-08-15
provenance:
  - "mechanism: terminal proof, negative attestation, and frontend liveness convergence"
  - "discussion: 2026-08-15 observation-only runtime filter terminal ownership"
code-anchors:
  - "novarocks/backend/src/query_lifecycle/registry.rs (QueryLifecycleRegistry)"
  - "novarocks/frontend/src/coordinator/query_lifecycle/lease.rs (AttemptControl)"
  - "novarocks-server/src/composition.rs (compose_backend_server_config)"
---

## 问题

当 runtime filter 的 terminal observation 发生捕获、编码或传输故障时，查询生命周期应如何保留可诊断的
participant 事实，同时保证纯性能优化的观测面不会否决本来正确的查询结果？

## 背景与执行事实

ADR-0043 已确定 runtime filter evaluator 只产生保守的性能 Effect，ADR-0044 将 participant 的物理生命周期
留在 Backend，ADR-0076 则曾把 terminal observation 作为 typed QLC contribution 跨进程交付。这个边界仍然成立，
但把 observation capture 失败直接等同于 query 成败，会让性能诊断反向成为 SQL 正确性的权威。

查询正确性已有独立且唯一的守卫：封存计划的静态验证、实际 fragment 执行结果、显式取消/abort，以及 QLC 的
participant 身份和终止状态机。join 本体而不是 runtime filter observation 保证 SQL 结果；因此 observation 的
缺失、降级或负 attestation 不能改变一份本已成功的 canonical execution verdict。

另一方面，终止时仍必须区分“明确证明”“明确无法证明”和“在有界等待后仍未得到 outcome”。否则 Frontend 会把
缺失误读为 empty observation，或把 transport/liveness 问题伪装成 Backend 已证明的事实。该区分也是跨进程
故障注入和 retained convergence snapshot 的可测试接口。

写入 fragment 的 commit evidence 同样需要上界，但它是 Connector 写入事实，不是 runtime filter 观测。完整
应用配置的 wire、默认值和跨 section 校验由 Server 独占，依据 ADR-0072 将已解析的
`WriteCommitEvidenceLimits` 投影给 Backend；Backend 不读取根配置，也不把该预算回流为 Frontend 或 observation
的隐式策略。

## 考虑过的选项

1. **延续 ADR-0076 的 capture-fail-closed 终止语义。** 这能保证 profile 从不静默丢失，但会让诊断面拥有
   否决查询的能力，违背 runtime filter 仅是性能优化的边界。
2. **capture 或传输失败时统一构造 empty contribution。** 查询不会被 observation 阻塞，但“没有 runtime filter”与
   “不能证明 runtime filter observation”不可区分，长期会污染 profile、故障归因和回归测试。
3. **将结果正确性与 observation 终止证明分离，并以 P0/P1/P2 交付协议表达缺失。** 选择此方案。
4. **把所有 observation event 持久化，再由 Frontend 在后台重放。** 可改善取证，但引入 durable telemetry、重放和
   retention authority；当前的 query-scoped profile 不需要也无法证明该成本合理。

## 裁决

Runtime filter terminal observation 是无否决权的观测面。它只能补充已由 canonical lifecycle verdict 决定的
查询结果；不得改变 SQL 正确性、fragment 成败、取消结果或已完成查询的成功/失败分类。唯一的 correctness
guards 仍是封存计划验证、实际执行/取消事实和 QLC 的 canonical terminal state。

终止交付使用三层明确契约：

1. **P0 proof capacity。** Backend 在发出 `ControlReady` 前为每个已接纳 participant 预留 terminal outcome
   retention 容量；终止后 immutable terminal record 可构造 proof，容量耗尽不能在事后把已接纳 participant
   变成无记录。
2. **P1 attestation。** Backend 对每个已接纳 participant 发送且只发送一个 typed terminal outcome：可验证的
   terminal proof，或包含明确原因的 negative attestation。前者传递 observation snapshot，后者传递“不能证明”的
   原因；两者均不是对 SQL 结果的额外 veto。
3. **P2 liveness。** Frontend 仅在 P1 后仍缺少合法 outcome、control stream/heartbeat 已确认失活，或有界等待
   到期时，产生 typed liveness 或 `NoOutcome` 收敛事实。它不能从 absence 推导 empty observation，也不能反向
   读取 Backend 私有 store。

Frontend 对 admitted participant 集合进行一次、确定性的 outcome 收敛，并保留 immutable convergence snapshot。
该 snapshot 的 source 必须是 typed 分类（Backend attestation、Frontend liveness、NoOutcome），而不是从错误字符串
猜测；调试和测试接口只读取该 retained value。

`WriteCommitEvidenceLimits` 由 Server 在 composition 中根据 ADR-0072 的应用配置 wire 解析、校验并投影给
`BackendServerConfig`。Backend fragment service 对每个 fragment 的 evidence collector 使用该 typed limit；Connector
提供的 evidence 超过预算应以明确写入错误处理，不得通过 runtime-filter observation 或进程全局配置绕过。

## 接受的妥协（诚实记录）

我们接受 observation 可能比 canonical query verdict 更晚到达、也可能永久只留下 negative attestation 或
NoOutcome。这降低了“每次成功查询都必须带完整 runtime filter profile”的诊断完整性；选择它不是因为不完整
profile 更好，而是因为让性能观测否决正确执行会破坏系统最重要的语义边界。

P0 reservation 与 retained terminal outcome 会为每个 admitted participant 占用有界内存，并增加 admission、终止
和 tombstone 的实现复杂度。我们接受这项成本，是为了把“容量不足”前移到可拒绝的 admission 时刻，而非在
终止时丢失已经承诺交付的证明。

P1/P2 对 failure source 做 typed 分类，会比只返回一个错误文本多出 wire、状态机和测试维护。我们接受该复杂度，
因为字符串匹配会随错误措辞漂移，且无法区分 Backend 无法 attestation 与 Frontend 未观察到 outcome。

写入 evidence budget 经过 Server 投影而非由 Connector 自行读取配置，意味着新增预算字段要修改 composition
代码。这是显式的集成成本；它保留了 ADR-0072 的单一 wire authority，避免每个执行域悄悄形成不同默认值。

## 何时重新评估

1. 若 runtime filter 不再只是保守性能优化、而被赋予影响 SQL 可见结果的语义，应先定义新的 correctness
   contract 与独立 verdict owner；不得直接把 observation 恢复为 QLC veto。
2. 若 P0 reservation 在生产规模下成为可观测的 admission 拒绝来源，应评估更精确的 per-participant sizing 或
   分层 retention；不得改为终止时静默丢弃 outcome。
3. 若需要事件级、跨 query 的长期取证，应新增具有独立 retention、隐私和重放 authority 的 durable telemetry，
   而不是扩张 P1 terminal outcome。
4. 若有第二个 application composition root 需要配置 write evidence budget，应按 ADR-0072 重新评估稳定的
   resolved-config API；不得复制 TOML wire schema 或让 Backend/Connector 直接解析根配置。
