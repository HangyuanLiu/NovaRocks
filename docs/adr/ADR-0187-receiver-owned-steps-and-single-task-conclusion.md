---
id: ADR-0187
title: "Receiver-owned steps preserve domain failure facts, one Task conclusion and physical exit"
domain: [pipeline-execution, error-contracts, distributed-query-lifecycle]
status: active
supersedes: []
partially-supersedes: [ADR-0095]
partially-supersedes-scope:
  ADR-0095: "Only the permission to erase known non-analyze execution facts into an opaque engine failure; Analyze code/span and the separate historical MV lineage facade remain unchanged; absence of SQL source/span does not authorize erasing known execution facts."
superseded-by: null
partially-superseded-by: [ADR-0193]
partially-superseded-by-scope:
  ADR-0193: "Only rule 5's implicit synchronous receipt contract in runtime/completion caching and notification; the single Task acceptance owner, refinement rules and every other rule remain effective."
date: 2026-10-20
provenance:
  - "discussion: 2026-10 receiver-owned preparation, domain failure projection and single Task conclusion"
code-anchors:
  - "novarocks/failure-contract/src/lib.rs (FailureProjection, FailureFact)"
  - "novarocks/spi/src/connector/codec.rs (ConnectorCodecError::from_domain)"
  - "novarocks/proto-codec/src/error.rs (ProtocolError::from_domain)"
  - "novarocks/connector-contract/src/pure_catalogue.rs (compile_read, compile_write)"
  - "novarocks/local-compiler/src/lib.rs (ProviderPreparationError)"
  - "novarocks/execution/src/exec/pipeline/input.rs (RawInput, AcceptedInput, PreparedInput)"
  - "novarocks/execution/src/exec/pipeline/shape.rs (InputPreparation, ReceiverSlot, TransformKernel)"
  - "novarocks/execution/src/exec/pipeline/shape/hydration.rs (HydrateInput)"
  - "novarocks/execution/src/exec/pipeline/step_context.rs (StepContext, TurnWork)"
  - "novarocks/execution/src/exec/pipeline/operator.rs (ExitObservation)"
  - "novarocks/execution/src/runtime/fragment/execution_failure.rs (ExecutionFailure::from_domain)"
  - "novarocks/worker/src/status.rs (TaskStatusOwner::submit_prepared_with_installed)"
  - "novarocks/worker/src/task_conclusion.rs (TaskConclusionState::accept)"
  - "novarocks/query-application/src/execution_error.rs (ExecutionFailureInput)"
  - "novarocks/query-application/src/coordination/runtime_registry.rs (reserve_active_native_owner, join_and_retire)"
  - "novarocks/query-application/src/coordination/statement_native_residual.rs (StatementNativeForegroundLease)"
  - "novarocks/frontend-application/src/task_execution/attempt_convergence.rs (NativeAttemptConvergenceOwner)"
  - "novarocks/mysql-adapter/src/error_mapping.rs (error_kind_for_domain_code)"
---

## 问题

执行引擎怎样在背压、分步计算和异步退出之间保留准确输入与原始失败，并让 Task 结论、逻辑恢复、客户端结果和资源退出各有一个真实 owner？

## 背景与执行事实

[领域错误规则](ADR-0088-domain-owned-sql-error-contracts.md)要求最早拥有完整语义的领域产生事实；[Analyze 错误规则](ADR-0095-typed-sql-analyze-errors-and-wire-mapping.md)要求 code 与真实 AST span 穿过应用边界。[纯程序与监督生命周期](ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md)分开 Accepted、Installed、terminal、read seal 和实际退出；[递归执行类型规则](ADR-0176-recursive-execution-types-and-independent-storage-ownership.md)分开语义、布局、编码与来源。本条补齐它们之间的执行步骤和失败交接，不把上述 ADR 整体替换。

| 对象或事实 | 原 owner 与合同 | 不授予的能力 |
|---|---|---|
| RawInput | 原 edge 持有未接受的数据及可选原 RootInputPermit | 不持有接收方的转换产物、scratch 或准备游标 |
| AcceptedInput / ReceiverSlot | 接收方一次接受原责任，准备与计算共享其 continuation | 准入接收槽不等于覆盖以后所有新增分配；失败不能把输入退回 edge |
| PreparedInput | 接收方自己的能力准备已经完成，计算才可读列 | 不承诺全部解码；合法编码可以保留 |
| StepResult / NextStep | 本步的唯一输出、已提交进度与下一动作 | Pending 不撤销已接受输入、已累计行或已发布输出 |
| PipelineDriver / StepContext | 跨轮次状态归原 driver；一次工作仅短借控制和同一轮额度 | 私有 factory/profile 不进入按下标查找表；不跨返回或 await 保存借用 |
| FailureProjection / FailureFact | 各领域投影自身原因到中立事实，包装仅转发 | 不带通用 retryable、SQL errno，不能用 Display 重新分类 |
| TaskConclusionPort / accepted receipt | Worker 同一事务接受从 Accepted 起的原失败、取消、abort 和成功 seal | runtime、completion、Native 和 FE 不能再独立 first-wins |
| ExitObservation | 必需 finalization、附加 cleanup 与实际退出分别报告 | 超时、Future Drop、一次 close 返回或 terminal 不证明整个 owner 已退出 |
| ExecutionFailureInput | Query Application 消费原 fact、准确 site 与有界安全诊断 | 原 Rust source graph 不跨 wire；位置不是新的 Task 结论 |

生产静态计划只走 [compiled v2 包路径](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md)。这里的步骤与失败合同接在该唯一路径；残留的测试 evaluator 不能成为 v1 构造或解码后门。

本文记录长期合同；其状态不代替对应变更的构建、功能、分布式验收或发布证据。

## 考虑过的选项

**保持字符串载体，在 Execution 或 MySQL 统一分类：设计否决。** 调用面简单，但原资源约束、外部依赖、表示边界和控制观察一旦丢失便不能恢复；统一跨领域大 match 同样把语义作者转移到了错误的 owner。

**driver 先物化再尝试交付：设计否决。** 被拒绝的 edge 会持有放大后的数据，准备与计算分属两个 continuation，且容易在重试时重复转换或转账。原 root 生产前许可也不能变成收到 Chunk 后的普通申请。

**等待或失败时重跑整个算子步骤：设计否决。** 一步可能已接受输入、推进游标或交付输出；通用回滚与重放会重复已提交的工作。查询 attempt 恢复属于另一个应用决策。

**Task terminal 后丢弃所有残余，或所有成功结果一直等到全图退出：设计否决。** 前者丢失真实工作责任，后者让不再贡献结果的失联工作阻塞已完成读结果。正常前台排空预算只限定等待，不能成为退出证明。

**接收方持有线性输入、领域事实贯穿、单一 Task 接受点与原 owner 退出：采纳。** 常用 shape 复用规则，复杂异步或有信用算子直接实现同一低层 step 合同。

## 裁决

1. **Receiver-custody rule。** 先判断原 RawInput 准入，再一次接受到 receiver；拒绝返回同一原输入且无准备/转账。`InputPreparation::prepare` 消费 AcceptedInput，Pending 或失败仍由同 receiver 持有。普通 kernel 只接 PreparedInput。当前准备调用已有 hydrate；后续规划、执行与内部值调用在同一接收方接线，driver 不重做展开。
2. **Single-output continuation rule。** 每次工作至多交付一个输出，只有输出 edge 空闲才推进生产。多批结果与剩余行留在原 continuation；output+wait 先交付输出，output+finished 排空后再传播 EOS。输入与 continuation 排空后的无 payload EOS 由作者显式报告，只有可能产尾输出的 finalizer 继续先取得输出准入；数据结束不证明物理退出。Full 是真实 owner 的推进信号，不自动成为查询失败或内存等待；Representation 区分最小单元与当前交付边界，Caller 保留原控制或资源原因。
3. **Short-borrow rule。** StepContext 只短借真实 driver 共享状态与本轮能力；同一 turn 的嵌套 reborrow 不补充额度。要求 Send 的长期状态随原 driver 移动，不因此强制缓存 Sync。私有构造能力在准备期装入算子；异步作业持必要 owned 输入和句柄，FE 与异步 source 保留自己的操作 owner。当前 TurnWork 仅为工作量额度，不声称字节准入、工作集或线程 stock 已交付。
4. **Domain-fact rule。** 每个领域实现自己的窄 FailureProjection；底层契约不依赖 Execution/Worker/Native，聚合包装只转发。SPI/Protocol 的 Domain 保同一原原因；Capacity 的已知数量与 Unsupported 的原稳定 reason 不在桥上重新 author。编译边界优先保已产生的领域错误，不让之后才调用的合作控制 checkpoint 覆盖它；已观察并锁存的先行控制与成功路径末尾控制仍按原合同保留。ExecutionFailure 保原 source 并附调用 context，完成态不提供 From<String>/From<&str> 或等价默认文本吞入口。被 selection/guard 屏蔽的行错误不晋升；只有 required-root 的真实失败进入执行链。TaskFailure::category 是有损展示投影，不能取代 fact。
5. **Single-acceptance rule。** Worker TaskStatusOwner 是唯一 Task 接受事务。先接受的取消保留为 Canceled，晚 failure/cleanup 只附诊断；只有尚未固定的派生 PeerTaskFailed 占位可以被真实 originating cause 一次细化。runtime/completion 缓存并通知同一收据；强制 stop 可以加强实际动作，不能覆盖原因。
6. **Required-before-seal rule。** 正确输出所必需的 finalization 在其成功 seal 前参加裁决。原失败立即提交，退出继续观察；一个 cleanup 错误或可捕获 panic 不跳过兄弟 owner，也不重启同一个 hook。seal 后 cleanup 不翻转固定结局。Task 成功、encoder 交接 ContextHeld、SQL seal、send 作业退出和最后 alias 退出是不同范围。
7. **Same-owner residual rule。** 原 Native attempt 的局部 holders、准确远端 contexts 和原 scope/registration 分别保留。派发前在原进程监督者预注册同一 owner 的责任，前台预算耗尽或取消只归还同一 Box；原 JoinHandle、query guard 与业务 publication owner 不 detach、不克隆、不重新 mint WorkScope。真正无法证明本地退出时保原 owner、原因和义务直到显式 process-exit，健康兄弟仍继续清理。Native caller capability 归还原 process record 仅为 custody，不冒充网络实际退出。
8. **Bounded-remote-observation rule。** 对已经接管且有 covered Worker receipt 的 Context，正常 Completed 的远端收敛还消费原成功 Release ACK；后者携带贡献而不证明物理退出。cleanup 使用原 never-established、covered exit 或准确 process replacement；不为缺失贡献制造成功。原远端观察预算不因移交或 Future Drop 重借；先保已接受的正面事实，否则到期锁存 RemoteTrackingEnded。Unknown 之后不升格，不宣称 WorkerStopped/RF 完整；所有本地 holders 均须有对应的具名退出事实；仅 Native operations/status subscriptions 的 caller capability 可以交还同一原 process record，由该原 owner 保留真实 join 责任。满足这些准确本地条件后，才可按原义务规则结束未知远端跟踪。本地未退出不能借远端预算退休。
9. **Recovery-and-client rule。** Query Application 结合准确原因、效果、可见性、隔离及 attempt/deadline 预算决定恢复；容量、Representation、普通 dependency 或本地 timeout 不自行授权换 attempt。启动与 schema/batch 流式出口保同一 descriptor 和 ErrorPhase::Execution，不伪造 Parse/span。SQL 授权为 1227/42000 或准确表级 1142/42000，登录认证仍为 1045/28000；外部凭据拒绝是 dependency。MySQL 仅编码 domain descriptor 对应的 errno/SQLSTATE，失败流不能成功 EOF，publication KnownCommitted/Unknown 不擦除已选执行原因或授权透明重试。
10. **Bounded-diagnostic rule。** 机器事实与安全文本分别保留；producer 在进入 wire/client 前脱敏，UTF-8 截断仅限制长度。主安全诊断至多 512 字节，Task 次要 late/cleanup 诊断共四槽、各至多 512 字节并保 dropped count；预算耗尽不删除主因事实。本地 source/panic payload 仍在原 owner，不能为传输复制无界错误图。

Native 兼容版本由 `novarocks/version/src/lib.rs::NATIVE_COMPAT_EPOCH` 与真实 carrier 描述共同确定。ADR-0161 记录的 epoch 4 是当时 Accepted/Installed 切换的历史事实；当前普通构建显式 epoch 为 6，开发兼容专项的不同岛为 7，不能把旧数字恢复成协商默认值。旧文档保持其历史正文。内存规则沿 ADR-0160 → ADR-0166 → ADR-0167 的原替代链解释；本条不恢复 Reservation/Arrow 谱系的历史协议，也不定义新的容量账本、hold、allocator 或容量等待政策。

## 接受的妥协（诚实记录）

- 错误必须逐语义边界接线，领域投影及稳定 reason 的维护量高于统一 String；代价换取独立 owner 和准确恢复判断。仅用于展示且没有已知机器事实的诊断仍可不透明，不能反过来抹除已有事实。
- receiver、shape 与复杂 operator 需要显式保留 continuation；工作额度不抢占已经进入的 kernel。真实内存等待、工作集、字节预算和多前缀策略仍要由相应 owner 后续交付，不能从通用 Pending 或一输出字段推断已实现。
- SQL 可以先固定结果而残余仍真实占用。前台预算保持可用性，未知远端状态允许按原规则结束跟踪；本地退出未知仍保责任和健康失败，不承诺任意故障下都能安全回收同一进程内对象。
- 原 Rust source 不跨进程；wire 只保中立稳定事实和有界安全诊断。可读文本不保证逐字等于本地 source，诊断预算外只保丢弃计数；它不能影响主因、恢复或客户端 descriptor。

## 何时重新评估

- 新增值表示、资源维度或 provider 原因时，扩展实际领域投影、稳定 reason 与对应端到端 oracle，不添加中央跨领域 classifier。
- 新的缓存或工作集进入 driver 时，先明确真正长期 owner、Send 及终止清引用，再增加 StepContext 具名短借用；不能借参数扩展恢复服务袋。
- 工作量粒度、不可中断 kernel 或局部实际退出不能满足已接受的可用性界时，重审真实任务切块与 join 接缝；不能延长 deadline、丢 owner 或提前停止责任计数。
- 新客户端协议需要不同诊断字段，或需要恢复已经可见的输出时，由该协议与逻辑执行 owner 定义新合同；不回灌错误文本分类，也不据内部 cause 声明通用 retryable。
