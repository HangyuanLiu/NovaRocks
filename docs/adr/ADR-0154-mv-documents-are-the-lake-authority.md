---
id: ADR-0154
title: "MV documents and exact publication attachments are the lake authority"
domain: [frontend-mv, provider-spi]
status: active
supersedes: [ADR-0112]
superseded-by: null
date: 2026-09-21
provenance:
  - "discussion: 2026-09-08 MV storage contract and persistence design, accepted revision 7"
  - "implementation: canonical MV document persistence and native REST integration"
code-anchors:
  - "novarocks/mv-application/src/persistence/codec/model.rs (DefinitionDocument, InterpretationDocument, PublicationDocument, ConfigurationDocument)"
  - "novarocks/mv-application/src/persistence/documents.rs (observe_current_management_document_set)"
  - "novarocks/connector/iceberg/src/document_storage/observation.rs (project_documents)"
  - "novarocks/frontend-application/src/mv/domain/staged_create.rs (install_configured_current_projection)"
  - "novarocks/frontend-application/src/mv/domain/iceberg_refresh.rs (update_iceberg_mv_configuration_with_ports)"
---

## 问题

MV 的计算含义、物理解释、发布进度和配置分别由谁持久保存，读者又如何证明它们属于同一个准确的湖上版本？

## 背景与执行事实

旧的整包 `MvDescriptorV3` 同时装入定义、解释、配置和发布状态。修改其中一个事实会重写无关事实；读者也容易把当前 descriptor 与另一快照的数据拼接。旧 ADR-0112 对运行态和 StateStore 的边界仍有价值，但它所指定的 descriptor 权威已不适用于独立文档及快照附着。

| 事实 | 持久责任 | 读者必须验证的关系 |
|---|---|---|
| D：计算定义 | MV 领域自有版本化 IDL、codec 和不可变 revision；保存查询、稳定源绑定、输出及计算身份。 | P 引用准确的 D revision；同名源对象重建不能以名称冒充原绑定。 |
| L：物化解释 | MV 领域保存状态槽、类型、算法角色、apply key 和准确物理绑定的不可变 revision。 | P 引用准确的 L revision；物理字段、schema/spec 等 provider 事实按同代准确观察绑定。 |
| P：发布 | MV 领域记录 publication identity、D/L revision 与实际输入水位；provider 把它原子附着于准确输出对象和 snapshot。 | 当前输出 snapshot 必须有匹配的 P；旧 snapshot 只能沿自己的 P→D/L 关系解释。 |
| C：声明式配置 | MV 领域保存策略、暂停和调度声明，由表级 metadata 更新。 | 配置变更不制造数据 snapshot，不重新宣布未改变的 D/L/P。 |

`observe_current_management_document_set` 解释和校验领域文档，`project_documents` 只投影 provider 持有的物理文档图和准确附着。Provider 保证 opaque 内容原样往返、完整性、对象身份、物理版本、目标提交及保留，不解码 MV 的聚合、状态或调度语义。格式版本、文档 revision、计算身份、输出 snapshot 和管理归属各有不同用途，不能由单个 hash 或 latest 属性替代。

StateStore 的 MV 记录是带来源的可重建 Accelerator。当前 FE 的 FIFO 准入、effect/terminal、scheduler、readiness 和接续责任是 ProcessRuntime；重启不能从旧本地记录获得继续派发的权力。湖上准确观察与受控管理接续才决定新进程可否管理。独立 owner/kind 标记及诊断用 incarnation 由目标表持有；它们不替代访问授权或 provider OCC。

## 考虑过的选项

1. **继续以整包 descriptor 和可变当前发布属性作权威。设计否决。** 写入路径简单，但无关事实反复重写；数据提交、发布指针和解释版本可以分属不同原子点，读者无法证明准确配对。
2. **把 MV 语义 schema 交给 provider，或让 Server 转换属性袋。设计否决。** Provider 可直接优化存储布局，却会随 AVG、UNION、状态编码等领域语义演进；Server 也会成为第二解释者。
3. **由 MV 领域定义 D/L/P/C，provider 保存 opaque 内容并签发准确物理附着。采纳。** 两侧各解释自己的事实，跨边界只传受治理的文档外壳、修订、引用和提交条件。
4. **让 StateStore 保存可恢复的发布账和管理租约。设计否决。** 跨重启本地状态会重新授权可能已经派发但结果未知的湖上效果，与 crash-only outcome 冲突。
5. **为所有历史版本建立通用迁移或双读。待评估。** 它能降低存量 MV 重建成本，但需要可证明的旧字段语义与准确 snapshot 关系；当前未建立这种证明，不能用猜测兼容放宽读取。

## 裁决

1. **唯一持久语义来源。** MV 领域自有 D/L/P/C 的 IDL、codec、版本和引用校验；运行对象、AST、计划及临时 catalog handle 不直接成为持久格式。`MvDescriptorV3` 不能作为新写入或生产读取的权威。
2. **按准确发布解释输出。** 当前或历史 snapshot 的读者从该输出的 P 解析所引用的 D/L，校验目标对象、字段和 provider 版本。缺少必需文档、附着、受支持的编码或同代绑定时拒绝；不能静默采用 latest D/L、旧发布或错误剪枝。
3. **一次目标提交。** 首次、普通、增量、全量、metadata-only 及 repartition 刷新将数据/布局变化与新 P 在同一个目标提交中生效，并保持冻结的 expected main 条件。Metadata-only 也创建准确附着的新 snapshot；C-only 更新只修改配置 metadata。CREATE 在不可见准备获得稳定物理绑定后，以同一 create intent 原子公布所需 D/L/C。
4. **按所有者解释。** MV 决定计算和文档语义；provider 决定表对象、schema/spec、snapshot/ref、文件、提交条件、附件和保留；FE ManagementEntrance 串行承担同一目标的业务写准入及效果结算。未知提交不凭本地超时推断未提交，旧进程效果未收束时不重新授权相冲突的写。
5. **只加速，不授权。** Startup、wipe、显式重观察和提交后的投影从准确湖上事实重建 StateStore Accelerator，并按来源 revision 防止旧投影覆盖新投影。目录不完整与单目标文档损坏分开隔离；本地缓存、调度记录和过期观察均不能补齐缺失的持久事实。

## 接受的妥协（诚实记录）

分离的文档和不可变 revision 增加 codec、引用验证、对象存储与保留成本；每次刷新都产生 snapshot，metadata-only 也会增加元数据。准确重观察及冲突后的重读增加远端 I/O，目录不完整时局部 MV 可能暂不可用。FE 重启会丢失运行中进度与调度历史，运维接续可能需要人工证明旧效果已结束。我们接受这些成本，以维持一个可验证的湖上发布边界。

该裁决定义长期权威，不表示所有现存生产消费者和历史版本已迁移完成。未迁移的旧属性读取必须在产品入口关闭或完成迁移后才能宣称旧格式退役；未满足准确 P 附着的维护操作也不能凭旧 P 推断新 snapshot 已发布。

## 何时重新评估

- 若 provider 可提供经验证的历史版本迁移证明，覆盖旧 descriptor 与每个输出 snapshot 的准确绑定，可单独设计受控迁移；在此之前旧格式保持拒绝或重建。
- 若真实工作负载表明 snapshot 或文档保留成本不可接受，评估新的可验证附着与保留能力，同时保留一次目标提交和准确读约束。
- 若产品要求跨 FE 并发管理或持久调度恢复，先定义外部效果寿命、权威围栏和读取协议，再评估是否需要新的管理机制；不能直接把 Accelerator 提升为权威。
