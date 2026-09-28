---
id: ADR-0162
title: "Query execution keeps intermediate computation state in memory"
domain: [memory-governance]
status: active
supersedes: []
superseded-by: null
date: 2026-09-29
provenance:
  - "discussion: 2026-09-28 to 2026-09-29 in-memory product boundary and spill retirement review"
code-anchors:
  - "idl/novarocks/service.proto (QueryOptions)"
  - "novarocks/execution/src/exec/operators/sort/sort_processor.rs (SortProcessorFactory)"
  - "novarocks/execution/src/exec/operators/local_exchanger.rs (LocalExchanger)"
  - "novarocks/memory/src/reclaim.rs (Reclaimer)"
---

## 问题

查询中间计算状态是否可以外溢到本地存储，以及这一产品边界如何约束执行算法、协议能力和内存回收？

## 背景与执行事实

NovaRocks 的 FE 负责 SQL 规划和全局协调，BE 在独立进程中运行本地 pipeline。外部表、已发布数据和最终结果的 I/O 各有明确 owner；为了降低驻留内存而把中间计算状态外溢，是另一种执行能力，不能仅凭系统已经存在文件读写就推导出来。

| 对象 | 能力与责任 |
|---|---|
| Sort / TopN | 保留内存 full sort、heap TopN、普通 TopN 与 partition TopN；算法选择、剪枝、ties 和输入释放由排序 owner 负责 |
| LocalExchanger | Buffered 按字节/行阈值背压，Handoff 按 chunk 数交接；EOS、consumer close、取消与真实 Chunk 释放归队列 owner |
| QueryOptions / NativeCompatibilityId | schema 不表达 spill；退役字段号与名字保留为 reserved。descriptor 变化进入既有兼容岛接纳规则 |
| 配置装配 | 不构造 spill 服务或把目录、文件、恢复状态注入执行 owner；未知输入的宽严规则仍由各输入 owner 决定 |
| 内存容量核心 | 容量授予、已登记持有、回收估计和确认释放是独立事实；回收不能凭候选字节增加可授予容量 |
| Connector / cache / publication / 结果流 | 合法 I/O 保持自身语义；它们的文件不自动成为可恢复的查询中间状态 |

ADR-0148 的单进程容量权威与 ADR-0160 的显式 Arrow backing 谱系继续成立。本条收敛产品允许的回收动作：ADR-0148 中提及未来 spill 消费者的描述不再构成引入该能力的授权。容量、账户、holder、pin 和回收结果协议不因此更换；本条也不取代 ADR-0160 的结算规则。

普通 SQL 的会话构造没有启用过 spill。未知 SET 与根配置未知字段如何处理，是既有通用输入契约；删除执行能力不应另外增加一份退役名称表或临时墓碑。QueryOptions 的协议构造与 runtime 投影在删除唯一的 spill 校验来源后采用不可失败转换，外层身份、必填字段和其他真实不变量继续校验。

## 考虑过的选项

**默认禁用并保留外溢实现。设计否决。** 它保留了协议表达、服务装配和恢复分支，却没有提供当前产品承诺。永久 false 开关、空 manager、空 validator 和只返回 Ok 的 Result 会把不存在的能力继续传播给后续 owner。

**以队列满或算子内存压力自动触发外溢。设计否决。** 背压和容量授予不是同一协议。队列阈值不能证明写盘、恢复和归并的峰值有界，也不能保证取消后 I/O、文件与持有者已经收敛。对没有依赖环的 scan Handoff，现有背压就是完整的交接策略，不能借统一接口偷偷扩张为磁盘缓冲。

**完整的外存查询执行架构。待评估。** 有界归并、磁盘配额、skew、异步 I/O、恢复峰值、取消和失败恢复都需要共同设计。它能服务超出内存工作集的负载，但属于新的产品路线；外部存储 exchange 与容错重试同样需要单独的端到端契约。

**内存内执行并完整退出既有外溢能力。采纳。** 保留有效算法、背压和关闭行为，移除不受产品契约支持的状态和表达，使后续容量治理只面对真实 owner 与真实持有者。

## 裁决

1. **中间状态驻留规则。** 查询中间计算状态保留在内存中。执行 owner 不建立 spill/restore 队列、临时外溢文件或自动磁盘缓冲，也不为假设中的外溢回收者预留 hook。
2. **能力完整退出规则。** 删除配置、wire 活跃字段、codec 投影、服务、算子恢复状态和专属指标。protobuf 退役字段采用 reserved 编号与名字；跨版本隔离复用兼容岛规则，不维护历史载体路径的 raw 拒绝或双格式执行。
3. **有效行为保留规则。** 删除包装不改变 full sort / 各类 TopN 分派、增量剪枝、ties、offset 或 schema。LocalExchanger 保留普通通知、EOS、部分/最后 consumer close、取消、late push 丢弃和真实释放；删除异步分支不能作为生命周期正确性的证据。
4. **容量事实分离规则。** 队列背压、回收候选与实际释放不能冒充容量授权。只有真实持有者释放后才撤账；内存内执行不证明全部增长已经受硬限治理。
5. **输入 owner 分离规则。** 能力退出只移除现存表达，不借此重写 SQL 会话或配置的通用未知输入规则。QueryOptions 失去唯一失败来源时收敛为不可失败转换，保留外层真实校验，不新造替代 bounds 校验。
6. **合法 I/O 区分规则。** Connector 数据文件、cache、publication、runtime-filter scratch 和最终结果流按照各自 owner 的协议使用。审查删除时按用途判断，不按文件名或 spill 字样批量清理；不扫描或删除用户历史目录。

## 接受的妥协（诚实记录）

- 工作集需要适配可用内存。多 BE 可以分担部分工作，但单热点分区与全局排序仍可能超过一个进程的可用内存；本条不承诺任意规模查询都能完成。
- full sort 的物化峰值、局部交换入队 overshoot 和其他未受治理增长仍需要容量接线与容器设计解决。普通 Rust 分配仍可能导致进程 OOM，不能把删除外溢实现描述成已经获得 OOM 防护。
- 内存回收可用的动作更少。后续仲裁只能调用真实、已支持的回收者，不能把不存在的 spill 字节纳入释放能力或承诺。
- 未知会话设置与根配置未知字段的现有宽松行为可能继续接受无效果的输入。通用输入契约需要独立裁决和验证，能力退出不建立一次性的名称名单来掩盖这一事实。

## 何时重新评估

- 产品明确要求超过内存工作集的查询必须完成：重新设计完整外存执行或外部 exchange/容错路线，并同时给出有界峰值、配额、失败恢复和取消收敛证据；不能仅恢复某个算子的目录和开关。
- 实际负载的热点分区、全局排序峰值或并发需求不能通过分区、准入与容量治理满足：先量化真实工作集和失败 owner，再评估是否需要改变内存内产品边界。
- 未来回收动作涉及异步 I/O 或外部状态：先定义确认释放、等待、失败与 owner 收敛的协议，不用估计值或请求发出事件返还容量。
- 新的输入 owner 或通用严格输入契约落地：更新该 owner 的外部行为说明与客户端验证，保持本条的执行能力和兼容岛边界，不增加退役名称特判。
