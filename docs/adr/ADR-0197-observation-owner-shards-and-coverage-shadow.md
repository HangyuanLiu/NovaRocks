---
id: ADR-0197
title: "Observation owners publish shards atomically and shadow unavailable coverage"
domain: [memory-governance]
status: active
supersedes: []
partially-supersedes: [ADR-0167]
superseded-by: null
date: 2026-10-23
provenance:
  - "discussion: 2026-10-23 observation owner scope, bounded record regions and role diagnostics"
code-anchors:
  - "novarocks/memory/src/lane/shards.rs (LaneShards, create_lane_shards)"
  - "novarocks/memory/src/lane/store.rs (RecordRegion, ensure_segment)"
  - "novarocks/memory/src/attribution/scope.rs (AmbientStep)"
  - "novarocks/memory/src/attribution/future.rs (AttributedFuture)"
  - "novarocks/memory/src/attribution/roles.rs (ThreadRole)"
  - "novarocks/memory/src/lifecycle.rs (AccountHandle::retire)"
---

## 问题

观测 owner 如何在有限记录容量、线程迁移与异步析构下保持来源正确，并确保覆盖缺失不改变资金准入？

## 背景与执行事实

| 实体 | 职责与边界 |
|---|---|
| `LaneShards` | 账户的观测 owner 句柄；覆盖态持有 16 条 lane，未覆盖态保留缺口原因。两种状态都可执行同步闭包。 |
| `FundingDomain` | 资金资格与独立结算 owner；资金 lane 仍可单独进入，和观测 owner 分开。 |
| `RecordStore` | 稳定记录地址、代次与回收；身份不授予访问权。永生区之后，资金区与观测区平分动态位置。 |
| TLS ambient/explicit | 无绑定、真实记录或遮蔽三种状态；遮蔽表明 owner 已知而记录不可用。 |
| `AttributedFuture` | 构造闭包、每次 poll、就地 Drop 分别进入同一个 owner 句柄；作用域不跨 await。 |
| `ThreadRole` | 应用登记并明确冻结的有界诊断角色；hook 只读取线程上的数字，不访问角色表。 |

记录库生产容量为 262144，永生区为 16 个角色（含 unknown）×16 个栈分片，共 256 条；资金区与观测区各 130944 条。资金域的活动 owner 配置上限不得超过资金区容量，但历史残留记录另有保留条件，活动名额不是历史存储上界。两个动态区共用稳定 segment 数组，边界可以落在同一 segment 内。

本条部分替代 ADR-0167 的规则 1（仅观测 owner 的覆盖失败策略）、3（容量分区）、5（owner 与 future 作用域）、10（有界角色与分区诊断）。其资金授权、尺寸段、8 B token、64 B 记录、单槽批量、真实分配计数/slot pin、释放路由与 Tree Borrows 验证边界继续有效。

## 考虑过的选项

- **逐条创建并发布分片：设计否决。** 实现直接，但关闭/退役可能只看到半组；失败后丢弃句柄不能撤销已发布的成员和注册责任。
- **覆盖缺失时沿用外层 ambient：设计否决（观测 owner）。** 工作得以继续，但会把来源记到外层服务或别的查询。资金域仍保留其既有回落合同，要求应用把它组合在同一 owner 的外层作用域内。
- **两区共享空闲位置：设计否决。** 提高利用率，却允许纯观测残留挤占资金准入；观测不能增加 SQL 拒绝。
- **独立区锁下直接覆盖 segment 指针：设计否决。** 同一边界 segment 可能被发布两次，早期发出的记录身份会指向错误 backing。
- **固定记录预算按残留字节推算：设计否决。** 非零大块、R1 小对象、slot pin 和强句柄都可保留记录，字节为零不能证明可回收。
- **每任务或每 driver 再各建一组分片：待评估。** 可以细化诊断，但会放大记录消耗与 owner 生命周期；当前按账户建组，未裁决更细粒度。
- **整组预备、单段发布与遮蔽：采用。** 准备成本在控制面承担，以明确缺口保住来源边界。

## 裁决

1. **整组发布。** 先在账户 gate 外取得所有记录、建立 lane/member 节点和集合 Arc 外壳，再在同一段账户路径共享 gate 内检查关闭并一次发布所有成员及 registry 强引用；该段不分配。关闭拒绝整个组；准备失败释放未发布资源，并返回有缺口的句柄。关闭/退役相对发布只见整组或空组。
2. **失败遮蔽。** 未覆盖句柄或覆盖态选中的 lane 已封口时，安装 ambient 遮蔽并继续闭包；带来源分配落到本线程角色的未归属记录。合法 explicit 仍优先。未覆盖态派生的 explicit owner 遮蔽显式层，不发布 R1 小对象事实。遮蔽不是记录，不解析、不钉住、不进入尾部。正常返回与 unwind 都恢复外层。资金域 lane 的失败回落不变。
3. **线程选片。** 线程首次使用以 relaxed 顺序计数取号，模 16 后稳定存于无需析构的 const TLS；TLS 不可用才按栈选片。分片只分散结构性写入，不宣称吞吐或延迟收益。
4. **完整 future 作用域。** 接口接收构造闭包；构造、poll 与内部 future 的就地 Drop 都经句柄作用域。内部 future 用 `ManuallyDrop`，只析构一次且不移动已 pin 的值；外层绑定在返回或 unwind 后恢复。spawn 不继承绑定。
5. **区域隔离与唯一发布。** 记录获取明确选择资金或观测区，各自空闲表不得互借。segment 在区锁外初始化，空指针到非空指针只允许一次 CAS；败者释放从未发布的 backing，所有记录从已发布槽取地址。回收位置只归还原区。观测耗尽先做有预算追赶，仍失败则报告缺口与故障，释放保留条件后在预算推出的调用次数内恢复。
6. **真实保留条件。** 未释放分配计数（含 R1 小对象）、唯一 slot pin、强句柄任一存在都可保留记录。仅在最后访问完成后 exact reclaim；记录数、残留 tagged/R1 字节与 pending reclaim 分列，不建立普遍的每记录最小字节推论。
7. **角色在控制面冻结。** unknown 为 0；角色名为应用提供的静态字符串，空名、重名、满表与冻结后登记均拒绝。hook 在选择 explicit/ambient 的同一次 TLS 读取中取得数字角色，不分配、不锁表、不回调；跨角色释放回到原 token。角色和区标签有界，不携带 query/account/origin。
8. **先证据后关闭。** retire 先拒绝根账户与控制 owner，再校验 teardown 证据；无效证据不关闭、不封口、不交接。有效证据之后仍按原协议关闭与预检活动 scope/外部责任，terminal 或 timeout 不能替代实际退出。
9. **分列诊断。** 按角色给出未归属 tagged 字节；按区给出容量、active、residual retained、pending reclaim 及残留 tagged/R1 字节。每字段独立采样，允许 signed 样本；各角色之和形成未归属总量，不与资金承诺、RSS 或 allocator 驻留相加。

## 接受的妥协（诚实记录）

- 固定平分牺牲空闲区的借用能力，换取观测不会挤占资金路径；资金自身的历史残留仍可能占满资金区。本条不替应用解决 sponsor 转移或缓存保留。
- 每个覆盖 owner 消耗 16 条记录，短生命周期控制面需要预算修剪；释放条件满足后才可恢复，不保证新请求立刻获得记录。
- 角色表增加永生记录与有界控制状态，角色由应用显式登记/冻结；机制交付不意味着生产线程或 query/R1 已接线。
- 遮蔽保持来源诚实，但这些工作的来源只能按角色标为未归属。环境小对象仍无来源事实；覆盖缺失不能被解释成零内存。
- 分片的争用收益尚未测量；线程选片测试只证明记录选择分散，不能代替性能证据。
- 有界 Loom 与 Tree Borrows Miri分别支持并发协议和地址/别名复核，不构成所有调度或 Rust 最终别名模型的证明。

## 何时重新评估

- 实际长运行显示区域耗尽、追赶成本或 retained backing 成为可用性问题：在保持资金隔离与 token 存活证明的前提下重评估分区比例或存储结构。
- 账户分片不能满足诊断粒度，或应用需要每任务/driver owner：重新评估组数量、保留责任与容量需求，不能只扩大创建调用面。
- 获得可重复的争用与空间测量：重新评估组大小和选片方式；当前不预填收益。
- 应用开始安装生产作用域与角色：分别验证资金回落组合、线程启动标记、真实退出与 residual 交接，不能从本条机制推出生产覆盖。
- Rust 别名规则或 allocator 接口改变：按 ADR-0167 重新评估尾部访问与固定地址 Drop 的证明。
