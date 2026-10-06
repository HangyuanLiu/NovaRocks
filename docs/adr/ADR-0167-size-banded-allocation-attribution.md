---
id: ADR-0167
title: "Size-banded allocation attribution separates origin, liability and funding"
domain: [memory-governance]
status: active
supersedes: [ADR-0166]
partially-supersedes: [ADR-0148, ADR-0163]
superseded-by: null
date: 2026-10-03
provenance:
  - "discussion: 2026-09-30 allocation attribution, observation boundaries and lifetime publication"
  - "approval: 2026-10-03 stable lane storage, safe scope interfaces and process observation implementation"
  - "decision: 2026-10-06 Miri aliasing model for in-band allocator tail reads (Tree Borrows)"
code-anchors:
  - "novarocks/memory/src/lane/record.rs (LaneRecord, LifetimeState)"
  - "novarocks/memory/src/lane/slot.rs (SlotCore)"
  - "novarocks/memory/src/lane/store.rs (RecordStore)"
  - "novarocks/memory/src/attribution/hook.rs (AttributingAllocator)"
  - "novarocks/memory/src/attribution/explicit.rs (ExplicitOwner)"
  - "novarocks/memory/src/lifecycle.rs (retire, stop_producing)"
  - "novarocks-server/src/memory_observation.rs (GLOBAL)"
---

## 问题

进程如何在可并行、跨线程和跨执行账户退出的 Rust 分配上，分别表达不可变来源、当前责任、真实物理释放与可恢复容量授权？

## 背景与执行事实

| 实体 | 身份与责任 | 入口 |
|---|---|---|
| Account | 严格父链上的容量政策、独占 slack、子域承诺及直接成员关系 | `AccountHandle`, `Membership`, `Path` |
| LaneRecord | 地址稳定的 64 B 原子事实；来源不变，分类由实际责任交接改变 | `LaneRecord`, `RecordRef` |
| RecordRef | 尾部 8 B 的下标 u32 + 代次 u32，不含账户/query 指针；身份不是存活能力 | `RecordRef::read/write`；`FactToken` 也只包装该身份 |
| LaneHandle | hook 外的记录访问责任与当前 affiliation；不自动取得授权 | `AccountHandle::create_lane` |
| FundingDomain | 组合 lane 与独立资金状态 A/L/O/E/F/C | `activate`, `settle`, `split_free` |
| TLS 槽 | 一个可复制到 Cell 的控制状态；已绑定槽唯一持有 pin，保存带符号增减 | `SlotCore::add/flush` |
| ExplicitOwner | R1 容器保留的 lane 句柄，选择分配来源但不授予容量 | `allocate_with/deallocate_with/resize_with` |

Rust 普通不可失败分配不能通过 SQL 错误恢复。因此 hook 只观察事实；授权与处置留在粗粒度安全边界。全局 allocator 在链接时固定，Server 默认 `AttributingAllocator<Jemalloc>`；无 jemalloc 构建为同协议的 `System` 诊断基线。jemalloc 配置、cgroup anon、RSS 与 allocator allocated/active/resident 继续遵循 ADR-0163 的独立质量与读数，Unknown 不写成零。

本条整体接替 ADR-0166，保留其分层授权、控制 floor、动态目标和有界结清规则，替换裸来源地址、账户退出与 residual 分类规则。ADR-0148 的两级覆盖与唯一容量权威、ADR-0163 的物理 allocator/cgroup 规则仍有效；这两条中“CountingAllocator 只计数/无 TLS 归属”的前提由本条替换，旧正文作为历史保留。

## 考虑过的选项

- **分配 header 保留 query/账户 Arc：设计否决。** 保活容易，但每个分配增加完整对象的引用流量，并把执行 teardown 绑在最后物理释放上。
- **只依据 TLS 当前线程扣减：设计否决。** 远端 free、future 迁移和共享容器不能由释放线程推导来源；来源必须随分配传播。
- **指针或 generation 单独保证安全：设计否决。** 身份检查不能阻止检查后回收，字节恰为零也不证明没有未发布分配；必须有独立的真实引用与 slot pin。
- **每次 hook 锁公共账本或动态映射：设计否决。** 引入再入、等待、metadata 分配及高频公共争用，且混合观察与容量政策。
- **所有小对象都加 header：成本否决。** 小对象可获得更完整来源，但固定 token 的尺寸放大和 hook 路径成本不满足当前分段目标；环境小对象保留进程计数，R1 显式发布可知的小事实。此成本取舍尚待正式 Linux 结果确认。
- **按 allocator usable size 充当请求事实：设计否决。** 请求布局、size class 放大与驻留页是不同事实；可把 usable size 用于独立成本测量，不能替换精确原 Layout。
- **以 exposed provenance 读取尾部，使 Stacked Borrows 通过：合同与成本否决。** 分配时暴露基址、释放与 resize 时按地址恢复，必须放弃 strict provenance，并让每个带来源分配在热路径上额外暴露一次指针。
- **以带外索引代替尾部读取：暂不采用。** 它是将来可选的实现优化，只在尾部税实测过高时考虑；引入它会改变尾部来源格式，需回到设计讨论。
- **稳定分段记录 + 线性分配义务 + 单槽批量：采用。** 控制面维持有界记录与成员；hook 仅用 token/TLS 与原子发布。

## 裁决

1. **观察与授权分离。** 观测 lane 创建不 qualify、不消费 stock、不增加 S1 的资金承诺；账户关闭或记录耗尽返回覆盖错误，`run` 记绑定失败后仍执行原闭包。不得制造 SQL 拒绝。FundingDomain 的正常 funded 步留在本地；补额、债务与交接沿严格父链提交。每个独立域保持 E=max(L+O−A,0)、F=max(A−L−O,0)、C=max(A,L+O)，其他域的 F 不能抵销本域 E。成功事实始终接受，下一步授权可以拒绝。`split_free` 只移动未兑现权利；O→L 不重计。
2. **来源按请求分段。** 用户请求 <512 B 只走进程计数，不读 TLS；≥512 B 保留原对齐并在尾部增加 8 B token，归属事实包括 token。free 使用分配时来源；带来源段内部 realloc 保留来源，小→带来源取 explicit/ambient/unattributed，带来源→小撤出 token 事实。失败保留原块、内容、token 与全部事实。底层每次使用精确原请求对应的 Layout：小段原 Layout，带来源段增加 8 B。GlobalAlloc 的零尺寸行为仍受其原调用合同约束；R1 allocator-api 的零尺寸另按规则 6 处理。
3. **存活能力先于身份。** 记录存放在 process-lifetime、System-backed、按需发布的稳定 segments，容量 2^18，16 条 immortal unattributed 分片。代次复用递增，u32 耗尽隔离而不回绕。真正 allocation count 与 slot pin 共用一条原子 lifetime word；计数允许远端负增减先于缓冲正发布。owner、真实未释放分配或唯一槽 pin 保证访问。draining 且 count/pin 为零时，hook 外以 CAS 认领；最后一次状态修改后不再访问记录。exact reclaim 完成才释放域的占用 metadata 责任，segment backing 仍可保留并单列报告。
4. **批量误差不冒充物理上界。** 单线程只持一个槽，Q=1 MiB；切换、作用域退出、显式 helper 无 outer ambient 退出和余额达到 Q 时直接发布或 flush。字节/序号先发布，计数增减与 unpin 合成最后一次原子修改。hook 返回后槽余额 <Q；任意时刻另有一笔在途操作，大小无先验上界。采样 Q×pins 仅为不含在途项的余额估计，不能作为瞬时一致上界或结清证明。
5. **同步作用域有结构化退出。** `LaneHandle::run` 只接同步闭包，没有公开 guard；私有同线程 guard 在正常返回及 unwind 恢复 outer ambient 并 flush。flags 中有界活动计数允许同 lane 嵌套和并发观察步骤，资金 stock writer 仍由资金状态单独互斥。`AttributedFuture` 每个 poll 安装同一 lane，Pending/Ready/unwind 都恢复；线程迁移后重新安装，spawn 不隐式继承。TLS 为 const Cell 且不需要析构，不用线程退出析构补漏。
6. **R1 容器有一份事实。** ExplicitOwner 优先于 ambient，helper 在成功后补小事实，带来源事实由 wrapper 独占发布。grow/grow_zeroed/shrink 的底层操作不得再次调用同一 helper。零尺寸 allocator-api 请求没有物理块，不发布事实。跨阈值转换由强 owner 保活到两侧发布结束，即使计数暂到零也不能回收。手工 `FactToken` 发布不得与同一块的 wrapper/helper 发布并用；资金消费者读取/结算同一 lane 事实，不能重复记账。
7. **停止生产与执行退出分开。** `stop_producing` 封口、结清并归还闲置授权，不改变 class。Work 路径仍在执行时是 Query；子 Task 退出交给仍在执行的 Work 后仍是 Query；真正 Work teardown 后交给同分支祖先才成为 Residual。来源永不改写，U=ΣC_query，不加 residual_query 来源诊断；handoff 自身保持 root C 与 N。任务/算子/自有 I/O 实际退出、无活动 scope 和无未交接 O 才允许退役，terminal/timeout 不能冒充实际退出。残留真实增长只记故障，不拒绝；退出前缓冲、退出后 flush 以及 shrink 分类迁移不误报新增增长。
8. **成员与回收属于控制面。** 直接成员节点在准入锁外预分配，事务内无分配；关闭/退役只收集目标子树，收集期间祖先生命周期读 gate 阻止成员交接遗漏。独立有界 observation registry 保持 token-only lane 的控制交接责任；有预算维护在最后 count/pin/强句柄消失后 exact reclaim。资金域及账户的 metadata 责任、active 名额与 retained backing 分别报告。S1 观测控制 metadata 只作诊断，不加入 C；物理驻留不是这些估计的总和。
9. **容量控制合同保留。** 唯一 CapacityWriter 维护零到配置上界的版本化目标；预付控制 floor 不借给普通 Work，也不因目标缩为零或 drain 被撤销。域内正常 funded attach/settle/detach 不访问父/root；量化 refill 在有足够 slack 处停止余额修改，但所有祖先增长资格仍握手。债务归还不产生新 A。有预算维护只在完成覆盖、精确版本与请求复查后给出 fresh shortage；core 不排队、不选 victim、不替应用证明 I/O 退出上界。
10. **指标如实分列。** 小/带来源请求计数、三类 tagged/R1 small、unattributed、signed blind spot/reconciliation、记录分类/生产状态、固定七类故障和采样时间/序号分别展示，标签不带 query/account/origin。独立样本可以负值；不得 clamp 成“无内存”。物理 RSS/cgroup/jemalloc 来源仍分列，不与责任事实相加。S1 生产 query/R1 接线尚未建立，零 query lane 指标不能证明查询已被硬治理。

## 接受的妥协（诚实记录）

- 尺寸门意味着普通小对象没有查询来源。R1 可补充可知小事实，C/JVM、库内 malloc、allocator 保留与碎片仍是盲区。该覆盖选择不承诺避免全部物理 OOM。
- 带来源请求多 8 B，可跨 size class；请求字节、usable 与 resident 放大须分别测量。小释放增加真实释放事件计数，额外原子成本待正式 Linux 门，不用 smoke 代替结论。
- 静态存储容量、保留 segments、无代次回绕和预算维护以固定资源换审查简单性；耗尽记覆盖错误。其 metadata 含观察控制估计与 retained backing，不冒充瞬时物理账本。
- 有界并发模型验证的是有限操作/抢占下真实协议；它不替代 Miri 的地址验证。Miri 与 Linux 正式成本有独立收敛门，未完成不得声称 unsafe/性能验收完成。
- **unsafe 验证以 Tree Borrows 为别名模型。** Stacked Borrows 把引用派生指针的权限限制在 `size_of::<T>()`，std 的 Box/Arc 把这样收窄的指针交给 `dealloc`。因此只要 allocator 经调用方指针读取请求长度之后的带内元数据，就会被 SB 判为 UB，std 自身的 Box 即可触发，与本条的协议实现无关。Miri 以 Tree Borrows 加 strict provenance 与 symbolic alignment 覆盖全部归属目标；在 SB 下，不经收窄指针的 resize、失败、跨线程释放、TLS 与 scope 路径同样通过。Tree Borrows 比 SB 更新，也更实验性，Rust 最终的别名模型可能更严格，因此这不是最终别名模型下的证明。
- 读数按字段/分片独立采样，不存在全堆一致快照；Q×sampled pins 排除在途项，无法用它证明严格峰值。

## 何时重新评估

- Linux 固定 manifest 成本门未通过或 small/tail 的真实热点放大：按原始数据重评估阈值、分片或存储方法；不能看到结果后修改通过线或只保留有利 workload。
- 小对象来源缺失成为实际治理缺口：评估全 header 的覆盖收益及空间/CPU 成本；它是成本取舍，非永久设计禁区。
- 2^18 容量、high-water 扫描、retained segments 或 observation registry 在真实长运行成为可用性问题：在保持 token 路由、存活证明及退出责任的前提下重评估分段/回收结构。
- 要把观测 metadata、R1、driver 安全点接入硬治理：先明确资金消费者、flush/结算顺序及 metadata capacity owner，不能重复发布或假定本条已交付全部 query 覆盖。
- Rust 采纳的别名模型拒绝 allocator 经 `dealloc`/`realloc` 收到的指针访问同一块内请求长度之外的字节：在保持 token 路由与存活证明的前提下，重评估尾部读取的 provenance 来源。
- 需要跨 sponsor 移动活跃分配或 native C 来源：先定义精确对象集合、原 Layout 与转移协议，不能以改 TLS、改单个 token 或 timeout 冒充交接。
