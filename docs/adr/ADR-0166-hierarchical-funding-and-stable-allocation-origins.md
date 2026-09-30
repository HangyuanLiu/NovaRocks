---
id: ADR-0166
title: "Independent funding domains and stable allocation origins replace retention lineage"
domain: [memory-governance]
status: active
supersedes: [ADR-0160]
superseded-by: null
date: 2026-09-30
provenance:
  - "discussion: 2026-09-29 independent funding, local stock and stable-origin retirement"
  - "approval: 2026-09-30 hierarchical funding implementation and manual Linux acceptance"
code-anchors:
  - "novarocks/memory/src/account.rs (Path, grow_locked, AccountHandle)"
  - "novarocks/memory/src/domain.rs (FundingDomain, split_free)"
  - "novarocks/memory/src/owner.rs (OwnerRecord, AllocationOrigin)"
  - "novarocks/memory/src/lifecycle.rs (retire, transfer_residual)"
  - "novarocks/memory/src/maintenance.rs (maintain, shortage_is_fresh)"
---

## 问题

进程容量如何同时支持局部高频分配、独立可兑现授权、动态目标，以及执行账户退出后仍存活的分配责任？

## 背景与执行事实

| 实体 | 身份与责任 | 承重入口 |
|---|---|---|
| Account | 严格父链中的容量约束、独占 slack 与子域承诺；不是 allocation header 的寿命 owner | `AccountHandle`、`Path`、`grow_locked` |
| FundingDomain | 独立的 A/L/O/E/F/C；不同域不得以净额抵消可兑现权 | `FundingDomain::split_free`、`settle` |
| ScopeLease | 唯一活动发布者与本地 stock；不可跨线程移动或跨 await 持有 | `activate`、`record_allocation`、`finish` |
| OwnerRecord | 稳定地址、不可变来源身份、live/访问计数；当前责任归属可在控制面转交 | `AllocationOrigin::record_deallocation`、`transfer_residual` |
| CapacityWriter / maintenance | 唯一容量目标版本与有界结清；不撤销活动兑现权 | `set_capacity`、`maintain`、`shortage_is_fresh` |

配置目标不是历史空闲余额。实际 C 包含未覆盖成功分配以及真实核心存储；归还 E 不能产生新的 A。每步 shared-leaf 事务会把工作频率变成跨线程竞争，完整账户强引用又使执行退出依赖最后一个分配的释放。ADR-0148 的单进程权威与观察责任保留，旧余额及通用仲裁框架由本条收敛；ADR-0160 的留存谱系整体替换。

## 考虑过的选项

- **每步进程或共享叶子事务：设计否决。** 容易获得一个统一快照，但让正常 funded 步强制竞争公共账本，违背局部执行责任与并行扩展要求。债务和授权控制仍必须具有显式慢路径。
- **完整账户保活到最后 free：设计否决。** 强引用能保地址，却把执行账户退出与物理释放绑在一起；不能满足真实 teardown 后独立保留残留责任的生命周期合同。
- **稳定小记录与分层量化控制：采用。** 正常步骤局部化，裸来源地址在发布/访问责任清零前保持稳定，退役转交同一记录。代价是记录实际字节持续收费，以及维护端承担最终归并和回收。
- **立即复用固定 slot/generation：待评估。** generation 可区分身份，但尚不足以证明多代迟到 free 的目标路由、转交发布窗口、回绕和最后访问安全。没有完整协议与地址证明前，不作为稳定记录的等价实现。

## 裁决

1. **目标与责任独立。** 进程目标由唯一、不可复制的 CapacityWriter 更新，范围为零到配置上界。每个独立授权域维护 A、L、O、E=max(L+O−A,0)、F=max(A−L−O,0)、C=max(A,L+O)。另一域的 F 不能抵消本域的 E。真实事实始终接收，下一步授权可以拒绝。
2. **正常步骤留在局部。** 长期 lane 激活为不可跨线程移动的 ScopeLease；stock 只来自该域已取得的工作集。成功分配先发布稳定 owner，再允许对象逃逸。hook 不锁账本、不访问账户、不分配、不调用回调。funded attach/settle/detach 不访问父/root；可复用工作集在正常 detach 后保留，显式维护才撤空闲权利。
3. **控制事务沿严格父链提交。** 先按 child→parent 取得增长资格门，再取本地域与账本锁。补额的余额修改在足够的父级 slack 停止，但所有祖先的 freeze 资格仍有效。shrink、政策切换、债务结算、关闭以排他门握手。失败不留下部分承诺。split_free 只移动尚未兑现的权利，并在同一提交建立目标域；已有 L/O 不通过猜测迁移。
4. **floor 是已支付的独立控制池。** 装配在 work 前预付容量，只允许指定控制分支使用；目标零不撤销该池，也不撤销原域旧 F。普通账户不能借 floor。E 的真实归还降低额外 C；只有已支付的闲置权利能回到控制池。
5. **来源与当前责任分开。** AllocationOrigin 只含稳定记录地址与不可变来源身份。一次 free 在底层存储释放后，最终减少访问计数，之后不再访问记录。teardown 还需应用任务/算子/自有 I/O 真实退出、core 无活动 scope/O；超时或 terminal 不冒充退出。残留沿同分支转交存活祖先，保留原记录、来源类别及真实 E；共同祖先的 payload/记录 metadata 责任不变。
6. **活动名额与物理存储分开。** 残留不占 max_active_owners。OwnerRecord 及 Arc 控制块的实际字节随域唯一计费；索引 backing 由公共 storage owner 计费。Shared/root Account 和每个实际 Account Arc box 同样属于公共 accounting storage，退休释放活动槽而不释放仍存活句柄的物理费用。只有实际销毁才能归还该费用；历史句柄数量仍受根字节容量限制。
7. **不足前先有界结清。** 三类请求合并到维护 epoch，以预算和固定游标驱动；活动域在安全边界响应 drain，未完成只报 Pending。SharedShortage 携带请求/实际约束、原请求量/所需增量、账本/政策/容量版本、成员截止范围、epoch、完成度、deferred active 和时间。新 free、成员或版本变化使收据失效，控制消费者须调用 freshness 检查并在自己的提交边界重查；core 不选 victim。
8. **分类与样本分列。** 管理端一次捕获 root 和 pressure，最多三次重试；最后 pin 在根门外释放。已结算 L/E 与 hook 的独立样本、dirty、已结算 inactive 授权、pending drain、公共 metadata、floor/elastic、residual query 来源分别报告。分类不完整时，policy-facing query pressure 返回 None。root 已包含 residual，handoff 不降低 U/N。

## 删除与保留

删除未使用的 Reservation、Charge、wait/reclaim/pressure 框架、memory-arrow 及其旧模型/基准，不保留兼容钱包。ExplicitGrant、ExternalBound 与 HolderPin 按独立域语义重建。核心 normal 依赖为空；dev 依赖不引入 first-party/Arrow/Tokio，StateStore 不闭包包含内存能力。

保留 ADR-0148 的唯一进程权威、两级覆盖和观察事实，替换其旧余额/Arrow charge/通用仲裁框架。ADR-0160 的谱系与叶子实现整体退出。ADR-0163 的 jemalloc、CountingAllocator、cgroup 和物理观察保持独立。当前 Worker/Native 只消费账户与 Work policy，实际业务分配接线仍由 M02b/M03 交付，不能据本条声称全查询已经硬治理。

## 验证与限制

行为测试覆盖独立域、split、债务、floor、动态目标、封口、退出 deadline、残留、metadata、结清后不足与管理分类。Loom 使用真实协议状态，限定有限操作数、最多三个参与者与两次抢占，不设置成功的时间/排列截断；地址生命周期另以 Miri 验证。并发模型覆盖最后 free 与记录回收、protected debt 归还和 drain/激活握手；模型不能代替裸地址及底层 allocator 的安全证明。

性能入口保存固定 manifest、全部解析输入、真实 storage/工作集、原始 iteration 样本、父/root 余额交互以及实际资格门/账本等待和持有统计。锁等待/持有尾部使用有界对数直方图，报告桶上界与 overflow，不能冒充精确逐次样本。原生 Linux 独占性能验收由用户后续手动执行；macOS correctness smoke 不代表成本门通过。

本条定义中立 teardown deadline 输入，不证明各生产 I/O owner 已交付退出上界。M02b/M03 接入安全点与 allocator，M08b 独立驱动回收，M10/M11 消费完整分类/新鲜收据并验证有界政策推进。

## 接受的妥协（诚实记录）

- 稳定记录以实际字节预算换取可审查的地址与归属寿命，不承诺 metadata 只随当前并发 owner 数增长。历史未释放对象会继续占用 payload 和记录预算，真实字节短缺仍可拒绝新工作。
- 成功分配事实不可回滚；未覆盖 E 先进入责任账本，下一步独立拒绝。控制面及观测需区分已结算事实、hook 样本与不完整覆盖，不提供瞬时全树一致 L 快照。
- 祖先资格门、债务和生命周期事务保留慢路径成本。正常局部化已有行为证据，Linux 尾延迟和吞吐门尚待手动采集，不以 macOS smoke 或软件计数器代替硬件成本。
- 原始地址采用 unsafe 线性 free 合同，process authority 必须覆盖发布者与 outstanding origin。当前没有覆盖全部业务分配的新 allocator，也没有证明各生产 I/O owner 的退出上界；这些由对应 owner 接入验证。

## 何时重新评估

- Linux 冻结成本门失败，或增长资格检查成为真实瓶颈：依据逐层频率、等待/持有数据重新裁决，不修改通过阈值。
- 下游无法给出自有 I/O 的实际退出上界或可靠安全点：先修正 owner/责任交接设计，不把 timeout 当 teardown。
- 需要跨 sponsor 移动活跃 L/O：先定义精确 owner/bound 集合与事务合同，不扩展 free-only split 为隐式迁移。
- 正常存活进程的原始 allocation origin 必須由 process authority 覆盖其完整寿命；若新增比进程 owner 更长的分配寿命，需独立稳定存储退出协议。authority shutdown 只断开无发布或 raw 访问权的空记录，保留活动 scope、ExternalBound（含零字节）和未释放记录；process authority 须覆盖这些能力的完整寿命。

- 固定 slot/generation 若取得多代路由、原子移交/回绕、发布者与最后访问回收的完整模型和真实地址证明，再评估其 metadata 收益；不得仅凭身份 CAS 替换存活证明。
