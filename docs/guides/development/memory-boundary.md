# Memory 核心边界

`novarocks/memory`（`novarocks-memory`）承载不依赖执行载体的内存责任、容量与生命周期事实。核心的 normal dependency closure 为空。Arrow、SQL、query context、调度器与 async runtime 均由各自应用或适配 owner 持有。

## 分层责任与授权

账户组成同一进程内的责任树。账户保存已承诺容量、独占 slack、政策上限与增长状态；`FundingDomain` 是能够独立兑现一份授权的局部域。并发 driver 的可兑现余额分别保留，不能用一个域的未用授权抵消另一个域的债务。

域内的授权 A、真实存活分配 L、外部剩余上界 O 派生出 E、F、C：

```text
E = max(L + O - A, 0)
F = max(A - L - O, 0)
C = A + E = L + O + F
```

正常 funded 步只结算本域事实。量化 refill、新承诺、真实责任归还和债务变更才进入账户路径。资金 `ScopeLease` 是当前线程的独占 stock writer，其 stock 来自同一域，stock miss 不等于域越界。lease 必须在 await 或迁移线程前退出；free 可以来自其他线程。allocator hook 只发布成功分配或真实释放事实，处置在安全边界进行。独立的观测 lane 与 `LaneHandle::run` 不取得资金资格，也不消费 stock；资金结算读取同一份事实，不能再发布一份。

`ExplicitGrant` 为可知的大分配提供操作前授权，成功分配不得超过剩余额度。`ExternalBound` 表示尚未能表达为 L 的责任上界，转换成功分配时同步扣 O、加 L；它不建立第二份承诺。`HolderPin` 保活稳定记录并提供曝光样本，不因为增加 holder 或 clone 而再次计 payload。

## 稳定 owner 与退役

生产来源以 8 B `RecordRef`（u32 下标 + u32 代次）标识；`FactToken` 也只有这份身份，不含 query/账户指针。稳定 `LaneRecord` 为 64 B，事实、来源、生产状态与当前责任分类分开。来源不变，责任可以在控制面交接。generation 只验证身份；访问必须由强 owner、真实未释放 allocation count 或唯一槽 pin 保活。复制 token 不增加义务，最终释放只做一次，最后减量之后不得再访问。手工 `FactToken` 发布不得与同一物理块的 wrapper/R1 helper 重复发布。

`stop_producing` 封口并结清闲置授权，不改责任分类。仍执行中的 Work 继续承担 Query；子 Task 退出交给同一仍执行的 Work 时也继续属于 Query。真正 Work teardown 才把责任交给同分支存活祖先并转为 Residual。teardown 要求任务退出、算子销毁、自有 I/O 实际退出，且没有活动 scope 或未交接外部责任；timeout、Task terminal 或一次 L=0 不能替代证据。

U=ΣC_query，只统计当前执行责任；`residual_query_committed` 只诊断来源。handoff 的重分类部分保持共同祖先/root C 与不可逐出责任 N，U 可以下降。归还闲置授权另行降低 C；真实 free 才降低残留 payload。换 sponsor 仍须在目标作用域复制，本机制不授予任意零复制迁账。

直接成员节点在账户事务外预分配，关闭/退役访问目标子树的成员，收集期间持祖先读 gate。`max_active_owners` 限制资金活动域，历史 residual 不占活动名额。记录存放在 process-lifetime、System-backed 稳定 segments；生产容量 2^18，16 个 immortal unattributed 分片，代次耗尽隔离不回绕。hook 不销毁记录，有预算控制面维护在 draining、count/pin/强句柄责任归零后 exact reclaim。metadata 责任仅在 exact reclaim 完成后退回；retained segment backing 另报。S1 纯观测控制 metadata 是诊断估计，不加入资金 C，也不代表物理驻留。

## 控制容量与结清

控制 floor 在开放 work 之前真实承诺，计入根 C，并只由控制分支使用。普通 work 不借用 floor；主动 drain 和容量下调不能撤销它。唯一、不可复制的 `CapacityWriter` 更新总容量目标，0 是合法目标，floor 之外的新增暴露受弹性容量限制。

容量下调、共享不足候选和显式本地回收请求合并到有界维护轮次。维护消费已发布的 free，归还可撤 idle/slack，保留活动 scope 的兑现权和 floor。扫描预算耗尽返回未完成覆盖；只有完成结清并在当前版本复查请求后，共享不足才能携带可供上层消费的收据。metadata 耗尽、自身硬限、不可能请求和关闭错误保持各自类型。

核心输出事实、拒绝原因及版本；查询准入顺序、终止受害者选择、应用 I/O 的退出上界和处置策略属于对应应用 owner。核心不创建通用 wait/reclaim 仲裁框架或后台线程。

## 观察与后续接线

Server 的 `GLOBAL` 已安装 `AttributingAllocator<Jemalloc>`；`--no-default-features` 使用同一协议的 `AttributingAllocator<System>` 诊断构建。`CountingAllocator` 保留为进程计数对照，`AllocatorSnapshot` 原字段继续有效。选择在链接期固定，jemalloc 配置、RSS/cgroup 和读数质量由 [ADR-0163](../../adr/ADR-0163-jemalloc-process-allocator-and-cgroup-memory-bound.md) 保持独立。

请求 <512 B 只计进程 small 段，不读 TLS；≥512 B 保持原对齐，在用户请求尾部增加 8 B token，tagged 请求事实含 token。底层 allocate/free/realloc 使用匹配的原始或扩展 Layout；失败保留旧块与事实。普通小对象跨入 tagged 时选择 explicit > ambient > unattributed，tagged 内部 resize 保留原来源。

TLS 是无需析构的 const `Cell`，线程只有一个 Q=1 MiB 槽。切槽、同步作用域退出、没有 outer ambient 的显式 helper 退出及达到 Q 时直接发布或 flush。hook 返回后余额 <Q；在途的一笔操作没有先验大小上界。`Q × sampled pins` 排除了在途项，又是独立采样，不能当作瞬时物理峰值上界或结清证明。字节/序号先发布，count/unpin 合成最后一次状态更新。

`LaneHandle::run` 只暴露同步闭包，私有同线程 guard 在正常返回及 unwind flush 并恢复 outer；同 lane 可嵌套/并发观察，资金 writer 仍独占。`AttributedFuture` 持有同一 lane，在每次 poll 安装，Pending/Ready/unwind 都恢复；Drop 没有未退出的 poll 绑定，spawn 不隐式继承。不能用跨 await 的公开 TLS guard。

`ExplicitOwner` 持有 R1 容器 lane。成功 small 分配后补一份事实，tagged 只由 wrapper 发布；grow/grow_zeroed/shrink 底层不能递归调用同一 helper。失败不变，跨阈值的强 owner 保活整个两段发布，allocator-api 零尺寸没有物理块、不发布。对同一份 L 另取 A 是授权，不是第二份分配。

进程 small/tagged 计数、三类 tagged/R1 small、unattributed、signed 盲区和对账、metadata 估计、批量估计分列。RSS/cgroup/jemalloc 与请求/责任事实不能相加。S1 尚未把生产 query/R1 调用面接线，零 query lane 指标不能证明查询已被硬治理；driver 安全点与 query 强制属于后续工作。

完整合同见 [ADR-0167](../../adr/ADR-0167-size-banded-allocation-attribution.md)，验证入口见 [memory tests](../../../novarocks/memory/tests/README.md)，成本入口见 [attribution harness](../../../tools/memory-attribution-bench/README.md)。本地 smoke、模型完成和文档检查均不代替 Miri 地址验证或用户的 Linux 成本结论。

## CI 依赖门

`tools/ci/check-memory-dependency-boundary.py` 同时读取 Cargo resolved graph 和 declared dependency edges，后者包含默认 feature 未启用的 optional 边。检查三项能力约束：

- `novarocks-memory` 的 normal closure 和 normal 声明表均为空，包括 optional dependency。
- 核心 dev closure 不含 `novarocks-*`、`arrow*` 或 `tokio`。从 dev 根边进入后继续检查 normal 传递闭包，包含 helper 的未启用 optional 边；中立测试工具可以进入 dev closure。
- byte-oriented `novarocks-state-store-api` 的 resolved 与 declared normal closure 均不包含内存核心。

build dependency closure 单列报告，当前不作为上述 dev 门强制。图检查不能判断一个只用算术实现的策略是否属于核心，也不代替生命周期、并发和地址安全验证。

检查遵循 [ADR-0058](../../adr/ADR-0058-crate-boundaries-enforce-isolation-not-source-shape-guards.md) 的能力边界原则，不添加旧符号缺席检查，不冻结源文件名称与目录形状。M02a 删除未使用的共享 Reservation 和 memory-arrow 谱系路径后，依赖门围绕当前核心与 StateStore 合同执行，不保留旧 adapter 可选规则。

```bash
python3 tools/ci/check-memory-dependency-boundary.py --manifest-path Cargo.toml
tools/ci/tests/memory-dependency-boundary-test.sh
```

mutation 测试在临时工作区构造合法图及 direct/transitive/optional 违规图，覆盖 normal 零依赖、dev 能力隔离、StateStore 无内存依赖及 build-only 报告。夹具只引用本地 path package，且每次夹具检查强制 Cargo offline。两个入口继续由 `tools/ci/local-full-ci.sh` 的对应阶段执行。
