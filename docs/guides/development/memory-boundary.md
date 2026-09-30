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

正常 funded 步只结算本域事实。量化 refill、新承诺、真实责任归还和债务变更才进入账户路径。`ScopeLease` 是当前线程的独占发布能力，其 stock 来自同一域，stock miss 不等于域越界。lease 必须在 await 或迁移线程前退出；free 可以来自其他线程。hook 只发布成功分配或真实释放事实、更新局部阈值信号，处置在安全边界进行。

`ExplicitGrant` 为可知的大分配提供操作前授权，成功分配不得超过剩余额度。`ExternalBound` 表示尚未能表达为 L 的责任上界，转换成功分配时同步扣 O、加 L；它不建立第二份承诺。`HolderPin` 保活稳定记录并提供曝光样本，不因为增加 holder 或 clone 而再次计 payload。

## 稳定 owner 与退役

allocation origin 指向地址稳定的小记录，保存不可变来源身份。当前责任归属可以变化，free 始终更新同一记录。raw origin 的使用要求配对：每次成功发布对应一次、长度匹配的真实 free；复制 origin 不创造新分配，也不能在匹配 free 之后再次访问。

账户关闭先封口新增生产。真实 teardown 要求任务退出、算子销毁、自有 I/O 实际退出，且不存在活动发布 scope 或未交接的外部责任。timeout、Task terminal 或一次 L=0 快照均不能替代这些证据。只有完成上述过程，才能把被动残留交给同分支存活祖先并让执行账户退役；祖先接收与关闭使用同一生命周期门。

残留保留 payload 和稳定记录 metadata 的容量责任。移交沿用已有记录及 backing，不新申请 owner 位置或容量；共同祖先 C、query 来源压力 U 和不可逐出责任 N 不因重分类而降低。只有真实 free 或无人可兑现的授权归还才减少责任。

`max_active_owners` 限制并发活动域，历史 residual 不占活动名额。稳定记录按 metadata 字节预算与实际存储能力准入；记录回收之后仍保留的公共索引 backing 继续归存储 owner 计费。free hook 不销毁记录，hook 外维护在 live、发布及访问责任归零后回收。一般业务改变 sponsor 时仍须在目标作用域复制，退役例外不提供任意零复制迁账能力。

## 控制容量与结清

控制 floor 在开放 work 之前真实承诺，计入根 C，并只由控制分支使用。普通 work 不借用 floor；主动 drain 和容量下调不能撤销它。唯一、不可复制的 `CapacityWriter` 更新总容量目标，0 是合法目标，floor 之外的新增暴露受弹性容量限制。

容量下调、共享不足候选和显式本地回收请求合并到有界维护轮次。维护消费已发布的 free，归还可撤 idle/slack，保留活动 scope 的兑现权和 floor。扫描预算耗尽返回未完成覆盖；只有完成结清并在当前版本复查请求后，共享不足才能携带可供上层消费的收据。metadata 耗尽、自身硬限、不可能请求和关闭错误保持各自类型。

核心输出事实、拒绝原因及版本；查询准入顺序、终止受害者选择、应用 I/O 的退出上界和处置策略属于对应应用 owner。核心不创建通用 wait/reclaim 仲裁框架或后台线程。

## 观察与后续接线

现有 `observe` 模块继续提供 `CountingAllocator`、allocator snapshot、物理来源质量与覆盖描述。allocator/RSS 观察和责任账本是不同口径，不能直接相加；现有 authority 装配、query 上限与管理指标消费者继续使用中立接口。

M02a 交付分层原语和当前消费者收敛。生产 allocation header/TLS、realloc、size class 与全局 allocator 的归属接线由 M02b 交付；driver 安全边界及 query 内存强制由后续子任务接入。核心测试通过不等于全部 RSS 已受约束，也不证明下游 I/O 的真实退出上界。

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
