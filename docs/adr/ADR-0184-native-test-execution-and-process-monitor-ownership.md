---
id: ADR-0184
title: "Native test execution and independent process-monitor ownership"
domain: [test-capabilities, crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0069]
date: 2026-10-19
provenance:
  - "discussion: 2026-10-19 native per-test isolation, independent payload monitoring and bounded diagnostic retention"
  - "partial replacement: ADR-0069 in-process output readers and Drop-based process ownership; the dependency leaf, domain ownership, readiness and port reservation rules remain effective"
code-anchors:
  - "tests/test-support/build.rs (main)"
  - "tests/test-support/src/monitor/client.rs (ManagedCommand, OwnedMonitor)"
  - "tests/test-support/src/monitor/main.rs (guard, payload)"
  - "tests/test-support/src/run_retention.rs (ensure_run_retention, close_registration, collect)"
  - "tests/test-support/src/managed_process.rs (ManagedProcess, ManagedProcessLogSource)"
  - "tools/ci/test-run.py (Executor, cleanup, batch)"
  - ".config/nextest.toml (profiles and native resource groups)"
---

## 问题

测试进程会被取消、超时终止或异常退出时，如何保持子进程和原始日志的准确所有权，并让日常调试与正式执行使用相同的回收能力？

## 背景与执行事实

逐测试进程隔离可以隔离 allocator、panic hook 和进程内缓存，却不能证明测试启动的外部进程已经退出。测试进程自身消失时，Rust Drop 不再是可依赖的回收责任方；nextest slot 释放也不是 fixture 可复用的证据。

| 对象 | 所有权与完成事实 | 不拥有的职责 |
|---|---|---|
| nextest | 发现、筛选、调度及实际 wait 每个原生测试进程 | 测试自行启动的任意服务及外部实例 |
| `ManagedCommand` / `ManagedProcess` | 从构造起请求 monitor、持控制端和日志 source，消费 ready/退出/清理结果 | 不从任意既有 Command 反向提取隐藏配置 |
| payload monitor | 独立 session，真实持有一个 payload Child；组信号、退出观察、最后组信号、实际 wait 后写终态 | 不判断测试断言是否通过，不接管历史 PID |
| retention guard | 观察准确 nextest 调用的内核退出、持材料 pin 并封闭登记 | 不拥有或代 spawn payload，不代理前台 TTY |
| 正式 wrapper | 持有自己创建的原生执行器 Child，保存原始结果、封闭登记并汇总全部监护记录 | 不成为 payload 宿主，不创建另一套测试身份或统一结果枚举 |

本裁决部分替代 ADR-0069 的进程内 reader 和 Drop 所有权实现。共享机械叶子与零产品 normal dependency、领域 oracle/fixture 的归属、child marker readiness、端口 release-to-bind 的真实冲突规则继续有效。ADR-0181 的 workspace/tests/bins/dev 主构建、薄来源边缘和有限授权规则保持不变。

## 考虑过的选项

1. **继续全局串行 libtest。设计否决。** 能减少部分共享状态竞争，但没有逐测试进程隔离，也不解决测试进程死亡后的 Child 回收。
2. **仅采用 nextest，仍在测试进程内依赖 Drop 和输出线程。设计否决。** 原生执行与报告合适，但测试被杀时仍会失去唯一回收者；直接 Cargo、IDE 和 runner 调用也需要相同的机械能力。
3. **独立 monitor 加原生执行器。采用。** 每次受管启动交付一个真实 Child owner；nextest 继续负责自己的测试，普通调用无需常驻服务。代价是额外进程、FD/锁协议和材料生命周期。
4. **统一中央 spawn 宿主及私有测试目录。设计否决。** 会让直接库调用依赖额外运行宿主，并把原生注册、选择和身份复制成第二套权威。外部 fixture 或作业级 containment 可以由各自 owner 另行评估，不改变本库的边界。
5. **暂不限制材料生命周期、永久保存所有普通调用。成本否决。** 能保留更多成功诊断，但日常调用会无限增长；正式证据按其保存合同维护，普通调用只保留有限窗口及尚未收束的例外。

## 裁决

1. **原生执行规则。** 常规 Rust 使用 nextest 的逐测试进程、原生 filter 和资源组；doctest 单独由 Cargo 执行。自定义 harness 归回原生测试，内部 ignored helper 只由其原 owner 精确启动。Loom/Miri、专用 fixture 与暂停矩阵不因迁移扩大选择。并发、watchdog、泄漏检查和失败策略放在 nextest 配置，不维护平行用例目录。
2. **独立 Child 规则。** monitor 在允许 payload 出生前完成隔离和接管；lifeline EOF 与显式停止走同一回收机制。原始 deadline 覆盖交接，ACK 丢失不重试 spawn。最后组信号先于 reap，任何后续动作不能再按已回收 PID 发信号。失联、清理失败和未完成终态不能因最终未发现残留而变成成功。
3. **自动交付规则。** 辅助程序由 test-support 的 build script 使用当前原生 Cargo 编译输入交付到自身 OUT_DIR。库通过编译期路径执行；运行期不调用 Cargo、不查 PATH、不增加产品依赖或来源身份监听。native 之外的交付必须明确拒绝或重新设计。
4. **FD 与记录规则。** lifeline、ACK、记录锁和 gate 原子 CLOEXEC，只向准确目标 dup2 交接；父端关闭继承副本而不主动 unlock 同一打开描述。Pending 到 payload spawn/拒绝受同一登记门保护，锁定的 JSONL inode 不替换。完整 Final 与实际退出是事实，锁空闲不是结论。
5. **日志规则。** payload 直接写 run-owned 双流文件，monitor/guard 不保活 nextest 捕获管道。实时观察按原始文件身份增量读取，完整 UTF-8 行诊断受 1 MiB 边界保护；该边界不截断原始日志、readiness 或完整 golden capture。原文件替换/截短明确失败，时点 snapshot 保留 visitor 原错误及独立完整性错误。成功消费全部所需断言前不释放日志。
6. **调用材料规则。** 普通 nextest 首次受管操作才按原生 run ID 懒建租约；纯测试不触发状态目录或平台前置。首次创建核实直接 nextest 父关系和内核退出观察；嵌套 runner 由外层先 ensure，再继承同一 Ready 租约。guard 只管理材料。没有 nextest 上下文的调用按调用进程归属，不伪造原生身份。
7. **封闭与保留规则。** 正式 wrapper 与普通 guard 都先封闭登记；Closed 后不得创建新 payload。普通调用只有在封闭、相关 pin 释放且监护记录完整后才进入有限调用窗口；活动、未知或未完成材料不被淘汰。正式材料不参加普通 release/GC，汇总不得改写原始 nextest/JUnit/Cargo 结果。
8. **前台与批处理规则。** 日常准备后前台 exec nextest，保留原生 TTY 和作业控制；正式 wrapper 单独转交准确 Child 的信号、实时展示原始文件并执行有界汇总。取消保持取消，普通断言失败不能遮住独立 doctest，回收违约也不能被测试退出 0 抵销。

## 接受的妥协（诚实记录）

每个 payload 增加一个 monitor，普通 nextest 调用首次使用还增加一个材料 guard；这带来启动、FD 与维护成本，不承诺提速。原生全图准备仍有真实编译/链接成本，资源组只约束测试调度，不代表进程树数或物理内存上界。

活跃控制目录依赖本地文件系统的锁、身份与 rename 语义。实现按实际目录 FD 检查文件系统，容器需显式挂载受支持本地目录；网络、overlay、未知文件系统及未验证 ABI 不通过猜测降级。跨平台正确性需要对应 native 运行，交叉编译或另一架构的探针不能替代。

普通诊断窗口按调用数量管理，不是磁盘配额；同次调用的多个失败、活动 pin 或未完成回收可以超出窗口。正常结果被消费并释放后，也不保证还能恢复更晚的任意断言所需日志；需要正式或长期引用时先交接保留。

该所有权只覆盖明确交付的 payload/进程组。monitor 被 SIGKILL、OOM、掉电、主动逃离组的后代、容器与外部 fixture 仍有平台或原 owner 边界。Linux 直接 child 的父死亡保护不证明完整进程树退出。没有正式 wrapper 的直接调用获得库级回收，不自动获得阶段级完整汇总。

## 何时重新评估

- 增加操作系统、架构、容器运行形态或文件系统时，先验证实际 ABI、FD/锁交接、准确退出观察和材料封闭；若这些事实不能成立，重新裁决支持范围。
- 若额外 monitor 的实际成本影响合法日常工作，评估同等失联语义与直接调用能力下的替代机制；不能仅为降低进程数退回 Drop-only。
- 若需要普通成功日志的更长可恢复窗口，或调用数量窗口不能满足磁盘管理，重新评估保留策略及 owner；不自动删除未完成或正式材料。
- 若 nextest 的父子关系、原生身份、信号或报告接口改变，验证最低版本及正式固定版本适配；不得以自造字段或 fallback 掩盖不兼容。
- 若需要跨实例 fixture 独占或作业级 containment，由 fixture/平台 owner 独立裁决；不把它们附会为 monitor 已提供的能力。
