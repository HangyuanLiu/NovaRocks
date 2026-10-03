# P04 原授 HTTP/2 stream Task 子切片

本收据记录 parent `79c1d417fd2dd43907021c8f8e90e78f48e35564` 上候选源码的实际 Hyper H2Stream Task 所有权验证。实际 allocator 五项、最终十一 protocol targets 77 项与 Native Adapter lib 673 项已通过；新 full CI、七个 Native System 场景、最终 source pins 和日志发布仍为 **PENDING**。上一 wave 的 CI 11947 不作为本轮证据。

## 实际能力与退出链

`NativeTaskExecutor` 接收调用方事前授予的原连接能力，不创建容量权威。固定 pool 请求上界包含实际 Core/Arc Layout、position Vec、每个 prepared Bytes carrier，以及实际 Tokio Cell/按普通 spawn 策略自动产生的 Future Box。Hyper 用实际 private H2Stream 类型查询 bound，在 body adaptation 和 `service.call` 前调用 prepare；位置不足先拒绝请求。

pool 不保存 Task、Channel 或 Weak。其私有强 Arc 最后通过 `Arc::into_inner` 先退出实际 Arc heap，再析构 Core 的 position Vec，最后退出原 owner。prepared carrier 通过 Bytes physical-exit guard 保留同一 pool；TaskCell 与自动 Future Box 退出后，最后 carrier wrapper 真正 deallocate 才让位置回到 FREE。仍存活的 Waker 或 prepared alias 会保留原位置；Future completion 不是物理回收。

只读 review 发现两项可复现漏洞，均已修复并纳入实际测试：prepared executor clone 可在一个位置上重复提交 Cell；以较小 Future prepare 后可通过另一 generic Future 类型绕过 bound。修复使用 FREE/PREPARED/EXECUTED CAS 保证单次提交，并在 execute 前重新核对实际当前 F 的 bound。拒绝不能转到 ordinary spawn，不能提前复用位置。

默认 None/ordinary 路径保持既有行为。funded Native 的 CONNECT 在 preparation 和 service 前明确拒绝；实际对照测试确认普通默认路径仍 dispatch CONNECT。这不是通用 Task 模型或零分配声明。

## 已完成的实际验证

| 项目 | 结果 | 原始日志 |
| --- | --- | --- |
| 实际 H2Stream/System allocator target | 初轮 5/5；最终 protocol 集合再次包含同五项通过 | `/tmp/m07-stream-task-first-tests.log` |
| 最终十一 protocol targets | 77/77，0 failed | `/tmp/m07-stream-task-final-protocol.log` |
| Native Adapter lib | 673/673，0 failed | `/tmp/m07-stream-task-libs.log` |
| std actual geometry 单例 | 1/1，已包含在上述 lib，不重复计数 | `/tmp/m07-stream-task-final-geometry.log` |
| 六个实际源码反例 | 全部编译成功、指定 runtime FAILED、exit 101、逐字恢复 | `/tmp/m07-stream-task-negatives.log` |

五个 allocator 测试使用真实 Worker `ResultRetainedBudget` 与 System allocator，追踪请求地址、大小、alignment、realloc 和 deallocation：small H2Stream + 最后 Waker；large stream 的自动 Future Box；overaligned stream 的实际 Layout；prepared alias 重放/跨 F 拒绝与最后复用；funded CONNECT 前置拒绝及默认路径对照。仪器 executor 只标记实际 generic prepare/spawn，bound 来自 Hyper 的实际 H2Stream，并非重建模型。

std geometry 的实际输出为：connection pool 6651541B、process stock 3578749354B、data/control positions 518/20、outer server Task 9808B、stream Task 1408B、stream-task pool 187600B。新增独立 checked 条件为 `187600 <= 128 * 4096 = 524288`，随后才加入 aggregate stock。这里证明的是 Task pool 局部额度，不能把其余 stream scaffolds 或完整独立 2MiB 连接图视为已闭合，也不能把逻辑 stream 数量当成实际 backing。

## 六个能失败的实际源码反例

[run_regressions.py](run_regressions.py) 针对真实生产文件做窄替换，运行精确测试，再在 finally 逐字恢复。该脚本由主 agent 在独占 Cargo/源码阶段实际运行；不得与普通 CI 并行。

| 反例 | 实际命中的 oracle |
| --- | --- |
| `ordinary-cell-without-original-owner` | 最后 Waker 仍保留 Cell 时，不得复用原任务位置 |
| `prepared-clone-replay` | 同一 prepared lease 不得提交第二个 Cell |
| `oversized-future-bypass` | 过大实际 Future 不得 allocate 或消耗 prepared position |
| `pool-original-before-position-vec` | Cell/自动 Box/carrier/pool 请求 backing 必须先物理退出再归还额度 |
| `hyper-prepare-forwarding-omitted` | 真实 Hyper dispatch 必须消费原 prepare 接点 |
| `connect-preallocation-gate-omitted` | funded CONNECT 必须在 prepare/service 前拒绝 |

六项都实际进入指定单例 runtime test 并 FAILED，exit 101；没有用 compile failure、零测试或异常进程退出冒充反例。完整 command、mutation/log SHA、原始源码 SHA 与 `exact_restoration=true` 见 [verification.json](verification.json)。原始日志与 diff.gz 位于 `/var/folders/_n/bq14zlm14v9d3lftd01lw1f00000gn/T/m07-original-stream-negatives-n_3sqmk1`，lossless副本已发布在本目录 source-negatives/。

## 明确限制

Future/Output 外部 heap、共享 scheduler/runtime queues、instrumentation、peer IO、fixture allocations、其余 stream scaffolds 与完整 TLS/auth/DNS/header/body/issuer 图仍属于独立范围。没有完整连接 2MiB 或 M07 完成声明；P04 继续执行，后续阶段仍待实现/验收，V1 未发布。Linux 由用户手动验收；本收据不证明 Linux 或正式性能。无 push、PR 或 archive。

## 当前候选最终集成证据

Cargo-only全量CI `logs/ci-full/20261003-184228` **PASS，11952 passed /7 ignored，490s**：component11775/7、serverowner173、binarysmoke4，guards/fmt/check/clippy/build全部PASS。Clippy按仓库warning-only策略；新executor与test无新增诊断。初次preparedclone/跨F问题被只读review发现并修正，新tests和六source negatives覆盖，不遮蔽首次缺口。

全量CI生成的候选生产binary在真实独立1FE+3BE中七场景全部PASS：plaintext-IP、automatic-DNS、PEM-IP、outer-preflight、blockingControl、partialBodyDeadline、registryContentionControl。JSON只投影结果/平台/build/source/lock与artifact hash，不复制private effective config。当前workspace parking_lot实测每conn6651357B/stock3578649978B、outer9808B、stream1408B、128pool187600B；std单包每conn6651541B/stock3578749354B，对应typed任务值相同。

639源码pins及lossless日志/negative diffs另列。所有检查在parent79c1d417f的dirty候选运行，不能冒称最终检查点SHA或M07最终同SHA SQL/defaultSystem/Linux/性能验收。实际profiledev，没有release或Miri claim。P04executing，P05–P10open，V1None，完整graph/DNS等其他子图仍open；Linux用户手动，无push/PR/archive。继续原执行目标。
