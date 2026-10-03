# P04 原授 Tokio TaskCell 子切片

本收据记录实际 vendored Tokio 的 task Cell、普通 spawn 策略自动产生的 Future Box，以及原 Bytes carrier 的有限所有权验证。当前只有初轮定向结果和实际反例已记录；最终源 pin、日志归档、最终 System target、同源回归与 full CI 由主 agent 后续整合，均仍为 **PENDING**。这不是完整 Native Task graph、每连接 2MiB 包络或 1FE+3BE 产品验收。

## 实际接口与验证边界

`Handle::task_allocation_capacity_bound<F>()` 给出实际 scheduler Cell 的请求 Layout 上界，并在普通 spawn 会自动 Box Future 时加入该 Box 的请求大小。`spawn_with_task_owner(F, Bytes)` 让同一原能力随实际 Cell 保留到最后 task/Waker/AbortHandle alias 退出，真实 Cell deallocation 后才退出 owner。调用方仍须事前取得能力；Bytes 自身不构成资金证明。

`novarocks/native-adapter/tests/native_original_task_cell.rs` 的实际 System allocator target 使用生产 Worker `ResultRetainedBudget`，先授 Cell/自动 Future Box 与 carrier，再观察真实地址、大小、alignment、deallocation 和完整额度回收。初轮及恢复后最终 5/5 通过：current-thread 小 Future/大 Output/overaligned Cell、多线程自动 Future Box 与跨线程最后 Waker、pending abort 后保留 Waker、未 poll 的 task/shutdown，以及 Future destructor panic。这些结果的来源是 `/tmp/m07-task-cell-first-tests.log`；最终复验见下文。

Future/Output 外部 heap、共享 scheduler queue、runtime、hook、instrumentation 和完整应用 Task 图不包含在这五项子图证明中。初轮 geometry 单例虽然通过，也不能据此宣称实际 whole-connection 图闭合。

## 已记录的初轮结果

| 项目 | 实际结果 | 来源 |
| --- | --- | --- |
| Native Adapter check | 日志有 `Finished dev profile`；该日志未单独记录 shell exit | `/tmp/m07-task-cell-first-check.log` |
| checked geometry | 1/1 通过；见 JSON 中原始输出数值 | `/tmp/m07-task-cell-first-geometry.log` |
| 实际 System target | 初轮及恢复后最终 5/5 通过 | `/tmp/m07-task-cell-first-tests.log` |
| Native Adapter / Native Trust lib | 初轮 673/673、9/9 通过 | `/tmp/m07-task-cell-libs.log` |
| unstable tracing 首次编译 | 4 个 E0277，未进入 runtime 测试 | `/tmp/m07-task-feature-first-trace-check.log` |
| tracing cfg 修复后 check | 日志有 `Finished dev profile`；该日志未单独记录 shell exit | `/tmp/m07-task-feature-fixed-trace-check.log` |
| 实际 feature 矩阵 | A/B/C 各 2/2，六个 metadata/test 命令 exit 0 | `/tmp/m07-task-feature-matrix.log` |

tracing 首次失败是 `cfg!` 的运行时拒绝仍让不支持的 generic scheduler 调用参与类型检查，普通 Future 与 `InstrumentedFuture` bound 不一致。修复采用编译期 cfg 分支；首次失败保留，不能当成有效 runtime negative。

feature 矩阵由 [feature-probe/replay.py](feature-probe/replay.py) 调用实际 Tokio 普通依赖：A 最小 current-thread rt/io-util，B rt-multi-thread，C `tokio_unstable` + tracing。A/B 执行实际 owned tiny Future；C 的静态 bound 和 owned spawn 都明确返回 `Unsupported`，拒绝的 Future 从未 poll，且先 Drop Future 后退出 Bytes owner。每行也实际执行普通 `Handle::spawn`，包括 traced cfg 路径。三行各核对 6 个 production package identity，并确认 source 未变化。

这里的 feature marker **只证明接口和生命周期事实，不证明资金或 allocator capacity**；它与上述生产 Worker 额度 + System target 是不同证据。

## 三个有效源码反例与构造正控制

[run_regressions.py](run_regressions.py) 对实际源码做窄替换，精确运行指定测试，并在 finally 逐字恢复。执行该脚本需要独占 Cargo 与可修改源码的授权，不能与普通 CI 并行。

| 反例/控制 | 真实观察 | 有效性 |
| --- | --- | --- |
| `owner-before-cell-deallocation` | 先 Drop owner、后 Drop Cell；精确 runtime test FAILED，exit 101，实际 deallocation-before-credit oracle 命中 | 有效 negative |
| `future-completion-owner` | owner 退到 Future completion；精确 runtime test FAILED，exit 101，仍存活 Waker 对应 owner 已退出 | 有效 negative |
| `post-box-constructor-positive` | 在实际 Cell Box 创建后注入安全 panic；构造 unwind、实际 Cell/FutureBox/carrier 退出及额度 oracle 通过，exit 0 | 有效正控制 |
| `post-box-constructor-missing-outer-clone` | 保持同一 panic，仅遗漏外层 original clone；精确 runtime test FAILED，exit 101，deallocation-before-credit oracle 命中 | 有效 negative |

前两个反例的实际日志、diff.gz 和逐字恢复收据在 scratch `m07-original-task-negatives-lxc1k67h`；最终构造正控制与 missing-clone 反例在 `m07-original-task-negatives-jal6iimb`。完整 scratch 绝对路径、command、log SHA、mutation SHA 和恢复事实保存在 [verification.json](verification.json)。两次均 `exact_restoration=true`。

首次 constructor scratch 因只复制部分 production patches，metadata 解析到 registry Arrow 58.4.0 等非生产 identity，严格 identity 检查先拒绝，**没有执行 constructor runtime test**。该失败在 `/tmp/m07-task-cell-negatives.log` 保留；不计为构造反例，也不否定此前已经完成的两个有效 runtime negatives。修正为完整 production path patches 后，最终构造 probe 核对 238 个 package identity；它复制真实 downstream allocator test 并连接真实生产 crates，不构造 TaskCell 算法模型。最终正控制和 negative 的日志见 `/tmp/m07-task-cell-constructor-regressions.log` 指向的 scratch。

## 当前最终定向结果

构造负例之后逐字恢复源码，八个相关protocol targets **61/61 PASS**，其中真实TaskCell/System allocator五项再次通过；与同候选Native673/Trust9lib合计743项unique定向通过。此前75 protocol集合的旧切片结果不重复计入本轮61。生产constructor、布局推导与spawn同型已由两位sub-agent只读复核，未发现新增blocker；review不是测试结果。

workspace fmt与九个改动Tokio文件edition2021 pinnedrustfmt均PASS。strict全targetsClippy被既有SPI和Native large-error/expect等基线拒绝，日志保留；全量CI沿仓库warning-only策略，不能冒称strict全通过。最初两个target名字误写，Cargo调度前拒绝，修正后运行实际八targets通过。

源pin已发布：555原Tokio文件只有17改动（8 socket+9 Task）；563项文件哈希见source-sha256.json。原始日志losslessgzip、3有效negative及构造positive的diff/probe/source/lock已保存。当前候选的最终结果如下，不能使用父检查点的结果覆盖当前候选。

## 当前候选集成结果与限制

Cargo-only全量CI `logs/ci-full/20261003-180547` **PASS，11947 passed /7 ignored，646s**：component11770/7、serverowner173、binarysmoke4，guards/fmt/check/clippy/build全部PASS。Clippy是仓库warning-only策略，新增源码无新增诊断；native_server既有map_err lint在parent源码中存在。

全量CI生成的当前候选生产binary，在实际独立1FE+3BE中七场景全部PASS：plaintext-IP、automatic-DNS、PEM-IP、outer-preflight、blockingControl、partialBodyDeadline、registryContentionControl。安全JSON仅投影场景结果、source/build/lock身份、平台与artifact hash，不复制private effective config。Linux未执行，由用户手动后补；上述macOS场景不证明Linux或正式性能。

实际workspace parking_lot geometry：每连接6463421B、processstock3477540402B，outerserverTask9472B；单Native/std对应6463605B、3477639778B、9472B。实际profile是dev，没有宣称release测试；API沿原debug/release自动boxing政策计算真实Layout。所有验证使用parent6794993ce的dirty候选且精确product pins另存，不假称最终检查点SHA，更不外推为完整M07最终同SHA SQL/defaultSystem/性能验收。

P04仍executing，P05–P10open，V1None。Hyper内部stream/升级task、出站Tonic/Hyper任务与channelbuffer worker、sharedscheduler/queue/PAL、DNS原增长及真实退休、TLS/auth/error/body/issuer/incomingpeer-lane与完整独立2MiB图仍OPEN。DNS有界resolver与OS/NSS语义需要用户裁决，其他独立切片继续。无push/PR/archive，继续原执行目标。
