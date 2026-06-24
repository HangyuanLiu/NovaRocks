# E0 Pipeline Driver Scheduler Measurement Spike

日期：2026-06-23

复评日期：2026-08-30

基线：`origin/main@211e0bbad`

目标线程数：8（本机 logical CPU）

2x 线程数：16

## Rebase 后结论

1. E0 的生产语境必须更新：NovaRocks 当前主线已经是 native FE/BE split-crate 结构，`standalone-server` 和旧 `tests/sql-test-runner` 路径不再是当前验收入口；all-in-one 只是测试便利，后续 E1 不能只按旧 standalone 语境设计。
2. 调度器的核心风险仍存在：`GlobalDriverExecutor` 仍在 `novarocks/execution/src/exec/pipeline/global_driver_executor.rs` 中用单个 `Mutex<VecDeque<DriverTask>>` 作为 ready queue，worker pop、Ready/Running requeue、`EventScheduler::enqueue_ready`、`BlockedDriverPoller::enqueue_one` 仍汇入同一把锁。
3. 2026-08-30 的 synthetic rerun 没有推翻旧方向，但强化了一个限制：10ms driver slice 下 affinity 没有稳定收益；极短任务下 per-worker/local upper bound 仍明显更高；packed level-head 在 8/16 线程下都弱于 padded，E1 禁止 packed shared atomics。
4. 当前无法给出新的 q18 profile 结论：Docker daemon 未运行，且当前 fixture 需要新的 `apache/iceberg-rest-fixture:1.10.1` 镜像。旧 2026-06-23 q18 采样只能作为历史参考，不再作为当前 rebase 后证据。
5. 下一步仍可进入 E1，但范围要收窄为“单级中央 lock-free substrate + metrics + loom + legacy mutex 对照”，不要同时做 MLFQ、公平 policy 或亲和槽。若 E1 要宣称真实 workload 收益，必须先补 current all-in-one 或 1FE+3BE q18/profile 证据。

## 当前代码证据

| 事实 | 当前证据 |
|---|---|
| ready queue 仍是单锁 FIFO | `novarocks/execution/src/exec/pipeline/global_driver_executor.rs:283-286` |
| submit 将 task 注册后 push 到同一 queue | `global_driver_executor.rs:329-346` |
| worker pop 仍锁住 queue | `global_driver_executor.rs:350-364` |
| Ready/Running requeue 仍锁住 queue | `global_driver_executor.rs:404-412` |
| `EventScheduler::enqueue_ready` 仍回写 executor shared queue | `novarocks/execution/src/exec/pipeline/schedule/event_scheduler.rs:409-416` |
| `BlockedDriverPoller::enqueue_one` 仍回写 executor shared queue | `novarocks/execution/src/exec/pipeline/blocked_driver_poller.rs:158-161` |
| execution runtime 按 `driver_threads` 创建 executor | `novarocks/execution/src/runtime/execution_runtime.rs:141-145` |
| 当前 server 入口是 all-in-one/FE/BE role 启动 | `novarocks-server/src/main.rs`，见根 `AGENTS.md` §7.3 |

## 证据文件

- 微基准工具：`tools/dev/driver_scheduler_e0_bench.rs`
- 2026-08-30 synthetic rerun：`reports/e0-driver-scheduler/microbench-*.csv`
- 历史 q18 轻量采样日志：`reports/e0-driver-scheduler/q18-run-sample20.log`（2026-06-23，旧命令路径；只作历史参考）

## 2026-08-30 微基准摘要

队列 benchmark 模型：512 个 driver token 在队列中循环；每个 worker 执行 `pop -> touch task cache lines -> spin work -> push`。0/50/500us 使用 5000 iterations，10ms 使用 100 iterations，因此只用于方向性复评，不与 2026-06-23 绝对值直接比较。

| threads | work_us | mutex central ops/s | atomic central ops/s | per-worker local ops/s | affinity upper-bound ops/s | mutex lock wait |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 0 | 2.37M | 2.20M | 153.55M | 189.50M | 70.56ms aggregate |
| 8 | 50 | 139.08K | 141.61K | 145.49K | 138.09K | 131.09ms aggregate |
| 8 | 500 | 15.25K | 15.25K | 15.18K | 15.10K | 29.77ms aggregate |
| 8 | 10000 | 794.8 | 789.6 | 792.3 | 795.2 | 0.85ms aggregate |
| 16 | 0 | 3.44M | 1.27M | 241.18M | 189.07M | 236.98ms aggregate |
| 16 | 50 | 142.60K | 143.08K | 147.27K | 146.56K | 2.20s aggregate |
| 16 | 500 | 16.89K | 15.41K | 16.96K | 16.83K | 1.40s aggregate |
| 16 | 10000 | 1427.1 | 1424.6 | 1391.2 | 1411.3 | 14.23ms aggregate |

Interpretation:

- 0us 是纯调度器压力测试，per-worker/local upper bound 远高，但它不代表当前 10ms driver slice 的真实成本比例。
- 50/500us 下 mutex wait 已明显存在，16 线程时 aggregate wait 达秒级；这支持 E1 做 ready substrate 与指标化。
- 10ms 下各模型吞吐接近，说明 E1 不能承诺直接改善 q18 wall-clock；它首先是移除扩展性天花板和建立可观测 substrate。
- affinity upper bound 在 10ms 下没有稳定优势，E3.5 继续 no-go。

## Level-head 原子热点

本表取 `work_us=0` 的 level-head 子测试。

| threads | packed level-head ops/s | padded level-head ops/s | per-worker local ops/s |
|---:|---:|---:|---:|
| 8 | 22.06M | 28.95M | 215.88M |
| 16 | 27.05M | 35.32M | 259.88M |

Interpretation:

- rebase 后新跑法中，packed 在 8/16 线程下都弱于 padded。
- E1/E3 不能使用 packed level-head array；至少要 `repr(align(64))` padding，E1 substrate 更应优先用 sharding 避免单一原子热点。

## q18 workload 状态

历史记录：

- 2026-06-23 旧路径下，`tpc-h/q18` 无采样 PASS 117.62s，20s `sample` PASS 152.03s。
- 该记录来自旧 `standalone-server` / `tests/sql-test-runner` 命令路径，rebase 后只作历史参考。

2026-08-30 当前状态：

- `cargo build --profile dev-opt` 通过，当前 workspace 默认构建 `novarocks-server`。
- `cargo build --manifest-path tests/sql/runner/Cargo.toml --profile dev-opt` 通过。
- `docker/iceberg-rest/up.sh --prepare-only` 通过并重建 runtime entry。
- `docker/iceberg-rest/up.sh` 失败：缺少 `apache/iceberg-rest-fixture:1.10.1`。
- `docker pull --platform linux/arm64 apache/iceberg-rest-fixture:1.10.1` 失败：Docker daemon 未运行。

因此当前报告不使用 q18 作为 rebase 后 Go/No-Go 证据。Docker 恢复后需要补：

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
tests/sql/runner/target/dev-opt/novarocks-sql-test \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite tpc-h --only q18 --mode verify --query-timeout 300
```

若 E1 目标包含生产形态收益，还应补 1FE+3BE 或对应 cluster-mode 的 profile。

## E0 回答（更新）

### 1. 单把全局锁是否真的热？

代码热路径仍成立；synthetic 短任务证明锁会热。当前没有新的 q18 profile，因此不能说它是当前 q18 主瓶颈。

### 2. 中央 vs per-worker deque crossover

10ms 片下中央模型没有被否决；极短任务下 per-worker/local upper bound 明显领先。E1 可继续中央化，但必须做 padded/sharded lock-free substrate，不能把单个 head/tail/level-head 原子作为新的热点。

### 3. 亲和是否有 CPU-bound cache 收益？

当前数据不给 go。10ms 片下 affinity upper bound 没有稳定优势，E3.5 暂不做。

### 4. vruntime 应该用什么计量？

不要直接用 `driver.process()` wall-clock。后续若进入 E3，vruntime 应使用 active compute time：

- 首选：per-driver active operator time。
- 次选：`DriverTotalTime - DriverBlockedTime - DriverInputEmptyTime - DriverOutputFullTime - DriverDependencyWaitTime`。
- IO-bound driver 应保持中性，不能因为 wall-clock 长而被反向惩罚。

## 下一步建议

1. 先不要启动 E3/E3.5；E0 对这两项仍是 no-go。
2. E1 可以作为 substrate PR 继续，但文档和代码路径必须改为 `novarocks/execution/src/exec/pipeline/**`，验证入口改为 `novarocks-server` + `tests/sql/runner`。
3. E1 的 `ReadyScheduler` trait 先承载四类 source metrics：`submit`、ready/running self requeue、`EventScheduler::enqueue_ready`、`BlockedDriverPoller::enqueue_one`。
4. E1 默认保留 mutex legacy flag；先做 `-j 1` byte-identical，再用短任务微基准与 q18/current profile 判断 lock samples 是否下降。
5. Docker 恢复后，先补 current q18 all-in-one；若 E1 要宣称生产收益，再补 1FE+3BE profile。没有这些证据时，E1 只能描述为并发 substrate 地基，不描述为 q18 优化 PR。

## 完成前复核

- `rustc -O tools/dev/driver_scheduler_e0_bench.rs -o target/e0_driver_scheduler_bench` 成功。
- `rustc --test tools/dev/driver_scheduler_e0_bench.rs -o target/e0_driver_scheduler_bench_tests && target/e0_driver_scheduler_bench_tests` 通过：1 passed，0 failed。
- `cargo build --profile dev-opt` 通过，用时 5m42s，仍有既有 warning。
- `cargo build --manifest-path tests/sql/runner/Cargo.toml --profile dev-opt` 通过，用时 2m29s，仍有既有 warning。
- 当前 q18 未运行：Docker daemon 未运行，且 fixture 需要新的 Iceberg REST 镜像。
