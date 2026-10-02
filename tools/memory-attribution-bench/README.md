# Allocation attribution measurement harness

MEM-1 M02b 的测量入口。`manifest.json` 的字节 SHA 固定在 `src/manifest.rs`，在任何测量前冻结；入口校验 SHA、core 的 512 B 阈值和 8 B token。不得观察结果后调整 manifest。变更参数需要新的测量版本和重新冻结。

**smoke 不是性能结论。** 正式 Linux 空间、CPU、吞吐和尾延迟结论由用户在 `172.26.95.220` 手动给出。所有 JSON，包括 `formal`，始终写 `formal_acceptance: false`。用户给出结论前 G1 保持待验证，也是 M03a 大规模接线的前置；不合格时回到设计讨论。

## 四个入口

| bin 后缀 | global allocator | 用途 |
|---|---|---|
| raw | Jemalloc | 裸 allocator 对照 |
| counting | CountingAllocator<Jemalloc> | M02a 的进程计数形态对照 |
| attributing | AttributingAllocator<Jemalloc> | 环境作用域与 R1 helper 成本 |
| attributing-system | AttributingAllocator<System> | 可移植 smoke 与诊断 |

执行时在仓库根目录，四个 bin 名为 `attribution-bench-<后缀>`。所有变体共享业务操作、原 Layout、线程数和远端通信代码；attributing 两个入口额外执行 lane 绑定/R1 helper，这部分属于被测成本。Authority 与 lane 在计时前建立，四变体使用相同控制对象。W5 不安装 ambient scope，直接比较纯小对象路径。

## macOS/Linux 入口 smoke

```bash
cargo test -p novarocks-memory-attribution-bench --profile dev-opt --locked
cargo clippy -p novarocks-memory-attribution-bench --all-targets --locked
smoke_root="$(mktemp -d)"
for variant in raw counting attributing attributing-system; do
  cargo run -p novarocks-memory-attribution-bench --profile dev-opt --locked \
    --bin "attribution-bench-$variant" -- --mode smoke --out "$smoke_root/$variant"
done
```

测试会实际运行四入口，检查冻结矩阵覆盖、SHA、schema、p50/p99、System 的 unavailable 字段与空间读数。输出为 `<out>/report.json`，入口拒绝已有报告目录中的 `report.json`。

## 用户手动执行 Linux formal

下列步骤由用户在 `172.26.95.220` 执行，计划执行者不通过 SSH 在该主机运行负载。

1. 使用相同 checkout、`Cargo.lock`、rustc 与冻结 manifest，保存 `git status --short`、`git rev-parse HEAD`、`rustc --version --verbose`、`uname -a`、`lscpu`。检查工作树只包含已知任务修改；结果记录 git dirty 标志、lock SHA 和 manifest SHA。
2. 使用 `cargo build -p novarocks-memory-attribution-bench --release --locked --bins`。formal 拒绝非 Linux 或带 debug assertions 的构建。
3. 根据 `lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE` 选定固定 CPU mask；建议17个可用逻辑 CPU（最大16个分配 worker + 1个释放 worker），优先选择同NUMA node的不同物理核。不要照抄 CPU 编号。机器少于17个CPU时记录共享CPU的事实，仍运行冻结线程档，不改 manifest。
4. 固定 `MALLOC_CONF` / `_RJEM_MALLOC_CONF`、CPU mask与环境，运行时避免其他 CPU/内存密集工作。配置不能在变体间变化；报告同时记录 build `malloc_conf` 和有效 `narenas/tcache_max/tcache/background_thread/dirty_decay_ms/muzzy_decay_ms`，不能仅凭环境字符串声称配置相同。
5. 按 manifest 固定的三轮轮换顺序串行运行，保留全部 JSON，不挑选最好一轮。报告同时记录该进程 CPU affinity。System 可另跑作诊断，其空间读数不用于 jemalloc 成本比值。

```bash
cargo build -p novarocks-memory-attribution-bench --release --locked --bins
formal_root="$(mktemp -d)"
# 根据上面的 CPU 拓扑选择并填写，例如 shell: export BENCH_CPU_MASK=...
: "${BENCH_CPU_MASK:?Set the fixed CPU mask after inspecting CPU topology}"
for round in 1 2 3; do
  case "$round" in
    1) variants="raw counting attributing" ;;
    2) variants="counting attributing raw" ;;
    3) variants="attributing raw counting" ;;
  esac
  for variant in $variants; do
    taskset -c "$BENCH_CPU_MASK" "target/release/attribution-bench-$variant" \
      --mode formal --out "$formal_root/round-$round/$variant"
  done
done
```

以上循环在 bash 中执行；若使用 zsh，先进入 bash，或将 variants 改为 shell 数组。报告路径不能复用，避免覆盖证据。正式规模预先写在 manifest；W1 最大 live hold 256块约16MiB；W2约30MiB；W3/W5最多每worker256操作。全矩阵逐case排空，并非同时保留全部矩阵。

## 测量口径和对照方法

- W1：阈值、jemalloc 档位边界两侧及页倍数，两个对齐档；每尺寸/对齐独立结果。`size_histogram` 用真实的单独 alloc + `usable_size` 探针记录 usable/requested 比值；该探针不进入计时或空间pass。
- W2：4096行 batch 的 bitmap、16/32KiB列、字符串 offset/data与48个小对象；A内产生，B真正释放后ack。batch payload Vec元数据也属于端到端工作。
- W3：1/4/8/16个分配worker，额外一个释放worker；每5次第5次远端释放，nominal比例为20%。每个worker的pass从索引0重新开始；若N不整除5，真实比例为`floor(N/5)/N`，不能把20%当成已发生比例。每worker独立lane，远端释放按唯一所有权转移。
- W4：Vec逐步增长越过阈值并shrink回来；另一个完整R1链经成功helper发布。请求布局链在manifest固定。
- W5：纯小对象，本地释放，无环境scope。

每条结果有三个独立pass。吞吐与延迟pass在各自实际worker线程内完成固定warmup，并在全部warmup/远端释放排空后同步开始计时，保持该线程的tcache/TLS存续；warmup不计入CPU或wall time。`throughput` 的 ops/s 来自整段循环 wall time，含同步开始、线程join和远端drain，不是单独 hook 的 CPU 时间；`process_cpu` 是 getrusage 的 user/system delta。`latency` 对每个完整operation单独计时，p50/p99来自单次样本，不是块平均的分位数；计时器开销未扣除。远端operation包括真实free后的ack，因此也包括一致的通信和同步成本。operation的定义与worker数写入每条结果，不混用batch和单block的op。

`space` 在计时外依次采 baseline、held、released：held时对象真实存活并触碰每个系统页；构建在主线程逐worker lane执行，是固定保留形状，**不证明并发峰值或realloc瞬态峰值**。远端case的该pass在释放线程排空全部保留对象，吞吐/延迟pass仍按冻结规则选择每5次第5次remote（W2是全部remote）；每条结果的`nominal_remote_release_fraction`记录规则比例，各pass的`remote_operation_count`在收到实际free的ack后逐次累加，`remote_release_fraction`用该真实计数除以该pass的operations。W2的operation是一整个batch，W3是single allocation/release；warmup不进入这些计数。jemalloc epoch更新后记录 allocated/active/resident的原数与有符号delta；这些值不等于OS RSS，也不保证释放后立刻回落。

Linux `current_rss_bytes` 来自 `/proc/self/statm`，`peak_rss_bytes` 单独记录；macOS没有current读数，peak来自getrusage并标为peak，绝不拿峰值冒充当前RSS。System入口的 jemalloc元数据、stats和usable字段为null，有明确原因。

以相同workload/subcase、worker数、配置和每轮为匹配键，分别计算 attributed/raw 与 attributed/counting 的 CPU、ops/s、p50/p99、held allocated/active/resident/RSS delta比值；requested→usable放大单独比对。分母零或有符号delta非正时标为不可比较，保留原值，不能制造无限比值。空间resident和RSS受历史arena/tcache/OS保留影响，因此同时审查baseline、held、released绝对值和三轮离散程度，不能只看一项。

用户的结论与原始报告位置记入 MEM-1 M02b spec §8.4 和 MEM-1 umbrella 的 G1 收据。工具不自动判定合格、不修改spec/umbrella、不启动NovaRocks或替代1FE+3BE行为验收。
