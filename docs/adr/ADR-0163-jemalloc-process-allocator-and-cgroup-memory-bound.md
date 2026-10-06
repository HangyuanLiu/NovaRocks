---
id: ADR-0163
title: "jemalloc serves the process and is chosen at build time; the memory bound and usage come from the process's own cgroup"
domain: [memory-governance]
status: active
supersedes: []
superseded-by: null
partially-superseded-by: [ADR-0167]
date: 2026-09-28
provenance:
  - "discussion: 2026-09-28 process allocator as default or switch, compared against Rust systems"
  - "PR: (回填)"
code-anchors:
  - "novarocks-server/Cargo.toml (feature jemalloc, Linux-only background_threads)"
  - "novarocks-server/src/memory_observation.rs (GLOBAL, jemalloc_configuration, sample_physical)"
  - "novarocks-server/src/cgroup_memory.rs (locate, effective_limit, anonymous_bytes)"
  - "novarocks-server/src/memory_limit.rs (visible_memory)"
---

## 问题

NovaRocks 进程的内存由哪个 allocator 分配？这个选择在构建期还是运行期做出？进程又从哪里得知自己的物理内存上限与实际用量？

## 背景与执行事实

本条建立在 ADR-0148 的观测级之上：`CountingAllocator` 是进程最外层的全局 allocator，只计数、不归属。本条回答它**里面**由谁真正分配，以及物理读数从哪里来。

Rust 与 jemalloc 的硬事实决定了设计空间：

| 事实 | 含义 |
|---|---|
| `#[global_allocator]` 是链接期确定的 static；每次释放必须回到分配它的 allocator | 进程启动后无法更换 allocator。运行时“开关”只能靠一个每次分配与释放都要分派的包装器，而且必须在第一次分配之前定下 |
| tikv-jemalloc-sys 默认以 `--disable-stats` 构建 | 不打开 `stats`，`stats.allocated / active / resident` 读不到 |
| jemalloc 只在非 Mach-O 平台编译 background thread 支持，但 `background_thread` 选项在所有平台都会被解析；平台不支持而选项为真时，jemalloc 初始化失败 | 在 macOS 上用任何途径打开 background thread，进程都会拿不到内存 |
| tikv-jemalloc-sys 的 `background_threads` feature 在除 musl 以外的所有目标上把 `background_thread:true` 编进默认配置 | 这个 feature 只能按目标条件在 Linux 上打开 |
| jemalloc 在带符号前缀的构建中总会读取 `_RJEM_MALLOC_CONF`，其优先级高于编译期配置 | 进程无法保证“不读环境变量”，只能读出生效值并公开 |
| tikv-jemalloc-ctl 返回的 C 字符串带结尾 NUL | 读取字符串类 mallctl 时要去掉 NUL |
| 各容器运行时不同：containerd / CRI-O 的 Pod 没有 `/.dockerenv`；cgroup 可能是 v1、v2 或混合模式；进程可能在嵌套 cgroup 中，而更小的上限设在父级 | 标记文件和固定路径都会读错上限；上限必须沿本进程的 cgroup 路径逐级取最小 |
| cgroup v1 用接近 `LONG_MAX`、随页大小变化的值表示无上限；v2 用 `max`；v2 的根 cgroup 没有 `memory.max` | 解析要分别识别这些形态；缺少上限文件的层级不是错误 |

同类 Rust 系统的做法（写入时核对过源码）：
- **默认**：服务端二进制普遍默认 jemalloc，包括 TiKV、Databend、RisingWave、GreptimeDB、Materialize、Neon 与 InfluxDB 3。
- **选择时机**：选择一律发生在构建期。
- **退出口**：多数保留编译期退出口，例如 TiKV 的 `SYSTEM_ALLOC`、Databend 与 InfluxDB 3 的默认 feature、Materialize 的 feature。
- **库**：DataFusion 这类库不替应用选择。
- **分歧**：只在 macOS。Materialize 因负载测试中延迟失控，在 macOS 上改用系统 allocator；其余系统在 macOS 上也用 jemalloc。
- **绑定**：这些系统都用 tikv-jemalloc 这组 crate，其中几家钉在打过补丁的 fork 上；Rust 编译器的发行构建也用它。

## 考虑过的选项

1. **jemalloc 无条件使用，不留退出口。** 吸引力在于只有一个构建组合。未选的原因是：同一代码版本上得不到系统 allocator 的基线，也做不了 sanitizer、heaptrack 这类需要系统 allocator 的构建。**成本否决**：退出口的代价是多维护一个构建组合，而不是与设计冲突。
2. **运行时切换 allocator。** 吸引力在于部署时可换。未选的原因是：它与“全局 allocator 链接期确定、释放必须回到原 allocator”冲突；做成分派包装器会给每次分配加分支，切换时机也无法早于第一次分配。**设计否决**。
3. **系统 allocator 为默认，jemalloc 为可选。** 吸引力在于零 C 依赖、最保守。未选的原因是：本领域后续的治理依赖 jemalloc（allocator 统计作为第二物理来源，size class 查询），生产中若同时存在两种 allocator，就要维护两套物理模型。**设计否决**，前提是本领域“生产只有一种物理模型”的约束。
4. **mimalloc 等其他 allocator。** 吸引力在于部分负载更快，DataFusion 的 CLI 与基准就默认 mimalloc。本条没有选它：统计与回收控制的成熟度、同类数据库系统的先例都在 jemalloc 一侧。**待评估**。
5. **macOS 使用系统 allocator（Materialize 的做法）。** 吸引力在于避开 jemalloc 在 macOS 上的历史问题，并兼容只认系统 allocator 的分析工具。本条没有选它：本地测试应当跑与生产相同的 allocator，多数同类系统在 macOS 上也用 jemalloc。**待评估**。
6. **让 C 库的 malloc 也经过 jemalloc（非前缀构建）。** 吸引力在于 allocator 统计能覆盖 zstd、zlib、openssl 等 C 库的分配，TiKV、Neon、InfluxDB 3、Materialize 采用这种做法。本条没有选它：前缀构建对 macOS 与其他库最安全，而 C 库的分配已由 cgroup anon 覆盖。**待评估**。
7. **按容器运行时的标记文件或固定路径探测上限。** 在 containerd / CRI-O、嵌套 cgroup 与混合模式下都会读错上限。**设计否决**。

## 裁决

**构建规则**
- **构建期选择**：进程 allocator 只在构建期选择。`jemalloc` 是 `novarocks-server` 的默认 Cargo feature，也是唯一受支持的生产形态。关闭它的构建回到系统 allocator，只用于同版本对照与诊断工具，CI 保证它持续可编译。
- **background thread 只在 Linux**：`background_threads` 只按目标条件在 Linux 上打开，任何配置途径都不得在 macOS 上打开它。
- **统计必须编入**：jemalloc 依赖必须打开 `stats`。

**配置与观测规则**
- **生效值公开**：默认配置在编译期写入。`_RJEM_MALLOC_CONF` 是运维的覆盖口，进程启动时读出生效的 `opt.*`，写入日志与指标，不声称忽略环境。
- **覆盖描述不随构建变化**：`CountingAllocator` 的覆盖描述与盲区列表描述的是计数包装器本身。allocator 统计作为单独的读数，回答“allocator 保留与碎片”这个盲区：jemalloc 构建中为 Measured，其他构建中为 Unknown。Unknown 不是 0，也不导出为 0。
- **来源分列**：cgroup anon、进程 RSS、allocator 统计与计数包装器的字节各自成列，永不相加。

**上限探测规则**
- **沿本进程 cgroup 路径取最小**：可见内存等于本进程 cgroup 目录到挂载点路径上的最小上限与物理内存两者中的较小者。cgroup 由 `/proc/self/cgroup` 与 `/proc/self/mountinfo` 定位；混合模式以 v1 的 memory 控制器为准。
- **失败带原因回退**：探测失败时回退到物理内存并记录原因，不当作“无上限”。每个进程只探测一次，所有 P 的推导看到同一个输入。

**调试规则**
- 排查 allocator 行为，先看启动日志 `process allocator configured`（allocator、background_thread、decay、编译期配置）与 `process visible memory detected`（上限来源）。

## 接受的妥协（诚实记录）

- 多维护一个构建组合：关闭 jemalloc 的构建要靠 CI 检查才能不腐烂。
- jemalloc 的版本受 tikv-jemalloc-sys 的发布节奏约束。写入时 crates.io 上的最新发布捆绑 5.3.1，缺少上游在 5.4.0 中修复的 TSD tcache 初始化顺序问题；这个问题在打开 heap profiling 时可能导致崩溃。因此在升级之前，本条禁止打开 heap profiling。
- tikv-jemalloc 在 crates.io 的发布权限集中在一个账号，靠 `Cargo.lock` 锁定版本与来源检查来缓解。
- macOS 与 Linux 的 jemalloc 行为不同：macOS 没有 background thread，脏页在业务线程中按 decay 策略归还。
- 前缀构建意味着 jemalloc 统计不覆盖 C 库的分配，要看 cgroup anon 才能看到它们。
- 构建需要 C 工具链与 make，因为 tikv-jemalloc-sys 从源码编译 jemalloc。
- 运维可以用环境变量改变 allocator 的行为；本条只保证这种改变可见，不阻止它。

## 何时重新评估

- tikv-jemalloc-sys 发布了捆绑 jemalloc 5.4.0 及以上的版本：升级依赖，此后才能打开 heap profiling。
- 本地 macOS 的 SQL 套件或基准出现与 allocator 相关的延迟失控：评估选项 5。
- 同版本对照或后续负载证明 jemalloc 在吞吐、尾延迟或 RSS 上不可接受：重新评估选项 4。
- C 库的分配成为需要 allocator 级统计的主要来源，或需要对 C 库的分配做归属：评估选项 6。
- 退出口构建长期没有被用于对照或工具构建，而它的 CI 维护成为负担：重新评估选项 1。
- tikv-jemalloc 的维护停滞，超过 12 个月没有发布而上游有关键修复：评估维护 fork 或更换绑定。
