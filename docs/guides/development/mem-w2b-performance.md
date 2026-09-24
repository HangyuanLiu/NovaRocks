# MEM-W2B Linux 性能验收准备

本指南只规定可复现的运行输入与判定方法。批准的阈值以外部 workflow 的 MEM-W2B core plan v5 §4 为准；macOS 冒烟不能填作 Linux 通过。运行前使用干净的同一代码 SHA、原生 Linux、release profile 和 `CountingAllocator<System>`，记录 kernel、CPU/硬件线程数、Rust 版本、allocator、频率/隔离条件、命令和原始文件目录。

## 叶子供给负载

在观察任何候选结果前，仅在目标机器上校准一次：

```bash
cargo bench -p novarocks-memory --bench reservation_cost -- --calibrate
```

保存输出中的三个 `work_iters`、`observed_ns` 和 checksum。随后每个进程显式传入同一档固定迭代数，不逐进程重校准。每档 T 为 1、硬件线程数、2 倍硬件线程数；每个候选至少五个独立进程，候选次序交错。示例单格命令：

```bash
cargo bench -p novarocks-memory --bench reservation_cost -- \
  --candidate reservation --threads 10 --pairs 100000 --work-iters "$WORK_ITERS_5US" \
  --latencies-file /absolute/output/reservation-10t-5us-run1.csv
```

同格分别运行 `reservation`、`protocol`、`mutex`、`none`，相同 `--threads`、`--pairs`、`--work-iters`。`--work-iters 0` 是饱和诊断，不设吞吐通过门。大额混合另用 `--candidate reservation --mixed-every N --large-bytes 16777216`，单列成功/拒绝对数、CAS 重试、父链补额/返还，不混入 64B 快路径门槛。保存所有 stdout 与每轮原始延迟 CSV。

每个候选取五轮 `pairs_per_s` 的中位数及五轮 `p999_ns` 的中位数；在每个相同 T、相同工作档比较：5 µs 时 Reservation/none ≥0.90，20 µs 时 ≥0.95，1 µs 时 Reservation/Mutex ≥1.00，全部三档 Reservation 的 p99.9 ≤ Mutex。任何硬门失败都按 plan 返回设计评审，不用饱和或 macOS 数字抵消。

## 端到端留存负载

当前入口可运行候选/无治理 90% filter、8 跳 derive、8 次本地 move、fanout 2/4/8、共享 body 的整组导入、Arrow IPC 往返后的整组导入、重复 slice、`unary_mut` 原地复用/复制回退，以及跨线程最终释放。`ipc_group` 是合成共享 body，`ipc_decode` 在计时前完成真实序列化/解码，并报告往返后的实际 backing 共享数。`--value-type` 可选 primitive、nullable、dictionary、nested、view；`--backing-bytes` 请求完整输入 backing 容量并输出实际容量。`--phase-alloc` 仅支持单线程 `unary_mut`，输出输入、kernel、输出阶段的分配量。输出的 `matrix_complete=false` 表示尚不能据此裁决完整 V4：move8 是本地交接槽，分阶段分配仅覆盖单线程 unary，长时间 slice 稳态和 Linux 多轮矩阵仍待测。先用下面的命令确认两个候选完成相同的行数与 checksum，并保存原始延迟；其余已批准布局与类型矩阵补齐后再做 Linux 正式裁决。

```bash
cargo bench -p novarocks-memory-arrow --bench retained_cost -- \
  --candidate retained --scenario derive8 --layout shared \
  --threads 10 --rows 4096 --columns 8 --batches 100 \
  --latencies-file /absolute/output/retained-derive8-10t-run1.csv
```

同输入执行 `--candidate none`。`shared`、`siblings`、`independent` 是域布局；以输出中的 `lineage_scope` 判断是否真为跨线程同一谱系，不以布局名推断。记录输入/输出实际 backing 数和容量、data/metadata exposure、分配次数/字节、父链次数、拒绝数与完成行数。正式 90% filter 与 8 跳 derive 的门为同 T median 延迟比 ≤1.15、p90 比 ≤1.25、吞吐比 ≥1/1.15；每格至少五个独立进程，并保留全部原始样本。
