# MEM-W2B 历史性能入口与当前成本边界

本页保留历史基线的用途和现阶段验证边界。共享 Reservation 与 memory-arrow 谱系记账已由 M02a 从当前核心删除；历史 W2B benchmark 应在对应历史 SHA 的独立 checkout/构建中运行，不在当前实现恢复旧 API 或创建等价“兼容 Reservation”。

## 历史基线

M02a plan 固定的原 Reservation 协议基线为：

```text
af9d0591676c75ede5fb53c2d64949e2d80667c5
```

比较时保留该 SHA 的原源码、Cargo 配置、benchmark 输入与原始输出。历史 `reservation_cost` 的供给负载、候选名称和校准方式属于历史构建；历史 memory-arrow `retained_cost` 的 Arrow 留存场景也属于历史构建。它们不能直接证明当前 FundingDomain、stock、稳定 owner 或 residual 的成本。

基线运行必须在独立目录完成，避免改写当前候选工作区或把旧代码移植进新库。只有确认相同工作量、相同授权责任与释放时点的操作才进行相对比较；新容量、drain、teardown/residual 服务没有旧协议等价操作时，按批准的绝对进展与服务门单独判断。

本页不记录或推断任何已通过的 Linux 性能格，也不把历史功能 smoke、结构校验或旧运行结果当作新协议验收。

## M02a 当前采集入口

新入口为 `novarocks/memory/benches/stock_cost.rs`，冻结输入为同目录 `stock_cost_manifest.json`。运行方式、候选选择与入口限制以该入口及其说明为准；正式门以批准的外部 workflow M02a plan 为准。

采集需要覆盖正常局部 funded 步、scope 切换、量化跨层、债务与增长冻结、三类 drain、残留 metadata 生命周期。记录每步父/root 交互、真实跨层频率、完整 iteration 的吞吐和 tail、idle/workset、floor、活动 owner 与 residual payload/metadata、公有 storage backing。hook 的局部成本不能替代跨层等待和持锁成本。

原生 Linux 正式采集由用户后续手动完成。应使用干净的相同候选 SHA、release profile，记录 Linux 主机、CPU/NUMA、编译器、allocator、cgroup、亲和性、一次性供给校准、manifest 哈希与所有原始轮次。macOS 本地 smoke 仅证明入口可运行；`protocol-only` 数据统一省略底层 allocator 时必须明确标记，真实 jemalloc/header/size class 成本由 M02b 交付。

当前入口交付与功能验证不产生 G1 通过结论。`formal_acceptance=false`、未完整覆盖矩阵、缺失历史基线外部结果或服务格时，应继续报告 pending；不得用少数短任务、平均 hook 时间或历史 W2B 阈值替代当前批准门。

## 尺寸分段归属的独立成本入口

当前全局 allocator 的真实成本由 [allocation attribution harness](../../../tools/memory-attribution-bench/README.md) 采集；冻结输入为该目录的 `manifest.json`。它与历史 W2B 和 FundingDomain/stock 原语 benchmark 分开：raw、counting、attributing、attributing-system 四入口比较同一业务操作，其中绑定、尾部、R1 helper、TLS 与原子更新属于 attributing 成本。不要把 CountingAllocator 对照结果写成当前 GLOBAL 的结果。

阈值固定 512 B，带来源请求尾部加 8 B、原对齐保持；usable size 可跨 jemalloc 档位。requested（含尾部）、usable、allocated/active/resident、current/peak RSS 分别保存；realloc 搬迁的旧+新瞬态峰值不由 held 空间 pass 证明。<512 B 环境请求不读 TLS，但真实释放事件有额外原子计数；它的 CPU 成本必须测量。

Q=1 MiB 是发布阈值，hook 返回后的槽余额 <Q，不是物理内存限额。任意时刻有一笔没有大小先验上界的在途操作；Q×sampled pins 排除该项，也不证明全堆一致性或结清。不能用它解释掉全部 resident/RSS 差额。

harness 使用固定 worker 中的 warmup、吞吐/延迟/空间三个独立 pass，并保留远端释放 ack 与 drain 成本。macOS/System smoke 只检查入口与输出；正式 Linux 空间、CPU、吞吐和尾延迟由用户手动运行并审查全部冻结矩阵和轮次，工具始终报告 `formal_acceptance=false`。在用户结论前 G1 保持待验证，不得宣称性能验收完成。
