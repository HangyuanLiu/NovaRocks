# P04 context-owned root 核心检查点

本收据只覆盖 P04 的 Worker 通道/生命周期和 preparation 所有权接口切片；P04 仍在执行，不能作为 C4 全部通过、候选产品支持或性能验收。准确来源与原样 gzip 日志见 `index.json`。

## 已实现与定向证据

- `RootResultChannel` 复用现有 `ResultRetainedBudget` 与纯 `OrderedRetainedStream`：W=2、独立 End 位置、immutable Data+End、累计 ACK、replay/NotReady 与实际 retained backing 分离。segment 先取得完整 backing credit，仅按 64 KiB quantum 初始化。
- root 固定 1 MiB 同时覆盖 schema 实际 spare 与 core metadata；超限在 channel 构造前拒绝。后续 host producer/driver metadata 从同一固定额度领取有限结构位置，真实 Drop 才归还；不增加 retained 钱包或 MEM 计费主张。core 对象/队列实际 capacity 独立核验，动态 metadata 持有者上限按冻结 driver/send 数量导出。
- 输入/scratch/send 独立 reservation 与 segment/delivery/header owner 跟随真实退出；ACK、queue clear、credit shrink 到零和 task terminal 都不能提前宣告物理释放。body-only `Bytes` alias 保留 delivery 位置、segment backing、fixed metadata 和原钱包。
- `PreparedTaskInstallation` 将纯 facts 与准确 runtime root 分开。creation transaction 在原 context fence 内接管；provisional/foreign root 不成为读路由。`ContextEntry.roots` 独立于 task record/horizon。
- 新读取 admission 与 context closing 在同一 registry fence。正常 release 先封读/退队列/唤醒，再等 producer、已准入 handler、payload alias、metadata 和 credit 实际退出。队列与最终 root metadata 在 registry 锁外 Drop。abort/lease 使用同一 owner，不伪造 consumed。
- scoped observable subscription 不永久积累已结束 context 的 callbacks。root render cursor 借用 immutable contract，保持 schema Vec/String 至真实 cursor Drop，避免逐输入 clone。

检查：Worker **301/301**（18 个 channel tests、6 个新增 context/root lifecycle tests，包含 allocation failure observer reentry）；Native execution-host **54/54**；Observable **3/3**；Renderer **28/28**，严格 Clippy `-D warnings` 通过；Worker Clippy 与 Native all-target check 通过，存在未改动上游/既有 warnings；格式及 diff check 通过。

## 真实反例与修复

1. segment allocation 失败曾在 channel mutex 未释放时 Drop credit/physical position，observer 读取状态会自锁。现在先固定 active builder 位置、解锁，再分配/清理；故障注入 observer 实际读取 snapshot/producer state 验证退出。
2. schema 名称很短但 String spare 接近 1 MiB，可通过单独 schema 限额，却超出 fixed schema+root 总包络。合并 bound 在 root allocation 前拒绝，测试确认钱包无新增扣账。
3. allocation probe 的开关已按线程隔离，但旧计数器是进程全局；默认并行 test harness 污染 constructor/step 收据。计数器改为同线程 `Cell`，默认并行 28/28 通过，错误原始日志保留。这不是 renderer 重新分配或以噪声忽略失败。
4. Native 测试使用新 installation wrapper 的 `expect_err`/facts 访问未同步，首次 all-target check 编译失败；已补安全 Debug（只显示 facts/has_root）并访问纯 facts。首次日志保留。

## 未完成边界

Native production host 当前仍返回 `root=None`；未 advertise V1。真实 root producer/finite CPU scheduling、最后 upstream pull 事前输入位置、bounded hydrate/domain/count、RootResultSession pipeline 接线、Native 新 fetch response/真实 H2 alias、control/data listener 与有限承载仍须继续 P04。固定 metadata 子位置只是可验证接缝，尚未用它证明完整 Native/DOP allocation。

FE 原子窗口/byte relay、领域/本地源头、MySQL 应用接线、LRA 退出、候选 1FE+3BE 与最终 CI 尚未完成。Linux 正式测试按用户明确指示后续手动执行；本收据无 Linux 或候选性能结论。无 push/PR。
