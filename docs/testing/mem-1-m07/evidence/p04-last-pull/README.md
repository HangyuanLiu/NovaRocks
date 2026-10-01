# P04 最后一次 pull 与 Execution root 生命周期

本检查点只覆盖 P04 的 Execution driver/root sink/fragment lifecycle 接线。P04 继续执行；真实 Native 有限 producer、输入 backing oracle、领域源头、fetch/H2/lane/listener 尚未完成，不表示候选产品支持或 C4/P09 验收。准确来源和原始日志 hash 见 index.json。

- RootResultSink 在最后一次 upstream pull 前获得准确、唯一 input permit；64 个 driver 共享一个位置，空 pull 归还，成功 pull 经 edge 到 host 保留原 grant。blocked generation 只有真实 readiness 变化才重试；失败的 wallet 尝试不伪造容量事件。
- 明确 takes_original_input 能力使最终 driver edge 直接移动原始 Chunk，跳过 dictionary 调查/hydration/schema deep clone/原 accounting 转移。开启 profiler 的实际 PipelineDriver 验证原始 ArrayRef、ChunkSchema Arc、dictionary carrier/accounting owner 保持；普通 receiver 仍按原路径 hydrate。
- 实际 Arrow owner 反例：source 分配并返回真实 Chunk 前发布 producer failure，原实现取消先归还 grant，而 edge backing 仍在 PendingFinish 存活。修复后先销毁 abandoned edge，再触发 operator failure/cancel；credit callback 观察不到存活 backing。失败原始日志保留。
- pipeline 在实例化前绑定实际 DOP；RootResultSink 从准确 built geometry 计一次 finish，允许在预覆盖上界内由64收敛为1，不猜 configured DOP。重复 driver/错误 DOP/重新绑定拒绝。
- StaticSinkProgram::RootResult 需要 exact task/contract 的显式 host session；缺失或冲突不打开 legacy writer。RootRegistration 处理 prepare rollback、cancel、failure；正常成功只卸下 producer registration，不关闭 context 留存。
- driver 从 terminal sink 观察异步失败，包含 source parked、pending-finish 与 success/failure 竞态；成功必须等待 producer actual exit 和 ContextHeld，单独 exit 不能替代 End/handoff。

定向结果：pipeline 122、fragment 97（含4个新 bounded-root lifecycle）、root sink10、root channel19、真实original-carrier2均通过。Native all-target check、Execution all-target Clippy、fmt/diff check通过；Clippy有未改动的既有 warnings。历史 fixture 首次测试中错误把 wrong task 同时设成 expected identity，以及把多次 abort 通知当成多次逻辑决策，已修正 oracle 并保留日志。

本检查点没有 producer/fetch 生产接线，没有新增第二钱包，也不宣称 MEM allocation 计费、完整 Native metadata 包络或最终产品验证。
