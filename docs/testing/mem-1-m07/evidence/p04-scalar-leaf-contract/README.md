# P04 ScalarValueV1 schema 与 leaf 检查点

本切片冻结准确 ScalarSchema、独立 flat Native schema wire、原始 envelope 预检、纯 SCV1 leaf 编解码、物理兼容验证和 Native leaf source 前置实现。JSON/opaque 身份由 schema 明确提供，不从 Arrow 存储类型推断。完整 List/Map/Struct producer、SQL 事实接线和 FE typed collector 尚未实现；Host 与 RootProducerSession 继续明确拒绝 ScalarValueV1。

验证对象为 parent `e61696e1fd577099d8d911614918ea40c1135de7` 上的 dirty 候选；[source-pins.json](source-pins.json) 固定 24 个最终产品/测试源文件。检查点提交不会改写原 artifact 的 source revision。

- 定向：公共合同相关 785、Native lib 720、root integration 40 项通过；fmt、相关 all-target Clippy 通过。
- Cargo-only 全量 CI：`logs/ci-full/20261004-024330`，12,104 项通过、7 项忽略，611 秒；不是完整 SQL/default System CI。
- 新 flat scalar schema codec 的 Miri 13 项通过；仅证明该 codec 范围，未证明整个 producer/backing 图。
- 最终冻结二进制下，8 项独立 1FE+3BE trust/ingress/replacement 回归全部通过。canonical binary 与 primary 相等，SHA256 `9fca658fa262decdc50b8a2e6251561212646c7a6dcba5c8e11b2ded5a109a39`；这些不是 scalar 产品端到端验收。
- 11 类源码变异编译后在真实测试运行中失败，随后逐字恢复。共 12 次尝试：最初只漏固定 Struct 目标费用的变异仍被其余字符串费用挡住，测试存活；不计反例证明。扩大为漏完整 prospective neutral/seen 共存费用后，同一真实测试失败。两次日志均保存。

[manifest.json](manifest.json) 给出结果、66 个证据文件的 hash 和未完成边界。System 仅保存显式 allowlist 投影，未复制 effective config、JWT/key 或私有诊断。最初编译错误与 fixture 错误保留在日志中，均未计为有效源码反例。

Native leaf cursor 持 immutable 原 RecordBatch alias，按 turn 重借 selected cell；没有 hydrate 或完整 variable copy。参数形式的 prepaid scratch capacity 不是资金能力，caller 仍须提供真实原输入增长准入、RootInputPermit 和整个原 backing 证明。每 turn 已不重复比较长 timezone；constructor 初始最多三次全 timezone 比较尚须增量预算化，必须在完整 producer 安装前闭合。纯 leaf 模块暂拒绝容器，不缩小最终领域支持要求。

P04 executing，P05–P10 open，V1 None。DNS 决策仍待用户；Linux 性能/正式验收由用户手测。没有完整 M07、完整物理 allocator/backing 图、release 性能或发布完成声明。
