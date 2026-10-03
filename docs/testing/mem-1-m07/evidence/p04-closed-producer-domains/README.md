# P04 已接纳 producer 的领域与最终结果移交

范围：从准确选择的内建/聚合 binding 和当前闭合 wrapper/selector 集合取得领域事实，逐输出 occurrence 经 Query Application、Native 最终布局、聚合/窗口同值输出符号重写、实际运行时 Project、typed IPC、FE Field/cache 和 MySQL 输出移交。Json/Bitmap/Hll/Object/Percentile 身份不靠载体或名字推断；私有 Object/Percentile 不能进入持久化入口。当前闭合清单并非所有内建、聚合、lambda 或 nested producer 的完成声明。

最终 identity Project 在覆盖 Field 前独立检查实际 batch、slot、array 和缓存事实。根 IPC 在 nested retag 前检查原 wire Field，FE 在重新构建前另检查嵌套缓存与实际 Array；已知冲突与未知 marker 拒绝，缺失事实允许补准确声明。只有最终结果路径保留 Int32→Utf8/LargeUtf8 dictionary，不扩大普通 Exchange/Project 的载体合同；选中 key/value 两类 NULL 均按实际值输出，payload buffers 保持共享。

定向测试、编译后运行时源码反例、恢复后的 Cargo-only 全量及同一源码候选的本地 1FE+3BE SQL/分布式基线证据见 manifest。原失败日志保留：前候选 Cargo 全量通过后，真实 HLL 聚合 SQL 暴露 logical build 新 ID 丢失 analyzer 领域事实；新增规划测试误用未准入 HLL window；两处 Host fixture 缺 fragment 输出声明；显式空/all-null JSON 数组丢域与 catalog JSON-list 被误拒的产品回归；dictionary helper 错误能力假设；越范围 FixedSizeList 成功 fixture；初始类型或 test fixture 构造编译错误。修复说明保存在批准计划的执行记录。源码 pins 明确绑定 dirty parent；不能把二进制编译身份写成后续检查点 commit。

这不是完整 ScalarValueV1 producer、source-growth funding 或 Host 安装验收。Scalar Host 门仍关闭，V1 未发布，P04 executing / P05–P10 open。完整 builtin inventory、nested cursor、FE collector/session assignment/live/staged 与 native transport 原目标继续。没有 full SQL/default System CI、Linux/release 性能或整个 M07 完成声明；Linux 由用户手动验证。无 push、PR 或 archive。

只保存 Cargo/focused/SQL 日志及 system 的显式 allowlist 投影；不复制 fixture 配置、JWT、密钥或完整 system diagnostics。较大日志以无损 gzip 保存，manifest 覆盖证据的 SHA256。
