# P04：保留实际 HTTP 字段原 owner

本检查点在原 fixed inbound HPACK block 之上，新增 `HeaderName::from_lowercase_bytes`，校验与既有 H2 lowercase API 相同，custom name 保留输入 Bytes，standard name 保持静态化。BorrowedSource 的 regular literal/indexed conversion 使用 shared name/value；默认 owned Cursor、pseudo 和错误语义保持原路径。http 1.4.0 原 archive 35 文件逐字核对，只有 name.rs 产品源码变化；版本及依赖不变，Cargo.lock 仅移除该 vendored package 的 registry identity。

实际 workspace 测试 1002 项通过（115 protocol +574 Native +313 Worker）。新增公开 HTTP 7 项使用实际 Worker ResultRetainedBudget 在 Vec/Bytes wrapper 分配前原授；System dealloc 哨兵确认两份 backing 实际退出后才回授。覆盖 custom name/value pointer、owner-backed clone 无新增分配、HeaderMap 移动/克隆/迭代逃逸、全部单字节 validator parity、64/65/65534/65535/65536 边界、standard/invalid/unwind。HeaderMap scaffolds 本身未授，不把此观察当完整 map 包络。普通 promotable Bytes 第一次 clone 可能分配 shared metadata；零 clone 分配结论限于原 owner wrapper 输入。

公开生产源码恢复-copy mutant 实际运行失败101；字节恢复后完整7项通过。[独立 shared-field 证据](../p04-shared-header-field/README.md)普通、恢复、fresh及Miri各10项通过，四个真实接点 runtime mutant 均101；128源码身份和18依赖锁身份核对。private sentinel 不冒充 Worker wallet。

最终 http/h2/Hyper/Tonic strict lib Clippy、Native all-target Clippy、workspace all-target check、fmt 均 exit0（既有 workspace warnings 保留）。fixture desktop-linux 全 BOM exit0，无缺 image/JAR、未pull。各完整日志/diff以确定性 gzip 保存，lossless-artifacts.json 包含原始/压缩 SHA 和长度；source-sha256.json 固定当前产品输入。

失败历史保留：首次 vendoring 假定缺失 checksum metadata 被 FileNotFoundError 拒绝，改用缓存 crate archive checksum 和35文件比对；初次 patch 缺 vendor 文件未修改；h2 私有 Name 路径 E0412 修正；HTTP no-default CLI 被非workspace package selector拒绝，不算源码失败或no-default验证。前两项是工具返回诊断，非完整shell日志。第一次库测试编译因缺依赖 rmeta 失败101，未执行测试；生产源码哈希不变，同命令重建后887项通过。root 仅删除记录中的旧 incremental 子目录；独立 agent 未清理生产 target。曾观察外部 cargo clean 进程，但其cwd/执行者未核实，不能归因；构建缺失原因仍未确认，保留记录。

本 slice 只消除 HTTP 转换副本。decoder plain/Huffman compact allocation 尚未原授；dynamic table、HeaderMap 三backing/duplicates/clone/iterator、Method/Scheme/Status/metadata 和完整2MiB连接/Native profile/lane/predecode/deadline 尚未闭合。P04 executing，P05–P10及完整SQL/system/性能验收保持open；V1未advertise。Linux性能测试由用户后续手动执行。无push/PR/archive。
