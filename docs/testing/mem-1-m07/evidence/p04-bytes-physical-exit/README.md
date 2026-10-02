# P04：Bytes wrapper 的物理退出与原 Root grant

parent `f987513c7ec847013b77a6329933b2a47f97f674`。当前 source SHA256、命令、退出码和完整日志 hash 见 [index.json](index.json)。这是 macOS arm64、Rust 1.92.0 的本地行为切片；Miri 使用记录在 index 的 nightly。P04 仍执行。

锁定的 bytes 1.11.0 原 `from_owner` 会先释放 owner 内的 grant，再释放 wrapper Box。新增 additive `from_owner_with_exit_guard` 将原 owner 和独立 guard 分开：owner 在唯一 AsRef 前已经到永久地址；最后 strong alias 先原址 Drop owner，再释放实际 Box，最后 Drop guard。owner Drop 或 AsRef panic 也保留该顺序。原 API 和转换语义保持；Vec/BytesMut 转换仍新建独立 copy，必须由转换点覆盖。

Worker 原段的 Vec/Bytes wrapper 退出后才归还原 credit/segment position；offered Bytes wrapper 退出后才释放 delivery。read owner 改为不暴露 Weak/raw Arc 的 strong-only handle，最后 Arc heap 先释放，再发布 read position exit。原固定 core metadata 检查包含实际 Bytes wrapper Layout 和 delivery Arc Layout；没有新增预算钱包。正常关闭的直接读 marker 不分配新 delivery。

新 allocator oracle 在实际 System.dealloc 返回后记录 wrapper 已释放，guard 对此断言。它覆盖 normal、owner Drop panic、AsRef panic，并验证 split/freeze 的实际 Shared Box 与 getter 一致。故意交换 Box/guard 退出顺序时该 oracle 失败（exit 101）；恢复源码后通过。语义测试覆盖 !Unpin owner 地址、切片、空值/高对齐、转换、panic 和用 Barrier 同时放行的最后两个 alias；7 项全部通过 Miri。Barrier 保证共同起点，不能声称证明所有可能线程交错。

bytes 原完整 suite 加新增测试共 1,250 项通过，其中 doctest 246 项；随后加强测试再次通过 7+1 项。Worker lib 313、Native reader/producer session 15+12、相关 backend_task_execution 150，共 490 项 consumer 测试通过。Worker root 23 项已包含在 313 中，不重复计数。workspace all-target check、两 consumer all-target Clippy、workspace/vendor fmt 和 diff check 通过；全仓编译因 crates.io bytes patch 影响全局依赖而在本切片收敛点执行。

vendor 的严格 all-target Clippy 首次报原 BufMut/UninitSlice/set_len/needless_return 四项上游告警；默认 all-target 随后遇上游故意 reversed range 测试的默认 deny lint。原文件没有为通过 Clippy 修改；new API/library 与两个新增 test targets 的严格 Clippy 只对已核实的三类上游 lint 使用明确 `-A`，通过。consumer 的首次 `-D warnings` 被既有依赖告警拒绝，默认 all-target 命令通过。首次 vendor offline 测试因 generator/Loom 的 windows index 元数据未缓存而在依赖解析失败；锁定在线解析后通过。失败日志均保留，不表示缺少 Docker image。

尚未安装真实 FetchTaskResult 新服务或 advertise V1；此切片不证明 HTTP/Tonic/H2 完整发送出口。Tonic EncodeBody 的初始 buffer、具体 Body/last DATA alias、H2 小 DATA 的独立 writer buffer、pre-admission lane/stream holder 仍需真实接线。Statistics 最后 materializer 的增长前 permit 仍继续。P05–P10、最终同 SHA native 1FE+3BE 功能/完整 CI 尚未完成；Linux 正式性能测试按用户指示由其后续手动执行。
