# P04：原授入站 HPACK typed table 与实际协议接线

`ReceiveHeaderTableBuffer` 为一次绑定的原 typed backing；完整 bound 检查真实 `Option<Header>` Layout、Core/Arc，容量必须覆盖协议初始4096和所广告的 incoming ceiling。h2 两方向在 bind/I/O 前要求 field pool（继承 fixed raw/encoded、显式 fitting decoded/encoded caps）；直接交给 bounded Decoder 的 fixed ring。默认 None 仍用 VecDeque。普通字段保持独立 arena owner；HeaderMap/pseudo conversion等不包含在本 bound。

Hyper 两方向 Config/clone 透传 table owner；server 新增 incoming `header_table_size(u32)` 与既有 client 对齐。Tonic 每个 physical attempt 提供 fresh 原 table，dial前检查 field pool 和 advertised capacity，继承 Endpoint 的 decoded cap 逻辑不变；Endpoint没有incoming table设置。outbound cap与incoming广告分开。并未安装到 Native production profile。

ACK仅更新 incoming ceiling，不替 peer resize/evict。Decoder 保留最低必要缩表与最新ACK ceiling；下一 block first representation/空END拒绝缺失更新，分片整数完整成功才清义务。HEADERS/PUSH开始一次block；CONT沿原block不能在已完成字段后发送size update。必要/越界/迟到size update为连接COMPRESSION_ERROR；原字段池资源耗尽保留原PROTOCOL_ERROR。语义 malformed 的非END继续HPACK直至END才RST当前stream，不丢后续table状态。规则依据 [RFC9113 §4.3.1](https://www.rfc-editor.org/rfc/rfc9113.html#section-4.3.1) 与 [RFC7541 §4.2](https://www.rfc-editor.org/rfc/rfc7541.html#section-4.2)。

最终 **133 protocol +574 Native +313 Worker =1020** 个非重复workspace测试通过。新增 target8项：两方向actual h2 dynamic index62/63、130次insert的eviction/wrap及defaultNone；Hyper双向/Tonicfreshfactory；preACK4096/ACK0必须更新；合法整数分片与非法CONT resize；几何/依赖/独立oncebind preIO；完整malformed !END后CONT插入、下一stream引用的fixed/default双向原wire/RST；Tonic缺field/广告超cap的实际predial拒绝。准确peer读preface、SETTINGS和request HEADERS END后才注入，无sleep。全部public buffers/builders退出后，live connection仍使原Worker钱包1B授额Blocked；真实connection/task/executor join及IO退出后raw/block/table可重授，field aliases仍使独立field grant等待最后alias。HeaderMap/peer/URI/framework等scaffold不当整连接证明。

五项生产 runtime regression 全部编译后101/testFAILED并finally字节恢复：Hyper client/server遗漏table、Tonic遗漏table、Hyper server遗漏incoming广告、Tonic遗漏table predial检查；恢复8/8。[私有证据](../p04-receive-header-table/README.md)另外包含10/10普通/Miri和五项实际源码runtime regression。所有完整日志/diffs保留raw/gzip hashes，无编译失败被当作负向通过。

四vendor strict lib Clippy无warnings，Native all-target Clippy/workspace all-target check（既有warnings）及root/touched vendor fmt通过。第一次错误映射扩大到全部HPACK errors导致旧字段资源耗尽两例失败，已收窄至本切片表大小协议错误并恢复49/49相关回归；首个库并行运行的旧 `bounded_root_seal_preserves_worker_abort_before_runnable_fanout` 在create Accepted断言失败，精确单跑和串行全套574+313通过，保留首次日志，不宣称已证明具体并发根因。新增target首版unused prefix删除；Root workspace edition2024与手工edition2021的assert格式差异按实际cargo fmt修正，最终新target8项复跑。产品核心源码在这些回归间保持字节恢复。

完整来源 pins、checks/receipt 和 lossless日志见本目录。fixture全BOM在desktop-linux再次live0，无缺Dockerimage/JAR，无pull/全局context修改。完整Native1FE+3BE/SQL/system/performance尚未验收；Linux按用户手动后补，无push/PR/archive。下一步继续HeaderMap/Status等工作集与Native实际安装，approved v5终态不变。
