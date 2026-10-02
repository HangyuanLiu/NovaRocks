# P04：原授 decoded header 字段 arena 与实际协议接线

`ReceiveHeaderFieldPool` 使用固定 aggregate arena、64B量子原子位图、有限 live wrapper positions 和非等待 checkout gate。完整 checked bound 先覆盖 Core/Arc、arena、位图及所有可能同时存在的 Bytes owner wrappers；没有 `positions × max_field` 的完整buffer，也没有 std Mutex 隐含backing。每份可变长度字符串取得独占 extent；临时/已发布/retiring 都受同一位置界。fragmentation、bytes、positions 或重入拒绝没有等待/heap fallback。最后 wrapper 物理释放后 FieldExit 才清位、归还位置；最后 Core/arena/位图先于原ownership carrier退出。

实际 h2 两方向 opt-in builder 要求 fixed raw/encoded input、显式 fitting block maximum 和显式 header-list cap>=32；pool最大单字段长度覆盖该cap减32。配置矛盾在bind/I/O前拒绝。选定 FramedRead 直接构造 bounded Decoder，删除此前的临时 legacy 4096 scratch；默认None仍走原constructor。plain直接copy到已授extent；Huffman只先校验/计数，两marker的完整合计通过后才checkout，沿原状态表decode_into；NeedMore不取得extent。原default Cursor/decoded None compact路径不变。pool exhaustion通过实际codec成为连接 PROTOCOL_ERROR，不是NeedMore或等待。

Hyper双向Config/clone透传；Tonic每attempt factory原授fresh pool，并在dial前检查依赖与有效cap。attempt override优先于Endpoint explicit，再用既有Hyper16KiB默认并明确forward；继承32KiB但只有16KiB pool时正确拒绝，显式attempt16KiB可以覆盖。未给Native安装此profile。

最终 125 protocol +574 Native +313 Worker =1012 项非重复workspace测试通过。新增实际protocol target9项：h2/Hyper两方向plain/Huffman字段及defaultNone、HeaderMap clone/field aliases逃逸、实际task取消/join/IO退出、builder依赖/cap/oncebind preIO拒绝、positions连接错误、8条各自合法stream保留alias导致aggregate arena耗尽。实际Tonic Channel/factory也保留原字段owner直到最后response alias退出。真实Worker ResultRetainedBudget事前授完整raw/block/field及各ownership-carrier metadata；connection退出后先重授raw/block，字段原grant持续阻塞，最后alias退出再全额重授。HeaderMap/URI/table/executor/peer等scaffolds分别声明未授，不当whole envelope证明。既有Tonic target新增1项10配置predial/effectivecap矩阵，总12项。

四个公开生产接点 runtime negatives 均编译成功后101/testFAILED：Hyper双向漏fieldpool、Tonic漏forward、Tonic丢失Endpoint继承cap。每次finally字节恢复，完整9+12恢复通过。负向script在无并发workspace Cargo时运行；[私有arena证据](../p04-header-field-arena/README.md)另有实际普通/Miri14项及6个runtime mutants，包括plain/Huff fallback、early position、遗漏exitguard/wrapper bound和重新引入legacy scratch。

最终四vendor strict lib Clippy无warnings、Native all-target Clippy/workspace all-target check（既有warnings）/root及touched vendor fmt、diff/staged checks通过。初始Hyperclient fixture把response写在lazy request future尚未poll时，造成Canceled；改为peer读取准确preface及已发HEADERS/CONT END_HEADERS后才注入，无sleep。首次产品check因6个新API缺doc被拒绝，修完整doc；第一次负向needle误用pool而非pool.clone，在mutation前被拒绝；首次新增test unused imports警告删除后复跑全部最终checks。全部历史及完整最终日志/diffs用确定性lossless gzip保存，raw/gzip SHA及长度见lossless-artifacts.json，183源码/配置输入见source-sha256.json。原stdMutex实现从未作为原授allocator证明；审查其隐含backing后用原子位图替换，实际完整峰值由私有probe核对。

本slice仅raw/encoded/decoded-field owners；dynamic table真实容器、HTTP HeaderMap三backing/clone/iterator、Method/Scheme/Status/metadata、所有框架stream/socket/task/TLS以及完整2MiB连接/Native lane/predecode/deadline仍open。P04 executing/P05–P10 open，V1未advertise，完整1FE+3BE/SQL/system/性能未接受。desktop-linux fixture全BOM再次live通过，无缺image/JAR、未pull或改全局context。Linux正式测试按用户后续手动执行。无push/PR/archive。
