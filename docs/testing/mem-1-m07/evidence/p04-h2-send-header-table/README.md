# P04：本地出站 HPACK 表上限

相对 parent `84b0a4cb137e1951d5e3d8fa1d5f657bed522b5f`，h2/Hyper两端和Tonic per-attempt配置增加默认None的`max_send_header_table_size(u32)`，独立于入站`header_table_size`广告。每次peer设置按当前local ceiling clamp，保留原minimum/final size-update序列；fresh encoder设0发准确initial table update，保持static索引/literal和sensitive never-indexed，动态indices Vec/slots VecDeque不分配。positive值只限制逻辑大小；已用表清零会保留spare，不当作物理回收。Native尚未安装该设置，P04仍executing/P05–P10未关闭、V1未advertise。

## 真实协议与负向验证

新增H2/Hyper六项覆盖两端local0/128/defaultNone、clonedConfig、连续六个重复/不同header和真实peer解码/HPACKwire；H2用PING/PONG确认peerSETTING先于header，Hyperclient首个完整response后的后五次继续检查。client侧fixture替换真实server单个初始SETTING，保留准确ACK obligation；其decoder实际仍支持4096，观察的0/128均在其范围内，不宣称serverdecoder已配置u32MAX。defaultNone用正常4096验证dynamic index复用；大peer用于0/128，未改变既有decoder五octet拒绝u32MAX update的行为。首轮compile与额外SETTING unmatched ACK/decoder拒绝的三项fixture失败完整保留。

Tonic新增真实Channel wire测试确认fresh factory在第一HEADERS之前发0x20 initial update；fixture-onlyIO trace不计入原attempt grant。四个shared-source negative真实cargo101/testFAILED：Hyper两端漏forward、Tonic漏forward、漏initial update；finally逐字节恢复及生产lock不变，恢复六H2/Hyper+九Tonic通过。第五项是private raw-peer clamp negative，见下文。完整executed runner快照、diff/log/hash在index.json；编译失败不算runtime负例。

最终协议75、Native lib574、Worker313，共962非重复workspace tests通过；vendor三包strictlibClippy零warning、Native普通all-targetClippy/workspaceall-targetcheck（既有warnings）、root/vendorfmt和diff通过。公开vendorAPI/sharedcodec触发waveworkspace检查，未跑upstream完整standalone dev suite。新target额外strictClippy被SPI11/Native35既有lint阻断，普通targetClippy新文件无diagnostic；这些失败及首次新config字段遗漏的编译失败保留，不称严格Native通过。

## 实际源码 allocator 与 Miri

```bash
cargo test -p novarocks-native-adapter --test native_h2_send_header_table_limit --test native_tonic_connection_factory --offline -- --test-threads=1
python3 docs/testing/mem-1-m07/evidence/p04-h2-send-header-table/reproduce.py --negative-local-clamp --quality --miri
```

probe复制完整实际HPACK encoder/Table/Huffman/decoder算法，移除外部fixture/fuzz接线，保留inline upstream tests，实际frame::Error逐字抽取；附加7探针。依赖identity匹配production锁、使用pairedbytes和offline/locked，无下载/安装/root目标访问。35 passed/1 upstream ignored，raw-peer clamp仅在隔离copy移除后实际allocation assertion失败101：Darwin2次/576requestedbytes；byteexact恢复后35/1通过。Miri仅7新probe通过（另29未跑），主agent完整重放通过。isolatedClippy有原代码/fixturewarnings，fmt通过；v3仅外壳fmt失败、v4只调整隔离外壳/wiring留白，算法不改。详细独立receipt/source hash保存；原日志/差异无损gzip避免统一diff context的空白检查干扰。

实际encode计量之前建立输入HTTPHeader/Vec与64KiB输出BytesMut，只计Table/encoder增长。覆盖fresh0不分配、hugepeer/重复name/敏感value、rawpeer与localraise、queued0→64实际wire、真实decoder清掉旧index而已用表spare仍存在。None正例观察真实dynamic分配/index复用。证明不涵盖wholeHPACK block/HTTPfield原授、完整writer/connection funding；positivecap没有物理容量/owner证明。

## 后续

HeaderMap三backing/duplicate/collision grow/IntoIter/clone、HTTPname/value与TonicStatus/metadata/message多次copy、whole encoded block、inbound framecopy/continuation/table及所有lane/stream/task/socket/TLS/deadline/完整2MiBconnection包络继续。Generic HeaderMap改共享COW会影响Cell等Clone语义；仅logical16KiB header不能覆盖481小字段map实际容量。后续必须取得准确原backing/holder，不仅预授一个2MiB数字。Native listener/client安装与完整1FE+3BE/SQL/system/performance验收尚未完成。

当前desktop-linux fixture完整BOM验证通过，无缺Dockerimage/JAR，未pull或切换全局context。Linux正式测试按用户安排后续手动执行；此处仅本地检查点，无push/PR/archive。
