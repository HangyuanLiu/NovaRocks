# P04：独立 GOAWAY debug backing 收据

相对 parent `87a6d4d5b9cbba0e30caee2df6d39e4415515bed`，h2/Hyper 两端增加默认 None 的 `receive_goaway_buffer_pool`。P04 仍 executing，P05–P10 未关闭，V1 未 advertise。Linux 正式测试由用户后续手动执行；本切片不是 Native/fullSQL/system/performance 验收。

## 原 owner 与拒绝行为

重复合法 GOAWAY（相同 last stream ID 不增长）可以通过每次 send_request 返回的公开错误保留多份不同 debug backing；stream数量限制不能限制这些副本。独立 pool 在第一次 debug copy 前取得位置，不能借 DATA pool 的全帧等待，也不能等待自己当前错误持有的槽。

caller先取得完整现有 ReceiveBufferPool block/Core/Arc/最大wrapper bound与另授carrier metadata，再供原信用。pool一次绑定一条物理connection；Data与GOAWAY用同一pool在第二次bind、握手I/O前拒绝。debug精确保留，不截断、不丢弃；empty debug没有独立backing，不占槽；固定payload不足8字节按原PROTOCOL_ERROR拒绝，不先copy。

实际原 PoolBuffer/RETIRING/wrapper dealloc/BufferExit 退出链不变；slot只能在最后alias与其wrapper物理退出后复用，原poolcredit保持到最后所有实际backing退出。新增try_copy_payload先检查available，再唯一parser CAS/扣槽，保护FREE已发布但count未增加的窗口。满池返回None，没有新payload/wrapper分配。

拒绝产生本地ENHANCE_YOUR_CALM，并沿原连接错误处理发送GOAWAY。既有take_error错误优先级不变：先前非NO_ERROR远端GOAWAY可能仍作为公开最终错误。真实wire案例确认本地拒绝已发出而public future仍返回remote PROTOCOL_ERROR与最新原诊断。既有DATA gate、writer flush仍可Pending；本切片只证明不等待GOAWAY池，不证明deadline或阻塞write下实际connection退出。

## 验证

新production-lock protocol9项：pool2保留不同诊断后的第三次copy明确拒绝；同debug多个公开error共享一slot，conn/builder/pool退出后escaped alias仍持真实Worker ResultRetainedBudget原grant；pool3连续64次替换/复用；empty64次不占槽且error保留不钉pool；默认None保留64次原行为；malformed长度0..7；geometry/rebind客户端preface前失败与server复用拒绝；cloned Hyper两方向保留h2 error source和同原credit；先前remote错误优先级与实际出站local拒绝。raw SETTINGS ACK是受控wire fixture，不是Native真实握手时序证明。

恢复后新9+原43=52协议通过；Native lib574+Worker313=939非重复workspace tests，重复运行不累加。隔离当前原样receive_pool源+patchedBytes，atomic-waker1.1.2精确锁/bytes1.11.0，普通9+Miri9通过：旧6 actualSystem allocator/physicalwrapper/owner/wakepanic/concurrentalias检查，加full拒绝无分配、最后alias退出复用同block、FREE/count0保护3项。count0窗口是人为设置真实source状态，不宣称完整race交错证明；wake panic有预期catch。

```bash
cargo test -p novarocks-native-adapter --test native_h2_goaway_backing --offline -- --test-threads=1
python3 docs/testing/mem-1-m07/evidence/p04-h2-goaway-backing/reproduce.py --miri
```

脚本不安装/下载，nightly/Miri/rust-src必须已存在。allocator探针只覆盖pool blocks/Core/wrappers，carrier metadata、I/O/task/stream、公开error箱体、allocator缓存/RSS另计；integration用真实Worker原预授，不新增钱包。

四反例完整diff/log及byteexact restore在index：满池fallback普通copy使第三次仍Pending；Hyper两端漏转发使slot/原owner oracle失败；删除available先验时FREE/count0真实source分支触发previous>0 assert。四项不能统一记为普通test FAILED：count0例在首个下溢断言后，allocator探针也记录了panic诊断分配，cleanup发生第二panic并SIGABRT，cargo101；不把次生退出断言当作正常路径所有权缺陷。初次runner错误要求此abort也必须有test FAILED字符串，runner自身拒绝后已修正分类，完整runner历史保留。其余三个有实际test FAILED/cargo101。恢复9普通/9Miri及52协议重跑通过。

h2/Hyper及隔离probe lib strictClippy零warning；Native正常all-target Clippy与workspace all-target check通过，保留既有warnings。额外Native target strictClippy被17个依赖告警阻断，不称strict目标通过、不改无关baseline。root/vendor/probe定向fmt、diff和实际文档目录扫描通过。公开vendor API/共享codec影响触发wave workspace check，产品尚未安装不提前用全量SQL/system冒充验收。h2原66/Hyper原68文件、crate checksum已live核对；source/log/gzip/mutant hash和完整命令结果见 [index.json](index.json)。独立上游dev suite未运行。

## 继续工作

pool未安装Native listener/FE Tonic Channel。BE真实入口直接Hyper，FE/BE outbound Tonic私有builder需要每次重连的factory，不能复用once-bound pool。frame copy、HTTP HeaderMap/duplicate/capacity/carrier、HPACKtable spare、writer/stream/task/socket/TLS、完整2MiB connection包络、分离lane/listener/predecode与FE整窗接管继续。每个error的箱体数量和实际退出仍由Native有限调用/尾部owner闭合。Docker desktop-linux全部fixture BOM通过；无缺镜像/JAR。
