# P04 Status 的原字段 arena 转换

本切片把已有 h2 decoded field arena 移到 HTTP 中立 `HeaderFieldAllocationPool`，h2 继续保留 `ReceiveHeaderFieldPool` API、几何与 DecoderError 映射。没有重建第二份 arena：实际 client/server 握手把同一个原授 field capability 附到 HeaderMap family。map/iterator/escaped Status 即使已移除字段，也会持有这份转换能力，直到最后持有者实际退出。Core/Arc/bitmap/arena/最大同时存活 Bytes owner wrapper 的 requested allocation 由实际类型 layout 计算；HeaderMap 新增 inline OnceLock 后亦由其真实 Core layout 计入原 metadata bound。

安装 field capability 的 Status 解码与编码直接在该 arena 的 disjoint extent 写入。message 按原 percent decoder 双遍计数/写入，UTF-8 失败的原诊断以 bounded formatter 写入；details 使用实际 STANDARD/NO_PAD base64 slice API，无 String/Vec/Cow 全值副本。合法 details 的准确长度通过末尾至多两字节 padding 计数获得，字母表、padding 与 trailing-bit 验证仍由原 decoder 完成。空值不取得 position。容量/碎片/position/非等待 checkout 不足均失败，不创建普通 payload fallback；field 与 metadata 容量错误均用静态文本。

普通/default 库用户未安装 field capability 时保留原行为。Status::new/with_details 的普通调用者输入、Status/错误包装器本身及其他 framework owner 尚未闭合完整 Native profile；此默认 API 兼容路径不是已安装 capability 的拒绝 fallback。借用 Status try_clone 保留同一个原字段 backing；Status 独立保留 arena capability，以便 metadata_mut 替换原 map 后仍可编码。输出 HeaderValue 持有原 Bytes owner，最后别名退出才释放字段位置与原授额度。

保持原依赖语义：percent 编码不额外编码 `%`/`+`，非法 percent escape 保留原文本，非法 UTF-8 返回原 Unknown 诊断；base64 Zg/Zg=/Zg== 均接受，非法 trailing bits、alphabet、padding 返回 Internal。不是另写一套严格 padding parser。

实际 HTTP/Bytes normal production source 的 23 个 private probes 普通/Miri 各23通过，覆盖 eager metadata copy/merge、原字段 aliases、fragmentation、callback typed error/unwind、reentrant refusal、并发 once bind、map/owning iterator capability 与最后原 owner 退出。Miri 范围仅实际 HTTP/Bytes + 3 个生产依赖，不外推完整 Hyper/Tonic。最后恢复源码后22个相关协议 targets 共181项通过（新增9项公开 Tonic/Worker/System/真实 Grpc，已有 h2 map target 加固两端和框架的原 capability 身份 oracle）。独立最小 Hyper client/http2、server/http2 与 Tonic channel-only 构建成功，63个依赖身份与 production lock 一致；四个 vendor libs strict Clippy 通过。

公开9项独立期望覆盖默认与安装路径、exact64B解码、percent原quirk、base64 permissive padding、UTF-8 原诊断、copy/response/独立 HeaderValue 别名、位置/aggregate/展开拒绝、满metadata位置拒绝零分配；真实 Endpoint factory→h2 raw peer→Hyper→Tonic Grpc.unary 在所有配置/Status/maps/实际IO和executor退出后仍由输出字段持原grant。System探针验证constructor真实requested layouts以及所记录backing/Core/carrier物理退出先于credit。

8个actual-source runtime negatives均编译后101/test FAILED，分别覆盖client/server丢capability、Status普通解码fallback、message/details编码未授副本、字段位置提前退休、metadata拒绝String、details quantum过估。原文件全部finally字节精确恢复。首轮第6个mutation因unused exit触发HTTP deny(warnings)，改为显式drop(exit)才计运行时失败；最后一个anchor因rustfmt换行不匹配在写入前拒绝，修准确anchor后运行时失败。脚本新增全部source/anchor前置核验及--start-at续跑，不重跑已取得的负例。所有首错/最终/恢复/negative日志和diff完整保留。

Native两相关target Clippy0（既有依赖/lib warnings，含变大的Status使原large-error诊断尺寸变化；两个测试target无warnings），root/vendor/driver fmt及diffcheck0。首次Native check泛型推断/Message类型错误、首次新target预算helper遗漏&Arc receiver均已修并保存原日志，不替换产品语义或独立期望。`reproduce.py --miri` 校验67个 HTTP/Bytes源文件快照，复制全部实际 normal sources；`reproduce_features.py` 校验5个vendor共264文件，并使用生产 lock 身份。无需下载或替代算法。

范围为原 arena 转换能力和 Status payload 副本。pseudo Method/Scheme 转换、自动协议 header payload、普通新 Status/response、body/error/task/socket/TLS、绝对 deadline 和完整2MiB connection包络仍待后续原授闭合；Native listener/client/profile/lane/predecode尚未安装，P04 executing、P05–P10 open、V1未advertise。不是完整Native1FE+3BE/SQL/system/性能验收。Linux由用户后续手动执行，本切片显式desktop-linux完整fixture BOM再次live verify0，image/JAR全部具备；没有更换全局Docker context或下载镜像。无push/PR/archive，goal继续active。
