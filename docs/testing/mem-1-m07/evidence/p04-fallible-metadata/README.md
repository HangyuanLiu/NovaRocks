# P04 metadata 可失败复制、合并和协议增补

本切片闭合实际 HTTP/Hyper/Tonic metadata 容器的容量错误与原授接管路径。HeaderMap 新增 grouped `try_extend`，保持 source 同名值组替换及 duplicate 顺序，固定 map 不按 size_hint 提前 reserve；`try_extend_map` 在普通目标＋funded source 时直接接管 source 的原位置，只将不冲突旧值组通过 Entry 移入，不 clone HeaderName、不新增 map、不 heap fallback。失败保留已应用前缀，消费方必须丢弃后再返回错误。

Tonic Status/MetadataMap 增加可失败复制；接收 grpc-status 先 claim 原 copy position，再解码 message/details，容量不足返回 ResourceExhausted。非法 details base64 返回 Internal，释放已取得 copy，不再 expect panic。client unary 错误分支移交原 initial metadata，client/server 合并保持 source group 优先；Streaming 合并失败清部分 trailers 并进入终止状态，不允许后续 trailers fastpath 或重复 poll 再发布前缀。Status borrowed add/serialize 保持 funded family；Status.into_http 消费原 metadata，不需要额外 copy position。Status/server 响应协议增补失败保留原 map，清除部分 headers，以 HTTP500＋单次 body error 结束，不递归重建普通错误 map。

Hyper client 的自动 Content-Length、server 的自动 Date/Content-Length 使用 try_entry/try_insert，沿已有 callback/error/reset 路径拒绝容量不足。真实测试把收到的 funded map 移作下一 POST 或响应；拒绝后下一请求仍成功，且不烧额外 stream。删除了已无调用的旧不可失败 helper，client-only/server-only 构建保持可用。

验证：21 个实际协议 targets 共172项，通过；其中公开 HTTP/System allocator 原13（新增4）、Hyper真实wire3（12个场景）、Tonic公开/真实Grpc/Streaming14。更新后的实际 HTTP/Bytes 全部 normal production source 普通/Miri各10，包含 owned/grouped merge 与失败后的 IntoIter 退出；不将 Miri 外推到完整 Hyper/Tonic。5个 actual-source runtime negatives 均101/test FAILED，覆盖 Status infallible copy、丢失原 family、partial trailers escape、server未授fallback、Hyper infallible Date；全部文件 finally 字节精确恢复，恢复后三个相关target30通过。最后测试 helper 清理后Tonic14再通过，新增targets Clippy 无 warnings。

HTTP/h2/Hyper/Tonic lib strictClippy0；Native三个相关targets Clippy0（依赖/现有lib warnings仍有）、fmt/diffcheck0。独立正常依赖 probe 验证 Hyper client/http2、Hyper server/http2、Tonic channel-only 均编译成功，63个依赖身份与 production lock 一致；probe 自身 Clippy0，不声称对每个 dependency 的最小feature配置分别strictClippy。首次直接向非workspace dependency请求features的Cargo语法失败保留；改为独立probe，没有修改production lock或下载输入。

独立审查修正了 partial trailers 发布、普通目标丢原family、into_http unwrap 和 server t! fallback；allocator 首回放捕获 custom HeaderName clone 的24字节共享控制块，改用Entry移交后零新增分配通过。另首次fixture extra=0与已有测试helper“三数组都非空”的前提冲突，修为extra=1；extra=0产品行为仍由原有公开/协议/私有测试覆盖。Hyper首次测试handshake泛型参数多写一个，修为实际API形状。完整首次失败、最终与negative日志/diff共32个lossless gzip，SHA和字节数见log-sha256。

`reproduce.py --miri` 从当前repo精确 source snapshot 复制实际HTTP/Bytes及3个生产依赖，不用替代算法或启用upstream cfg(test)。`reproduce_features.py` 从完整5个vendor262 source pins校验后，在独立workspace离线构建3个最小feature配置并核对production依赖身份。`run_regressions.py` 串行修改实际repo源码、要求运行时失败并 finally 恢复；执行期间不要并行workspace Cargo。

范围仅metadata容器/原位置与可失败框架路径。Status message/details 的独立解码/编码 String/Bytes、字段共享控制块、自动Date/数字CL payload、普通新Status/response、body/error/task/socket/TLS/绝对deadline及完整2MiB connection包络仍有后续原授工作。Native listener/client/profile/lane/predecode尚未安装，P04 executing、P05–P10 open、V1未advertise，不是完整Native1FE+3BE/SQL/system/性能验收。Linux按用户后续手动执行；本轮未更换Docker context或下载镜像。无push/PR/archive，goal继续active。
