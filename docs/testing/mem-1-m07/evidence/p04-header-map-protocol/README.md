# P04 实际入站 HeaderMap 原授接线

本切片把 `HeaderMapAllocationPool` 接到实际 h2 FramedRead，并沿 Hyper 两端和 Tonic 每次连接配置透传。两端 builder 默认 None；启用要求 decoded field pool，因而继承固定 raw/encoded input 和明确 fitting header-list/block 几何。HTTP pool 增加 aliases 共用的 once-only connection CAS；其 Bool/Core 实际布局计入原 typed bound。所有参数和 raw/DATA/GOAWAY 容量先于第一个 binding 校验，随后 before-I/O bind；非法 frame 几何不能烧掉任一原 capability。

HEADERS/PUSH 在 frame metadata/encoded-length 校验后、HPACK 前取得一张固定 map；空 placeholder 不分配。CONT 保留原 Partial.frame 的同一 map，重复值通过 `try_append`，耗尽不 panic、等待或 heap fallback。本地 map/keys/extra shortage 关闭连接 `INTERNAL_ERROR`；真实完整 HPACK 错误保留原有 precedence，只有 shortage 与 NeedMore 同时出现才立即退出，不等后续 CONT。实际 malformed stream/HPACK/table 状态沿用此前行为。

新公开 target 9 项覆盖 actual h2 双向、Hyper 双向、Tonic factory：真实 HEADERS gate、complete task/executor abort+await 和 IO exit 后，逃逸 map、eager 原授 copy、IntoIter 及字段 alias 分阶段占原 Worker wallet；raw/block 可重新授额，metadata/field 的最后 alias 退出后才分别重授。另独立同 map pool+fresh other buffers 的第二次 handshake 证明 5 层实际 forwarding/once bind，避免靠其他已经绑定的 buffer 误通过。两端 CONT 使用 max_maps=1，key/extra/aggregate positions exhaustion 均实际 INTERNAL_ERROR；default None 回归通过。非法 frame32768/raw16384 拒绝 I/O0 后，用同一组全部原 capabilities frame16384 真实握手/请求成功。

已保存完整 19 个相关公开 protocol targets：151 项；Native lib574、Worker lib313 串行通过，合计1038项非重复 workspace tests。更新后的完整实际 HTTP/Bytes normal dependency普通/Miri各8（包括 aliases/跨线程 once CAS），只证明库与绑定 primitive，不能外推 Miri 检验了完整 h2/Hyper/Tonic。8 production runtime mutants均101/test FAILED：codec install、Hyper两端、Tonic forward/predial dependency、h2两端 prebind capacity、HPACK precedence；所有文件 finally 字节精确恢复，恢复后新target9通过。HTTP/h2/Hyper/Tonic libs严格Clippy0；Native alltargetClippy0（已有warnings）、workspace alltargetcheck0、root/vendor/driver fmt0、diffcheck0。

独立审查发现的两项产品问题已经修正并有实际 oracle：先metadata shortage掩盖非NeedMore HPACK失败，和map bind早于raw/DATA/GOAWAY capacity validation。两fields keys=1后0x20→原COMPRESSION_ERROR、0x80→原PROTOCOL_ERROR；unfinished literal且!END、不发送CONT→立即INTERNAL_ERROR。首全protocol replay发现提前校验改变旧pool诊断文本；保留DATA/GOAWAY→raw诊断及既有字符串后151通过。完整首次失败与最终日志均保留，未修改原测试期待来绕过回归。

`product-sha256.json` 固定实际修改的产品/测试文件；`vendor-source-sha256.json` 固定完整5个实际vendor源包；`source-sha256.json` 和3个生产dependency身份属于独立HTTP/Bytes普通/Miri复现。`log-sha256.json` 记录每个完整lossless gzip日志/diff的SHA及字节数。

范围仍是实际入站 metadata producer 和 transport factory 配置，Native listener/client/profile 尚未安装。Tonic Status/MetadataMap 的不可失败 clone/merge/sanitize/message 副本、Hyper增补、出站独立map、pseudo/Method、stream/socket/task/TLS/absolute deadline、完整2MiB连接与lane/predecode及FE后续P04–P10继续。不能将本切片视为完整Native/1FE+3BE/fullSQL/system/性能验收；V1未advertise。Linux由用户后续手动；desktop-linux fixture BOM live通过，无缺image/JAR、不pull/改全局context，无push/PR/archive。
