# P04：固定 H2 writer 与本地出站帧边界

相对 parent `b78fcfa1c2f0bf16146e0c0fca7bf8de498b3e6f`，h2 两端增加默认 None 的 `SendFrameBuffer`，Hyper cloned Config 和 Tonic 每次物理连接 factory 透传同一原 owner。容量与最大 payload 由 caller 事前取得完整 Vec/Core/Arc bound；carrier metadata 另授。构造只分配 Core，handshake 一次绑定先于 preface/SETTINGS I/O，直接创建选中的 writer，避免临时默认 Vec。P04 仍 executing，P05–P10 未关闭，V1 未 advertise。

## 行为与所有权

固定模式在每次 append 前保留完整本地最大帧及九字节 header 的空间；实际 send maximum 为 `min(peer SETTINGS, local maximum)`，DATA、HEADERS/PUSH_PROMISE 和 CONTINUATION 共用该值。clear/flush 重用完整固定 backing。GOAWAY 的 debug 加八字节 payload 前缀在 append 前检查，超限明确拒绝，不截断。真实 GoAway caller 将拒绝传播为 `InvalidInput`，替换旧 `.expect`。

writer 的实际 encoder/buffer 先于唯一 lease 退出；最后 Core/Arc 分配先于 original carrier 退出。公开 clone 保守保留原授信，无 Weak/raw Arc 或 writer split/freeze/逃逸 alias。只有固定 writer Vec/Core 属于此授信，独立 HPACK full block/table、queued headers/DATA、HeaderMap、stream/queue/task/socket/TLS、carrier/error wrapper、allocator cache/RSS 不在此证明范围。

## 验证与复现

```bash
cargo test -p novarocks-native-adapter --test native_h2_fixed_write_buffer --test native_hyper_fixed_write_buffer --test native_tonic_connection_factory --offline -- --test-threads=1
python3 docs/testing/mem-1-m07/evidence/p04-h2-fixed-writer/reproduce.py --miri
python3 docs/testing/mem-1-m07/evidence/p04-h2-fixed-writer/goaway-reproduce.py --mutations
```

新 H2 6 与 Hyper 2 测试覆盖原 Worker `ResultRetainedBudget` 完整 grant、两端连续 64 个 distinct headers 的单帧批量和跨 CONTINUATION 批量、scalar/vectored、固定地址范围、每帧大小/END_HEADERS、peer 大 frame 设置下 DATA `16384/16384/1`、partial 1/3 byte write、重复 Pending flush、write/flush error、WriteZero、cancel 和一次绑定 I/O 前拒绝。Hyper 用 PING/PONG 和 gated Body 确认大 peer SETTINGS 已生效，abort 后等待准确 task Join 才检验释放；不把 abort 调用或逻辑 EOF 当实际退出。默认 None 保留 peer 大帧路径。地址 oracle 只证明观察到的 write backing 没有 relocation/增长，真实物理退出由独立 allocator probe 证明。

当前实际 `SendFrameBuffer` source 的普通 6/Miri 6 和真实公共 h2 writer System allocator 3 通过。实际 Core 72B 加选中的 Vec 65536B 共 65608B，低于 bound 65624B；两者的 `System.dealloc` 均先于 original Bytes guard Drop。构造只分配 Core，clone/bind 不分配第二 Vec，复用拒绝先于第二次 preface/64KiB 分配。该 probe 的原 carrier 使用 paired patched Bytes/counter witness；实际协议测试另外消费真实 Worker wallet，不把两者混称。

公共 h2 API 不提供 arbitrary GOAWAY debug producer。隔离 current-source 副本只追加内部 helper，执行真实 Codec/GoAway/encoder 路径：debug 0/16376 保留准确 wire，16377 返回 `InvalidInput` 和实际 `UserError::PayloadTooBig`，零 wire。它是私有路径 wire/limit oracle，不称 public RPC、部署或完整连接 deadline 证明。共享 checked-in writer lock 和全部依赖 identity 与 production 对应，offline/locked，无上游 dev dependency、下载或工具安装。

五类实际负例及 byteexact 恢复证据包括：Hyper 两端各漏转发（实际 DATA 变为 32769 单帧）、恢复旧 writer 最小剩余容量（实际固定地址/范围 oracle）、GOAWAY 去 local cap、caller 恢复 `.expect`。每类要求执行后的 test FAILED/cargo101，编译失败不算负例；GOAWAY mutants 只发生在 scratch source。完整 diff/log/hash 见 [index.json](index.json)。

新 8 + 原协议 60 = 68，Native lib 574 + Worker lib 313，共 955 非重复 workspace tests 通过。恢复后的新 target 单独重跑通过。公开 vendor API/共享 codec 的 wave 收敛触发 workspace all-target check；h2/Hyper/Tonic strict lib Clippy、Native 普通 all-target Clippy、root/vendor/probe fmt、diff 通过。Native/workspace 既有 warning 保留，不声称全 workspace strict Clippy。独立 upstream dev suites 未运行。

首次文档注释附着错误导致 missing docs 编译失败，已复原正确方法注释；probe 首次锁选到缓存非 production identity，在 writer 编译前拒绝，后按 production 精确版本固定。H2 harness 首次 budget receiver 编译错误和重复 indexed header 未达到 wire-total oracle 的失败均保留。后续增强测试的 mutability/Pong unit/missing helper argument 编译错误、never-loop Clippy 拒绝，以及 scratch GOAWAY workspace membership/locked setup 拒绝都保留原始日志，不计实际 negative 或最终通过。最终 header 两组同时保留，不能用 continuation 的强制 flush 掩盖单帧批量的最小剩余容量漏洞。

## 后续边界

本 slice 未安装 Native FE/BE client 或 BE listener，也未宣称完整 2MiB connection envelope。实际 HeaderMap 三个 backing/duplicate/clone/iterator、HPACK table spare/whole encoded blocks、continuation/frame 独立 copy、queue/event/stream/task/socket/TLS、lane/predecode/admission/deadline 与 P05–P10 继续。已有逻辑 header/frame 上限不能替代这些实际 backing/holder 证明。完整 Native 1FE+3BE、SQL/system 和性能接受仍未完成。

当前 `desktop-linux` fixture BOM live 校验通过，没有缺少声明中的 Docker image/JAR；未 pull、下载或切换全局 context。Linux 正式测试按用户安排后续手动执行。本检查点仅本地实现与验证，无 push/PR/archive。
