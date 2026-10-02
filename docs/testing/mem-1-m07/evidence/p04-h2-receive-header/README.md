# P04 原授 encoded inbound header workspace 检查点

本切片在 opt-in fixed raw reader 后安装独立的 `ReceiveHeaderBlockBuffer`。Caller 在构造前取得完整 encoded Vec/Core/Arc bound；唯一不可 clone 的 lease 只追加已去除 padding/priority/promised-id 的 HPACK payload，累计上限在 copy 前检查，不 reserve/grow。HEADERS、PUSH_PROMISE 与 CONTINUATION 在同一原 Vec 上增量解码；完成 representation 后才推进 committed offset，NeedMore 保留尚未完成的 literal。默认 None 路径继续使用 owned input，共享原 HPACK 算法与 frame metadata parser。

借用输入不逃逸；plain/Huffman decoded 字符串具有独立 owner，未归入 encoded workspace 原授。最后 strong handle 退出时先释放 Core/Arc 和 Vec，再退出原 ownership carrier。新 option 要求 fixed raw input 和显式正 block maximum，geometry 在 bind/I/O 前拒绝；Hyper 两端 clone config 与 Tonic 每次 physical attempt 完整透传。Incoming header table 的 SETTINGS/ACK 行为未改变。

本轮还修复实际协议缺陷：非法 header 后出现未完成 literal 时，原局部 malformed 状态在 NeedMore 后丢失。现在 HeaderBlock 保留该状态，继续解码以收敛 HPACK 表，最终 reset 非法 stream；默认和 fixed 两种路径的下一合法 stream 均能取得已插入的 indexed value。

最终实际 workspace 协议 108、Native lib 574、Worker lib 313，共 995 个非重复测试通过；三个 vendor 的 strict lib Clippy、Native 普通 all-target Clippy、workspace all-target check、root/touched vendor fmt 与 diff 检查通过。Native 既有 warnings 保留，两份新增/修改测试没有 Clippy diagnostic。公共 builder 与 shared codec 的 wave 收敛触发 workspace 编译检查，尚未跑最终完整 Native/SQL/system 验收。

```bash
cargo test -p novarocks-native-adapter --test native_h2_receive_header_block --test native_tonic_connection_factory
python3 docs/testing/mem-1-m07/evidence/p04-h2-receive-header-buffer/reproduce.py --negative --quality --miri
python3 docs/testing/mem-1-m07/evidence/p04-h2-borrowed-hpack/reproduce.py --negative-commit --quality --miri
```

新增实际协议 13 项包括：两端每 byte split 的 plain/Huffman/mixed block、跨 stream 动态表、eager declared-size 拒绝、累计 block 上限、padding/priority、partial cancel/wrong continuation、PUSH 策略、sticky malformed reset、两端 cloned Hyper 安装与 decoded alias 独立退出。Tonic 11 项包含 17 组 predial policy/geometry 拒绝和独立 header once-bind oracle。实际 Worker wallets 分别承担 raw/encoded 原授；bind witness 与 actual codec allocation oracle 分别验证透传和消费，不能互相代替。

实际 codec 的 allocation stress input 使用重复 table-size updates 加 static status，观察成功 poll 无独立 full-frame allocation；这是当前 decoder 接受的输入，不称 canonical/RFC-valid header。删除 codec 安装产生真实 16,393 B raw copy 并使 oracle 失败。该测试允许 small wrapper/stream 分配，不声称整个 poll 零分配。

五类生产反例均实际 runtime FAILED/cargo101，finally 逐字恢复后重跑通过：Tonic、Hyper client、Hyper server 各漏转发，h2 client 漏 codec 安装，以及 malformed 状态丢失（非法 `/must-reset` 被发布）。`negative.py --dispatch` 运行前四类，`--server-forward-only` 运行 server 类；只能在没有并发 Cargo、源码复制或产品编辑时执行。

两份独立实际源码证据各普通/恢复后/Miri 9 项通过。Fixed input probe 测得 Vec/Core 两次实际分配在保守 bound 内，clone/bind/append/decode/reset 零分配，System dealloc 完成先于原 sentinel；该 sentinel 不是生产钱包 authority。Shared HPACK 的 151 B 混合输入覆盖 152 个 split cuts；borrowed static decode 零分配，owned positive control 2 allocations/56 B，plain/Huffman compact 输出各 4 allocations/34 B 明确另计。四个 scratch runtime negatives 全部失败101，safe slice escape 是独立 compile101，未混作 runtime 证明。

全部初始失败历史保留：editor marker 部分应用诊断（单独标注非完整命令日志）、Tonic 新字段漏初始化、测试 helper 遮蔽、7 pass/2 fail 的 Tokio cooperative-budget fixture、private helper missing-docs 和 oracle/style 修正。Split fixture 仅每轮开始 yield，没有扩大 pump/忽略失败/改变产品。最后库测试首次 linker ENOSPC 尚未执行测试；仅删除本任务 51 个可重建 scratch target 目录，释放约 3.6 GiB，原源码副本和收据/日志保留，相同命令重跑 574/313 通过。

主 agent 核对 private input 22、完整 h2 58、Bytes/lock 21 项当前源码身份及两份原 evidence manifest；缓存原 upstream h2/Hyper/Tonic 66/68/73 文件与 crate checksums 已核对，生产 Cargo.toml/lock 逐字未改。完整日志、helper/mutant diff 无损 gzip 保存；原始/压缩 SHA 与长度见各 `compressed-artifacts.json`，最终源和证据 identity 见 `source-sha256.json` / `evidence-sha256.json`。执行过的初始 edit script 快照仅记录历史，不能重新应用。完整命令、失败分类和范围见 `receipt.json`。

范围仅 encoded input Vec/Core 与借用增量解码。Decoded fields、Huffman、HeaderMap、dynamic table/initial scratch、Status/metadata、stream/task/socket/TLS/deadline 和完整 2 MiB connection envelope 继续；没有实际 Native profile/lane/predecode 安装或完整 `1FE+3BE`/SQL/system/performance 接受。P04 executing、P05–P10 open、V1 未 advertise。desktop-linux fixture 全 BOM 通过，无缺 image/JAR，未 pull/切全局 context；Linux 正式测试按用户安排手动后补。只本地检查点，无 push/PR/archive。
