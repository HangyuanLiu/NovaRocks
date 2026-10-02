# P04 完整出站 HPACK block：原授 backing 与真实协议验证

新增默认 None 的 `SendHeaderBlockPool`，单槽 checked `4D+20` 编码 backing。构造前授完整 block/slot/Core/wrapper bound；clone 一次 bind，h2 两端及 Tonic 每个 attempt 明确要求 outbound table cap zero，先于 I/O/bind/dial 拒绝冲突。public receive geometry 保持原约束。

六 pseudo（含 `:protocol`）、所有 duplicate 和 name/value/32 成本在 iterator、checkout、HPACK mutation 与帧头输出前 checked 累计。实际原编码/Huffman/prefix搬移算法使用只暴露固定 Vec spare 的 BufMut；HEADERS 与 PUSH_PROMISE 共用，真实 prioritize caller 传播 InvalidInput，拒绝不 panic。未完成 CONTINUATION 持有 suffix；完整复制到独立原授 writer 后 source wrapper 物理退出回槽，pool Core/完整原 grant 继续到最后 connection/config/pool holder 退出。

实际协议85、Native lib574、Worker lib313 共 **972** 个不重复 workspace 测试通过。新协议9覆盖三帧/partial/flush pending、explicit-zero/once-bind、准确 status/duplicate/protocol、write/flush/WriteZero/cancel、Hyper两端clone及真实peer、default None；实际 Tonic10 含oversized block在首HEADERS前拒绝、fresh reconnect及pre-dial scalar/policy拒绝。严格 vendor lib Clippy、Native all-target normal Clippy、workspace all-target check、fmt/diff通过；Native基线warnings保留。

原池 actual-source System6/Miri6通过；D=16KiB 时实际请求65780B≤bound65796B，编码不重建Vec，仅checkout72B wrapper。独立实际固定encoder38通过/1上游ignored、10新Miri通过，完整全octet/最长30bit长串、prefix搬移/真实Decoder/零表/duplicate/sensitive/spare与panic范围；输入/header/decoder等在计量范围外。见相邻 `p04-h2-fixed-header-encoder/receipt.json`。共享 receive pool旧探针也按当前源码重放通过。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-h2-send-header-block/reproduce.py --miri
python3 docs/testing/mem-1-m07/evidence/p04-h2-fixed-header-encoder/reproduce.py --negative-fixed-growth --quality --miri
python3 docs/testing/mem-1-m07/evidence/p04-h2-send-header-block/negative.py
```

negative runner要求无并发workspace构建或产品写入。六生产mutation逐项真实 runtime failed/101（遗漏protocol、duplicate、panic caller、Hyper两端、Tonic forwarding），逐字恢复后19实际目标通过；独立fixed-growth mutant实际realloc1并失败101。编译错误不算negative证据。所有首轮失败、正向、negative diff/日志均lossless gzip保存并记录raw/gzip hashes。upstream缓存66/68/73原文件及crate checksum核对通过，Cargo.toml/lock与parent逐字不变。文档扫描使用现有novarocks目录，无命中exit1。

本范围仅fixed encoded block/pool/wrapper与独立writer，不代表完整HeaderMap、inbound HPACK/frame、stream/queue/socket/task/TLS/error/carrier或2MiB connection包络。Native仍未安装这些options，P04继续executing，P05–P10与V1广告/完整Native SQL/system/性能验收未关闭。desktop-linux全fixture BOM再次通过，未pull/切全局context；Linux后续由用户手动测试。只本地实现与检查点，无push/PR/archive。
