# P04 fixed inbound frame：借用原 backing 解析

fixed reader 在完整 frame 的原 Vec 上调用不可逃逸安全 slice 的同步 decoder，移除控制帧中间 BytesMut copy。DATA 验证 stream/padding 后只复制 stripped payload 到独立原授 pool，GOAWAY 沿用独立 diagnostic pool；默认 owned reader 保持既有路径。callback 前重置读取位置，panic 后不重放旧 frame。

真实 production lock 协议 94 + Native lib 574 + Worker lib 313 = 981 项通过。新协议 target 9 项覆盖两端、zero/exact/padded DATA、实际原指针/授额/alias Drop、满池与 flow-credit 独立、真实 ACK、坏 stream/padding、oversize、partial-header gate 和 Priority reset。私有 actual raw reader 普通/Miri 各 8，实际 decoder 普通/Miri 各 9；后者完整证据见相邻 `p04-h2-borrowed-decode/`。旧 raw reader 14 项普通回归通过。

```bash
cargo test -p novarocks-native-adapter --test native_h2_borrowed_frame
python3 docs/testing/mem-1-m07/evidence/p04-h2-borrowed-frame/reproduce.py --miri
python3 docs/testing/mem-1-m07/evidence/p04-h2-borrowed-decode/reproduce.py --negative-owned-copy --quality --miri
```

原 raw Vec/Core 实测 16,481 B ≤ bound 16,497 B；成功 borrowed callback 为零分配。真实 dispatch 负向将 Borrowed 改为 Owned full copy 后，控制轮次额外 6 allocations / 16,459 B，运行时测试 FAILED/101。`negative.py` 保存完整 diff/log，finally 逐字恢复后重跑 9 项通过；只能在没有并发构建、源码复制或产品编辑时运行。安全 slice 逃逸是独立编译拒绝，不算运行时反例。

严格 vendor lib Clippy、普通 Native all-target Clippy、workspace all-target check、root/touched vendor fmt、diff 检查通过；既有 warnings 保留。source provenance 核对缓存 upstream H2/Hyper/Tonic 原 66/68/73 文件与 crate checksums，当前 Cargo.toml/lock 未改。所有首次 oracle/编译/lint 诊断和最终日志无损 gzip 保存；`compressed-artifacts.json` 保存原日志与 gzip 的 SHA/长度，`source-sha256.json` 和 `evidence-sha256.json` 固定检查点内容。

HEADERS/PUSH/CONT owned fallback、无 DATA pool 的 copy 路径仍明确有分配。DATA flow/content-length 保持当前 stripped-body 语义；未声称 padding wire-credit 修正。满 DATA pool 仍在读取前挡所有帧，包括 control；独立 lane/deadline 和完整 liveness 留后续。私有 actual decoder 的 empty carrier 不证明原授；公开协议另外消费真实 Worker grants。HeaderMap/HPACK/stream/task/socket/TLS、完整 2 MiB connection、Native 安装及 P04–P10、最终 Native/SQL/system/性能验收尚未闭合。Linux 正式测试按用户要求手动后补；desktop-linux 全 BOM 已通过，不缺 image/JAR。只本地检查点，无 push/PR/archive。
