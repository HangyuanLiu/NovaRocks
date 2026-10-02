# P04 shared HPACK DecodeSource：独立实际源码证据

此探针复制完整当前 h2 与 patched Bytes 为普通依赖，在实际 `hpack/decoder.rs` 末尾追加薄入口，直接调用现有 `Decoder::decode_source`、`Cursor<&mut BytesMut>` 和 `BorrowedSource`。未替换算法、重建 decoder 模型或编译上游 `cfg(test)` fixture。Opaque wrapper 内保留实际 Header / DecoderError，只提供原 typed output 比较、value 借用和 error 分类，没有额外 payload 投影。三个 `*.helper.diff.gz` 保存全部入口与导出。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-h2-borrowed-hpack/reproduce.py --negative-commit --quality --miri
```

所有 normal dependency 的 name/version/source/checksum 在运行前核对生产锁，使用 `--offline --locked`。Miri 只运行这 9 个新 driver 测试，未运行上游测试、真实 I/O 或安装工具。最终在共享产品反例恢复后 fresh-copy 重放；完整 h2 Rust 源码与 manifest 的原始 SHA 保存于 `source-sha256.json`，最终逐项与当前源码核对。Bytes 的精确副本身份另存 `dependency-source-sha256.json`。

| 验证 | 结果 | 日志 |
| --- | --- | --- |
| 普通 actual decoder | 9/9，exit 0 | `ordinary.log.gz` |
| 提前 advance 更新 commit | runtime assertion 失败，exit 101 | `advance-commits-incomplete.log.gz` / `.diff.gz` |
| 成功 representation 不更新 commit | runtime assertion 失败，exit 101 | `commit-not-updated.log.gz` / `.diff.gz` |
| 字节级恢复后普通测试 | 9/9，exit 0 | `restored-ordinary.log.gz.gz`、`restore-sha256.json` |
| 独立 Clippy / helper 与 driver fmt | 全部 exit 0；未启用 `-D warnings` | `clippy.log.gz`、`helper-fmt.log.gz`、`driver-fmt.log.gz` |
| 私有 actual decoder Miri | 9/9，exit 0 | `miri-private-driver.log.gz` |

测试覆盖：

- 同一 151 B 混合 HPACK block 的每个 byte 断点，以及每次只追加一个 byte：Borrowed/Owned 输出、错误、动态表 size/count/max 和完整 representation commit 逐步一致。
- Huffman name 完整而 value 不足时，commit 仍停在前一个完整 representation；补足后只输出/插表一次，再由 dynamic index62 取得同一个实际 Header。
- 两个独立 indexed literal 后，跨 block 的 indices62/63 得到准确先后内容。
- Authority / Path / Scheme 及普通 Header 的实际 bytes 在 workspace 修改/释放、decoder Drop 后仍有效；动态表在覆写后仍返回原值。
- malformed integer、table index、uppercase name、Huffman 和 oversized 声明的错误与 Owned 对照一致。
- 现有 `can_resize` 每次 decode 调用重置的时序保持：同一次调用的 literal 后 size update 拒绝；分两次调用的同样 bytes 仍按现有行为接受。没有顺带修改该语义。

独立 allocator 测量仅在实际 decode / 输入构造之间开启，不计 fixture、decoder、wire 和结果格式化。16 个 static `:method GET` 的 Borrowed 路径为 0 allocation / 0 reallocation；Owned positive control 在同一计量区内构造实际 BytesMut 输入，观察到 2 allocations / 56 B，能检测输入 copy 与 shared metadata。Plain 与 Huffman 的 compact decoded 输出各观察到 4 allocations / 34 B，明确是独立 owner 的分配义务，没有声称它们零分配或归 encoded workspace 原授。

首次 public reexport 让旧 private Header/Error 进入文档 lint 的可达范围，导致 27 项 `missing_docs` 编译失败，保存于 `preliminary-v1.log.gz`；仅新入口改为 opaque wrapper。第二轮两个 mutant 都已真实失败，但 harness 期望的 assertion 文字与实际最先失败位置不符，完整保存于 `preliminary-v2.log.gz`；明确 final commit assertion 后第三轮全通过，见 `preliminary-v3.log.gz`。最终 fresh-copy 全部结果见 `reproduce.log.gz`。每个负向都要求测试实际运行并命中具体 assertion，不能接受任意编译 exit 101；每次 finally 字节恢复，随后重跑全部测试。

范围仅 actual shared HPACK decoder 与 source seam。没有实际 `HeaderBlock::load_source`、FramedRead protocol dispatch、网络、Native RPC 或完整连接包络的接线证明；最终 h2 source hash 包含这些文件是依赖身份快照，不能将未调用文件误称为已测试。动态表、decoded strings、HeaderMap、HTTP copies 和 funding/credit authority 仍分别验证。

主 agent 已逐项核对最终源码与原证据 manifest；完整日志/diff 无损 gzip 保存，原始与压缩 SHA/长度见 `compressed-artifacts.json`。复现脚本会重新产生普通日志。
