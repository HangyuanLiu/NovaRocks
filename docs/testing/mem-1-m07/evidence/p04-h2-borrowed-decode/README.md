# P04 borrowed frame decode：实际源码独立证据

本证据复制完整当前 `vendor/h2-0.4.12` 为普通依赖，在其实际 `codec/framed_read.rs` 末尾追加测试入口，调用同一个 `decode_frame_input`。没有重建 decoder 模型或替换算法；普通依赖不编译上游 `cfg(test)` fixture。四份 `*.helper.diff.gz` 保存全部额外入口和导出；`wrapper-helper.rs` 使用真实私有 `PoolBuffer` / `BufferExit` 与 Bytes 的 Layout getter，未猜测包装大小。

复现命令（仓库根目录；仅使用缓存和已安装工具）：

```bash
python3 docs/testing/mem-1-m07/evidence/p04-h2-borrowed-decode/reproduce.py --negative-owned-copy --quality --miri
```

脚本逐项核对依赖的 name/version/source/checksum 与生产锁一致，以 `--offline --locked` 运行独立临时 workspace。Miri 只运行这 9 个新 driver 测试，未运行上游测试或真实 I/O，也未安装工具。`source-sha256.json` 保存追加 helper 前完整 h2 Rust 源码与 manifest 的 57 项 SHA；最终核实全部与当前产品源码一致。`dependency-source-sha256.json` 另存最终 Bytes 源码和生产锁身份快照。

| 检查 | 结果 | 完整证据 |
| --- | --- | --- |
| 实际 decoder 普通测试 | 9/9，exit 0 | `ordinary.log.gz` |
| scratch Borrowed→Owned 负向 | 实际运行 assertion 失败，exit 101 | `owned-copy.diff.gz`、`owned-copy-mutant.log.gz` |
| 字节级恢复后普通测试 | 9/9，exit 0 | `restore-sha256.json`、`restored-ordinary.log.gz` |
| 独立 Clippy / helper、driver fmt | 全部 exit 0；Clippy 未启用 `-D warnings` | `clippy.log.gz`、`helper-fmt.log.gz`、`driver-fmt.log.gz` |
| 实际 decoder 私有 Miri | 9/9，exit 0 | `miri-private-driver.log.gz` |

System allocator 的计量起点在真实 `FrameInput` 构造前，终点在实际 decoder 返回后且实际 `Frame` 仍持有时。测试 wire、decoder、pool 和 HPACK 构造在计量外；格式化结果、摘要分配和 metadata 读取在计量结束后。

- SETTINGS / PING / WINDOW_UPDATE / RST_STREAM / PRIORITY / Unknown 的 Borrowed 路径为 0 allocation、0 reallocation。对应 Owned positive control 在同一计量区内复制完整 raw frame，观察到 1 allocation，分别请求 15 / 17 / 13 / 13 / 14 / 4105 B；输出与 Borrowed 一致，证明 allocator oracle 能检测复制。
- 合法 DATA（含 PADDED `Some(0)` 和非零 padding、EOS）与非空 GOAWAY debug 各仅 1 allocation / 72 B；72 B 由实际包装类型 Layout getter 核对。原 wire 修改/释放、decoder 释放后逃逸 Bytes 仍有效，pool 直到最终 alias 实际 Drop 才重新可用。
- DATA stream 0、空 PADDED、非法 padding 在 checkout/copy 前拒绝且 0 allocation；空/非法 GOAWAY 不占池，池满明确 `ENHANCE_YOUR_CALM`。partial HEADERS 时 PING、Unknown、DATA 都在 shared gate 拒绝且 0 allocation；Priority self-dependency 保留 stream Reset 错误作用域。
- HEADERS / CONTINUATION 的 owned fallback 为明确 positive control，分别观察到 1 allocation / 9 B 与 2 allocations / 49 B，没有声称这两个路径无分配。

首次 helper 缺少 `Debug` 导致实际 h2 lint 编译失败（`first-compile-failure.log.gz`），修正只涉及新 helper。中间已复制快照运行全部通过（`intermediate-v2.log.gz`）；最终在产品恢复后重新 fresh-copy 的完整运行见 `reproduce.log.gz`。负向仅在 scratch helper 将输入切换为 Owned，要求测试确实运行并命中 allocation assertion，不能将任意编译 exit 101 当作有效反例；恢复 SHA 相同后重跑全部 9 项。

此证据仅覆盖私有实际 decoder seam。它不验证生产 fixed dispatch 安装、HTTP/H2 connection future、完整 Native RPC 或真实网络。Pool 使用 `Bytes::new()` 作为原 carrier，未证明 funding/credit authority；raw reader、固定 Vec/Core、pool 和 HPACK 构造均未计量。HEADERS/CONTINUATION、HeaderMap 与 HPACK owner、整个 connection 的 2 MiB 包络仍由其他切片完成。

完整日志/diff 以无损 gzip 保存；`compressed-artifacts.json` 保存压缩前后 SHA 和长度，主 agent 收拢前核对完整 source/evidence manifest。复现脚本仍在临时目录产生未压缩的原日志。
