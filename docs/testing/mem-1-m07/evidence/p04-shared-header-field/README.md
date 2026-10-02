# P04：共享 Header 字段独立实际源码证据

本目录使用完整当前 vendored h2 0.4.12、http 1.4.0、bytes 1.11.0 的不可变副本作为普通依赖；只在 scratch `hpack/decoder.rs` 追加公开 opaque wrapper。wrapper 调用实际 `Header::new_shared`、`Name::into_entry_shared`、`Decoder::decode_source` 与实际 dynamic Table，不重写算法，不编译上游 cfg(test) fixtures。source-sha256.json 覆盖 128 个 Rust/manifest 文件；source-current-match.json 记录封存时全部吻合。HTTP 与本地原始 registry 的 Rust 源码只有 header/name.rs 不同；其他两 crate 的 UPSTREAM.json 另附。18 个最小依赖 package identity（版本/source/checksum）均与 production lock 一致。

结果：普通 10/10、默认 fresh recipe 独立重放 10/10、四个实际接点 runtime negative 均 exit101、字节恢复后 10/10、isolated Clippy/fmt exit0、安装好的 nightly Miri 10/10（189.65s）。没有网络/安装/产品更改/workspace Cargo/target 清理。

| 实际观察 | 范围 |
| --- | --- |
| custom name/value exact pointer、Header clone 零新增 alloc/realloc | 构造在已持有原 funded input Bytes 后；每份输入 Vec 及其 Bytes private wrapper 均物理 dealloc 后，原 exit-credit sentinel 才退出 |
| indexed regular-name 替换、Header clone 保留原 name 与新 value owner | `Name::into_entry_shared` 的真实调用；没有新字符串副本 |
| 实际 dynamic Table → index62 → decoder Drop → 最后输出 clone Drop | 直接向实际 Table 插入已持有 owner 的 Header，证明 retention/exit，不声称 wire ingress 已原授 |
| 标准 name 静态化 | name 原 owner 可立即退出，value owner 持续；无需保留已经没有动态 backing 的标准 name |
| HTTP lowercase validation | 256 个字节和 0/1/255/256/65535/65536 长度边界，对比旧 API；Header 伪字段/非法输入结果也与 legacy 一致 |
| borrowed plain/Huffman literal | 各 2 alloc、0 realloc、26B（12B name +14B value），只是独立 compact output；default owned positive 各 6 alloc、0 realloc、121B/114B，包括原输入 copy/shared metadata 与字段副本 |
| borrowed indexed-name literal | 1 个 compact value backing；name 复用实际动态表 |
| 混合 plain/Huffman 每个切分点 | 输出、NeedMore/error、commit 和 dynamic table 状态与默认 owned decoder 一致；跨 block 实际62/63索引也一致 |

四个 scratch-only mutants 分别改真实 shared custom-name constructor 回 copying、shared value 回 copying、BorrowedSource literal override 回默认 copying、indexed override 回默认 copying。前两个原 pointer oracle 失败，后两个实际 allocator oracle 分别 2→4 和 1→2；均成功编译后出现 `test result: FAILED` 和指定 runtime assertion。各完整 diff/log 与 before/restored hash 已保存；编译失败不算负向证明。

复现（只创建独立 scratch）：

```sh
python3 docs/testing/mem-1-m07/evidence/p04-shared-header-field/reproduce.py --negative-commit --quality --miri
```

`--miri` 只使用已经安装的 nightly 组件，不安装。所有 Cargo 命令均 `--offline --locked`，`--manifest-path`/`--target-dir` 明确指向临时根。原运行地址见 snapshot-path.txt；probe-Cargo.toml 是该次运行的实例配置，正式重放由 reproduce.py 生成新路径。

首次失败未隐藏：first-locked-preflight.log 来自 probe path dependency 未 exclude，Cargo 自动纳入 workspace 导致 lock 不符；离线 lock-generation 尝试因此含上游 dev-dependency graph，被 identity 检查拒绝，rejected-unpinned-Cargo.lock 只保存反例、从未编译。修复 scratch workspace exclude 后恢复原最小18依赖锁。first-driver-compile-failure.log 是 InvalidHeaderName 没有 PartialEq，探针改为比较成功值/失败类别。preliminary-v2 保留首轮成功普通及前两 runtime negatives；随后 harness 因 helper 中同名调用使 needle count 多1而停下，修为精确首个 product 调用，并对真正 System.realloc 路径增加计数后重放全部最终证据。

证明边界：ExitCredit 是实际物理 Drop 哨兵，不是产品钱包。借用输入 decoder 仍创建独立、尚未原授的 compact string backing；本目录不证明它已申请 connection budget。实际 Table/输出 Vec/HTTP HeaderMap、Method/Scheme/Status、URI/MetadataMap 泛用复制、最终框架/H2 元数据、whole connection 2MiB 及 Native 安装均独立未闭合。不把原输入逻辑长度或两次字符串分配当连接包络。标准 HeaderValue/Name alias 持原 owner 的接点可供后续原授安装，但新 API 本身不 mint grant。

所有完整 `.log`/`.diff` 以 lossless gzip 封存，lossless-artifacts.json 同时记录原始/压缩SHA及原长度；正文旧日志名对应同名 `.gz`。重放脚本产生新原始日志，产品输入未变化。
