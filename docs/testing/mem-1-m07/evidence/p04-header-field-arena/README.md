# P04：Header field arena 的原 backing 独立证据

完整当前 h2 0.4.12、http 1.4.0、bytes 1.11.0 复制为不可变普通依赖；scratch 只追加薄 opaque helper，调用真实 ReceiveHeaderFieldPool.try_fill/bind、实际 PoolField/FieldExit metadata getter，以及真实 Decoder::new_bounded/BorrowedSource/decode_source/Huffman/dynamic Table。没有复制 allocator/HPACK 算法，也不编译上游 cfg(test) fixtures。source-sha256.json 的129个 Rust/manifest文件最终与产品全部吻合；18个最小依赖的版本/source/checksum全与production lock一致。

最终普通14/14、恢复后14/14、默认fresh recipe独立重放14/14、nightly Miri14/14（24.27s）；isolated Clippy与两个fmt检查均exit0，无warnings。首轮13项全部成功的source/helper/driver/recipe与完整日志单独保留在historical-13；新增第14项补验真实constructor接点，然后完整重放。没有初始编译/运行失败被隐藏；日志中fill callback panic是catch_unwind测试的预期行为。

| 真实观察 | 实际证据 |
| --- | --- |
| 完整原授上界 | 小几何capacity256、positions4、max_field128：arena256B、bitmap4B、Core/Arc144B，固定3alloc；4×actual wrapper56B，完整峰值628B≤公开checked bound644B。无realloc；caller ownership carrier另列，不计入pool公开bound |
| constructor事前不分配scratch | actual new_bounded Some/None分别0alloc/0realloc；actual Decoder::new(4096)阳性1alloc/4096B，验证allocator未盲测 |
| 真正退出后复用 | allocator在目标wrapper实际System.dealloc之前只记录atomic available状态，allocator外断言；slot仍不可用。之后才可重用原arena pointer，不在GlobalAlloc中panic |
| aggregate动态extent | 64B量子占满后交错释放：虽有足够总free bytes，128B无连续extent时明确拒绝且0fallback alloc；邻接extent最终退出后同pointer复用，其余live数据保持 |
| escaping alias的原owner | public pool/bound handle、decoder退出后，真实Bytes slice/clone和实际Table/index62 Header clone仍保持original backing；最后真实owner退出后才释放原ownership哨兵 |
| empty/failure/unwind/reentry | empty执行callback但无slot/heap；callback error/panic回收实际claim；nonempty reentrant/concurrent checkout明确Exhausted，不等待。跨线程aliases最终退出后原owner回收 |
| actual pooled string decode | plain/Huffman都仅2×56B真实wrapper，没有独立字符串副本；每个fragment NeedMore都0checkout。combined decoded name+value+32在checkout前检查；exhaustion不是NeedMore，部分name已领取时也正确回收 |
| optional None兼容 | 用实际new_bounded(None)作为compact heap对照，合法/非法name与Huffman错误类别、输出一致；不把这些默认独立heap副本算进arena证明 |

六个scratch实际接点负向均成功编译后runtime FAILED/101并命中指定断言：

1. new_bounded恢复legacy4096 scratch（Some/None合计2alloc）；
2. plain consume绕过pool改copy；
3. pooled Huffman改为heap destination；
4. 把bitmap/position发布从FieldExit提前到PoolField.Drop；
5. 遗漏Bytes physical-exit guard；
6. capacity bound漏算所有live wrappers。

前两种字符串旁路仍可能只有两次分配，因而必须验证真实requested layout总额是2×wrapper56B，不能只看allocation count。全部mutant完整diff/log、before/restored byte hashes保留；finally字节恢复后完整14项通过，compile failure不计作负向。

复现，仅创建自己的scratch根：

```sh
python3 docs/testing/mem-1-m07/evidence/p04-header-field-arena/reproduce.py --negative-commit --quality --miri
```

每个Cargo命令明确独立manifest/target，并使用offline/locked。recipe检查精确source identity，源变化时拒绝冒用旧证据。Miri只使用已经安装的nightly，不安装、不访问网络、不操作生产target。snapshot-path.txt指向原运行根；probe-Cargo.toml记录该运行实例，recipe生成新路径可独立重放。

边界：ExitCredit是实际物理退出观察，不是产品钱包；调用方必须先获得公开bound的原授，ownership carrier自身另授。本目录确认固定arena/bitmap/Core、有限wrapper positions与实际bounded decoder使用该arena；不证明HeaderMap/输出Vec/Table容器、Method/Scheme/Status等pseudo metadata、泛用HTTP/Tonic复制、实际Native安装或完整连接2MiB包络。对Darwin Mutex隐含heap的初始源码疑点由主agent在探针snapshot之前改为Atomic版；没有运行旧Mutex版，所以不声称已有旧版反证。

完整 `.log`/`.diff` 以lossless gzip封存，正文原日志名对应同名 `.gz`；lossless-artifacts.json记录原始/压缩SHA和长度。重放recipe仍生成新的原始日志，产品源未变化。
