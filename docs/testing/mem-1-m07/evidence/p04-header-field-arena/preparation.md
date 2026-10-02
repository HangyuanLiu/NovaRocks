# P04 Header field arena：独立证据准备

历史准备记录：当时等待源码冻结，未执行；现在最终结果见README.md与receipt.json。本文件保留原准备范围，不是执行证据。

最小 scratch helper 只包装真实算法：opaque DecoderError，真实 try_fill 的公开转发，真实 once-bind 的 opaque token，以及按实际 PoolField/ExitGuard 类型调用 bytes metadata getter。现有公开 capacity_bytes、field_positions、max_field_bytes、available_positions 已足够；不公开 bitmap、extent 或私有 Core。

预定实际源码探针：

1. System allocator 分别跟踪固定 Core/Vec/bitmap、每个实际 Bytes wrapper、独立原 ownership carrier；构造/填满所有合法 positions 的峰值不超过公开 checked capacity bound。填充 closure、clone/slice 不新增副本，refusal 在 copy/增长前出现。所有 realloc 如实转发 System。
2. 64B 量子小几何测试：占满、交错释放造成碎片、拒绝不能容纳的连续 extent、相邻最终 alias 释放后复用。使用真实 pool，不复制 bitmap/allocator 算法。
3. 原 pool/connection public handles 退出后，逃逸 Header 字符串 Bytes clone/slice 仍持 original grant；最后 alias 的 wrapper 真实 dealloc 后才发布 position/extent 可用；Core 与固定 backing 退出后才归还原 ownership 哨兵。原 credit 哨兵是物理退出观察，不是产品新钱包。
4. 空串、len 上界、positions 上界、checked overflow、构造无效参数、一次绑定失败/并发绑定；具体合法范围沿冻结后的实际 API，不假定默认或补齐参数。
5. Fill closure 返回 error、panic、重入和多线程 alias Drop：未构造 wrapper 的失败不丢失 position/extent；已产生 alias 的 position 保持直到最终物理退出。重入行为按真实排他/原子协议判定，不能以测试自创锁修补产品。
6. Miri 只跑新 driver；完整当前 h2/bytes 用普通依赖，不编译上游 cfg(test) fixtures；所有 source hashes、helper diff、production lock identities、ordinary/restored/negative/quality/Miri 完整日志保留。

物理位置负向须改 scratch 实际 ExitGuard 的发布时点（提前到 owner Drop）并让 allocator 在目标 wrapper dealloc 前只记录 available 状态、在 allocator 外断言，不能在 GlobalAlloc 内 panic。另测原 grant 提前 Drop 与增长/旁路复制的实际接点，必须成功编译后 runtime oracle 失败；compile failure 不算负向证明。负向结束 finally 字节恢复，再跑同一普通测试。

范围：原授固定 arena、bitmap/Core、动态 extent 与固定 wrapper positions；其他 HeaderMap/table/Method/Scheme/Status/框架容器和完整连接包络独立未闭合。新 source 没冻结前不宣称上述测试已完成。
