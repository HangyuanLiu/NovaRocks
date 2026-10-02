# P04 receive header table：准备记录

准备阶段已结束，实际普通/Miri/回归证据见 [README](README.md)。最初只准备了完整普通依赖副本和两份薄 helper；未执行验证。原准备讨论保存在 historical-preparation.md，它的「归还 Vec」是未落地提案，不能用作实现说明。实际 lease 使用 Vec-first/owner-second field order，直接物理释放 typed Vec；没有归还或再绑定。

最终证据由主 agent 完成。实际源码固定 ring/Decoder 保留在普通依赖内；薄 wrapper 只开放已有方法和真实 Layout。公开 primitive ring 满 128 slots 是实际容器测试，不是声称它是合法 4096-byte HPACK table 占用。实际 Decoder/wire 另行按 entry 的 HPACK logical size 做驱逐。字段载体与 typed backing 分别计量。
