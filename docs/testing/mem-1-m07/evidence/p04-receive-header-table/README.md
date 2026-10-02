# P04：实际入站 HPACK typed table 原授证据

本切片直接使用完整普通依赖中的 h2/http/bytes 实现，不运行 upstream cfg(test) 的私有测试副本，不建立第二套 HPACK/ring。`table-helper.rs`、`decoder-helper.rs` 与 `pool-helper.rs` 只在 scratch 追加薄访问器；完整来源 130 项 SHA、18 项 production-pinned Cargo 身份和每份 helper diff 随证据保存。初始 prepare-only 副本没有执行收据，保留为历史；最终实际验证由主 agent 完成。

`ReceiveHeaderTableBuffer` 的完整 checked bound 来自 `Layout::array::<Option<actual Header>>(max_table_bytes/32)`、真实 Core/Arc。4096-byte HPACK 逻辑表对应 128 个 typed slots；本机实际 slots 为 9216 bytes，Core/Arc 为 88 bytes，两次固定 allocation 的 requested 峰值为 9304 bytes，低于完整 bound 9320 bytes，无 realloc。caller ownership carrier 独立计量。32-byte entry overhead 不能充当 Rust 物理大小；primitive 满 128 slots 的测试也不是合法 4096-byte HPACK 占用声明。

一次成功 CAS 独占取出原 Vec；公开 aliases 不访问 UnsafeCell，lease 不可 clone。实际 ring push/get/pop/wrap 不增长，满时显式拒绝。Vec-first/owner-second field order 在正常退出和 unwind 时先清 Header、物理释放 typed Vec，再释放 table owner；最终 Arc 先于 Core/original credit 退出。没有归还或再绑定 Vec，没有共享 Core 持 live Header 的循环。原字段 Bytes alias 可以越过 eviction/table Drop，独立 field arena owner 留到最后 alias；table credit 不必等待这些已独立覆盖的字段。

普通、恢复后、全新 recipe 和已安装 nightly Miri 均为 **10/10**。Miri 用时 12.19 秒；isolated Clippy、helper/driver fmt 通过且无 warnings。实际 allocator 哨兵在 System.dealloc 完成后减少 live ledger，table 原 credit 回调断言 typed Vec/Core/Arc 和 carrier wrapper 都已物理退出。独立 Header owner 的故意 Drop panic 验证剩余元素仍清理并释放 Vec，然后原 table credit 才返回。

真实 Decoder 探针同时覆盖 fixed/default table：ACK 不代替 peer eviction；最低 necessary reduction 与最新 ceiling 独立，1024→8192、8192→1024 有准确判据；已选择512再ACK1024不强制新更新；空 END 检查缺失必要缩表；整数分片 NeedMore 保留义务；CONT 不重新开放 resize；固定 Decoder 的 literal insertion 只有两个已授 field wrapper 分配，没有独立 table/scratch fallback。实际 wire/Worker wallet 接线见 [协议证据](../p04-receive-header-table-protocol/README.md)。

五项 scratch runtime regression 全部编译成功后由实际 assertion 检出：选回默认 VecDeque、遗漏 typed layout bound、table credit 先于 Vec、丢 required minimum、CONT 重置 resize。每次 finally 字节恢复；恢复10/10。五项生产 forwarding/advertisement/predial regression 另见协议证据，总10项。首次 regression needle 假定 layout.size 在一行，被执行前的唯一匹配检查拒绝；修为实际格式后从该项继续，首六项记录保留。

复现（只用已缓存依赖/已安装工具，不下载）：

```bash
python3 docs/testing/mem-1-m07/evidence/p04-receive-header-table/reproduce.py --quality --miri
python3 docs/testing/mem-1-m07/evidence/p04-receive-header-table/run_regressions.py --private <printed-probe-workspace> --output /tmp/m07-table-regressions-review
```

第二条会临时修改所列的精确任务源码并 finally 恢复；运行时不得有并发 workspace Cargo/产品编辑。所有完整日志/diffs以 lossless gzip 留存，raw/gzip SHA 和长度见 lossless-artifacts.json。只覆盖原 typed table backing；HeaderMap三块、pseudo-header conversion、Status/metadata/message、框架/socket/task/TLS、2MiB完整连接和 Native profile/lane/predecode/deadline仍 open。P04 executing，P05–P10 open，V1 未 advertise；Linux 测试由用户后续手动执行。
