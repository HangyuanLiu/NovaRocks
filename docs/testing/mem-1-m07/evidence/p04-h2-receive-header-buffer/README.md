# P04 fixed encoded input Vec/Core：独立物理退出证据

探针逐字节复制当前 `receive_header.rs` 与当前 patched Bytes 的完整源目录，在独立 crate 末尾追加测试；没有改写算法、产品文件或 workspace。`receive_header.rs.snapshot` 保存实际原文件；`source-sha256.json` 保存原文件、Bytes 源码和生产锁身份，最终全部与当前文件一致。测试依赖仅生产锁中的 Bytes 1.11.0，以 `--offline --locked` 运行；无需网络或工具安装。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-h2-receive-header-buffer/reproduce.py --negative --quality --miri
```

最终普通测试、字节恢复后测试和私有 Miri 各 9/9；Clippy/fmt exit 0。两个实际 scratch runtime negative 都命中具体断言并 exit 101：在实际 append 内引入临时 copy 会破坏零分配 oracle；把 Core 原 owner 字段移到 Vec 前会触发 credit 提前退出 oracle。完整 diff、失败日志与恢复 SHA 均保存。slice 逃逸另用编译反例证明回调不能返回借用输入：确实得到 lifetime error，未将这种编译失败当作 runtime negative。

验证涵盖：

- 构造确实观察 Vec 与 Core/Arc 两次分配，总 Rust-requested backing 不超过公开 conservative bound。
- System allocator 在实际 dealloc 返回后才更新 live backing 记录；原 ExitCredit 要求 encoded Vec/Core 和单独覆盖的 Bytes carrier wrapper 都已物理退出。最后 public handle 仍保留原 owner；最后退出恰好一次。
- clone、bind、逐字节 append、decode、reset 为零分配；满 capacity、较小 local limit、checked overflow 拒绝不改变已写数据/commit，reset 复用原指针。
- 非法 bind 不消耗一次绑定；四个 public clones 竞争仅产生一个 mutable lease，lease Drop 后不能重新 bind。
- inline owned 回调结果在 workspace 覆写后有效；真实 `to_vec` positive control 能被 allocator 捕获。callback panic 释放 capture，owner 仍被 lease 保留，状态可安全访问/reset。
- 原 physical-exit guard 自身 panic 时，Vec/Core/carrier backing 已先实际释放；Miri 覆盖这一退出路径和并发绑定。

`preliminary-v1.log.gz` 保存最初 8 项通过记录；`preliminary-v2.log.gz` 保存增加第 9 项后通过及一个探针样式警告；最终修正警告后的完整证据在 `reproduce.log.gz`。完整运行结果与限制见 `receipt.json`。

范围仅 encoded input Vec/Core。ExitCredit 是实际物理 Drop sentinel，不是生产 budget 钱包接线。Carrier metadata 单独计量且未加入 buffer bound；没有实际 HPACK decode、HTTP/H2 connection、Native RPC、allocator caches/RSS 或完整连接 2 MiB 包络的证明。buffer 回调 commit/retry 测试只验证实际 buffer API，HPACK 的语义由独立 actual decoder 探针验证。

主 agent 已逐项核对最终源码与原证据 manifest；完整日志/diff 无损 gzip 保存，原始与压缩 SHA/长度见 `compressed-artifacts.json`。复现脚本会重新产生普通日志。
