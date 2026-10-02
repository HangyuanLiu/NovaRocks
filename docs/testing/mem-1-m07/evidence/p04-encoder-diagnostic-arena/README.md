# P04：编码诊断原 field arena

Parent：`56e0066aaed4a943db2acbd6a338014b4aaa702a`。approved spec/plan v5 不变；P04 executing、P05–P10 open、V1 未 advertise。Linux 测试由用户手动执行；无 push、PR 或 archive。

安装预授 trailer map 的 EncodeBody 从同一 map 取得并保留原 field arena handle。encoder/compressor 错误和长度诊断用原 formatter 准确计数、填充原 arena，构造 Shared Status message，再由原 trailer arena 生成 percent 编码输出；没有中间 owned String、复制 message 或容量失败后的堆回退。没有安装能力的既有路径保持原错误码和文本。Source Err(Status) 与此前成功 DATA 的有序行为不变。

原 raw diagnostic 与其 percent 输出在转换时共存，必须覆盖两个 extent 和 wrapper。真实 Body 测试使用 512B arena 容纳已观察到的 128B+192B 共存，随后 raw wrapper 实际退出，只剩最终 HeaderValue；fixture 几何调整不是修改产品 profile。新增 diagnostic handle 仅克隆原 Arc，没有新 backing。Root unary 的真实 RootEncodedBody Layout 自动覆盖这个新增字段。

8 个相关目标共 94 项 PASS。Body 目标现有 9 项：增强真实 encoder 错误的分配/退出验证，新增原 formatter 最大长度/位置耗尽，以及真实 finish_encoding 长度超限。成功 poll 精确允许两个原授 wrapper 及其 Rust-requested 元数据，没有 String/realloc；字节 golden 独立，长度超限还与旧路径对比完整 code/text。拒绝分支零分配、静态 ResourceExhausted、清空后的原 map 保留至实际退出；终态不发布部分 DATA、不复读 source；独立 HeaderValue alias 延续原授额。

4 个实际源码负例均编译后 runtime FAILED，finally 精确恢复：不安装 diagnostic arena、复制 formatted message、formatter 拒绝后退回 owned String、长度诊断忽略 arena。完整 diff/log 留存。Native check、HTTP/Tonic strict lib Clippy、4 个 Native target Clippy、root/vendor 格式和 diff 检查均 0，改变目标无 warning；Hyper server/http2 与 Tonic channel-only 两个最小 feature 变体均 0，63 个生产锁 dependency identities 核对一致。4 个改变 product/test pins、264 个完整 vendor source/manifests pins，结果与 lossless logs 见 `verification.json`。

独立只读审查未发现本切片阻断项。范围仍是 diagnostic backing，不覆盖既有 source Status backing、压缩/message buffers、外部 formatter 内部分配与 CPU quantum。当前 root 禁用压缩，实际压缩错误和 >4GiB 分支没有通过本次 Body 测试取得覆盖；两者接线不等于产品可达性证明。Native listener/client/profile/lane/predecode、完整 2MiB connection、body/future/task/socket/TLS/绝对 deadline 与后续 FE 整窗仍开放；没有完整 Native1FE+3BE、SQL/system 或性能验收结论。本轮采用 contract 第8节定向检查，未触发里程碑全量。

首次机械替换遇到不唯一注释 anchor，停在 formatter 可见性修改，未写入 Status/EncodeBody；`first-related` 日志仅对应该中间状态，不作为新诊断路径通过证据。修正精确 anchor 后第二轮相关测试和最终 pinned 验证通过。

复跑需使用本检查点源码，从仓库根目录执行：

```bash
python3 docs/testing/mem-1-m07/evidence/p04-encoder-diagnostic-arena/verify.py
python3 docs/testing/mem-1-m07/evidence/p04-encoder-diagnostic-arena/run_regressions.py
python3 docs/testing/mem-1-m07/evidence/p04-encoder-diagnostic-arena/reproduce_features.py
```

preflight 校验 `product-sha256.json` 与 `vendor-source-sha256.json`；feature probe 使用完整实际 vendor crate，dependency identity 与生产锁一致。没有复制简化 formatter 或另行生成算法替代品，也不把旧 Miri 证据扩写成本次完整验证。
