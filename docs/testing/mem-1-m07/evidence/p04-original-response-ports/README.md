# P04 actual response-port 原授切片

本收据验证 parent `23ebb9093248b55aae691b7f9e29d49fc506a321` 上的独立 dirty wave，不是完整 M07 验收。Tokio 1.52.3 新增显式 owned oneshot；Tower 0.4.13 保持原版本，新增 opt-in `original-response-cells`；Tonic 同名独立 feature 由 Native 明确启用，旧 channel-only、普通 None 路径继续工作。Tower 原始 127 个文件先与本机缓存 `.crate` 逐字核验，checksum 与原锁一致。新增源码及 vendored 输入共 145 个 pins，最终验证后逐一核对未变。

Native 从同一个 StockCore、同一原 generation 能力预授真实 `Result<T::Future, ServiceError>` cell 与 Bytes 出口 wrapper，按每 logical Worker 8 位、230 代计入 startup stock，不新增钱包。owned 模式将原有 pending semaphore permit 从 Message 移到实际 cell 出口。peer 实际收到八个请求、Worker 已交出 service futures 后，第九个 readiness 仍 Pending；最后尚未 poll 的真实 Rx 退出后才放行。真实 cache 已 detach、Channel/Worker 等已退出，最后 Rx 仍挡住新 cache generation；不匹配的 Endpoint pending count 在 connector 调用前拒绝。移出 cell 的 service future 是独立 owner，其外部 backing 仍需另证。

oneshot 私有 strong-only wrapper 不导出 Weak/raw Arc。最后 `Arc::into_inner` 先释放实际 allocation，再销毁 retained value/Wakers，最后退出原能力。唯一析构先移动全部初始化资源并清 state bit，value/Waker panic 时原能力仍最后退出。Bytes 出口先释放自身 wrapper，再归还同一 permit，其 wake 期间原 generation 能力仍在。真实 System allocator 记录实际分配/释放；测试专用无分配 gate 排除地址并发复用误判。新增 unsafe 的独立同源码 Miri 14 项通过，使用 nightly `1.101.0-nightly (5c543b0b8 2026-09-29)`，不声称 Rust 1.92 Miri 或整个 Native 图通过。

最终定向 812 项通过：Native lib 744、七个实际 task/IO/Tonic targets 49、owned oneshot 14、Tower 5。独立普通 Tower、owned Tower、旧 Tonic channel 与显式 owned channel consumer 均通过；最终 production-lock identity 无 mismatch。四个 compiled runtime negative 分别观察普通 Arc Drop 提前返额、empty Buffer 丢原 owner、Message 退出提前返同 permit、真实 Tonic/Native 绕过 owned pair，均失败 101；随后逐字恢复源。Loom cfg 只验证显式拒绝，active unstable tracing 同样拒绝，不冒称模型验证。

公共依赖和 unsafe wave 的最终 Cargo-only CI `20261004-093341` PASS，824 秒：component 12080/7 ignored、server owner 173、binary smoke 4，共 12257 项通过。此前 `092827` 在 DataSketches source 前置 guard 失败，未进入 fmt/check/test：历史 reproduction 的 channel-only manifest 未启用新增 Tower 接线。修成独立 additive Tonic feature，没有修改旧收据。独立 scratch workspace/feature 编译失败及一次 optional dependency 导致的 23 个缓存版本漂移都保留；最终用产品锁恢复零漂移。编译期间 target 被外部移除造成的 link/build-script 中断也保留，不归类为产品失败或负载噪声。

最终 binary `4349e4f2c40d97d0d5cce73cf36e9991cabcfa3a0ac5097fdef15f802b00fe07` 上，本地独立 1FE+3BE 八个 System 场景与八个 SQL case 全 PASS。System 覆盖 distributed baseline、plaintext-IP/automatic-DNS/PEM-IP、outer-preflight、blocking-saturation-control、partial-body-deadline、registry-contention-control；SQL 覆盖 analytic 两项、aggregate 四项、iceberg-dml 两项。System 仅保存 safe allowlist 投影和原 artifact 路径/hash，不复制运行 config/JWT/private keys。automatic-DNS 兼容通过不等于 bounded DNS ownership 裁决。

实际 geometry 重新查询：response cell+wrapper 200B，8×230 共 368000B；Worker 640B、driver 1152B、server task 10432B。process stock 3590453850B 通过冻结 4GiB 检查；physical connection 6672053B 是已查询子图，不证明独立完整 2MiB ceiling。queue/MPSC blocks、Semaphore、Handle、readiness、error payload、外部 service future/body、DNS/TLS/socket/auth/shared runtime 仍需各自闭合。P04 executing、P05–P10 open、V1 None，Scalar Host gate 仍闭合。未声称 full SQL/default System、Linux/release 性能或最终 M07 完成；Linux 测试由用户后续执行。未 push、PR 或 archive。
