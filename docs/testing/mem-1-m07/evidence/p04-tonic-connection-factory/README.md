# P04：Tonic 每次物理连接的配置工厂

相对 parent `5ffabd042b373c24f0f9e4481127e194a73f468c`，锁定并 vendor Tonic 0.12.3，增加默认 None 的 `Endpoint::http2_connection_factory`。每次实际连接尝试（含同 Channel reconnect）取得独立的非 Clone `Http2ConnectionConfig`，将新 raw input、DATA、GOAWAY backing 及 frame/header/event/send 配置交给原 Hyper builder。版本和依赖版本不变；channel feature 启用原有 optional h2。P04 仍 executing，P05–P10 未关闭，V1 未 advertise。

## 所有权与拒绝

factory 先取得原信用并构造配置；scalar 与每类独立 backing geometry 在 builder mutation 和 `connector.call` 前检查。拒绝不创建 dial future，返回的配置随原 backing 实际退出。成功配置移动进局部 fresh builder，由真实 attempt future 持有；dial failure/cancel 退出后释放，握手成功后继续由 connection 与逃逸 DATA/error aliases 持有。Endpoint 克隆共享 factory，不保存或重复使用一次输出。旧连接退出而旧 DATA alias 仍活着时，下一次 reconnect 必须取得新的独立 grant。

once-bound reuse 由 h2 bind 检查，发生在取得 IO 后、首个 preface/SETTINGS I/O 前；本 API 不把它称为 pre-dial 拒绝。custom connector 的 `poll_ready` 可先于 factory，需另有 owner。取消一次请求等待不证明后台 attempt 已退出；Tonic connect timeout 覆盖 connector future，不覆盖后续 Hyper handshake。

## 验证与复现

8 个真实 Channel/connector 测试覆盖同 Channel 两次 fresh attempt、旧 DATA alias 跨旧 connection IO/executor exit、原 GOAWAY error 跨 connection exit、factory refusal、11 组 scalar/独立 DATA/raw/GOAWAY geometry 拒绝、dial failure、真实 pending-dial Drop、once-bound reuse 与默认 None。使用真实 Worker `ResultRetainedBudget` 预授同一 grant；以原 IO Drop、peer Join 和 executor future Ready/Drop 作退出 oracle，无 sleep 充当退出证据。测试的 combined grant 保留全部池直到最后 alias，不声称未测 socket/task/header 等已覆盖。

```bash
cargo test -p novarocks-native-adapter --test native_tonic_connection_factory --offline -- --test-threads=1
python3 docs/testing/mem-1-m07/evidence/p04-tonic-connection-factory/channel-only-reproduce.py
```

恢复后新 8 + 原 h2/Hyper 52 = 60 协议测试通过；Native lib 574 + Worker lib 313 通过，共 947 非重复 workspace tests。两项 negative mutation 均实际 test FAILED/cargo101：跳过配置 forwarding 使原 wallet/alias oracle 失败；提前 dial 使 refusal 的 connector count 失败。完整 diff/log 和 byteexact restore hash 保存于 [index.json](index.json)。

Native 首轮 lib 在 `bounded_root_count_finishes_before_read_and_survives_task_retirement` 的即时 task-runtime retirement 断言失败（573 pass/1 fail，Worker 尚未执行）；该用例单跑与全 lib 再跑均通过，按 workflow 时序噪声判据记录。helper 只等待 Finished，而 retirement_ready 另需 actual_stopped/output_released/resources_converged（Worker convergence/status 与 registry settle）；不修改产品源，不将 Finished 当作资源退出证据。初次 helper lifetime、dial error Debug 文本 oracle、never-loop Clippy 失败均已修复并保留完整日志。

channel-only 私有锁的 62 个依赖身份与 production lock 完全对应，offline/locked check、probe strict Clippy、实际 Tonic strict lib Clippy 及主 agent 重放通过；无下载/升级。初次从 root 用 feature flags 检查非 workspace-member Tonic 的命令被 Cargo 拒绝（101），不计通过。73 个原文件 hash 同时核对缓存 registry 与 exact crate checksum，修改仅五个原文件与一个新模块；完整 diff 为 [tonic.patch.gz](tonic.patch.gz)。独立上游 dev suite 未运行。

最终 Tonic/h2/Hyper strict lib Clippy 零 warning；Native 正常 all-target Clippy、workspace all-target check、root/vendor fmt 通过，保留既有 warnings。额外 target `--no-deps -D warnings` 被 Native lib 35 个既有/shared lint 阻断，未到新测试，不称 strict target 通过。全局 Cargo patch 与公共传输 API 触发此次 wave workspace 编译检查；尚未安装的产品不提前做最终 SQL/system/native 验收。

## 后续边界

本 slice 未安装 Native FE/BE client 或 BE listener。pending handshake cancellation、lazy/Balance 的运行时测试尚未完成；源码路径传播已只读核对。frame 独立 copy、HTTP HeaderMap/HPACK/continuation、writer/stream/task/queue/socket/TLS、error Box/carrier 与完整 2MiB connection 包络、真实 lane/predecode/admission/deadline 继续。单独预授 2MiB 不证明完整连接有界。

当前 `desktop-linux` fixture BOM live 校验通过，无缺 Docker image/JAR；默认 context `orbstack` 未切换，无 pull。Linux 正式测试按用户安排后续手动执行；本检查点不声称 Native 1FE+3BE/fullSQL/system/performance 验收，不 push/PR/archive。

首次 staged diff whitespace 检查发现原样 upstream benchmark README 的空白，以及嵌套 unified diff 的 context 前缀。原 README hash 保持不变，以 vendor 内仅该文件的 `.gitattributes` 保留其 upstream 空白；完整证据 diff 无损 gzip 保存。修正后 staged diff 检查通过，首次失败日志保留，不修改原源以伪造来源一致。
