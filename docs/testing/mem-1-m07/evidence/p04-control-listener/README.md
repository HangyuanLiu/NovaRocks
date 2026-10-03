# M07 P04：独立生产 Control listener 与准确端点路由

本地实现切片，parent `ce4b32946468190ea2be9ee3965cfd0e17be785d`。approved spec/plan v5 不变；P04 executing、P05–P10 open，root V1 不 advertise。此收据不构成完整 M07 交付或最终同 SHA 验收。

BE 生产 Host 安装独立 Data/Control Native listener、运行时、连接 stock、acquisition 和 ingress 门。两者共享同一业务 owner 与 Control executor。`BackendProcessDescriptor` 的 mandatory Control endpoint 独立于 optional root support；announce/exact heartbeat 检查完整不可变 descriptor，missing/invalid/相同端点及 support 漂移严格拒绝。FE 冻结完整 descriptor，按原唯一 Native method manifest 选择端点；Heartbeat 和 Lifecycle/Control 走 Control，普通提交、观察与结果保持 Data，placement/process 所属仍以 Data endpoint 表达。没有 Data+1、membership 重查或 V1 fallback。

入口先验证 JWT，再同步检查 manifest/domain，先于 body polling、decoder、service clone 和 ingress gate；未知、retired、错域方法静态拒绝。两 listener 在关闭时都先 begin_stop，再 join；第二端口 bind/readiness 失败回收 Data、Control 和 management。Server 配置要求 BE 显式非零 `control_grpc_port`，独立 NAT advertise；fixture publication、harness 真正端口预留、成对故障代理和两域 probe 均消费此配置。两个 proxy 共享原每 BE 转发预算，未放大容量。

FD 启动前检查只读取 `RLIMIT_NOFILE`：BE soft≥1024，FE soft≥2048；BE 原 stock 算术为 518+20+2 listener+2 refusal=542。检查发生在日志、budget/runtime、StateStore 和 listener 副作用前。真实低 FD child 测试证明拒绝且父进程限制不变。该算术不预留其他 Connector FD，也不证明完整 FE 连接图。

实际 1FE+3BE TLS 验证发现两个真实失败，均修复并保留初次日志：

- 原 field arena 把 checkout CAS 临时争用误报为 Exhausted，BE 自动 Date 在 response HEADERS 前发出 `RST_STREAM(INTERNAL_ERROR)`。改为原授的短 Mutex 临界区，只保护固定 scan/claim/retirement；callback、Bytes 发布和 owner Drop 在锁外。构造时预热 final Arc 的 Darwin PAL，并纳入原 bound；真实 position/fragmentation/单字段上限继续拒绝。操作系统 mutex 调度时间不是本切片的有限退出证明。
- registry-contention 场景持 Worker mutex 等 Control-start 读数，而完整 metrics scrape 为 preparation ledger 等同一锁。新增准确非阻塞采样：Busy/无 owner 的本次 availability=0，省略 preparation 数值，不能沿用旧 gauge 或伪造零；成功恢复准确 ledger，Poison 明确使两种格式失败。旧阻塞 owner API 保留；其他 metrics owner 仍有各自同步边界。

最终定向 **670 Native lib +165 protocol +1,634 FE/shared/harness +4 Server binary +3 Worker owner =2,476 PASS**（harness 3 ignored）。相关 Clippy、fmt/diff 和五个依赖边界 guards 通过。当前原 checked bound 为每连接 **6,452,461 B**、进程 stock **3,471,643,290 B**；完整独立 2MiB 子图仍开放。验证结果与准确源快照见 `verification.json`。9 个最终本地独立进程 1FE+3BE 场景通过：membership/self-registration；plaintext-IP、automatic-DNS、PEM-IP；outer preflight、message bounds、blocking saturation/control、partial body deadline、registry contention/control。field 修复后另外串行重复 automatic-DNS/PEM-IP 六轮全部通过。实际 Native 相关分配/协议测试 50 项通过；新增并发 5 项及 preparation owner/metrics 7 项具有能失败的 oracle。原 HTTP/Bytes 完整 normal sources 的 25 个探针普通/Miri 各通过，仅证明该子图，不外推整任务或 TLS heap。

7 项 Control actual-source negatives 和 3 项新增 checkout/metrics actual-source negatives 均编译后 runtime FAILED，并逐字恢复。源码负例不以 compile error、zero-test 或 timeout 冒充行为击穿。复现：`verify.py`、`run_regressions.py`、`run_concurrency_regressions.py`、`reproduce_field_arena.py --miri`。

Cargo-only 全量 CI 在收敛前候选通过：11,899 PASS/7 ignored，781 秒。它的准确源清单单列 `cargo-only-candidate-*.json`，日志 HEAD 是 parent；之后 field/采样修复及更严格 malformed probe 有独立最终定向验证，不能把前候选全量结果说成最终检查点全量。首次 CI 因工作树 dev 构建输出耗尽磁盘失败，`cargo clean --profile dev` 回收可再生输出后重跑通过；没有删除源码、fixture 输入或 Docker 数据。SQL/default system 全量及最终同 SHA 收敛仍属 P09/P10。

源码与日志 SHA256 清单独立保存。System 收据仅保存不含凭据/私钥的字段和原 artifact/hash locator；fixture 仅保存端口/绑定状态摘要，prepare-only 不证明 Docker 健康。当前环境 image/JAR BOM 完整，无下载/构建缺失输入。Linux 正式性能测试由用户手动后补。

下一继续 incoming authenticated peer/lane admission、完整 task/future/socket/TLS/auth/error/body/issuer 原授与真实退出图，以及 BE root 通道、FE 原子整窗/lane/closing 和 P04–P10。Data518/Control20 与 acquisition32/8 不等于认证后的每 peer/lane 界。独立生产 Control 已接线，但没有宣称抵御 Control 自身风暴、整连接独立 2MiB 或完整资源账本。无 push/PR/archive，persistent goal active，按正常模式继续。
