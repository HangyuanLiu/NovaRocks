# P04：Hyper 传递 H2 接收事件上限

parent `13b89c41be03ca1f2d6d222a8419f3a02284fcdc`。本切片只把 opt-in H2 count gate 传到真实 Hyper client/server connection builders；尚未安装 Native listener/FE Channel、独立 control/result lanes、连接/握手 admission 或完整 read/write backing 的 physical-exit owner。P04 仍 executing，V1 未 advertise，无产品或性能验收。

锁定 Hyper 1.8.1 从本地 registry 原样 vendoring；UPSTREAM.json 保存 crate checksum 与 68 个原文件 hash。仅 client/server conn::http2 builder 和 proto::h2 client/server Config 四个原文件变化：positive max_receive_buffered_events setter、Option 默认 None、before-handshake 条件转发。builder clone 保留配置，客户端已有显式 enable_push(false)。Cargo.lock 只移除 Hyper 原 registry source/checksum，版本和依赖未迁移。未修改 Tonic Endpoint/Channel 或 hyper-util wrapper，因此这些现有入口不会自动安装新界。

真实 Hyper 7 测试：client/server 64 个 1-byte 或空非终态 DATA，在 cap=2 下第三条必须 Pending，逐次 pump/消费保持所有 DATA 与 EOS；server default 未安装数量界时保持原 readiness；两端 setter 在构造连接前拒绝 0；local Incoming Body Drop 清掉满节点并唤醒，使另一 stream 的 HEADERS 被实际 accept。所有 raw frame 在 consumer/connection poll 前已经写好，manual executor 在明确 pump 点驱动真实 Hyper future，避免 socket/executor 调度掩盖节点数。它使用合法解析字段与预置 SETTINGS ACK，未证明真实网络握手的时序，更不等于 Native 任务取消、deadline 或服务 future 退出。fixture 成功路径锁外清空全部 executor tasks，解除内部 executor clone 引用环。

两类 negative mutant 去掉 client/server Config 到 h2 的转发，各自测试真实失败 101（额外已缓冲 DATA / 第三条错误 Ready），随后逐字节恢复；完整 mutant、final source hash、日志 raw/gzip hash 和 exit codes 在 index.json。独立只读审查核对 68 个原文件来源、四文件 delta、默认路径、clone/handshake 链与 fixture cleanup，没有发现 actionable 缺陷；审查未运行测试。

Count gate 会阻塞整个连接的读取：一个停读 body 占满节点后，后续其他 stream HEADERS/RST/WINDOW_UPDATE 也不能读取。既有 outbound flush 仍运行，但不能保证获得堵在输入之后的新发送窗口。后续 Native 接线必须具备独立控制连接和真实 local cancel/deadline→body Drop，不依赖同连接远端 RST 来解除阻塞。最大 frame size 和 flow-control byte window 均不能替代该数量门或 retained backing 证明。

本切片扩展库验证暴露两个中间失败，原始日志均保存。第一次并行 Native lib 为 573 pass/1 fail，Statistics source refusal 的错误文本断言未匹配；同源码隔离该用例立即通过，增加实际错误诊断后后续全库也通过，原因尚未确定，不归因于 Hyper、Docker 或缺失输入。第二次为 573 pass/1 fail，ClientRows 测试强制要求 Data+End 同返；单跑和 serial 全库通过，但这条假设与 accepted spec 的即时小行发布、不可补改历史 Data、允许独立 End 冲突。仅修正 ClientRows/Statistics 两个测试：如果 Data 已固定 end_after_data 则核对行数/序号，并在任何 ACK 前准确读取/重放 End(2)、核对 output_rows=1 和 accepted_consumed=0；FINISHED 仍在所有读取前观察，保留原字节 golden。生产协议和 End 发布实现没有改动。额外临时 strict-coalescence mutant 在先成功读取正确独立 End(2)/output_rows=1/ACK=0 后仍强制 Data 必须带 End，第二次并行运行真实失败并打印 `None`；第一次通过也保留。它直接确认合法独立 End 分支会随推进 cut 出现，不将这类协议测试缺陷泛称为负载噪声。mutant 已逐字节恢复，最终同一恢复源码的 lib/H2/Hyper 588 测试再次通过。

最终 Native lib 574 + H2 7 + Hyper 7 = 588 非重复测试通过；Host 64 为其中子集。Hyper lib strict Clippy 无 warning，Native all-target Clippy 有既有 warnings；workspace all-target check、root fmt、四个 vendor 文件 fmt、diff check 通过。根 Cargo patch 与公开 builder 接缝触发此 wave 的 workspace check。未尝试 standalone Hyper 上游 dev-dependency suite，不声称它已通过；实际测试使用生产 workspace 的锁定依赖。最终完整 SQL/system/1FE+3BE CI 留待 P04–P10 收敛，Linux 正式性能测试按用户安排由用户后续手动执行。当前 desktop-linux fixture BOM live 验证通过，镜像/JAR 输入齐备，未 pull 或切换全局 Docker context。无 push/PR/archive。
