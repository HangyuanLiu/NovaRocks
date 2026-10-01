# P04：真实 Native Host 的 bounded root 接管

这是 P04 的本地行为检查点；parent 为 `b7fa0edfa2c46fff067415948a436b85ffd6f902`。源码逐文件 SHA256、完整命令、退出码和原始/压缩日志 hash 见 [index.json](index.json)。平台为本地 macOS arm64，功能检查用 Cargo dev；本切片没有候选 native 集群或性能收据。

## 已实现与验证

- Host 在准备前为准确 `RootResult` 创建 channel/session，复用同一 `ResultRetainedBudget`；Worker 的原 context creation fence 接管。TaskRuntime 只保留 channel，避免 session metadata 与 Release 相互等待。
- 实际 VALUES→pipeline→固定 CPU producer 的 ClientRows 输出准确 MySQL 文本字节；CountOnly 不产生 Data。FINISHED 来自 End、真实 producer exit 与 context 接管，发生在 fetch/最终 ACK 之前。task 退休后 End 仍可读取/重放；无最终 ACK 仍可 Quiesce/Release，持有 delivery 时 Release 保持 Releasing，到实际 Drop 才收敛。
- QueryFailed、PeerTaskFailed、LeaseExpired 在 Worker seal 前固定准确 cause；真实 Host 在 runnable fan-out 暂未到达时仍报告准确 Aborted。DOP=64 的 VALUES 结果按实际构建的 root drivers 计一次；准备回滚不留下 session/输入责任。
- root producer 的线程数、安装位置和 stack 是 Server 的独立有限配置，统一验证后注入同一进程池。Host 停止 ingress 后先 cancel/drain/join producer，再停止 driver 和 completion。这里只证明已审计 Rust 请求 backing/stack 的覆盖，不以 join 当成 pool backing 已释放，也不声称 libc/TLS/stack cache/whole RSS 受这些 Layout 证明覆盖。
- 原 factory/driver metadata 在构造前由同一固定 envelope 子额度覆盖；导出的 authority observable、固定 slot、token 和 notification snapshot 保留同一 backing owner 到最后真实退出。
- bounded notification 对单次/连续 observer panic 仍通知全部 waiter；析构正在 unwind 时不恢复第二个 panic。Session 独立完成通知、input、builder 清理，首个 panic 在 Complete→actual exited 后恢复。input permit 即使 credit Drop panic 也明确清 occupied 并通知，再恢复首个 payload；PhysicalOwners 在通知前发布 progress。

## 定向验证

553 个相关测试通过，重复的 focused root 子集未重复计数：Native Host 61、pool 11、Backend application 7、session integration 12、Worker lib 308、pipeline 122、Observable 19、input authority 3、实际 allocation probes 6、Server config 4。四包 all-target Clippy、fmt 和 diff check 通过；依赖与既有源码 warnings 保留，没有把它们写成严格无 warning 通过。Server 其它 integration binaries 的 0 selected 不算覆盖。

新增真实退出 oracle 检查 Weak 原始 Arrow owner 消失、全部 input credit 可再次 reserve、5 秒实际 pool join；observer 连续 panic 时输入必须在耗尽 64 次 panic 预算前释放。对仅恢复 parent 的 Session fail/abort 实现的局部 mutation，同一最终 oracle 退出 101；文件已在 finally 恢复。这证明实际清理不依赖 observer 后续恢复正常。

## 保留的失败

- 最初 session snapshot 测试留下导出的 authority token，正确的新 retained owner 因此仍在；修正测试在 callback 被暂停期间释放 token，继续单独证明真实 snapshot 持有原 metadata，未改释放 oracle。
- Host fan-out race 测试最初 pause callback 跑在 producer 的取消线程而非 AbortQueryContext 线程，锁内阻断了自身清理；改为准确 ThreadId 条件，QueryFailed/PeerTaskFailed/LeaseExpired 的原状态与期限 oracle 保持。
- 连续 panic 最初阻断 cleanup；第一次只捕获整个 Drop 暴露 segment 字段析构的第二个 panic，真实测试 SIGABRT。新增 unwind 通知边界与 input position 清理后解决。早期 `<=3 callbacks` 错误地把固定 W2 的正常释放通知当作重试，改为未耗尽连续 panic 预算，并保留原始 Weak/credit/join oracle；同一最终 oracle 的旧逻辑仍失败。
- 新 Observable unwind 单测第一次漏了 Arc 包装，编译失败日志保留；修正后 19 项通过。

## 继续实施的边界

P04 仍 executing。InternalFacts 私有 codec 未安装时在 Host 先明确 Protocol 拒绝；统计/写提交/scalar/COW codec 与源头增长门、Native 结果读取、prost/H2 实际传输 copy/alias/有限退出和物理 lane/listener 接线继续。没有 advertise V1，没有以本检查点代替 C4、P09/P10、生产 native 或性能验收。Linux 正式测试按用户安排后补。

`DOCKER_CONTEXT=desktop-linux` 完整 all-consumer fixture BOM 再次校验退出 0；当前没有缺失 image/JAR。不更改全局 Docker context，不下载或拉取输入。
