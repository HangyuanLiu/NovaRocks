# P04：seal 与未应用 ACK 的准确水位

parent `2cf1ff727ad7d2fc7e6ac4967bbbedd3957a5300`；逐源 SHA256、完整命令、退出码与原始/压缩日志 hash 见 [index.json](index.json)。本地 macOS arm64、Cargo dev。

Worker 在原 context fence 内返回 closed root 的实际 consumed 水位，不抄请求、不默认 0。已接受到 1 后 Release seal，晚到 consumed=2 的请求准确返回 AwaitTerminalControl/1；仍有 body alias 时保持 Releasing，真实 alias Drop 后收敛。task-codec 仅对此 closed outcome 允许 actual 水位小于未生效请求；identity/profile/kind 与实际 socket-delivery 上界继续验证，其它 outcome 的请求下界保持。

Worker lib 309、root codec 5、真实 Native Host 61 共 375 个相关测试通过；三包 all-target Clippy（既有 warnings）、fmt/diff 通过。新 codec 反例覆盖 ACK-only 和带 wanted 的 seal 胜出、真实交付上界、正常 AckOnly/NotReady/Retired 不可假称 ACK 已应用。

这是准确 reader port / codec 的切片。未新增 RPC、未改 frozen 方法 manifest，没有 root tombstone。真实 FetchTaskResult DTO/服务协调切换、send-copy 事前额度到 prost/HTTP/H2 的准确所有权和有限尾部、lane/listener/FE 仍继续；P04 未完成，V1 未 advertise，没有 candidate native/性能验收。
