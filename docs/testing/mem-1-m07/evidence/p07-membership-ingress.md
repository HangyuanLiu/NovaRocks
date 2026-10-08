# FE Membership 原始承载数量准入（2026-10-08）

本切片补齐已有 R2 / D13 数量约束的生产缺口。FE report listener 从 FrontendDataRuntime
取得同一 process NativeTransportAdmission；独立 incoming Membership class 固定96 physical、
32 bootstrap，不借 outgoing Data/Control。角色/endpoint/class 不匹配在 listener 启动前拒绝，
生产构造器要求 admission，没有无保护的 public start。签名进程身份沿原 NativeIncomingKey seal，
同一 socket 不能改 key，原 IO 未退出不能复用 peer live quota。

Frontend incoming Membership gate 为96×128=12288。请求在 handler/decoder前取位；
handler返回不归还，response body EOF、error/reset、drop，或未poll的dispatch future退出才归还。
Membership没有BE任务gate，保持既有application handler路径。BE原lane拒绝计数保持。
physical随OwnedNativeIo到实际socket/TLS退出；握手随首个认证请求头或bootstrap截止后的IO退出。

FE Native FD包络补计一个accepted-then-refused transient，合计710；2048底线不变。
同一checked Membership数量函数用于原始承载、stream、FD与geometry。FE handshake包络从
仅outgoing128补齐为160（+ incoming32）；704 total connections、90112 streams及103232MiB
公开配置结构小计不变。系数仍null，没有实测bytes或完整容量结论。

定向membership13 PASS。新增反例覆盖：96 physical与32 handshake池饱和不借其他池，
失败取握手的physical回滚，peer key复用等待原IO退出；Membership ingress body在handler返回后
持位，EOF/reset/drop/未poll取消归还，12288满位在dispatch前拒绝；真实H2 authenticated
Announce到probe handler、32个并发半开TCP socket饱和/拒绝、2秒bootstrap取消、存活认证连接
与实际IO退出归还。body细项使用DrivenHandler unit，真实socket项不冒称完整Membership业务验收。

完整Frontend lib1438与Native Adapter lib712 PASS，均0FAIL；fmt/diff/checked算术PASS。
首次真实socket反例在client connection join时超时：fixture保留已读取完的h2 RecvStream，
其第三方handle仍保持connection活着。显式drop该body后定向与全库复跑通过；未放宽产品期限
或测试watchdog。首次debug日志留在logs/mem-1-m07/membership-socket-debug-20261008.log。

仍OPEN：P06s集成、其余P07生产用途/窗口/Local真实backing/Internal CPU owner，P08唯一切换、
P00b测量、P09原生1FE+3BE与性能、P10同HEAD最终全量。此收据不是workspace/Server/native
cluster/性能/transport measurement PASS；旧FE/BE防护保留，local only无push/PR/归档。

## 原始日志 SHA256

- `logs/mem-1-m07/membership-targeted-20261008.log`: `bbd07680546af73d1b51a71d90968419d76cbc78d540ffe4fbb30cec840d660b`
- `logs/mem-1-m07/membership-native-fe-libs-20261008.log`: `619cc3015dbb4579555cccf1166fd1045434d55da2f495b7fb395c5d9a6ccca5`
- `logs/mem-1-m07/membership-fmt-20261008.log`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- `logs/mem-1-m07/membership-profile-20261008.json`: `f3ee63bc44d934f835717abde1d9c96424acd79566c80bb2d813658f26ebc340`
