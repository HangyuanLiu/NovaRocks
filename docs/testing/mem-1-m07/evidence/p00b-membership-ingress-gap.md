# P00b FE membership 数量缺口（2026-10-08，只读审查）

当前 `frontend-application/src/native/report_server.rs` 的 start_at_host 调用
`NativeRpcServerHandle::start`，Native Adapter start_inner 收到 admission=None。
`native-adapter/src/native_server.rs` 的 serve_connection 明确保留未准入 membership 分支；
`native_transport_admission.rs::incoming_lane_stream_limit` 对 role=Frontend 返回0。
NativeIncomingKeyCapacity 已能表达 Backend→Frontend/Membership，但生产 listener 未接该 owner。

结果：per-connection H2 window/header/send-buffer 参数已有，然而 TCP accept/spawn、TLS/bootstrap
与 incoming stream 的全进程有限数量没有在此路径落实。`native_fd_capacity.rs` 与 geometry JSON
按96 incoming Membership连接计目标，不是实际 gate。当前 FE outgoing admission 与 incoming
listener 也没有共享完整的 role capacity/observer 接线。

需要补齐既有 D13/R2/P08 合同：共享准确 role admission、认证前 physical/handshake gate、
authenticated peer/lane key、Membership stream位置到公开body退出、指标、bootstrap绝对期限、
饱和拒绝/继续accept/实际IO drop测试，并重新核对handshake/FD/结构式。
不能仅用 max_concurrent_streams（每连接）或更大的测得 c_*掩盖全进程无连接数量界。

因此 P00b JSON 是配置目标算术；coefficients/total_bytes 仍 null，实际测量门与 P08 切换不可据此
宣称成立。本记录不改协议/数量配置，不声称该缺口已修复。
