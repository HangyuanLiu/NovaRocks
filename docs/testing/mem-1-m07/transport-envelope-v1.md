# Native transport v1：第 6 版结构算术（P00b 部分输出）

当前只复核公开配置推导的结构项。`c_conn/c_stream/c_handshake/c_queue` 尚未测量或冻结；
完整 E_native_transport、repeatability、soak、P09 测量门均未通过。JSON 中 coefficients 与
total_bytes 为 null，禁止把结构项或空系数当成完整容量承诺。
FE Membership incoming 已接共享数量准入并通过定向socket/库测试，见
[实现收据](evidence/p07-membership-ingress.md)；[原缺口](evidence/p00b-membership-ingress-gap.md)
保留为历史审查。96连接数量准入的实现不替代transport系数或原生cluster测量。

规范来自 accepted spec revision 6 §5.8/§5.9、approved plan §2.4/§2.8/P00b。
代码事实为 `native-adapter/src/native_transport_geometry.rs` 的 `envelope` 与
`execution-contract/src/native_result_support.rs` 的 `NativeResultSupportGeometry::V1`。
复算：`python3 docs/testing/mem-1-m07/scripts/transport_envelope.py`；完整自有对象目标小计：
`python3 docs/testing/mem-1-m07/scripts/check_profile.py`。脚本不是产品 owner 验证或永久源码形状 guard。

每 lane 使用 `N_conn × [conn_window + 128 × (stream_window + send_buf + header_list)]`，
再加测得的固定项 `N_conn×c_conn + N_stream×c_stream + N_handshake×c_handshake + N_queue×c_queue`。
连接包含 live/dial/closing，stream 按每连接完整配置位置计算，不把仅有业务 active 数当成整个
transport 的数量上界。receive window 是配置结构项，不等于实际驻留内存或事前字节授权。

当前 V1：conn window 1MiB、stream window 256KiB、header list 16KiB；incoming send buffer 64KiB。
tonic 0.12.3 Endpoint 没有公开 client send-buffer 设置，按批准的 hyper 默认常量 1MiB/stream
计算 outgoing 项。每 incoming connection 43MiB、outgoing connection 163MiB；不沿用 v5 的
每连接“全部独立 backing 2MiB”断言。依赖升级必须重新核实公开默认与测量门。

| FE lane / direction | connections（32 BE ceiling，含 dial/closing） | structural MiB |
|---|---:|---:|
| ResultData outgoing | 192 | 31,296 |
| Submission outgoing | 128 | 20,864 |
| Observation outgoing | 192 | 31,296 |
| LifecycleControl outgoing | 96 | 15,648 |
| Membership incoming | 96 | 4,128 |
| 合计 | 704 | 103,232 |

FE 对应 90,112 个配置 stream positions、160 handshake positions（outgoing 128 + incoming Membership 32）、4,864 queued requests。
结构小计为 100.8125GiB；自有 FE 结果/input/local/internal/control/nonroot workspace 目标小计
10.5GiB，二者合计 111.3125GiB，尚不含 c_*。这既不是 RSS 预测，也不是默认硬件要求或 MEM
容量 grant；它如实展示当前保守结构公式的大小。旧 3GiB Native 附加与“总量≤16GiB”已退休，
不能保留为 revision 6 已证明的门。量化成本、系数、实际高水位与回落仍需 P00b/P09。
文档中的 historical_v5_transport_targets 仅保存原目标。当前 geometry/wire 中残留的对应旧字段
尚待 P08 删除；此次文档修订不声称那些字段已从产品协议或配置退出。

BE 的各 lane/direction 与完整数量见 JSON。算术保守地按 B=32 计 peer directions，实际 placement
仍来自 live registry，不能硬编码测试拓扑或据此声称 32 BE 产品验收已运行。

待完成：真实 lane grid 与负载 owner、测量方法/轮次/容差在采样前冻结、两次独立系数与残差证据、
半开握手及多 FE 重连 soak、当前 main release 旧路径基线、startup coefficients 接入。
不得以大结构项替代线性/回落验证，也不得把已有全量 Cargo 作为本测量门证明。
