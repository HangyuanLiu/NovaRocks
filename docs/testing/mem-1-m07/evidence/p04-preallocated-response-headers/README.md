# P04：预授 initial/trailer 响应能力

Parent：`8aec027de0747801563eafc5cf537ebdbd82fb5e`。这是 P04 的本地行为切片；approved spec/plan v5 保持不变，P04 executing、P05–P10 open，V1 未 advertise。Linux 测试由用户手动执行；没有 push、PR 或 archive。

`ResponseHeaderMaps` 消费已经取得的两个 HeaderMap，要求原始 map family 和 field arena 身份都相同。检查只比较原有 Arc 身份，不取得新位置或分配新元数据。`Grpc::unary_with_response_headers` 在第一次 decode/service await 前持有这两个能力，成功时把 source metadata 移入 initial，把 trailer 移入 EncodeBody；decode/service 失败消费 initial，未用 trailer 随实际退出释放。旧 unary/streaming 入口保持原有路径。

`Status::from_static` 无 owned String；消费式 HTTP/trailer 方法使用原 field arena 转换，移动 metadata 而不复制 map。失败不发布部分结果：保留清空后的原 map 至 HTTP Response 或 Err Status 实际退出。EncodeBody 的 EOF/错误消费同一个预授 trailer，server 终态后不再 poll source 或发布旧 staging DATA；清空逻辑长度不宣称容量已物理释放。

`root_result_unary` 的第三个参数必须提供上述配对能力，没有 unfunded 默认调用入口；固定拒绝/编码错误使用 static Status。真实 root 测试先从同一个 Worker process budget 取得完整 pool backing/carrier grants，再制造 process data 满池，不额外建立生产钱包。实际 RootEncodedBody 类型 Layout 自动覆盖新增 trailer 字段。

验证范围是 8 个相关目标：HTTP map pool、3 个既有 Tonic 目标、3 个新增目标、root reader。新增 19 项实际 System/Worker/Grpc/Body 测试覆盖原始 backing 的退出顺序、独立 field alias、完整转换字节、source metadata 移动、不同 map family 共用 field arena 的拒绝、初始/尾部容量不足、Pending decode 取消和终态唯一发布。`verify.py` 固定源码后串行执行相关测试、Native check、HTTP/Tonic strict lib Clippy、4 个 Native target Clippy、格式和 diff 检查；结果见 `verification.json` 和 lossless logs。定向范围依据 execute contract 第 8 节；本轮不触发 shared wire/持久化格式或里程碑全量，不重复无关 workspace/SQL/system CI。

`run_regressions.py` 对实际产品源码执行 7 个负例：static message 改 owned String、新建 trailer map、部分失败 trailers 逃逸、终态继续 poll source、忽略 map family 身份、新建 unary response map、root 回退普通 unary。要求编译成功后运行期 `test result: FAILED`，逐项保存 diff/log，finally 恢复完整原始字节。初轮 Status 2 个预算 oracle 错误（归还 map 可容纳 field-sized reserve）、Body 2 个 oracle/fixture 错误（原错误 Display 和合法 field geometry）、unary 1 个 Request::into_parts tuple 编译错误均保存原始日志；修正测试后复跑。独立只读审查发现并闭合 pair map family 校验遗漏，随后未发现本切片剩余阻断项。

边界：实际 Native RPC/listener/client/profile 尚未安装本路径，三个 streaming 接口没有获得这组新能力。既有 encode_item 错误格式化 String、message/compression buffers、body/error/future、socket/TLS/task、predecode lane、绝对 deadline 和完整 2 MiB connection 仍需独立原始 owner/容量证明。此次未引入新的 unsafe 算法，不把先前 private Miri 或本次定向测试扩写为完整传输/产品证明；没有完整 Native 1FE+3BE、SQL/system 或性能验收结论。

复跑（仓库根目录，离线输入已存在）：

```bash
python3 docs/testing/mem-1-m07/evidence/p04-preallocated-response-headers/verify.py
python3 docs/testing/mem-1-m07/evidence/p04-preallocated-response-headers/run_regressions.py
python3 docs/testing/mem-1-m07/evidence/p04-preallocated-response-headers/reproduce_features.py
```

源码 pins：`product-sha256.json`、`vendor-source-sha256.json`；初轮 5 个负例的旧输入独立记录在 `negative-initial-input-sha256.json`。最终通过的源码以 final pins 为准。最小 feature probe 使用完整本地 vendor crate 和实际 Cargo.lock，验证 dependency identities 与生产锁一致；不是复制简化实现。
