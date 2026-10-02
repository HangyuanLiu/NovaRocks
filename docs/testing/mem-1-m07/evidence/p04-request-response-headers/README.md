# P04：从原 request 传递响应头能力

Parent：`37955646bdf81851e2f0d1eec1eae825eb22c302`。approved spec/plan v5 不变，P04 executing、P05–P10 open、V1 未 advertise。Linux 测试按用户安排手动后补，无 push、PR 或 archive。

Tonic 四种 server handler 从实际 incoming HeaderMap 的原 map/field 能力，在 decoder 构造、body poll 与 service 调用前取得 initial/trailer 两个位置。位置不足时丢弃未 poll 的 body，清空并复用实际 input map；部分取得的 response 位置真实归还，无普通 map 回退。已有显式 root unary 仍消费调用者预授 pair，不再重复取得。fields=None 与 map-only 既有路径保持原行为。

Native listener auth 拒绝和外层 Router fallback 同样复用实际 input map。BE ingress 在任何 gate wait 前预取一个 error map，使 request 进入 handler 后发生 timeout 时仍可发布原能力响应；取消 future 时 input/error map 随真实 owner 退出。arrival 保持同步记录且先于容量准备，原 deadline 包含该准备及 first-poll 延迟。duplicate timeout 不再先建 Vec，仍只允许一个 timeout。静态拒绝 reason 直接 fallible 写入原 map，避免中间普通 Status metadata scaffold；失败清空原 map、HTTP500、单次 terminal body error，Body Box 仍待完整 composition 覆盖。

真实 BE ingress→Grpc→EncodeBody 在 decode 前同时占用 **4 个 map**：input 1、ingress error 1、initial/trailer 2。组合测试检查实际 body 每次 poll 的位置、source HeaderValue 指针、真实 DATA/terminal、最后 field alias 与同一个 Worker retained budget。不能把四位置扩写为完整 framework map census：received trailers、clone、owning drain、队列 owner 仍须分别覆盖。

最终 10 个相关目标 **107 项 PASS**，Native response/ingress/server 三组定向 lib **22 项 PASS**。新 target 13 项、新 lib 13 项；实际四种 gRPC 形态包含 decoder/service 错误、unsupported encoding、容量拒绝前 decoder constructor/decode/body/service 均零次、Pending 取消与 legacy。新 ingress target 7 项还覆盖 invalid deadline、Content-Length、运行中 200ms header deadline、FE bypass。200ms 是测试请求的真实期限，2s 只是外围防挂保护，未放宽产品 deadline。Native owner 测试没有安装 global allocator；系统级 backing/dealloc 次序依赖同时运行的真实 pool probes，不能仅由 credit 数值宣称零分配或 physical deallocation。

6 个实际源码负例均编译后 runtime FAILED，并 finally 精确恢复：不消费 request 能力、ordinary initial map、ordinary capacity refusal、缺失 ingress fallback、Native early refusal 不使用 input map、运行中 timeout 不使用备用原 map。完整 log/diff 保存。负例之后只补了两个 scoped Clippy expect（真实 Tonic Status 直接返回，不增加 Box），没有行为改变；最终 pinned 129 项重新通过。最初新 ingress test 的 unused Code import 已移除；新 fixture/helper 的 result_large_err 已按上述原值返回契约明确 expect。Native lib 仍有既有 warnings，包括 HEAD 中本已有同签名的两个 deadline-parser result_large_err；没有新 target/helper warning。Native check、HTTP/Tonic strict lib Clippy、Native lib/六目标 Clippy、root/vendor fmt、diff 均 0。

Hyper server/http2 与 Tonic channel-only 两个最小 feature 变体 0，63 个 dependency identities 与生产锁一致。7 个改变 product/test pins、264 个完整实际 vendor source/manifests pins、36 份 lossless log/diff 见同目录 JSON。初始日志只证明其当时接线；`final-*` 对应最终 pinned 源码。没有替代算法副本。

**范围**仅为原 response header 能力传递。既有 decode/compression/任意 service Status 的 raw message/details/metadata、Native auth 内部 token/verdict backing、LimitedRequestBody 的 source Status/scaffold、generated service unknown-method 分支与 Tonic interceptor early errors、Body/future/task/socket/TLS、完整 pool census 与绝对退出 deadline 仍开放。实际 Native listener/client 尚未安装原 connection factory；无完整 Native1FE+3BE、SQL/system 或性能结论。本轮按 contract 第8节定向验证，未触发里程碑全量。

下一安装切片复用 `backend_application` 现有同一个 ResultRetainedBudget，server 在 TLS accept/首 I/O 前授额，client 在每次 reconnect 前取得新 pools；控制保底须先于 root 增长。冻结算术保持 `2MiB connection independent backing + streams*(4KiB bookkeeping+8KiB idle decoder+32KiB header workspace) + Tonic pending`，不能把后两项吞入 2MiB。全部 owners/lane gates 未闭合前继续禁止 advertise V1。

复跑本检查点源码：

```bash
python3 docs/testing/mem-1-m07/evidence/p04-request-response-headers/verify.py
python3 docs/testing/mem-1-m07/evidence/p04-request-response-headers/run_regressions.py
python3 docs/testing/mem-1-m07/evidence/p04-request-response-headers/reproduce_features.py
```

Preflight 校验实际 product/vendor 哈希；negative driver 要求编译后的 runtime 拒绝并精确恢复。Docker 此前 desktop-linux 全 BOM 已通过，无缺 image/JAR；本切片不重复下载、构建输入或改变全局 context。
