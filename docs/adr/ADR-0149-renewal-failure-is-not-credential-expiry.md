---
id: ADR-0149
title: "A failed credential renewal ends its round, not the attempt"
domain: [distributed-query-lifecycle, provider-spi]
status: superseded
supersedes: []
superseded-by: ADR-0151
date: 2026-09-16
provenance:
  - "PR: <backfill after merge>"
  - "discussion: 2026-09-16 refining UEA-1 D2.4 renewal failure semantics"
code-anchors:
  - "novarocks/frontend-application/src/task_execution/credential.rs (refresh_timing soft/hard margins)"
  - "novarocks/frontend-application/src/task_execution/credential_pump.rs (settle_vending, abandon_round, lease_bound)"
  - "novarocks/worker/src/credential_slot.rs (resolve_vended_s3 expiry rejection)"
  - "novarocks/frontend-application/src/query_execution/lifecycle_plan.rs (resolve_vended_s3_access expiry rejection)"
  - "novarocks/frontend-application/src/native/task_transport.rs (credential rotation round metric)"
---

## 问题

一次 vended 凭据续期失败时，谁有权终止 attempt？具体地：给 provider 调用设的那个硬 deadline
到期，应当终结这一轮续期，还是终结整个查询？

## 背景与执行事实

ADR-0145 确立了 per-attempt 执行访问：每个 attempt 独立取得自己的数据凭据。长查询靠 FE 侧的
轮换 owner 在凭据到期前续租。UEA-1 的 D2.4 要求 provider 调用必须接受一个覆盖连接、读取与全部
内部重试的硬 deadline，理由是一个卡住的 provider 不得把已线性化的 query completion 变成无界
drain。那条要求是对的，本 ADR 不动它。

被遗漏的是**到期之后做什么**。实现选择了失败整个 attempt，而该时刻按构造并不是凭据失效的时刻：

- `refresh_timing` 计算 `hard_margin = (remaining/20).clamp(1s, 30s)`，
  `hard_delay = remaining − hard_margin`（`credential.rs`）。
- 因此硬 deadline 恒等于「凭据真实失效 − 1..30 秒」。一小时寿命的凭据会在 T−30s 被判死。

这让轮换 owner 成为「凭据是否还能用」的**第三个**执行者，而且是最不准的一个。另外两个是准确的，
各自在取用凭据的地方按 `not_after` 拒绝，没有余量：BE 的 `resolve_vended_s3`
（`credential_slot.rs`）与 FE 的 `resolve_vended_s3_access`（`lifecycle_plan.rs`）。

D2.3 早已声明「长查询续租、过期和撤权分别处理」。实现把前两者合并了。

实测证据：让夹具扣住一次 `/credentials` 响应，三轮续期全部在预算内被放弃、零次铸成，查询随后
死于 provider 超时——而它当时仍持有可用凭据。改判后同一场景一次即铸成。

业界对照：Trino 的 `AbstractIcebergRestVendedCredentialsProvider` 与 Apache Iceberg 上游的
`VendedCredentialsProvider` 都是用时触发、无后台刷新器、无独立调用预算；续期失败只能通过某个
真正需要凭据的操作失败来表现。两者都没有「独立计时器到点即判查询死」这种东西。

## 考虑过的选项

**选项一：保持现状，硬 deadline 到期即失败 attempt。** 实现最简单，失败点也最靠近原因。但它在
凭据仍可用时杀掉查询，且与 D2.3 的「分别处理」冲突；一次瞬时的 catalog 抖动会终结一个已经跑了
一小时的查询。**设计否决。**

**选项二：照搬 Trino 的用时触发模型，去掉独立的轮换 owner 与调用预算。** 更简单，且是成熟产品的
做法。但它并没有解决 D2.4 要防的问题，只是把卡住的 `/credentials` 变成卡住的对象存储读取；
NovaRocks 的有界调用是 Trino 没有的一层保护，放弃它是倒退。**设计否决。**

**选项三：硬 deadline 只终结本轮，允许下一轮，失效判定收归访问解析点。** 采纳。

## 裁决

1. **Round-abandon rule。** provider 调用的硬 deadline 到期只终结**本轮**续期。到期时关闭本轮的
   provider generation fence、把已进入 provider 的调用交给 FE process-runtime 残留 owner、清空
   本轮状态，attempt 不失败。fence 每轮新建，关闭第 N 轮不影响第 N+1 轮。

2. **Single-expiry-judge rule。** 「凭据是否已失效」只由取用凭据的地方判定，按 `not_after`
   精确判断、无余量。轮换 owner 是续期驱动者，不是失效裁决者。attempt 因凭据原因失败，只能是
   某次访问解析在 `not_after` 之后被拒。

3. **Retry-bound rule。** 放弃一轮后的退避以**该 lease 的真实剩余寿命**为上界，不以已成过去的
   本轮 deadline 为界——后者会把退避变成对刚刚失答的 provider 的热循环。lease 已无剩余寿命时
   停止轮换，而不是开出一轮生来就过期的空转。

4. **Bounded-provider-call rule（沿用 D2.4，不削弱）。** provider 调用仍必须接受覆盖连接、读取
   与全部内部重试的硬 deadline，并由底层实现实际执行。本 ADR 改的是到期后的动作，不是有没有界。

5. **Rotation-observability rule。** 轮换 loop 必须有生产可观测量，能区分「从未发起」「发起后被
   放弃」「发起后失败」「发起并铸成」。只有安装计数不足以说明 loop 在转，而本裁决把失败点后移到
   访问解析点之后，定位性必须由该观测量补回。标签是固定词汇，不含端点、lease 材料、attempt 身份
   或 provider 消息。

## 接受的妥协

失败的定位性变弱。原先 FE 一条 `credential rotation failed` 直指原因；现在典型失败是访问解析点
的「vended 访问不可用」，离原因更远。接受它，因为它**是真的**——查询确实一直读到凭据真失效为止；
裁决 5 的观测量是对这项妥协的补偿，不是它的消除。

同一 attempt 内多轮放弃会增加残留 job 数量。这正是 D2.4 设残留 owner 的用途，上限由既有的
Connector blocking-I/O slot 预算约束，本裁决不新增预算。

窗口有限：一小时寿命的凭据只多争取 30 秒。但被救下的是本可在那 30 秒内读完的查询，以及只因一次
瞬时抖动被杀的长查询，且它消除的是一个语义错误，不只是一个时间窗。
