---
id: ADR-0064
title: "External MV publication fencing at the lake commit point"
domain: [frontend-mv, provider-spi]
status: superseded
supersedes: []
superseded-by: ADR-0110
date: 2026-08-13
provenance:
  - "discussion: 2026-08-10 MV refresh active-active recalibration"
code-anchors:
  - "novarocks/spi/src/connector/mv_publication_fencing.rs (ConnectorMvPublicationFencing)"
  - "novarocks/connector/iceberg/src/commit/mv_publication_fence.rs (establish_publication_fence)"
  - "novarocks/connector/iceberg/src/commit/mv_refresh_ref.rs (decide_v2_publication)"
---

## 问题

当一个 Frontend 已经失去 MV refresh 的控制面所有权，为什么仅靠比较 `main` 与 staging ref 无法阻止它继续 publish，
以及应该在哪里建立那个能在外部提交点拒绝旧 owner 的线性化点？

## 背景与执行事实

MV publication 此前的 guard 是 `refresh_id + mv_id + marker token`（`ConnectorRefreshPublicationGuard`），
Iceberg 侧在一次 commit 中要求 `main` 仍是冻结的 expected snapshot、staging ref 仍指向冻结的 staged snapshot
（见 ADR-0036 的 lifecycle 划分）。

这套 guard 能证明「这个 staged 结果属于这一次 refresh attempt」，但不能证明「这个 attempt 的 owner 仍然有效」。
两者是不同的命题。只要目标表的 `main` 尚未变化，一个已经在 StateStore 中丢失 lease 的旧 attempt 依然满足全部
requirement，因此仍能推进 `main`。把 fencing epoch 只写进 provenance 可以事后诊断，但事后诊断不是 fencing：
外部提交点在决策的那一刻并不读它。

另有一个 identity 事实使问题更硬：MV 的 numeric `mv_id` 在 StateStore 丢失后重建时会被重新分配
（`mv/{mv_id}` 因此不是跨 rebuild 稳定的协调 identity），而 catalog display name 与 catalog attachment lifecycle ID
都会在 DROP/recreate 下复用。它们都不能充当外部 fence 的 key。

## 考虑过的选项

1. **只把 fencing token 写进 provenance/marker。** 改动最小，且不增加 metadata commit。但它只能在事后审计中
   发现「旧 owner 曾经 publish 过」，无法阻止那次 publish，因此并不解决问题。
2. **让 StateStore 成为唯一线性化点，publication 前再查一次 lease。** 直觉上简单，但 StateStore 与 Iceberg 是两个
   独立系统，两次检查之间存在窗口；这等于假装存在一个跨系统原子事务。
3. **用 `main` 的 snapshot ancestry 反推 owner 是否被取代。** 无需新增 ref，但 full refresh 可以合法地 publish 一个
   并非从旧 snapshot 派生的 snapshot，ancestry 因此不是可靠的 owner 证据。
4. **在目标表上建立一个专用的、由 provider 拥有的 internal fence ref，并在推进 `main` 的同一 commit 中要求它。**
   代价是每次 ownership generation 多一次 metadata commit。

## 裁决

采用选项 4，并固定三件事：

**其一，fence domain 的 identity 只由 provider ID 与 provider 观测到的 immutable target table UUID 组成。**
UUID 在契约中是 typed 而非 opaque string，因此 display name、numeric `mv_id` 与 attachment lifecycle ID 在类型层面
就无法被当作 fence key。外部 DROP/recreate 产生新的 table UUID，也就自然进入新的 fence domain；StateStore rebuild
重新分配 `mv_id` 则不改变 domain。

**其二，一个 ownership generation 只有先以 provider-authoritative CAS 赢得 fence ref，才成为 publication-capable。**
generation 由 StateStore coordination 的 resource lease fencing token（`FencingToken`：cluster ID +
control-plane incarnation + resource epoch）派生，跨边界只传 canonical digest，raw token 永不进入 lake。
ordering 只在同一 cluster 内定义：跨 cluster 比较、以及同一 `(incarnation, epoch)` 携带两个不同 token digest，
都 fail closed 而不猜测顺序。同一 generation 的重复 establish 是幂等的（包括丢失回复后换用新 operation ID 的重试）；但同一个 operation ID
跨 generation 复用被拒绝，否则丢失回复将无法判定究竟哪个 generation 提交成功。

**其三，推进 `main` 的那一次 commit 同时要求四件事**：table UUID、冻结的 `main`、冻结的 staged snapshot、以及本
generation 建立的那个 exact fence snapshot。第四项正是阻止旧 owner 的那一项——takeover 移动了 fence ref，旧
generation 即使 `main` 期望仍然成立也会失败。

由此得到的保证是精确的：**更高 external fence 建立之后，较旧 generation 不能再 publish。** 它不是「StateStore 与
Iceberg 存在跨系统原子事务」。两个线性化点依然存在，先后由外部 CAS 决定：takeover 先线性化则旧 publication 失败；
旧 publication 先线性化则新 owner 在建立 fence 后观察到已变化的 `main`，按已提交的 lake truth reconcile，而不覆盖或
盲重放。

fence snapshot 是 data-free 的，并以观测到的 `main` 为 parent，因此 fence ref 永远只命名一个活跃 fence snapshot，
不会累积自己的 ancestry。

establish 与 publish 都是 external mutation，复用既有的 `ExternalMutationOutcome` / `ExternalMutationEvidence`
词汇，而不新增第二套 evidence codec。失败被明确切成两类：commit 之前的全部检查是 precondition，因此确定未提交；
只有 catalog commit 本身可能 ambiguous。ambiguous 时返回 bounded provider evidence 并由 inspect 在同一个
operation ID 下从 lake truth 判定，lake 无法证明答案时报告 unresolved——不按时间戳或 numeric `mv_id` 猜测赢家
（与 ADR-0037 的跨 incarnation inspection 取向一致）。

## 接受的妥协（诚实记录）

- **每次 ownership generation 多一次 Iceberg metadata commit。** 这是为了让外部资源可执行 fencing 而付的代价，
  明确拒绝降级为「只记录 token」的事后审计方案。它只发生在 generation 切换时，不在每次 refresh。
- **V2 provenance 不是 V1 的超集，而是替换了权威 identity。** V2 丢掉 `refresh_id`/`mv_id`/`token` 作为权威身份，
  并且不与 V1 双写权威记录——一个 snapshot 上并存两套权威 identity schema 会使「哪个说了算」无定义。代价是 V1 与
  V2 是两条 publication 路径，V1 的 ledger-driven recovery 继续可读但不获得 ledgerless takeover 保证。
- **legacy V1 lake marker 在 StateStore ledger 全失时不承诺可完整枚举。** 对无法证明的历史 attempt 确定 fail
  closed，这是保守恢复的直接代价。
- **publication 的 unknown 判定偏保守。** 若 `main` 停在一个与我们无关的 snapshot 且 fence 已被更高 generation 取
  代，我们报告 unresolved 而不是 uncommitted。原因是 full refresh 可以合法 publish 非派生 snapshot，ancestry 不足以
  证伪。代价是这类 attempt 需要人工或后续 reconcile 收敛，而不是自动判死。
- **stable resource identity 在契约中要求 UUID。** 这对「没有 UUID 概念的 provider」是过度约束。当前只有 Iceberg
  拥有 MV target（其规范强制 `table-uuid`），因此换取了 acceptance criterion「display name / numeric id 不可能成为
  fence key」的类型级保证。这是为当前唯一 provider 做的取舍，不是普适判断。

## 何时重新评估

- 出现第二个拥有 MV target 的 provider，且其表身份不是 UUID：此时 stable resource identity 需要重新裁决为
  provider-signed opaque bytes，本 ADR 关于 fence domain identity 的裁决应被 supersede。
- 每次 generation 一次 metadata commit 在生产中成为可观测成本（例如 lease 频繁翻转导致 fence ref 抖动）：应重新评估
  是否把 fence 与某次真实 publication 合并提交，但不得以放宽「同一 commit exact compare」为代价。
- Iceberg 或 catalog 层出现原生的、可用于 fencing 的条件更新原语（例如 provider 侧的 lease/epoch requirement）：
  届时专用 internal fence ref 可能变成多余的一层。
- ledgerless attempt discovery / StateStore rebuild 落地后，若发现 unresolved 判定过于保守以致运维负担不可接受：
  重新评估 publication inspection 的判定规则，但保守方向优先于自动化程度。
