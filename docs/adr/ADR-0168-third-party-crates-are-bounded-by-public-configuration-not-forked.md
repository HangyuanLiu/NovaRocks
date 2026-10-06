---
id: ADR-0168
title: "Third-party crates are bounded through public configuration, not forked for resource accounting"
domain: [crate-boundary, memory-governance]
status: active
supersedes: []
superseded-by: null
date: 2026-10-06
provenance:
  - "discussion: 2026-10-06 rejection of a vendored Tokio/Hyper/H2/Tonic/Tower/HTTP/Bytes/Arrow stack for bounded result delivery"
  - "PR: pending — backfill the number once MEM-1 M07 merges"
code-anchors:
  - "Cargo.toml ([patch.crates-io])"
  - "novarocks/native-adapter/src/native_transport_admission.rs (NativeTransportAdmission)"
  - "novarocks/worker/src/guarded_bytes.rs (bytes_with_exit_guard)"
  - "novarocks/execution/src/exec/chunk/root_array_storage.rs (ARROW_BUFFER_OWNER_METADATA_BOUND)"
---

## 问题

需要对第三方库（Tokio、Hyper、H2、Tonic、Tower、HTTP、Bytes、Arrow 等）内部的内存或任务做上界控制时，为什么不 vendor/fork 它们来取得内部钩子，而是只用公开配置、库外准入和测量？

## 背景与执行事实

NovaRocks 的内存治理分两类对象，保证程度不同：

| 对象 | 例子 | 保证方式 | 退出口径 |
|---|---|---|---|
| NovaRocks 自有对象 | 结果窗口段、行游标、collector、交给传输层的 payload `Bytes`、自建队列 | 事前精确授权 | 最后一个 NovaRocks owner 被 Drop（payload 用上游 `Bytes::from_owner` 观察第三方持有的最后 alias） |
| 第三方内部对象 | HPACK 表、帧缓冲、Hyper/Tonic 任务、Tokio socket 注册与 TaskCell、Tower Buffer 内部、错误 Box | 公开配置限定数量和单项尺寸；NovaRocks 在库外持有计数门；字节为结构上界，用 jemalloc 测量验证 | 公开 API 可观察的事件：IO wrapper 被 Drop、JoinHandle 返回、response body EOF/RST/Drop |
| 第三方内部分配的记账 | 上一行对象的实际字节 | BE 侧由归属 allocator 在执行作用域下分配时归属（见 memory-governance 领域）；FE 侧只有结构上界与测量 | 归属 allocator 的真实 free |

`[patch.crates-io]` 对整个 workspace 全局生效：一旦 patch Tokio，锁文件中所有依赖 Tokio 的包（数十个，包括 AWS SDK、OpenDAL）都跑在私有副本上。path 依赖没有 registry 身份，cargo-deny 对这些包的已知安全公告不再可见，`deny.toml` 中对应的 ignore 条目也会因“未使用”而必须删除，于是公告从治理视野中消失，而不是被修复。

截至本 ADR，仓库只允许以下 vendor patch，各自有 `vendor/*/PATCH.md` 记录接缝与退出条件：`iceberg`、`iceberg-catalog-rest`、`iceberg-catalog-hms`、`paimon`（ADR-0138）、`opensrv-mysql`。它们 patch 的是领域协议/SDK 行为，而不是为了在库内部计量分配。

正例锚点：`NativeTransportAdmission` 用 Tokio 公开的 `Semaphore` 在库外持有每类连接的物理位置与握手位置，位置随连接 IO wrapper 一起退出；`bytes_with_exit_guard` 只用上游 `Bytes::from_owner` 让授权在最后一个 alias 消失时归还；`ARROW_BUFFER_OWNER_METADATA_BOUND` 用计数 allocator 测试钉住上游 Arrow 私有 owner 记录的尺寸，而不是给 Arrow 加 getter。

## 考虑过的选项

**A（选中）只对自有对象精确授权；第三方内部用公开配置、库外准入和测量约束。** 增长控制只依赖公开 API，升级第三方库只需重跑测量门与复核配置语义。代价是第三方内部字节只有结构上界加测量，不能逐字节事前授权。

**B 为取得内部钩子 vendor/fork 第三方库。** 可以对库内每次分配事前授权并证明物理释放。**设计否决**：fork 通过全局 `[patch]` 影响全部依赖它的包；隐藏安全公告；每次升级都要移植与重新审计补丁；对基础网络/运行时栈这类钩子几乎不可能被上游接受，私有分支会无限期存在。曾经的实践中，一个结果投递任务为此在 7 个网络/运行时 crate 与 3 个 Arrow crate 上累积了约 1.5 万行补丁，并引出更多仍无法闭合的内部对象（每个 clone 的 readiness future、错误 Box、DNS 解析退出），说明这条路线没有终点。

**C 自研替代传输/格式实现以拥有全部内部内存。** 例如为结果数据面写私有 TCP 帧协议。**成本否决**：要自行承担流控、TLS、认证、背压与诊断，并偏离“FE/BE 之间用 native gRPC”的边界；控制面与运行时仍在第三方库上，问题只是转移。

同类 Rust 引擎（Databend、RisingWave）的依赖清单中没有 fork Tokio/H2/Hyper/Tonic；这只说明 fork 网络栈不是常规做法，不说明它们对传输内存给出了任何特定上界。

## 裁决

**依赖规则：**
1. **No accounting forks**：不得为取得资源计量、授权或退出观测钩子而 vendor/fork 第三方 crate。
2. **Patch allowlist**：`[patch.crates-io]` 只包含本 ADR 列出的条目；新增条目必须先经设计讨论，并以新 ADR（或 supersede 本 ADR）记录接缝、理由与退出条件，同时提供 `PATCH.md`。
3. **Registry identity**：被 patch 以外的依赖保持 registry 原版本与 checksum；升级第三方库是独立、显式的依赖变更，不借重构顺带发生。
4. **Visible advisories**：已知公告的 ignore 条目必须对应真实的 registry 包；不得用 path 依赖让公告“消失”。

**设计规则：**
5. **Own what you fund**：只有 NovaRocks 自己创建并持有的对象才做事前精确授权；其退出以最后一个 NovaRocks owner 被 Drop 为准。
6. **Bound by public knobs**：第三方内部的数量与单项尺寸用其公开配置限定；公开配置不够时，在库外加 NovaRocks 持有的计数门（信号量、单飞、连接 IO wrapper），位置持到公开退出事件。
7. **Measure the rest**：第三方内部字节以“数量上限 × 配置单项上限 + 测得的每对象固定开销”作为结构上界，用 jemalloc 测量门验证线性与回落；测量失败先补库外准入或修正配置，不以修改第三方库过门。
8. **Pin private layouts by test**：确需引用第三方私有类型尺寸时，用常量加计数 allocator 测试钉住，测试在升级导致尺寸变化时失败。

## 接受的妥协（诚实记录）

**第三方内部字节没有事前逐字节授权。** 单帧解码临时缓冲、任务结构体、Waker、错误对象等只由数量与配置间接约束，并通过测量验证；测量门本身依赖代表性负载，不是形式证明。

**公开退出事件可能早于内部全部 free。** 例如连接 IO wrapper 已 Drop 时 H2 仍在析构 stream 表；差额是有限固定开销，由测量覆盖、由归属 allocator 记账，而不是由位置持有到物理 dealloc。

**一些上游语义观察不到。** 例如 H2 SETTINGS 交换何时完成、GOAWAY 关闭阶段；对应的控制点要改用可观察的事件（首个认证请求头、IO Drop），语义比钩子版本更粗。

**常量钉住私有布局会随升级失效。** 这是有意的：失效以测试失败的形式出现，提醒复核，而不是静默漂移。

## 何时重新评估

- 某个第三方库在公开 API 中提供了所需的计量或退出钩子：改用公开接口，并删除对应的库外近似。
- 测量门持续出现无法用库外准入或配置解释的非线性增长或不回落：先回到设计讨论，评估更换实现（选项 C）或升级版本，而不是 fork。
- 某个第三方依赖停止维护、出现无法规避的安全问题或许可证变化：评估替换依赖；vendor 只作为有退出条件的过渡，并按规则 2 单独立 ADR。
- 产品需要对 FE 进程建立单一内存账本：重新审视 FE 侧第三方内部内存只做结构上界与测量的范围。
