---
id: ADR-0016
title: "Connector control and execution runtime role separation"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-07-30
provenance:
  - "discussion: 2026-07-30 SPI-4A connector control and execution role separation"
code-anchors:
  - "novarocks/frontend/src/connector/control_host.rs (control generation and planning lease)"
  - "novarocks/backend/src/connector/execution_host.rs (execution binding and query lease)"
  - "novarocks/spi/src/connector (role-specific contracts)"
---

## 问题

同一 Connector catalog identity 如何同时支持 FE metadata/planning 与 BE read execution，而不让两个进程共享 runtime object、registry 或 capability aggregate？

## 背景与执行事实

Connector 的 catalog client、metadata、snapshot planning 与 catalog lifecycle 属于 FE control plane；文件 access handle、reader、cache 与 cancellation task 属于每个 BE execution process。两者需要一致的 provider、instance identity 与 generation，才能避免 drop/recreate 和迟到 fragment 读到错误 catalog；但它们处在不同进程、故障域和生命周期，不能共享 Rust instance 或用 all-in-one shortcut 绕过 wire boundary。

此前 `ConnectorInstance` 同时暴露 metadata、planning、distribution 与 `open_reader`，core `ConnectorHost` 同时承担 FE 和 BE registry。这把 capability aggregate 与 process host 混为一个 owner，也使 BE 看见不应拥有的 catalog control capability。

## 考虑过的选项

1. 保留一个跨角色 aggregate instance，仅在部署时约定哪些方法可调用。实现改动较小，但 capability 泄漏仍由类型系统无法阻止，且 all-in-one 容易绕过真实分发路径。
2. 让 BE 每次从 fragment 重新连接 catalog 并构造 reader。无需 install barrier，但会传递配置或 credential，无法安全处理 generation、catalog drop 和可重复重试。
3. 共享 immutable logical binding key，分离 FE control binding 与 BE execution binding；FE 以 planning lease 跨越 install barrier，BE 以 query lease 绑定到现有 query lifecycle。

## 裁决

采用选项 3。SPI 明确区分 control 的 metadata/scan planning/execution distribution 与 execution 的 read binding/installer/resolver。每个 binding 都以 `{instance_id, incarnation}` 标识；provider ID 只用于 BE installer 选择和诊断，绝不进入 fragment carrier。

frontend 独占 `ConnectorControlHost`：catalog lifecycle register/retire control binding，并以 planning lease 防止 drop/recreate 与 ensure barrier 竞态。backend 独占 `ConnectorExecutionHost`：安装 startup-composed execution binding，按 query lease 给 fragment resolver 暴露精确 generation；retiring generation 拒绝新 query，已租用 reader 可 drain。Ensure/retire 是控制 RPC；fragment 只携带 binding key 和 opaque connector payload，generic decoder 不安装、不分支、不解析 provider 业务。

## 接受的妥协（诚实记录）

同一进程 all-in-one 测试仍须经过 FE control host、ensure RPC/port 和 BE execution resolver，增加了一些组合代码，但避免测试便利扭曲生产边界。远端 retire 是 best-effort 资源回收；正确性由 FE generation、planning lease 和 BE key 校验保证。

SPI-4A 保留 Iceberg 的 crate 物理位置与已裁决的 read correctness，不迁移 catalog mutation、write staging、maintenance 或 predicate pushdown。旧 FE-only application registry 会暂时存在，直到后续 capability 任务逐项迁移；它不得再进入 native decode 或 backend execution。

## 何时重新评估

- 多 FE 或跨 FE failover 需要 control binding durable fencing；
- execution declaration 需要在 BE restart 后持久恢复；
- provider 需要非读 execution capability，且其生命周期不能附着于 query；
- 未来独立 connector crate 证明 SPI contract 仍引用 core-private runtime 类型。
