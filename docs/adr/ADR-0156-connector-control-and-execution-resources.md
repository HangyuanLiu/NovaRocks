---
id: ADR-0156
title: "Connector 操作控制与执行资源为何分离，并在 BE 准入后绑定"
domain: [runtime-role, memory-governance]
status: active
supersedes: []
superseded-by: null
date: 2026-09-23
provenance:
  - "discussion: UEA-4A-4 spec v9 与 plan v9"
code-anchors:
  - "novarocks/spi/src/connector/context.rs (ConnectorRequestContext)"
  - "novarocks/spi/src/connector/binding/role.rs (ConnectorExecutionReadBinding)"
  - "novarocks/worker/src/typed_scan_runtime.rs (admitted_connector_resources_for_bound_task)"
  - "novarocks/native-adapter/src/fragment_typed_connector_scan.rs (new_deferred)"
  - "novarocks/frontend-application/src/task_execution/blocking_io.rs"
  - "novarocks/connector/paimon/src/resources.rs"
  - "novarocks/fs/src/runtime.rs (FileCancellation::from_connector_request)"
---

## 问题

同一条 Connector 请求既可能在 FE 读取 catalog、metadata、manifest 并规划 split，也可能在 BE
读取数据页。如果公共请求 context 同时承载取消、授权和内存账本，那么 FE 的普通发现工作会被误认为已获
BE 容量，BE 也可绕过任务准入提前创建 reader。额外的 FE 请求字节门、文件数门和桥接 semaphore
会在 SDK 已经构造对象之后才拒绝，不能保证避免分配失败，却能阻塞另一条独立请求。

## 考虑过的选项

**公共 `NoQuota / Managed` 模式。** 它让调用方先选择资源模式，再走同一套申请接口。`NoQuota`
只能返回空 lease，表面调用没有真实容量责任；模式分支继续混合 FE 发现和 BE 读取。这是设计否决。

**保留公共账本，只放大 FE 配额。** 数值改变不能修正资源所有权，也无法限制 SDK 在检查点之前的完整
构造。这是设计否决。

**拆开操作控制、授权访问和 BE 执行资源。** FE 使用可取消、带 deadline、准确 generation 的
请求和授权 FileIO；BE 在真实任务 tracker 安装后，把必填执行资源交给 provider factory。这是裁决。

## 裁决

1. `ConnectorRequestContext` 只携带操作控制、请求 scope、payload 协议预算与授权访问能力，
   不携带资源字段、`with_resources` 或资源申请入口。FE metadata、manifest、split discovery 的
   retained 对象使用普通 Rust 所有权；不设置 Connector 专属字节/文件数总额。
2. BE 的 `ConnectorExecutionReadBinding` 只暴露必填 `ConnectorExecutionResources` 的 admitted
   factory。Native 先验证和解码静态 plan，再由 Worker 准入、安装精确 fragment tracker，最后在
   `ScanSource::bind` 创建 page-source 或 system-table provider。资源不足是拒绝，不退化为无账本
   或另一条容量权威。
3. Paimon FE SDK 路径只有活性与授权检查；BE 文件读取、schema、Parquet、merge 和输出持有
   对应真实 reservation；SDK `Table` 的 schema 深拷贝也在分配前另行预留，并随 reader 持有。
   静态类型区分普通 FE 所有权和 BE charged 所有权。Iceberg 文件读取把
   原请求取消接入 FileCancellation，并保留独立 deadline 和授权；外部 HTTP 中不可打断的 await
   返回后再检查取消。
4. FE source-open 等阻塞调用只由其操作 owner 监督、等待真实退出；不再附加 Connector 专属
   semaphore 或 request-ledger。Native wire payload、格式完整性和执行侧真实容量检查仍由各自
   owner 执行，不把它们解释成 FE discovery 配额。

## 接受的妥协

FE discovery 仍可能由 SDK 一次性构造完整结果，普通 Rust 分配没有可恢复的硬上限。本裁决不承诺
避免所有 OOM；若要有分配前上界，需要 provider/SDK 提供真正的流式、有界发现协议，并另行设计。
取消也不是抢占：已经进入不可中断的外部 SDK/HTTP 调用只能在返回后的检查点退出。

## 何时重新评估

- Iceberg/Paimon SDK 提供可暂停且有明确 retained owner 的流式枚举时，设计分配前的有界发现。
- BE 引入新的持有内存对象时，把 charge 接到该对象的最后持有者，并用真实 tracker 验证拒绝与释放。
- 外部 I/O 提供可靠的主动取消接口时，把请求取消直接下传，而不只在调用前后检查。
