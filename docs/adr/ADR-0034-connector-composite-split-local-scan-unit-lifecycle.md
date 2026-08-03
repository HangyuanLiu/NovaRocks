---
id: ADR-0034
title: "Connector composite split and local scan-unit lifecycle"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-08-03
provenance:
  - "discussion: 2026-08-03 connector composite scheduling and local reader lifecycle"
code-anchors:
  - "novarocks/spi/src/connector/execution.rs (ConnectorReadExecution)"
  - "novarocks/core/src/connector/runtime.rs (ConnectorReadScanSource)"
---

## 问题

Connector 的 cluster scheduling work 与 Backend 本地 reader work 是否应继续由同一个 split 表示，以及哪个角色负责冻结、认证和调度物理 leaf membership？

## 背景与执行事实

现有 `ConnectorSplit` 同时承担 FE placement、native carrier、Core morsel 和 provider reader input。一个 split 因而只能对应一个 morsel 与一次 reader open，既不能以有界 composite work 降低小文件的 cluster 调度开销，也不能让一个大 Parquet file 的多个 row group 在同一 Backend 独立调度。把 physical file、row group 或 segment 放进通用 protobuf会泄漏 provider semantics；让 BE 在 open 时按 latest metadata 重建 membership 会破坏 FE 的 pinned snapshot、generation 和 retry identity。

## 考虑过的选项

1. 保持一 split 一 reader，并在 Core 加 provider callback。改动较小，但本地 admission 仍无法拥有稳定 leaf，且 callback会混淆 provider 与 runtime owner。
2. 在通用 native wire 增加 file、row-group、segment DTO。调度可见，但把 table-format 和 storage provider semantics固化到 protocol，阻碍独立演进。
3. 让 FE Provider冻结 bounded opaque composite split，BE Provider认证并 materialize sealed local unit set，Core只调度和打开 prepared unit。边界增加一个阶段，但每个角色只拥有自己可证明的事实。

## 裁决

采用选项3。`ConnectorSplit`是 FE 冻结的 cluster work package，包含 stable membership、aggregate cost和bounded provider-private payload；它不是 reader handle。BE `ConnectorReadExecution`必须先 `prepare_split`，验证 exact binding/incarnation、schema/generation、payload和membership，再原子返回非空、bounded、stable-ordinal prepared unit set。Core为每个prepared unit建立本地morsel，reader只接受prepared unit。

native wire继续只携带 split ID、opaque payload与aggregate cost。Provider可以在opaque codec中携带自己的file、row-group、segment和remote token facts；BE不得依赖latest metadata新增、删除、合并、拆分或重排unit。prepare失败不发布unit；terminal cancellation阻止新的prepare和reader open并关闭已注册reader。

## 接受的妥协（诚实记录）

该边界会让Iceberg在FE多做footer metadata I/O，也会让StarRocks direct在BE认证frozen storage metadata；选择它是为了消除split同时承担三个生命周期的长期歧义，而不是因为增加阶段本身更简单。P0不引入unit级distributed retry、page-level unit或Runtime Filter facts；大于显式split hard limit的不可再分leaf必须失败，而不是偷偷降级到whole-file reader。

prepared set允许携带provider-private共享与unit payload，并要求严格资源上限。这会复制少量codec数据，但避免在Core建立provider-global mutable membership registry或跨进程runtime object。

## 何时重新评估

- 新provider需要一个unit在多个Backend协同完成，无法保持一个FE授权的local placement；
- query retry需要独立于fragment retry的unit-level durable progress；
- opaque payload的可验证资源上限无法容纳真实、稳定的provider membership；
- Runtime Filter contract已独立接受，需要在此稳定unit identity之上增加事实或effect，而非回写split语义。
