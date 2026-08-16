---
id: ADR-0074
title: "Frontend durable records use bounded canonical encoding and whole-record budgets"
domain: [frontend-durable-records]
status: active
supersedes: []
superseded-by: null
date: 2026-08-16
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/906"
  - "discussion: 2026-08-16 canonical encoding budget acceptance"
code-anchors:
  - "novarocks/frontend/src/durable/mod.rs (DurableRecordStore)"
---

## 问题

Frontend 的多个 durable owner 应各自把记录直接编码为 StateStore `Value`，还是应共享一个在类型和写入入口上都强制整记录预算的契约？

## 背景与执行事实

StateStore 对单个值施加全局大小上限，而 JSON 将裸 `Vec<u8>` 编码为数字数组，放大比例随字节值变化，不能从原始字段长度可靠地推导完整记录大小。此前 DML 已使用有界 opaque 字节与 canonical lowercase hex；statistics jobs、table maintenance、views 和 catalog attachments 则分别直接编码并写入。

外部 provider handle 还可能随 metadata 与 manifest 历史增长。一个具体的 ANALYZE job 曾因旧的数字数组表示超出单值限制；改为 canonical hex 后，同一完整记录实测为 42,974B，落入 60KiB 的记录预算。这说明预算必须依据最终候选记录的实际编码，而不能为了保留历史失败模式设置另一套虚构阈值。

## 考虑过的选项

1. 各 repository 保留独立的 JSON 编码、字段界和错误映射。局部改动最少，但新的 durable owner 仍会重复遗漏整记录预算校验。
2. 只要求每个 repository 在写入前手工检查大小。可以改善当前路径，但正确性依赖调用纪律，且无法保证后续状态转换不会绕过检查。
3. 以共享 `DurableRecordStore`、有界 opaque 类型和不可直接写入的已检查记录值收敛全部记录写入；索引等非记录小值使用独立的小值入口。该方式让编码、预算、错误脱敏和写入漏斗在同一边界成立。
4. 增大 StateStore 全局限制，或为大记录引入分片、side record、外部 artifact。它们改变了公共存储契约或 durable 模型，不能作为这次收敛的隐式替代方案。

## 裁决

选择选项 3。每种 frontend durable record 声明 record kind、schema version 和编码上界；不透明 byte 字段以有界、canonical lowercase hex 的类型表示。repository 在 create 与每次状态转换时先编码完整候选记录，比较实际编码长度与 `min(record limit, StateStore limit)`，成功后只能将已检查值交给 record writer。

超预算错误必须携带 record kind、schema version、实际字节数和生效预算，但不得包含 opaque 内容。CAS、fence、状态机和恢复语义仍由原 repository 负责；索引与控制 marker 不伪装成记录，而走独立的有界小值入口。

canonical 表示变紧凑而使某个既有请求恢复，是该契约的有效结果，不为维持旧错误引入兼容预算。它不意味着外部历史增长问题已经解决：后续的 analyze-job 外部提交边界重构仍必须移除 durable handle，并在 attempt 生命周期内重新绑定。

## 接受的妥协（诚实记录）

hex 比 base64 更大，但选择它是为了与既有 DML durable 形式完全一致、可预计算并可 fail-closed 地验证，不是因为它的存储密度最佳。这个统一契约也不消除 provider handle 随外部历史增长的根因；它只确保增长越界时在外部副作用前得到可定位、无内容泄漏的失败。当前一个 fixture 恢复通过是表示变更的副作用，不能被误读为已经交付了长期的统计恢复模型。

## 何时重新评估

- StateStore 的持久化编码或单值预算发生架构级变化时；
- 新 durable owner 需要无法由有界字段与完整记录预算表达的事实时；
- canonical hex 无法满足已批准的存储成本或跨语言兼容性要求时；
- analyze job 完成外部提交边界重构后，需要确认本契约仍覆盖其新 schema 与 evidence 终态。
