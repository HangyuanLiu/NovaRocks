---
title: "CI-20260803：恢复 Iceberg distributed rewrite 的校验、提交与可见性语义"
date: 2026-08-03
type: design-spec
status: archived
pr: "https://github.com/NovaRocks/NovaRocks/pull/832"
tags:
  - dev-workflow/design
  - dev-workflow/specs
---

# 恢复 Iceberg distributed rewrite 的校验、提交与可见性语义

## 问题

distributed rewrite cutover 后，Iceberg suite 出现三个确定性回归：V2 Parquet position-delete rewrite 被过早的 generic v3 guard 覆盖；成功 OPTIMIZE 后 metadata table 仍看不到 Replace snapshot；包含 COW mutation history 的 v3 row-lineage 表在 aggregate commit 时被错误判定为没有拥有全部 live files。测试 runner 又只等待 `FINISHED`，使已经进入 `FAILED` 的 job 表现为 300 秒超时。

## 当前行为与代码证据

- `novarocks/core/src/connector/iceberg/distributed_rewrite.rs::build_plan` 在文件分类前先拒绝非 v3 table，覆盖了原有 `V2 Parquet position delete rewrite is not supported` 的明确错误。
- `novarocks/core/src/connector/iceberg/commit/rewrite_position_delete_files.rs::classify_delete_file_for_rewrite` 已定义并测试 V2 Parquet position delete 的精确拒绝语义；旧路径先规划、分类候选文件，再对 Puffin rewrite 检查 v3。
- `novarocks/core/src/connector/iceberg/commit/selected_rewrite.rs` 用当前 snapshot 的全部 live data/delete manifest paths 与 frozen ownership 做集合相等校验。
- planner 的 `extract_data_files_with_stats` 通过 read snapshot 返回 live data files，并只把仍适用于这些 data files 的 delete files 附着到对应 cohort；COW 替换 referenced data file 后，当前 snapshot 仍可能保留一个 live、但不再附着到任何 data file 的 delete manifest entry。
- 同一 base snapshot 上，planner 冻结了 2 个 live data files，但漏掉上述孤立 live delete path；commit validator 正确看到完整 live manifest set，因此触发 `selected data rewrite does not own every live data and delete file`。
- 隔离串行复现的 StateStore 显示 OPTIMIZE job 在约 80 ms 内进入 `FAILED`，错误为上述 ownership mismatch；随后 operation 停留在 `RECONCILE_PENDING`，因为 snapshot marker 从未提交。
- `IcebergDistributedRewriteAdapter::finalize_rewrite` 没有 invalidation 当前 table cache；成功 row-lineage OPTIMIZE 后，数据不变量通过，但 `$snapshots` 查询仍返回 0 个 Replace snapshot。
- `tests/sql-test-runner/src/main.rs::execute_wait_alter` 只识别输出中的 `FINISHED`，不识别 `FAILED` 及其错误文本。
- serial full CI 与 targeted serial rerun 均稳定复现三个 SQL case 失败。

## 目标

- 恢复 position-delete rewrite 的 provider-specific 错误优先级。
- 让 frozen planner 与 aggregate commit 对同一 snapshot 使用一致的逻辑 live-file 集合。
- 成功提交或 authoritative reconcile 为 committed 后，使后续普通表查询和 metadata-table 查询立即看到新 snapshot。
- 让 distributed rewrite receipt 使用已有 connector receipt 中的 committed snapshot 与 resulting row count，避免丢失已经冻结的提交事实；输出文件指标仅在有可证明来源时填写。
- 让 SQL runner 在 ALTER job 已 `FAILED` 时立即失败并打印失败原因，不再等待到超时。

## 非目标

- 不改变 ADR-0029 的 one group per cohort、one aggregate commit、one snapshot 语义。
- 不增加 partial data rewrite fallback；data rewrite 仍必须精确拥有 frozen logical live set。
- 不把 unknown commit 当成 uncommitted，不在 marker 缺失时猜测成功。
- 不更新 SQL golden，不放宽 row-lineage identity 断言。
- 不实现 distributed rewrite 的跨 frontend-incarnation恢复。
- 不改变 Hadoop CTAS 能力。

## 设计裁决

### 1. Position-delete 校验顺序

planner 必须先基于 frozen snapshot 分类 position delete 文件。只要发现 Parquet position delete，返回既有精确错误；只有候选是 Puffin DV 且需要执行 rewrite 时，才要求 format v3。无候选时继续保持 no-op，不因 table version 单独制造副作用或错误。

### 2. Logical live-file ownership

ownership validator 与 planner 必须基于同一 snapshot 的完整 live set。data cohorts 继续由 read snapshot 冻结，以保留适用 delete files、sequence、spec id 与 row-lineage 事实；planner 还必须枚举当前 snapshot 的全部 live delete manifest paths，并把未附着到任何 data cohort 的路径稳定归属给一个 canonical cohort，使 aggregate Replace 精确拥有 validator 所要求的全集。

不得通过放宽集合相等为 subset 来修复；否则 frozen plan 可能漏删 live files 或把 partial rewrite 错当 whole-table replacement。

### 3. 提交结果与 cache 可见性

known-committed 与 reconcile-to-committed 路径都必须在返回成功前 invalidation exact catalog/table cache。invalidation 只使后续加载重读 catalog authority，不替代 snapshot marker、receipt 或 StateStore 终态。

distributed rewrite final receipt 从 `ConnectorWriteReceipt` 的 committed version 与 resulting row count 派生 `target_version` 和 `output_rows`。output file counts 若当前 receipt 没有权威事实，保持明确的 unknown/zero contract或扩展 provider-private bounded payload；不得从 plan 的 expected count 猜测实际输出。

### 4. ALTER wait 终态

SQL runner 的 wait helper 同时识别 `FINISHED` 与 `FAILED`。`FAILED` 必须立即返回失败，并把 SHOW 结果中的 error message 写入 case log。其他非终态继续按现有间隔轮询。

## 边界与关键语义

- planner、writer source 与 committer 都绑定同一个 base snapshot、table UUID、schema id、spec id 和 target ref。
- frozen ownership 必须覆盖 read snapshot 的全部 live data files，以及当前 snapshot delete manifests 中的全部 live delete paths；孤立 delete path 也必须恰好由一个 cohort 拥有。
- row-lineage data rewrite 必须保留 `_row_id` 与 `_last_updated_sequence_number`；`next_row_id` 不得前进。
- 成功 data rewrite snapshot 的 operation 必须为 `replace`，并携带 canonical distributed-rewrite marker。
- commit error 若发生在 catalog mutation 前应为 known-uncommitted；只有确实无法判断外部提交结果时才返回 unknown。
- cache invalidation 不能改变 exact-generation lease 或把当前 generation 替代 frozen generation。
- runner 的 FAILED 识别不能把 error message 中偶然出现的单词当作 job state；应解析 SHOW 返回的状态列或使用有边界的行匹配。

## 验收标准

- 单元测试覆盖：
  - V2 Parquet position delete 优先返回精确 unsupported 错误；
  - v2 table 没有可 rewrite position delete 时保持 no-op；
  - read snapshot 未附着、但 delete manifest 仍标记 live 的路径被稳定归属给 canonical cohort；
  - frozen ownership 对 COW update/delete snapshot 覆盖完整 live data/delete set；
  - final receipt 包含 committed snapshot id 与 resulting row count；
  - wait helper 对 FINISHED、FAILED、非终态分别处理，并保留失败文本。
- targeted serial SQL rerun 在 cross-process 1FE+3BE、`-j 1` 下通过：
  - `iceberg_spark_procedures_errors`
  - `iceberg_v3_optimize_row_lineage`
  - `iceberg_v3_row_lineage_uniqueness`
- `iceberg_v3_optimize_compact_data_files` 与 `iceberg_v3_rewrite_position_delete_files` 继续通过，防止普通 data rewrite 和 Puffin DV rewrite 回归。
- `iceberg_v3_optimize_row_lineage` 仍断言 1 个 Replace snapshot且 row-lineage triples 完全不变。
- `iceberg_v3_row_lineage_uniqueness` 两次 OPTIMIZE 都完成，当前与历史 tag 的 I1/I2 不变量保持通过。
- `git diff` 不包含 SQL result golden 更新。
- 串行 full CI 的 iceberg suite 27/27 通过，其他稳定 suite 无回归。

## 风险与取舍

- 额外读取 delete manifests 会增加一次规划 I/O；实现只提取有界路径集合，并保留 read snapshot 作为 data cohort 与适用 delete 关系的权威来源。
- cache invalidation 会增加一次后续 catalog load，但比返回 stale metadata 更可控。
- runner fail-fast 会缩短失败用例耗时并暴露原始错误，可能使历史上依赖超时文本的诊断脚本需要调整；SQL result golden 不受影响。
- receipt 若要扩展输出文件计数，需要版本化 bounded provider payload；本修复不为补齐展示指标而扩大 generic SPI。

## 实现阶段可决定的事项

- live delete path 枚举可放在 planner 或共享 helper；必须绑定 frozen base snapshot，并且不能替代 read snapshot 的 cohort/row-lineage 事实。
- cache invalidation 放在 adapter finalize、write-control committed hook 或 frontend engine boundary；必须覆盖 direct commit 与 reconcile 两条成功路径。
- runner 是否抽取结构化 ALTER job row parser，或在现有 MySQL result 模型上读取状态列。
- provider-private receipt payload 是否在本次补齐 output file counts；若无权威数据来源，可留作后续独立工作。

## 相关文档

- `docs/adr/ADR-0029-connector-distributed-rewrite-contract.md`
- `novarocks/core/src/connector/iceberg/distributed_rewrite.rs`
- `novarocks/core/src/connector/iceberg/commit/selected_rewrite.rs`
- `novarocks/core/src/connector/iceberg/commit/overwrite_partitions.rs`
- `novarocks/core/src/connector/iceberg/commit/rewrite_position_delete_files.rs`
- `tests/sql-test-runner/src/main.rs`
- `sql-tests/iceberg/sql/iceberg_spark_procedures_errors.sql`
- `sql-tests/iceberg/sql/iceberg_v3_optimize_row_lineage.sql`
- `sql-tests/iceberg/sql/iceberg_v3_row_lineage_uniqueness.sql`

## 实现计划

- [[CI-20260803-ci-regressions-implementation-plan]]
