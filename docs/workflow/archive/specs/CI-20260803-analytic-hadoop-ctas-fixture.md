---
title: "CI-20260803：解除 analytic 用例对 Hadoop CTAS 的错误依赖"
date: 2026-08-03
type: design-spec
status: archived
pr: "https://github.com/NovaRocks/NovaRocks/pull/832"
tags:
  - dev-workflow/design
  - dev-workflow/specs
---

# 解除 analytic 用例对 Hadoop CTAS 的错误依赖

## 问题

`analytic_test_window_function_with_join` 的测试目标是验证窗口函数、CTE 与 legacy join hint 的结果一致性，但其 fixture 使用两条 `CREATE TABLE ... AS SELECT` 创建 `nt1` 和 `nt2`。该 suite 的自动 catalog 是 Hadoop Iceberg catalog，而 frontend CTAS cutover 后会按 ADR-0030 在 source execution 和任何外部副作用前拒绝没有 atomic staged-publication capability 的 provider，导致真正的 analytic 查询尚未执行便失败。

## 当前行为与代码证据

- `sql-tests/analytic/sql/analytic_test_window_function_with_join.sql` 的 Test Objective 只覆盖窗口函数、CTE 和 join hint；CTAS 不是被测能力。
- 同一文件的 query 1 已显式创建 `nt0`、`nt3`，但用 CTAS 创建 `nt1`、`nt2`。
- `novarocks/spi/src/connector/control.rs` 的 `derive_staged_create_lease` 在 capability 缺失时返回 `connector control generation has no atomic staged-create capability`。
- `docs/adr/ADR-0030-frontend-ctas-staged-publication.md` 规定：没有等价 atomic staged-publication capability 的 provider 必须在 source execution 与任何外部副作用前返回 typed `Unsupported`；Hadoop catalog 在 fencing 能力完成前不得广告支持。
- 串行 CI 与 targeted serial rerun 均在 query 1 复现该拒绝，尚未进入 query 2-5 的 analytic 断言。

## 目标

- 让该 analytic 用例只依赖所有测试 catalog 已支持的显式建表与 INSERT 路径。
- 保留 `nt1`、`nt2` 与 `nt0` 相同的数据和 schema，使 query 2-5 的语义、规模与 join-hint 覆盖不变。
- 恢复 `analytic_test_window_function_with_join` 在串行 cross-process CI 中通过。

## 非目标

- 不为 Hadoop catalog 实现或模拟 atomic staged publication。
- 不放宽 frontend CTAS 的 fail-fast 安全边界。
- 不修改 analytic result golden，不改变 query 2-5 的预期结果。
- 不把该用例迁移到 REST catalog，也不改变 suite 级 catalog 配置。

## 设计裁决

在 fixture 中用与 `nt0` 相同的显式 schema 和 table properties 创建 `nt1`、`nt2`，随后分别执行 `INSERT INTO ... SELECT * FROM nt0`。不在生产 CTAS 路径中增加 Hadoop fallback，也不在测试 runner 中对该错误做特殊处理。

## 边界与关键语义

- `nt1`、`nt2` 必须继续包含 `nt0` 的全部 16384 行。
- 三列的名称、顺序、类型和 nullable 语义必须与原 CTAS 结果一致。
- table format 继续显式为 Iceberg v3，与 `nt0`、`nt3` 一致。
- query 2-5 的 SQL 文本和 result golden 保持不变。
- Hadoop CTAS 的独立负向覆盖继续由 `sql-tests/iceberg-dml/sql/ctas.sql` 负责。

## 验收标准

- targeted serial rerun：`analytic_test_window_function_with_join` 在 `--cluster-mode cross-process --cluster-size 3 -j 1` 下通过。
- `sql-tests/iceberg-dml/sql/ctas.sql` 中 Hadoop staged-create unsupported 断言仍通过。
- `git diff` 不包含 analytic result golden 变更。
- 串行 full CI 的 analytic suite 34/34 通过。

## 风险与取舍

- fixture 会增加两段显式 schema，存在未来 schema 漂移风险；三张表的 schema 应保持并列、易比较。
- 显式 INSERT 与 CTAS 都会执行两次全表复制，测试数据规模和主要运行成本基本不变。
- 该修复恢复的是测试意图，不恢复 Hadoop CTAS 能力；这是 ADR-0030 明确接受的短期能力缺口。

## 实现阶段可决定的事项

- 是否通过紧邻注释说明不使用 CTAS 的原因。
- 三张相同 schema 是否保持展开书写，或使用测试框架已有且不改变可读性的 fixture 复用机制。

## 相关文档

- `docs/adr/ADR-0030-frontend-ctas-staged-publication.md`
- `sql-tests/analytic/sql/analytic_test_window_function_with_join.sql`
- `sql-tests/iceberg-dml/sql/ctas.sql`

## 实现计划

- [[CI-20260803-ci-regressions-implementation-plan]]
