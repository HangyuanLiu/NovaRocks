# TPC 基线：20260622 1FE + 3BE dev-opt

这个目录用于长期维护 TPC plan 与运行时分析基线。这里刻意只保留适合 review 和 git diff 的稳定产物，临时日志、二进制、pid 文件仍留在被忽略的 `logs/` 目录下。

## 范围

- 集群形态：`1 FE + 3 BE`
- NovaRocks build profile：`dev-opt`
- Benchmark suites：SSB scale 1，TPC-H scale 1，TPC-DS scale 1GB
- NovaRocks 运行模式：`EXPLAIN ANALYZE`
- NovaRocks 结果来源：
  `/Users/harbor/.codex/worktrees/4f87/NovaRocks/logs/baseline/20260622-231416-tpc-baseline`
- StarRocks FE plan 来源：
  `/Users/harbor/project/NovaRocks/logs/plan-compare/20260613-201518`

StarRocks FE plan 是从更早的一次 plan-compare 采集中拷贝过来的，适合作为 plan shape 参照。后续如果重新跑 StarRocks FE 侧 plan，应新增一个 dated baseline，或者明确更新本目录并同步修改 metadata。

## 目录结构

- `plans/starrocks-fe/`
  StarRocks FE 生成的 plan 基线，按 suite 分组。
- `plans/novarocks-explain-analyze/`
  本次 NovaRocks `EXPLAIN ANALYZE` 输出，按 suite 分组。
- `reports/`
  本次基线生成的 CSV / JSON 汇总与中文分析报告。
- `metadata/starrocks-fe-plan-compare/`
  StarRocks FE plan-compare 采集的来源 metadata。
- `metadata/current-run/`
  本次 NovaRocks run 的状态、配置和 SQL manifest metadata。

## 产物数量

- StarRocks FE plan 文件：138
  - SSB：13
  - TPC-H：22
  - TPC-DS：103 个 query section
- NovaRocks `EXPLAIN ANALYZE` result 文件：134
  - SSB：13
  - TPC-H：22
  - TPC-DS：99 个 case 文件
  - TPC-DS 多段 query 在 `reports/explain_sections.csv` 中按 103 个 query section 记录。

## 关键报告

- `reports/SUMMARY.md`
  基线运行汇总和慢节点概览。
- `reports/PLAN_GAP_AND_EXEC_HOTSPOTS.md`
  对照 StarRocks FE 参照 plan 的差距分析，以及执行层慢节点优化优先级。
- `reports/plan_gap_node_heading_current_nr_vs_starrocks_fe.csv`
  query 级 plan shape 对比。
- `reports/plan_gap_node_heading_aggregate_current_nr_vs_starrocks_fe.json`
  suite 级 plan shape 对比。
- `reports/slow_nodes.csv`
  所有带 `act` 时间的 operator 明细，按节点时间排序。

## 维护规则

1. 这里只保留可复现、适合 review 的结果产物，不提交 raw build output 或 binary。
2. 完整重跑时优先新增 dated baseline 目录。
3. 如果需要原地刷新本基线，应同步更新 README 和 metadata，让 reviewer 能看清变化来源。
4. StarRocks FE 参照 plan 与 NovaRocks `EXPLAIN ANALYZE` 输出必须保持分离；二者回答的问题不同。
