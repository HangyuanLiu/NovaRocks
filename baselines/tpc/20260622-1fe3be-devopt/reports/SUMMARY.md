# NovaRocks TPC 基线分析报告

Artifact 根目录：`/Users/harbor/.codex/worktrees/4f87/NovaRocks/logs/baseline/20260622-231416-tpc-baseline`

## 运行配置

- NovaRocks binary：`bin/novarocks-dev-opt`
- SQL runner：`bin/sql-tests-dev-opt`
- 集群形态：cross-process，`1 FE + 3 BE`，即 `--cluster-size 3`
- 运行模式：`EXPLAIN ANALYZE`，通过 `--mode record --record-from target --update-expected` 记录结果
- 数据规模：SSB scale 1，TPC-H scale 1，TPC-DS scale 1GB
- Spark 数据准备：使用本次 run-local 的 `bootstrap_benchmark_data_spark4g.sh`，参数为 `--driver-memory 4g`、`--master local[4]`
- Docker fixture：MinIO、Iceberg REST、Spark 均已启动并验证健康

## 结论概览

这次 NovaRocks dev-opt 基线在 `1 FE + 3 BE` 形态下完成了 SSB、TPC-H、TPC-DS 的 `EXPLAIN ANALYZE` 采集。三套 benchmark 均通过，没有 query 超时或运行失败。

需要注意的是，报告中的“慢节点”来自 plan 里的 `act={time=...}` 运行时计数器。它适合定位算子热点，但不一定能和 case 端到端耗时相加对齐，尤其 TPC-DS 中 exchange wait、driver 调度、分布式同步等开销不会完整体现在某一个 operator 的 `act` 时间里。

## Suite 汇总

| Suite | Total | Pass | Fail | CPU time | Wall time |
|---|---:|---:|---:|---:|---:|
| ssb | 13 | 13 | 0 | 50.31s | 50.39s |
| tpc-h | 22 | 22 | 0 | 176.12s | 178.11s |
| tpc-ds | 99 | 99 | 0 | 704.68s | 709.04s |

TPC-DS 原始 SQL 中 `q14`、`q23`、`q24`、`q39` 是多段 query 文件；本次报告按 99 个 case 文件、103 个 query section 采集并记录。

## SSB 慢 Case

| Rank | Case | Elapsed |
|---:|---|---:|
| 1 | q4.1 | 6.07s |
| 2 | q4.2 | 4.84s |
| 3 | q4.3 | 4.48s |
| 4 | q3.2 | 4.43s |
| 5 | q2.1 | 4.30s |
| 6 | q3.3 | 4.28s |
| 7 | q3.1 | 4.12s |
| 8 | q3.4 | 3.83s |

## SSB 慢节点

| Rank | Case | Step | Node | Operator | Time | Rows | Peak |
|---:|---|---:|---:|---|---:|---:|---:|
| 1 | q4.1 | 1 | 14 | HASH AGGREGATE (LOCAL, group by: [d_year, c_nation]) | 451.5ms | 90424 | 1.9MB |
| 2 | q2.1 | 1 | 11 | HASH AGGREGATE (LOCAL, group by: [d_year, p_brand]) | 275.1ms | 45415 | 1.8MB |
| 3 | q4.1 | 1 | 4 | HASH JOIN (BROADCAST, INNER, eq: [lo_partkey = p_partkey]) | 219.5ms | 2639872 | 54.8MB |
| 4 | q3.1 | 1 | 11 | HASH AGGREGATE (LOCAL, group by: [c_nation, s_nation, d_year]) | 209.3ms | 247214 | 2.2MB |
| 5 | q2.1 | 1 | 4 | HASH JOIN (BROADCAST, INNER, eq: [lo_orderdate = d_datekey]) | 176.4ms | 6008839 | 12.6MB |
| 6 | q4.2 | 1 | 10 | HASH JOIN (BROADCAST, INNER, eq: [lo_suppkey = s_suppkey]) | 175.4ms | 109948 | 618.2KB |
| 7 | q4.2 | 1 | 7 | HASH JOIN (BROADCAST, INNER, eq: [lo_partkey = p_partkey]) | 174.7ms | 816693 | 56.2MB |
| 8 | q4.1 | 1 | 13 | HASH JOIN (BROADCAST, INNER, eq: [lo_orderdate = d_datekey]) | 174.5ms | 97987 | 2.4MB |

SSB 的端到端慢点主要集中在 q4 系列和 q3/q2 系列。节点层面，热点集中在局部聚合和 broadcast hash join，单个节点时间都在 500ms 以内。

## TPC-H 慢 Case

| Rank | Case | Elapsed |
|---:|---|---:|
| 1 | q18 | 91.26s |
| 2 | q9 | 24.40s |
| 3 | q21 | 10.80s |
| 4 | q17 | 8.55s |
| 5 | q19 | 6.07s |
| 6 | q8 | 4.88s |
| 7 | q10 | 3.88s |
| 8 | q1 | 3.88s |

## TPC-H 慢节点

| Rank | Case | Step | Node | Operator | Time | Rows | Peak |
|---:|---|---:|---:|---|---:|---:|---:|
| 1 | q18 | 1 | 9 | HASH JOIN (PARTITIONED, INNER, eq: [l_orderkey = o_orderkey]) | 44.8s | 13502430 | 607.7MB |
| 2 | q18 | 1 | 18 | HASH JOIN (PARTITIONED, LEFT SEMI, eq: [o_orderkey = lineitem.l_orderkey]) | 32.0s | 6001287 | 82.5KB |
| 3 | q18 | 1 | 7 | HASH JOIN (PARTITIONED, INNER, eq: [o_custkey = c_custkey]) | 3.2s | 3150000 | 57.6MB |
| 4 | q9 | 1 | 8 | HASH JOIN (BROADCAST, INNER, eq: [l_suppkey = ps_suppkey, l_partkey = p_partkey]) | 2.0s | 455759 | 85.1MB |
| 5 | q9 | 1 | 17 | PROJECT [n_name AS nation, year(o_orderdate) AS o_year, l_extendedprice * 1 - l_discount - ps_supplycost * l_quantity AS amount] | 1.8s | 325847 | 13.0KB |
| 6 | q9 | 1 | 10 | HASH JOIN (BROADCAST, INNER, eq: [o_orderkey = l_orderkey]) | 1.4s | 1303388 | 1.7GB |
| 7 | q8 | 1 | 16 | HASH JOIN (PARTITIONED, INNER, eq: [o_custkey = c_custkey]) | 887.4ms | 577655 | 33.2MB |
| 8 | q8 | 1 | 18 | HASH JOIN (PARTITIONED, INNER, eq: [l_orderkey = o_orderkey]) | 822.9ms | 136864 | 57.0MB |

TPC-H 的最突出慢点是 q18。q18 的两个 partitioned hash join 分别占 44.8s 和 32.0s，是当前 TPC-H 基线里最明确的优化目标。q9 也值得关注，热点在多表 join 后的 project 和 hash join。

## TPC-DS 慢 Case

| Rank | Case | Elapsed |
|---:|---|---:|
| 1 | q14 | 39.85s |
| 2 | q23 | 32.14s |
| 3 | q64 | 31.64s |
| 4 | q88 | 22.14s |
| 5 | q9 | 20.82s |
| 6 | q28 | 19.67s |
| 7 | q51 | 16.17s |
| 8 | q20 | 14.51s |

## TPC-DS 慢节点

| Rank | Case | Step | Node | Operator | Time | Rows | Peak |
|---:|---|---:|---:|---|---:|---:|---:|
| 1 | q75 | 1 | 27 | UNION ALL | 2.1s | 819316 | 6.1MB |
| 2 | q75 | 1 | 25 | PROJECT [d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ss_quantity - coalesce(sr_return_quantity, 0) AS sales_cnt, ss_ext_sales_price - coalesce(sr_return_amt, 0.0) AS sales_amt] | 2.1s | 269449 | 12.3MB |
| 3 | q64 | 1 | 24 | HASH JOIN (BROADCAST, INNER, eq: [c_first_shipto_date_sk = d3.d_date_sk]) | 2.0s | 312601 | 53.1MB |
| 4 | q81 | 1 | 49 | NEST LOOP JOIN (CROSS) | 1.8s | 67906697 | 1018.8KB |
| 5 | q75 | 1 | 12 | PROJECT [d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, cs_quantity - coalesce(cr_return_quantity, 0) AS sales_cnt, cs_ext_sales_price - coalesce(cr_return_amount, 0.0) AS sales_amt] | 1.5s | 140209 | 11.5MB |
| 6 | q13 | 1 | 14 | PROJECT [ss_sold_date_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_quantity, ss_sales_price, ss_ext_sales_price, ss_ext_wholesale_cost, ss_net_profit, hd_demo_sk, hd_dep_count, ca_address_sk, ca_state, ca_country, hd_dep_count = 1 AS __cse_0, ss_sales_price BETWEEN 100.00 AND 150.00 AS __cse_1, ss_sales_price BETWEEN 50.00 AND 100.00 AS __cse_2, ss_sales_price BETWEEN 150.00 AND 200.00 AS __cse_3] | 1.1s | 8751 | 50.4KB |
| 7 | q75 | 1 | 29 | HASH AGGREGATE (SINGLE, group by: [d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, sales_cnt, sales_amt]) | 996.6ms | 1226766 | 66.4MB |
| 8 | q64 | 1 | 26 | HASH JOIN (BROADCAST, INNER, eq: [ss_customer_sk = c_customer_sk]) | 909.8ms | 701577 | 1.4GB |

TPC-DS 的端到端慢 case 和单节点 `act` 慢节点不完全重合。q14、q23、q64 是端到端最慢 case，但全局单节点 `act` 最大值出现在 q75、q64、q81 等 case。这个现象说明后续分析不能只看单个 operator 时间，还需要结合 fragment 之间的 exchange、driver wait 和调度开销。

## 产物说明

- `suite_summary.csv`：suite 级别通过数、失败数、CPU time、wall time
- `case_timings.csv`：runner log 中解析出的 case 级别耗时
- `step_timings.csv`：runner log 中解析出的 step 级别耗时，多段 TPC-DS case 会拆开记录
- `explain_sections.csv`：每个 `EXPLAIN ANALYZE` section 的 Planning、Execution、Rows
- `slow_nodes.csv`：所有带 `act` 信息的 plan node，按节点时间降序排列
- `slow_nodes_top5_per_query.csv`：每个 query section 内部 top 5 慢节点
- `PLAN_GAP_AND_EXEC_HOTSPOTS.md`：当前 NovaRocks 基线与 StarRocks FE plan 参照的差距分析，以及执行层慢节点优化优先级
- `plans/analyze/{ssb,tpc-h,tpc-ds}/*.result`：完整 `EXPLAIN ANALYZE` plan 文本

## 后续建议

1. TPC-H 优先看 q18。当前 q18 的两处 partitioned hash join 已经给出明确热点。
2. TPC-DS 优先看 q14、q23、q64 的端到端 profile，再结合 `slow_nodes_top5_per_query.csv` 判断是算子 CPU、exchange wait 还是调度开销。
3. 如果要和 StarRocks FE plan 对齐，应单独跑 `1 StarRocks FE + 3 NovaRocks BE` 的 plan capture，并和本次 `plans/analyze` 中的 NovaRocks FE plan 分开存放。
