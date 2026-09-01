# Standard benchmark workload correctness coverage audit

## Scope and decision

This audit is the migration gate for moving SSB, TPC-H, and TPC-DS out of the
blocking SQL correctness lane.  It covers the 13 SSB queries, 22 TPC-H queries,
and 99 TPC-DS queries that existed at the time of the audit under
`tests/sql/suites/{ssb,tpc-h,tpc-ds}/sql/`: **134 queries in total**.

The conclusion is that no benchmark-scale SQL needs to remain in the
correctness lane and no new micro-case is required by this audit.  The relevant
language, result, optimizer-safety, and distributed runtime-filter contracts
already have deterministic, small-data owners.  This is not a claim that a
benchmark query has an identical plan to a small test; it is a claim that
removing the workload does not remove a unique *correctness* contract.

The physical paths in this document are deliberately the pre-migration paths.
T02 moves the files; when it does so it must update the path prefixes in this
audit to `tests/sql/benchmarks/` and `tests/sql/correctness/` without changing
the mapping or restoring the old root.

## Method

1. Enumerate `*.sql` files in each workload directory.  The static counts are
   SSB=13, TPC-H=22, TPC-DS=99.
2. Read each SQL statement after removing the license comments and classify the
   query-level semantics actually present: join shape, aggregate, scalar or
   correlated subquery, CTE, set operation, outer join, window, grouping set,
   conditional expression, projection/type/date expression, and ordering/TopN.
3. Map each unique semantic to a named, deterministic correctness case below.
   A mapping is evidence of a semantic contract, not a statement that the
   benchmark's table size, data distribution, file count, cache state, or
   elapsed time is covered by that case.
4. Treat a feature as a gap only if a query introduces result-visible semantics
   with no natural existing correctness owner.  No such feature was found.

## Existing correctness evidence

The shorthand used in the per-query tables refers to these source-controlled
small-data cases.  Paths are pre-migration paths for the same reason as above.

| ID | Correctness contract and direct evidence |
| --- | --- |
| E-J | Inner, chained and composite/null-key joins: `tests/sql/suites/join/sql/join_inner_basic.sql`, `join_three_table_chain.sql`, `join_multi_key_null_semantics.sql` |
| E-JO | Outer join null-extension and residual predicates: `tests/sql/suites/join/sql/join_left_outer_null_fill.sql`, `join_left_outer_residual_nullable.sql`, `join_full_outer_null_fill.sql` |
| E-D | Distributed join/exchange shape: `tests/sql/suites/join/sql/join_partition_hash.sql`, `join_bucket_shuffle_right_outer_equivalence.sql`; optimizer distribution contracts in `tests/sql/suites/optimizer/sql/distribution_join_shuffle_reuse.sql` |
| E-RF | Runtime-filter construction/consumption and cross-process transport: `tests/sql/suites/runtime-filter/sql/runtime_filter_three_table_chain.sql`, `runtime_filter_multi_columns.sql`; `tests/sql/suites/runtime-filter-distributed/sql/runtime_filter_distributed_cross_process.sql`, `runtime_filter_distributed_partitioned_probe.sql` |
| E-A | Grouped/scalar aggregates, HAVING and multi-phase execution: `tests/sql/suites/aggregate/sql/agg_group_sum_count_avg.sql`, `agg_having_threshold.sql`, `agg_test_agg_split_two_phase.sql` |
| E-AD | DISTINCT and multi-column distinct aggregation: `tests/sql/suites/aggregate/sql/agg_count_distinct_single.sql`, `agg_count_distinct_multi_column.sql`, `agg_sum_distinct.sql` |
| E-G | GROUPING SETS/ROLLUP/GROUPING: `tests/sql/suites/aggregate/sql/agg_grouping_sets_v1.sql`, `agg_grouping_sets_v2.sql`, `agg_test_grouping_set.sql` |
| E-W | Window aggregate/rank/order/frame semantics: `tests/sql/suites/analytic/sql/analytic_row_rank_dense.sql`, `analytic_sum_rows_cumulative.sql`, `analytic_test_basic_multi_window_function.sql`, `analytic_filter_topn_with_window.sql` |
| E-S | Scalar, correlated, IN/EXISTS and null-aware anti-subquery semantics: `tests/sql/suites/join/sql/join_exists_subquery_semantics.sql`, `join_not_exists_subquery_semantics.sql`, `join_not_in_correlated_conjunct_null_aware.sql`; lowering guards in `tests/sql/suites/optimizer/sql/subquery_scalar_to_window.sql`, `subquery_exists_to_join.sql` |
| E-C | CTE alias, scope and recursive contract: `tests/sql/suites/cte/sql/cte_multi_alias.sql`, `cte_in_where_subquery.sql`, `cte_recursive.sql` |
| E-X | UNION ALL/DISTINCT, INTERSECT and EXCEPT null/duplicate semantics: `tests/sql/suites/set-op/sql/set_union_all_projection_alignment.sql`, `set_union_distinct_dedup.sql`, `set_intersect_multi_column_null.sql`, `set_except_three_stage_null_duplicates.sql` |
| E-P | CASE/COALESCE, arithmetic, cast, date and string projection semantics: `tests/sql/suites/project/sql/project_case_coalesce.sql`, `project_arithmetic_cast.sql`, `project_date_function_semantics.sql`, `project_string_functions.sql` |
| E-F | BETWEEN/IN/LIKE, OR and three-valued/null predicate semantics: `tests/sql/suites/filter/sql/filter_in_between_like.sql`, `filter_null_or_predicate.sql`, `filter_nullable_three_valued_logic.sql` |
| E-L | ORDER BY, LIMIT/OFFSET and TopN ties/null ordering: `tests/sql/suites/sort/sql/sort_multi_key.sql`, `topn_order_limit.sql`, `topn_null_order_limit_offset.sql` |
| E-O | Projection/output pruning and optimizer expression equivalence: `tests/sql/suites/optimizer/sql/oq1a_aggregate_output_pruning.sql`, `oq1c_cte_receive_column_remap.sql`, `cse_projection.sql`, `runtime_filter_project_setop_remap.sql` |

`E-J` and `E-D` are the result/distribution preconditions for `E-RF`; the
dedicated runtime-filter tests assert filter semantics explicitly.  A standard
workload query that happens to receive a runtime filter therefore has coverage
through `E-RF`, not through an inferred timing improvement in SSB/TPC output.

## SSB (13)

| Query source | Result-visible risk found in the SQL | Evidence | Conclusion |
| --- | --- | --- | --- |
| `ssb/sql/q1.1.sql` | inner date join; range predicates; scalar SUM | E-J, E-A, E-F, E-RF | covered |
| `ssb/sql/q1.2.sql` | inner date join; BETWEEN ranges; scalar SUM | E-J, E-A, E-F, E-RF | covered |
| `ssb/sql/q1.3.sql` | inner date join; conjunctive date/range predicates; scalar SUM | E-J, E-A, E-F, E-RF | covered |
| `ssb/sql/q2.1.sql` | three-dimension inner join; grouped SUM; ordered grouping keys | E-J, E-D, E-A, E-L, E-RF | covered |
| `ssb/sql/q2.2.sql` | three-dimension inner join; string BETWEEN; grouped SUM/order | E-J, E-D, E-A, E-F, E-L, E-RF | covered |
| `ssb/sql/q2.3.sql` | three-dimension inner join; equality predicates; grouped SUM/order | E-J, E-D, E-A, E-L, E-RF | covered |
| `ssb/sql/q3.1.sql` | three-dimension inner join; grouped SUM; mixed ASC/DESC order | E-J, E-D, E-A, E-L, E-RF | covered |
| `ssb/sql/q3.2.sql` | three-dimension inner join; grouped SUM; mixed ASC/DESC order | E-J, E-D, E-A, E-L, E-RF | covered |
| `ssb/sql/q3.3.sql` | OR predicates on both join dimensions; grouped SUM | E-J, E-D, E-A, E-F, E-L, E-RF | covered |
| `ssb/sql/q3.4.sql` | OR predicates plus month equality; grouped SUM | E-J, E-D, E-A, E-F, E-L, E-RF | covered |
| `ssb/sql/q4.1.sql` | four-dimension inner join; aggregate subtraction; OR predicate | E-J, E-D, E-A, E-P, E-F, E-L, E-RF | covered |
| `ssb/sql/q4.2.sql` | four-dimension join; aggregate subtraction; grouped order | E-J, E-D, E-A, E-P, E-F, E-L, E-RF | covered |
| `ssb/sql/q4.3.sql` | four-dimension join; aggregate subtraction; grouped order | E-J, E-D, E-A, E-P, E-F, E-L, E-RF | covered |

## TPC-H (22)

| Query source | Result-visible risk found in the SQL | Evidence | Conclusion |
| --- | --- | --- | --- |
| `tpc-h/sql/q1.sql` | grouped SUM/AVG/COUNT with date predicate and order | E-A, E-P, E-F, E-L | covered |
| `tpc-h/sql/q2.sql` | five-way join with correlated scalar MIN subquery and TopN | E-J, E-D, E-S, E-F, E-L, E-RF | covered |
| `tpc-h/sql/q3.sql` | three-way join, aggregate expression, TopN | E-J, E-D, E-A, E-P, E-L, E-RF | covered |
| `tpc-h/sql/q4.sql` | correlated EXISTS semi-subquery with grouped COUNT | E-S, E-A, E-F, E-L | covered |
| `tpc-h/sql/q5.sql` | five-way chain join and grouped revenue | E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q6.sql` | scalar aggregate with date/range predicates and arithmetic | E-A, E-P, E-F | covered |
| `tpc-h/sql/q7.sql` | derived table, five-way join, EXTRACT(year), OR predicate | E-J, E-D, E-S, E-P, E-F, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q8.sql` | derived table, six-way join, CASE ratio and EXTRACT(year) | E-J, E-D, E-S, E-P, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q9.sql` | derived table, five-way join, arithmetic profit and EXTRACT | E-J, E-D, E-S, E-P, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q10.sql` | four-way join, grouped aggregate, TopN | E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q11.sql` | correlated scalar aggregate in HAVING | E-J, E-S, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q12.sql` | join, conditional COUNT via CASE/cast and IN | E-J, E-A, E-P, E-F, E-L, E-RF | covered |
| `tpc-h/sql/q13.sql` | LEFT OUTER JOIN with predicate in ON; second aggregation | E-JO, E-A, E-F, E-L | covered |
| `tpc-h/sql/q14.sql` | joined scalar ratio with CASE, LIKE and division | E-J, E-A, E-P, E-F, E-RF | covered |
| `tpc-h/sql/q15.sql` | repeated derived aggregate and scalar MAX comparison | E-J, E-S, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q16.sql` | NOT IN anti-subquery, LIKE and COUNT DISTINCT | E-J, E-S, E-AD, E-F, E-L, E-RF | covered |
| `tpc-h/sql/q17.sql` | correlated scalar AVG threshold and arithmetic division | E-J, E-S, E-A, E-P, E-RF | covered |
| `tpc-h/sql/q18.sql` | IN aggregate/HAVING subquery with three-way join/TopN | E-J, E-D, E-S, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q19.sql` | OR-of-conjunction join predicate and range/IN filters | E-J, E-P, E-F, E-A, E-RF | covered |
| `tpc-h/sql/q20.sql` | nested IN and correlated aggregate subqueries | E-J, E-S, E-A, E-F, E-L, E-RF | covered |
| `tpc-h/sql/q21.sql` | EXISTS plus NOT EXISTS with join/aggregate/TopN | E-J, E-D, E-S, E-A, E-L, E-RF | covered |
| `tpc-h/sql/q22.sql` | derived projection, correlated scalar AVG and NOT EXISTS | E-S, E-P, E-A, E-F, E-L | covered |

## TPC-DS (99)

| Query source | Result-visible risk found in the SQL | Evidence | Conclusion |
| --- | --- | --- | --- |
| `tpc-ds/sql/q1.sql` | CTE aggregate and correlated AVG threshold | E-C, E-S, E-A, E-J | covered |
| `tpc-ds/sql/q2.sql` | CTE; UNION ALL sales sources; CASE aggregates | E-C, E-X, E-P, E-A, E-J | covered |
| `tpc-ds/sql/q3.sql` | three-way join, grouped SUM and TopN | E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q4.sql` | CTE; UNION ALL yearly sales; CASE/aggregate comparison | E-C, E-X, E-P, E-A, E-J | covered |
| `tpc-ds/sql/q5.sql` | CTE; UNION ALL, left join and rollup-style aggregate inputs | E-C, E-X, E-JO, E-A, E-P | covered |
| `tpc-ds/sql/q6.sql` | correlated scalar AVG with DISTINCT month lookup | E-J, E-S, E-AD, E-A, E-P | covered |
| `tpc-ds/sql/q7.sql` | five-way join with AVG aggregates and OR predicate | E-J, E-D, E-A, E-F, E-RF | covered |
| `tpc-ds/sql/q8.sql` | nested derived/IN projection with string substring filters | E-J, E-S, E-P, E-F, E-A | covered |
| `tpc-ds/sql/q9.sql` | CASE selected from repeated scalar aggregate subqueries | E-S, E-P, E-A, E-F | covered |
| `tpc-ds/sql/q10.sql` | demographic join/group aggregate; EXISTS semi-subquery | E-J, E-S, E-A, E-RF | covered |
| `tpc-ds/sql/q11.sql` | CTE and UNION ALL year-total comparison | E-C, E-X, E-P, E-A, E-J | covered |
| `tpc-ds/sql/q12.sql` | grouped aggregate with partitioned window ratio | E-J, E-A, E-W, E-P, E-L | covered |
| `tpc-ds/sql/q13.sql` | six-way join, AVG/SUM and numeric range predicates | E-J, E-D, E-A, E-F, E-RF | covered |
| `tpc-ds/sql/q14.sql` | CTE, INTERSECT, correlated subquery and HAVING | E-C, E-X, E-S, E-A, E-L | covered |
| `tpc-ds/sql/q15.sql` | OR predicate with substring and grouped sales | E-J, E-A, E-P, E-F, E-L | covered |
| `tpc-ds/sql/q16.sql` | COUNT DISTINCT, EXISTS and date arithmetic | E-J, E-S, E-AD, E-P, E-F | covered |
| `tpc-ds/sql/q17.sql` | multi-join statistical aggregates and ratio expression | E-J, E-D, E-A, E-P, E-RF | covered |
| `tpc-ds/sql/q18.sql` | multi-join AVG with explicit decimal casts | E-J, E-A, E-P, E-RF | covered |
| `tpc-ds/sql/q19.sql` | multi-join grouped SUM with substring comparison | E-J, E-D, E-A, E-P, E-F, E-L | covered |
| `tpc-ds/sql/q20.sql` | grouped aggregate with partitioned window ratio | E-J, E-A, E-W, E-P, E-L | covered |
| `tpc-ds/sql/q21.sql` | CASE aggregates around date boundary | E-J, E-A, E-P, E-F | covered |
| `tpc-ds/sql/q22.sql` | ROLLUP aggregate and ordered null-bearing keys | E-J, E-G, E-A, E-L | covered |
| `tpc-ds/sql/q23.sql` | multiple CTEs, HAVING and scalar MAX comparison | E-C, E-S, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q24.sql` | CTE with join/aggregate/HAVING | E-C, E-J, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q25.sql` | multiple fact-table joins with grouped profit/loss | E-J, E-D, E-A, E-P, E-L, E-RF | covered |
| `tpc-ds/sql/q26.sql` | five-way join with AVG aggregates and OR predicate | E-J, E-D, E-A, E-F, E-RF | covered |
| `tpc-ds/sql/q27.sql` | GROUPING/ROLLUP with AVG measures | E-J, E-G, E-A, E-L | covered |
| `tpc-ds/sql/q28.sql` | independent scalar aggregate subqueries and COUNT DISTINCT | E-S, E-AD, E-A, E-F | covered |
| `tpc-ds/sql/q29.sql` | three fact-table join with grouped quantities | E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q30.sql` | CTE aggregate and correlated AVG threshold | E-C, E-S, E-A, E-J | covered |
| `tpc-ds/sql/q31.sql` | paired CTE aggregates and ratio comparison | E-C, E-J, E-A, E-P | covered |
| `tpc-ds/sql/q32.sql` | correlated scalar AVG threshold with date arithmetic | E-J, E-S, E-A, E-P, E-F | covered |
| `tpc-ds/sql/q33.sql` | CTE plus UNION ALL sales channels | E-C, E-X, E-J, E-A, E-RF | covered |
| `tpc-ds/sql/q34.sql` | derived aggregate/filter and conditional predicate | E-S, E-J, E-A, E-F, E-L | covered |
| `tpc-ds/sql/q35.sql` | EXISTS demographic filter and repeated MIN/MAX/AVG | E-J, E-S, E-A, E-RF | covered |
| `tpc-ds/sql/q36.sql` | ROLLUP/GROUPING plus rank window over aggregate | E-J, E-G, E-W, E-P, E-A, E-L | covered |
| `tpc-ds/sql/q37.sql` | inventory/fact join with IN and date/range predicates | E-J, E-D, E-F, E-P, E-L, E-RF | covered |
| `tpc-ds/sql/q38.sql` | DISTINCT derived rows and INTERSECT | E-J, E-X, E-AD, E-A | covered |
| `tpc-ds/sql/q39.sql` | CTE standard deviation, CASE null guard and ratio | E-C, E-A, E-P, E-J | covered |
| `tpc-ds/sql/q40.sql` | LEFT OUTER JOIN with COALESCE and CASE aggregate | E-JO, E-P, E-A, E-F | covered |
| `tpc-ds/sql/q41.sql` | DISTINCT projection with correlated COUNT predicate | E-S, E-AD, E-F, E-P | covered |
| `tpc-ds/sql/q42.sql` | three-way grouped SUM and TopN | E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q43.sql` | day-of-week CASE aggregates | E-J, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q44.sql` | aggregate-derived monthly average and rank window | E-J, E-S, E-A, E-W, E-L | covered |
| `tpc-ds/sql/q45.sql` | derived aggregate with substring/date expression | E-J, E-S, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q46.sql` | derived aggregate/filter and multi-key order | E-J, E-S, E-A, E-L | covered |
| `tpc-ds/sql/q47.sql` | CTE, conditional aggregate and window comparison | E-C, E-J, E-A, E-W, E-P | covered |
| `tpc-ds/sql/q48.sql` | multi-way inner join, grouped aggregate and TopN | E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q49.sql` | UNION ALL, outer join, window and CASE expression | E-X, E-JO, E-W, E-P, E-A | covered |
| `tpc-ds/sql/q50.sql` | conditional aggregate over multi-way join | E-J, E-A, E-P, E-L, E-RF | covered |
| `tpc-ds/sql/q51.sql` | CTE, outer join and aggregate window | E-C, E-JO, E-W, E-A, E-P | covered |
| `tpc-ds/sql/q52.sql` | multi-way inner join, grouped aggregate and TopN | E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q53.sql` | derived aggregate with window and CASE predicate | E-J, E-S, E-W, E-A, E-P | covered |
| `tpc-ds/sql/q54.sql` | CTE, UNION ALL and DISTINCT set semantics | E-C, E-X, E-AD, E-J | covered |
| `tpc-ds/sql/q55.sql` | multi-way inner join, grouped aggregate and TopN | E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q56.sql` | CTE with UNION ALL channel aggregation | E-C, E-X, E-J, E-A | covered |
| `tpc-ds/sql/q57.sql` | CTE, rank window and conditional aggregate | E-C, E-W, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q58.sql` | CTE fan-in with scalar date lookups | E-C, E-S, E-J, E-A, E-L | covered |
| `tpc-ds/sql/q59.sql` | CTE day-of-week conditional aggregates | E-C, E-J, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q60.sql` | CTE plus UNION ALL sales-channel aggregation | E-C, E-X, E-J, E-A | covered |
| `tpc-ds/sql/q61.sql` | derived ratios with explicit decimal casts | E-J, E-S, E-A, E-P | covered |
| `tpc-ds/sql/q62.sql` | CASE date-difference buckets and grouped aggregate | E-J, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q63.sql` | aggregate-over-aggregate window and CASE filters | E-J, E-S, E-W, E-A, E-P | covered |
| `tpc-ds/sql/q64.sql` | CTE/HAVING and aggregate comparison | E-C, E-S, E-A, E-J, E-RF | covered |
| `tpc-ds/sql/q65.sql` | nested aggregate-derived joins and TopN | E-J, E-S, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q66.sql` | UNION ALL monthly conditional aggregates | E-X, E-J, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q67.sql` | rank window over nested grouped aggregate | E-J, E-S, E-W, E-A, E-L | covered |
| `tpc-ds/sql/q68.sql` | derived grouped sales with multi-column join/order | E-J, E-S, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q69.sql` | correlated EXISTS over fact/date join | E-J, E-S, E-A, E-RF | covered |
| `tpc-ds/sql/q70.sql` | ROLLUP/GROUPING plus rank window | E-J, E-G, E-W, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q71.sql` | UNION ALL sources with date/time join and aggregate | E-X, E-J, E-D, E-A, E-L, E-RF | covered |
| `tpc-ds/sql/q72.sql` | explicit JOIN chain with nullable promotion CASE counts | E-J, E-A, E-P, E-L, E-RF | covered |
| `tpc-ds/sql/q73.sql` | derived aggregate/filter with conditional predicates | E-J, E-S, E-A, E-F, E-L | covered |
| `tpc-ds/sql/q74.sql` | CTE and UNION ALL yearly customer totals | E-C, E-X, E-J, E-A, E-P | covered |
| `tpc-ds/sql/q75.sql` | CTE, LEFT JOIN returns and COALESCE measures | E-C, E-JO, E-A, E-P, E-RF | covered |
| `tpc-ds/sql/q76.sql` | UNION ALL channel projection and grouped aggregate | E-X, E-J, E-A, E-L | covered |
| `tpc-ds/sql/q77.sql` | CTE/UNION ALL, outer joins and ROLLUP inputs | E-C, E-X, E-JO, E-G, E-A, E-P | covered |
| `tpc-ds/sql/q78.sql` | CTE with LEFT JOIN anti-null return filter | E-C, E-JO, E-F, E-A, E-RF | covered |
| `tpc-ds/sql/q79.sql` | derived aggregate with substring and TopN | E-J, E-S, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q80.sql` | CTE/outer join/COALESCE and grouped aggregates | E-C, E-JO, E-A, E-P, E-G | covered |
| `tpc-ds/sql/q81.sql` | CTE aggregate and correlated threshold | E-C, E-S, E-A, E-J | covered |
| `tpc-ds/sql/q82.sql` | inventory/fact join with date/range/IN predicates | E-J, E-D, E-F, E-P, E-L, E-RF | covered |
| `tpc-ds/sql/q83.sql` | CTE fan-in and nested date IN subqueries | E-C, E-S, E-J, E-A, E-L | covered |
| `tpc-ds/sql/q84.sql` | COALESCE string projection and multi-way join filters | E-J, E-P, E-F, E-RF | covered |
| `tpc-ds/sql/q85.sql` | multi-way join with substring/date projection and AVG | E-J, E-A, E-P, E-L, E-RF | covered |
| `tpc-ds/sql/q86.sql` | ROLLUP/GROUPING plus rank window | E-J, E-G, E-W, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q87.sql` | DISTINCT operands and EXCEPT set semantics | E-J, E-X, E-AD, E-A | covered |
| `tpc-ds/sql/q88.sql` | independent aggregate-derived scalar projections | E-S, E-J, E-A, E-P | covered |
| `tpc-ds/sql/q89.sql` | aggregate-derived window and CASE filtering | E-J, E-S, E-W, E-A, E-P | covered |
| `tpc-ds/sql/q90.sql` | scalar count projections with decimal ratio casts | E-J, E-S, E-A, E-P | covered |
| `tpc-ds/sql/q91.sql` | multi-way join, aggregate and LIKE/date predicates | E-J, E-D, E-A, E-F, E-L, E-RF | covered |
| `tpc-ds/sql/q92.sql` | correlated scalar AVG threshold with date arithmetic | E-J, E-S, E-A, E-P, E-F | covered |
| `tpc-ds/sql/q93.sql` | LEFT JOIN returns with CASE and grouped aggregate | E-JO, E-A, E-P, E-L | covered |
| `tpc-ds/sql/q94.sql` | COUNT DISTINCT plus EXISTS and date arithmetic | E-J, E-S, E-AD, E-P, E-F, E-RF | covered |
| `tpc-ds/sql/q95.sql` | self-join CTE and COUNT DISTINCT aggregation | E-C, E-J, E-AD, E-A, E-RF | covered |
| `tpc-ds/sql/q96.sql` | time/demographic/store join with scalar COUNT/TopN | E-J, E-A, E-F, E-L, E-RF | covered |
| `tpc-ds/sql/q97.sql` | CTE fan-in, outer join and CASE counting | E-C, E-JO, E-A, E-P | covered |
| `tpc-ds/sql/q98.sql` | grouped aggregate with partitioned window ratio | E-J, E-A, E-W, E-P, E-L | covered |
| `tpc-ds/sql/q99.sql` | CASE date-difference buckets and grouped aggregate | E-J, E-A, E-P, E-L | covered |

## Deliberately not claimed as correctness coverage

The following are benchmark concerns.  They remain in the benchmark protocol
and must not be represented by copying large workload data into correctness
suites:

- elapsed time, throughput, p50/p95, warm-cache behavior, repeated-sample
  variance, and baseline comparability;
- SSB/TPC scale factor, cardinality/NDV distribution, skew, file count, object
  store latency, cache capacity, spill pressure, or data locality;
- a particular cost-model choice, join order, broadcast/shuffle decision, or
  whether a runtime filter happens to be profitable for a large input;
- physical column pruning volume or bytes read.  `E-O` checks result-preserving
  projection/output-pruning contracts; it does not assert benchmark I/O volume;
- `EXPLAIN ANALYZE` operator timing/profile text.  Benchmark profile artifacts
  are diagnostic and need their own completeness requirement, not a replacement
  for result correctness.

## Reproducible static review commands

Run from repository root before T02 moves the paths:

```bash
find tests/sql/suites/ssb/sql -name '*.sql' | wc -l
find tests/sql/suites/tpc-h/sql -name '*.sql' | wc -l
find tests/sql/suites/tpc-ds/sql -name '*.sql' | wc -l

rg --files tests/sql/suites/{optimizer,runtime-filter,runtime-filter-distributed,project,join,aggregate,analytic,subquery,cte,set-op,filter,sort}
```

Expected counts are `13`, `22`, and `99`; the three tables above contain one
data row per source query.  No correctness suite was changed, so there is no
new SQL case to run for this audit.  T02 must rerun the counts against the new
benchmark root and retain the named evidence cases in the correctness root.
