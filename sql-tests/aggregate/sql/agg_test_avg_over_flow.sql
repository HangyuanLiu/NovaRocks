-- Migrated from dev/test/sql/test_agg_function/R/test_avg_over_flow
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_over_flow
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `v1` varchar(65533) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ('a', 10000000, 3), ('a', 40000000, 5), ('a', 40000000, 5), ('a', 40000000, 5),
('b', 10000000, 3), ('b', 40000000, 5), ('b', 40000000, 5), ('b', 40000000, 5);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ('a', 10000000, 3), ('a', 40000000, 5), ('a', 40000000, 5), ('a', 40000000, 5),
('b', 10000000, 3), ('b', 40000000, 5), ('b', 40000000, 5), ('b', 40000000, 5);

-- query 5
USE ${case_db};
select avg(v2 - 1.86659630566164 * (v3 - 3.062175673706)) from t1 group by v1;
