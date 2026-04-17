-- @order_sensitive=true
-- Test Objective:
-- 1. Exercise the 3-phase DISTINCT aggregation (LOCAL -> DISTINCT_GLOBAL -> GLOBAL).
-- 2. TPC-DS has no GROUP BY + count(distinct) query; this is the coverage gap filler.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_dg;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_dg (
    g INT,
    x INT,
    a BIGINT
);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t_dg VALUES
    (1, 100, 10), (1, 100, 20), (1, 200, 30),
    (2, 100, 40), (2, 300, 50), (2, 300, 60),
    (3, 400, 70);

-- query 4
SELECT g, count(distinct x) AS dc, sum(a) AS sa
FROM ${case_db}.t_dg
GROUP BY g
ORDER BY g;
