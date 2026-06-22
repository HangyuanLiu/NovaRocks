-- @tags=optimizer,stats,regression
-- Test Objective:
-- Regression input for the removed name-based scan row fallback. The table
-- name intentionally contains sales/fact/dim/lineitem tokens; scan row count
-- must come from ANALYZE/catalog statistics, not table-name heuristics.
DROP TABLE IF EXISTS ${case_db}.misleading_sales_fact_dim_lineitem;
CREATE TABLE ${case_db}.misleading_sales_fact_dim_lineitem (
    id INT,
    payload INT
);
INSERT INTO ${case_db}.misleading_sales_fact_dim_lineitem VALUES
    (1, 10),
    (2, 20),
    (3, 30);
ANALYZE TABLE ${case_db}.misleading_sales_fact_dim_lineitem;

EXPLAIN VERBOSE
SELECT payload
FROM ${case_db}.misleading_sales_fact_dim_lineitem;
