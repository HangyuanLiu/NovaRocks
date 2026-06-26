-- @tags=optimizer,topn,aggregate,session_rule_disable
-- Test Objective:
-- Lock in PushDownTopNToPreAgg plan-shape coverage. The full EXPLAIN golden
-- distinguishes the pushed pre-aggregate LOCAL TOP-N from the generic
-- SplitTopN LOCAL TOP-N above the global aggregate.

DROP TABLE IF EXISTS ${case_db}.t_pushdown_topn_preagg;
CREATE TABLE ${case_db}.t_pushdown_topn_preagg (city INT, sales INT);
INSERT INTO ${case_db}.t_pushdown_topn_preagg
    SELECT generate_series % 1000, generate_series
    FROM TABLE(generate_series(1, 100000));
ANALYZE TABLE ${case_db}.t_pushdown_topn_preagg;

SET disable_optimizer_rules = '';

-- Rule enabled: ORDER BY group key can insert a partial TopN between
-- global and local aggregate.
EXPLAIN VERBOSE
SELECT city, SUM(sales)
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY city
LIMIT 10;

SET disable_optimizer_rules = 'PushDownTopNToPreAgg';

-- Rule disabled: no pre-aggregate partial TopN.
EXPLAIN VERBOSE
SELECT city, SUM(sales)
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY city
LIMIT 10;

SET disable_optimizer_rules = '';

-- Negative: ORDER BY aggregate output is not eligible for pre-aggregate
-- partial TopN pushdown.
EXPLAIN VERBOSE
SELECT city, SUM(sales) AS s
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY s DESC
LIMIT 10;

SET disable_optimizer_rules = '';
