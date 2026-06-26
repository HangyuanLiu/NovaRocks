-- @order_sensitive=true
-- @tags=aggregate,topn,optimizer,session_rule_disable
-- Test Objective:
-- 1. Validate PushDownTopNToPreAgg preserves grouped TopN results.
-- 2. Compare the same deterministic GROUP BY city ORDER BY city LIMIT query
--    with the rule enabled and disabled.

DROP TABLE IF EXISTS ${case_db}.t_pushdown_topn_preagg_result;
CREATE TABLE ${case_db}.t_pushdown_topn_preagg_result (
    city VARCHAR(16),
    sales INT
);

INSERT INTO ${case_db}.t_pushdown_topn_preagg_result VALUES
    ('a', 1),
    ('a', 2),
    ('a', 10),
    ('b', 5),
    ('b', 1),
    ('c', 3),
    ('c', 4),
    ('d', 9),
    ('d', 2),
    ('e', 7),
    ('e', 1),
    ('f', 8);

ANALYZE TABLE ${case_db}.t_pushdown_topn_preagg_result;

SET disable_optimizer_rules = '';

SELECT city, SUM(sales) AS total_sales
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
) o
GROUP BY city
ORDER BY city
LIMIT 3;

SET disable_optimizer_rules = 'PushDownTopNToPreAgg';

SELECT city, SUM(sales) AS total_sales
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
) o
GROUP BY city
ORDER BY city
LIMIT 3;

SET disable_optimizer_rules = '';
