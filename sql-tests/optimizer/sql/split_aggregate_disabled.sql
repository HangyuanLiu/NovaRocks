-- OQ-4: disabling SplitAggregateRule keeps ordinary aggregate lowering single-phase.

CREATE TABLE ${case_db}.t_split_agg_disabled (k INT, v INT);
INSERT INTO ${case_db}.t_split_agg_disabled VALUES
    (1, 10), (1, 20), (2, 30), (2, 40), (3, 50), (3, 60);
ANALYZE TABLE ${case_db}.t_split_agg_disabled;

SET disable_optimizer_rules = 'SplitAggregateRule';

-- @result_not_contains=HASH AGGREGATE (LOCAL
-- @result_not_contains=HASH AGGREGATE (GLOBAL
EXPLAIN VERBOSE
SELECT k, SUM(v) AS s
FROM ${case_db}.t_split_agg_disabled
GROUP BY k
ORDER BY k;

SET disable_optimizer_rules = '';
