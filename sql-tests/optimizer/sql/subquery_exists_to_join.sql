-- @tags=optimizer,subquery_apply,exists_to_join
-- Test Objective:
-- Apply-mode EXISTS / NOT EXISTS subqueries are eliminated into semi/anti joins.
-- Disabling ExistentialApplyToJoin is covered by an explicit sanity query.
DROP TABLE IF EXISTS ${case_db}.sq_t1;
DROP TABLE IF EXISTS ${case_db}.sq_t2;
CREATE TABLE ${case_db}.sq_t1 (k INT, v INT);
CREATE TABLE ${case_db}.sq_t2 (k INT, v INT);
INSERT INTO ${case_db}.sq_t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO ${case_db}.sq_t2 VALUES (1, 100), (1, 101), (3, 300), (5, 500);
ANALYZE TABLE ${case_db}.sq_t1;
ANALYZE TABLE ${case_db}.sq_t2;

SET subquery_unnest_mode='apply';

-- Correlated EXISTS rewrites to a LEFT SEMI join and leaves no Apply node.
-- @explain_contains=HASH JOIN (BROADCAST, LEFT SEMI
-- @explain_not_contains=APPLY
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE EXISTS (
    SELECT 1
    FROM ${case_db}.sq_t2 t2
    WHERE t2.k = t1.k
)
ORDER BY k;

-- Correlated NOT EXISTS rewrites to a LEFT ANTI join and leaves no Apply node.
-- @explain_contains=HASH JOIN (BROADCAST, LEFT ANTI
-- @explain_not_contains=APPLY
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE NOT EXISTS (
    SELECT 1
    FROM ${case_db}.sq_t2 t2
    WHERE t2.k = t1.k
)
ORDER BY k;

SET disable_optimizer_rules='ExistentialApplyToJoin';

-- Disabling ExistentialApplyToJoin should make the Apply backstop reject the
-- still-correlated EXISTS instead of silently changing shape.
-- @expect_error=subquery decorrelation failed
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE EXISTS (
    SELECT 1
    FROM ${case_db}.sq_t2 t2
    WHERE t2.k = t1.k
)
ORDER BY k;

SET disable_optimizer_rules='';
SET subquery_unnest_mode='legacy';
