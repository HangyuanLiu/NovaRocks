-- @order_sensitive=true
-- @tags=runtime_filter,complete_only,semi_join
-- Test Objective:
-- 1. Left semi join RF remains enabled for the supported semi-join shape.
-- 2. Result is identical with RF enabled and disabled.

DROP TABLE IF EXISTS ${case_db}.rf_co_l;
DROP TABLE IF EXISTS ${case_db}.rf_co_r;

CREATE TABLE ${case_db}.rf_co_l (
    id INT,
    k INT
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_co_r (
    k INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_co_l VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40);

INSERT INTO ${case_db}.rf_co_r VALUES
    (20),
    (40);

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=LEFT SEMI
-- @explain_contains=build runtime filters:
-- @explain_contains=probe runtime filters:
SELECT l.id, l.k
FROM ${case_db}.rf_co_l l
WHERE l.k IN (SELECT k FROM ${case_db}.rf_co_r)
ORDER BY l.id;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT l.id, l.k
FROM ${case_db}.rf_co_l l
WHERE l.k IN (SELECT k FROM ${case_db}.rf_co_r)
ORDER BY l.id;

SET disable_optimizer_rules = '';
