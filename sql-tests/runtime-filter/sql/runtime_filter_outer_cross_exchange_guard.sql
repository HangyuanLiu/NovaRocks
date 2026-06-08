-- @order_sensitive=true
-- @tags=runtime_filter,outer_join,cross_exchange_guard
-- Test Objective:
-- 1. Validate FULL OUTER JOIN null-preserving semantics with runtime filter enabled.
-- 2. Compare against RuntimeFilterPushDown-disabled execution to guard cross-exchange placement.
-- Test Flow:
-- 1. Create/reset left, right, and dimension tables.
-- 2. Insert deterministic rows including a NULL probe key.
-- 3. Run the same outer-join query with runtime filter enabled and disabled.
DROP TABLE IF EXISTS ${case_db}.rf_outer_l;
DROP TABLE IF EXISTS ${case_db}.rf_outer_r;
DROP TABLE IF EXISTS ${case_db}.rf_outer_dim;
CREATE TABLE ${case_db}.rf_outer_l (
    id INT,
    k INT
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.rf_outer_r (
    k INT
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.rf_outer_dim (
    k INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_outer_l VALUES
    (1, 10),
    (2, NULL),
    (3, 30);
INSERT INTO ${case_db}.rf_outer_r VALUES (10);
INSERT INTO ${case_db}.rf_outer_dim VALUES (10), (30);

SET disable_optimizer_rules = '';
SELECT id, k
FROM (
    SELECT l.id, l.k, r.k AS rk
    FROM ${case_db}.rf_outer_l l
    FULL OUTER JOIN ${case_db}.rf_outer_r r
      ON l.k = r.k
) x
WHERE x.k IS NULL OR x.k IN (
    SELECT d.k FROM ${case_db}.rf_outer_dim d
)
ORDER BY id;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT id, k
FROM (
    SELECT l.id, l.k, r.k AS rk
    FROM ${case_db}.rf_outer_l l
    FULL OUTER JOIN ${case_db}.rf_outer_r r
      ON l.k = r.k
) x
WHERE x.k IS NULL OR x.k IN (
    SELECT d.k FROM ${case_db}.rf_outer_dim d
)
ORDER BY id;
