-- @order_sensitive=true
-- @tags=runtime_filter,complete_only,inner_join
-- Test Objective:
-- 1. Inner join result is identical with RF enabled and disabled.
-- 2. Complete-only lifecycle must never expose a partial filter to the probe.

DROP TABLE IF EXISTS ${case_db}.rf_co_orders;
DROP TABLE IF EXISTS ${case_db}.rf_co_customers;

CREATE TABLE ${case_db}.rf_co_customers (
    id INT,
    name STRING
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_co_orders (
    oid INT,
    customer_id INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_co_customers VALUES
    (2, 'b'),
    (4, 'd'),
    (8, 'h');

INSERT INTO ${case_db}.rf_co_orders VALUES
    (1, 2),
    (2, 4),
    (3, 8),
    (4, 5),
    (5, 4);

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=build runtime filters:
-- @explain_contains=probe runtime filters:
SELECT o.oid, o.customer_id
FROM ${case_db}.rf_co_orders o
JOIN ${case_db}.rf_co_customers c ON o.customer_id = c.id
ORDER BY o.oid;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT o.oid, o.customer_id
FROM ${case_db}.rf_co_orders o
JOIN ${case_db}.rf_co_customers c ON o.customer_id = c.id
ORDER BY o.oid;

SET disable_optimizer_rules = '';
