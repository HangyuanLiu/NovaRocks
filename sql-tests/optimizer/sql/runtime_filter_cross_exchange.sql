-- OQ-10: conservative runtime filter placement is enabled by default, but a
-- PARTITIONED join's probe runtime filter still must not cross a shuffle
-- exchange.
--
-- The outer join is planned as PARTITIONED because each inner join's estimated
-- output is large, so both of the outer join's inputs exceed the broadcast row
-- limit. By default that outer RF is gated out (build side too large); raising
-- build_max + dropping probe_min_selectivity lets it through.
--
-- Conservative exchange placement keeps the outer PARTITIONED join's probe RF
-- build-only and does NOT cross the shuffle exchange (no probe_expr=(t1.av)).
-- Within-fragment RFs still push through BROADCAST joins to the base scan
-- (probe_expr=(a.k)).

CREATE TABLE ${case_db}.ra (k INT, v INT);
CREATE TABLE ${case_db}.customer_demographics (k INT, v INT);
CREATE TABLE ${case_db}.rf_x_probe (k INT, v INT);
CREATE TABLE ${case_db}.rf_x_build (k INT, v INT);
INSERT INTO ${case_db}.ra VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.customer_demographics VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.rf_x_probe
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.rf_x_build VALUES (1, 10), (2, 20), (3, 30);
ANALYZE TABLE ${case_db}.ra;
ANALYZE TABLE ${case_db}.customer_demographics;
ANALYZE TABLE ${case_db}.rf_x_probe;
ANALYZE TABLE ${case_db}.rf_x_build;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;

-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=build_expr = (t2.cv)
-- @explain_contains=probe_expr = (a.k)
-- @explain_not_contains=probe_expr = (t1.av)
SELECT count(*) AS cnt
FROM (
    SELECT a.v AS av
    FROM ${case_db}.ra a
    JOIN ${case_db}.customer_demographics b ON a.k = b.k
) t1
JOIN (
    SELECT c.v AS cv
    FROM ${case_db}.ra c
    JOIN ${case_db}.customer_demographics d ON c.k = d.k
) t2
ON t1.av = t2.cv;

-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=build runtime filters:
-- @explain_contains=build_expr = (b.k)
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (p.k)
SELECT count(*) AS cnt
FROM ${case_db}.rf_x_probe p
JOIN ${case_db}.rf_x_build b ON p.k = b.k;
