-- OQ-10: runtime filter placement over iceberg base tables. With real stats the
-- small joins are BROADCAST; plan-shape goldens capture RF placement on iceberg.
-- PARTITIONED-join probe-RF-vs-shuffle behavior at scale is covered by the
-- benchmark suites.
CREATE TABLE ${case_db}.ra (k INT, v INT);
CREATE TABLE ${case_db}.customer_demographics (k INT, v INT);
CREATE TABLE ${case_db}.rf_x_probe (k INT, v INT);
CREATE TABLE ${case_db}.rf_x_build (k INT, v INT);
INSERT INTO ${case_db}.ra VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.customer_demographics VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.rf_x_probe
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.rf_x_build VALUES (1, 10), (2, 20), (3, 30);
ANALYZE TABLE ${case_db}.ra;
ANALYZE TABLE ${case_db}.customer_demographics;
ANALYZE TABLE ${case_db}.rf_x_probe;
ANALYZE TABLE ${case_db}.rf_x_build;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;

EXPLAIN VERBOSE
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

EXPLAIN VERBOSE
SELECT count(*) AS cnt
FROM ${case_db}.rf_x_probe p
JOIN ${case_db}.rf_x_build b ON p.k = b.k;
