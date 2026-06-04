-- OQ-5: a hash join emits a build runtime filter on the build (right) side and
-- pushes a matching probe runtime filter down to the probe-side scan. The small
-- dimension build side + broadcast distribution keep the filter past gating
-- under OQ-8 distribution-aware join search.

CREATE TABLE ${case_db}.customer_demographics (k INT, v INT);
CREATE TABLE ${case_db}.rf_probe (k INT, v INT);
INSERT INTO ${case_db}.customer_demographics VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.rf_probe
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 100000));
ANALYZE TABLE ${case_db}.customer_demographics;
ANALYZE TABLE ${case_db}.rf_probe;

-- @explain_contains=build runtime filters:
-- @explain_contains=build_expr = (b.k)
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (p.k)
SELECT count(*)
FROM ${case_db}.rf_probe p
JOIN ${case_db}.customer_demographics b ON p.k = b.k;
