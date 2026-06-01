-- OQ-4: scalar aggregate over join can use Local->Global aggregate.

CREATE TABLE ${case_db}.t_split_agg_l (k INT, payload INT);
CREATE TABLE ${case_db}.t_split_agg_r (k INT, payload INT);
INSERT INTO ${case_db}.t_split_agg_l VALUES
    (1, 10), (1, 11), (2, 20), (2, 21), (3, 30), (4, 40);
INSERT INTO ${case_db}.t_split_agg_r VALUES
    (1, 100), (1, 101), (2, 200), (5, 500);
ANALYZE TABLE ${case_db}.t_split_agg_l;
ANALYZE TABLE ${case_db}.t_split_agg_r;

-- @explain_contains=HASH AGGREGATE (LOCAL)
-- @explain_contains=HASH AGGREGATE (GLOBAL)
SELECT COUNT(r.payload) AS cnt
FROM ${case_db}.t_split_agg_l AS l
JOIN ${case_db}.t_split_agg_r AS r
  ON l.k = r.k;
