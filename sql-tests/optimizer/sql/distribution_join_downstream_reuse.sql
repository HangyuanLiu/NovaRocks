-- @tags=optimizer,oq8,distribution
DROP TABLE IF EXISTS ${case_db}.oq8_down_l;
DROP TABLE IF EXISTS ${case_db}.oq8_down_r;
CREATE TABLE ${case_db}.oq8_down_l (k INT, v INT);
CREATE TABLE ${case_db}.oq8_down_r (k INT, w INT);
INSERT INTO ${case_db}.oq8_down_l VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.oq8_down_r VALUES (1, 100), (2, 200), (3, 300);
ANALYZE TABLE ${case_db}.oq8_down_l;
ANALYZE TABLE ${case_db}.oq8_down_r;

-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=WINDOW [
-- @explain_contains=HASH EXCHANGE (source: ShuffleJoin
-- @explain_not_contains=HASH EXCHANGE (source: ShuffleAgg
SELECT l.k,
       r.k AS rk,
       ROW_NUMBER() OVER (PARTITION BY l.k, r.k ORDER BY l.v) AS rn
FROM ${case_db}.oq8_down_l l
INNER JOIN ${case_db}.oq8_down_r r ON l.k = r.k
ORDER BY l.k, r.k;
