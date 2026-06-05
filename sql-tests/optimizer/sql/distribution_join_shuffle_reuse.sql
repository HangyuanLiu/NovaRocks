-- @tags=optimizer,oq8,distribution
DROP TABLE IF EXISTS ${case_db}.oq8_reuse_l;
DROP TABLE IF EXISTS ${case_db}.oq8_reuse_r;
CREATE TABLE ${case_db}.oq8_reuse_l (k INT, v INT);
CREATE TABLE ${case_db}.oq8_reuse_r (k INT, v INT);
INSERT INTO ${case_db}.oq8_reuse_l VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.oq8_reuse_r VALUES (1, 100), (2, 200), (3, 300);

SET disable_optimizer_rules = 'JoinCommutativity';

-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=HASH EXCHANGE (source: ShuffleJoin
SELECT l.k, SUM(r.v) AS total_v
FROM ${case_db}.oq8_reuse_l l
INNER JOIN ${case_db}.oq8_reuse_r r ON l.k = r.k
GROUP BY l.k
ORDER BY l.k;

SET disable_optimizer_rules = '';
