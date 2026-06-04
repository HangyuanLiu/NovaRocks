-- @tags=optimizer,oq8,distribution
-- Keep the lineitem/orders names: optimizer fixture stats assign these names
-- fallback-scale row counts, which should conservatively reject broadcast.
DROP TABLE IF EXISTS ${case_db}.lineitem;
DROP TABLE IF EXISTS ${case_db}.orders;
CREATE TABLE ${case_db}.lineitem (k INT, v INT);
CREATE TABLE ${case_db}.orders (k INT, w INT);
INSERT INTO ${case_db}.lineitem VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.orders VALUES (1, 100), (2, 200);

SET disable_optimizer_rules = 'JoinCommutativity';

-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=HASH JOIN (BROADCAST
SELECT l.k, r.w
FROM ${case_db}.lineitem l
INNER JOIN ${case_db}.orders r ON l.k = r.k
ORDER BY l.k;

SET disable_optimizer_rules = '';
