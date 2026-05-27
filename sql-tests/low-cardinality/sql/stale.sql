-- @tags=low-cardinality,dictionary,stale
-- Verify a write after ANALYZE FULL flips the snapshot to STALE and the
-- next query falls back to the plain string operator (no DECODE in plan).
DROP TABLE IF EXISTS ${case_db}.dict_stale_t;
CREATE TABLE ${case_db}.dict_stale_t (
  k INT,
  s STRING
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1');
INSERT INTO ${case_db}.dict_stale_t VALUES (1, 'a'), (2, 'b');
ANALYZE FULL TABLE ${case_db}.dict_stale_t;
INSERT INTO ${case_db}.dict_stale_t VALUES (3, 'c');
-- @result_not_contains=DECODE
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT DISTINCT s FROM ${case_db}.dict_stale_t;
SELECT DISTINCT s FROM ${case_db}.dict_stale_t ORDER BY s;
