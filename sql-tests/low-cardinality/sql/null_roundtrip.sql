-- @tags=low-cardinality,dictionary,null
-- Verify a nullable dict-encoded column round-trips NULL through the dict-id
-- plan: NULL -> null_id -> NULL, and GROUP BY keeps a NULL group with the
-- correct count. Locks the null-semantics gate.
DROP TABLE IF EXISTS ${case_db}.dict_null_t;
CREATE TABLE ${case_db}.dict_null_t (
  k INT,
  s STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_null_t VALUES
  (1, 'a'), (2, NULL), (3, 'a'), (4, NULL), (5, 'b');
ANALYZE FULL TABLE ${case_db}.dict_null_t;
-- @result_contains=DECODE
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT s, COUNT(*) FROM ${case_db}.dict_null_t GROUP BY s;
SELECT s, COUNT(*) AS c FROM ${case_db}.dict_null_t GROUP BY s ORDER BY s;
