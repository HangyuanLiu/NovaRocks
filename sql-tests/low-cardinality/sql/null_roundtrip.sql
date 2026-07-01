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
-- @result_contains=dict=[s]
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT s,
  CASE WHEN COUNT(s) = 0 THEN 'true' ELSE 'false' END AS is_null,
  COUNT(*) AS c
FROM ${case_db}.dict_null_t
GROUP BY s
ORDER BY is_null DESC, s;
SELECT s,
  CASE WHEN COUNT(s) = 0 THEN 'true' ELSE 'false' END AS is_null,
  COUNT(*) AS c
FROM ${case_db}.dict_null_t
GROUP BY s
ORDER BY is_null DESC, s;
