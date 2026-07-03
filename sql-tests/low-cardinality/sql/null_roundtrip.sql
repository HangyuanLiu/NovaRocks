-- @tags=low-cardinality,dictionary,null
-- Verify nullable low-cardinality string metadata preserves plain NULL
-- semantics: GROUP BY keeps a NULL group with the correct count.
CREATE TABLE ${case_db}.dict_null_t (
  k INT,
  s STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_null_t VALUES
  (1, 'a'), (2, NULL), (3, 'a'), (4, NULL), (5, 'b');
ANALYZE FULL TABLE ${case_db}.dict_null_t;
-- @explain_not_contains=DECODE
-- @explain_not_contains=dict=[
SELECT s,
  CASE WHEN COUNT(s) = 0 THEN 'true' ELSE 'false' END AS is_null,
  COUNT(*) AS c
FROM ${case_db}.dict_null_t
GROUP BY s
ORDER BY is_null DESC, s;
