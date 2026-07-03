-- @tags=low-cardinality,dictionary,rewrite
-- Verify ANALYZE FULL keeps standalone SQL on plain string plan shape while
-- GROUP BY results remain correct over dictionary metadata.
CREATE TABLE ${case_db}.dict_rewrite_t (
  k INT,
  s STRING,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_rewrite_t VALUES
  (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40);
ANALYZE FULL TABLE ${case_db}.dict_rewrite_t;
-- @explain_not_contains=DECODE
-- @explain_not_contains=dict=[
SELECT s, SUM(v) FROM ${case_db}.dict_rewrite_t GROUP BY s ORDER BY s;
