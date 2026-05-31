-- @tags=low-cardinality,dictionary,rewrite
-- Verify ANALYZE FULL + LowCardinalityDictionaryRewrite drives DISTINCT and
-- GROUP BY through a dict-id plan and decodes at the user output boundary.
DROP TABLE IF EXISTS ${case_db}.dict_rewrite_t;
CREATE TABLE ${case_db}.dict_rewrite_t (
  k INT,
  s STRING,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_rewrite_t VALUES
  (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40);
ANALYZE FULL TABLE ${case_db}.dict_rewrite_t;
-- @result_contains=DECODE
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT DISTINCT s FROM ${case_db}.dict_rewrite_t;
SELECT s, SUM(v) FROM ${case_db}.dict_rewrite_t GROUP BY s ORDER BY s;
