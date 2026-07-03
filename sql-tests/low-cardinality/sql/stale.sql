-- @tags=low-cardinality,dictionary,stale
-- Verify a write after ANALYZE FULL leaves stale dictionary metadata behind
-- without changing the rows returned by a subsequent query.
CREATE TABLE ${case_db}.dict_stale_t (
  k INT,
  s STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_stale_t VALUES (1, 'a'), (2, 'b');
ANALYZE FULL TABLE ${case_db}.dict_stale_t;
INSERT INTO ${case_db}.dict_stale_t VALUES (3, 'c');
SELECT DISTINCT s FROM ${case_db}.dict_stale_t ORDER BY s;
