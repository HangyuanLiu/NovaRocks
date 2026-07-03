-- @tags=low-cardinality,dictionary,disable
-- Verify the retired standalone rewrite no longer needs a session disable rule:
-- fresh ANALYZE FULL plans stay free of native dictionary plan nodes.
CREATE TABLE ${case_db}.dict_disabled_t (
  k INT,
  s STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_disabled_t VALUES (1, 'a'), (2, 'b'), (3, 'a');
ANALYZE FULL TABLE ${case_db}.dict_disabled_t;
-- @explain_not_contains=DECODE
-- @explain_not_contains=dict=[
-- @skip_result_check=true
SELECT DISTINCT s FROM ${case_db}.dict_disabled_t;
