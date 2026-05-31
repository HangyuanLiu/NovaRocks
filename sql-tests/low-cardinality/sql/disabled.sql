-- @tags=low-cardinality,dictionary,disable
-- Verify SET disable_optimizer_rules = 'LowCardinalityDictionaryRewrite'
-- suppresses the rewrite: the plan must not contain a DECODE node even with
-- fresh ANALYZE FULL statistics.
DROP TABLE IF EXISTS ${case_db}.dict_disabled_t;
CREATE TABLE ${case_db}.dict_disabled_t (
  k INT,
  s STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_disabled_t VALUES (1, 'a'), (2, 'b'), (3, 'a');
ANALYZE FULL TABLE ${case_db}.dict_disabled_t;
SET disable_optimizer_rules = 'LowCardinalityDictionaryRewrite';
-- @result_not_contains=DECODE
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT DISTINCT s FROM ${case_db}.dict_disabled_t;
SET disable_optimizer_rules = '';
