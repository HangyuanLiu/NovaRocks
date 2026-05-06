-- @order_sensitive=true
-- Test Point: variant tables reject DELETE / UPDATE / MERGE / INSERT
-- OVERWRITE / ADD EQUALITY DELETE with the matching guard message.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant_neg FORCE;
CREATE TABLE ${case_db}.t_v3_variant_neg (
  id INT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_v3_variant_neg VALUES (1, parse_json('{"a":1}'));

-- query 2
-- @expect_error=variant
DELETE FROM ${case_db}.t_v3_variant_neg WHERE id = 1;

-- query 3
-- @expect_error=variant
UPDATE ${case_db}.t_v3_variant_neg SET id = 2 WHERE id = 1;

-- query 4
-- @expect_error=variant
INSERT OVERWRITE ${case_db}.t_v3_variant_neg VALUES (5, parse_json('{}'));

-- query 5
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_variant_neg FORCE;
