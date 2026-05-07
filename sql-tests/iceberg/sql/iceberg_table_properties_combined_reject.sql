-- @order_sensitive=true
-- Parser-level rejects: empty parens, duplicate keys, unsupported grammar.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
USE iceberg_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=at least one
ALTER TABLE p SET TBLPROPERTIES ();

-- query 3
-- @expect_error=at least one
ALTER TABLE p UNSET TBLPROPERTIES ();

-- query 4
-- @expect_error=duplicate
ALTER TABLE p SET TBLPROPERTIES ('a' = '1', 'a' = '2');

-- query 5
-- @expect_error=duplicate
ALTER TABLE p UNSET TBLPROPERTIES ('a', 'a');

-- query 6
DROP TABLE p;
DROP DATABASE iceberg_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
