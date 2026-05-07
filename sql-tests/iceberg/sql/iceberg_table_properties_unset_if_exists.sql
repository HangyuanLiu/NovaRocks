-- @order_sensitive=true
-- UNSET TBLPROPERTIES strict vs IF EXISTS.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
USE iceberg_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");
ALTER TABLE p SET TBLPROPERTIES ('a' = '1', 'b' = '2');

-- query 2
SELECT count(*) FROM p;

-- query 3
-- Strict: missing key fails.
-- @expect_error=UNSET TBLPROPERTIES key 'c' does not exist
ALTER TABLE p UNSET TBLPROPERTIES ('a', 'c');

-- query 4
-- Existing keys unchanged after the failed strict UNSET; table still queryable.
SELECT count(*) FROM p;

-- query 5
-- IF EXISTS: missing keys silently skipped, present keys still removed.
ALTER TABLE p UNSET TBLPROPERTIES IF EXISTS ('a', 'c');

-- query 6
SELECT count(*) FROM p;

-- query 7
DROP TABLE p;
DROP DATABASE iceberg_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
