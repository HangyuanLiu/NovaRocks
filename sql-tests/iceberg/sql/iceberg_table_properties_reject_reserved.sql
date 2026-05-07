-- @order_sensitive=true
-- Denylist coverage: each reserved category errors clearly.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.tblprops_reject_${uuid0};
USE iceberg_cat_${suite_uuid0}.tblprops_reject_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=format-version is reserved
ALTER TABLE p SET TBLPROPERTIES ('format-version' = '3');

-- query 3
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p SET TBLPROPERTIES ('identifier-field-ids' = '[1]');

-- query 4
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p SET TBLPROPERTIES ('current-schema-id' = '5');

-- query 5
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE p SET TBLPROPERTIES ('novarocks.logical_type.foo' = 'TINYINT');

-- query 6
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE p SET TBLPROPERTIES ('novarocks.future' = 'whatever');

-- query 7
-- UNSET path covered too.
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p UNSET TBLPROPERTIES ('last-column-id');

-- query 8
DROP TABLE p;
DROP DATABASE iceberg_cat_${suite_uuid0}.tblprops_reject_${uuid0};
