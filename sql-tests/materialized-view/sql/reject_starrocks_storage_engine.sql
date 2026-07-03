CREATE DATABASE IF NOT EXISTS ${case_db};
CREATE TABLE ${case_db}.base (id int)
TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true");
-- @expect_error=storage_engine='starrocks'
CREATE MATERIALIZED VIEW ${case_db}.mv_bad
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'starrocks')
AS SELECT id FROM ${case_db}.base;
DROP TABLE ${case_db}.base;
DROP DATABASE ${case_db};
