-- @order_sensitive=true
-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate ALTER TABLE ADD COLUMN on an Iceberg table preserves COUNT(*).
-- 2. Verify the new column is visible with NULL backfill after schema change.
-- Iceberg ALTER is synchronous at metadata commit, so no sleep/retry is needed
-- between the ALTER and the follow-up reads.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t0;
CREATE TABLE ${case_db}.t0 (
  k1 INT,
  c1 INT
);
INSERT INTO ${case_db}.t0 VALUES (1, 1);

-- query 2
-- @order_sensitive=true
SELECT count(*) AS row_count FROM ${case_db}.t0;

-- query 3
-- @order_sensitive=true
SELECT k1, c1 FROM ${case_db}.t0 ORDER BY k1;

-- query 4
-- @skip_result_check=true
ALTER TABLE ${case_db}.t0 ADD COLUMN b1 BOOLEAN;

-- query 5
-- @order_sensitive=true
SELECT count(*) AS row_count FROM ${case_db}.t0;

-- query 6
-- @order_sensitive=true
SELECT k1, c1, b1 FROM ${case_db}.t0 ORDER BY k1;
