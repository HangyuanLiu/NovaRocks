-- @order_sensitive=true
-- Test Point: INSERT INTO t (a) SELECT ... materializes write-default for column b on the FROM QUERY path.
-- Method: CREATE v3 table with b INT DEFAULT 5, INSERT (a) SELECT 1, SELECT to confirm row materialized as (1, 5).
-- Scope: write-default on FROM QUERY path (Task 17).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_iselect FORCE;
CREATE TABLE ${case_db}.t_v3_default_iselect (
  a INT,
  b INT DEFAULT 5
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_v3_default_iselect (a) SELECT 1;

-- query 2
SELECT a, b FROM ${case_db}.t_v3_default_iselect;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_iselect FORCE;
