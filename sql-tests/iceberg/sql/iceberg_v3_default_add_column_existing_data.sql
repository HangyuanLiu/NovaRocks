-- @order_sensitive=true
-- Test Point: ALTER TABLE ADD COLUMN with DEFAULT on a v3 table with existing rows: old rows read default via initial-default; new INSERTs materialize write-default.
-- Method: CREATE v3 table, INSERT two rows, ALTER ADD COLUMN b INT DEFAULT 9, SELECT (expect default backfill on existing rows), then INSERT one more row with column list, SELECT again.
-- Scope: read-side initial-default backfill (A) + write-side write-default materialization (B).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_addcol FORCE;
CREATE TABLE ${case_db}.t_v3_default_addcol (
  a INT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_v3_default_addcol VALUES (1), (2);
ALTER TABLE ${case_db}.t_v3_default_addcol ADD COLUMN b INT DEFAULT 9;

-- query 2
-- Pre-existing rows must read b = 9 via initial-default backfill, not NULL.
SELECT a, b FROM ${case_db}.t_v3_default_addcol ORDER BY a;

-- query 3
-- New INSERT (col list omits b) must materialize write-default = 9.
INSERT INTO ${case_db}.t_v3_default_addcol (a) VALUES (3);
SELECT a, b FROM ${case_db}.t_v3_default_addcol ORDER BY a;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_addcol FORCE;
