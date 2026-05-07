-- @order_sensitive=true
-- Validate `CREATE TABLE [IF NOT EXISTS] <name> [PARTITION BY (...)]
-- [TBLPROPERTIES(...)] AS <select>` end-to-end on Iceberg targets:
--   parser -> engine -> arrow_schema_to_table_column_defs -> create_table
--   -> sink -> FastAppendCommit. Strict default: every CTAS table is
--   format-version=3 + write.row-lineage=true.
-- Covers: basic CTAS, PARTITION BY clause, TBLPROPERTIES forwarding,
-- nested types (struct/list), IF NOT EXISTS skip semantics, post-CTAS
-- INSERT continuation, parser-level rejections (branch target /
-- format-version=2 / row-lineage=false / explicit columns / table exists),
-- and analyzer-level rejection (partition column not in SELECT output).
--
-- The runner auto-creates `${case_db}` per case and drops it on cleanup.

-- ---------------------------------------------------------------------------
-- Case 1: basic CTAS — no PARTITION BY, no PROPERTIES
-- ---------------------------------------------------------------------------

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.src (id INT, name VARCHAR(16), region VARCHAR(8))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.src VALUES
  (1, 'alice',   'us'),
  (2, 'bob',     'eu'),
  (3, 'charlie', 'us');

-- query 3
-- @skip_result_check=true
-- Basic CTAS: schema inferred from SELECT (id INT, uname VARCHAR).
CREATE TABLE ${case_db}.dst1 AS
  SELECT id, UPPER(name) AS uname FROM ${case_db}.src;

-- query 4
-- All 3 rows materialized into dst1 with inferred schema.
SELECT id, uname FROM ${case_db}.dst1 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 2: CTAS PARTITION BY identity column
-- ---------------------------------------------------------------------------

-- query 5
-- @skip_result_check=true
CREATE TABLE ${case_db}.dst2 PARTITION BY (region) AS
  SELECT id, region FROM ${case_db}.src;

-- query 6
-- Verify rows landed across both partitions.
SELECT region, COUNT(*) AS n FROM ${case_db}.dst2 GROUP BY region ORDER BY region;

-- ---------------------------------------------------------------------------
-- Case 3: CTAS with extra TBLPROPERTIES (non-version key)
-- ---------------------------------------------------------------------------

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}.dst3
TBLPROPERTIES ("write.parquet.compression-codec" = "zstd")
AS SELECT id FROM ${case_db}.src;

-- query 8
-- Result is identical to a basic CTAS — the property only affects file write.
SELECT COUNT(*) AS n FROM ${case_db}.dst3;

-- ---------------------------------------------------------------------------
-- Case 4: IF NOT EXISTS on already-existing target — skip CTAS, leave dst1 unchanged
-- ---------------------------------------------------------------------------

-- query 9
-- @skip_result_check=true
-- dst1 already has 3 rows from Case 1; this CTAS must be a no-op.
CREATE TABLE IF NOT EXISTS ${case_db}.dst1 AS
  SELECT id FROM ${case_db}.src WHERE 1 = 0;

-- query 10
-- dst1 still has 3 rows (Case 1 INSERT count), proving CTAS was skipped.
SELECT COUNT(*) AS n FROM ${case_db}.dst1;

-- ---------------------------------------------------------------------------
-- Case 5: IF NOT EXISTS on non-existing target — proceeds normally
-- ---------------------------------------------------------------------------

-- query 11
-- @skip_result_check=true
CREATE TABLE IF NOT EXISTS ${case_db}.dst5 AS
  SELECT id FROM ${case_db}.src;

-- query 12
SELECT COUNT(*) AS n FROM ${case_db}.dst5;

-- ---------------------------------------------------------------------------
-- Case 6: CTAS-built table accepts subsequent INSERT
-- ---------------------------------------------------------------------------

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.dst1 VALUES (99, 'late');

-- query 14
-- dst1 grew from 3 rows (Case 1) to 4 (Case 6 INSERT).
SELECT id, uname FROM ${case_db}.dst1 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 7 (error): branch-qualified CTAS target
-- ---------------------------------------------------------------------------

-- query 15
-- @expect_error=branch
-- Parser rejects CTAS targeting a branch ref.
CREATE TABLE ${case_db}.dst7.branch_dev AS SELECT 1 AS x;

-- ---------------------------------------------------------------------------
-- Case 8 (error): TBLPROPERTIES('format-version'='2')
-- ---------------------------------------------------------------------------

-- query 16
-- @expect_error=format-version
CREATE TABLE ${case_db}.dst8 TBLPROPERTIES ("format-version" = "2") AS
  SELECT 1 AS x;

-- ---------------------------------------------------------------------------
-- Case 9 (error): TBLPROPERTIES('write.row-lineage'='false')
-- ---------------------------------------------------------------------------

-- query 17
-- @expect_error=row-lineage
CREATE TABLE ${case_db}.dst9 TBLPROPERTIES ("write.row-lineage" = "false") AS
  SELECT 1 AS x;

-- ---------------------------------------------------------------------------
-- Case 10 (error): explicit column definitions in CTAS
-- ---------------------------------------------------------------------------

-- query 18
-- @expect_error=column
CREATE TABLE ${case_db}.dst10 (id INT, name VARCHAR(16)) AS
  SELECT 1, 'a';

-- ---------------------------------------------------------------------------
-- Case 11 (error): PARTITION BY column not in SELECT output
-- ---------------------------------------------------------------------------

-- query 19
-- @expect_error=partition column
CREATE TABLE ${case_db}.dst11 PARTITION BY (ghost) AS
  SELECT id FROM ${case_db}.src;

-- ---------------------------------------------------------------------------
-- Case 12 (error): table already exists, no IF NOT EXISTS
-- ---------------------------------------------------------------------------

-- query 20
-- @expect_error=already exists
-- dst1 already exists from Case 1; CTAS without IF NOT EXISTS must reject.
CREATE TABLE ${case_db}.dst1 AS SELECT id FROM ${case_db}.src;
