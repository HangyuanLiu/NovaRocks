-- @order_sensitive=true
-- Validate ALTER TABLE x REMOVE ORPHAN FILES OLDER THAN '<ts>' end-to-end:
--   parser -> engine -> run_remove_orphan_files -> opendal s3 scan -> catalog.
-- Covers: healthy-table noop, empty-table noop, EXPIRE+ORPHAN pipeline,
-- no-OLDER-THAN reject, branch-suffix reject, v2 table support,
-- epoch-ms integer timestamp, future-timestamp acceptance,
-- trailing-tokens reject, combined EXPIRE->ORPHAN->INSERT maintenance sequence.
--
-- Note: "orphan" file simulation is not possible from SQL alone (it requires
-- injecting stale files into the object store). All positive cases therefore
-- run on healthy tables where REMOVE ORPHAN FILES is a noop (deleted_count=0).
-- This is still a meaningful smoke test: it exercises the opendal s3 scan path,
-- the metadata traversal, and the timestamp-filter logic in the real MinIO
-- environment used by the iceberg suite.
--
-- Note: tbl$snapshots metadata table queries are not used here because the iceberg
-- suite's current_catalog mode causes the pre-query table-registration step to try
-- to load `__nr_meta_snapshots__` as a real Iceberg table, which fails. Instead,
-- correctness is verified through data-level assertions (SELECT COUNT(*), SELECT *).
--
-- The runner auto-creates `${case_db}` per case and drops it on cleanup, so
-- there is no explicit CREATE/DROP DATABASE here. All table refs are 2-part
-- `${case_db}.<table>` against the suite-level iceberg catalog
-- (`iceberg_cat_${suite_uuid0}`) selected by `SET catalog` at session setup.

-- ---------------------------------------------------------------------------
-- Case 1: Healthy table -> no deletions, data intact
-- Verifies: future cutoff does not remove any live data files; table remains
-- fully readable after the command completes.
-- ---------------------------------------------------------------------------

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (1, 'a');

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (2, 'b');

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (3, 'c');

-- query 5
-- Sanity: 3 rows present before REMOVE ORPHAN FILES.
SELECT COUNT(*) AS n FROM ${case_db}.t1;

-- query 6
-- @skip_result_check=true
-- Future cutoff: all data files are referenced by live snapshots so 0 files deleted.
ALTER TABLE ${case_db}.t1 REMOVE ORPHAN FILES OLDER THAN '2099-01-01 00:00:00';

-- query 7
-- All 3 rows must remain visible after the noop orphan removal.
SELECT id, v FROM ${case_db}.t1 ORDER BY id;

-- query 8
-- Row count still 3 confirming no data files were removed.
SELECT COUNT(*) AS n FROM ${case_db}.t1;

-- ---------------------------------------------------------------------------
-- Case 2: Empty table -> no deletions, no error
-- Verifies: empty table (no current snapshot, no data files) returns Ok.
-- ---------------------------------------------------------------------------

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 10
-- Table is empty before REMOVE ORPHAN FILES.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- query 11
-- @skip_result_check=true
-- Empty table has no data files to scan; command succeeds without error.
ALTER TABLE ${case_db}.t2 REMOVE ORPHAN FILES OLDER THAN '2099-01-01 00:00:00';

-- query 12
-- Still empty after noop orphan removal.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- ---------------------------------------------------------------------------
-- Case 3: After EXPIRE, ORPHAN succeeds and data is intact
-- Verifies: chaining EXPIRE SNAPSHOTS -> REMOVE ORPHAN FILES works without
-- error and data remains visible.
-- Note: EXPIRE on a healthy main chain is also a noop here (all snapshots live),
-- so ORPHAN still removes 0 files. The test value is the pipeline composition.
-- ---------------------------------------------------------------------------

-- query 13
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES (10, 'x');

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES (20, 'y');

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES (30, 'z');

-- query 17
-- Baseline: 3 rows before the EXPIRE+ORPHAN pipeline.
SELECT COUNT(*) AS n FROM ${case_db}.t3;

-- query 18
-- @skip_result_check=true
-- Step 1: EXPIRE SNAPSHOTS (noop on live chain).
ALTER TABLE ${case_db}.t3 EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00';

-- query 19
-- @skip_result_check=true
-- Step 2: REMOVE ORPHAN FILES after EXPIRE succeeds without error.
ALTER TABLE ${case_db}.t3 REMOVE ORPHAN FILES OLDER THAN '2099-01-01 00:00:00';

-- query 20
-- All rows visible after EXPIRE+ORPHAN pipeline.
SELECT id, v FROM ${case_db}.t3 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 4: No OLDER THAN clause -> reject
-- Verifies: REMOVE ORPHAN FILES without OLDER THAN is a parse-time error.
-- ---------------------------------------------------------------------------

-- query 21
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 22
-- @expect_error=requires OLDER THAN
ALTER TABLE ${case_db}.t4 REMOVE ORPHAN FILES;

-- ---------------------------------------------------------------------------
-- Case 5: Branch suffix -> reject
-- Verifies: parser rejects ALTER TABLE t.branch_dev REMOVE ORPHAN FILES.
-- ---------------------------------------------------------------------------

-- query 23
-- @skip_result_check=true
CREATE TABLE ${case_db}.t5 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 24
-- @expect_error=does not support branch
ALTER TABLE ${case_db}.t5.branch_dev REMOVE ORPHAN FILES OLDER THAN '2099-01-01 00:00:00';

-- ---------------------------------------------------------------------------
-- Case 6: V2 table support
-- Verifies: REMOVE ORPHAN FILES is not v3-only; works on format-version 2 tables.
-- ---------------------------------------------------------------------------

-- query 25
-- @skip_result_check=true
CREATE TABLE ${case_db}.t6 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 26
-- @skip_result_check=true
INSERT INTO ${case_db}.t6 VALUES (1, 'first');

-- query 27
-- @skip_result_check=true
INSERT INTO ${case_db}.t6 VALUES (2, 'second');

-- query 28
-- Baseline: 2 rows before REMOVE ORPHAN FILES on v2 table.
SELECT COUNT(*) AS n FROM ${case_db}.t6;

-- query 29
-- @skip_result_check=true
-- REMOVE ORPHAN FILES on v2 table: noop (all files live) and no error.
ALTER TABLE ${case_db}.t6 REMOVE ORPHAN FILES OLDER THAN '2099-01-01 00:00:00';

-- query 30
-- Data intact after orphan removal on v2 table.
SELECT id, v FROM ${case_db}.t6 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 7: Epoch-ms integer timestamp accepted
-- Verifies: OLDER THAN accepts a bare epoch-ms integer (e.g. 1700000000000).
-- ---------------------------------------------------------------------------

-- query 31
-- @skip_result_check=true
CREATE TABLE ${case_db}.t7 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 32
-- @skip_result_check=true
INSERT INTO ${case_db}.t7 VALUES (1, 'one');

-- query 33
-- @skip_result_check=true
INSERT INTO ${case_db}.t7 VALUES (2, 'two');

-- query 34
-- @skip_result_check=true
-- Epoch-ms integer: 1700000000000 is 2023-11-14, which is before test data.
-- All data files are newer than the cutoff so 0 files are removed.
ALTER TABLE ${case_db}.t7 REMOVE ORPHAN FILES OLDER THAN 1700000000000;

-- query 35
-- Data intact after epoch-ms REMOVE ORPHAN FILES.
SELECT id, v FROM ${case_db}.t7 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 8: Trailing tokens after clause -> reject
-- Verifies: parser rejects extra tokens after the OLDER THAN timestamp.
-- ---------------------------------------------------------------------------

-- query 36
-- @skip_result_check=true
CREATE TABLE ${case_db}.t8 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 37
-- @expect_error=unsupported trailing
ALTER TABLE ${case_db}.t8 REMOVE ORPHAN FILES OLDER THAN '2099-01-01 00:00:00' WHERE size_in_bytes < 100;

-- ---------------------------------------------------------------------------
-- Case 9: OLDER THAN far-future is accepted
-- Verifies: cutoff in year 2099 is parsed and applied without error.
-- (All current data files are "newer" than the threshold under REMOVE ORPHAN
-- semantics when the threshold is in the future -- files referenced by live
-- snapshots are always exempt; this is a noop.)
-- ---------------------------------------------------------------------------

-- query 38
-- @skip_result_check=true
CREATE TABLE ${case_db}.t9 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 39
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (100, 'alpha');

-- query 40
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (200, 'beta');

-- query 41
-- Baseline: 2 rows.
SELECT COUNT(*) AS n FROM ${case_db}.t9;

-- query 42
-- @skip_result_check=true
-- Far-future cutoff: all files are live-referenced, 0 orphans, command succeeds.
ALTER TABLE ${case_db}.t9 REMOVE ORPHAN FILES OLDER THAN '2099-12-31 23:59:59';

-- query 43
-- Row count unchanged confirming no data files removed.
SELECT COUNT(*) AS n FROM ${case_db}.t9;

-- ---------------------------------------------------------------------------
-- Case 10: Combined EXPIRE -> ORPHAN -> INSERT maintenance sequence
-- Verifies: full maintenance pipeline leaves table healthy for continued writes.
-- Multiple INSERTs build up snapshot history, EXPIRE and ORPHAN are applied,
-- then a subsequent INSERT proves the snapshot chain is intact.
-- ---------------------------------------------------------------------------

-- query 44
-- @skip_result_check=true
CREATE TABLE ${case_db}.t10 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 45
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (1, 'row1');

-- query 46
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (2, 'row2');

-- query 47
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (3, 'row3');

-- query 48
-- Pre-maintenance baseline: 3 rows.
SELECT COUNT(*) AS n FROM ${case_db}.t10;

-- query 49
-- @skip_result_check=true
-- Maintenance step 1: expire old snapshots (noop on live chain).
ALTER TABLE ${case_db}.t10 EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00';

-- query 50
-- @skip_result_check=true
-- Maintenance step 2: remove orphan files (noop, no stale files).
ALTER TABLE ${case_db}.t10 REMOVE ORPHAN FILES OLDER THAN '2099-01-01 00:00:00';

-- query 51
-- @skip_result_check=true
-- Maintenance step 3: new INSERT proves snapshot parent chain is healthy.
INSERT INTO ${case_db}.t10 VALUES (4, 'row4');

-- query 52
-- All 4 rows visible: 3 pre-maintenance + 1 post-maintenance.
SELECT id, v FROM ${case_db}.t10 ORDER BY id;

-- query 53
-- Total count confirms no data loss through the full maintenance pipeline.
SELECT COUNT(*) AS n FROM ${case_db}.t10;
