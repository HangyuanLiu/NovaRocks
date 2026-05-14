-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,full_rebuild,recovery
-- Test Objective:
-- Validate the REFRESH FULL recovery path after the target MV table is externally
-- corrupted (TargetVisibleFieldDropped). REFRESH FULL drops the damaged target,
-- re-creates the MV target from scratch, and re-establishes the schema contract.
-- A subsequent incremental REFRESH must then succeed.
--
-- Flow: create MV -> refresh -> Spark externally drops visible column from target ->
--   incremental REFRESH fails (TargetVisibleFieldDropped) ->
--   REFRESH FULL succeeds (drops damaged target, recreates, new contract) ->
--   incremental REFRESH succeeds and shows correct data.
--
-- Note: after FULL, the MV is empty; the incremental REFRESH populates it.
-- This tests the complete error -> FULL -> recover -> incremental path.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_a11_full_rb_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_a11_full_rb_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_a11_full_rb_${uuid0}.ns_${uuid0}.base_${uuid0} (
  id INT NOT NULL,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_a11_full_rb_${uuid0}.ns_${uuid0}.base_${uuid0} VALUES
  (1, 100),
  (2, 50),
  (3, 200);

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_a11_full_rb_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, amount FROM base_${uuid0};

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 4
-- Initial MV state: 3 rows.
SELECT id, amount FROM mv_${uuid0} ORDER BY id;

-- query 5
-- Spark: externally drop `amount` from the target MV table (corrupt the target).
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-full-rb-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.mv_${uuid0} DROP COLUMN amount;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- @skip_result_check=true
INSERT INTO ice_ivm_a11_full_rb_${uuid0}.ns_${uuid0}.base_${uuid0} VALUES (4, 400);

-- query 7
-- Incremental refresh must fail: TargetVisibleFieldDropped for `amount`.
-- @expect_error=was dropped
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 8
-- REFRESH FULL must succeed: drops the damaged target, recreates it from scratch,
-- rebuilds the schema contract. The base table is intact (NovaRocks-managed).
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0} FULL;

-- query 9
-- MV is empty immediately after FULL (FULL recreates the empty target).
SELECT id, amount FROM mv_${uuid0} ORDER BY id;

-- query 10
-- Incremental refresh after FULL: reads from base table beginning, must succeed.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 11
-- All 4 rows (including id=4 inserted before the blocked refresh) should now appear.
SELECT id, amount FROM mv_${uuid0} ORDER BY id;

-- query 12
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_${uuid0};
DROP TABLE ice_ivm_a11_full_rb_${uuid0}.ns_${uuid0}.base_${uuid0} FORCE;
DROP DATABASE ice_ivm_a11_full_rb_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_a11_full_rb_${uuid0};
