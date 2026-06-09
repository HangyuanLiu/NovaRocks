-- @tags=iceberg,statistics,ndv
-- Verify ANALYZE writes Puffin NDV so the optimizer uses the real-NDV
-- denominator estimate instead of the many-to-many fallback for an iceberg
-- join. Same-session ANALYZE-then-EXPLAIN relies on ANALYZE invalidating the
-- table-metadata cache.

-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0};

-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0} (k INT, payload INT);

-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0} (k INT, flag INT);

-- l: k in [0,99] over 1000 rows -> NDV(k)=100 ; r: k in [0,79] over 800 -> NDV(k)=80
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0}
  SELECT generate_series % 100, generate_series FROM TABLE(generate_series(1, 1000));

-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0}
  SELECT generate_series % 80, generate_series % 2 FROM TABLE(generate_series(1, 800));

-- @skip_result_check=true
ANALYZE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0};

-- @skip_result_check=true
ANALYZE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0};

-- With real NDV the inner-join estimate uses |l|*|r|/max(ndv_l,ndv_r), which is
-- far below the many-to-many fallback (|l|*|r|*0.25). Assert the optimizer does
-- NOT produce the many-to-many blow-up for this same-scale join.
-- @explain_contains=HASH JOIN
-- @explain_not_contains=stats={rows=162000}
EXPLAIN VERBOSE SELECT l.k
FROM iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0} l
JOIN iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0} r ON l.k = r.k;

-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0};
