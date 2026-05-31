-- @order_sensitive=true
-- OQ-3 end-to-end cardinality guard.
--
-- Builds a real REST-catalog Iceberg table with 1000 rows where k1 = 1..1000,
-- then asserts that a range predicate (k1 < 100) drives the scan/filter
-- row-count estimate strictly below the full-table row count via the
-- stats={rows=N} trailer that EXPLAIN VERBOSE emits on every physical node.
--
-- Observed numbers (NovaRocks debug build, REST catalog, 2026-05):
--   full table .................. stats={rows=1000}
--   WHERE k1 < 100 .............. stats={rows=500}
-- The reduction proves the selectivity chain (predicate -> LogicalProperties
-- -> stats trailer) is wired end-to-end on a real Iceberg table.
--
-- NOTE: 500 is the default 50% range-predicate selectivity, NOT the ~10%
-- that finite min/max bounds would yield. The NovaRocks Iceberg writer does
-- not currently persist per-column lower/upper bounds into the manifest
-- (value_count and null_count are written, min/max are not), so column min/max
-- stay at +/-inf and the range formula falls back to the default. When the
-- writer starts emitting bounds, k1 < 100 should drop to ~100 rows and the
-- post-filter assertion below must be re-recorded. The full-vs-filtered
-- inequality this case locks holds either way.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0} (
  k1 INT,
  v INT
);

-- query 3
-- @skip_result_check=true
-- Populate 1000 rows with k1 = v = 1..1000.
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0}
SELECT CAST(generate_series AS INT) AS k1, CAST(generate_series AS INT) AS v
  FROM TABLE(generate_series(1, 1000));

-- query 4
-- Sanity: the table holds exactly 1000 rows spanning [1, 1000].
SELECT COUNT(*) AS n, MIN(k1) AS lo, MAX(k1) AS hi
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0};

-- query 5
-- Full-table scan estimate is the real row count.
-- @skip_result_check=true
-- @explain_contains=stats={rows=1000}
SELECT k1 FROM iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0};

-- query 6
-- Range predicate drives the estimate strictly below the full row count.
-- @skip_result_check=true
-- @explain_contains=stats={rows=500}
SELECT k1 FROM iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0}
  WHERE k1 < 100;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0};

-- query 8
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0};
