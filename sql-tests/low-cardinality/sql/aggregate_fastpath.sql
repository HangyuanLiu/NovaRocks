-- @tags=low-cardinality,dictionary,aggregate
-- C3: aggregate may keep a single dict group-by slot encoded internally.
-- Results must match flat Utf8 semantics, including NULL group handling.
DROP TABLE IF EXISTS ${case_db}.dict_agg_fastpath_t;
CREATE TABLE ${case_db}.dict_agg_fastpath_t (
  k INT,
  status STRING,
  region STRING,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_agg_fastpath_t VALUES
  (1, 'PAID', 'east', 10),
  (2, 'NEW', 'west', 20),
  (3, 'PAID', 'west', 30),
  (4, NULL, 'east', 40),
  (5, 'CANCELLED', 'east', 50),
  (6, NULL, 'west', 60);
ANALYZE FULL TABLE ${case_db}.dict_agg_fastpath_t;

SELECT status, COUNT(*) AS c, SUM(v) AS total
FROM ${case_db}.dict_agg_fastpath_t
GROUP BY status
ORDER BY status IS NOT NULL, status;

-- Mixed dict+plain grouping is deliberately hydrated/fallback in C3 first cut.
SELECT status, region, COUNT(*) AS c
FROM ${case_db}.dict_agg_fastpath_t
GROUP BY status, region
ORDER BY status IS NOT NULL, status, region;

-- min/max on dict string remains correctness-first fallback.
SELECT MIN(status), MAX(status)
FROM ${case_db}.dict_agg_fastpath_t;
