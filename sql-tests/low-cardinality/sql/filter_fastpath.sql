-- @tags=low-cardinality,dictionary,filter
-- Verify simple string filters over low-cardinality metadata preserve selected
-- and passthrough string columns.
CREATE TABLE ${case_db}.dict_filter_fastpath_c1_t (
  id INT,
  status STRING,
  channel STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_filter_fastpath_c1_t VALUES
  (1, 'PAID', 'web'),
  (2, 'PENDING', 'retail'),
  (3, 'CLOSED', 'ops'),
  (4, NULL, 'retail'),
  (5, 'PAID', 'ops'),
  (6, NULL, 'web');
ANALYZE FULL TABLE ${case_db}.dict_filter_fastpath_c1_t;
SELECT id, status, channel
FROM ${case_db}.dict_filter_fastpath_c1_t
WHERE status = 'PAID'
ORDER BY id;
SELECT id, status
FROM ${case_db}.dict_filter_fastpath_c1_t
WHERE status IN ('PAID', 'CLOSED')
ORDER BY id;
SELECT id, status
FROM ${case_db}.dict_filter_fastpath_c1_t
WHERE status IS NULL
ORDER BY id;
