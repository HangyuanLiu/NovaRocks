-- @order_sensitive=true
-- Validate static Iceberg scan pruning preserves results for identity partitions and file stats.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_scan_pruning;
CREATE TABLE ${case_db}.t_scan_pruning (
  id INT,
  part_id INT,
  payload STRING
)
PARTITION BY (part_id);
INSERT INTO ${case_db}.t_scan_pruning VALUES
  (1, 1, 'cold-a'),
  (2, 1, 'cold-b'),
  (12, 12, 'target'),
  (13, 12, 'neighbor');

-- query 2
SELECT id, part_id, payload
FROM ${case_db}.t_scan_pruning
WHERE part_id = 12 AND id = 12
ORDER BY id;

-- query 3
SELECT COUNT(*) AS cnt
FROM ${case_db}.t_scan_pruning
WHERE part_id = 99 AND id = 99;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_scan_pruning FORCE;
