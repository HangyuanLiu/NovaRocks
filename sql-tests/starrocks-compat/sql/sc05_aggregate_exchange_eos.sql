CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

CREATE TABLE aggregate_rows (
  k INT NOT NULL,
  category VARCHAR(16) NOT NULL,
  amount DECIMAL128(18, 2) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO aggregate_rows VALUES
  (1, 'a', 10.00), (2, 'a', 20.50), (3, 'b', 7.25), (4, 'b', 2.75);

-- @be_log_count_at_least=compat_exchange_receive eos=true,2
-- @be_log_be_count_at_least=compat_exchange_receive eos=true,2
SELECT category,
       CAST(SUM(amount) AS DECIMAL128(18, 2)) AS total_amount,
       COUNT(*) AS row_count
FROM aggregate_rows
GROUP BY category
ORDER BY category;
