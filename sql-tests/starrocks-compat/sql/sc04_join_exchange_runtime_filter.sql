CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

CREATE TABLE fact (
  k INT NOT NULL,
  amount DECIMAL128(18, 2) NOT NULL,
  payload VARCHAR(64)
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE dim (
  k INT NOT NULL,
  rate DECIMAL128(18, 2) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO fact VALUES (1, 10.25, 'a'), (2, 20.50, 'b'), (3, 30.75, 'c');
INSERT INTO dim VALUES (1, 2.00), (3, 4.00);
SET enable_global_runtime_filter = true;

-- @be_log_count_at_least=compat_exchange_receive eos=true,2
-- @be_log_be_count_at_least=compat_exchange_receive eos=true,2
-- @explain_contains=INNER JOIN (BROADCAST)
-- @explain_contains=build runtime filters:
-- @explain_contains=MERGING-EXCHANGE
-- @explain_contains=TOP-N
SELECT f.k, CAST(f.amount * d.rate AS DECIMAL128(18, 2)) AS weighted
FROM (
  SELECT k, amount
  FROM fact
  ORDER BY k
  LIMIT 100
) f JOIN [BROADCAST] dim d ON f.k = d.k
ORDER BY f.k;
