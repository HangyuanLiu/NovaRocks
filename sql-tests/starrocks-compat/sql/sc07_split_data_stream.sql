CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

CREATE TABLE skew_left (
  k INT NOT NULL,
  v INT NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE skew_right (
  k INT NOT NULL,
  label VARCHAR(16) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO skew_left VALUES (1, 10), (1, 11), (2, 20), (3, 30);
INSERT INTO skew_right VALUES (1, 'one'), (2, 'two'), (4, 'four');

SET enable_optimize_skew_join_v1 = false;
SET enable_optimize_skew_join_v2 = true;

-- @order_sensitive=true
-- @explain_contains=SplitCastDataSink
-- @be_log_contains=compat_fragment_sink sink=SPLIT_DATA_STREAM_SINK stage=materialized
SELECT l.k, l.v, r.label
FROM skew_left l JOIN [skew|l.k(1)] skew_right r ON l.k = r.k
ORDER BY l.k, l.v;
