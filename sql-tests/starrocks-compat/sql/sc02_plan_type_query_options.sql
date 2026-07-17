CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

CREATE TABLE typed_rows (
  k INT NOT NULL,
  flag BOOLEAN NOT NULL,
  bigv LARGEINT NOT NULL,
  amount DECIMAL128(18, 2) NOT NULL,
  event_date DATE NOT NULL,
  event_time DATETIME NOT NULL,
  tags ARRAY<INT> NOT NULL,
  attrs MAP<VARCHAR(16), INT> NOT NULL,
  note VARCHAR(32) NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO typed_rows VALUES
  (1, TRUE, 170141183460469231731687303715884105, 12.34,
   '2026-07-17', '2026-07-17 12:34:56', [1, 2], map{'a': 7}, NULL);

SET time_zone = 'UTC';
SET query_timeout = 60;
SET pipeline_dop = 2;

-- @compat_probe=malformed-plan
SELECT flag,
       bigv,
       amount,
       event_date,
       event_time,
       array_length(tags) AS tag_count,
       element_at(attrs, 'a') AS attr_a,
       note IS NULL AS note_is_null
FROM typed_rows
ORDER BY k;
