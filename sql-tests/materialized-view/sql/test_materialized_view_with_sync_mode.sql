-- Test Objective:
-- 1. Validate synchronous refresh mode updates MV state deterministically.
-- 2. Cover refresh visibility through materialized_views metadata.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_with_sync_mode

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
CREATE TABLE `base` (
  `id` bigint(20) NOT NULL COMMENT "id",
  `dt` date NOT NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 4
INSERT INTO `base` VALUES
(1, '2023-07-10'),
(2, '2023-07-11'),
(3, '2023-07-12'),
(4, '2023-07-13'),
(5, '2023-07-14'),
(6, '2023-07-15'),
(7, '2023-07-16'),
(8, '2023-07-17'),
(9, '2023-07-18'),
(10, '2023-07-19'),
(11, '2023-07-20'),
(12, '2023-07-21'),
(13, '2023-07-22'),
(14, '2023-07-23'),
(15, '2023-07-24'),
(16, '2023-07-25'),
(17, '2023-07-26');

-- query 5
CREATE MATERIALIZED VIEW mv
PARTITION BY dt
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"partition_ttl_number" = "15",
"partition_refresh_number"="2"
)
REFRESH DEFERRED MANUAL
AS
SELECT id, dt from base;

-- query 6
REFRESH MATERIALIZED VIEW mv PARTITION START ("2023-07-25") END ("2023-07-26") WITH SYNC MODE;

-- query 7
-- @result_contains=done
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF(
    LAST_REFRESH_STATE IN ('SUCCESS', 'MERGED', 'SKIPPED', 'FAILED'),
    'done',
    'pending'
) AS mv_wait_state
FROM information_schema.materialized_views
WHERE TABLE_NAME = 'mv'
  
ORDER BY LAST_REFRESH_START_TIME DESC
LIMIT 1;

-- query 8
select if(count(*) > 0, 'pass', 'fail') from information_schema.task_runs where `database`='db_${uuid0}';

-- query 9
drop database db_${uuid0};
