-- @sequential=true
-- Test Objective:
-- 1. Validate an MV over an isolated Iceberg catalog can refresh before and after the external base table is dropped and recreated.
-- 2. Document current NovaRocks post-recreate behavior: refresh succeeds but old MV rows remain visible until table identity invalidation is implemented.
-- Source: adapted from dev/test/sql/test_materialized_view/T/test_mv_with_iceberg_recreate.

-- query 1
-- Use the managed-lake MinIO warehouse with a case-specific suffix so the
-- recreate flow has an isolated writable Hadoop-style Iceberg namespace.
create external catalog mv_iceberg_${uuid0}
properties
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "${iceberg_catalog_type}",
    "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_recreate_${uuid0}",
    "aws.s3.access_key" = "${oss_ak}",
    "aws.s3.secret_key" = "${oss_sk}",
    "aws.s3.endpoint" = "${oss_endpoint}",
    "aws.s3.enable_path_style_access" = "true"
);

-- query 2
set catalog mv_iceberg_${uuid0};

-- query 3
create database mv_iceberg_${uuid0}.mv_ice_db_${uuid0};

-- query 4
set catalog default_catalog;

-- query 5
create table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} (
  col_str string,
  col_int int,
  dt date
) partition by(dt);

-- query 6
insert into mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} values
  ('1d8cf2a2c0e14fa89d8117792be6eb6f', 2000, '2023-12-01'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2000, '2023-12-01'),
  ('abc', 2000, '2023-12-02'),
  (NULL, 2000, '2023-12-02'),
  ('ab1d8cf2a2c0e14fa89d8117792be6eb6f', 2001, '2023-12-03'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2001, '2023-12-03'),
  ('abc', 2001, '2023-12-04'),
  (NULL, 2001, '2023-12-04');

-- query 7
set catalog default_catalog;

-- query 8
create database db_${uuid0};

-- query 9
use db_${uuid0};

-- query 10
CREATE MATERIALIZED VIEW test_mv1
DISTRIBUTED BY HASH(dt) BUCKETS 2
REFRESH DEFERRED MANUAL AS SELECT dt, sum(col_int) AS s
FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0}  GROUP BY dt;

-- query 11
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_mv1;

-- query 12
-- @skip_result_check=true
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt='2023-12-01' GROUP BY dt;

-- query 13
-- @skip_result_check=true
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt='2023-12-02' GROUP BY dt;

-- query 14
-- @skip_result_check=true
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt='2023-12-03' GROUP BY dt;

-- query 15
-- @skip_result_check=true
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} GROUP BY dt;

-- query 16
-- @skip_result_check=true
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt='2023-12-03' GROUP BY dt;

-- query 17
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt>='2023-12-03' GROUP BY dt order by dt;

-- query 18
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} GROUP BY dt order by dt;

-- query 19
admin set frontend config('enable_mv_automatic_active_check'='false');

-- query 20
-- drop base table
set catalog default_catalog;

-- query 21
DROP TABLE mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0};

-- query 22
set catalog default_catalog;

-- query 23
use db_${uuid0};

-- query 24
-- recreate it
set catalog default_catalog;

-- query 25
create table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} (
  col_str string,
  col_int int,
  dt date
) partition by(dt);

-- query 26
insert into mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} values
  ('1d8cf2a2c0e14fa89d8117792be6eb6f', 2000, '2023-12-01'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2000, '2023-12-01');

-- query 27
set catalog default_catalog;

-- query 28
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW db_${uuid0}.test_mv1;

-- query 29
-- @result_contains=test_mv1
SHOW MATERIALIZED VIEWS FROM db_${uuid0};

-- query 30
-- @result_not_contains=test_mv1
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt='2023-12-01' GROUP BY dt;

-- query 31
USE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
SELECT dt,sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt>='2023-12-03' GROUP BY dt order by dt;

-- query 32
-- Current behavior: refresh after external table recreate appends to the
-- existing MV state instead of invalidating/replacing it.
select * from db_${uuid0}.test_mv1 order by 1, 2;

-- query 33
admin set frontend config('enable_mv_automatic_active_check'='true');

-- query 34
drop table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} force;

-- query 35
drop materialized view db_${uuid0}.test_mv1;

-- query 36
drop database mv_iceberg_${uuid0}.mv_ice_db_${uuid0} force;

-- query 37
drop catalog mv_iceberg_${uuid0};
