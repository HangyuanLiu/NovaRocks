-- Migrated from dev/test/sql/test_agg/R/test_meta_scan_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_meta_scan_agg @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table t0 (
    c0 DATE,
    c1 INT,
    c2 BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
set enable_rewrite_simple_agg_to_meta_scan=true;

-- query 4
USE ${case_db};
select count(*) from t0[_META_];

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t0 values ('2024-01-01', 1, 1), ('2024-01-02', 2, 2), ('2024-01-03', 3, 3), ('2024-01-04', 4, 4), ('2024-01-05', 5, 5);

-- query 6
-- @skip_result_check=true
USE ${case_db};
set enable_exchange_pass_through=false;

-- query 7
USE ${case_db};
select count() from t0;

-- query 8
USE ${case_db};
select count(c0) from t0;

-- query 9
USE ${case_db};
select count(c1) from t0;

-- query 10
USE ${case_db};
select count(c2) from t0;

-- query 11
USE ${case_db};
select min(c0),max(c0),min(c1),max(c1),min(c2),max(c2) from t0;

-- query 12
USE ${case_db};
select count(c0) from t0[_META_];

-- query 13
USE ${case_db};
select count(c1) from t0[_META_];

-- query 14
USE ${case_db};
select count(c2) from t0[_META_];

-- query 15
USE ${case_db};
select min(c0),max(c0),min(c1),max(c1),min(c2),max(c2) from t0[_META_];

-- query 16
USE ${case_db};
select count(1) from t0[_META_];

-- query 17
USE ${case_db};
select count(*) from t0[_META_];

-- query 18
-- @skip_result_check=true
USE ${case_db};
create table t1 (
    c0 DATE,
    c1 INT
)
TBLPROPERTIES ("format-version" = "3");

-- query 19
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ('2024-01-15', 10), ('2024-01-20', 20), ('2024-02-10', 30), ('2024-02-15', 40);

-- name: test_meta_scan_with_renamed_column
-- query 20
-- @skip_result_check=true
USE ${case_db};
create table t2 (
    c0 INT
)
TBLPROPERTIES ("format-version" = "3");

-- query 21
-- @skip_result_check=true
USE ${case_db};
insert into t2 values (1), (2), (3);

-- query 22
USE ${case_db};
select count(*) from t2[_META_];

-- query 23
USE ${case_db};
select count(*) from t2;

-- query 24
-- @skip_result_check=true
USE ${case_db};
alter table t2 rename column c0 to c1;

-- query 25
USE ${case_db};
select count(*) from t2[_META_];

-- query 26
USE ${case_db};
select count(*) from t2;

-- query 27
-- @skip_result_check=true
USE ${case_db};
alter table t2 rename column c1 to c2;

-- query 28
USE ${case_db};
select count(*) from t2[_META_];

-- query 29
USE ${case_db};
select count(*) from t2;
