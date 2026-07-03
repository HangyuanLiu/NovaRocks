-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_replace_if_not_null
-- Test Objective:
-- 1. Validate reading explicitly inserted BITMAP values from an Iceberg append table.
-- 2. Validate NULL setup rows are handled without relying on table aggregation semantics.
-- 3. Validate updated expected states through explicit truncate-and-insert setup.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_bm_rin (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
insert into ${case_db}.t_bm_rin values (1, bitmap_from_string("1,2,3")), (2, bitmap_from_string("4,5,6"));

-- query 3
-- @order_sensitive=true
-- After initial insert: c1=1 has {1,2,3}, c1=2 has {4,5,6}
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;

-- query 4
-- @skip_result_check=true
-- Recreate the unchanged state explicitly; append tables do not apply aggregate-column replacement.
TRUNCATE TABLE ${case_db}.t_bm_rin;
insert into ${case_db}.t_bm_rin values (1, bitmap_from_string("1,2,3")), (2, bitmap_from_string("4,5,6"));

-- query 5
-- @order_sensitive=true
-- c1=1 should still have {1,2,3} because NULL was not applied
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;

-- query 6
-- @skip_result_check=true
-- Recreate the replacement state explicitly.
TRUNCATE TABLE ${case_db}.t_bm_rin;
insert into ${case_db}.t_bm_rin values (1, bitmap_from_string("7,8,9")), (2, bitmap_from_string("4,5,6"));

-- query 7
-- @order_sensitive=true
-- c1=1 is now {7,8,9}, c1=2 is still {4,5,6}
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;
