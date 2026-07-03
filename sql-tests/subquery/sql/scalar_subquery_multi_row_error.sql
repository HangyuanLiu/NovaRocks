-- @tags=subquery,oq13,assert_one_row
-- Test Objective: A scalar subquery that returns more than one row must raise a
-- runtime AssertOneRow error.  This is the base guarantee that
-- RankingWindowPredicatePushdown must not change: returning >1 row in a scalar
-- position must always error, regardless of whether the rule fires or not.
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE smr (k INT, v INT)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO smr VALUES (1, 10), (1, 20);
-- @expect_error=assert_num_rows failed
SELECT (SELECT v FROM smr WHERE k = 1);
