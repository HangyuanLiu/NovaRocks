-- Test Objective:
-- A catalog that cannot enumerate views must say so, rather than let
-- DROP DATABASE ... FORCE assume the namespace holds none. This suite's
-- default catalog type is hadoop, which cannot hold views at all.
--
-- FORCE expands into a listing of the namespace's children, so a catalog that
-- cannot list one kind of child cannot answer the question. It used to answer
-- "no views" anyway, which is indistinguishable from an authoritative result.
-- The refusal is a deliberate, user-visible behavior change.

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_drop_force_views;
CREATE DATABASE sql_tests_drop_force_views;
USE sql_tests_drop_force_views;

-- query 2
-- @skip_result_check=true
CREATE TABLE force_probe (id BIGINT);

-- query 3
-- An absent target never reaches view enumeration: the namespace check runs
-- first, and IF EXISTS makes it a no-op.
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_drop_force_absent FORCE;

-- query 4
-- The namespace exists, so FORCE must enumerate its views, and this catalog
-- cannot answer that.
-- @expect_error=not supported by this catalog
DROP DATABASE sql_tests_drop_force_views FORCE;

-- query 5
-- Dropping the children explicitly is how a namespace is removed on a catalog
-- that cannot enumerate views.
-- @skip_result_check=true
USE sql_tests_drop_force_views;
DROP TABLE IF EXISTS force_probe;
DROP DATABASE IF EXISTS sql_tests_drop_force_views;
