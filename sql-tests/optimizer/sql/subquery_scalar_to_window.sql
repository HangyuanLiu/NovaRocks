-- @tags=optimizer,oq13,subquery_to_window
-- Test Objective: a correlated scalar-aggregate subquery in a WHERE comparison
-- is rewritten to an analytic WINDOW over the outer relation (StarRocks WinMagic),
-- not a re-scan + LEFT OUTER JOIN. apply mode only.
DROP TABLE IF EXISTS ${case_db}.wm_line;
DROP TABLE IF EXISTS ${case_db}.wm_part;
CREATE TABLE ${case_db}.wm_line (l_partkey INT, l_quantity INT, l_ext INT);
CREATE TABLE ${case_db}.wm_part (p_partkey INT, p_brand VARCHAR(16));
INSERT INTO ${case_db}.wm_line VALUES (1,5,100),(1,50,200),(2,7,300),(2,8,150),(3,9,90);
INSERT INTO ${case_db}.wm_part VALUES (1,'B1'),(2,'B1'),(3,'B2');
ANALYZE TABLE ${case_db}.wm_line;
ANALYZE TABLE ${case_db}.wm_part;

SET subquery_unnest_mode='apply';

-- Apply mode rewrites the scalar aggregate subquery to a window function.
-- @explain_contains=WINDOW [
-- @explain_not_contains=APPLY
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);

-- Correctness: apply-mode result rows (golden captures them; they must equal legacy).
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);

SET disable_optimizer_rules='ApplyToWindow';

-- With ApplyToWindow disabled, falls back to the outer JOIN form.
-- @explain_contains=OUTER
-- @explain_not_contains=WINDOW [
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);

SET disable_optimizer_rules='';
