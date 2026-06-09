-- @tags=optimizer,g3
-- Test Objective:
-- 1. Lock in the G3 contract: a BROADCAST Inner Join preserves its left
--    child's distribution through passthrough join output derivation.
--    G4 source-aware semantics then require a ShuffleAgg exchange before
--    a downstream Window keyed on a narrower analytic partition.
-- 2. Regression guard for "Broadcast preserves left output" + the
--    G4 rule that ShuffleJoin output does not satisfy narrower ShuffleAgg.
DROP TABLE IF EXISTS ${case_db}.g3_bj_left;
DROP TABLE IF EXISTS ${case_db}.g3_bj_right;
DROP TABLE IF EXISTS ${case_db}.g3_bj_small;
CREATE TABLE ${case_db}.g3_bj_left  (k INT, v INT);
CREATE TABLE ${case_db}.g3_bj_right (k INT, w INT);
CREATE TABLE ${case_db}.g3_bj_small (s INT, x INT);
INSERT INTO ${case_db}.g3_bj_left  VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.g3_bj_right VALUES (1, 100), (2, 200);
INSERT INTO ${case_db}.g3_bj_small VALUES (1, 1000);
ANALYZE TABLE ${case_db}.g3_bj_left;
ANALYZE TABLE ${case_db}.g3_bj_right;
ANALYZE TABLE ${case_db}.g3_bj_small;
-- Inner BROADCAST(small) join above an INNER SHUFFLE join keyed on a.k.
-- Window keyed on a.k must add a ShuffleAgg HASH EXCHANGE above the
-- Broadcast output instead of reusing the ShuffleJoin source directly.
EXPLAIN VERBOSE
SELECT a.k, a.v, b.w, ROW_NUMBER() OVER (PARTITION BY a.k ORDER BY a.v) AS rn
FROM ${case_db}.g3_bj_left  a
INNER JOIN ${case_db}.g3_bj_right b ON a.k = b.k
INNER JOIN ${case_db}.g3_bj_small s ON a.k = s.s;
