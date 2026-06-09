-- OQ-4: grouped non-DISTINCT aggregate over a small iceberg table. With real
-- stats the optimizer picks a single-phase aggregate; the two-phase
-- (LOCAL/ShuffleAgg/GLOBAL) split triggers at scale and is covered by the
-- ssb/tpc-* benchmark suites. Plan-shape golden.
CREATE TABLE ${case_db}.t_split_agg_grouped (k INT, v INT);
INSERT INTO ${case_db}.t_split_agg_grouped VALUES
    (1, 10), (1, 20), (1, 30),
    (2, 5),  (2, 15), (2, 25),
    (3, 7),  (3, 11), (3, 13),
    (4, 1),  (4, 2),  (4, 3);
ANALYZE TABLE ${case_db}.t_split_agg_grouped;
EXPLAIN VERBOSE
SELECT k, SUM(v) AS s
FROM ${case_db}.t_split_agg_grouped
GROUP BY k
ORDER BY k;
