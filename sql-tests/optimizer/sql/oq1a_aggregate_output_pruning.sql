-- name: oq1a_aggregate_output_pruning
DROP TABLE IF EXISTS oq1a_t;
CREATE TABLE oq1a_t (
    k INT,
    a BIGINT,
    b BIGINT
);
INSERT INTO oq1a_t VALUES
    (1, 10, 100),
    (1, 20, 200),
    (2, 30, 300);

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE
-- @explain_contains=sum
-- @explain_not_contains=count(b)
EXPLAIN VERBOSE SELECT sum(s) AS s
FROM (
    SELECT k, sum(a) AS s, count(b) AS unused_count
    FROM oq1a_t
    GROUP BY k
) q;

SELECT sum(s) AS s
FROM (
    SELECT k, sum(a) AS s, count(b) AS unused_count
    FROM oq1a_t
    GROUP BY k
) q;

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE
-- @explain_contains=count
EXPLAIN VERBOSE SELECT count(*) AS c
FROM oq1a_t;

SELECT count(*) AS c
FROM oq1a_t;

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE
-- @explain_contains=sum
EXPLAIN VERBOSE SELECT sum_a
FROM (
    SELECT k, sum(a) AS sum_a
    FROM oq1a_t
    GROUP BY k
    HAVING sum(a) > 0
) q
ORDER BY sum_a;

SELECT sum_a
FROM (
    SELECT k, sum(a) AS sum_a
    FROM oq1a_t
    GROUP BY k
    HAVING sum(a) > 0
) q
ORDER BY sum_a;
