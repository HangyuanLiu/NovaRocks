-- @tags=optimizer,distribution,cost
-- Large exact generate_series build is below broadcast_row_limit but exceeds
-- the broadcast fanout byte cap. It should choose PARTITIONED.
-- @explain_contains=HASH JOIN (PARTITIONED
EXPLAIN VERBOSE SELECT COUNT(*) AS cnt
FROM TABLE(generate_series(1, 120000000)) AS p(k)
INNER JOIN TABLE(generate_series(1, 12000000)) AS b(k)
    ON p.k = b.k;

-- Tiny build should remain BROADCAST.
-- @explain_contains=HASH JOIN (BROADCAST
EXPLAIN VERBOSE SELECT COUNT(*) AS cnt
FROM TABLE(generate_series(1, 6000000)) AS p(k)
INNER JOIN TABLE(generate_series(1, 10000)) AS b(k)
    ON p.k = b.k;
