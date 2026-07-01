-- @order_sensitive=true
-- @tags=set_op,union,union_all,null,chain
-- Test Objective:
-- 1. Validate mixed UNION DISTINCT / UNION ALL chaining after optimizer normalization.
-- 2. Preserve SQL left-associative semantics: the final UNION ALL keeps duplicates
--    introduced after the distinct stage.
SELECT x
FROM (
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(1 AS BIGINT)
        UNION ALL
        SELECT CAST(2 AS BIGINT)
    )
    UNION
    (
        SELECT CAST(2 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(3 AS BIGINT)
        UNION ALL
        SELECT CAST(NULL AS BIGINT)
    )
    UNION ALL
    (
        SELECT CAST(NULL AS BIGINT) AS x
        UNION ALL
        SELECT CAST(4 AS BIGINT)
    )
) t
ORDER BY x IS NULL, x;
