-- @order_sensitive=true
-- @tags=complex,map
-- Test Objective:
-- Regression for the map higher-order functions map_apply / transform_keys /
-- transform_values. Their (key, value) lambda parameters must bind as lambda
-- slots (LambdaParamRef), not as freshly-minted scope columns. The latter
-- produced a phantom ColumnId for each parameter, which the ColumnId-binding
-- verifier rejected with "ColumnId(N) is not produced by child scope" in
-- projections, filters, and join predicates.
-- Test Flow:
-- 1. Apply each map higher-order function to a constant MAP.
-- 2. Assert the rewritten MAP output (key+value transforms).
-- 3. Exercise a lambda body in a nested (non-projection) scalar position.
-- Join-predicate coverage lives in the join suite (join_map_type).

-- query 1
-- map_apply: body is a (new_key, new_value) tuple.
SELECT map_apply((k, v) -> (k + 100, v * 2), MAP(1, 10, 2, 20)) AS applied;

-- query 2
-- transform_keys: rewrite keys, values pass through.
SELECT transform_keys((k, v) -> k + 100, MAP(1, 10, 2, 20)) AS keyed;

-- query 3
-- transform_values: rewrite values, keys pass through.
SELECT transform_values((k, v) -> v * 2, MAP(1, 10, 2, 20)) AS valued;

-- query 4
-- Lambda body in a nested scalar position (wrapped by map_size).
SELECT map_size(map_apply((k, v) -> (k, v + 1), MAP(1, 10, 2, 20))) AS sz;
