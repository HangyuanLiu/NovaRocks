-- query 1
-- @order_sensitive=true
SELECT bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2)));

-- query 2
-- @order_sensitive=true
SELECT bitmap_to_string(bitmap_xor(bitmap_from_string('1,2,3'), bitmap_from_string('2,3,4')));

-- query 3
-- @order_sensitive=true
SELECT bitmap_to_string(bitmap_andnot(bitmap_from_string('1,2,3'), bitmap_from_string('2')));

-- query 4
-- @order_sensitive=true
SELECT bitmap_to_string(bitmap_intersect(bitmap_from_string('1,2,3'), bitmap_from_string('2,3,4')));

-- query 5
SELECT bitmap_contains(to_bitmap(1), 1), bitmap_contains(to_bitmap(1), 2);

-- query 6
-- NULL propagation
SELECT bitmap_or(NULL, to_bitmap(1)) IS NULL;

-- query 7
-- NULL propagation for pre-existing binary ops
SELECT bitmap_and(NULL, to_bitmap(1)) IS NULL;

-- query 8
SELECT bitmap_has_any(NULL, to_bitmap(1)) IS NULL;

-- query 9
-- NULL propagation for unary ops
SELECT bitmap_count(NULL) IS NULL;

-- query 10
SELECT bitmap_to_string(NULL) IS NULL;

-- query 11
SELECT bitmap_to_binary(NULL) IS NULL;

-- query 12
SELECT bitmap_from_binary(NULL) IS NULL;

-- query 13
SELECT bitmap_to_base64(NULL) IS NULL;

-- query 14
SELECT bitmap_from_string(NULL) IS NULL;

-- query 15
-- NULL propagation for subset ops
SELECT sub_bitmap(NULL, 0, 1) IS NULL;

-- query 16
SELECT bitmap_subset_limit(NULL, 0, 1) IS NULL;

-- query 17
SELECT bitmap_subset_in_range(NULL, 0, 10) IS NULL;

-- query 18
-- NULL propagation for min/max
SELECT bitmap_min(NULL) IS NULL;

-- query 19
SELECT bitmap_max(NULL) IS NULL;
