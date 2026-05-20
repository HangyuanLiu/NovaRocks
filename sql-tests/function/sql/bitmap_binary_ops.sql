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
