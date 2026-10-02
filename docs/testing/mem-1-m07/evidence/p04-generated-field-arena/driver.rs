#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::header::{HeaderFieldAllocationPool, HeaderFieldFillError, HeaderValue};

    fn pool(capacity: usize, positions: usize, max: usize) -> HeaderFieldAllocationPool {
        HeaderFieldAllocationPool::new(capacity, positions, max, Bytes::new()).unwrap()
    }

    #[test]
    fn exact_decimal_lengths_use_original_extents_until_last_alias() {
        let fields = pool(128, 2, 20);
        for (num, expected) in [
            (0, "0"),
            (9, "9"),
            (10, "10"),
            (u64::MAX, "18446744073709551615"),
        ] {
            let value = HeaderValue::try_from_u64_with_pool(num, &fields).unwrap();
            assert_eq!(value.as_bytes(), expected.as_bytes());
            assert_eq!(fields.available_positions(), 1);
            let alias = value.clone();
            assert_eq!(alias.as_bytes().as_ptr(), value.as_bytes().as_ptr());
            drop(value);
            assert_eq!(fields.available_positions(), 1);
            let sibling = HeaderValue::try_from_u64_with_pool(7, &fields).unwrap();
            assert!(matches!(
                HeaderValue::try_from_u64_with_pool(8, &fields),
                Err(HeaderFieldFillError::Exhausted)
            ));
            drop(sibling);
            drop(alias);
            assert_eq!(fields.available_positions(), 2);
        }
    }

    #[test]
    fn actual_digits_fit_limit_without_maximum_width_overestimate_or_fallback() {
        let fields = pool(64, 1, 1);
        let value = HeaderValue::try_from_u64_with_pool(0, &fields).unwrap();
        assert_eq!(value, "0");
        assert!(matches!(
            HeaderValue::try_from_u64_with_pool(10, &fields),
            Err(HeaderFieldFillError::TooLarge)
        ));
        assert!(matches!(
            HeaderValue::try_from_u64_with_pool(1, &fields),
            Err(HeaderFieldFillError::Exhausted)
        ));
        drop(value);
        assert_eq!(fields.available_positions(), 1);
        let alias = HeaderValue::try_from_u64_with_pool(9, &fields).unwrap();
        let clone = alias.clone();
        drop(fields);
        drop(alias);
        assert_eq!(clone, "9");
    }

    #[test]
    fn default_integer_constructor_remains_valid_and_independently_owned() {
        for (num, expected) in [(0, "0"), (10, "10"), (u64::MAX, "18446744073709551615")] {
            let value = HeaderValue::from(num);
            let alias = value.clone();
            assert_eq!(value.as_bytes(), expected.as_bytes());
            drop(value);
            assert_eq!(alias.as_bytes(), expected.as_bytes());
        }
    }
}
