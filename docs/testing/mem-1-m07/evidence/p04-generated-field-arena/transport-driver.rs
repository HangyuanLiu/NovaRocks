#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::header::{HeaderFieldAllocationPool, HeaderFieldFillError};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    thread_local! {
        static TRACK: Cell<bool> = const { Cell::new(false) };
        static CALLS: Cell<usize> = const { Cell::new(0) };
        static BYTES: Cell<usize> = const { Cell::new(0) };
    }
    struct Allocator;
    unsafe impl GlobalAlloc for Allocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if TRACK.try_with(Cell::get).unwrap_or(false) {
                CALLS.with(|v| v.set(v.get() + 1));
                BYTES.with(|v| v.set(v.get() + layout.size()));
            }
            System.alloc(layout)
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            System.dealloc(pointer, layout);
        }
        unsafe fn realloc(&self, pointer: *mut u8, old: Layout, size: usize) -> *mut u8 {
            if TRACK.try_with(Cell::get).unwrap_or(false) {
                CALLS.with(|v| v.set(v.get() + 1));
                BYTES.with(|v| v.set(v.get() + size));
            }
            System.realloc(pointer, old, size)
        }
    }
    #[global_allocator]
    static ALLOCATOR: Allocator = Allocator;
    fn measured<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
        CALLS.with(|v| v.set(0));
        BYTES.with(|v| v.set(0));
        TRACK.with(|v| v.set(true));
        let value = f();
        TRACK.with(|v| v.set(false));
        (value, CALLS.with(Cell::get), BYTES.with(Cell::get))
    }
    fn pool(capacity: usize, positions: usize, max: usize) -> HeaderFieldAllocationPool {
        HeaderFieldAllocationPool::new(capacity, positions, max, Bytes::new()).unwrap()
    }
    fn wrapper_size() -> usize {
        HeaderFieldAllocationPool::allocation_capacity_bound(192, 3, 29).unwrap()
            - HeaderFieldAllocationPool::allocation_capacity_bound(192, 2, 29).unwrap()
    }
    const EPOCH: &[u8] = b"Thu, 01 Jan 1970 00:00:00 GMT";

    #[test]
    fn cold_date_cache_and_generated_field_need_only_one_original_wrapper() {
        let fields = pool(192, 3, 29);
        let (date, allocations, bytes) = measured(|| hyper::__m07_date(&fields).unwrap());
        assert_eq!(allocations, 1, "no ordinary Date payload/cache promotion");
        assert_eq!(bytes, wrapper_size());
        assert_eq!(date.len(), 29);
        assert!(date.as_bytes().ends_with(b" GMT"));
        let (alias, allocations, bytes) = measured(|| date.clone());
        assert_eq!((allocations, bytes), (0, 0));
        assert_eq!(date.as_bytes().as_ptr(), alias.as_bytes().as_ptr());
        drop(date);
        assert_eq!(fields.available_positions(), 2);
        drop(alias);
        assert_eq!(fields.available_positions(), 3);
    }

    #[test]
    fn original_cache_check_and_default_lazy_payload_preserve_clock_rollback_bytes() {
        hyper::__m07_seed_date();
        let fields = pool(192, 3, 29);
        let (date, calls, bytes) = measured(|| hyper::__m07_date(&fields).unwrap());
        assert_eq!(date.as_bytes(), EPOCH);
        assert_eq!((calls, bytes), (1, wrapper_size()));
        let (ordinary, calls, bytes) = measured(hyper::__m07_ordinary_date);
        assert_eq!(ordinary.as_bytes(), EPOCH);
        assert_eq!(calls, 2, "ordinary 29B payload and first Bytes promotion");
        assert!(
            bytes > 29,
            "ordinary payload and separate promotion metadata"
        );
        let (next, calls, bytes) = measured(hyper::__m07_ordinary_date);
        assert_eq!(next.as_bytes(), EPOCH);
        assert_eq!((calls, bytes), (0, 0));
        let generated = hyper::__m07_date(&fields).unwrap();
        assert_eq!(generated.as_bytes(), EPOCH);
        assert_ne!(generated.as_bytes().as_ptr(), ordinary.as_bytes().as_ptr());
    }

    #[test]
    fn date_limit_position_and_real_extent_shortages_have_no_fallback_allocation() {
        let small = pool(64, 1, 28);
        let (result, calls, bytes) = measured(|| hyper::__m07_date(&small));
        assert!(matches!(result, Err(HeaderFieldFillError::TooLarge)));
        assert_eq!((calls, bytes), (0, 0));
        let fields = pool(128, 2, 128);
        let raw = fields.try_fill(128, |_| Ok::<_, ()>(())).unwrap();
        assert_eq!(fields.available_positions(), 1);
        let (result, calls, bytes) = measured(|| hyper::__m07_date(&fields));
        assert!(matches!(result, Err(HeaderFieldFillError::Exhausted)));
        assert_eq!((calls, bytes), (0, 0));
        drop(raw);
        let first = hyper::__m07_date(&fields).unwrap();
        let second = hyper::__m07_date(&fields).unwrap();
        let (result, calls, bytes) = measured(|| hyper::__m07_date(&fields));
        assert!(matches!(result, Err(HeaderFieldFillError::Exhausted)));
        assert_eq!((calls, bytes), (0, 0));
        drop(first);
        assert!(hyper::__m07_date(&fields).is_ok());
        drop(second);
    }

    struct Exit(Arc<AtomicUsize>);
    impl Drop for Exit {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn only_generated_date_alias_keeps_exact_original_owner_after_pool_exit() {
        let exit = Arc::new(AtomicUsize::new(0));
        let owner = Bytes::from_owner_with_exit_guard(Bytes::new(), Exit(exit.clone()));
        let fields = HeaderFieldAllocationPool::new(64, 1, 29, owner).unwrap();
        let value = hyper::__m07_date(&fields).unwrap();
        let alias = value.clone();
        drop(value);
        drop(fields);
        assert_eq!(exit.load(Ordering::SeqCst), 0);
        assert_eq!(alias.len(), 29);
        drop(alias);
        assert_eq!(exit.load(Ordering::SeqCst), 1);
    }
}
