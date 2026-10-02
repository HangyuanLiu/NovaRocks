// Current shared DATA/GOAWAY pool source, not a protocol or deadline oracle.
#[cfg(test)]
mod ownership_probe {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicUsize};
    use std::sync::{Arc, Barrier};
    use std::task::{Wake, Waker};

    thread_local! { static TRACK: Cell<u8> = const { Cell::new(0) }; }
    static POINTERS: [AtomicPtr<u8>; 32] = [const { AtomicPtr::new(ptr::null_mut()) }; 32];
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static REQUESTED_TOTAL: AtomicUsize = AtomicUsize::new(0);
    static WRAPPER: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
    static WRAPPER_FREED: AtomicBool = AtomicBool::new(false);
    struct Tracked;
    // SAFETY: Calls delegate to System with the unchanged pointer/Layout.
    // Fixed atomics only record allocations made in the explicit test scope.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: Forward the allocator contract unchanged.
            let ptr = unsafe { System.alloc(layout) };
            let mode = TRACK.try_with(Cell::get).unwrap_or(0);
            if mode > 0 && !ptr.is_null() {
                REQUESTED_TOTAL.fetch_add(layout.size(), Ordering::AcqRel);
                for entry in &POINTERS {
                    if entry
                        .compare_exchange(ptr::null_mut(), ptr, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        LIVE.fetch_add(1, Ordering::AcqRel);
                        if mode == 2 {
                            WRAPPER.store(ptr, Ordering::Release);
                        }
                        break;
                    }
                }
            }
            ptr
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: Forward the allocator contract unchanged. Observations
            // follow actual System.dealloc, not owner Drop or logical release.
            unsafe { System.dealloc(ptr, layout) };
            if WRAPPER.load(Ordering::Acquire) == ptr {
                WRAPPER_FREED.store(true, Ordering::Release);
            }
            for entry in &POINTERS {
                if entry
                    .compare_exchange(ptr, ptr::null_mut(), Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    LIVE.fetch_sub(1, Ordering::AcqRel);
                    break;
                }
            }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Tracked = Tracked;

    struct TrackGuard;
    impl Drop for TrackGuard {
        fn drop(&mut self) {
            TRACK.with(|v| v.set(0));
        }
    }
    fn tracked<T>(mode: u8, f: impl FnOnce() -> T) -> T {
        TRACK.with(|v| v.set(mode));
        let guard = TrackGuard;
        let result = f();
        drop(guard);
        result
    }
    struct Original(Arc<AtomicUsize>);
    impl Drop for Original {
        fn drop(&mut self) {
            assert_eq!(
                LIVE.load(Ordering::Acquire),
                0,
                "pool allocations remain when original owner exits"
            );
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn pool(slots: usize) -> (ReceiveBufferPool, Arc<AtomicUsize>) {
        assert_eq!(LIVE.load(Ordering::Acquire), 0);
        REQUESTED_TOTAL.store(0, Ordering::Release);
        WRAPPER.store(ptr::null_mut(), Ordering::Release);
        WRAPPER_FREED.store(false, Ordering::Release);
        let exit = Arc::new(AtomicUsize::new(0));
        let carrier = Bytes::from_owner_with_exit_guard(Bytes::new(), Original(exit.clone()));
        let pool = tracked(1, || ReceiveBufferPool::new(slots, 16384, carrier).unwrap());
        assert!(
            LIVE.load(Ordering::Acquire) > 0,
            "actual pool allocations must be observed"
        );
        let bound = pool.bind(16384).unwrap();
        drop(bound);
        (pool, exit)
    }
    fn copy(pool: &ReceiveBufferPool, data: &[u8]) -> Bytes {
        tracked(2, || pool.copy_data(data))
    }

    #[test]
    fn allocation_bound_covers_pool_and_all_checked_out_wrappers() {
        let (pool, exit) = pool(2);
        let first = copy(&pool, &[1]);
        let second = copy(&pool, &[]);
        assert_eq!(pool.available_buffers(), 0);
        assert!(
            REQUESTED_TOTAL.load(Ordering::Acquire)
                <= ReceiveBufferPool::allocation_capacity_bound(2, 16384).unwrap()
        );
        drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 0);
        drop(first);
        assert_eq!(exit.load(Ordering::Acquire), 0);
        drop(second);
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn last_alias_exits_all_allocations_before_original_owner() {
        let (pool, exit) = pool(2);
        let data = copy(&pool, &[1, 2, 3]);
        let alias = data.slice(1..);
        drop(data);
        drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 0);
        assert_eq!(alias.as_ref(), &[2, 3]);
        drop(alias);
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    struct PhysicalWake(Arc<AtomicUsize>);
    impl Wake for PhysicalWake {
        fn wake(self: Arc<Self>) {
            assert!(
                WRAPPER_FREED.load(Ordering::Acquire),
                "wake preceded physical Bytes wrapper exit"
            );
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[test]
    fn actual_wrapper_deallocation_precedes_return_and_wake() {
        let (pool, exit) = pool(1);
        let data = copy(&pool, &[9]);
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(PhysicalWake(wakes.clone())));
        let cx = Context::from_waker(&waker);
        assert!(pool.poll_ready(&cx).is_pending());
        drop(data);
        assert_eq!(wakes.load(Ordering::Acquire), 1);
        assert_eq!(pool.available_buffers(), 1);
        let data = copy(&pool, &[8]);
        assert_eq!(data.as_ref(), &[8]);
        drop(data);
        drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn empty_owner_clone_retains_backing_but_empty_slice_detaches() {
        let (pool, exit) = pool(1);
        let data = copy(&pool, &[]);
        let empty_slice = data.slice(..);
        let strong = data.clone();
        drop(data);
        drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 0);
        drop(strong);
        assert_eq!(exit.load(Ordering::Acquire), 1);
        assert!(empty_slice.is_empty());
        drop(empty_slice);
    }

    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("injected wake failure");
        }
    }
    #[test]
    fn wake_panic_restores_position_and_completes_original_owner_exit() {
        let (pool, exit) = pool(1);
        let data = copy(&pool, &[9]);
        let waker = Waker::from(Arc::new(PanicWake));
        let cx = Context::from_waker(&waker);
        assert!(pool.poll_ready(&cx).is_pending());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(data))).is_err());
        assert_eq!(pool.available_buffers(), 1);
        drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn concurrent_last_aliases_keep_original_owner_until_physical_exit() {
        let (pool, exit) = pool(1);
        let data = copy(&pool, &[9]);
        let a = data.clone();
        let b = data.clone();
        drop(data);
        drop(pool);
        let barrier = Arc::new(Barrier::new(3));
        let first = barrier.clone();
        let second = barrier.clone();
        let a = std::thread::spawn(move || {
            first.wait();
            drop(a);
        });
        let b = std::thread::spawn(move || {
            second.wait();
            drop(b);
        });
        assert_eq!(exit.load(Ordering::Acquire), 0);
        barrier.wait();
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn full_diagnostic_pool_refuses_without_copy_or_new_wrapper_allocation() {
        let (pool, exit) = pool(1);
        let first = copy(&pool, b"first diagnostic");
        assert_eq!(pool.available_buffers(), 0);
        let before = REQUESTED_TOTAL.load(Ordering::Acquire);
        let live = LIVE.load(Ordering::Acquire);
        let rejected = tracked(2, || pool.try_copy_payload(b"refused diagnostic"));
        assert!(rejected.is_none());
        assert_eq!(
            REQUESTED_TOTAL.load(Ordering::Acquire),
            before,
            "full diagnostic admission must precede copy/wrapper allocation"
        );
        assert_eq!(LIVE.load(Ordering::Acquire), live);
        assert_eq!(first.as_ref(), b"first diagnostic");
        drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 0);
        drop(first);
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn final_diagnostic_alias_exit_reuses_the_same_complete_backing() {
        let (pool, exit) = pool(1);
        let initial_backings = LIVE.load(Ordering::Acquire);
        let first = copy(&pool, b"first");
        let alias = first.clone();
        assert_eq!(LIVE.load(Ordering::Acquire), initial_backings + 1);
        drop(first);
        assert_eq!(pool.available_buffers(), 0);
        let before = REQUESTED_TOTAL.load(Ordering::Acquire);
        assert!(tracked(2, || pool.try_copy_payload(b"next")).is_none());
        assert_eq!(REQUESTED_TOTAL.load(Ordering::Acquire), before);
        drop(alias);
        assert_eq!(pool.available_buffers(), 1);
        assert_eq!(
            LIVE.load(Ordering::Acquire),
            initial_backings,
            "old physical wrapper must exit before slot reuse"
        );
        let next = tracked(2, || pool.try_copy_payload(b"next")).unwrap();
        assert_eq!(next.as_ref(), b"next");
        assert_eq!(pool.available_buffers(), 0);
        assert_eq!(
            LIVE.load(Ordering::Acquire),
            initial_backings + 1,
            "reuse allocates only one new wrapper, never a new block"
        );
        drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 0);
        drop(next);
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }

    #[test]
    fn free_slot_with_zero_count_refuses_without_checkout_or_underflow() {
        let (pool, exit) = pool(1);
        // Deliberately construct the small publication window in which the
        // exit guard has published FREE but has not incremented available.
        // This tests a real source-state branch, not concurrent-race coverage.
        assert_eq!(pool.core().slots[0].state.load(Ordering::Acquire), FREE);
        pool.core().available.store(0, Ordering::Release);
        let before = REQUESTED_TOTAL.load(Ordering::Acquire);
        assert!(tracked(2, || pool.try_copy_payload(b"diagnostic")).is_none());
        assert_eq!(REQUESTED_TOTAL.load(Ordering::Acquire), before);
        assert_eq!(pool.available_buffers(), 0);
        assert_eq!(
            pool.core().slots[0].state.load(Ordering::Acquire),
            FREE,
            "the zero-count guard must precede the checkout CAS"
        );
        pool.core().available.store(1, Ordering::Release);
        let bytes = tracked(2, || pool.try_copy_payload(b"diagnostic")).unwrap();
        assert_eq!(bytes.as_ref(), b"diagnostic");
        drop(bytes);
        assert_eq!(pool.available_buffers(), 1);
        drop(pool);
        assert_eq!(exit.load(Ordering::Acquire), 1);
    }
}
