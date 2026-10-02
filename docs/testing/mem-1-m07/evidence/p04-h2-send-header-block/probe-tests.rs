// Exact-source block/pool ownership probe; no HeaderMap or HPACK codec claim.
#[cfg(test)]
mod probe {
    use super::*;
    use bytes::Bytes;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    thread_local! { static TRACK: Cell<bool> = const { Cell::new(false) }; }
    static POINTERS: [AtomicPtr<u8>; 32] = [const { AtomicPtr::new(ptr::null_mut()) }; 32];
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static TOTAL: AtomicUsize = AtomicUsize::new(0);
    static ALLOCS: AtomicUsize = AtomicUsize::new(0);
    struct Ledger;
    // SAFETY: The original pointer and layout are forwarded to System; fixed
    // atomics observe only allocations in the explicit tracking scope.
    unsafe impl GlobalAlloc for Ledger {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: Forward the allocator contract unchanged.
            let allocation = unsafe { System.alloc(layout) };
            if TRACK.try_with(Cell::get).unwrap_or(false) && !allocation.is_null() {
                TOTAL.fetch_add(layout.size(), Ordering::AcqRel);
                ALLOCS.fetch_add(1, Ordering::AcqRel);
                for slot in &POINTERS {
                    if slot
                        .compare_exchange(
                            ptr::null_mut(),
                            allocation,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        LIVE.fetch_add(1, Ordering::AcqRel);
                        break;
                    }
                }
            }
            allocation
        }
        unsafe fn dealloc(&self, allocation: *mut u8, layout: Layout) {
            // SAFETY: Record physical exit after the actual System deallocation.
            unsafe { System.dealloc(allocation, layout) };
            for slot in &POINTERS {
                if slot
                    .compare_exchange(
                        allocation,
                        ptr::null_mut(),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    LIVE.fetch_sub(1, Ordering::AcqRel);
                    break;
                }
            }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Ledger = Ledger;
    struct Tracking;
    impl Drop for Tracking {
        fn drop(&mut self) {
            TRACK.with(|track| track.set(false));
        }
    }
    fn measured<T>(f: impl FnOnce() -> T) -> T {
        TRACK.with(|track| track.set(true));
        let tracking = Tracking;
        let result = f();
        drop(tracking);
        result
    }
    struct Original(Arc<AtomicUsize>);
    impl Drop for Original {
        fn drop(&mut self) {
            assert_eq!(
                LIVE.load(Ordering::Acquire),
                0,
                "fixed backing must physically exit before its original carrier"
            );
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn new(max: usize) -> (SendHeaderBlockPool, Arc<AtomicUsize>) {
        assert_eq!(LIVE.load(Ordering::Acquire), 0);
        TOTAL.store(0, Ordering::Release);
        ALLOCS.store(0, Ordering::Release);
        let exited = Arc::new(AtomicUsize::new(0));
        // Carrier metadata is deliberately outside the tracked grant scope.
        let carrier = Bytes::from_owner_with_exit_guard(Bytes::new(), Original(exited.clone()));
        let pool = measured(|| SendHeaderBlockPool::new(max, carrier).unwrap());
        assert_eq!(
            ALLOCS.load(Ordering::Acquire),
            3,
            "block Vec, slot Vec and Core Arc only"
        );
        assert_eq!(LIVE.load(Ordering::Acquire), 3);
        eprintln!(
            "max_header_list_size={max} block_capacity={} constructor_requested={} allocation_bound={}",
            pool.buffer_capacity_bytes(),
            TOTAL.load(Ordering::Acquire),
            SendHeaderBlockPool::allocation_capacity_bound(max).unwrap()
        );
        (pool, exited)
    }

    #[test]
    fn checked_geometry_and_exact_minimum_capacity() {
        assert!(SendHeaderBlockPool::allocation_capacity_bound(0).is_err());
        assert!(SendHeaderBlockPool::allocation_capacity_bound(usize::MAX).is_err());
        if usize::BITS > 32 {
            assert!(SendHeaderBlockPool::allocation_capacity_bound(u32::MAX as usize).is_ok());
        } else {
            assert!(SendHeaderBlockPool::allocation_capacity_bound(u32::MAX as usize).is_err());
        }
        let (pool, exited) = new(1);
        assert_eq!(pool.max_header_list_size(), 1);
        assert_eq!(pool.buffer_capacity_bytes(), 24);
        assert!(
            TOTAL.load(Ordering::Acquire)
                <= SendHeaderBlockPool::allocation_capacity_bound(1).unwrap()
        );
        drop(pool);
        assert_eq!(exited.load(Ordering::Acquire), 1);
    }

    #[test]
    fn exact_capacity_fill_allocates_only_wrapper_and_funding_outlives_alias() {
        let (pool, exited) = new(16384);
        let capacity = pool.buffer_capacity_bytes();
        let bound = pool.bind().unwrap();
        let before = ALLOCS.load(Ordering::Acquire);
        let bytes = measured(|| bound.try_encode(|buffer| buffer.resize(capacity, 0xa5))).unwrap();
        assert_eq!(
            ALLOCS.load(Ordering::Acquire),
            before + 1,
            "filling the preallocated Vec cannot grow it"
        );
        assert_eq!(bytes.len(), capacity);
        assert_eq!(bytes[0], 0xa5);
        assert_eq!(bytes[capacity - 1], 0xa5);
        assert!(
            TOTAL.load(Ordering::Acquire)
                <= SendHeaderBlockPool::allocation_capacity_bound(16384).unwrap()
        );
        eprintln!(
            "exact_capacity_fill_total_requested={} live_backings={}",
            TOTAL.load(Ordering::Acquire),
            LIVE.load(Ordering::Acquire)
        );
        let alias = bytes.slice(1..);
        drop(bytes);
        drop(bound);
        drop(pool);
        assert_eq!(exited.load(Ordering::Acquire), 0);
        assert_eq!(LIVE.load(Ordering::Acquire), 4);
        drop(alias);
        assert_eq!(exited.load(Ordering::Acquire), 1);
    }

    #[test]
    fn clone_and_successful_bind_add_no_allocation_and_binding_is_once() {
        let (pool, exited) = new(512);
        let before = ALLOCS.load(Ordering::Acquire);
        let cloned = measured(|| pool.clone());
        let bound = measured(|| cloned.bind().unwrap());
        assert_eq!(ALLOCS.load(Ordering::Acquire), before);
        assert!(pool.bind().is_err());
        drop(pool);
        drop(cloned);
        assert_eq!(exited.load(Ordering::Acquire), 0);
        drop(bound);
        assert_eq!(exited.load(Ordering::Acquire), 1);
    }

    #[test]
    fn full_slot_refuses_before_callback_and_reuses_after_final_alias() {
        let (pool, exited) = new(512);
        let bound = pool.bind().unwrap();
        let first =
            measured(|| bound.try_encode(|buffer| buffer.extend_from_slice(b"first"))).unwrap();
        let first_pointer = first.as_ptr();
        let alias = first.clone();
        drop(first);
        let before = ALLOCS.load(Ordering::Acquire);
        let mut called = false;
        assert!(measured(|| bound.try_encode(|_| called = true)).is_none());
        assert!(!called);
        assert_eq!(ALLOCS.load(Ordering::Acquire), before);
        drop(alias);
        assert_eq!(
            LIVE.load(Ordering::Acquire),
            3,
            "the previous wrapper physically exited before reuse"
        );
        let second =
            measured(|| bound.try_encode(|buffer| buffer.extend_from_slice(b"next"))).unwrap();
        assert_eq!(second.as_ref(), b"next");
        assert_eq!(
            second.as_ptr(),
            first_pointer,
            "reuse must return the same complete Vec backing"
        );
        assert_eq!(ALLOCS.load(Ordering::Acquire), before + 1);
        drop(second);
        drop(bound);
        drop(pool);
        assert_eq!(exited.load(Ordering::Acquire), 1);
    }

    #[test]
    fn encoding_panic_returns_the_actual_vec_and_slot() {
        let (pool, exited) = new(512);
        let bound = pool.bind().unwrap();
        // Panic runtime/diagnostic allocations are outside the fixed backing
        // ledger; the constructor's actual physical allocations remain tracked.
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bound.try_encode(|buffer| {
                buffer.extend_from_slice(b"unfinished");
                panic!("injected encoding failure");
            });
        }));
        assert!(panic.is_err());
        assert_eq!(LIVE.load(Ordering::Acquire), 3);
        let bytes =
            measured(|| bound.try_encode(|buffer| buffer.extend_from_slice(b"complete"))).unwrap();
        assert_eq!(
            bytes.as_ref(),
            b"complete",
            "unpublished partial content must be cleared"
        );
        drop(bytes);
        drop(bound);
        drop(pool);
        assert_eq!(exited.load(Ordering::Acquire), 1);
    }

    #[test]
    fn racing_bind_has_exactly_one_success() {
        let (pool, exited) = new(512);
        let gate = Arc::new(Barrier::new(3));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let pool = pool.clone();
            let gate = gate.clone();
            threads.push(std::thread::spawn(move || {
                gate.wait();
                pool.bind().is_ok()
            }));
        }
        gate.wait();
        assert_eq!(
            threads
                .into_iter()
                .map(|thread| usize::from(thread.join().unwrap()))
                .sum::<usize>(),
            1
        );
        drop(pool);
        assert_eq!(exited.load(Ordering::Acquire), 1);
    }
}
