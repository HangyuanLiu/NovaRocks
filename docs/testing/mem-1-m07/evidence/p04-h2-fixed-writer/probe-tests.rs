// Appended to the exact current SendFrameBuffer source. Core-only oracle;
// this is not the actual writer Vec or a whole-connection allocation proof.
#[cfg(test)]
mod ownership_probe {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicUsize};
    use std::sync::Barrier;

    thread_local! { static TRACK: Cell<bool> = const { Cell::new(false) }; }
    static CORE: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    static REQUESTED: AtomicUsize = AtomicUsize::new(0);
    struct Tracked;
    // SAFETY: Delegate unchanged allocator contracts to System. The explicit
    // TLS measurement records one constructor allocation; other metadata is
    // constructed outside that scope. Observations follow System.dealloc.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if TRACK.try_with(Cell::get).unwrap_or(false) && !pointer.is_null() {
                CALLS.fetch_add(1, Ordering::AcqRel);
                REQUESTED.fetch_add(layout.size(), Ordering::AcqRel);
                // Only the constructor allocates in successful measured calls.
                let _ = CORE.compare_exchange(
                    ptr::null_mut(),
                    pointer,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) };
            let _ = CORE.compare_exchange(
                pointer,
                ptr::null_mut(),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
    #[global_allocator]
    static ALLOCATOR: Tracked = Tracked;
    struct Scope;
    impl Drop for Scope {
        fn drop(&mut self) {
            TRACK.with(|v| v.set(false));
        }
    }
    fn measured<T>(f: impl FnOnce() -> T) -> T {
        CALLS.store(0, Ordering::Release);
        REQUESTED.store(0, Ordering::Release);
        TRACK.with(|v| v.set(true));
        let scope = Scope;
        let value = f();
        drop(scope);
        value
    }
    struct Original(Arc<AtomicUsize>);
    impl Drop for Original {
        fn drop(&mut self) {
            assert!(
                CORE.load(Ordering::Acquire).is_null(),
                "actual Core allocation must be freed before original funding exits"
            );
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn original() -> (Bytes, Arc<AtomicUsize>) {
        assert!(CORE.load(Ordering::Acquire).is_null());
        let exits = Arc::new(AtomicUsize::new(0));
        (
            Bytes::from_owner_with_exit_guard(Bytes::new(), Original(exits.clone())),
            exits,
        )
    }
    fn construct() -> (SendFrameBuffer, Arc<AtomicUsize>) {
        let (carrier, exits) = original();
        let result = measured(|| SendFrameBuffer::new(65536, 16384, carrier));
        let buffer = result.unwrap();
        assert_eq!(
            CALLS.load(Ordering::Acquire),
            1,
            "constructor must allocate only Core, not a writer Vec"
        );
        // std 1.92 ArcInner: two atomic counters followed by aligned Core.
        let arc = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<Core>())
            .unwrap()
            .0
            .pad_to_align();
        assert_eq!(REQUESTED.load(Ordering::Acquire), arc.size());
        assert!(
            REQUESTED.load(Ordering::Acquire)
                <= SendFrameBuffer::allocation_capacity_bound(65536, 16384).unwrap() - 65536
        );
        assert!(!CORE.load(Ordering::Acquire).is_null());
        (buffer, exits)
    }
    #[test]
    fn constructor_allocates_only_actual_core_and_frees_it_before_original_carrier() {
        let (buffer, exits) = construct();
        assert_eq!(buffer.capacity_bytes(), 65536);
        assert_eq!(buffer.max_payload_bytes(), 16384);
        println!("Actual Core requested={}B, fixed writer requested capacity=65536B, complete bound={}B (carrier excluded)", REQUESTED.load(Ordering::Acquire), SendFrameBuffer::allocation_capacity_bound(65536, 16384).unwrap());
        drop(buffer);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn clone_and_successful_bind_allocate_nothing_and_keep_original_grant() {
        let (buffer, exits) = construct();
        let (clone, lease) = measured(|| (buffer.clone(), buffer.bind()));
        assert_eq!(CALLS.load(Ordering::Acquire), 0);
        let lease = lease.unwrap();
        assert_eq!(lease.capacity_bytes(), 65536);
        assert_eq!(lease.max_payload_bytes(), 16384);
        drop(buffer);
        drop(clone);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(lease);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn binding_is_once_even_after_the_first_lease_exits() {
        let (buffer, exits) = construct();
        drop(buffer.bind().unwrap());
        assert_eq!(
            buffer.bind().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        drop(buffer);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn racing_bind_mints_exactly_one_lease() {
        let (buffer, exits) = construct();
        let barrier = Arc::new(Barrier::new(3));
        let mut joins = Vec::new();
        for _ in 0..2 {
            let buffer = buffer.clone();
            let barrier = barrier.clone();
            joins.push(std::thread::spawn(move || {
                barrier.wait();
                buffer.bind()
            }));
        }
        barrier.wait();
        let results: Vec<_> = joins.into_iter().map(|j| j.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        drop(buffer);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(results);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn external_original_carrier_alias_exits_after_core_allocation() {
        let (carrier, exits) = original();
        let alias = carrier.clone();
        let buffer = measured(|| SendFrameBuffer::new(65536, 16384, carrier)).unwrap();
        drop(buffer);
        assert!(CORE.load(Ordering::Acquire).is_null());
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(alias);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn geometry_and_checked_bound_reject_invalid_requests() {
        for (capacity, maximum) in [
            (0, 16384),
            (16392, 16384),
            (65536, 0),
            (65536, 16383),
            (16777224, 16777216),
            (usize::MAX, 16384),
        ] {
            assert_eq!(
                SendFrameBuffer::allocation_capacity_bound(capacity, maximum)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                SendFrameBuffer::new(capacity, maximum, Bytes::new())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        for maximum in [16384, 16777215] {
            assert!(
                SendFrameBuffer::allocation_capacity_bound(maximum + 9, maximum).unwrap()
                    > maximum + 9
            );
        }
    }
}
