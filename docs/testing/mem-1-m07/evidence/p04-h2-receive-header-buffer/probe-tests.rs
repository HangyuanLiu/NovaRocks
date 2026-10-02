// Appended to the unmodified current receive_header.rs in an isolated crate.
// These probes measure Rust-requested encoded Vec/Core allocations. Frame copies,
// ownership-carrier metadata, transport/task metadata, libc/TLS and RSS are
// separate. ExitCredit below is a physical-drop oracle, not a second wallet.
#[cfg(test)]
mod header_input_probes {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize};
    use std::sync::Barrier;

    const RAW: usize = 1;
    const FRAME: usize = 2;
    const CARRIER: usize = 3;
    const MAX_RECORDS: usize = 64;
    thread_local! { static MODE: Cell<usize> = const { Cell::new(0) }; }
    static POINTERS: [AtomicPtr<u8>; MAX_RECORDS] =
        [const { AtomicPtr::new(ptr::null_mut()) }; MAX_RECORDS];
    static ROLES: [AtomicU8; MAX_RECORDS] = [const { AtomicU8::new(0) }; MAX_RECORDS];
    static LIVE: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
    static REQUESTED: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
    static ALLOCATIONS: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
    static MAX_REQUEST: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
    static OVERFLOW: AtomicBool = AtomicBool::new(false);
    struct Tracked;
    // SAFETY: All operations forward the unchanged allocator contract to System.
    // Fixed atomic records observe only allocations in the explicit TLS scope;
    // deallocation is recorded after the real System.dealloc has returned.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: Forward the requested allocation unchanged.
            let allocation = unsafe { System.alloc(layout) };
            let mode = MODE.try_with(Cell::get).unwrap_or(0);
            if mode != 0 && !allocation.is_null() {
                REQUESTED[mode].fetch_add(layout.size(), Ordering::AcqRel);
                ALLOCATIONS[mode].fetch_add(1, Ordering::AcqRel);
                MAX_REQUEST[mode].fetch_max(layout.size(), Ordering::AcqRel);
                let mut recorded = false;
                for (index, pointer) in POINTERS.iter().enumerate() {
                    if pointer
                        .compare_exchange(
                            ptr::null_mut(),
                            allocation,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        ROLES[index].store(mode as u8, Ordering::Release);
                        LIVE[mode].fetch_add(1, Ordering::AcqRel);
                        recorded = true;
                        break;
                    }
                }
                if !recorded {
                    OVERFLOW.store(true, Ordering::Release);
                }
            }
            allocation
        }
        unsafe fn dealloc(&self, allocation: *mut u8, layout: Layout) {
            // SAFETY: Forward the requested physical deallocation unchanged.
            unsafe { System.dealloc(allocation, layout) };
            for (index, pointer) in POINTERS.iter().enumerate() {
                if pointer
                    .compare_exchange(
                        allocation,
                        ptr::null_mut(),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    let role = ROLES[index].swap(0, Ordering::AcqRel) as usize;
                    LIVE[role].fetch_sub(1, Ordering::AcqRel);
                    break;
                }
            }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Tracked = Tracked;
    struct Tracking;
    impl Drop for Tracking {
        fn drop(&mut self) {
            MODE.with(|mode| mode.set(0));
        }
    }
    fn measured<T>(role: usize, action: impl FnOnce() -> T) -> T {
        MODE.with(|mode| {
            assert_eq!(mode.replace(role), 0);
        });
        let guard = Tracking;
        let result = action();
        drop(guard);
        result
    }
    fn reset() {
        assert!(
            !OVERFLOW.load(Ordering::Acquire),
            "allocation ledger overflowed"
        );
        for role in 1..4 {
            assert_eq!(
                LIVE[role].load(Ordering::Acquire),
                0,
                "prior backing still live"
            );
            REQUESTED[role].store(0, Ordering::Release);
            ALLOCATIONS[role].store(0, Ordering::Release);
            MAX_REQUEST[role].store(0, Ordering::Release);
        }
    }
    struct ExitCredit {
        exits: Arc<AtomicUsize>,
        funding: Arc<AtomicUsize>,
    }
    impl Drop for ExitCredit {
        fn drop(&mut self) {
            assert!(!OVERFLOW.load(Ordering::Acquire));
            assert_eq!(
                LIVE[RAW].load(Ordering::Acquire),
                0,
                "original credit exited before physical encoded Vec/Core deallocation"
            );
            assert_eq!(
                LIVE[CARRIER].load(Ordering::Acquire),
                0,
                "credit exited before its separately covered ownership carrier allocation"
            );
            assert_ne!(self.funding.swap(0, Ordering::AcqRel), 0);
            assert_eq!(self.exits.fetch_add(1, Ordering::AcqRel), 0);
        }
    }
    fn buffer(max: usize) -> (ReceiveHeaderBlockBuffer, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        reset();
        let bound = ReceiveHeaderBlockBuffer::allocation_capacity_bound(max).unwrap();
        let exits = Arc::new(AtomicUsize::new(0));
        let funding = Arc::new(AtomicUsize::new(bound));
        let carrier = measured(CARRIER, || {
            Bytes::from_owner_with_exit_guard(
                Bytes::new(),
                ExitCredit {
                    exits: exits.clone(),
                    funding: funding.clone(),
                },
            )
        });
        let buffer = measured(RAW, || ReceiveHeaderBlockBuffer::new(max, carrier).unwrap());
        assert_eq!(
            ALLOCATIONS[RAW].load(Ordering::Acquire),
            2,
            "probe must observe the actual Vec and Arc backing"
        );
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 2);
        assert!(REQUESTED[RAW].load(Ordering::Acquire) <= bound);
        (buffer, exits, funding)
    }

    #[test]
    fn constructor_exact_backing_and_last_public_handle_physical_exit() {
        let (buffer, exits, funding) = buffer(256);
        assert_eq!(buffer.max_encoded_bytes(), 256);
        assert_eq!(MAX_REQUEST[RAW].load(Ordering::Acquire), 256);
        let last = measured(FRAME, || buffer.clone());
        assert_eq!(ALLOCATIONS[FRAME].load(Ordering::Acquire), 0);
        let mut bound = measured(FRAME, || buffer.bind(256).unwrap());
        assert!(bound.append(b"whole block"));
        drop(buffer);
        drop(bound);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 2);
        assert!(funding.load(Ordering::Acquire) > 0);
        drop(last);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert_eq!(funding.load(Ordering::Acquire), 0);
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 0);
    }
    #[test]
    fn append_reset_reuses_exact_pointer_and_never_allocates() {
        let (buffer, exits, _) = buffer(128);
        let mut bound = buffer.bind(128).unwrap();
        let pointer = bound.storage().as_ptr();
        measured(FRAME, || {
            for _ in 0..3 {
                for byte in 0..128u8 {
                    assert!(bound.append(&[byte]));
                }
                assert!(!bound.append(&[99]));
                bound.decode(|bytes, committed| {
                    assert_eq!(bytes.len(), 128);
                    assert_eq!(bytes.as_ptr(), pointer);
                    for (at, byte) in bytes.iter().enumerate() {
                        assert_eq!(*byte, at as u8);
                    }
                    assert_eq!(*committed, 0);
                    *committed = bytes.len();
                });
                bound.reset();
                bound.decode(|bytes, committed| {
                    assert!(bytes.is_empty());
                    assert_eq!(*committed, 0);
                });
                assert_eq!(bound.storage().as_ptr(), pointer);
            }
        });
        assert_eq!(
            ALLOCATIONS[FRAME].load(Ordering::Acquire),
            0,
            "fixed append/decode/reset must not allocate"
        );
        drop(bound);
        drop(buffer);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn refused_append_preserves_payload_commit_and_local_limit() {
        let (buffer, _, _) = buffer(64);
        let mut bound = buffer.bind(4).unwrap();
        assert!(bound.append(b"abc"));
        bound.decode(|_, committed| *committed = 2);
        assert!(!bound.append(b"xy"));
        bound.decode(|bytes, committed| {
            assert_eq!(bytes, b"abc");
            assert_eq!(*committed, 2);
        });
        assert!(bound.append(b"d"));
        assert!(bound.append(b""));
        assert!(!bound.append(b"e"));
        bound.filled = usize::MAX;
        assert!(
            !bound.append(b"x"),
            "checked overflow must refuse before storage access"
        );
        bound.reset();
        assert!(bound.append(b"z"));
        drop(bound);
        drop(buffer);
    }
    #[test]
    fn invalid_bind_does_not_consume_once_binding_and_dropped_lease_cannot_rebind() {
        let (buffer, _, _) = buffer(64);
        assert!(buffer.bind(0).is_err());
        assert!(buffer.bind(65).is_err());
        let bound = buffer.bind(64).unwrap();
        assert!(buffer.bind(64).is_err());
        drop(bound);
        assert!(buffer.bind(64).is_err());
        drop(buffer);
    }
    #[test]
    fn racing_public_clones_mint_only_one_mutable_lease() {
        let (buffer, exits, _) = buffer(64);
        let barrier = Arc::new(Barrier::new(4));
        let mut threads = Vec::new();
        for _ in 0..4 {
            let buffer = buffer.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                buffer.bind(64).ok()
            }));
        }
        let mut leases = Vec::new();
        for thread in threads {
            if let Some(lease) = thread.join().unwrap() {
                leases.push(lease);
            }
        }
        assert_eq!(leases.len(), 1);
        drop(buffer);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        assert!(leases[0].append(b"winner"));
        drop(leases);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn callback_commit_retry_is_inline_and_input_copy_is_positive_control() {
        let (buffer, _, _) = buffer(64);
        let mut bound = buffer.bind(64).unwrap();
        assert!(bound.append(b"a"));
        let complete = bound.decode(|bytes, committed| {
            assert_eq!(bytes, b"a");
            assert_eq!(*committed, 0);
            false
        });
        assert!(!complete);
        assert!(bound.append(b"b"));
        let owned = bound.decode(|bytes, committed| {
            assert_eq!(*committed, 0);
            *committed = 2;
            [bytes[0], bytes[1]]
        });
        bound.reset();
        assert!(bound.append(b"xy"));
        assert_eq!(owned, *b"ab");
        let copied = measured(FRAME, || bound.decode(|bytes, _| bytes.to_vec()));
        assert_eq!(copied, b"xy");
        assert_eq!(ALLOCATIONS[FRAME].load(Ordering::Acquire), 1);
        assert_eq!(REQUESTED[FRAME].load(Ordering::Acquire), 2);
        drop(copied);
        drop(bound);
        drop(buffer);
    }
    #[test]
    fn callback_panic_drops_capture_keeps_owner_and_leaves_valid_state() {
        let (buffer, exits, _) = buffer(64);
        let mut bound = buffer.bind(64).unwrap();
        assert!(bound.append(b"abc"));
        let capture = Arc::new(7u8);
        let weak = Arc::downgrade(&capture);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bound.decode(move |bytes, committed| {
                assert_eq!(bytes, b"abc");
                *committed = 1;
                drop(capture);
                panic!("intentional callback panic");
            });
        }));
        assert!(result.is_err());
        assert!(weak.upgrade().is_none());
        assert_eq!(exits.load(Ordering::Acquire), 0);
        bound.decode(|bytes, committed| {
            assert_eq!(bytes, b"abc");
            assert_eq!(*committed, 1);
        });
        assert!(bound.append(b"d"));
        bound.reset();
        drop(bound);
        drop(buffer);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn geometry_is_checked_before_constructor_allocation() {
        assert!(ReceiveHeaderBlockBuffer::allocation_capacity_bound(0).is_err());
        if usize::BITS > 32 {
            assert!(
                ReceiveHeaderBlockBuffer::allocation_capacity_bound(u32::MAX as usize + 1).is_err()
            );
        }
        assert!(
            ReceiveHeaderBlockBuffer::allocation_capacity_bound(1).unwrap()
                > std::mem::size_of::<Core>()
        );
    }
    #[test]
    fn original_exit_panic_occurs_after_all_physical_backing_is_freed() {
        struct PanicExit(Arc<AtomicUsize>);
        impl Drop for PanicExit {
            fn drop(&mut self) {
                assert_eq!(LIVE[RAW].load(Ordering::Acquire), 0);
                assert_eq!(LIVE[CARRIER].load(Ordering::Acquire), 0);
                self.0.fetch_add(1, Ordering::AcqRel);
                panic!("intentional original exit panic after deallocation");
            }
        }
        reset();
        let exits = Arc::new(AtomicUsize::new(0));
        let carrier = measured(CARRIER, || {
            Bytes::from_owner_with_exit_guard(Bytes::new(), PanicExit(exits.clone()))
        });
        let buffer = measured(RAW, || ReceiveHeaderBlockBuffer::new(64, carrier).unwrap());
        let bound = buffer.bind(64).unwrap();
        drop(buffer);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(bound)));
        assert!(outcome.is_err());
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 0);
        assert_eq!(LIVE[CARRIER].load(Ordering::Acquire), 0);
    }
}
