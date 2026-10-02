#[cfg(test)]
mod probe {
    use bytes::Bytes;
    use h2::{
        m07_field_bind, m07_field_copy, m07_field_fill, m07_field_wrapper_size, M07ArenaDecoder,
        M07FieldError, ReceiveHeaderFieldPool,
    };
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    const RAW: usize = 1;
    const FIELD: usize = 2;
    const CARRIER: usize = 3;
    const ACTION: usize = 4;
    const RECORDS: usize = 128;
    thread_local! {
        static MODE: Cell<usize> = const { Cell::new(0) };
        // A test-scoped borrowed pool stays alive until this watch is cleared.
        static WATCH: Cell<*const ReceiveHeaderFieldPool> = const { Cell::new(ptr::null()) };
        static EXPECTED: Cell<usize> = const { Cell::new(0) };
    }
    static POINTERS: [AtomicPtr<u8>; RECORDS] =
        [const { AtomicPtr::new(ptr::null_mut()) }; RECORDS];
    static ROLES: [AtomicUsize; RECORDS] = [const { AtomicUsize::new(0) }; RECORDS];
    static SIZES: [AtomicUsize; RECORDS] = [const { AtomicUsize::new(0) }; RECORDS];
    static LIVE: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
    static LIVE_BYTES: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
    static PEAK: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
    static ALLOC: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
    static REALLOC: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
    static REQUESTED: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
    static OVERFLOW: AtomicBool = AtomicBool::new(false);
    static EARLY_REUSE: AtomicBool = AtomicBool::new(false);
    fn record(pointer: *mut u8, size: usize, role: usize) {
        if role == 0 || pointer.is_null() {
            return;
        }
        ALLOC[role].fetch_add(1, Ordering::AcqRel);
        REQUESTED[role].fetch_add(size, Ordering::AcqRel);
        for (i, slot) in POINTERS.iter().enumerate() {
            if slot
                .compare_exchange(
                    ptr::null_mut(),
                    pointer,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                ROLES[i].store(role, Ordering::Release);
                SIZES[i].store(size, Ordering::Release);
                LIVE[role].fetch_add(1, Ordering::AcqRel);
                let total = LIVE_BYTES[role].fetch_add(size, Ordering::AcqRel) + size;
                PEAK[role].fetch_max(total, Ordering::AcqRel);
                return;
            }
        }
        OVERFLOW.store(true, Ordering::Release);
    }
    struct Tracked;
    // SAFETY: System receives every unchanged allocator pointer/Layout. The
    // fixed atomic ledger and TLS do not allocate. No allocator callback panics.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(layout) };
            record(p, layout.size(), MODE.try_with(Cell::get).unwrap_or(0));
            p
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(layout) };
            record(p, layout.size(), MODE.try_with(Cell::get).unwrap_or(0));
            p
        }
        unsafe fn realloc(&self, old: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let p = unsafe { System.realloc(old, layout, size) };
            if p.is_null() {
                return p;
            }
            let mode = MODE.try_with(Cell::get).unwrap_or(0);
            if mode != 0 {
                REALLOC[mode].fetch_add(1, Ordering::AcqRel);
                REQUESTED[mode].fetch_add(size, Ordering::AcqRel);
            }
            for (i, slot) in POINTERS.iter().enumerate() {
                if slot
                    .compare_exchange(old, p, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    let role = ROLES[i].load(Ordering::Acquire);
                    let old_size = SIZES[i].swap(size, Ordering::AcqRel);
                    if size >= old_size {
                        let total = LIVE_BYTES[role].fetch_add(size - old_size, Ordering::AcqRel)
                            + size
                            - old_size;
                        PEAK[role].fetch_max(total, Ordering::AcqRel);
                    } else {
                        LIVE_BYTES[role].fetch_sub(old_size - size, Ordering::AcqRel);
                    }
                    return p;
                }
            }
            // No measured allocation in these probes reallocates from an
            // untracked origin; flag such an event rather than hide it.
            if mode != 0 {
                OVERFLOW.store(true, Ordering::Release);
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
            let index = POINTERS
                .iter()
                .position(|slot| slot.load(Ordering::Acquire) == p);
            if let Some(i) = index {
                if ROLES[i].load(Ordering::Acquire) == FIELD {
                    WATCH
                        .try_with(|watch| {
                            let pool = watch.get();
                            if !pool.is_null() {
                                // SAFETY: test installs a pointer to a live stack
                                // pool, clears it before that pool may move/drop,
                                // and only observes the atomic public getter.
                                let available = unsafe { (&*pool).available_positions() };
                                if available != EXPECTED.with(Cell::get) {
                                    EARLY_REUSE.store(true, Ordering::Release);
                                }
                            }
                        })
                        .ok();
                }
            }
            unsafe { System.dealloc(p, layout) };
            if let Some(i) = index {
                POINTERS[i].store(ptr::null_mut(), Ordering::Release);
                let role = ROLES[i].swap(0, Ordering::AcqRel);
                let size = SIZES[i].swap(0, Ordering::AcqRel);
                LIVE[role].fetch_sub(1, Ordering::AcqRel);
                LIVE_BYTES[role].fetch_sub(size, Ordering::AcqRel);
            }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Tracked = Tracked;
    struct Tracking;
    impl Drop for Tracking {
        fn drop(&mut self) {
            MODE.with(|m| m.set(0));
        }
    }
    fn measured<T>(role: usize, action: impl FnOnce() -> T) -> T {
        MODE.with(|m| assert_eq!(m.replace(role), 0));
        let guard = Tracking;
        let out = action();
        drop(guard);
        out
    }
    fn begin() {
        MODE.with(|m| assert_eq!(m.replace(ACTION), 0));
    }
    fn end() {
        MODE.with(|m| m.set(0));
    }
    fn reset() {
        assert!(!OVERFLOW.load(Ordering::Acquire));
        assert!(!EARLY_REUSE.load(Ordering::Acquire));
        for role in 1..5 {
            assert_eq!(
                LIVE[role].load(Ordering::Acquire),
                0,
                "prior tracked owner still live"
            );
            assert_eq!(LIVE_BYTES[role].load(Ordering::Acquire), 0);
            for counters in [&PEAK, &ALLOC, &REALLOC, &REQUESTED] {
                counters[role].store(0, Ordering::Release);
            }
        }
    }
    struct ExitCredit(Arc<AtomicUsize>);
    impl Drop for ExitCredit {
        fn drop(&mut self) {
            assert!(!OVERFLOW.load(Ordering::Acquire));
            for role in [RAW, FIELD, CARRIER] {
                assert_eq!(LIVE[role].load(Ordering::Acquire),0,
                    "original credit must exit after arena/Core and every real wrapper physically deallocate");
            }
            assert_eq!(self.0.fetch_add(1, Ordering::AcqRel), 0);
        }
    }
    fn pool(
        capacity: usize,
        positions: usize,
        max: usize,
    ) -> (ReceiveHeaderFieldPool, Arc<AtomicUsize>, usize) {
        reset();
        let bound =
            ReceiveHeaderFieldPool::allocation_capacity_bound(capacity, positions, max).unwrap();
        let exits = Arc::new(AtomicUsize::new(0));
        let owner = measured(CARRIER, || {
            Bytes::from_owner_with_exit_guard(Bytes::new(), ExitCredit(exits.clone()))
        });
        let pool = measured(RAW, || {
            ReceiveHeaderFieldPool::new(capacity, positions, max, owner).unwrap()
        });
        assert_eq!(
            ALLOC[RAW].load(Ordering::Acquire),
            3,
            "actual arena/bitmap/Core must be tracked"
        );
        assert_eq!(REALLOC[RAW].load(Ordering::Acquire), 0);
        (pool, exits, bound)
    }
    fn integer(value: usize, prefix: u8, flag: u8, wire: &mut Vec<u8>) {
        let mask = (1usize << prefix) - 1;
        if value < mask {
            wire.push(flag | value as u8);
            return;
        }
        wire.push(flag | mask as u8);
        let mut remaining = value - mask;
        while remaining >= 128 {
            wire.push((remaining as u8 & 127) | 128);
            remaining >>= 7;
        }
        wire.push(remaining as u8);
    }
    fn string(input: &[u8], huffman: bool, wire: &mut Vec<u8>) {
        let encoded = if huffman {
            M07ArenaDecoder::huffman(input)
        } else {
            input.to_vec()
        };
        integer(encoded.len(), 7, if huffman { 128 } else { 0 }, wire);
        wire.extend_from_slice(&encoded);
    }
    fn literal(name: &[u8], value: &[u8], huffman: bool, indexed: bool) -> Vec<u8> {
        let mut wire = vec![if indexed { 0x40 } else { 0 }];
        string(name, huffman, &mut wire);
        string(value, huffman, &mut wire);
        wire
    }

    #[test]
    fn actual_new_bounded_constructor_has_no_legacy_scratch_allocation() {
        let (pool, _, _) = pool(128, 2, 128);
        let pooled = measured(ACTION, || {
            M07ArenaDecoder::new(Some(pool.clone()), 128, 128)
        });
        let none = measured(ACTION, || M07ArenaDecoder::new(None, 128, 128));
        assert_eq!(
            ALLOC[ACTION].load(Ordering::Acquire),
            0,
            "actual new_bounded constructor must not allocate legacy scratch"
        );
        assert_eq!(REALLOC[ACTION].load(Ordering::Acquire), 0);
        let legacy = measured(ACTION, M07ArenaDecoder::legacy);
        assert_eq!(
            ALLOC[ACTION].load(Ordering::Acquire),
            1,
            "actual legacy constructor allocation must be visible to the oracle"
        );
        assert_eq!(REQUESTED[ACTION].load(Ordering::Acquire), 4096);
        assert_eq!(LIVE[ACTION].load(Ordering::Acquire), 1);
        drop(legacy);
        assert_eq!(LIVE[ACTION].load(Ordering::Acquire), 0);
        drop(pooled);
        drop(none);
        drop(pool);
    }
    #[test]
    fn constructor_and_all_positions_fit_complete_original_capacity_bound() {
        let (pool, exits, bound) = pool(256, 4, 128);
        assert_eq!(pool.capacity_bytes(), 256);
        assert_eq!(pool.field_positions(), 4);
        assert_eq!(pool.max_field_bytes(), 128);
        let fields = measured(FIELD, || {
            [0, 1, 2, 3].map(|byte| m07_field_copy(&pool, &[byte]).unwrap())
        });
        assert_eq!(ALLOC[FIELD].load(Ordering::Acquire), 4);
        assert_eq!(REALLOC[FIELD].load(Ordering::Acquire), 0);
        assert_eq!(
            REQUESTED[FIELD].load(Ordering::Acquire),
            4 * m07_field_wrapper_size()
        );
        let peak = PEAK[RAW].load(Ordering::Acquire) + PEAK[FIELD].load(Ordering::Acquire);
        eprintln!(
            "pool actual peak={peak}, checked bound={bound}, wrapper={}",
            m07_field_wrapper_size()
        );
        assert!(
            peak <= bound,
            "complete original capacity must include every actual allocation"
        );
        assert_eq!(pool.available_positions(), 0);
        assert!(m07_field_copy(&pool, b"x").unwrap_err().is_exhausted());
        let aliases = measured(ACTION, || fields.clone());
        assert_eq!(ALLOC[ACTION].load(Ordering::Acquire), 0);
        drop(pool);
        drop(fields);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(aliases);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn escaping_slice_retains_original_pool_after_public_handles_drop() {
        let (pool, exits, _) = pool(128, 2, 128);
        let bound = m07_field_bind(&pool).unwrap();
        let body = measured(FIELD, || {
            m07_field_copy(&bound, b"retained original field").unwrap()
        });
        let last = body.slice(3..9);
        let pointer = last.as_ptr();
        drop(body);
        drop(bound);
        drop(pool);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        assert_eq!(&last[..], b"ained ");
        assert_eq!(last.as_ptr(), pointer);
        drop(last);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn position_is_not_published_until_actual_wrapper_deallocation() {
        let (pool, _, _) = pool(64, 1, 64);
        let field = measured(FIELD, || m07_field_copy(&pool, b"x").unwrap());
        let pointer = field.as_ptr();
        let alias = field.clone();
        drop(field);
        assert_eq!(
            pool.available_positions(),
            0,
            "position must remain occupied while alias retained"
        );
        EXPECTED.with(|n| n.set(0));
        WATCH.with(|w| w.set(&pool));
        drop(alias);
        WATCH.with(|w| w.set(ptr::null()));
        assert!(
            !EARLY_REUSE.load(Ordering::Acquire),
            "position was published before wrapper physical deallocation"
        );
        assert_eq!(pool.available_positions(), 1);
        let next = m07_field_copy(&pool, b"y").unwrap();
        assert_eq!(next.as_ptr(), pointer);
        drop(next);
        drop(pool);
    }
    #[test]
    fn fragmented_extents_refuse_without_fallback_then_reuse_adjacent_space() {
        let (pool, _, _) = pool(256, 4, 128);
        let [a, b, c, d] = [1, 2, 3, 4].map(|byte| m07_field_copy(&pool, &[byte; 64]).unwrap());
        let pointer = a.as_ptr();
        drop(a);
        drop(c);
        let result = measured(ACTION, || m07_field_copy(&pool, &[9; 128]));
        assert!(result.unwrap_err().is_exhausted());
        assert_eq!(ALLOC[ACTION].load(Ordering::Acquire), 0);
        assert_eq!(REALLOC[ACTION].load(Ordering::Acquire), 0);
        drop(b);
        let next = m07_field_copy(&pool, &[9; 128]).unwrap();
        assert_eq!(next.as_ptr(), pointer);
        assert_eq!(&next[..], &[9; 128]);
        assert_eq!(&d[..], &[4; 64]);
        drop(next);
        drop(d);
        drop(pool);
    }
    #[test]
    fn empty_error_panic_and_reentrant_fill_preserve_actual_claims() {
        let (pool, _, _) = pool(128, 2, 128);
        let called = Cell::new(false);
        let empty = measured(ACTION, || {
            m07_field_fill(&pool, 0, |dst| {
                called.set(true);
                assert!(dst.is_empty());
                Ok(())
            })
            .unwrap()
        });
        assert!(called.get());
        assert!(empty.is_empty());
        assert_eq!(pool.available_positions(), 2);
        assert_eq!(ALLOC[ACTION].load(Ordering::Acquire), 0);
        assert_eq!(
            m07_field_fill(&pool, 65, |_| Err(M07FieldError::fill_error())),
            Err(M07FieldError::fill_error())
        );
        assert_eq!(pool.available_positions(), 2);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = m07_field_fill(&pool, 128, |_| panic!("actual fill callback panic"));
        }));
        assert!(panic.is_err());
        assert_eq!(pool.available_positions(), 2);
        let out = m07_field_fill(&pool, 64, |dst| {
            assert!(m07_field_copy(&pool, b"nested").unwrap_err().is_exhausted());
            dst.fill(4);
            Ok(())
        })
        .unwrap();
        assert_eq!(pool.available_positions(), 1);
        assert_eq!(&out[..], &[4; 64]);
        drop(out);
        drop(pool);
    }
    #[test]
    fn concurrent_checkout_refuses_and_thread_aliases_release_final_original_owner() {
        let (pool, exits, _) = pool(128, 2, 128);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let other = pool.clone();
        let e = entered.clone();
        let r = release.clone();
        let worker = std::thread::spawn(move || {
            m07_field_fill(&other, 64, |dst| {
                e.wait();
                r.wait();
                dst.fill(5);
                Ok(())
            })
            .unwrap()
        });
        entered.wait();
        assert!(m07_field_copy(&pool, b"parallel")
            .unwrap_err()
            .is_exhausted());
        release.wait();
        let body = worker.join().unwrap();
        let last = body.clone();
        drop(pool);
        let peers = [body.clone(), body.clone(), body];
        let jobs = peers.map(|bytes| {
            std::thread::spawn(move || {
                assert_eq!(&bytes[..], &[5; 64]);
                drop(bytes);
            })
        });
        for job in jobs {
            job.join().unwrap();
        }
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(last);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn checked_geometry_and_once_binding_are_finite() {
        for args in [
            (0, 1, 1),
            (63, 1, 1),
            (64, 0, 1),
            (64, 2, 1),
            (64, 1, 0),
            (64, 1, 65),
            (usize::MAX & !63, 1, 64),
        ] {
            assert!(
                ReceiveHeaderFieldPool::allocation_capacity_bound(args.0, args.1, args.2).is_err()
            );
        }
        let (pool, _, _) = pool(64, 1, 64);
        let copy = pool.clone();
        let b = m07_field_bind(&pool).unwrap();
        assert!(m07_field_bind(&copy).is_err());
        drop(b);
        assert!(m07_field_bind(&copy).is_err());
        let called = Cell::new(false);
        let error = m07_field_fill(&pool, 65, |_| {
            called.set(true);
            Ok(())
        })
        .unwrap_err();
        assert!(error.is_too_large());
        assert!(!called.get());
        drop(copy);
        drop(pool);
    }
    #[test]
    fn actual_bounded_plain_and_huffman_use_only_exact_pool_wrappers() {
        for huff in [false, true] {
            let (pool, _, _) = pool(256, 4, 128);
            let mut decoder = M07ArenaDecoder::new(Some(pool.clone()), 128, 128);
            let wire = literal(b"custom-field", b"original value", huff, false);
            let (commit, result) = decoder.discard(&wire, begin, end);
            result.unwrap();
            assert_eq!(commit, wire.len());
            assert_eq!(
                ALLOC[ACTION].load(Ordering::Acquire),
                2,
                "actual pooled decoder must allocate only two owner wrappers"
            );
            assert_eq!(
                REQUESTED[ACTION].load(Ordering::Acquire),
                2 * m07_field_wrapper_size(),
                "pooled decoder must not allocate independent string backing"
            );
            assert_eq!(REALLOC[ACTION].load(Ordering::Acquire), 0);
            assert_eq!(LIVE[ACTION].load(Ordering::Acquire), 0);
            assert_eq!(pool.available_positions(), 4);
            drop(decoder);
            drop(pool);
        }
    }
    #[test]
    fn actual_need_more_never_claims_either_field() {
        for huff in [false, true] {
            let wire = literal(b"custom-field", b"original value", huff, false);
            for cut in 1..wire.len() {
                let (pool, _, _) = pool(256, 4, 128);
                let mut decoder = M07ArenaDecoder::new(Some(pool.clone()), 128, 128);
                let (commit, error) = decoder.discard(&wire[..cut], begin, end);
                assert!(error.unwrap_err().is_need_more(), "cut={cut}");
                assert_eq!(commit, 0);
                assert_eq!(
                    ALLOC[ACTION].load(Ordering::Acquire),
                    0,
                    "NeedMore must not checkout name before complete value"
                );
                assert_eq!(pool.available_positions(), 4);
                let (_, result) = decoder.discard(&wire, || {}, || {});
                result.unwrap();
                assert_eq!(pool.available_positions(), 4);
                drop(decoder);
                drop(pool);
            }
        }
    }
    #[test]
    fn actual_combined_field_limit_precedes_plain_and_huffman_checkout() {
        for huff in [false, true] {
            let (pool, _, _) = pool(128, 2, 64);
            let mut decoder = M07ArenaDecoder::new(Some(pool.clone()), 49, 128);
            let wire = literal(b"custom-xx", b"originalx", huff, false);
            let (_, result) = decoder.discard(&wire, begin, end);
            assert!(result.unwrap_err().is_too_large());
            assert_eq!(
                ALLOC[ACTION].load(Ordering::Acquire),
                0,
                "combined decoded lengths+32 must be checked before arena checkout"
            );
            assert_eq!(pool.available_positions(), 2);
            drop(decoder);
            drop(pool);
        }
    }
    #[test]
    fn actual_pool_exhaustion_is_not_need_more_and_releases_partial_name() {
        let (pool, _, _) = pool(128, 1, 64);
        let mut decoder = M07ArenaDecoder::new(Some(pool.clone()), 128, 128);
        let wire = literal(b"custom-field", b"original value", false, false);
        let (_, error) = decoder.discard(&wire, || {}, || {});
        let error = error.unwrap_err();
        assert!(error.is_exhausted());
        assert!(!error.is_need_more());
        assert_eq!(pool.available_positions(), 1);
        let hold = m07_field_copy(&pool, b"held").unwrap();
        let (_, error) = decoder.discard(&wire, begin, end);
        assert!(error.unwrap_err().is_exhausted());
        assert_eq!(ALLOC[ACTION].load(Ordering::Acquire), 0);
        drop(hold);
        drop(decoder);
        drop(pool);
    }
    #[test]
    fn actual_dynamic_table_clones_retain_original_alias_past_decoder_and_handles() {
        let (pool, exits, _) = pool(256, 4, 128);
        let mut decoder = M07ArenaDecoder::new(Some(pool.clone()), 128, 128);
        let wire = literal(b"custom-field", b"original value", true, true);
        let (fields, result) = decoder.borrowed(&wire, &mut 0);
        result.unwrap();
        assert_eq!(pool.available_positions(), 2);
        assert_eq!(decoder.table_state().1, 1);
        let (indexed, result) = decoder.borrowed(&[0xbe], &mut 0);
        result.unwrap();
        assert_eq!(fields, indexed);
        assert_eq!(fields[0].name_pointer(), indexed[0].name_pointer());
        assert_eq!(fields[0].value_pointer(), indexed[0].value_pointer());
        let last = indexed[0].clone();
        drop(fields);
        drop(indexed);
        drop(decoder);
        drop(pool);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        assert_eq!(last.value_slice(), b"original value");
        drop(last);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
    #[test]
    fn bounded_none_preserves_compact_default_semantics_and_error_classes() {
        for huff in [false, true] {
            let (pool, _, _) = pool(256, 4, 128);
            let mut pooled = M07ArenaDecoder::new(Some(pool.clone()), 128, 128);
            let mut old = M07ArenaDecoder::new(None, 128, 128);
            let wire = literal(b"custom-field", b"original value", huff, false);
            let (actual, a) = pooled.borrowed(&wire, &mut 0);
            let (expected, b) = old.borrowed(&wire, &mut 0);
            assert_eq!(a, b);
            assert_eq!(actual, expected);
            let mut invalid = literal(b"INVALID", b"value", huff, false);
            let (actual, a) = pooled.borrowed(&invalid, &mut 0);
            let (expected, b) = old.borrowed(&invalid, &mut 0);
            assert_eq!(a, b);
            assert_eq!(actual, expected);
            invalid = vec![0, 0x81, 0xff, 0];
            let (_, a) = pooled.borrowed(&invalid, &mut 0);
            let (_, b) = old.borrowed(&invalid, &mut 0);
            assert_eq!(a, b);
            drop(actual);
            drop(expected);
            drop(pooled);
            drop(old);
            drop(pool);
        }
    }
}
