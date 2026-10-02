#[cfg(test)]
mod probe {
    use bytes::Bytes;
    use h2::{
        m07_field_wrapper_size, m07_table_slot_layout, M07BoundTable, M07TableDecoder,
        M07TableHeader, ReceiveHeaderFieldPool, ReceiveHeaderTableBuffer,
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
    struct TableCredit(Arc<AtomicUsize>);
    impl Drop for TableCredit {
        fn drop(&mut self) {
            assert_eq!(
                LIVE[RAW].load(Ordering::Acquire),
                0,
                "typed Vec/Core/Arc must physically exit before original table credit"
            );
            assert_eq!(
                LIVE[CARRIER].load(Ordering::Acquire),
                0,
                "carrier wrapper must physically exit before table credit"
            );
            assert_eq!(self.0.fetch_add(1, Ordering::AcqRel), 0);
        }
    }
    fn table() -> (ReceiveHeaderTableBuffer, Arc<AtomicUsize>, usize) {
        reset();
        let bound = ReceiveHeaderTableBuffer::allocation_capacity_bound(4096).unwrap();
        let exited = Arc::new(AtomicUsize::new(0));
        let original = measured(CARRIER, || {
            Bytes::from_owner_with_exit_guard(Bytes::new(), TableCredit(exited.clone()))
        });
        let table = measured(RAW, || {
            ReceiveHeaderTableBuffer::new(4096, original).unwrap()
        });
        assert_eq!(
            ALLOC[RAW].load(Ordering::Acquire),
            2,
            "actual typed slots and Core/Arc"
        );
        assert_eq!(REALLOC[RAW].load(Ordering::Acquire), 0);
        (table, exited, bound)
    }
    fn header(value: &'static [u8]) -> M07TableHeader {
        M07TableHeader::new(Bytes::from_static(b"x"), Bytes::from_static(value)).unwrap()
    }
    #[test]
    fn actual_typed_backing_fits_original_bound_without_ring_growth() {
        let (public, exits, bound) = table();
        let mut ring = M07BoundTable::bind(&public).unwrap();
        assert_eq!(ring.capacity(), 128);
        let (slot_bytes, slot_align) = m07_table_slot_layout(4096);
        assert!(
            slot_bytes > 4096,
            "logical 32-byte charge is not the typed Rust layout"
        );
        assert!(slot_align > 0);
        assert!(
            PEAK[RAW].load(Ordering::Acquire) <= bound,
            "complete original bound must cover every typed allocation"
        );
        assert!(REQUESTED[RAW].load(Ordering::Acquire) >= slot_bytes);
        eprintln!(
            "typed slots={slot_bytes}; Core/Arc={}; peak={}; bound={bound}",
            REQUESTED[RAW].load(Ordering::Acquire) - slot_bytes,
            PEAK[RAW].load(Ordering::Acquire)
        );
        let pointer = ring.backing_pointer();
        begin();
        for _ in 0..128 {
            ring.push_front(header(b"v")).unwrap();
        }
        let error = ring.push_front(header(b"extra")).unwrap_err();
        assert!(error.is_table_full());
        for _ in 0..512 {
            assert_eq!(ring.pop_back().unwrap().value(), b"v");
            ring.push_front(header(b"v")).unwrap();
            assert_eq!(ring.get(0).unwrap().value(), b"v");
            assert_eq!(ring.back().unwrap().value(), b"v");
        }
        end();
        assert_eq!(ring.len(), 128);
        assert!(ring.get(128).is_none());
        assert_eq!(ring.backing_pointer(), pointer);
        assert_eq!(
            ALLOC[ACTION].load(Ordering::Acquire),
            0,
            "ring may not grow or allocate a fallback"
        );
        assert_eq!(REALLOC[ACTION].load(Ordering::Acquire), 0);
        drop(public);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(ring);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        reset();
    }
    #[test]
    fn actual_ring_newest_oldest_and_once_bind_are_independent_of_public_clones() {
        let (public, exits, _) = table();
        let copy = public.clone();
        let mut ring = M07BoundTable::bind(&public).unwrap();
        assert!(M07BoundTable::bind(&copy).is_err());
        ring.push_front(header(b"oldest")).unwrap();
        ring.push_front(header(b"middle")).unwrap();
        ring.push_front(header(b"newest")).unwrap();
        assert_eq!(ring.get(0).unwrap().value(), b"newest");
        assert_eq!(ring.get(1).unwrap().value(), b"middle");
        assert_eq!(ring.back().unwrap().value(), b"oldest");
        assert_eq!(ring.pop_back().unwrap().value(), b"oldest");
        assert_eq!(ring.pop_back().unwrap().value(), b"middle");
        drop(ring);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(public);
        drop(copy);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        reset();
    }
    #[test]
    fn concurrent_bind_has_one_actual_winner() {
        let (public, exits, _) = table();
        let gate = Arc::new(Barrier::new(3));
        let threads = (0..2)
            .map(|_| {
                let public = public.clone();
                let gate = gate.clone();
                std::thread::spawn(move || {
                    gate.wait();
                    M07BoundTable::bind(&public).ok()
                })
            })
            .collect::<Vec<_>>();
        gate.wait();
        let leases = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(leases.iter().filter(|lease| lease.is_some()).count(), 1);
        drop(public);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(leases);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        reset();
    }
    #[test]
    fn invalid_typed_geometry_is_refused_before_table_construction() {
        for value in [0, 1, 4095, usize::MAX] {
            assert!(ReceiveHeaderTableBuffer::allocation_capacity_bound(value).is_err());
            assert!(ReceiveHeaderTableBuffer::new(value, Bytes::new()).is_err());
        }
        assert!(ReceiveHeaderTableBuffer::allocation_capacity_bound(4096).is_ok());
    }
    struct FieldCredit(Arc<AtomicUsize>);
    impl Drop for FieldCredit {
        fn drop(&mut self) {
            assert_eq!(
                LIVE[FIELD].load(Ordering::Acquire),
                0,
                "field backing must physically exit before field credit"
            );
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[test]
    fn actual_field_alias_outlives_table_credit_and_typed_backing() {
        let (public, table_exit, _) = table();
        let field_exit = Arc::new(AtomicUsize::new(0));
        let carrier =
            Bytes::from_owner_with_exit_guard(Bytes::new(), FieldCredit(field_exit.clone()));
        let fields = measured(FIELD, || {
            ReceiveHeaderFieldPool::new(256, 4, 128, carrier).unwrap()
        });
        let mut ring = M07BoundTable::bind(&public).unwrap();
        let original = measured(FIELD, || {
            M07TableHeader::from_pool(&fields, b"x-alias", b"retained").unwrap()
        });
        let clone = original.clone();
        ring.push_front(original).unwrap();
        assert_eq!(ring.get(0).unwrap().value_pointer(), clone.value_pointer());
        drop(ring.pop_back());
        assert_eq!(clone.value(), b"retained");
        drop(public);
        drop(ring);
        assert_eq!(table_exit.load(Ordering::Acquire), 1);
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 0);
        drop(fields);
        assert_eq!(field_exit.load(Ordering::Acquire), 0);
        drop(clone);
        assert_eq!(field_exit.load(Ordering::Acquire), 1);
        reset();
    }
    struct PanicValue {
        exits: Arc<AtomicUsize>,
        panic: bool,
    }
    impl AsRef<[u8]> for PanicValue {
        fn as_ref(&self) -> &[u8] {
            b"v"
        }
    }
    impl Drop for PanicValue {
        fn drop(&mut self) {
            self.exits.fetch_add(1, Ordering::AcqRel);
            if self.panic {
                panic!("intentional independent field destructor panic");
            }
        }
    }
    #[test]
    fn actual_vec_drop_glue_clears_remaining_entries_before_table_credit_on_unwind() {
        let (public, table_exit, _) = table();
        let values = Arc::new(AtomicUsize::new(0));
        let mut ring = M07BoundTable::bind(&public).unwrap();
        // Physical slot 127 is visited after slot 126. The panic occurs at
        // 126 and the remaining nonpanic entry must still be dropped.
        for panic in [false, true] {
            let value = Bytes::from_owner(PanicValue {
                exits: values.clone(),
                panic,
            });
            ring.push_front(M07TableHeader::new(Bytes::from_static(b"x"), value).unwrap())
                .unwrap();
        }
        drop(public);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(ring)));
        assert!(result.is_err());
        assert_eq!(values.load(Ordering::Acquire), 2);
        assert_eq!(table_exit.load(Ordering::Acquire), 1);
        reset();
    }
    fn update(size: usize) -> Vec<u8> {
        if size < 31 {
            return vec![0x20 | size as u8];
        }
        let mut wire = vec![0x3f];
        let mut rest = size - 31;
        while rest >= 128 {
            wire.push((rest as u8 & 127) | 128);
            rest >>= 7;
        }
        wire.push(rest as u8);
        wire
    }
    fn block(decoder: &mut M07TableDecoder, wire: &[u8]) -> Result<(), h2::M07TableError> {
        decoder.begin();
        let mut committed = 0;
        decoder.discard(wire, &mut committed)?;
        decoder.finish()
    }
    #[test]
    fn actual_decoder_preserves_required_minimum_and_latest_ack_limit() {
        for fixed in [false, true] {
            let table = ReceiveHeaderTableBuffer::new(8192, Bytes::new()).unwrap();
            let lease = if fixed {
                Some(M07BoundTable::bind(&table).unwrap())
            } else {
                None
            };
            let mut decoder = M07TableDecoder::new(None, lease);
            decoder.ack(1024);
            decoder.ack(8192);
            assert_eq!(
                decoder.state().2,
                4096,
                "ACK alone must not resize chosen table"
            );
            assert_eq!(decoder.state().3, 8192);
            assert_eq!(decoder.state().4, Some(1024));
            assert!(block(&mut decoder, &update(8192))
                .unwrap_err()
                .is_invalid_max());
            let mut valid = update(1024);
            valid.extend_from_slice(&update(4096));
            valid.push(0x88);
            block(&mut decoder, &valid).unwrap();
            assert_eq!(decoder.state().2, 4096);
            assert_eq!(decoder.state().4, None);
            decoder.ack(8192);
            decoder.ack(1024);
            assert_eq!(decoder.state().3, 1024);
            assert!(block(&mut decoder, &update(4096))
                .unwrap_err()
                .is_invalid_max());
        }
    }
    #[test]
    fn actual_decoder_requires_only_a_needed_reduction_and_checks_empty_end() {
        let mut decoder = M07TableDecoder::new(None, None);
        block(&mut decoder, &update(512)).unwrap();
        decoder.ack(1024);
        assert_eq!(decoder.state().4, None);
        block(&mut decoder, &[0x88]).unwrap();
        decoder.ack(0);
        decoder.begin();
        assert!(decoder.finish().unwrap_err().is_invalid_max());
        block(&mut decoder, &[0x20, 0x88]).unwrap();
        assert_eq!(decoder.state().2, 0);
    }
    #[test]
    fn actual_decoder_partial_integer_keeps_obligation_and_continuation_cannot_resize() {
        let mut decoder = M07TableDecoder::new(None, None);
        decoder.ack(1024);
        decoder.begin();
        let wire = update(1024);
        let mut committed = 0;
        for length in 1..wire.len() {
            assert!(decoder
                .discard(&wire[..length], &mut committed)
                .unwrap_err()
                .is_need_more());
            assert_eq!(committed, 0);
            assert_eq!(decoder.state().4, Some(1024));
        }
        decoder.discard(&wire, &mut committed).unwrap();
        decoder.finish().unwrap();
        assert_eq!(decoder.state().4, None);
        decoder.begin();
        committed = 0;
        decoder.discard(&[0x88], &mut committed).unwrap();
        committed = 0;
        assert!(decoder
            .discard(&[0x20], &mut committed)
            .unwrap_err()
            .is_invalid_max());
        block(&mut decoder, &[0x20, 0x88]).unwrap();
    }
    #[test]
    fn actual_fixed_decoder_inserts_without_table_allocation_and_retains_payload_aliases() {
        let (public, exited, _) = table();
        let lease = M07BoundTable::bind(&public).unwrap();
        let fields = ReceiveHeaderFieldPool::new(256, 4, 128, Bytes::new()).unwrap();
        let mut decoder = measured(ACTION, || {
            M07TableDecoder::new(Some(fields.clone()), Some(lease))
        });
        assert_eq!(
            ALLOC[ACTION].load(Ordering::Acquire),
            0,
            "bounded decoder must not allocate default table or scratch"
        );
        decoder.begin();
        let mut committed = 0;
        measured(ACTION, || {
            decoder
                .discard(b"\x40\x07x-owned\x01v", &mut committed)
                .unwrap()
        });
        assert_eq!(
            ALLOC[ACTION].load(Ordering::Acquire),
            2,
            "only the two independently funded field wrappers may allocate"
        );
        assert_eq!(
            REQUESTED[ACTION].load(Ordering::Acquire),
            2 * m07_field_wrapper_size(),
            "no independent table allocation or scratch fallback"
        );
        assert_eq!(REALLOC[ACTION].load(Ordering::Acquire), 0);
        assert_eq!(decoder.state().1, 1);
        decoder.finish().unwrap();
        decoder.begin();
        committed = 0;
        let (alias, result) = decoder.borrowed(&[0xbe], &mut committed);
        result.unwrap();
        decoder.finish().unwrap();
        assert_eq!(alias[0].value(), b"v");
        drop(public);
        drop(decoder);
        assert_eq!(exited.load(Ordering::Acquire), 1);
        drop(fields);
        assert_eq!(alias[0].value(), b"v");
        drop(alias);
        reset();
    }
}
