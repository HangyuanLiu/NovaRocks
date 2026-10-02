#[cfg(test)]
mod probe {
    use bytes::{Bytes, BytesMut};
    use h2::{M07HpackDecoder, M07HpackField};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering};
    use std::sync::Arc;
    const RAW: usize = 1;
    const FRAME: usize = 2;
    const CARRIER: usize = 3;
    const VALUE_CARRIER: usize = 4;
    const ACTION: usize = 5;
    const MAX_RECORDS: usize = 64;
    thread_local! { static MODE: Cell<usize> = const { Cell::new(0) }; }
    static POINTERS: [AtomicPtr<u8>; MAX_RECORDS] =
        [const { AtomicPtr::new(ptr::null_mut()) }; MAX_RECORDS];
    static ROLES: [AtomicU8; MAX_RECORDS] = [const { AtomicU8::new(0) }; MAX_RECORDS];
    static LIVE: [AtomicUsize; 6] = [const { AtomicUsize::new(0) }; 6];
    static REQUESTED: [AtomicUsize; 6] = [const { AtomicUsize::new(0) }; 6];
    static ALLOCATIONS: [AtomicUsize; 6] = [const { AtomicUsize::new(0) }; 6];
    static REALLOC: [AtomicUsize; 6] = [const { AtomicUsize::new(0) }; 6];
    static MAX_REQUEST: [AtomicUsize; 6] = [const { AtomicUsize::new(0) }; 6];
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
        unsafe fn realloc(&self, old: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            // SAFETY: Delegate the unchanged realloc contract to the real System
            // allocator. A failed realloc retains the original allocation/record.
            let new = unsafe { System.realloc(old, layout, size) };
            if new.is_null() {
                return new;
            }
            let mode = MODE.try_with(Cell::get).unwrap_or(0);
            if mode != 0 {
                REALLOC[mode].fetch_add(1, Ordering::AcqRel);
                REQUESTED[mode].fetch_add(size, Ordering::AcqRel);
                MAX_REQUEST[mode].fetch_max(size, Ordering::AcqRel);
            }
            for pointer in &POINTERS {
                if pointer
                    .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return new;
                }
            }
            if mode != 0 {
                for (index, pointer) in POINTERS.iter().enumerate() {
                    if pointer
                        .compare_exchange(ptr::null_mut(), new, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        ROLES[index].store(mode as u8, Ordering::Release);
                        LIVE[mode].fetch_add(1, Ordering::AcqRel);
                        return new;
                    }
                }
                OVERFLOW.store(true, Ordering::Release);
            }
            new
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
        for role in 1..6 {
            assert_eq!(
                LIVE[role].load(Ordering::Acquire),
                0,
                "prior backing still live"
            );
            REQUESTED[role].store(0, Ordering::Release);
            ALLOCATIONS[role].store(0, Ordering::Release);
            REALLOC[role].store(0, Ordering::Release);
            MAX_REQUEST[role].store(0, Ordering::Release);
        }
    }
    struct ExitCredit {
        role: usize,
        carrier: usize,
        exits: Arc<AtomicUsize>,
    }
    impl Drop for ExitCredit {
        fn drop(&mut self) {
            assert_eq!(
                LIVE[self.role].load(Ordering::Acquire),
                0,
                "original input backing must physically deallocate before its credit exits"
            );
            assert_eq!(
                LIVE[self.carrier].load(Ordering::Acquire),
                0,
                "actual Bytes owner wrapper must deallocate before original credit exits"
            );
            assert_eq!(self.exits.fetch_add(1, Ordering::AcqRel), 0);
        }
    }
    fn funded(value: &[u8], role: usize, carrier: usize) -> (Bytes, Arc<AtomicUsize>) {
        let exits = Arc::new(AtomicUsize::new(0));
        let input = measured(role, || value.to_vec());
        let bytes = measured(carrier, || {
            Bytes::from_owner_with_exit_guard(
                input,
                ExitCredit {
                    role,
                    carrier,
                    exits: exits.clone(),
                },
            )
        });
        assert_eq!(ALLOCATIONS[role].load(Ordering::Acquire), 1);
        assert_eq!(ALLOCATIONS[carrier].load(Ordering::Acquire), 1);
        assert_eq!(
            MAX_REQUEST[carrier].load(Ordering::Acquire),
            Bytes::owner_with_exit_guard_metadata_size::<Vec<u8>, ExitCredit>()
        );
        (bytes, exits)
    }
    fn count() -> usize {
        ALLOCATIONS[ACTION].load(Ordering::Acquire) + REALLOC[ACTION].load(Ordering::Acquire)
    }
    fn stats() -> (usize, usize, usize) {
        (
            ALLOCATIONS[ACTION].load(Ordering::Acquire),
            REALLOC[ACTION].load(Ordering::Acquire),
            REQUESTED[ACTION].load(Ordering::Acquire),
        )
    }
    fn integer(value: usize, prefix: u8, flag: u8, wire: &mut Vec<u8>) {
        let mask = (1usize << prefix) - 1;
        if value < mask {
            wire.push(flag | value as u8);
            return;
        }
        wire.push(flag | mask as u8);
        let mut remainder = value - mask;
        while remainder >= 128 {
            wire.push((remainder as u8 & 127) | 128);
            remainder >>= 7;
        }
        wire.push(remainder as u8);
    }
    fn string(value: &[u8], huffman: bool, wire: &mut Vec<u8>) {
        let encoded = if huffman {
            M07HpackDecoder::huffman(value)
        } else {
            value.to_vec()
        };
        integer(encoded.len(), 7, if huffman { 128 } else { 0 }, wire);
        wire.extend_from_slice(&encoded);
    }
    fn literal(
        name: &[u8],
        value: &[u8],
        index: bool,
        huff_name: bool,
        huff_value: bool,
    ) -> Vec<u8> {
        let mut wire = vec![if index { 0x40 } else { 0 }];
        string(name, huff_name, &mut wire);
        string(value, huff_value, &mut wire);
        wire
    }
    fn mixed_wire() -> Vec<u8> {
        let mut wire = vec![0x82];
        wire.extend(literal(b"foo", b"huffman value", true, true, true));
        wire.extend(literal(b"bar", &[b'x'; 128], true, false, false));
        wire
    }

    #[test]
    fn shared_custom_name_and_value_preserve_exact_original_owners_through_clone() {
        reset();
        let (name, ne) = funded(b"custom-field", RAW, CARRIER);
        let (value, ve) = funded(b"original value", FRAME, VALUE_CARRIER);
        let np = name.as_ptr();
        let vp = value.as_ptr();
        let header = measured(ACTION, || M07HpackField::shared(name, value).unwrap());
        assert_eq!(
            header.name_pointer(),
            np,
            "shared custom name must retain original pointer"
        );
        assert_eq!(
            header.value_pointer(),
            vp,
            "shared value must retain original pointer"
        );
        let last = measured(ACTION, || header.clone());
        assert_eq!(
            count(),
            0,
            "shared field construction and clone must not copy backing"
        );
        drop(header);
        assert_eq!(ne.load(Ordering::Acquire), 0);
        assert_eq!(ve.load(Ordering::Acquire), 0);
        drop(last);
        assert_eq!(ne.load(Ordering::Acquire), 1);
        assert_eq!(ve.load(Ordering::Acquire), 1);
    }
    #[test]
    fn legacy_constructor_positive_control_copies_and_releases_original_owner() {
        reset();
        let (name, ne) = funded(b"custom-field", RAW, CARRIER);
        let (value, ve) = funded(b"original value", FRAME, VALUE_CARRIER);
        let np = name.as_ptr();
        let vp = value.as_ptr();
        let header = measured(ACTION, || {
            M07HpackField::legacy(name.clone(), value.clone()).unwrap()
        });
        assert_ne!(header.name_pointer(), np);
        assert_ne!(header.value_pointer(), vp);
        assert_eq!(
            count(),
            2,
            "legacy regular name and value copying control must be observed"
        );
        drop(name);
        drop(value);
        assert_eq!(ne.load(Ordering::Acquire), 1);
        assert_eq!(ve.load(Ordering::Acquire), 1);
        assert_eq!(header.value_slice(), b"original value");
        drop(header);
    }
    #[test]
    fn indexed_regular_name_replacement_retains_name_and_new_value_original_owners() {
        reset();
        let (name, ne) = funded(b"custom-field", RAW, CARRIER);
        let original = M07HpackField::shared(name, Bytes::from_static(b"old")).unwrap();
        let (value, ve) = funded(b"replacement value", FRAME, VALUE_CARRIER);
        let np = original.name_pointer();
        let vp = value.as_ptr();
        let replaced = measured(ACTION, || original.replace_shared(value).unwrap());
        assert_eq!(replaced.name_pointer(), np);
        assert_eq!(replaced.value_pointer(), vp);
        let last = measured(ACTION, || replaced.clone());
        assert_eq!(count(), 0);
        drop(original);
        drop(replaced);
        assert_eq!(ne.load(Ordering::Acquire), 0);
        assert_eq!(ve.load(Ordering::Acquire), 0);
        drop(last);
        assert_eq!(ne.load(Ordering::Acquire), 1);
        assert_eq!(ve.load(Ordering::Acquire), 1);
    }
    #[test]
    fn actual_dynamic_table_and_indexed_output_retain_original_owners_past_decoder_drop() {
        reset();
        let (name, ne) = funded(b"custom-field", RAW, CARRIER);
        let (value, ve) = funded(b"original value", FRAME, VALUE_CARRIER);
        let np = name.as_ptr();
        let vp = value.as_ptr();
        let mut decoder = M07HpackDecoder::new();
        decoder.retain_funded(M07HpackField::shared(name, value).unwrap());
        let (out, result) = decoder.borrowed(&[0xbe], &mut 0);
        result.unwrap();
        assert_eq!(out[0].name_pointer(), np);
        assert_eq!(out[0].value_pointer(), vp);
        let last = out[0].clone();
        drop(out);
        drop(decoder);
        assert_eq!(ne.load(Ordering::Acquire), 0);
        assert_eq!(ve.load(Ordering::Acquire), 0);
        drop(last);
        assert_eq!(ne.load(Ordering::Acquire), 1);
        assert_eq!(ve.load(Ordering::Acquire), 1);
    }
    #[test]
    fn standard_name_is_static_but_value_keeps_original_owner() {
        reset();
        let (name, ne) = funded(b"content-type", RAW, CARRIER);
        let (value, ve) = funded(b"original value", FRAME, VALUE_CARRIER);
        let header = measured(ACTION, || M07HpackField::shared(name, value).unwrap());
        assert_eq!(count(), 0);
        assert_eq!(ne.load(Ordering::Acquire), 1);
        assert_eq!(ve.load(Ordering::Acquire), 0);
        drop(header);
        assert_eq!(ve.load(Ordering::Acquire), 1);
    }
    #[test]
    fn lowercase_owned_http_constructor_validation_matches_all_bytes_and_length_edges() {
        for byte in 0..=255u8 {
            let raw = [b'x', byte];
            let old = http::header::HeaderName::from_lowercase(&raw);
            let new = http::header::HeaderName::from_lowercase_bytes(Bytes::copy_from_slice(&raw));
            assert_eq!(
                old.map_err(|_| ()),
                new.map_err(|_| ()),
                "validation parity byte={byte}"
            );
        }
        for length in [0, 1, 255, 256, 65535, 65536] {
            let raw = vec![b'x'; length];
            assert_eq!(
                http::header::HeaderName::from_lowercase(&raw).map_err(|_| ()),
                http::header::HeaderName::from_lowercase_bytes(Bytes::copy_from_slice(&raw))
                    .map_err(|_| ()),
                "length={length}"
            );
        }
    }
    #[test]
    fn shared_header_errors_and_pseudo_conversions_match_legacy() {
        for name in [
            b"".as_slice(),
            b"X-foo",
            b"x:foo",
            b":path",
            b":authority",
            b":method",
            b":scheme",
            b":protocol",
            b":status",
            b":unknown",
        ] {
            for value in [b"".as_slice(), b"original", b"200", b"\r", b"\xff"] {
                assert_eq!(
                    M07HpackField::shared(
                        Bytes::copy_from_slice(name),
                        Bytes::copy_from_slice(value)
                    ),
                    M07HpackField::legacy(
                        Bytes::copy_from_slice(name),
                        Bytes::copy_from_slice(value)
                    ),
                    "name={name:?} value={value:?}"
                );
            }
        }
        reset();
        let (name, ne) = funded(b"INVALID", RAW, CARRIER);
        let (value, ve) = funded(b"original value", FRAME, VALUE_CARRIER);
        assert!(M07HpackField::shared(name, value).is_err());
        assert_eq!(ne.load(Ordering::Acquire), 1);
        assert_eq!(ve.load(Ordering::Acquire), 1);
    }
    #[test]
    fn actual_borrowed_literal_shared_dispatch_allocates_only_compact_outputs() {
        for huff in [false, true] {
            reset();
            let wire = literal(b"custom-field", b"original value", false, huff, huff);
            let mut decoder = M07HpackDecoder::new();
            decoder
                .discard_borrowed(
                    &wire,
                    || {
                        MODE.with(|m| m.set(ACTION));
                    },
                    || {
                        MODE.with(|m| m.set(0));
                    },
                )
                .unwrap();
            eprintln!("borrowed huffman={huff}: {:?}", stats());
            assert_eq!(
                count(),
                2,
                "actual BorrowedSource must use shared compact name and value without recopy"
            );
            assert_eq!(LIVE[ACTION].load(Ordering::Acquire), 0);
            reset();
            let mut decoder = M07HpackDecoder::new();
            decoder
                .discard_owned(
                    &wire,
                    || {
                        MODE.with(|m| m.set(ACTION));
                    },
                    || {
                        MODE.with(|m| m.set(0));
                    },
                )
                .unwrap();
            eprintln!("owned huffman={huff}: {:?}", stats());
            assert!(
                count() > 2,
                "owned default positive copying control must be observed"
            );
            drop(decoder);
            assert_eq!(LIVE[ACTION].load(Ordering::Acquire), 0);
        }
    }
    #[test]
    fn actual_borrowed_indexed_name_dispatch_has_one_new_value_backing() {
        reset();
        let mut decoder = M07HpackDecoder::new();
        decoder.retain_funded(
            M07HpackField::shared(
                Bytes::from_static(b"custom-field"),
                Bytes::from_static(b"old"),
            )
            .unwrap(),
        );
        // Literal without indexing, name index62, then a new value.
        let mut wire = Vec::new();
        integer(62, 4, 0, &mut wire);
        string(b"new value", false, &mut wire);
        decoder
            .discard_borrowed(
                &wire,
                || {
                    MODE.with(|m| m.set(ACTION));
                },
                || {
                    MODE.with(|m| m.set(0));
                },
            )
            .unwrap();
        assert_eq!(
            count(),
            1,
            "indexed regular name must retain the only compact value backing"
        );
        assert_eq!(LIVE[ACTION].load(Ordering::Acquire), 0);
        drop(decoder);
    }
    #[test]
    fn mixed_plain_huffman_every_cut_matches_default_output_errors_and_table() {
        let wire = mixed_wire();
        for cut in 0..=wire.len() {
            let mut borrowed = M07HpackDecoder::new();
            let mut owned = M07HpackDecoder::new();
            let mut commit = 0;
            let mut raw = BytesMut::from(&wire[..cut]);
            let (mut out, a) = borrowed.borrowed(&wire[..cut], &mut commit);
            let (mut expected, b) = owned.owned(&mut raw);
            assert_eq!(a, b);
            assert_eq!(out, expected);
            assert_eq!(commit, cut - raw.len());
            raw.extend_from_slice(&wire[cut..]);
            let (tail, a) = borrowed.borrowed(&wire, &mut commit);
            let (old, b) = owned.owned(&mut raw);
            assert_eq!(a, b);
            out.extend(tail);
            expected.extend(old);
            assert_eq!(out, expected);
            assert_eq!(borrowed.table_state(), owned.table_state());
            assert_eq!(commit, wire.len());
            let (indexed, a) = borrowed.borrowed(&[0xbe, 0xbf], &mut 0);
            let (mutraw, b) = {
                let mut raw = BytesMut::from(&[0xbe, 0xbf][..]);
                owned.owned(&mut raw)
            };
            assert_eq!(a, b);
            assert_eq!(indexed, mutraw);
        }
    }
}
