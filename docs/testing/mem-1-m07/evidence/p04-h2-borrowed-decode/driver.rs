#[cfg(test)]
mod probe {
    use bytes::Bytes;
    use h2::{M07BorrowedDecoder, M07DecodedSummary, ReceiveBufferPool};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};

    thread_local! { static TRACK: Cell<bool> = const { Cell::new(false) }; }
    static ALLOC: AtomicUsize = AtomicUsize::new(0);
    static REALLOC: AtomicUsize = AtomicUsize::new(0);
    static BYTES: AtomicUsize = AtomicUsize::new(0);
    struct Tracked;
    fn count(counter: &AtomicUsize, size: usize) {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            counter.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(size, Ordering::Relaxed);
        }
    }
    // SAFETY: Forward every System pointer/Layout unchanged. Counters/TLS do
    // not allocate. The actual dependency decoder controls tracking callbacks.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            count(&ALLOC, layout.size());
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            count(&ALLOC, layout.size());
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            count(&REALLOC, size);
            unsafe { System.realloc(pointer, layout, size) }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Tracked = Tracked;
    fn begin() {
        ALLOC.store(0, Ordering::Relaxed);
        REALLOC.store(0, Ordering::Relaxed);
        BYTES.store(0, Ordering::Relaxed);
        TRACK.with(|value| value.set(true));
    }
    fn end() {
        TRACK.with(|value| value.set(false));
    }
    fn decode(
        decoder: &mut M07BorrowedDecoder,
        wire: &[u8],
        owned: bool,
    ) -> (M07DecodedSummary, (usize, usize, usize)) {
        let summary = decoder.decode(wire, owned, begin, end);
        let observed = (
            ALLOC.load(Ordering::Relaxed),
            REALLOC.load(Ordering::Relaxed),
            BYTES.load(Ordering::Relaxed),
        );
        println!(
            "actual decode owned={owned} alloc={}, realloc={}, requested={}B",
            observed.0, observed.1, observed.2
        );
        (summary, observed)
    }
    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let mut wire = Vec::with_capacity(payload.len() + 9);
        wire.extend_from_slice(&[
            (payload.len() >> 16) as u8,
            (payload.len() >> 8) as u8,
            payload.len() as u8,
            kind,
            flags,
        ]);
        wire.extend_from_slice(&stream.to_be_bytes());
        wire.extend_from_slice(payload);
        wire
    }
    fn pool() -> ReceiveBufferPool {
        ReceiveBufferPool::new(1, 16384, Bytes::new()).unwrap()
    }
    fn wrapper() -> (usize, usize, usize) {
        (1, 0, h2::m07_data_wrapper_metadata_size())
    }
    #[test]
    fn borrowed_controls_have_zero_allocations_and_owned_frame_is_positive_control() {
        let cases = [
            frame(4, 0, 0, &[0, 1, 0, 0, 16, 0]),
            frame(6, 1, 0, b"12345678"),
            frame(8, 0, 1, &[0, 0, 0, 7]),
            frame(3, 0, 1, &[0, 0, 0, 8]),
            frame(2, 0, 1, &[0, 0, 0, 3, 15]),
            frame(0xff, 0xff, 1, &[42; 4096]),
        ];
        for wire in cases {
            let (borrowed, observed) =
                decode(&mut M07BorrowedDecoder::new(None, None), &wire, false);
            assert_eq!(
                observed,
                (0, 0, 0),
                "borrowed controls must not allocate an owned raw frame"
            );
            assert_eq!(borrowed.error_class, None);
            let (owned, observed) = decode(&mut M07BorrowedDecoder::new(None, None), &wire, true);
            assert!(
                observed.0 > 0 && observed.2 >= wire.len(),
                "actual Owned decoder positive control must allocate the complete frame"
            );
            assert_eq!(owned, borrowed);
        }
    }
    #[test]
    fn data_has_only_exact_actual_wrapper_allocation_and_alias_outlives_raw() {
        let pool = pool();
        let mut decoder = M07BorrowedDecoder::new(Some(pool.clone()), None);
        let mut wire = frame(0, 1, 1, b"body");
        let (summary, observed) = decode(&mut decoder, &wire, false);
        assert_eq!(observed, wrapper());
        assert_eq!(summary.kind, Some(0));
        assert_eq!(summary.stream, 1);
        assert!(summary.end_stream);
        assert_eq!(summary.body.as_ref().unwrap(), "body");
        assert_eq!(pool.available_buffers(), 0);
        let alias = summary.body.as_ref().unwrap().clone();
        wire.fill(0);
        drop(summary);
        drop(decoder);
        assert_eq!(alias, "body");
        assert_eq!(pool.available_buffers(), 0);
        drop(alias);
        assert_eq!(pool.available_buffers(), 1);
    }
    #[test]
    fn padded_data_some_zero_and_nonzero_padding_preserve_flags_and_body() {
        for (payload, expected, padding) in [
            (&[0][..], &b""[..], 0),
            (&[2, b'X', 0xff, 0xaa][..], &b"X"[..], 2),
        ] {
            let pool = pool();
            let mut decoder = M07BorrowedDecoder::new(Some(pool.clone()), None);
            let wire = frame(0, 9, 1, payload);
            let (summary, observed) = decode(&mut decoder, &wire, false);
            assert_eq!(observed, wrapper());
            assert_eq!(summary.body.as_ref().unwrap().as_ref(), expected);
            assert!(summary.end_stream);
            assert!(summary.details.contains("PADDED"));
            assert!(summary.details.contains(&format!("pad_len: {padding}")));
            assert_eq!(pool.available_buffers(), 0);
            drop(summary);
            assert_eq!(pool.available_buffers(), 1);
        }
    }
    #[test]
    fn malformed_data_refuses_before_pool_checkout_or_copy() {
        let pool = pool();
        let mut decoder = M07BorrowedDecoder::new(Some(pool.clone()), None);
        for wire in [
            frame(0, 0, 0, b"body"),
            frame(0, 8, 1, &[]),
            frame(0, 8, 1, &[3, 1, 2]),
            frame(0, 8, 0, &[0]),
        ] {
            let (summary, observed) = decode(&mut decoder, &wire, false);
            assert_eq!(observed, (0, 0, 0));
            assert_eq!(summary.error_class, Some("GoAway"));
            assert_eq!(summary.reason, Some(1));
            assert_eq!(pool.available_buffers(), 1);
        }
    }
    #[test]
    fn goaway_debug_has_only_wrapper_and_survives_original_wire_drop() {
        let pool = pool();
        let mut decoder = M07BorrowedDecoder::new(None, Some(pool.clone()));
        let wire = frame(7, 0, 0, &[0, 0, 0, 3, 0, 0, 0, 2, b'D', b'B', b'G']);
        let (summary, observed) = decode(&mut decoder, &wire, false);
        assert_eq!(observed, wrapper());
        assert_eq!(summary.kind, Some(7));
        assert_eq!(summary.stream, 3);
        assert_eq!(summary.reason, Some(2));
        drop(wire);
        drop(decoder);
        assert_eq!(summary.body.as_ref().unwrap(), "DBG");
        assert_eq!(pool.available_buffers(), 0);
        drop(summary);
        assert_eq!(pool.available_buffers(), 1);
    }
    #[test]
    fn empty_or_invalid_goaway_does_not_checkout_and_full_pool_refuses() {
        let pool = pool();
        let mut decoder = M07BorrowedDecoder::new(None, Some(pool.clone()));
        for wire in [frame(7, 0, 0, &[0; 8]), frame(7, 0, 0, &[0; 7])] {
            let (summary, observed) = decode(&mut decoder, &wire, false);
            assert_eq!(observed, (0, 0, 0));
            assert_eq!(pool.available_buffers(), 1);
            drop(summary);
        }
        let wire = frame(7, 0, 0, &[0, 0, 0, 1, 0, 0, 0, 2, 42]);
        let (held, observed) = decode(&mut decoder, &wire, false);
        assert_eq!(observed, wrapper());
        let (refused, observed) = decode(&mut decoder, &wire, false);
        assert_eq!(observed, (0, 0, 0));
        assert_eq!(refused.error_class, Some("GoAway"));
        assert_eq!(refused.reason, Some(11));
        drop(held);
        assert_eq!(pool.available_buffers(), 1);
    }
    #[test]
    fn partial_headers_gate_refuses_controls_unknown_and_data_without_copy() {
        let pool = pool();
        let mut decoder = M07BorrowedDecoder::new(Some(pool.clone()), None);
        let seed = frame(1, 0, 1, &[]);
        let seeded = decoder.decode(&seed, false, || {}, || {});
        assert_eq!(seeded.kind, None);
        assert_eq!(seeded.error_class, None);
        for wire in [
            frame(6, 0, 0, b"12345678"),
            frame(0xff, 0, 1, &[42; 1024]),
            frame(0, 1, 1, b"body"),
        ] {
            let (summary, observed) = decode(&mut decoder, &wire, false);
            assert_eq!(observed, (0, 0, 0));
            assert_eq!(summary.error_class, Some("GoAway"));
            assert_eq!(summary.reason, Some(1));
            assert_eq!(pool.available_buffers(), 1);
        }
    }
    #[test]
    fn headers_and_continuation_owned_fallback_is_explicit_positive_allocation() {
        let mut decoder = M07BorrowedDecoder::new(None, None);
        let (summary, observed) = decode(&mut decoder, &frame(1, 0, 1, &[]), false);
        assert_eq!(summary.kind, None);
        assert!(
            observed.0 > 0,
            "HEADERS fallback remains independently allocated"
        );
        let (summary, observed) = decode(&mut decoder, &frame(9, 4, 1, &[]), false);
        assert_eq!(summary.kind, Some(1));
        assert_eq!(summary.error_class, None);
        assert!(
            observed.0 > 0,
            "CONT fallback remains independently allocated"
        );
    }
    #[test]
    fn invalid_control_and_priority_self_dependency_preserve_error_scope() {
        let (summary, observed) = decode(
            &mut M07BorrowedDecoder::new(None, None),
            &frame(4, 0, 0, &[0]),
            false,
        );
        assert_eq!(observed, (0, 0, 0));
        assert_eq!(summary.error_class, Some("GoAway"));
        assert_eq!(summary.reason, Some(1));
        let (summary, observed) = decode(
            &mut M07BorrowedDecoder::new(None, None),
            &frame(2, 0, 1, &[0, 0, 0, 1, 0]),
            false,
        );
        assert_eq!(observed, (0, 0, 0));
        assert_eq!(summary.error_class, Some("Reset"));
        assert_eq!(summary.stream, 1);
        assert_eq!(summary.reason, Some(1));
    }
}
