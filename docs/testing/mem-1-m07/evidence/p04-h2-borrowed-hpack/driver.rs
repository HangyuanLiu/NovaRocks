#[cfg(test)]
mod probe {
    use bytes::BytesMut;
    use h2::{M07HpackDecoder, M07HpackError};
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
    fn every_byte_split_matches_owned_output_table_and_commit() {
        let wire = mixed_wire();
        for cut in 0..=wire.len() {
            let mut borrowed = M07HpackDecoder::new();
            let mut owned = M07HpackDecoder::new();
            let mut commit = 0;
            let mut raw = BytesMut::from(&wire[..cut]);
            let (mut got, error) = borrowed.borrowed(&wire[..cut], &mut commit);
            let (mut expected, old_error) = owned.owned(&mut raw);
            assert_eq!(error, old_error, "first error at cut={cut}");
            assert_eq!(got, expected, "first output at cut={cut}");
            assert_eq!(
                commit,
                cut - raw.len(),
                "only complete representation commits at cut={cut}"
            );
            assert_eq!(borrowed.table_state(), owned.table_state());
            raw.extend_from_slice(&wire[cut..]);
            let (tail, error) = borrowed.borrowed(&wire, &mut commit);
            got.extend(tail);
            let (tail, old_error) = owned.owned(&mut raw);
            expected.extend(tail);
            assert_eq!(error, old_error);
            assert_eq!(error, Ok(()));
            assert_eq!(got, expected, "final output at cut={cut}");
            assert_eq!(got.len(), 3);
            assert_eq!(
                commit,
                wire.len(),
                "complete decoder must commit entire full input"
            );
            assert!(raw.is_empty());
            assert_eq!(borrowed.table_state(), owned.table_state());
        }
    }
    #[test]
    fn every_single_byte_retry_keeps_dynamic_table_and_output_exact() {
        let wire = mixed_wire();
        let mut cumulative = Vec::with_capacity(wire.len());
        let mut borrowed = M07HpackDecoder::new();
        let mut owned = M07HpackDecoder::new();
        let mut commit = 0;
        let mut raw = BytesMut::new();
        let mut got = Vec::new();
        let mut expected = Vec::new();
        for byte in wire.iter().copied() {
            cumulative.push(byte);
            raw.extend_from_slice(&[byte]);
            let (fields, error) = borrowed.borrowed(&cumulative, &mut commit);
            got.extend(fields);
            let (fields, old_error) = owned.owned(&mut raw);
            expected.extend(fields);
            assert_eq!(error, old_error);
            assert_eq!(got, expected);
            assert_eq!(commit, cumulative.len() - raw.len());
            assert_eq!(borrowed.table_state(), owned.table_state());
        }
        assert_eq!(got.len(), 3);
        assert_eq!(
            commit,
            wire.len(),
            "complete decoder must commit entire full input"
        );
    }
    #[test]
    fn huffman_name_complete_value_need_more_retries_whole_literal() {
        let mut wire = vec![0x82];
        wire.extend(literal(b"foo", b"long value", true, true, true));
        let mut decoder = M07HpackDecoder::new();
        let mut commit = 0;
        let (fields, error) = decoder.borrowed(&wire[..wire.len() - 1], &mut commit);
        assert!(error.unwrap_err().is_need_more());
        assert_eq!(fields.len(), 1);
        assert_eq!(
            commit, 1,
            "incomplete literal must not commit name or advanced value cursor"
        );
        assert_eq!(decoder.table_state(), (0, 0, 4096));
        let (fields, result) = decoder.borrowed(&wire, &mut commit);
        assert_eq!(result, Ok(()));
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].value_slice(), b"long value");
        assert_eq!(
            commit,
            wire.len(),
            "complete decoder must commit entire full input"
        );
        assert_eq!(decoder.table_state().1, 1);
        let (indexed, result) = decoder.borrowed(&[0xbe], &mut 0);
        assert_eq!(result, Ok(()));
        assert_eq!(indexed, fields);
    }
    #[test]
    fn dynamic_indices_62_and_63_cross_blocks_match_owned() {
        let mut borrowed = M07HpackDecoder::new();
        let mut owned = M07HpackDecoder::new();
        let blocks = [
            literal(b"foo", b"first", true, true, true),
            literal(b"bar", b"second", true, false, false),
            vec![0xbe, 0xbf],
        ];
        for block in blocks {
            let (got, error) = borrowed.borrowed(&block, &mut 0);
            let (expected, old_error) = owned.owned(&mut BytesMut::from(block.as_slice()));
            assert_eq!(error, old_error);
            assert_eq!(got, expected);
            assert_eq!(borrowed.table_state(), owned.table_state());
            if block == [0xbe, 0xbf] {
                assert_eq!(got[0].value_slice(), b"second");
                assert_eq!(got[1].value_slice(), b"first");
            }
        }
    }
    #[test]
    fn pseudo_and_header_bytes_outlive_workspace_overwrite_and_decoder_drop() {
        let mut workspace = literal(b":authority", b"example.com", false, false, false);
        workspace.extend(literal(b":path", b"/long/path", false, true, true));
        workspace.extend(literal(b":scheme", b"https", false, false, false));
        workspace.extend(literal(b"foo", b"preserved", true, true, true));
        let mut decoder = M07HpackDecoder::new();
        let (fields, result) = decoder.borrowed(&workspace, &mut 0);
        assert_eq!(result, Ok(()));
        assert_eq!(fields.len(), 4);
        let expected = [
            b"example.com".as_slice(),
            b"/long/path",
            b"https",
            b"preserved",
        ];
        workspace.fill(0);
        drop(workspace);
        let (indexed, result) = decoder.borrowed(&[0xbe], &mut 0);
        assert_eq!(result, Ok(()));
        assert_eq!(indexed[0], fields[3]);
        drop(decoder);
        for (field, expected) in fields.iter().zip(expected) {
            assert_eq!(field.value_slice(), expected);
        }
    }
    #[test]
    fn malformed_input_error_classes_match_actual_owned() {
        let cases = [
            vec![0x80],
            vec![0xff, 0xff, 0xff, 0xff, 0xff],
            vec![0, 1, b'X', 1, b'y'],
            vec![0, 1, b'x', 0x81, 0xff],
            vec![0, 1, b'x', 127, 255, 255, 255, 255],
        ];
        for wire in cases {
            let mut borrowed = M07HpackDecoder::new();
            let mut owned = M07HpackDecoder::new();
            let (got, error) = borrowed.borrowed(&wire, &mut 0);
            let (expected, old_error) = owned.owned(&mut BytesMut::from(wire.as_slice()));
            assert!(error.is_err());
            assert_eq!(error, old_error);
            assert_eq!(got, expected);
            assert_eq!(borrowed.table_state(), owned.table_state());
        }
    }
    #[test]
    fn per_call_can_resize_timing_matches_existing_owned_semantics() {
        let mut borrowed = M07HpackDecoder::new();
        let mut owned = M07HpackDecoder::new();
        let (_, got) = borrowed.borrowed(&[0x82, 0x20], &mut 0);
        let (_, expected) = owned.owned(&mut BytesMut::from(&[0x82, 0x20][..]));
        assert_eq!(got, Err(M07HpackError::InvalidMaxDynamicSize));
        assert_eq!(got, expected);
        let mut borrowed = M07HpackDecoder::new();
        let mut owned = M07HpackDecoder::new();
        let mut commit = 0;
        let mut raw = BytesMut::from(&[0x82][..]);
        assert_eq!(borrowed.borrowed(&[0x82], &mut commit).1, Ok(()));
        assert_eq!(owned.owned(&mut raw).1, Ok(()));
        raw.extend_from_slice(&[0x20]);
        assert_eq!(borrowed.borrowed(&[0x82, 0x20], &mut commit).1, Ok(()));
        assert_eq!(owned.owned(&mut raw).1, Ok(()));
        assert_eq!(borrowed.table_state(), (0, 0, 0));
        assert_eq!(borrowed.table_state(), owned.table_state());
    }
    #[test]
    fn borrowed_static_input_zero_allocation_and_owned_copy_positive_control() {
        let wire = [0x82; 16];
        let mut borrowed = M07HpackDecoder::new();
        let mut owned = M07HpackDecoder::new();
        assert_eq!(borrowed.discard_borrowed(&wire, begin, end), Ok(()));
        let observed = (
            ALLOC.load(Ordering::Relaxed),
            REALLOC.load(Ordering::Relaxed),
            BYTES.load(Ordering::Relaxed),
        );
        println!("actual static borrowed allocation={observed:?}");
        assert_eq!(observed, (0, 0, 0));
        assert_eq!(owned.discard_owned(&wire, begin, end), Ok(()));
        let observed = (
            ALLOC.load(Ordering::Relaxed),
            REALLOC.load(Ordering::Relaxed),
            BYTES.load(Ordering::Relaxed),
        );
        println!("actual static Owned copy allocation={observed:?}");
        assert!(observed.0 > 0 && observed.2 >= wire.len());
    }
    #[test]
    fn compact_plain_and_huffman_output_allocations_are_explicitly_separate() {
        for huffman in [false, true] {
            let wire = literal(b"foo", b"decoded output", false, huffman, huffman);
            let mut decoder = M07HpackDecoder::new();
            assert_eq!(decoder.discard_borrowed(&wire, begin, end), Ok(()));
            let observed = (
                ALLOC.load(Ordering::Relaxed),
                REALLOC.load(Ordering::Relaxed),
                BYTES.load(Ordering::Relaxed),
            );
            println!("actual compact output huffman={huffman} allocation={observed:?}");
            assert!(
                observed.0 > 0,
                "decoded owners remain independent allocation obligations"
            );
        }
    }
}
