use crate::hpack::{Decoder, DecoderError};
use bytes::BytesMut;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Allocator;
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static MAX_ALLOCATION: Cell<usize> = const { Cell::new(0) };
}
static TARGET: AtomicUsize = AtomicUsize::new(0);
static TARGET_FREED: AtomicBool = AtomicBool::new(false);
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        TRACK.with(|track| {
            if track.get() {
                ALLOCATIONS.with(|n| n.set(n.get() + 1));
                MAX_ALLOCATION.with(|n| n.set(n.get().max(layout.size())));
            }
        });
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        if ptr as usize == TARGET.load(Ordering::Relaxed) {
            TARGET_FREED.store(true, Ordering::Relaxed);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        TRACK.with(|track| {
            if track.get() {
                ALLOCATIONS.with(|n| n.set(n.get() + 1));
                MAX_ALLOCATION.with(|n| n.set(n.get().max(new_size)));
            }
        });
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

fn length(buf: &mut Vec<u8>, n: usize, huffman: bool) {
    let flag = if huffman { 128 } else { 0 };
    if n < 127 {
        buf.push(flag | n as u8);
        return;
    }
    buf.push(flag | 127);
    let mut rest = n - 127;
    while rest >= 128 {
        buf.push((rest as u8 & 127) | 128);
        rest >>= 7;
    }
    buf.push(rest as u8);
}
fn bounded() -> Decoder {
    let mut de = Decoder::new(4096);
    de.set_max_field_size(256, 16384);
    // Initialize any lazy tracing before allocation observations.
    de.decode(&mut Cursor::new(&mut BytesMut::new()), |_| ())
        .unwrap();
    de
}
fn tracked_decode(
    de: &mut Decoder,
    buf: &mut BytesMut,
) -> (Result<(), DecoderError>, usize, usize) {
    ALLOCATIONS.with(|n| n.set(0));
    MAX_ALLOCATION.with(|n| n.set(0));
    TRACK.with(|n| n.set(true));
    let res = de.decode(&mut Cursor::new(buf), |_| ());
    TRACK.with(|n| n.set(false));
    (
        res,
        ALLOCATIONS.with(Cell::get),
        MAX_ALLOCATION.with(Cell::get),
    )
}
fn huffman(value: &[u8]) -> Vec<u8> {
    let mut encoded = BytesMut::new();
    crate::hpack::huffman::encode(value, &mut encoded);
    encoded.to_vec()
}

#[test]
fn bounded_declared_plain_or_huffman_string_refused_without_allocation_or_body() {
    for huff in [false, true] {
        let mut wire = vec![0x00, 1, b'x'];
        length(&mut wire, 1 << 20, huff);
        let mut buf = BytesMut::from(wire.as_slice());
        let (res, allocations, _) = tracked_decode(&mut bounded(), &mut buf);
        assert_eq!(res, Err(DecoderError::HeaderFieldTooLarge));
        assert_eq!(
            allocations, 0,
            "declared over-limit string must not allocate"
        );
    }
}
#[test]
fn bounded_combined_plain_field_refused_before_http_copy_or_table_insert() {
    let mut wire = vec![0x40];
    length(&mut wire, 128, false);
    wire.extend([b'x'; 128]);
    length(&mut wire, 128, false);
    wire.extend([b'y'; 128]);
    let mut buf = BytesMut::from(wire.as_slice());
    let mut de = bounded();
    let (res, allocations, _) = tracked_decode(&mut de, &mut buf);
    assert_eq!(res, Err(DecoderError::HeaderFieldTooLarge));
    assert_eq!(allocations, 0);
    // A refused field has not acquired a dynamic-table index.
    let mut indexed = BytesMut::from(&b"\xbe"[..]);
    assert_eq!(
        de.decode(&mut Cursor::new(&mut indexed), |_| ()),
        Err(DecoderError::InvalidTableIndex)
    );
}
#[test]
fn bounded_indexed_name_checks_combined_field_before_value_copy() {
    let mut wire = vec![0x40];
    length(&mut wire, 220, false);
    wire.extend([b'x'; 220]);
    wire.push(0);
    let mut de = bounded();
    de.decode(
        &mut Cursor::new(&mut BytesMut::from(wire.as_slice())),
        |_| (),
    )
    .unwrap();
    let mut buf = BytesMut::from(&b"\x7e\x05abcde"[..]);
    let (res, allocations, _) = tracked_decode(&mut de, &mut buf);
    assert_eq!(res, Err(DecoderError::HeaderFieldTooLarge));
    assert_eq!(allocations, 0);
}
#[test]
fn bounded_huffman_overflow_and_invalid_input_are_checked_before_output_reserve() {
    for encoded in [huffman(&vec![b'a'; 4096]), vec![255; 4]] {
        let mut wire = vec![0x00, 1, b'x'];
        length(&mut wire, encoded.len(), true);
        wire.extend_from_slice(&encoded);
        let mut buf = BytesMut::from(wire.as_slice());
        let (res, allocations, _) = tracked_decode(&mut bounded(), &mut buf);
        let expected = if encoded.len() == 4 {
            DecoderError::InvalidHuffmanCode
        } else {
            DecoderError::HeaderFieldTooLarge
        };
        assert_eq!(res, Err(expected));
        assert_eq!(allocations, 0);
    }
}
#[test]
fn bounded_huffman_accepts_encoded_expansion_with_exact_output_capacity() {
    // '#' takes 12 bits: compressed bytes may exceed the decoded field budget.
    let encoded = huffman(&[b'#'; 180]);
    assert!(encoded.len() > 224);
    let mut wire = vec![0x00, 1, b'x'];
    length(&mut wire, encoded.len(), true);
    wire.extend_from_slice(&encoded);
    let mut de = bounded();
    let mut headers = Vec::with_capacity(1);
    de.decode(
        &mut Cursor::new(&mut BytesMut::from(wire.as_slice())),
        |h| headers.push(h),
    )
    .unwrap();
    assert_eq!(headers[0].value_slice(), &[b'#'; 180]);
    let exact = crate::hpack::huffman::decode_bounded(&encoded, 180).unwrap();
    assert_eq!(exact.len(), 180);
    assert_eq!(exact.try_into_mut().unwrap().capacity(), 180);
}
#[test]
fn bounded_plain_pseudo_and_dynamic_table_do_not_pin_original_raw_backing() {
    for strict in [true, false] {
        let mut de = if strict {
            bounded()
        } else {
            Decoder::new(4096)
        };
        let mut buf = BytesMut::with_capacity(65536);
        buf.extend_from_slice(b"\x44\x06/hello"); // indexed :path name, insert table entry
        let start = buf.as_ptr() as usize;
        TARGET.store(start, Ordering::Relaxed);
        TARGET_FREED.store(false, Ordering::Relaxed);
        let mut heads = Vec::with_capacity(2);
        de.decode(&mut Cursor::new(&mut buf), |h| heads.push(h))
            .unwrap();
        let value_ptr = heads[0].value_slice().as_ptr() as usize;
        assert_eq!(value_ptr >= start && value_ptr < start + 65536, !strict);
        drop(buf);
        assert_eq!(
            TARGET_FREED.load(Ordering::Relaxed),
            strict,
            "table/head aliases may not retain raw backing in bounded mode"
        );
        let mut indexed = BytesMut::from(&b"\xbe"[..]);
        de.decode(&mut Cursor::new(&mut indexed), |h| heads.push(h))
            .unwrap();
        assert_eq!(heads[1].value_slice(), b"/hello");
        drop(de);
        if !strict {
            assert!(!TARGET_FREED.load(Ordering::Relaxed));
        }
        drop(heads);
        assert!(TARGET_FREED.load(Ordering::Relaxed));
        TARGET.store(0, Ordering::Relaxed);
    }
}

#[test]
fn bounded_combined_huffman_field_has_only_bounded_temporary_outputs() {
    let name = huffman(&[b'x'; 128]);
    let value = huffman(&[b'y'; 128]);
    let mut wire = vec![0x40];
    length(&mut wire, name.len(), true);
    wire.extend(name);
    length(&mut wire, value.len(), true);
    wire.extend(value);
    let mut buf = BytesMut::from(wire.as_slice());
    let mut de = bounded();
    let (res, allocations, max_allocation) = tracked_decode(&mut de, &mut buf);
    assert_eq!(res, Err(DecoderError::HeaderFieldTooLarge));
    assert!(
        allocations >= 2,
        "both validated Huffman markers may allocate before combined check"
    );
    assert!(
        max_allocation <= 128,
        "temporary output capacity comes from validated output length"
    );
    let mut indexed = BytesMut::from(&b"\xbe"[..]);
    assert_eq!(
        de.decode(&mut Cursor::new(&mut indexed), |_| ()),
        Err(DecoderError::InvalidTableIndex)
    );
}
