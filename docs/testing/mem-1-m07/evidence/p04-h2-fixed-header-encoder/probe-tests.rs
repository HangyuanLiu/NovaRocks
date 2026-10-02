// Appended only to an isolated copy of the actual production encoder.
#[cfg(test)]
mod fixed_header_probe {
    use super::*;
    use crate::hpack::{BytesStr, Decoder};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::io::Cursor;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicUsize, Ordering};

    thread_local! { static TRACK: Cell<bool> = const { Cell::new(false) }; }
    static ALLOC: AtomicUsize = AtomicUsize::new(0);
    static REALLOC: AtomicUsize = AtomicUsize::new(0);
    struct Tracked;
    fn tracked(counter: &AtomicUsize) {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
    // SAFETY: All pointers and Layouts pass unchanged to System. Counters do
    // not allocate. Header construction, clones, output storage and decoder
    // allocations are outside the measured encode scope.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            tracked(&ALLOC);
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            tracked(&ALLOC);
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            tracked(&REALLOC);
            unsafe { System.realloc(pointer, layout, size) }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Tracked = Tracked;
    struct Scope;
    impl Drop for Scope {
        fn drop(&mut self) {
            TRACK.with(|value| value.set(false));
        }
    }
    fn measured(f: impl FnOnce()) -> (usize, usize) {
        ALLOC.store(0, Ordering::Relaxed);
        REALLOC.store(0, Ordering::Relaxed);
        TRACK.with(|value| value.set(true));
        let scope = Scope;
        f();
        drop(scope);
        let observed = (
            ALLOC.load(Ordering::Relaxed),
            REALLOC.load(Ordering::Relaxed),
        );
        println!("actual encode alloc={}, realloc={}", observed.0, observed.1);
        observed
    }
    fn zero_encoder() -> Encoder {
        let mut encoder = Encoder::default();
        encoder.set_max_size_limit(0);
        encoder.update_max_size(u32::MAX as usize);
        encoder
    }
    fn field(name: &'static str, value: HeaderValue) -> Header<Option<HeaderName>> {
        Header::Field {
            name: Some(HeaderName::from_static(name)),
            value,
        }
    }
    fn decode(decoder: &mut Decoder, wire: &[u8]) -> Vec<Header> {
        let mut bytes = BytesMut::from(wire);
        let mut headers = Vec::new();
        decoder
            .decode(&mut Cursor::new(&mut bytes), |header| headers.push(header))
            .unwrap();
        assert!(bytes.is_empty());
        headers
    }
    fn decoded_size(headers: &[Header<Option<HeaderName>>]) -> usize {
        let mut previous_name = None;
        headers
            .iter()
            .map(|header| match header {
                Header::Field { name, value } => {
                    if let Some(name) = name {
                        previous_name = Some(name.as_str().len());
                    }
                    previous_name.unwrap() + value.len() + 32
                }
                Header::Method(value) => 7 + value.as_str().len() + 32,
                Header::Scheme(value) => 7 + value.len() + 32,
                Header::Authority(value) => 10 + value.len() + 32,
                Header::Path(value) => 5 + value.len() + 32,
                Header::Protocol(value) => 9 + value.as_str().len() + 32,
                Header::Status(_) => 7 + 3 + 32,
            })
            .sum()
    }
    fn compare_headers(headers: Vec<Header<Option<HeaderName>>>) -> (Vec<u8>, Vec<Header>) {
        let decoded = decoded_size(&headers);
        // Executes the verbatim production geometry helper, not a model.
        let capacity = crate::send_header_geometry::block_capacity(decoded).unwrap();
        let default_input = headers.clone();
        let mut default_encoder = zero_encoder();
        let mut default = BytesMut::new();
        default_encoder.encode(default_input, &mut default);
        let mut encoder = zero_encoder();
        let mut output = Vec::with_capacity(capacity);
        let pointer = output.as_ptr();
        let original_capacity = output.capacity();
        let observed =
            measured(|| encoder.encode_into(headers, &mut FixedEncodeBuffer::new(&mut output)));
        assert_eq!(
            observed,
            (0, 0),
            "original fixed encoder must not allocate or reallocate"
        );
        assert_eq!(output.as_ptr(), pointer);
        assert_eq!(output.capacity(), original_capacity);
        assert_eq!(output.as_slice(), default.as_ref());
        assert!(output.len() <= capacity);
        assert_eq!(encoder.table.capacity(), 0);
        let decoded = decode(&mut Decoder::new(4096), &output);
        (output, decoded)
    }
    fn string_length(wire: &[u8]) -> (usize, usize) {
        if wire == [0] {
            return (1, 0);
        }
        assert_ne!(wire[0] & 0x80, 0);
        let mut length = usize::from(wire[0] & 127);
        let mut prefix = 1;
        if length == 127 {
            let mut shift = 0;
            loop {
                let byte = wire[prefix];
                prefix += 1;
                length += usize::from(byte & 127) << shift;
                if byte & 128 == 0 {
                    break;
                }
                shift += 7;
            }
        }
        assert_eq!(prefix + length, wire.len());
        (prefix, length)
    }
    fn compare_string(input: &[u8]) -> (usize, usize) {
        let mut default = BytesMut::new();
        encode_str(input, &mut default);
        let capacity = crate::send_header_geometry::block_capacity(input.len().max(1)).unwrap();
        let mut fixed = Vec::with_capacity(capacity);
        let pointer = fixed.as_ptr();
        assert_eq!(
            measured(|| encode_str(input, &mut FixedEncodeBuffer::new(&mut fixed))),
            (0, 0)
        );
        assert_eq!(fixed.as_ptr(), pointer);
        assert_eq!(fixed.as_slice(), default.as_ref());
        let (prefix, length) = string_length(&fixed);
        let decoded = huffman::decode_bounded(&fixed[prefix..], input.len()).unwrap();
        assert_eq!(decoded.as_ref(), input);
        (prefix, length)
    }

    #[test]
    fn all_byte_huffman_and_multibyte_length_prefix_shift_match_actual_default() {
        // The raw string algorithm accepts all octets. HTTP HeaderValue does
        // not accept control octets; those cases use the actual Huffman decoder
        // directly, rather than pretending they are valid HTTP fields.
        for length in [0, 1, 32, 127, 128, 512, 2048] {
            let input: Vec<u8> = (0..length).map(|index| index as u8).collect();
            let (prefix, _) = compare_string(&input);
            if length >= 512 {
                assert!(prefix >= 2);
            }
        }
    }
    #[test]
    fn thirty_bit_huffman_codes_cross_three_byte_string_prefix() {
        // RFC HPACK symbol 0x0a has a 30-bit code; 0xff has only 26.
        let input = vec![0x0a; 4370];
        let (prefix, payload) = compare_string(&input);
        assert_eq!(prefix, 3);
        assert_eq!(payload, (input.len() * 30).div_ceil(8));
    }
    #[test]
    fn headers_duplicates_nameless_static_and_sensitive_match_real_decoder() {
        let mut sensitive = HeaderValue::from_static("second-sensitive");
        sensitive.set_sensitive(true);
        let raw_value: Vec<u8> = (128..=255).cycle().take(1024).collect();
        let headers = vec![
            Header::Method(http::Method::POST),
            field("x-fixed-name", HeaderValue::from_static("first")),
            Header::Field {
                name: None,
                value: sensitive,
            },
            field("authorization", HeaderValue::from_static("secret")),
            field("x-high-byte", HeaderValue::from_bytes(&raw_value).unwrap()),
        ];
        let (wire, decoded) = compare_headers(headers);
        assert_eq!(&wire[..3], &[0x20, 0x83, 0]);
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded[0], Header::Method(http::Method::POST));
        assert_eq!(
            decoded[2],
            Header::Field {
                name: HeaderName::from_static("x-fixed-name"),
                value: HeaderValue::from_static("second-sensitive")
            }
        );
        assert_eq!(
            decoded[4],
            Header::Field {
                name: HeaderName::from_static("x-high-byte"),
                value: HeaderValue::from_bytes(&raw_value).unwrap()
            }
        );
        // Actual upstream decoder strips the sensitivity marker. Independently
        // walk the first literal's lengths and assert never-indexed wire form.
        let mut at = 3;
        for _ in 0..2 {
            let mut length = usize::from(wire[at] & 127);
            at += 1;
            if length == 127 {
                let mut shift = 0;
                loop {
                    let byte = wire[at];
                    at += 1;
                    length += usize::from(byte & 127) << shift;
                    if byte & 128 == 0 {
                        break;
                    }
                    shift += 7;
                }
            }
            at += length;
        }
        assert_eq!(wire[at], 0x10);
    }
    #[test]
    fn all_six_pseudo_types_use_actual_geometry_and_default_wire() {
        let (_, decoded) = compare_headers(vec![
            Header::Method(http::Method::from_bytes(b"CUSTOM-LONG-METHOD").unwrap()),
            Header::Scheme(BytesStr::from_static("https")),
            Header::Authority(BytesStr::from_static("backend.example:443")),
            Header::Path(BytesStr::from_static("/long/path?value=1")),
            Header::Protocol(crate::ext::Protocol::from_static("websocket")),
            Header::Status(http::StatusCode::OK),
        ]);
        assert_eq!(decoded.len(), 6);
        // This is HPACK vocabulary coverage, not a claim that a mixed request
        // and response pseudo set is a valid HTTP request or response.
        assert_eq!(
            decoded[4],
            Header::Protocol(crate::ext::Protocol::from_static("websocket"))
        );
    }
    #[test]
    fn original_fixed_allocation_reused_across_updates_without_growth() {
        let mut fixed_encoder = zero_encoder();
        let mut default_encoder = zero_encoder();
        let mut decoder = Decoder::new(4096);
        let mut fixed = Vec::with_capacity(8192);
        let pointer = fixed.as_ptr();
        for peer in [0, 4096, u32::MAX as usize, 1] {
            fixed_encoder.update_max_size(peer);
            default_encoder.update_max_size(peer);
            let headers = vec![
                Header::Method(http::Method::POST),
                field("x-reused", HeaderValue::from_static("value")),
            ];
            let default_input = headers.clone();
            let mut default = BytesMut::new();
            default_encoder.encode(default_input, &mut default);
            fixed.clear();
            assert_eq!(
                measured(
                    || fixed_encoder.encode_into(headers, &mut FixedEncodeBuffer::new(&mut fixed))
                ),
                (0, 0)
            );
            assert_eq!(fixed.as_slice(), default.as_ref());
            assert_eq!(fixed.as_ptr(), pointer);
            assert_eq!(fixed.capacity(), 8192);
            assert_eq!(decode(&mut decoder, &fixed).len(), 2);
        }
    }
    #[test]
    fn exact_initialized_range_and_full_capacity_support_bufmut() {
        let mut vector = Vec::with_capacity(4);
        let pointer = vector.as_ptr();
        {
            let mut fixed = FixedEncodeBuffer::new(&mut vector);
            assert_eq!(fixed.chunk_mut().len(), 4);
            fixed.chunk_mut().write_byte(0, 7);
            // SAFETY: The sole newly live byte was initialized above.
            unsafe {
                fixed.advance_mut(1);
            }
            assert_eq!(fixed.as_ref(), &[7]);
            assert_eq!(fixed.chunk_mut().len(), 3);
            fixed.put_slice(&[8, 9, 10]);
            assert_eq!(fixed.remaining_mut(), 0);
            assert_eq!(fixed.chunk_mut().len(), 0);
            fixed.as_mut()[0] = 11;
        }
        assert_eq!(vector, [11, 8, 9, 10]);
        assert_eq!(vector.as_ptr(), pointer);
        assert_eq!(vector.capacity(), 4);
    }
    #[test]
    fn zero_capacity_empty_chunks_are_valid_and_never_grow() {
        let mut vector = Vec::new();
        let mut fixed = FixedEncodeBuffer::new(&mut vector);
        assert_eq!(fixed.remaining_mut(), 0);
        assert_eq!(fixed.chunk_mut().len(), 0);
        fixed.put_slice(&[]);
        // SAFETY: Zero bytes need initialization and remain inside the bound.
        unsafe {
            fixed.advance_mut(0);
        }
        assert!(fixed.as_ref().is_empty());
        assert_eq!(vector.capacity(), 0);
    }
    #[test]
    fn oversized_advance_panics_before_changing_initialized_len() {
        let mut vector = Vec::with_capacity(3);
        vector.push(42);
        let pointer = vector.as_ptr();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut fixed = FixedEncodeBuffer::new(&mut vector);
            // Deliberately invoke the implementation's checked refusal path.
            // It must panic before set_len or any uninitialized read occurs.
            unsafe {
                fixed.advance_mut(usize::MAX);
            }
        }));
        assert!(result.is_err());
        assert_eq!(vector.as_slice(), &[42]);
        assert_eq!(vector.as_ptr(), pointer);
        assert_eq!(vector.capacity(), 3);
    }
    #[test]
    fn actual_geometry_checks_zero_u32_range_and_overflow_without_allocation() {
        assert!(crate::send_header_geometry::block_capacity(0).is_err());
        assert_eq!(crate::send_header_geometry::block_capacity(1).unwrap(), 24);
        assert_eq!(
            crate::send_header_geometry::block_capacity(16384).unwrap(),
            65556
        );
        let maximum = crate::send_header_geometry::block_capacity(u32::MAX as usize);
        if usize::BITS == 64 {
            assert_eq!(maximum.unwrap(), 17179869200);
        } else {
            assert!(maximum.is_err());
        }
        assert!(crate::send_header_geometry::block_capacity(usize::MAX).is_err());
    }
    #[test]
    fn default_output_allocation_is_positive_control_for_system_measurement() {
        let headers = vec![field(
            "x-positive-control",
            HeaderValue::from_static("nonempty"),
        )];
        let mut default_encoder = zero_encoder();
        let mut default = BytesMut::new();
        let observed = measured(|| default_encoder.encode(headers, &mut default));
        assert!(observed.0 > 0 || observed.1 > 0);
        assert!(!default.is_empty());
    }
}
