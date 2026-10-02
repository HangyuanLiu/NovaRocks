// Appended only to a temporary byte-for-byte copy of the production encoder.
// Actual encoder, table, Huffman and decoder execute; no algorithm model.
#[cfg(test)]
mod send_table_probe {
    use super::*;
    use crate::hpack::{Decoder, DecoderError};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};

    thread_local! { static TRACK: Cell<bool> = const { Cell::new(false) }; }
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    static REQUESTED: AtomicUsize = AtomicUsize::new(0);
    struct Tracked;
    // SAFETY: Forward every allocator pointer/Layout unchanged to System.
    // Input headers and output BytesMut are prepared outside the measured scope.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() && TRACK.try_with(Cell::get).unwrap_or(false) {
                CALLS.fetch_add(1, Ordering::AcqRel);
                REQUESTED.fetch_add(layout.size(), Ordering::AcqRel);
            }
            pointer
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
            TRACK.with(|v| v.set(false));
        }
    }
    fn measured<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
        CALLS.store(0, Ordering::Release);
        REQUESTED.store(0, Ordering::Release);
        TRACK.with(|v| v.set(true));
        let scope = Scope;
        let value = f();
        drop(scope);
        (
            value,
            CALLS.load(Ordering::Acquire),
            REQUESTED.load(Ordering::Acquire),
        )
    }
    fn field(name: &'static str, value: &'static str) -> Header<Option<HeaderName>> {
        Header::Field {
            name: Some(HeaderName::from_static(name)),
            value: HeaderValue::from_static(value),
        }
    }
    fn post() -> Header<Option<HeaderName>> {
        Header::Method(http::Method::POST)
    }
    fn encode_measured(
        encoder: &mut Encoder,
        headers: Vec<Header<Option<HeaderName>>>,
    ) -> (BytesMut, usize) {
        let mut output = BytesMut::with_capacity(65536);
        let (_, calls, bytes) = measured(|| encoder.encode(headers, &mut output));
        println!("actual encode allocations={calls}, requested={bytes}B, output={}B, table_entries={}, table_capacity={}", output.len(), encoder.table.len(), encoder.table.capacity());
        (output, calls)
    }
    fn decode(decoder: &mut Decoder, bytes: &mut BytesMut) -> Vec<Header> {
        let mut headers = Vec::new();
        decoder
            .decode(&mut Cursor::new(bytes), |header| headers.push(header))
            .unwrap();
        headers
    }
    fn skip_string(bytes: &[u8], at: &mut usize) {
        let mut length = usize::from(bytes[*at] & 127);
        *at += 1;
        if length == 127 {
            let mut shift = 0;
            loop {
                let byte = bytes[*at];
                *at += 1;
                length += usize::from(byte & 127) << shift;
                if byte & 128 == 0 {
                    break;
                }
                shift += 7;
            }
        }
        *at += length;
        assert!(*at <= bytes.len());
    }
    #[test]
    fn zero_initial_update_and_static_index_have_no_encoder_allocation() {
        let mut encoder = Encoder::default();
        encoder.set_max_size_limit(0);
        let (mut bytes, calls) = encode_measured(&mut encoder, vec![post()]);
        assert_eq!(calls, 0);
        assert_eq!(bytes.as_ref(), &[0x20, 0x83]);
        assert_eq!(encoder.table.len(), 0);
        assert_eq!(encoder.table.capacity(), 0);
        assert_eq!(
            decode(&mut Decoder::new(4096), &mut bytes),
            vec![Header::Method(http::Method::POST)]
        );
    }
    #[test]
    fn huge_peer_cannot_allocate_zero_table_or_change_duplicate_sensitive_values() {
        let mut encoder = Encoder::default();
        encoder.set_max_size_limit(0);
        encoder.update_max_size(u32::MAX as usize);
        let mut sensitive = HeaderValue::from_static("second-sensitive");
        sensitive.set_sensitive(true);
        let headers = vec![
            post(),
            field("x-send-cap", "first"),
            Header::Field {
                name: None,
                value: sensitive,
            },
            field("authorization", "secret"),
            field("x-another-cap", "last"),
        ];
        let (mut bytes, calls) = encode_measured(&mut encoder, headers);
        assert_eq!(calls, 0);
        assert_eq!(bytes[0], 0x20);
        assert_eq!(encoder.table.len(), 0);
        assert_eq!(encoder.table.capacity(), 0);
        // Independent representation oracle: after size update + static POST,
        // skip the first custom literal's two strings. The repeated sensitive
        // value must use the never-indexed representation (not just same text).
        assert_eq!(&bytes[..3], &[0x20, 0x83, 0]);
        let mut at = 3;
        skip_string(&bytes, &mut at);
        skip_string(&bytes, &mut at);
        assert_eq!(bytes[at], 0x10);
        let headers = decode(&mut Decoder::new(4096), &mut bytes);
        assert_eq!(headers.len(), 5);
        assert_eq!(
            headers[1],
            Header::Field {
                name: HeaderName::from_static("x-send-cap"),
                value: HeaderValue::from_static("first")
            }
        );
        // Upstream decoder intentionally does not preserve the never-indexed
        // marker in HeaderValue; its wire representation was checked above.
        match &headers[2] {
            Header::Field { name, value } => {
                assert_eq!(name, "x-send-cap");
                assert_eq!(value, "second-sensitive");
            }
            other => panic!("unexpected header {other:?}"),
        }
        assert_eq!(
            headers[4],
            Header::Field {
                name: HeaderName::from_static("x-another-cap"),
                value: HeaderValue::from_static("last")
            }
        );
    }
    #[test]
    fn zero_remains_zero_across_queued_and_applied_peer_updates() {
        let mut encoder = Encoder::default();
        encoder.set_max_size_limit(0);
        let mut decoder = Decoder::new(4096);
        for peer in [4096, 0, u32::MAX as usize, 1, 4096] {
            encoder.update_max_size(peer);
            let (mut bytes, calls) =
                encode_measured(&mut encoder, vec![post(), field("x-cap-repeat", "literal")]);
            assert_eq!(calls, 0);
            assert_eq!(encoder.table.capacity(), 0);
            assert_eq!(encoder.table.max_size(), 0);
            assert_eq!(decode(&mut decoder, &mut bytes).len(), 2);
        }
    }
    #[test]
    fn queued_peer_minimum_is_preserved_before_raise_under_nonzero_cap() {
        let mut encoder = Encoder::default();
        encoder.set_max_size_limit(64);
        encoder.update_max_size(0);
        encoder.update_max_size(4096);
        let (mut bytes, _) = encode_measured(&mut encoder, vec![post()]);
        assert_eq!(bytes.as_ref(), &[0x20, 0x3f, 0x21, 0x83]);
        assert_eq!(encoder.table.max_size(), 64);
        assert_eq!(
            decode(&mut Decoder::new(4096), &mut bytes),
            vec![Header::Method(http::Method::POST)]
        );
    }
    #[test]
    fn setter_uses_current_raw_peer_limit_and_can_raise_its_own_ceiling() {
        let mut encoder = Encoder::default();
        encoder.update_max_size(16);
        encoder.set_max_size_limit(128);
        let (mut bytes, _) = encode_measured(&mut encoder, vec![post()]);
        assert_eq!(bytes.as_ref(), &[0x30, 0x83]);
        let mut decoder = Decoder::new(4096);
        decode(&mut decoder, &mut bytes);
        encoder.update_max_size(4096);
        let (mut bytes, _) = encode_measured(&mut encoder, vec![post()]);
        assert_eq!(bytes.as_ref(), &[0x3f, 0x61, 0x83]);
        decode(&mut decoder, &mut bytes);
        encoder.set_max_size_limit(256);
        let (mut bytes, _) = encode_measured(&mut encoder, vec![post()]);
        assert_eq!(bytes.as_ref(), &[0x3f, 0xe1, 1, 0x83]);
        decode(&mut decoder, &mut bytes);
        assert_eq!(encoder.table.max_size(), 256);
    }
    #[test]
    fn zero_update_clears_preexisting_decoder_entries_but_does_not_claim_encoder_capacity_reclaim()
    {
        let mut encoder = Encoder::default();
        let (mut bytes, calls) =
            encode_measured(&mut encoder, vec![field("x-before-zero", "inserted")]);
        assert!(
            calls > 0,
            "positive control: real dynamic table must allocate"
        );
        assert_eq!(encoder.table.len(), 1);
        let mut decoder = Decoder::new(4096);
        assert_eq!(decode(&mut decoder, &mut bytes).len(), 1);
        encoder.set_max_size_limit(0);
        let (mut bytes, calls) = encode_measured(&mut encoder, vec![post()]);
        assert_eq!(calls, 0);
        assert_eq!(bytes.as_ref(), &[0x20, 0x83]);
        assert_eq!(encoder.table.len(), 0);
        assert!(
            encoder.table.capacity() > 0,
            "late shrink retains prior table backing"
        );
        decode(&mut decoder, &mut bytes);
        let mut stale_index = BytesMut::from(&[0xbe][..]);
        assert_eq!(
            decoder.decode(&mut Cursor::new(&mut stale_index), |_| {}),
            Err(DecoderError::InvalidTableIndex)
        );
    }
    #[test]
    fn absent_local_cap_keeps_real_dynamic_indexing_and_original_size_updates() {
        let mut encoder = Encoder::default();
        encoder.update_max_size(0);
        encoder.update_max_size(4096);
        let (mut bytes, calls) =
            encode_measured(&mut encoder, vec![field("x-default-cap", "inserted")]);
        assert!(calls > 0);
        assert_eq!(&bytes[..4], &[0x20, 0x3f, 0xe1, 0x1f]);
        let mut decoder = Decoder::new(4096);
        assert_eq!(decode(&mut decoder, &mut bytes).len(), 1);
        let (mut bytes, calls) =
            encode_measured(&mut encoder, vec![field("x-default-cap", "inserted")]);
        assert_eq!(calls, 0);
        assert_eq!(bytes.as_ref(), &[0xbe]);
        assert_eq!(decode(&mut decoder, &mut bytes).len(), 1);
    }
}
