// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual Tonic status transforms in original-funded HTTP field storage.
//! The synchronous allocator oracle tracks requested fixed pool backing and
//! carrier layouts; rejected transforms also count every requested allocation.
//! Network/task/HPACK/URI/body scaffolds are separate, not a connection bound.

use bytes::Bytes;
use h2::{ReceiveFrameBuffer, ReceiveHeaderBlockBuffer, ReceiveHeaderFieldPool};
use hyper::http::header::{HeaderFieldAllocationPool, HeaderMapAllocationPool};
use hyper::http::{HeaderMap, HeaderValue};
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::task::JoinHandle;
use tonic::{Code, Status};

#[derive(Clone, Copy)]
struct Allocation {
    pointer: usize,
    family: u8,
}
const EMPTY: Allocation = Allocation {
    pointer: 0,
    family: 0,
};
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static FAMILY: Cell<u8> = const { Cell::new(0) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static REQUESTED: Cell<usize> = const { Cell::new(0) };
    static RECORDS: RefCell<[Allocation; 128]> = const { RefCell::new([EMPTY; 128]) };
}
fn allocated(pointer: *mut u8, size: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        CALLS.with(|v| v.set(v.get() + 1));
        REQUESTED.with(|v| v.set(v.get() + size));
        let family = FAMILY.with(Cell::get);
        if family != 0 {
            RECORDS.with(|records| {
                let mut records = records.borrow_mut();
                let slot = records.iter_mut().find(|r| r.pointer == 0).unwrap();
                *slot = Allocation {
                    pointer: pointer as usize,
                    family,
                };
            });
        }
    }
}
fn freed(pointer: *mut u8) {
    let _ = RECORDS.try_with(|records| {
        if let Some(record) = records
            .borrow_mut()
            .iter_mut()
            .find(|r| r.pointer == pointer as usize)
        {
            *record = EMPTY;
        }
    });
}
struct Probe;
// SAFETY: Allocation operations delegate unchanged to System. Fixed TLS records
// neither allocate nor dereference the recorded pointers.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, size) };
        freed(pointer);
        allocated(next, size);
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        freed(pointer);
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|v| v.set(false));
        FAMILY.with(|v| v.set(0));
    }
}
fn measure<T>(family: u8, operation: impl FnOnce() -> T) -> (T, usize, usize) {
    assert!(!TRACK.with(|v| v.replace(true)));
    FAMILY.with(|v| v.set(family));
    CALLS.with(|v| v.set(0));
    REQUESTED.with(|v| v.set(0));
    let tracking = Tracking;
    let result = operation();
    drop(tracking);
    (result, CALLS.with(Cell::get), REQUESTED.with(Cell::get))
}
fn no_allocation<T>(operation: impl FnOnce() -> T) -> T {
    let (result, calls, bytes) = measure(0, operation);
    assert_eq!((calls, bytes), (0, 0));
    result
}
struct PhysicalExit {
    family: u8,
    _credit: ResultWriteCredit,
}
impl Drop for PhysicalExit {
    fn drop(&mut self) {
        if self.family != 0 {
            assert!(
                RECORDS.with(|records| records
                    .borrow()
                    .iter()
                    .all(|r| r.pointer == 0 || r.family != self.family)),
                "original fixed pool/Core/carrier backing must be physically freed before credit"
            );
        }
    }
}
fn grant(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("complete original family pregrant");
    };
    credit
}
fn owner(budget: &Arc<ResultRetainedBudget>, bytes: usize, family: u8) -> Bytes {
    let credit = grant(budget, bytes);
    // The carrier has its own complete pregrant, in addition to the fixed pool.
    let (owner, _, requested) = measure(family, || {
        Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            PhysicalExit {
                family,
                _credit: credit,
            },
        )
    });
    assert!(requested <= carrier());
    owner
}
fn carrier() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<Bytes, PhysicalExit>()
}
struct Funded {
    maps: HeaderMapAllocationPool,
    fields: HeaderFieldAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
    map_grant: usize,
    field_grant: usize,
}
impl Funded {
    fn new(capacity: usize, positions: usize, max_field: usize) -> Self {
        let map_bound = HeaderMapAllocationPool::allocation_capacity_bound(16, 16, 4).unwrap();
        let field_bound =
            HeaderFieldAllocationPool::allocation_capacity_bound(capacity, positions, max_field)
                .unwrap();
        let map_grant = map_bound + carrier();
        let field_grant = field_bound + carrier();
        let total = map_grant + field_grant;
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let field_owner = owner(&budget, field_grant, 1);
        let (fields, calls, bytes) = measure(1, || {
            HeaderFieldAllocationPool::new(capacity, positions, max_field, field_owner).unwrap()
        });
        assert_eq!(calls, 3, "arena, extent bitmap and Core Arc");
        assert!(bytes <= field_bound);
        let map_owner = owner(&budget, map_grant, 2);
        let (maps, calls, bytes) = measure(2, || {
            HeaderMapAllocationPool::new(16, 16, 4, map_owner).unwrap()
        });
        assert_eq!(calls, 1, "map family Core Arc");
        assert!(bytes <= map_bound);
        fields.try_bind_once().unwrap();
        maps.try_bind_connection_with_fields(&fields).unwrap();
        Self {
            maps,
            fields,
            budget,
            total,
            map_grant,
            field_grant,
        }
    }
    fn normal() -> Self {
        Self::new(4096, 16, 1024)
    }
    fn map(&self) -> HeaderMap {
        let (map, calls, _) = measure(2, || {
            HeaderMap::try_from_allocation_pool(&self.maps).unwrap()
        });
        assert_eq!(calls, 3, "fixed indices, entries and duplicate storage");
        map
    }
    fn input(&self, message: Option<&'static str>, details: Option<&'static str>) -> HeaderMap {
        let mut map = self.map();
        headers(&mut map, message, details);
        map
    }
    fn finish(self) {
        let budget = self.budget.clone();
        let total = self.total;
        drop(self);
        drop(grant(&budget, total));
    }
}
fn headers(map: &mut HeaderMap, message: Option<&'static str>, details: Option<&'static str>) {
    map.try_insert(Status::GRPC_STATUS, HeaderValue::from_static("3"))
        .unwrap();
    if let Some(message) = message {
        map.try_insert(Status::GRPC_MESSAGE, HeaderValue::from_static(message))
            .unwrap();
    }
    if let Some(details) = details {
        map.try_insert(
            Status::GRPC_STATUS_DETAILS,
            HeaderValue::from_static(details),
        )
        .unwrap();
    }
}
fn ordinary(message: Option<&'static str>, details: Option<&'static str>) -> HeaderMap {
    let mut map = HeaderMap::new();
    headers(&mut map, message, details);
    map
}
fn capacity_error(status: &Status) {
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(status.message(), "HTTP status field capacity exhausted");
}
fn held(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    assert!(matches!(
        budget.try_reserve_process(bytes).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}

#[test]
fn percent_decoding_preserves_default_literal_bytes_and_legacy_percent_quirk() {
    for (wire, expected) in [
        ("", ""),
        ("A+%41", "A+A"),
        ("%", "%"),
        ("%G1%1%", "%G1%1%"),
        ("%2541", "%41"),
        ("%00", "\0"),
        ("%E4%B8%AD%E6%96%87", "中文"),
    ] {
        let f = Funded::normal();
        let input = f.input(Some(wire), None);
        let default = Status::from_header_map(&ordinary(Some(wire), None)).unwrap();
        let status = Status::from_header_map(&input).unwrap();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message().as_bytes(), expected.as_bytes());
        assert_eq!(status.message(), default.message());
        assert_eq!(
            f.fields.available_positions(),
            16 - usize::from(!expected.is_empty())
        );
        drop((status, default, input));
        f.finish();
    }
}

#[test]
fn outgoing_percent_and_base64_have_independent_expected_bytes() {
    for (message, wire) in [
        ("%41+", "%41+"),
        ("中文", "%E4%B8%AD%E6%96%87"),
        (" \"#<>`?{}\0\u{7f}", "%20%22%23%3C%3E%60%3F%7B%7D%00%7F"),
    ] {
        let f = Funded::normal();
        let status = Status::with_details(Code::InvalidArgument, message, Bytes::from_static(b"f"));
        let mut funded = f.map();
        let mut default = HeaderMap::new();
        status.add_header(&mut funded).unwrap();
        status.add_header(&mut default).unwrap();
        assert_eq!(funded[Status::GRPC_MESSAGE].as_bytes(), wire.as_bytes());
        assert_eq!(funded[Status::GRPC_STATUS_DETAILS].as_bytes(), b"Zg");
        assert_eq!(funded[Status::GRPC_MESSAGE], default[Status::GRPC_MESSAGE]);
        drop((status, default, funded));
        f.finish();
    }
    for wire in ["Zg", "Zg=", "Zg=="] {
        let f = Funded::normal();
        let input = f.input(None, Some(wire));
        let status = Status::from_header_map(&input).unwrap();
        assert_eq!(status.details(), b"f");
        assert_eq!(
            status.details(),
            Status::from_header_map(&ordinary(None, Some(wire)))
                .unwrap()
                .details()
        );
        drop((status, input));
        f.finish();
    }
}

#[test]
fn malformed_utf8_and_base64_return_exact_errors_without_heap_fallback() {
    for (wire, expected) in [
        (
            "%FF",
            "Error deserializing status message header: invalid utf-8 sequence of 1 bytes from index 0",
        ),
        (
            "%E4%B8",
            "Error deserializing status message header: incomplete utf-8 byte sequence from index 0",
        ),
    ] {
        let f = Funded::normal();
        let input = f.input(Some(wire), None);
        let default = Status::from_header_map(&ordinary(Some(wire), None)).unwrap();
        let status = Status::from_header_map(&input).unwrap();
        assert_eq!(status.code(), Code::Unknown);
        assert_eq!(status.message(), expected);
        assert_eq!(status.message(), default.message());
        assert_eq!(
            f.fields.available_positions(),
            15,
            "failed UTF-8 extent was rolled back before bounded formatting"
        );
        drop((status, default, input));
        assert_eq!(f.fields.available_positions(), 16);
        f.finish();
    }
    for wire in ["Zh", "Zh=", "Zh==", "Zg===", "Z g==", "_w=="] {
        let f = Funded::normal();
        let input = f.input(None, Some(wire));
        let status = Status::from_header_map(&input).unwrap();
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), "Invalid grpc-status-details-bin header");
        assert_eq!(
            f.fields.available_positions(),
            16,
            "malformed details roll back checkout"
        );
        drop((status, input));
        f.finish();
    }
}

#[test]
fn cloned_status_http_body_and_escaped_header_values_keep_original_field_grant() {
    let f = Funded::normal();
    let input = f.input(Some("space%20%E4%B8%AD"), Some("AAEC/w=="));
    let status = Status::from_header_map(&input).unwrap();
    assert_eq!(
        f.fields.available_positions(),
        14,
        "message and details both occupy original arena positions"
    );
    let copy = status.try_clone().unwrap();
    assert_eq!(copy.message().as_ptr(), status.message().as_ptr());
    assert_eq!(copy.details().as_ptr(), status.details().as_ptr());
    let mut output = f.map();
    copy.add_header(&mut output).unwrap();
    let message = output[Status::GRPC_MESSAGE].clone();
    let message_copy = no_allocation(|| message.clone());
    let details = output[Status::GRPC_STATUS_DETAILS].clone();
    assert_eq!(message.as_bytes(), b"space%20%E4%B8%AD");
    assert_eq!(details.as_bytes(), b"AAEC/w");
    assert_eq!(
        message.as_bytes().as_ptr(),
        message_copy.as_bytes().as_ptr()
    );
    let response = status.try_clone().unwrap().into_http();
    assert_eq!(
        response.headers()[Status::GRPC_MESSAGE].as_bytes(),
        message.as_bytes()
    );
    let (parts, body) = response.into_parts();
    drop((parts, body, input, status, copy, output));
    let budget = f.budget.clone();
    let (total, map_grant, field_grant) = (f.total, f.map_grant, f.field_grant);
    drop(f); // Includes the map parent's retained neutral field pool.
    held(&budget, total);
    drop(grant(&budget, map_grant));
    assert_eq!(map_grant + field_grant, total);
    drop((message, details));
    held(&budget, total);
    assert_eq!(message_copy.as_bytes(), b"space%20%E4%B8%AD");
    drop(message_copy);
    drop(grant(&budget, total));
}

#[test]
fn absent_and_empty_fields_consume_no_field_positions_or_payload_allocations() {
    for value in [None, Some("")] {
        let f = Funded::normal();
        let input = f.input(value, value);
        let (status, calls, _) = measure(0, || Status::from_header_map(&input).unwrap());
        assert_eq!(calls, 3, "only the required fixed metadata map clone");
        assert_eq!(status.message(), "");
        assert_eq!(status.details(), b"");
        assert_eq!(f.fields.available_positions(), 16);
        let mut output = f.map();
        no_allocation(|| status.add_header(&mut output).unwrap());
        assert!(!output.contains_key(Status::GRPC_MESSAGE));
        assert!(!output.contains_key(Status::GRPC_STATUS_DETAILS));
        drop((status, input, output));
        f.finish();
    }
}

#[test]
fn position_and_aggregate_exhaustion_refuse_encoding_without_new_allocation() {
    for aggregate in [false, true] {
        let f = if aggregate {
            Funded::new(128, 2, 128)
        } else {
            Funded::new(128, 1, 128)
        };
        let length = if aggregate { 128 } else { 1 };
        let blocker = f
            .fields
            .try_fill::<Infallible>(length, |bytes| {
                bytes.fill(b'x');
                Ok(())
            })
            .unwrap();
        assert_eq!(f.fields.available_positions(), usize::from(aggregate));
        let status = Status::invalid_argument(" "); // Owned input prepared outside measurement.
        let mut output = f.map();
        let error = no_allocation(|| status.add_header(&mut output).unwrap_err());
        capacity_error(&error);
        assert!(!output.contains_key(Status::GRPC_MESSAGE));
        drop((error, blocker));
        status.add_header(&mut output).unwrap();
        assert_eq!(output[Status::GRPC_MESSAGE].as_bytes(), b"%20");
        drop((status, output));
        f.finish();
    }
}

#[test]
fn encoded_expansion_and_decoded_limits_refuse_before_new_field_backing() {
    for details in [false, true] {
        let f = Funded::new(64, 1, if details { 3 } else { 8 });
        let status = if details {
            Status::with_details(Code::InvalidArgument, "", Bytes::from_static(b"abc"))
        } else {
            Status::invalid_argument("   ")
        };
        let mut output = f.map();
        let error = no_allocation(|| status.add_header(&mut output).unwrap_err());
        capacity_error(&error);
        assert_eq!(f.fields.available_positions(), 1);
        drop((status, output, error));
        f.finish();
    }
    let f = Funded::new(64, 1, 8);
    let input = f.input(Some("123456789"), None);
    let (status, calls, _) = measure(0, || Status::from_header_map(&input).unwrap());
    capacity_error(&status);
    assert_eq!(
        calls, 3,
        "metadata clone only; no field wrapper or copied fallback"
    );
    assert_eq!(f.fields.available_positions(), 1);
    drop((status, input));
    let input = f.input(Some("%20%20"), None);
    let status = Status::from_header_map(&input).unwrap();
    assert_eq!(status.message(), "  ");
    drop((status, input));
    f.finish();
}

#[test]
fn exact_decoded_quantum_and_metadata_refusal_do_not_allocate_unfunded_fields() {
    // Independent literal: 64 zero bytes encode to 86 symbols without padding.
    const SIXTY_FOUR_ZERO_BYTES: &str =
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    assert_eq!(SIXTY_FOUR_ZERO_BYTES.len(), 86);
    let f = Funded::new(64, 1, 64);
    let input = f.input(None, Some(SIXTY_FOUR_ZERO_BYTES));
    let status = Status::from_header_map(&input).unwrap();
    assert_eq!(status.details(), &[0; 64]);
    assert_eq!(f.fields.available_positions(), 0);
    drop((input, status));
    assert_eq!(f.fields.available_positions(), 1);
    f.finish();

    let f = Funded::normal();
    let input = f.input(Some("must-not-decode"), None);
    let held_maps: Vec<_> = (0..15).map(|_| f.map()).collect();
    assert_eq!(f.maps.available_maps(), 0);
    let status = no_allocation(|| Status::from_header_map(&input).unwrap());
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(status.message(), "HTTP metadata capacity exhausted");
    assert_eq!(f.fields.available_positions(), 16);
    drop((status, input, held_maps));
    f.finish();
}

use std::convert::Infallible;
const MAX: usize = 16384;
const DEADLINE: Duration = Duration::from_secs(5);
struct ObservedIo {
    inner: DuplexStream,
    exits: Arc<AtomicUsize>,
}
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl Drop for ObservedIo {
    fn drop(&mut self) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Clone, Default)]
struct JoinedExecutor(Arc<Mutex<Vec<JoinHandle<()>>>>);
impl<F: Future + Send + 'static> hyper::rt::Executor<F> for JoinedExecutor
where
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(tokio::spawn(async move {
            let _ = future.await;
        }));
    }
}
impl JoinedExecutor {
    async fn stop_and_join(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            if tasks.is_empty() {
                return;
            }
            for task in &tasks {
                task.abort();
            }
            for task in tasks {
                if let Err(error) = tokio::time::timeout(DEADLINE, task).await.unwrap() {
                    assert!(
                        error.is_cancelled(),
                        "connection executor panicked: {error}"
                    );
                }
            }
        }
    }
}
fn frame(wire: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
}
fn literal(block: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    assert!(name.len() < 127 && value.len() < 127);
    block.push(0); // No dynamic indexing: only the original field arena retains values.
    block.push(name.len() as u8);
    block.extend_from_slice(name);
    block.push(value.len() as u8);
    block.extend_from_slice(value);
}
async fn published(peer: &mut DuplexStream) {
    tokio::time::timeout(DEADLINE, async {
        let mut preface = [0; 24];
        peer.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        let mut headers = false;
        for _ in 0..32 {
            let mut header = [0; 9];
            peer.read_exact(&mut header).await.unwrap();
            let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            let stream = u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fffffff;
            let mut payload = vec![0; length];
            peer.read_exact(&mut payload).await.unwrap();
            if header[3] == 1 && stream == 1 {
                headers = true;
            }
            if headers && stream == 1 && matches!(header[3], 1 | 9) && header[4] & 4 != 0 {
                return;
            }
        }
        panic!("actual request HEADERS publication gate did not complete");
    })
    .await
    .unwrap();
}
#[derive(Clone, PartialEq, prost::Message)]
struct EmptyMessage {}

#[tokio::test(flavor = "current_thread")]
async fn actual_h2_hyper_tonic_status_attachment_and_escaped_output_hold_original_grant() {
    let amounts = [
        ReceiveFrameBuffer::allocation_capacity_bound(MAX).unwrap() + carrier(),
        ReceiveHeaderBlockBuffer::allocation_capacity_bound(MAX).unwrap() + carrier(),
        ReceiveHeaderFieldPool::allocation_capacity_bound(65536, 1024, MAX).unwrap() + carrier(),
        HeaderMapAllocationPool::allocation_capacity_bound(16, 16, 8).unwrap() + carrier(),
    ];
    let total: usize = amounts.iter().sum();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let factory_budget = budget.clone();
    let saved = Arc::new(Mutex::new(None));
    let observed = saved.clone();
    let executor = JoinedExecutor::default();
    let endpoint = tonic::transport::Endpoint::from_static("http://localhost")
        .executor(executor.clone())
        .http2_connection_factory(move || {
            // Wire/runtime allocation measurement is deliberately disabled. Each
            // original family, including its carrier, still pregrants before allocation.
            let mut owners = amounts.map(|n| owner(&factory_budget, n, 0));
            let raw = ReceiveFrameBuffer::new(MAX, std::mem::take(&mut owners[0])).unwrap();
            let block = ReceiveHeaderBlockBuffer::new(MAX, std::mem::take(&mut owners[1])).unwrap();
            let fields =
                ReceiveHeaderFieldPool::new(65536, 1024, MAX, std::mem::take(&mut owners[2]))
                    .unwrap();
            let maps =
                HeaderMapAllocationPool::new(16, 16, 8, std::mem::take(&mut owners[3])).unwrap();
            assert!(
                observed
                    .lock()
                    .unwrap()
                    .replace((fields.clone(), maps.clone()))
                    .is_none()
            );
            Ok::<_, io::Error>(tonic::transport::Http2ConnectionConfig {
                max_header_list_size: Some(MAX as u32),
                max_receive_header_block_size: Some(MAX),
                receive_frame_buffer: Some(raw),
                receive_header_block_buffer: Some(block),
                receive_header_field_pool: Some(fields),
                receive_header_map_pool: Some(maps),
                ..Default::default()
            })
        });
    let (io, mut peer) = tokio::io::duplex(131072);
    let exits = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(Mutex::new(Some(ObservedIo {
        inner: io,
        exits: exits.clone(),
    })));
    let connector = tower::service_fn(move |_: hyper::http::Uri| {
        let io = input.lock().unwrap().take();
        async move {
            io.map(TokioIo::new)
                .ok_or_else(|| io::Error::from(io::ErrorKind::ConnectionRefused))
        }
    });
    let channel = endpoint.connect_with_connector(connector).await.unwrap();
    drop(endpoint);
    let request = tokio::spawn(async move {
        let mut grpc = tonic::client::Grpc::new(channel);
        grpc.ready().await.unwrap();
        let codec = tonic::codec::ProstCodec::<EmptyMessage, EmptyMessage>::default();
        grpc.unary(
            tonic::Request::new(EmptyMessage {}),
            hyper::http::uri::PathAndQuery::from_static("/status.Status/Fail"),
            codec,
        )
        .await
        .unwrap_err()
    });
    published(&mut peer).await;
    let mut block = vec![0x88]; // :status 200
    literal(&mut block, b"content-type", b"application/grpc");
    literal(&mut block, b"grpc-status", b"3");
    literal(&mut block, b"grpc-message", b"wire%20%E4%B8%AD%25");
    literal(&mut block, b"grpc-status-details-bin", b"AAEC/w==");
    let mut wire = Vec::new();
    frame(&mut wire, 4, 0, 0, &[]);
    frame(&mut wire, 1, 5, 1, &block);
    peer.write_all(&wire).await.unwrap();
    let status = tokio::time::timeout(DEADLINE, request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "wire 中%");
    assert_eq!(status.details(), b"\0\x01\x02\xff");
    // A funded metadata map proves the actual H2 -> Hyper -> Tonic attachment;
    // a default String/details fallback cannot satisfy this retained field oracle.
    let (fields, maps) = saved.lock().unwrap().take().unwrap();
    assert!(
        status
            .metadata()
            .try_clone()
            .unwrap()
            .into_headers()
            .field_allocation_pool()
            .unwrap()
            .same_pool(fields.allocation_pool())
    );
    let copy = status.try_clone().unwrap();
    assert_eq!(copy.message().as_ptr(), status.message().as_ptr());
    assert_eq!(copy.details().as_ptr(), status.details().as_ptr());
    let mut output = HeaderMap::new();
    copy.add_header(&mut output).unwrap(); // Ordinary destination adopts funded metadata and original field capability.
    let message = output[Status::GRPC_MESSAGE].clone();
    let details = output[Status::GRPC_STATUS_DETAILS].clone();
    assert_eq!(message.as_bytes(), b"wire%20%E4%B8%AD%");
    assert_eq!(details.as_bytes(), b"AAEC/w");
    drop((status, copy, output, fields, maps, saved));
    executor.stop_and_join().await;
    assert_eq!(
        exits.load(Ordering::SeqCst),
        1,
        "original IO physically exited after every task joined"
    );
    drop(peer);
    held(&budget, total);
    let other: usize = amounts[0] + amounts[1] + amounts[3];
    drop(grant(&budget, other));
    assert_eq!(message.as_bytes(), b"wire%20%E4%B8%AD%");
    drop(message);
    held(&budget, total);
    drop(details);
    drop(grant(&budget, total));
}
