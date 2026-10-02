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

//! Consuming Tonic statuses use caller-preclaimed original header storage.
//! System/Worker probes cover fixed map/field/Core/carrier/wrapper backing.
//! Response body Boxes and pre-existing owned String/details allocations are
//! separate stream/composition obligations, not a complete Native bound.

use bytes::Bytes;
use hyper::body::Body;
use hyper::http::header::{HeaderFieldAllocationPool, HeaderMapAllocationPool};
use hyper::http::{HeaderMap, HeaderValue, StatusCode};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tonic::metadata::MetadataMap;
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
    fn new(maps: usize, keys: usize, capacity: usize, positions: usize, max: usize) -> Self {
        let map_bound = HeaderMapAllocationPool::allocation_capacity_bound(maps, keys, 4).unwrap();
        let field_bound =
            HeaderFieldAllocationPool::allocation_capacity_bound(capacity, positions, max).unwrap();
        let map_grant = map_bound + carrier();
        let field_grant = field_bound + carrier();
        let total = map_grant + field_grant;
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let original = owner(&budget, field_grant, 1);
        let (fields, calls, requested) = measure(1, || {
            HeaderFieldAllocationPool::new(capacity, positions, max, original).unwrap()
        });
        assert_eq!(calls, 3);
        assert!(requested <= field_bound);
        let original = owner(&budget, map_grant, 2);
        let (maps, calls, requested) = measure(2, || {
            HeaderMapAllocationPool::new(maps, keys, 4, original).unwrap()
        });
        assert_eq!(calls, 1);
        assert!(requested <= map_bound);
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
        Self::new(2, 8, 1024, 8, 128)
    }
    fn map(&self) -> HeaderMap {
        let (map, calls, _) = measure(2, || {
            HeaderMap::try_from_allocation_pool(&self.maps).unwrap()
        });
        assert_eq!(calls, 3);
        map
    }
    fn input(&self, input: &[u8]) -> Bytes {
        let (bytes, calls, _) = measure(1, || {
            self.fields
                .try_fill(input.len(), |out| {
                    out.copy_from_slice(input);
                    Ok::<_, ()>(())
                })
                .unwrap()
        });
        assert_eq!(calls, usize::from(!input.is_empty()));
        bytes
    }
    fn finish(self) {
        let budget = self.budget.clone();
        let total = self.total;
        drop(self);
        drop(grant(&budget, total));
    }
}
fn held(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    assert!(matches!(
        budget.try_reserve_process(bytes).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn response_error_once(response: &mut hyper::http::Response<tonic::body::BoxBody>) {
    let mut cx = Context::from_waker(Waker::noop());
    let frame = match Pin::new(response.body_mut()).poll_frame(&mut cx) {
        Poll::Ready(Some(Err(error))) => error,
        _ => panic!("one immediate terminal body error required"),
    };
    assert_eq!(frame.code(), Code::ResourceExhausted);
    assert!(matches!(
        Pin::new(response.body_mut()).poll_frame(&mut cx),
        Poll::Ready(None)
    ));
}

#[test]
fn static_status_construction_and_clone_do_not_allocate_owned_message_storage() {
    let status = no_allocation(|| Status::from_static(Code::InvalidArgument, "fixed failure"));
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "fixed failure");
    assert!(status.details().is_empty());
    assert!(status.metadata().is_empty());
    let copy = no_allocation(|| status.try_clone().unwrap());
    assert_eq!(status.message().as_ptr(), copy.message().as_ptr());
    assert_eq!(copy.message(), "fixed failure");
}

#[test]
fn consuming_trailers_use_original_percent_payload_and_final_header_alias_owner() {
    let f = Funded::normal();
    let headers = f.map();
    let status = no_allocation(|| Status::from_static(Code::InvalidArgument, "A +%41中文"));
    let (trailers, calls, _) = measure(1, || status.into_trailers_with_headers(headers).unwrap());
    assert_eq!(calls, 1, "only the prepaid transformed message wrapper");
    assert_eq!(trailers[Status::GRPC_STATUS].as_bytes(), b"3");
    assert_eq!(
        trailers[Status::GRPC_MESSAGE].as_bytes(),
        b"A%20+%41%E4%B8%AD%E6%96%87"
    );
    assert!(!trailers.contains_key("content-type"));
    assert!(trailers.allocation_pool().is_some());
    assert!(
        trailers
            .field_allocation_pool()
            .unwrap()
            .same_pool(&f.fields)
    );
    let alias = no_allocation(|| trailers[Status::GRPC_MESSAGE].clone());
    assert_eq!(
        alias.as_bytes().as_ptr(),
        trailers[Status::GRPC_MESSAGE].as_bytes().as_ptr()
    );
    let budget = f.budget.clone();
    let total = f.total;
    let map_grant = f.map_grant;
    let field_grant = f.field_grant;
    drop(f);
    held(&budget, total);
    drop(trailers);
    // The escaped field keeps only its original field family; map backing exits.
    let restored_map = grant(&budget, map_grant);
    // Occupy exactly the released map bytes so the remaining field-sized
    // request cannot be satisfied from a different family's free capacity.
    held(&budget, field_grant);
    drop(restored_map);
    assert_eq!(alias.as_bytes(), b"A%20+%41%E4%B8%AD%E6%96%87");
    drop(alias);
    drop(grant(&budget, total));
}

#[test]
fn consuming_http_has_exact_code_percent_and_unpadded_details_without_map_copy() {
    let f = Funded::new(1, 8, 1024, 8, 128);
    let headers = f.map();
    // These pre-existing owned inputs are outside the measured transform cut.
    let status = Status::with_details(Code::DataLoss, "雪 ?", Bytes::from_static(b"fo"));
    let (response, calls, _) = measure(1, || status.into_http_with_headers(headers));
    assert_eq!(
        calls, 2,
        "message/details wrappers only; no new HeaderMap position"
    );
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[Status::GRPC_STATUS].as_bytes(), b"15");
    assert_eq!(
        response.headers()[Status::GRPC_MESSAGE].as_bytes(),
        b"%E9%9B%AA%20%3F"
    );
    assert_eq!(
        response.headers()[Status::GRPC_STATUS_DETAILS].as_bytes(),
        b"Zm8"
    );
    assert_eq!(
        response.headers()["content-type"].as_bytes(),
        b"application/grpc"
    );
    assert_eq!(f.maps.available_maps(), 0);
    let message = no_allocation(|| response.headers()[Status::GRPC_MESSAGE].clone());
    let details = no_allocation(|| response.headers()[Status::GRPC_STATUS_DETAILS].clone());
    let budget = f.budget.clone();
    let total = f.total;
    drop(f);
    drop(response);
    held(&budget, total);
    drop(message);
    held(&budget, total);
    drop(details);
    drop(grant(&budget, total));
}

#[test]
fn source_metadata_is_moved_with_duplicate_groups_and_no_spare_map_position() {
    let f = Funded::normal();
    let mut target = f.map();
    target
        .try_insert("x-group", HeaderValue::from_static("old"))
        .unwrap();
    target
        .try_insert("x-target", HeaderValue::from_static("keep"))
        .unwrap();
    let mut source = f.map();
    let first = HeaderValue::from_maybe_shared(f.input(b"first")).unwrap();
    let second = HeaderValue::from_maybe_shared(f.input(b"second")).unwrap();
    let first_pointer = first.as_bytes().as_ptr();
    let second_pointer = second.as_bytes().as_ptr();
    source.try_insert("x-group", first).unwrap();
    source.try_append("x-group", second).unwrap();
    source
        .try_insert(Status::GRPC_STATUS, HeaderValue::from_static("2"))
        .unwrap();
    let mut status = Status::from_static(Code::Ok, "");
    *status.metadata_mut() = MetadataMap::from_headers(source);
    assert_eq!(f.maps.available_maps(), 0);
    let trailers = no_allocation(|| status.into_trailers_with_headers(target).unwrap());
    assert_eq!(
        f.maps.available_maps(),
        1,
        "source storage physically exits after move"
    );
    let mut values = trailers.get_all("x-group").iter();
    let first = values.next().unwrap();
    let second = values.next().unwrap();
    assert_eq!(first.as_bytes(), b"first");
    assert_eq!(second.as_bytes(), b"second");
    assert_eq!(first.as_bytes().as_ptr(), first_pointer);
    assert_eq!(second.as_bytes().as_ptr(), second_pointer);
    assert!(values.next().is_none());
    assert_eq!(trailers["x-target"].as_bytes(), b"keep");
    assert_eq!(trailers[Status::GRPC_STATUS].as_bytes(), b"0");
    drop(trailers);
    f.finish();
}

#[test]
fn trailer_map_exhaustion_exposes_no_partial_status_and_retains_the_target_claim() {
    let f = Funded::new(1, 1, 128, 2, 64);
    let mut headers = f.map();
    headers
        .try_insert("x-full", HeaderValue::from_static("keep"))
        .unwrap();
    let status = Status::from_static(Code::InvalidArgument, "");
    let error = no_allocation(|| status.into_trailers_with_headers(headers).unwrap_err());
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        error.metadata().is_empty(),
        "partial ordinary/status fields must not escape"
    );
    assert_eq!(f.maps.available_maps(), 0);
    let budget = f.budget.clone();
    let total = f.total;
    drop(f);
    held(&budget, total);
    drop(error);
    drop(grant(&budget, total));
}

#[test]
fn trailer_field_exhaustion_clears_inserted_status_and_keeps_original_metadata_owner() {
    let f = Funded::new(1, 8, 64, 1, 32);
    let blocker = f.input(b"occupied");
    let mut headers = f.map();
    headers
        .try_insert("x-old", HeaderValue::from_static("old"))
        .unwrap();
    let error = no_allocation(|| {
        Status::from_static(Code::Internal, "hi")
            .into_trailers_with_headers(headers)
            .unwrap_err()
    });
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.metadata().is_empty());
    let budget = f.budget.clone();
    let total = f.total;
    let map_grant = f.map_grant;
    let field_grant = f.field_grant;
    drop(f);
    held(&budget, total);
    drop(error);
    let restored_map = grant(&budget, map_grant);
    held(&budget, field_grant);
    drop(restored_map);
    drop(blocker);
    drop(grant(&budget, total));
}

#[test]
fn http_map_and_field_shortage_return_one_error_keep_cleared_headers_and_no_fallback() {
    for field_shortage in [false, true] {
        let f = Funded::new(1, if field_shortage { 8 } else { 1 }, 64, 1, 32);
        let blocker = field_shortage.then(|| f.input(b"occupied"));
        let mut headers = f.map();
        headers
            .try_insert("x-full", HeaderValue::from_static("keep"))
            .unwrap();
        let status = Status::from_static(Code::Internal, if field_shortage { "hi" } else { "" });
        // The one failure-body Box is a separate stream owner obligation. Do not
        // attribute it to the fixed map/field family being physically probed.
        let (mut response, calls, _) = measure(0, || status.into_http_with_headers(headers));
        assert_eq!(
            calls, 1,
            "one terminal error body Box, no fallback HeaderMap/String"
        );
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().is_empty());
        assert!(response.headers().allocation_pool().is_some());
        assert!(
            response
                .headers()
                .field_allocation_pool()
                .unwrap()
                .same_pool(&f.fields)
        );
        assert_eq!(f.maps.available_maps(), 0);
        no_allocation(|| response_error_once(&mut response));
        let budget = f.budget.clone();
        let total = f.total;
        drop(f);
        held(&budget, total);
        drop(response);
        drop(blocker);
        drop(grant(&budget, total));
    }
}
