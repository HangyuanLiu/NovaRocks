// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual requested allocations of fresh Prost DTO decoding. These wire
//! fixtures exercise resource projection, not valid physical business plans.
//! Input, model and controls are created outside the measured interval.

use novarocks_plan_codec::resource_preflight_v2::{
    DecodeProjectionLimits, DecodeResourceProjection, FragmentDecodeResourceModel,
    ResourceCursorStatus,
};
use novarocks_proto_models::physical_package_v2::FragmentPackage;
use novarocks_proto_models::physical_type_v2::carrier_type_definition::Kind;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use prost::Message;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

#[derive(Clone, Copy, Debug, Default)]
struct Stats {
    live: usize,
    peak: usize,
    cumulative: usize,
    allocations: usize,
    reallocations: usize,
    arithmetic_error: bool,
}
impl Stats {
    const ZERO: Self = Self {
        live: 0,
        peak: 0,
        cumulative: 0,
        allocations: 0,
        reallocations: 0,
        arithmetic_error: false,
    };
    fn add(&mut self, value: usize, amount: usize) -> usize {
        let (result, overflow) = value.overflowing_add(amount);
        self.arithmetic_error |= overflow;
        result
    }
    fn allocated(&mut self, size: usize) {
        self.cumulative = self.add(self.cumulative, size);
        self.live = self.add(self.live, size);
        self.peak = self.peak.max(self.live);
        self.allocations = self.add(self.allocations, 1);
    }
    fn reallocated(&mut self, old: usize, new: usize) {
        // Conservatively count old and new backing simultaneously, even when
        // System can resize in place. Old backing was already charged once.
        let overlap = self.add(self.live, new);
        self.peak = self.peak.max(overlap);
        self.cumulative = self.add(self.cumulative, new);
        let (remaining, underflow) = self.live.overflowing_sub(old);
        self.arithmetic_error |= underflow;
        self.live = self.add(remaining, new);
        self.reallocations = self.add(self.reallocations, 1);
    }
    fn freed(&mut self, size: usize) {
        let (remaining, underflow) = self.live.overflowing_sub(size);
        self.arithmetic_error |= underflow;
        self.live = remaining;
    }
}

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static COUNTERS: Cell<Stats> = const { Cell::new(Stats::ZERO) };
}

fn record(update: impl FnOnce(&mut Stats)) {
    // Const-initialized, non-Drop TLS and Cell access allocate no backing and
    // do not enter the allocator recursively. Other test threads are isolated.
    let enabled = ENABLED.try_with(Cell::get).unwrap_or(false);
    if enabled {
        let _ = COUNTERS.try_with(|counters| {
            let mut stats = counters.get();
            update(&mut stats);
            counters.set(stats);
        });
    }
}

struct RequestedAllocator;
// SAFETY: Every operation forwards the original pointer, Layout and sizes to
// System unchanged. Observation touches only inline TLS counters; it never
// reads allocation contents, allocates, unwinds or retains a pointer.
unsafe impl GlobalAlloc for RequestedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies System's required valid allocation Layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(|stats| stats.allocated(layout.size()));
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The Layout is forwarded unchanged to System.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(|stats| stats.allocated(layout.size()));
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The caller's original allocation and requested size are
        // forwarded unchanged, preserving System's realloc preconditions.
        let replacement = unsafe { System.realloc(pointer, layout, new_size) };
        if !replacement.is_null() {
            record(|stats| stats.reallocated(layout.size(), new_size));
        }
        replacement
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The original owned pointer and Layout are forwarded unchanged.
        unsafe { System.dealloc(pointer, layout) };
        record(|stats| stats.freed(layout.size()));
    }
}

#[global_allocator]
static ALLOCATOR: RequestedAllocator = RequestedAllocator;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn fixture_limits() -> DecodeProjectionLimits {
    // Explicit test envelope, not a decoder or product default.
    DecodeProjectionLimits {
        max_input_bytes: 8 * 1024 * 1024,
        max_requested_heap_bytes: 64 * 1024 * 1024,
        max_message_occurrences: 65_536,
        max_scalar_elements: 65_536,
        max_field_occurrences: 65_536,
        max_copied_bytes: 8 * 1024 * 1024,
        max_initialization_bytes: 64 * 1024 * 1024,
        max_wire_depth: 100,
    }
}

struct Capture;
impl Drop for Capture {
    fn drop(&mut self) {
        ENABLED.with(|enabled| enabled.set(false));
    }
}

fn measure<O>(raw: &[u8], inspect: impl FnOnce(&FragmentPackage) -> O) -> (Stats, Option<O>) {
    assert!(!ENABLED.with(Cell::get));
    COUNTERS.with(|stats| stats.set(Stats::ZERO));
    ENABLED.with(|enabled| enabled.set(true));
    let capture = Capture;
    let decoded = FragmentPackage::decode(raw);
    // The supplied observations are stack-only scalars. Do not log, assert,
    // clone DTO backing or format decoder errors inside this interval.
    let observation = decoded.as_ref().ok().map(inspect);
    drop(decoded);
    let stats = COUNTERS.with(Cell::get);
    drop(capture);
    (stats, observation)
}

fn projection(raw: &[u8]) -> DecodeResourceProjection {
    let control = Control;
    let model = FragmentDecodeResourceModel::try_new(&control).unwrap();
    model.preflight(raw, fixture_limits(), &control).unwrap()
}

fn assert_bound(raw: &[u8], projection: DecodeResourceProjection, measured: Stats) {
    assert!(!measured.arithmetic_error, "{measured:?}");
    assert_eq!(
        measured.live, 0,
        "DTO/error backing must be released before capture ends"
    );
    assert!(
        measured.cumulative <= projection.usage.cumulative_requested_heap_bytes_upper,
        "actual {measured:?}, model {:?}",
        projection.usage
    );
    assert!(
        measured.peak <= projection.usage.peak_requested_heap_bytes_upper,
        "actual {measured:?}, model {:?}",
        projection.usage
    );
    assert_eq!(projection.usage.input_bytes, raw.len());
}

fn varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 128 {
        output.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    output.push(value as u8);
}
fn scalar(tag: u32, value: u64, output: &mut Vec<u8>) {
    varint(u64::from(tag) << 3, output);
    varint(value, output);
}
fn bytes(tag: u32, body: &[u8], output: &mut Vec<u8>) {
    varint((u64::from(tag) << 3) | 2, output);
    varint(body.len() as u64, output);
    output.extend_from_slice(body);
}
fn packed(tag: u32, values: &[u32], output: &mut Vec<u8>) {
    let mut body = Vec::new();
    for value in values {
        varint(u64::from(*value), &mut body);
    }
    bytes(tag, &body, output);
}

#[test]
fn actual_requested_allocations_cover_repeated_empty_messages() {
    let mut raw = Vec::new();
    for _ in 0..37 {
        bytes(5, &[], &mut raw);
    }
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |dto| dto.schemas.len());
    assert_eq!(proof.status, ResourceCursorStatus::Complete);
    assert_eq!(observed, Some(37));
    assert!(stats.allocations > 0 && stats.reallocations > 0);
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_singular_and_same_oneof_message_merge() {
    let mut first_struct = Vec::new();
    packed(1, &[0, u32::MAX, 7], &mut first_struct);
    let mut second_struct = Vec::new();
    for value in [9, 0, 11] {
        scalar(1, value, &mut second_struct);
    }
    let mut carrier = Vec::new();
    bytes(18, &first_struct, &mut carrier);
    bytes(18, &second_struct, &mut carrier);
    let mut first_table = Vec::new();
    bytes(1, &carrier, &mut first_table);
    let mut second_carrier = Vec::new();
    scalar(1, u64::from(u32::MAX), &mut second_carrier);
    scalar(2, 6, &mut second_carrier);
    let mut second_table = Vec::new();
    bytes(1, &second_carrier, &mut second_table);
    let mut raw = Vec::new();
    bytes(4, &first_table, &mut raw);
    bytes(4, &second_table, &mut raw);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |dto| {
        dto.types.as_ref().map(|table| {
            let ids = match table.carriers.first().and_then(|c| c.kind.as_ref()) {
                Some(Kind::StructType(fields)) => (
                    fields.field_ids.len(),
                    fields.field_ids.get(1).copied(),
                    fields.field_ids.last().copied(),
                ),
                _ => (0, None, None),
            };
            (
                table.carriers.len(),
                ids,
                table.carriers.last().map(|carrier| carrier.id),
            )
        })
    });
    assert_eq!(
        observed,
        Some(Some((2, (6, Some(u32::MAX), Some(11)), Some(u32::MAX))))
    );
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_packed_unpacked_and_nonminimal_numeric_items() {
    let values = (0..257).collect::<Vec<u32>>();
    let mut schema = Vec::new();
    packed(2, &values, &mut schema);
    scalar(2, u64::from(u32::MAX), &mut schema);
    // Legal non-shortest zero varint, and an empty packed segment.
    schema.extend_from_slice(&[0x10, 0x80, 0]);
    bytes(2, &[], &mut schema);
    let mut raw = Vec::new();
    bytes(5, &schema, &mut raw);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |dto| {
        dto.schemas.first().map(|schema| {
            (
                schema.field_ids.len(),
                schema.field_ids.get(256).copied(),
                schema.field_ids.get(257).copied(),
                schema.field_ids.last().copied(),
            )
        })
    });
    assert_eq!(
        observed,
        Some(Some((259, Some(256), Some(u32::MAX), Some(0))))
    );
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_overwritten_string_capacity_retention() {
    let large = vec![b'a'; 4097];
    let mut annotation = Vec::new();
    for body in [
        large.as_slice(),
        b"small".as_slice(),
        b"".as_slice(),
        b"final-key".as_slice(),
    ] {
        bytes(2, body, &mut annotation);
    }
    bytes(3, &vec![b'v'; 1027], &mut annotation);
    let mut raw = Vec::new();
    bytes(21, &annotation, &mut raw);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |dto| {
        dto.annotations.first().map(|value| {
            (
                value.key == "final-key",
                value.key.capacity(),
                value.value.len(),
            )
        })
    });
    assert!(matches!(observed, Some(Some((true, capacity, 1027))) if capacity >= 4097));
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_slice_bytes_double_copy_and_overwrites() {
    let large = vec![b'x'; 1025];
    let final_value = [b'z'; 31];
    let mut raw = Vec::new();
    for body in [
        large.as_slice(),
        b"short".as_slice(),
        b"".as_slice(),
        final_value.as_slice(),
    ] {
        bytes(1, body, &mut raw);
    }
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |dto| {
        (
            dto.plan_version.as_slice() == final_value.as_slice(),
            dto.plan_version.capacity(),
        )
    });
    assert!(matches!(observed, Some((true, capacity)) if capacity >= 1025));
    // The first slice payload allocates both a temporary and destination.
    assert!(stats.cumulative >= 2 * large.len());
    assert!(stats.peak >= 2 * large.len());
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_discarded_oneof_backing() {
    let mut old_struct = Vec::new();
    packed(1, &(0..300).collect::<Vec<u32>>(), &mut old_struct);
    let mut timestamp = Vec::new();
    scalar(1, 1, &mut timestamp);
    bytes(2, &vec![b't'; 1027], &mut timestamp);
    let mut carrier = Vec::new();
    bytes(18, &old_struct, &mut carrier);
    bytes(3, &timestamp, &mut carrier);
    bytes(18, &[], &mut carrier);
    let mut table = Vec::new();
    bytes(1, &carrier, &mut table);
    let mut raw = Vec::new();
    bytes(4, &table, &mut raw);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |dto| {
        dto.types
            .as_ref()
            .and_then(|table| table.carriers.first())
            .and_then(|carrier| match carrier.kind.as_ref() {
                Some(Kind::StructType(fields)) => Some(fields.field_ids.len()),
                _ => None,
            })
    });
    assert_eq!(observed, Some(Some(0)));
    assert!(stats.cumulative >= 300 * std::mem::size_of::<u32>() + 1027);
    assert_bound(&raw, proof, stats);
}

fn prefix_annotation(raw: &mut Vec<u8>) {
    let mut annotation = Vec::new();
    bytes(2, &vec![b'p'; 257], &mut annotation);
    bytes(21, &annotation, raw);
}

#[test]
fn actual_requested_allocations_cover_invalid_utf8_and_partial_dto_drop() {
    let mut raw = Vec::new();
    prefix_annotation(&mut raw);
    let mut invalid = vec![b'a'; 513];
    invalid[512] = 0xff;
    let mut annotation = Vec::new();
    bytes(2, &invalid, &mut annotation);
    bytes(21, &annotation, &mut raw);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |_| ());
    assert_eq!(observed, None);
    assert!(stats.cumulative > invalid.len());
    // Resource projection may leave UTF-8 legality to Prost; either way its
    // prefix must account the allocation before the actual decoder error.
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_malformed_length_after_allocated_prefix() {
    let mut raw = Vec::new();
    prefix_annotation(&mut raw);
    // A types message whose declared payload is longer than the remaining input.
    raw.extend_from_slice(&[0x22, 8, 0x08]);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |_| ());
    assert_eq!(proof.status, ResourceCursorStatus::MalformedPrefix);
    assert_eq!(observed, None);
    assert!(stats.allocations > 0);
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_dynamic_key_and_wire_error_descriptions() {
    let mut invalid_key = Vec::new();
    varint(u64::MAX, &mut invalid_key);
    for raw in [invalid_key.as_slice(), &[0x0e][..]] {
        let proof = projection(raw);
        let (stats, observed) = measure(raw, |_| ());
        assert_eq!(proof.status, ResourceCursorStatus::MalformedPrefix);
        assert_eq!(observed, None);
        assert!(stats.allocations > 0);
        assert!(stats.cumulative >= 50);
        assert_bound(raw, proof, stats);
    }
}

fn short_message(tag: u32, declared: usize, body: &[u8]) -> Vec<u8> {
    let mut raw = Vec::new();
    varint((u64::from(tag) << 3) | 2, &mut raw);
    varint(declared as u64, &mut raw);
    raw.extend_from_slice(body);
    raw
}

#[test]
fn actual_requested_allocations_cover_string_crossing_nested_message_boundary() {
    let payload = vec![b's'; 32769];
    let mut annotation = Vec::new();
    bytes(2, &payload, &mut annotation);
    // Prost's merge_loop passes the original Buf to each field merge. The
    // String copies from outside its enclosing declared payload, then the
    // enclosing loop detects that the field exceeded the logical boundary.
    let raw = short_message(21, annotation.len() - payload.len(), &annotation);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |_| ());
    assert_eq!(proof.status, ResourceCursorStatus::MalformedPrefix);
    assert_eq!(observed, None);
    assert!(stats.cumulative >= payload.len());
    assert!(proof.usage.copied_bytes_upper >= 3 * payload.len());
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_bytes_crossing_nested_message_boundary() {
    let payload = vec![b'b'; 32769];
    let mut constant = Vec::new();
    bytes(5, &payload, &mut constant);
    let raw = short_message(6, constant.len() - payload.len(), &constant);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |_| ());
    assert_eq!(proof.status, ResourceCursorStatus::MalformedPrefix);
    assert_eq!(observed, None);
    assert!(stats.cumulative >= 2 * payload.len());
    assert!(proof.usage.copied_bytes_upper >= 3 * payload.len());
    assert_bound(&raw, proof, stats);
}

#[test]
fn actual_requested_allocations_cover_packed_varint_crossing_logical_boundary() {
    let mut payload = vec![0; 1000];
    payload.extend_from_slice(&[0x80; 9]);
    payload.push(0); // A legal ten-byte nonminimal zero.
    let mut schema = Vec::new();
    varint((2 << 3) | 2, &mut schema);
    varint(1001, &mut schema); // Last item extends nine bytes beyond this end.
    schema.extend_from_slice(&payload);
    let mut raw = Vec::new();
    bytes(5, &schema, &mut raw);
    let proof = projection(&raw);
    let (stats, observed) = measure(&raw, |_| ());
    assert_eq!(proof.status, ResourceCursorStatus::MalformedPrefix);
    assert_eq!(observed, None);
    assert_eq!(proof.usage.scalar_elements, 1001);
    assert!(stats.cumulative >= 1001 * std::mem::size_of::<u32>());
    assert_bound(&raw, proof, stats);
}
