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

use arrow::array::{ArrayRef, Int32Array, StructArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Arc;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
}
struct AllocationProbe;
// SAFETY: The wrapper delegates every allocator operation unchanged and only
// observes a thread-local counter, without allocating inside the observation.
unsafe impl GlobalAlloc for AllocationProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = CALLS.try_with(|c| c.set(c.get() + 1));
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = CALLS.try_with(|c| c.set(c.get() + 1));
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = CALLS.try_with(|c| c.set(c.get() + 1));
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: AllocationProbe = AllocationProbe;
fn no_allocation<T>(f: impl FnOnce() -> T) -> T {
    CALLS.with(|c| c.set(0));
    TRACK.with(|c| c.set(true));
    let value = f();
    TRACK.with(|c| c.set(false));
    assert_eq!(
        CALLS.with(Cell::get),
        0,
        "a borrowed capacity proof must not allocate"
    );
    value
}
#[test]
fn standard_buffer_slice_reports_complete_allocator_layout_without_copy() {
    let mut values = Vec::<u8>::with_capacity(8192);
    values.extend_from_slice(b"payload");
    let capacity = values.capacity();
    let buffer = Buffer::from_vec(values);
    let alias = buffer.slice_with_length(2, 1);
    assert_eq!(alias.len(), 1);
    assert_eq!(
        no_allocation(|| alias.standard_allocation_capacity()),
        Some(capacity)
    );
    assert_eq!(
        no_allocation(|| buffer.standard_allocation_capacity()),
        Some(capacity)
    );
    assert_eq!(alias.data_ptr(), buffer.data_ptr());
}
#[test]
fn custom_buffer_declared_region_is_never_a_backing_capacity_proof() {
    let mut backing = Vec::<u8>::with_capacity(8192);
    backing.push(7);
    let pointer = NonNull::new(backing.as_mut_ptr()).unwrap();
    let owner = Arc::new(backing);
    let weak = Arc::downgrade(&owner);
    // SAFETY: owner retains the initialized one-byte region at pointer and
    // its complete backing until the last custom buffer alias is dropped.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, 1, owner) };
    let alias = buffer.clone();
    assert_eq!(buffer.capacity(), 1);
    assert_eq!(
        no_allocation(|| buffer.standard_allocation_capacity()),
        None
    );
    assert_eq!(no_allocation(|| alias.standard_allocation_capacity()), None);
    drop(buffer);
    assert!(weak.upgrade().is_some());
    drop(alias);
    assert!(weak.upgrade().is_none());
}
#[test]
fn record_batch_reports_original_column_vector_spare_without_clone() {
    let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
    let mut columns = Vec::with_capacity(8192);
    columns.push(Arc::clone(&array));
    let capacity = columns.capacity();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        columns,
    )
    .unwrap();
    assert_eq!(batch.columns().len(), 1);
    assert_eq!(no_allocation(|| batch.columns_capacity()), capacity);
    assert!(Arc::ptr_eq(&batch.columns()[0], &array));
    assert_eq!(no_allocation(|| batch.columns_capacity()), capacity);
}
#[test]
fn struct_array_reports_original_child_vector_spare_without_clone() {
    let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
    let mut columns = Vec::with_capacity(8192);
    columns.push(Arc::clone(&array));
    let capacity = columns.capacity();
    let structure = StructArray::try_new(
        vec![Arc::new(Field::new("value", DataType::Int32, false))].into(),
        columns,
        None,
    )
    .unwrap();
    assert_eq!(structure.columns().len(), 1);
    assert_eq!(no_allocation(|| structure.columns_capacity()), capacity);
    assert!(Arc::ptr_eq(&structure.columns()[0], &array));
    assert_eq!(no_allocation(|| structure.columns_capacity()), capacity);
}

#[test]
fn field_replacement_metadata_does_not_duplicate_unknown_original_table() {
    let mut original = std::collections::HashMap::with_capacity(16384);
    original.insert("original".to_string(), "value".to_string());
    let field = Field::new("", DataType::Int32, true).with_metadata(original);
    let replacement =
        std::collections::HashMap::from([("replacement".to_string(), "bounded".to_string())]);
    let cloned = no_allocation(|| field.clone_with_metadata(replacement));
    assert_eq!(cloned.name(), field.name());
    assert_eq!(cloned.data_type(), field.data_type());
    assert_eq!(cloned.is_nullable(), field.is_nullable());
    assert_eq!(
        cloned.metadata().get("replacement").map(String::as_str),
        Some("bounded")
    );
    assert!(!cloned.metadata().contains_key("original"));
    assert_eq!(
        field.metadata().get("original").map(String::as_str),
        Some("value")
    );
}

#[test]
#[allow(deprecated)]
fn field_replacement_metadata_preserves_exact_dictionary_ipc_properties() {
    let field = Field::new_dict(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
        77,
        true,
    );
    let cloned = field.clone_with_metadata(std::collections::HashMap::new());
    assert_eq!(cloned.name(), field.name());
    assert_eq!(cloned.data_type(), field.data_type());
    assert_eq!(cloned.is_nullable(), field.is_nullable());
    assert_eq!(cloned.dict_id(), field.dict_id());
    assert_eq!(cloned.dict_is_ordered(), field.dict_is_ordered());
}
