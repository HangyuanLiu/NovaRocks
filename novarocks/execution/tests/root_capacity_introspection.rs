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

fn storage_limits(bytes: usize) -> novarocks_execution::exec::chunk::RootArrayStorageLimits {
    novarocks_execution::exec::chunk::RootArrayStorageLimits {
        bytes,
        nodes: 65536,
        depth: 64,
    }
}

#[test]
fn borrowed_storage_rejects_sliced_away_standard_capacity_without_cell_scan() {
    use arrow::buffer::ScalarBuffer;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let mut backing = Vec::<i32>::with_capacity(32768);
    backing.push(42);
    let capacity = backing.capacity() * std::mem::size_of::<i32>();
    let array = Int32Array::new(ScalarBuffer::from(backing), None).slice(0, 1);
    assert_eq!(array.len(), 1);
    let bytes =
        no_allocation(|| borrowed_root_array_storage(&array, storage_limits(capacity + 4096)))
            .unwrap();
    assert!(bytes >= capacity);
    assert_eq!(
        no_allocation(|| { borrowed_root_array_storage(&array, storage_limits(capacity - 1)) }),
        Err(RootArrayStorageError::CapacityExceeded)
    );
}

#[test]
fn borrowed_storage_rejects_unknown_custom_buffer_without_copy_or_hydration() {
    use arrow::buffer::ScalarBuffer;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let mut backing = Vec::<i32>::with_capacity(32768);
    backing.push(42);
    let pointer = NonNull::new(backing.as_mut_ptr().cast::<u8>()).unwrap();
    let owner = Arc::new(backing);
    let weak = Arc::downgrade(&owner);
    // SAFETY: The owner retains the initialized aligned i32 value and all
    // backing until the final array/buffer alias exits.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, 4, owner) };
    let array = Int32Array::new(ScalarBuffer::new(buffer, 0, 1), None);
    assert_eq!(
        no_allocation(|| { borrowed_root_array_storage(&array, storage_limits(96 * 1024 * 1024)) }),
        Err(RootArrayStorageError::UnknownBufferOwner)
    );
    assert!(weak.upgrade().is_some());
    drop(array);
    assert!(weak.upgrade().is_none());
}

#[test]
fn dictionary_storage_includes_unreferenced_complete_values_backing() {
    use arrow::array::{DictionaryArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Int32Type;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let mut backing = Vec::<u8>::with_capacity(1024 * 1024);
    backing.extend_from_slice(b"aunused");
    let capacity = backing.capacity();
    let values = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0_i32, 1, 7].into()),
        Buffer::from_vec(backing),
        None,
    )) as ArrayRef;
    let dictionary =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), values).unwrap();
    let bytes =
        no_allocation(|| borrowed_root_array_storage(&dictionary, storage_limits(capacity + 4096)))
            .unwrap();
    assert!(bytes >= capacity);
    assert_eq!(
        no_allocation(|| {
            borrowed_root_array_storage(&dictionary, storage_limits(capacity - 1))
        }),
        Err(RootArrayStorageError::CapacityExceeded)
    );
}

#[test]
fn borrowed_storage_counts_struct_spare_and_stops_at_finite_work() {
    use novarocks_execution::exec::chunk::{
        RootArrayStorageError, RootArrayStorageLimits, borrowed_root_array_storage,
    };
    let mut columns = Vec::with_capacity(8192);
    columns.push(Arc::new(Int32Array::from(vec![1])) as ArrayRef);
    let spare_bytes = columns.capacity() * std::mem::size_of::<ArrayRef>();
    let structure = StructArray::try_new(
        vec![Arc::new(Field::new("v", DataType::Int32, false))].into(),
        columns,
        None,
    )
    .unwrap();
    let bytes = no_allocation(|| {
        borrowed_root_array_storage(&structure, storage_limits(spare_bytes + 4096))
    })
    .unwrap();
    assert!(bytes >= spare_bytes);
    for (nodes, depth) in [(1, 64), (65536, 0)] {
        assert_eq!(
            no_allocation(|| {
                borrowed_root_array_storage(
                    &structure,
                    RootArrayStorageLimits {
                        bytes: spare_bytes + 4096,
                        nodes,
                        depth,
                    },
                )
            }),
            Err(RootArrayStorageError::WorkExceeded)
        );
    }
}

#[test]
fn borrowed_storage_includes_list_child_and_sliced_null_backing() {
    use arrow::array::ListArray;
    use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use novarocks_execution::exec::chunk::borrowed_root_array_storage;
    let mut values = Vec::<i32>::with_capacity(8192);
    values.push(5);
    let values_capacity = values.capacity() * std::mem::size_of::<i32>();
    let mut validity = Vec::<u8>::with_capacity(8192);
    validity.push(0xff);
    let null_capacity = validity.capacity();
    let nulls = NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(validity), 0, 1));
    let values = Arc::new(Int32Array::new(ScalarBuffer::from(values), Some(nulls))) as ArrayRef;
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0_i32, 1].into()),
        values,
        None,
    )
    .unwrap();
    let bytes = no_allocation(|| {
        borrowed_root_array_storage(
            &list,
            storage_limits(values_capacity + null_capacity + 4096),
        )
    })
    .unwrap();
    assert!(bytes >= values_capacity + null_capacity);
}

#[test]
fn storage_inspection_does_not_accept_unclosed_view_carriers() {
    use arrow::array::StringViewArray;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let view = StringViewArray::from(vec!["value"]);
    assert_eq!(
        no_allocation(|| { borrowed_root_array_storage(&view, storage_limits(96 * 1024 * 1024)) }),
        Err(RootArrayStorageError::UnsupportedCarrier)
    );
}

#[test]
fn physical_map_entries_do_not_spend_an_extra_semantic_depth_level() {
    use arrow::array::{Array, MapArray};
    use arrow::buffer::OffsetBuffer;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let mut child = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    for _ in 0..64 {
        let fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", child.data_type().clone(), false)),
        ]
        .into();
        let entries = StructArray::try_new(
            fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef, child],
            None,
        )
        .unwrap();
        child = Arc::new(
            MapArray::try_new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                entries,
                None,
                false,
            )
            .unwrap(),
        );
    }
    let bytes =
        no_allocation(|| borrowed_root_array_storage(child.as_ref(), storage_limits(1024 * 1024)))
            .expect("64 legal semantic Map levels");
    assert!(bytes > 0);
    let mut limits = storage_limits(1024 * 1024);
    limits.depth = 63;
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(child.as_ref(), limits)),
        Err(RootArrayStorageError::WorkExceeded)
    );
}

#[test]
fn dictionary_physical_keys_do_not_spend_the_last_list_semantic_level() {
    use arrow::array::{DictionaryArray, ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Int32Type;
    use novarocks_execution::exec::chunk::borrowed_root_array_storage;
    let mut child = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["value"])),
        )
        .unwrap(),
    ) as ArrayRef;
    for _ in 0..64 {
        child = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", child.data_type().clone(), false)),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                child,
                None,
            )
            .unwrap(),
        );
    }
    no_allocation(|| borrowed_root_array_storage(child.as_ref(), storage_limits(1024 * 1024)))
        .expect("dictionary leaf at semantic depth 64");
}

#[test]
fn forwarded_as_any_cannot_hide_the_actual_outer_owner() {
    use arrow::array::Array;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let inner = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    // Arrow's ArrayRef trait implementation forwards as_any to the concrete
    // array, but this borrowed wrapper is a distinct outer object. Callers
    // must inspect inner.as_ref(), or supply an explicit source-owner proof.
    let wrapper = &inner as &dyn Array;
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(wrapper, storage_limits(4096))),
        Err(RootArrayStorageError::UnsupportedCarrier)
    );
    no_allocation(|| borrowed_root_array_storage(inner.as_ref(), storage_limits(4096))).unwrap();
}

#[test]
fn caller_cannot_relax_the_frozen_structural_work_ceiling() {
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let array = Int32Array::from(vec![1]);
    let mut limits = storage_limits(4096);
    limits.nodes = 0;
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(&array, limits)),
        Err(RootArrayStorageError::WorkExceeded)
    );
    // Array aliases stay alive; the fixed maximum prevents repeated references
    // from turning a single root inspection into unbounded structural work.
    let count = 2 * novarocks_result_contract::RootProfileV1::SCHEMA_TYPE_NODES;
    let children = vec![Arc::new(array) as ArrayRef; count];
    let fields = (0..count)
        .map(|_| Arc::new(Field::new("", DataType::Int32, false)))
        .collect::<Vec<_>>()
        .into();
    let structure = StructArray::try_new(fields, children, None).unwrap();
    limits.bytes = usize::MAX;
    limits.nodes = usize::MAX;
    limits.depth = usize::MAX;
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(&structure, limits)),
        Err(RootArrayStorageError::WorkExceeded)
    );
}

#[test]
fn standard_owner_metadata_descriptor_is_borrowed_and_custom_is_unknown() {
    let buffer = Buffer::from_vec(vec![1_u8]);
    let metadata = no_allocation(|| buffer.standard_owner_metadata_size()).unwrap();
    assert!(metadata > 0);
    assert_eq!(
        no_allocation(|| buffer.slice(0).standard_owner_metadata_size()),
        Some(metadata)
    );
    let mut backing = Vec::<u8>::with_capacity(8192);
    backing.push(1);
    let ptr = NonNull::new(backing.as_mut_ptr()).unwrap();
    // SAFETY: The owner retains the initialized declared region and the larger
    // whole backing for the complete custom buffer lifetime.
    let custom = unsafe { Buffer::from_custom_allocation(ptr, 1, Arc::new(backing)) };
    assert_eq!(
        no_allocation(|| custom.standard_owner_metadata_size()),
        None
    );
}

#[test]
fn whole_batch_storage_checks_real_spare_column_backing_and_shared_work_limit() {
    use novarocks_execution::exec::chunk::{
        RootArrayStorageError, RootArrayStorageLimits, borrowed_root_batch_storage,
    };
    let array = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let mut columns = Vec::with_capacity(8192);
    columns.push(Arc::clone(&array));
    let spare_bytes = columns.capacity() * std::mem::size_of::<ArrayRef>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
        columns,
    )
    .unwrap();
    let bound =
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(spare_bytes + 4096)))
            .unwrap();
    assert!(bound >= spare_bytes);
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(spare_bytes - 1))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ])),
        vec![Arc::clone(&array), array],
    )
    .unwrap();
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(
            &batch,
            RootArrayStorageLimits {
                bytes: 4096,
                nodes: 1,
                depth: 64,
            }
        )),
        Err(RootArrayStorageError::WorkExceeded)
    );
    assert!(
        no_allocation(|| borrowed_root_batch_storage(
            &batch,
            RootArrayStorageLimits {
                bytes: 4096,
                nodes: 2,
                depth: 64,
            }
        ))
        .is_ok()
    );
}

#[test]
fn whole_batch_storage_rejects_wide_inputs_without_scanning_columns() {
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_batch_storage};
    let schema = Schema::new(
        (0..=novarocks_result_contract::RootProfileV1::MAX_COLUMNS)
            .map(|i| Field::new(i.to_string(), DataType::Int32, false))
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::new_empty(Arc::new(schema));
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(96 * 1024 * 1024))),
        Err(RootArrayStorageError::WorkExceeded)
    );
}
