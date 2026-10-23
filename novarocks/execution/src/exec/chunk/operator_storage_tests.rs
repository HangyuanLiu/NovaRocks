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

use super::*;
use arrow::{
    array::{Array, ArrayRef, FixedSizeListArray, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn int_chunk(rows: usize) -> Chunk {
    let field = Field::new("value", DataType::Int32, false);
    let values = (0..rows as i32).collect::<Vec<_>>();
    let backing = values.capacity() * std::mem::size_of::<i32>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(Int32Array::from(values)) as ArrayRef],
    )
    .unwrap();
    let mut chunk = typed_fixture::chunk(
        batch,
        &[novarocks_types::SlotId(1)],
        &[FunctionValueType::new(DataType::Int32, false)],
    );
    chunk
        .attach_source_backing_receipt(ChunkSourceBackingReceipt::from_source(
            backing,
            ValueTypeStorageLifetime::pure(),
        ))
        .unwrap();
    chunk
}
fn limits() -> OperatorStorageLimits {
    OperatorStorageLimits {
        bytes: 128 << 20,
        nodes: 32768,
        depth: 64,
        columns: 8192,
    }
}
#[test]
fn a_tiny_slice_retains_and_charges_its_whole_parent_buffer() {
    let parent = int_chunk(16384);
    let slice = parent
        .slice_typed_observed::<crate::exec::pipeline::operator::ConstructionFailure, _>(
            17,
            1,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap();
    let full = parent.retained_operator_geometry(limits()).unwrap();
    let small = slice.retained_operator_geometry(limits()).unwrap();
    assert_eq!(
        small, full,
        "visible row count cannot release sliced-away allocation capacity"
    );
    let narrow = OperatorStorageLimits {
        bytes: 4096,
        ..limits()
    };
    assert!(slice.retained_operator_geometry(narrow).is_err());
    let raw = crate::exec::pipeline::input::RawInput::for_test(slice);
    assert_eq!(raw.retained_geometry(limits()).unwrap(), full);
}
#[test]
fn operator_profiles_do_not_enlarge_the_root_result_carrier_or_column_policy() {
    use super::root_array_storage::borrowed_batch_storage_profile;
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let field = Arc::new(Field::new("item", DataType::Int32, false));
    let list: ArrayRef = Arc::new(FixedSizeListArray::try_new(field, 2, values, None).unwrap());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "list",
            list.data_type().clone(),
            false,
        )])),
        vec![list],
    )
    .unwrap();
    let bounds = RootArrayStorageLimits {
        bytes: 128 << 20,
        nodes: 32768,
        depth: 64,
    };
    assert_eq!(
        borrowed_root_batch_storage(&batch, bounds),
        Err(RootArrayStorageError::UnsupportedCarrier)
    );
    assert!(borrowed_batch_storage_profile(&batch, bounds, 8192, true, &mut |_| Ok(())).is_ok());
    let fields = (0..4097)
        .map(|n| Field::new(n.to_string(), DataType::Int32, false))
        .collect::<Vec<_>>();
    let columns = (0..4097)
        .map(|_| Arc::new(Int32Array::from(vec![1])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert_eq!(
        borrowed_root_batch_storage(&batch, bounds),
        Err(RootArrayStorageError::WorkExceeded)
    );
    assert!(borrowed_batch_storage_profile(&batch, bounds, 8192, true, &mut |_| Ok(())).is_ok());
}
#[test]
fn storage_holder_follows_real_slice_and_source_loans_until_the_last_array_owner() {
    struct Released(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Released {}
    impl Drop for Released {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let released = Arc::new(AtomicUsize::new(0));
    let mut parent = int_chunk(3);
    let holder = ValueTypeStorageLifetime::held(Arc::new(Released(released.clone())));
    let mut requests = 0;
    parent
        .retain_operator_holder::<crate::exec::pipeline::operator::ConstructionFailure>(
            holder,
            |_| {
                requests += 1;
                Ok(())
            },
        )
        .unwrap();
    assert!(requests <= 1);
    let slice = parent
        .slice_typed_observed::<crate::exec::pipeline::operator::ConstructionFailure, _>(
            0,
            1,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap();
    let loan=slice.source_storage_lifetime_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(ValueTypeStorageLifetime::pure(),&mut |_|Ok(())).unwrap();
    drop(parent);
    drop(slice);
    assert_eq!(released.load(Ordering::SeqCst), 0);
    drop(loan);
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[test]
fn actual_typed_operator_schema_accepts_view_and_fixed_list_without_root_policy() {
    use arrow::array::StringViewArray;
    for values in [
        Arc::new(StringViewArray::from(vec![
            "a string longer than twelve bytes",
        ])) as ArrayRef,
        Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, false)),
                2,
                Arc::new(Int32Array::from(vec![1, 2])),
                None,
            )
            .unwrap(),
        ) as ArrayRef,
    ] {
        let ty = FunctionValueType::new(values.data_type().clone(), false);
        let mut chunk = typed_fixture::columns(
            &["v"],
            &[novarocks_types::SlotId(1)],
            &[ty],
            vec![values],
            1,
        );
        // This fixture itself created every original standard allocation above.
        // Its explicit source fact covers all parent payloads; arbitrary
        // externally supplied arrays cannot elect this construction proof.
        let extent = match chunk.columns()[0].data_type() {
            DataType::Utf8View => {
                let a = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap();
                a.views().inner().capacity()
                    + a.data_buffers().iter().map(|b| b.capacity()).sum::<usize>()
            }
            DataType::FixedSizeList(_, _) => 8,
            _ => unreachable!(),
        };
        chunk
            .attach_source_backing_receipt(ChunkSourceBackingReceipt::from_source(
                extent,
                ValueTypeStorageLifetime::pure(),
            ))
            .unwrap();
        assert!(chunk.retained_operator_geometry(limits()).is_ok());
        assert!(
            borrowed_root_chunk_storage(
                &chunk,
                RootArrayStorageLimits {
                    bytes: 96 << 20,
                    nodes: 8192,
                    depth: 64
                }
            )
            .is_err()
        );
    }
}

#[test]
fn unknown_custom_parent_is_refused_and_explicit_full_parent_receipt_survives_slices() {
    use arrow::buffer::{Buffer, ScalarBuffer};
    use std::ptr::NonNull;
    let mut values = Vec::<i32>::with_capacity(32768);
    values.push(7);
    let full = values.capacity() * std::mem::size_of::<i32>();
    let pointer = NonNull::new(values.as_mut_ptr().cast()).unwrap();
    let owner = Arc::new(values);
    let weak = Arc::downgrade(&owner);
    // SAFETY: The original source retains the initialized aligned i32 region
    // and complete parent allocation until the last custom buffer exits.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, 4, owner) };
    let array: ArrayRef = Arc::new(Int32Array::new(ScalarBuffer::new(buffer, 0, 1), None));
    let mut chunk = typed_fixture::columns(
        &["v"],
        &[novarocks_types::SlotId(1)],
        &[FunctionValueType::new(DataType::Int32, false)],
        vec![array],
        1,
    );
    let error = chunk.retained_operator_geometry(limits()).unwrap_err();
    assert_eq!(
        error.failure_fact().class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert!(error.detail().contains("complete source backing receipt"));
    chunk
        .attach_source_backing_receipt(ChunkSourceBackingReceipt::from_source(
            full,
            ValueTypeStorageLifetime::pure(),
        ))
        .unwrap();
    assert!(chunk.retained_operator_geometry(limits()).unwrap() >= full);
    let sliced = chunk
        .slice_typed_observed::<crate::exec::pipeline::operator::ConstructionFailure, _>(
            0,
            1,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap();
    let projected = sliced.batch_projection().into_chunk();
    assert_eq!(
        projected.source_backing_receipt().unwrap().retained_bytes(),
        full
    );
    let (storage,receipt)=projected.source_storage_with_backing_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(ValueTypeStorageLifetime::pure(),&mut |_|Ok(())).unwrap();
    assert_eq!(receipt.unwrap().retained_bytes(), full);
    drop(chunk);
    drop(sliced);
    assert!(weak.upgrade().is_some());
    assert!(
        projected
            .retained_operator_geometry(OperatorStorageLimits {
                bytes: full - 1,
                ..limits()
            })
            .is_err()
    );
    drop(projected);
    drop(storage);
    assert!(weak.upgrade().is_none());
}
