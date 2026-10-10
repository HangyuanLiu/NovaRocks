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

//! Actual Chunk accounting is the input owner. New copied output backing is
//! separately admitted through the SAME production control allocator.
use super::*;
use novarocks_functions::selected_copy::{CopyIndices, CopyOperationError, take_copy_in};
use arrow::array::UInt32Array;

#[test]
fn by_copy_joint_actual_chunk_input_and_new_output_keep_distinct_lifetimes() {
    let query = MemTracker::new_root("joint take actual query");
    query.install_limit_once(1024 * 1024).unwrap();
    let input_tracker = MemTracker::new_child("original Chunk", &query);
    let copy_tracker = MemTracker::new_child("actual copy host", &query);
    let original = owned_source(&input_tracker);
    let array = Arc::clone(original.batch.column(X));
    let original_stock = input_tracker.current();
    assert!(original_stock > 0);
    let mut control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    control.bind_mem_tracker(Arc::clone(&copy_tracker));
    let host = control.allocator().unwrap();
    let source = SourceBackingOwner::try_from_owned(original, host.clone(), &control).unwrap();
    let result = take_copy_in(
        array,
        CopyIndices::UInt32(Arc::new(UInt32Array::from(vec![1_u32, 0, 1]))),
        source,
        host,
        &control,
    )
    .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[9, 4, 9]
    );
    let result = result.into_values();
    let slice = result.slice(1, 1);
    let data = slice.to_data();
    let buffer = data.buffers()[0].clone();
    drop(data);
    drop(slice);
    drop(result);
    assert_eq!(
        input_tracker.current(),
        original_stock,
        "actual input Chunk lease has not been released"
    );
    assert!(
        copy_tracker.current() > 0,
        "new output still belongs to the true host"
    );
    drop(buffer);
    assert_eq!(input_tracker.current(), 0);
    assert_eq!(copy_tracker.current(), 0);
    assert_eq!(query.current(), 0);
}
#[test]
fn by_copy_joint_actual_chunk_refusal_releases_original_input_without_output() {
    let query = MemTracker::new_root("joint take refused query");
    let input_tracker = MemTracker::new_child("original Chunk", &query);
    let copy_tracker = MemTracker::new_child("refused actual copy", &query);
    let original = owned_source(&input_tracker);
    let array = Arc::clone(original.batch.column(X));
    let mut control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    control.bind_mem_tracker(Arc::clone(&copy_tracker));
    let host = control.allocator().unwrap();
    let source = SourceBackingOwner::try_from_owned(original, host.clone(), &control).unwrap();
    // Policy is installed from the actual constructor bill after that separate
    // original input owner was admitted. No guessed metadata ordinal or wallet.
    copy_tracker
        .install_limit_once(copy_tracker.current() + 1)
        .unwrap();
    assert!(matches!(
        take_copy_in(
            array,
            CopyIndices::UInt32(Arc::new(UInt32Array::from(vec![1_u32, 0]))),
            source,
            host,
            &control
        ),
        Err(CopyOperationError::Control(
            novarocks_functions::KernelFailure::ResourceExhausted
        ))
    ));
    assert_eq!(input_tracker.current(), 0);
    assert_eq!(copy_tracker.current(), 0);
    assert_eq!(query.current(), 0);
}
