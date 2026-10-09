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

//! Actual source Chunk lease handoff, separately from wrapping admission.
//! This probe covers one original input custody operation, not whole BY/MEM.
use super::*;
use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use novarocks_functions::{retain_source_backing, SourceBackingOwner};

fn owned_source(source_tracker: &Arc<MemTracker>) -> Chunk {
    let (program, batch) = source(
        &[
            vec![Some(1), Some(1), Some(4)],
            vec![Some(1), Some(2), Some(9)],
        ],
        false,
    );
    let mut source = chunk(&program, batch);
    source.try_transfer_to(source_tracker).unwrap();
    source
}

#[test]
fn by_window_source_custody_actual_chunk_charge_survives_last_buffer_loan() {
    let query = MemTracker::new_root("actual source custody probe");
    query.install_limit_once(1024 * 1024).unwrap();
    let input_tracker = MemTracker::new_child("original input", &query);
    let wrap_tracker = MemTracker::new_child("actual wrapping host", &query);
    let original = owned_source(&input_tracker);
    let array = Arc::clone(original.batch.column(X));
    let stock = input_tracker.current();
    assert!(stock > 0);
    let mut control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    control.bind_mem_tracker(Arc::clone(&wrap_tracker));
    let host = control.allocator().unwrap();
    let source = SourceBackingOwner::try_from_owned(original, host, &control).unwrap();
    let result = retain_source_backing(array, &source, &control).unwrap();
    let data = result.to_data();
    let loan = data.buffers()[0].clone();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[4, 9]
    );
    drop(source);
    drop(result);
    drop(data);
    assert_eq!(
        input_tracker.current(),
        stock,
        "the original Chunk owner is still alive"
    );
    assert!(
        wrap_tracker.current() > 0,
        "real wrapper backing remains admitted"
    );
    drop(loan);
    assert_eq!(input_tracker.current(), 0);
    assert_eq!(wrap_tracker.current(), 0);
    assert_eq!(query.current(), 0);
}

#[test]
fn by_window_source_custody_actual_metadata_refusal_drops_original_chunk_owner() {
    let query = MemTracker::new_root("source custody refusal");
    query.install_limit_once(1024 * 1024).unwrap();
    let input_tracker = MemTracker::new_child("original input", &query);
    let wrap_tracker = MemTracker::new_child("refused wrapping", &query);
    wrap_tracker.install_limit_once(1).unwrap();
    let original = owned_source(&input_tracker);
    assert!(input_tracker.current() > 0);
    let mut control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    control.bind_mem_tracker(Arc::clone(&wrap_tracker));
    let host = control.allocator().unwrap();
    let cause = match SourceBackingOwner::try_from_owned(original, host, &control) {
        Ok(_) => panic!("actual metadata allocation must be refused"),
        Err(cause) => cause,
    };
    assert_eq!(cause, novarocks_functions::KernelFailure::ResourceExhausted);
    assert_eq!(input_tracker.current(), 0);
    assert_eq!(wrap_tracker.current(), 0);
    assert_eq!(query.current(), 0);
}
