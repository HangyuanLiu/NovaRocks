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

//! Exact count-family key and decoder computation, independent of the shell.
use super::*;
use allocator_api2::alloc::{AllocError, Allocator, Global};
use std::{alloc::Layout, ptr::NonNull};
#[derive(Clone, Debug)]
struct TestAllocator;
unsafe impl Allocator for TestAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(layout)
    }
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(ptr, layout) }
    }
}
impl ScalarStateAllocator for TestAllocator {
    fn scalar_allocation_error(&self, operation: &str) -> ScalarStateError {
        ScalarStateError::Legacy(operation.to_string())
    }
}

#[test]
fn count_distinct_core_high_cardinality_observation_work_stays_linear() {
    use crate::kernel_input::EvaluationCheckpoints;
    use crate::{KernelEvaluationControl, KernelFailure};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct Observations(AtomicUsize);
    impl KernelEvaluationControl for Observations {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= 256);
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("COUNT DISTINCT does not wait")
        }
    }

    let rows = 4096;
    let control = Observations(AtomicUsize::new(0));
    let mut checkpoints = EvaluationCheckpoints::new(&control);
    let mut state = CountDistinctState::<TestAllocator>::new(TestAllocator);
    for value in 0..rows {
        state
            .insert_with_work(
                &(value as u64).to_le_bytes(),
                &mut ScalarWork::new(Some(&mut checkpoints)),
            )
            .unwrap();
    }
    checkpoints.finish().unwrap();
    assert_eq!(state.len(), rows);
    let observations = control.0.load(Ordering::Relaxed);
    // Fixed-width unique keys must not revisit every prior key's work scope.
    // This bounds observation overhead, not opaque hash-table instructions.
    assert!(
        observations <= rows * 16,
        "{rows} unique keys required {observations} observations"
    );
}

#[test]
fn count_distinct_core_borrowed_lookup_matches_original_byte_scan_and_hash() {
    use std::hash::BuildHasher;

    let keys = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 1],
        vec![255],
        (0..=255).collect(),
        vec![42; 4096],
        [vec![42; 4096], vec![0]].concat(),
        [vec![42; 4096], vec![1]].concat(),
    ];
    let mut state = CountDistinctState::<TestAllocator>::new(TestAllocator);
    for key in &keys {
        state.insert(key.clone()).unwrap();
    }
    let original = serialize_set(&state, Vec::new(), &mut ScalarWork::new(None)).unwrap();
    for key in state.iter() {
        assert_eq!(
            state.values.hasher().hash_one(key),
            state
                .values
                .hasher()
                .hash_one(BorrowedCountKey(key.as_slice())),
            "borrowed query must use the original stored key hash"
        );
    }
    let mut queries = keys.clone();
    for key in &keys {
        let mut missing = key.clone();
        missing.push(255);
        queries.push(missing);
        if !key.is_empty() {
            queries.push(key[..key.len() - 1].to_vec());
            let mut changed = key.clone();
            changed[0] ^= 1;
            queries.push(changed);
        }
    }
    let capacity = state.table_bytes();
    for query in queries {
        let original_scan = state.iter().any(|key| key.as_slice() == query.as_slice());
        assert_eq!(
            state.values.contains(&BorrowedCountKey(&query)),
            original_scan
        );
    }
    assert_eq!(state.len(), keys.len());
    assert_eq!(state.table_bytes(), capacity);
    assert_eq!(
        serialize_set(&state, Vec::new(), &mut ScalarWork::new(None)).unwrap(),
        original,
        "read-only lookup must not change the same state's iteration or codec"
    );
}

#[test]
fn count_distinct_core_float_bits_and_tolerant_variable_key_wire() {
    let input = std::sync::Arc::new(Float64Array::from(vec![
        0.0,
        -0.0,
        f64::from_bits(0x7ff8_0000_0000_0123),
        f64::from_bits(0x7ff8_0000_0000_0124),
    ])) as ArrayRef;
    let mut state = CountDistinctState::<TestAllocator>::new(TestAllocator);
    for row in 0..input.len() {
        let key = encode_row(
            &input,
            row,
            &LegacyCountReader,
            Vec::new(),
            &mut ScalarWork::new(None),
        )
        .unwrap()
        .unwrap();
        state.insert(key).unwrap();
    }
    assert_eq!(finalize_count(&state), 4);
    let mut bytes = serialize_set(&state, Vec::new(), &mut ScalarWork::new(None)).unwrap();
    bytes.extend_from_slice(b"trailing");
    let keys = deserialize_set(&bytes, Vec::<Vec<u8>>::new(), &mut ScalarWork::new(None)).unwrap();
    let mut merged = CountDistinctState::<TestAllocator>::new(TestAllocator);
    merge_decoded(&mut merged, keys, &mut ScalarWork::new(None)).unwrap();
    assert_eq!(finalize_count(&merged), 4);
}
#[test]
fn count_distinct_core_one_recursive_encoder_matches_legacy_and_tracked_values() {
    use arrow_schema::{Field, Fields};
    let child = std::sync::Arc::new(Int32Array::from(vec![None, Some(1)])) as ArrayRef;
    let input = std::sync::Arc::new(StructArray::new(
        Fields::from(vec![Field::new("child", DataType::Int32, true)]),
        vec![child],
        None,
    )) as ArrayRef;
    for row in 0..2 {
        let legacy = encode_row(
            &input,
            row,
            &LegacyCountReader,
            Vec::new(),
            &mut ScalarWork::new(None),
        )
        .unwrap();
        let tracked = encode_row(
            &input,
            row,
            &TrackedCountReader(&TestAllocator),
            StateVec::new_in(TestAllocator),
            &mut ScalarWork::new(None),
        )
        .unwrap();
        assert_eq!(
            legacy.as_deref(),
            tracked.as_ref().map(|key| key.as_slice())
        );
    }
}
#[test]
fn count_distinct_core_decoder_keeps_empty_duplicate_trailing_keys_and_rejects_truncation() {
    let bytes = [
        3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, b'x', 1, 0, 0, 0, b'x', 0xff,
    ];
    let legacy =
        deserialize_set(&bytes, Vec::<Vec<u8>>::new(), &mut ScalarWork::new(None)).unwrap();
    let tracked = deserialize_set(
        &bytes,
        StateVec::<StateVec<u8, TestAllocator>, _>::new_in(TestAllocator),
        &mut ScalarWork::new(None),
    )
    .unwrap();
    assert_eq!(legacy, vec![vec![], vec![b'x'], vec![b'x']]);
    assert_eq!(
        legacy.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        tracked.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );
    for bytes in [&bytes[..7], &bytes[..16]] {
        assert_eq!(
            deserialize_set(bytes, Vec::<Vec<u8>>::new(), &mut ScalarWork::new(None))
                .unwrap_err()
                .to_string(),
            "invalid distinct set encoding"
        );
    }
}
#[test]
fn count_distinct_core_variadic_tuple_is_the_original_packed_struct_key() {
    use arrow_schema::{Field, Fields};
    let left = std::sync::Arc::new(Int64Array::from(vec![Some(7), None])) as ArrayRef;
    let right = std::sync::Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef;
    let packed = std::sync::Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("f0", DataType::Int64, true),
            Field::new("f1", DataType::Utf8, true),
        ]),
        vec![left.clone(), right.clone()],
        None,
    )) as ArrayRef;
    for row in 0..2 {
        let legacy = encode_row(
            &packed,
            row,
            &LegacyCountReader,
            Vec::new(),
            &mut ScalarWork::new(None),
        )
        .unwrap();
        let tuple = encode_tuple(
            &[(&left, row), (&right, row)],
            &TestAllocator,
            StateVec::new_in(TestAllocator),
            &mut ScalarWork::new(None),
        )
        .unwrap();
        assert_eq!(legacy.as_deref(), tuple.as_ref().map(|key| key.as_slice()));
    }
}
