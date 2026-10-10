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

//! Original take NULL-index uses differ between Run, sparse Union and dense
//! Union. The hosted comparison is intentionally RED until those exact uses
//! and diagnostic backing are admitted by the real host.
use super::*;
use arrow::buffer::NullBuffer;

fn nullable(raw: Vec<u32>, valid: Vec<bool>) -> Arc<UInt32Array> {
    Arc::new(UInt32Array::new(raw.into(), Some(NullBuffer::from(valid))))
}
fn run_source() -> ArrayRef {
    Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1, 3]),
            &Int32Array::from(vec![10, 20]),
        )
        .unwrap(),
    )
}
fn union_source(dense: bool, zero_id: bool) -> ArrayRef {
    let ids = if zero_id { [0, 127] } else { [1, 127] };
    let fields = UnionFields::try_new(
        ids,
        [
            Field::new("first", DataType::Int64, true),
            Field::new("last", DataType::Int64, true),
        ],
    )
    .unwrap();
    let (offsets, children): (Option<ScalarBuffer<i32>>, Vec<ArrayRef>) = if dense {
        (
            Some(vec![0_i32, 0, 1].into()),
            vec![
                Arc::new(Int64Array::from(vec![1, 3])),
                Arc::new(Int64Array::from(vec![4])),
            ],
        )
    } else {
        (
            None,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 5, 6])),
            ],
        )
    };
    Arc::new(
        UnionArray::try_new(fields, vec![ids[0], 127, ids[0]].into(), offsets, children).unwrap(),
    )
}

#[test]
fn by_original_take_null_run_reads_raw_zero_and_hidden_oob_returns_full_data() {
    let source = run_source();
    let indices = nullable(vec![0, 2], vec![false, true]);
    let output = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
    let output = output
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    assert_eq!(output.run_ends().values(), &[1_i16, 2]);
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[10_i32, 20]);
    assert_eq!(values.null_count(), 0);
    let indices = nullable(vec![99, 0], vec![false, true]);
    let error = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid argument error: Logical index 99 is out of bounds for RunArray of length 3"
    );
}

#[test]
fn by_original_take_null_sparse_union_preserves_raw_type_id_and_child_validity() {
    let source = union_source(false, true);
    let indices = nullable(vec![2, 1, 99], vec![false, true, false]);
    let output = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
    let output = output.as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(output.type_ids().as_ref(), &[0_i8, 127, 0]);
    for (id, middle) in [(0, 2_i64), (127, 5_i64)] {
        let child = output
            .child(id)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            child.iter().collect::<Vec<_>>(),
            vec![None, Some(middle), None]
        );
    }
}

#[test]
fn by_original_take_null_dense_union_uses_raw_offsets_then_nonnull_children() {
    let source = union_source(true, true);
    let indices = nullable(vec![2, 1, 99], vec![false, true, false]);
    let output = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
    let output = output.as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(output.type_ids().as_ref(), &[0_i8, 127, 0]);
    assert_eq!(output.offsets().unwrap().as_ref(), &[0_i32, 0, 1]);
    assert_eq!(
        output
            .child(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(3), Some(1)]
    );
    assert_eq!(
        output
            .child(127)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(4)]
    );
}

#[test]
fn by_original_take_null_sparse_union_default_zero_can_produce_whole_data() {
    let source = union_source(false, false);
    let indices = nullable(vec![99], vec![false]);
    let error = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid argument error: Type Ids values must match one of the field type ids"
    );
}

#[test]
fn by_copy_joint_take_null_actual_original_all_four_success_and_data_paths() {
    for (source, indices) in [
        (run_source(), nullable(vec![0, 2], vec![false, true])),
        (run_source(), nullable(vec![99, 0], vec![false, true])),
        (
            union_source(false, true),
            nullable(vec![2, 1, 99], vec![false, true, false]),
        ),
        (
            union_source(true, true),
            nullable(vec![2, 1, 99], vec![false, true, false]),
        ),
        (union_source(false, false), nullable(vec![99], vec![false])),
    ] {
        let expected = arrow::compute::take(source.as_ref(), indices.as_ref(), None);
        let tr = tracker();
        let host = Host::new(tr.clone(), None);
        let control = Control::new(tr, None);
        let actual = take_copy_in(
            source,
            CopyIndices::UInt32(indices),
            (),
            host.clone(),
            &control,
        );
        match (expected, actual) {
            (Ok(expected), Ok(actual)) => {
                assert_eq!(actual.values().to_data(), expected.to_data());
                drop(actual);
            }
            (Err(expected), Err(CopyOperationError::OriginalData(actual))) => {
                assert_eq!(actual, expected.to_string());
                drop(actual);
            }
            (expected, actual) => panic!(
                "original/hosted NULL-index mismatch: expected={expected:?}, actual={}",
                match actual {
                    Ok(_) => "success".into(),
                    Err(error) => format!("{error:?}"),
                }
            ),
        }
        assert_released(&host);
    }
}
