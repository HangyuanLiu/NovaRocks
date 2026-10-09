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
//! Original scalar percentile reader before its missing pure owner is installed.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::object::eval_object_function;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn setup(arrays: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let slots = (0..arrays.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect::<Vec<_>>();
    let fields = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("original_{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays.clone()).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let args = arrays
        .iter()
        .zip(slots)
        .map(|(a, s)| arena.push_typed(ExprNode::SlotId(s), a.data_type().clone()))
        .collect();
    (arena, args, chunk)
}
fn raw(payloads: ArrayRef, quantiles: ArrayRef) -> Result<ArrayRef, String> {
    let (arena, args, chunk) = setup(vec![payloads, quantiles]);
    eval_object_function(
        "percentile_approx_raw",
        &arena,
        ExprId(usize::MAX),
        &args,
        &chunk,
    )
}
fn binary(v: &[Option<Vec<u8>>]) -> ArrayRef {
    Arc::new(BinaryArray::from_iter(v.iter().map(|v| v.as_deref())))
}
fn singleton() -> Vec<u8> {
    novarocks_functions::approx_percentile_core::encode_single_value(10.)
}
fn floats(a: ArrayRef) -> Vec<Option<f64>> {
    assert_eq!(a.data_type(), &DataType::Float64);
    a.as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn legacy_percentile_approx_raw_original_payload_numeric_null_and_invalid_quantile() {
    let p = binary(&[
        Some(singleton()),
        None,
        Some(vec![]),
        Some(singleton()),
        Some(singleton()),
        Some(singleton()),
    ]);
    let q = Arc::new(Float64Array::from(vec![
        Some(0.5),
        Some(0.5),
        Some(0.5),
        None,
        Some(2.),
        Some(f64::NAN),
    ]));
    assert_eq!(
        floats(raw(p, q).unwrap()),
        vec![Some(10.), None, None, None, None, None]
    );
}
#[test]
fn legacy_percentile_approx_raw_original_four_payload_carriers_slice_and_empty() {
    let empty = novarocks_functions::approx_percentile_core::encode_empty_state();
    let arrays: Vec<ArrayRef> = vec![
        binary(&[Some(singleton()), None, Some(empty.clone())]),
        Arc::new(LargeBinaryArray::from_iter(
            [Some(singleton()), None, Some(empty)]
                .iter()
                .map(|v| v.as_deref()),
        )),
        Arc::new(StringArray::from(vec![Some(""), None, Some("bad")])),
        Arc::new(LargeStringArray::from(vec![Some(""), None, Some("bad")])),
    ];
    for a in arrays {
        let q = Arc::new(Float64Array::from(vec![0.5; 3])) as ArrayRef;
        if matches!(a.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            assert_eq!(
                raw(a.clone(), q.clone()).unwrap_err(),
                "percentile state payload too short: expected>=11 actual=3"
            );
        } else {
            assert_eq!(
                floats(raw(a.clone(), q.clone()).unwrap()),
                vec![Some(10.), None, None]
            );
        }
        assert_eq!(floats(raw(a.slice(0, 2), q.slice(0, 2)).unwrap()).len(), 2);
        assert!(raw(a.slice(0, 0), q.slice(0, 0)).unwrap().is_empty());
    }
}
#[test]
fn legacy_percentile_approx_raw_original_all_numeric_quantile_readers() {
    let qs: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(0), None])),
        Arc::new(Int16Array::from(vec![Some(0), None])),
        Arc::new(Int32Array::from(vec![Some(0), None])),
        Arc::new(Int64Array::from(vec![Some(0), None])),
        Arc::new(Float32Array::from(vec![Some(0.5), None])),
        Arc::new(Float64Array::from(vec![Some(0.5), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(50), None])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ),
        novarocks_functions::largeint::array_from_i128(&[Some(0), None]).unwrap(),
    ];
    for q in qs {
        assert_eq!(
            floats(raw(binary(&[Some(singleton()), Some(singleton())]), q).unwrap()),
            vec![Some(10.), None]
        );
    }
}
#[test]
fn legacy_percentile_approx_raw_original_payload_then_quantile_even_payload_null() {
    assert_eq!(
        raw(binary(&[None]), Arc::new(UInt32Array::from(vec![None]))).unwrap_err(),
        "percentile_approx_raw: unsupported numeric input type UInt32"
    );
    assert_eq!(
        raw(
            Arc::new(UInt32Array::from(vec![None])),
            Arc::new(BooleanArray::from(vec![None]))
        )
        .unwrap_err(),
        "percentile_approx_raw: unsupported percentile payload type UInt32"
    );
    // A malformed payload is not decoded when the supported quantile is NULL.
    assert_eq!(
        floats(
            raw(
                binary(&[Some(b"bad".to_vec())]),
                Arc::new(Float64Array::from(vec![None]))
            )
            .unwrap()
        ),
        vec![None]
    );
}
#[test]
fn legacy_percentile_approx_raw_original_full_actual_carrier_errors_and_zero_rows() {
    let long = DataType::Struct(
        (0..80)
            .map(|i| Field::new(format!("original_field_{i}"), DataType::Utf8, true))
            .collect(),
    );
    for ty in [
        DataType::Null,
        DataType::Boolean,
        DataType::UInt64,
        DataType::Decimal256(76, 2),
        long.clone(),
    ] {
        let expected = format!(
            "percentile_approx_raw: unsupported percentile payload type {:?}",
            ty
        );
        assert_eq!(
            raw(
                new_null_array(&ty, 1),
                Arc::new(Float64Array::from(vec![0.5]))
            )
            .unwrap_err(),
            expected
        );
        if ty == long {
            assert!(expected.len() > 512);
        }
        assert!(
            raw(
                new_empty_array(&ty),
                Arc::new(Float64Array::from(Vec::<f64>::new()))
            )
            .unwrap()
            .is_empty()
        );
        let expected = format!(
            "percentile_approx_raw: unsupported numeric input type {:?}",
            ty
        );
        assert_eq!(
            raw(binary(&[None]), new_null_array(&ty, 1)).unwrap_err(),
            expected
        );
    }
}
#[test]
fn legacy_percentile_approx_raw_original_decode_full_header_errors_and_ignored_tail() {
    let cases = [
        (
            b"bad".to_vec(),
            "percentile state payload too short: expected>=11 actual=3".to_owned(),
        ),
        (
            vec![0; 11],
            "unsupported percentile state payload magic: expected=0xa2 actual=0x00".to_owned(),
        ),
        (
            {
                let mut v = vec![0; 11];
                v[0] = 0xa2;
                v[1] = 99;
                v
            },
            "unsupported percentile state payload version: expected=4 actual=99".to_owned(),
        ),
    ];
    for (p, e) in cases {
        assert_eq!(
            raw(binary(&[Some(p)]), Arc::new(Float64Array::from(vec![0.5]))).unwrap_err(),
            e
        );
    }
    let (arena, mut args, chunk) = setup(vec![
        binary(&[Some(singleton())]),
        Arc::new(Float64Array::from(vec![0.5])),
    ]);
    args.push(ExprId(usize::MAX));
    assert_eq!(
        floats(
            eval_object_function(
                "percentile_approx_raw",
                &arena,
                ExprId(usize::MAX),
                &args,
                &chunk
            )
            .unwrap()
        ),
        vec![Some(10.)]
    );
}
#[test]
fn legacy_percentile_approx_raw_original_child_prefix_precedes_any_row_error() {
    let (arena, args, chunk) = setup(vec![
        Arc::new(UInt32Array::from(vec![1])),
        Arc::new(UInt32Array::from(vec![1])),
    ]);
    for children in [[ExprId(usize::MAX), args[1]], [args[0], ExprId(usize::MAX)]] {
        assert_eq!(
            eval_object_function(
                "percentile_approx_raw",
                &arena,
                ExprId(usize::MAX),
                &children,
                &chunk
            )
            .unwrap_err(),
            "invalid ExprId"
        );
    }
    // The raw surface has indexing panics, while the actual declaration requires two.
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_object_function(
            "percentile_approx_raw",
            &arena,
            ExprId(usize::MAX),
            &[],
            &chunk
        )))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_object_function(
            "percentile_approx_raw",
            &arena,
            ExprId(usize::MAX),
            &args[..1],
            &chunk
        )))
        .is_err()
    );
}

#[test]
fn legacy_percentile_approx_raw_original_v3_json_and_v4_same_math_entry() {
    // Original version-3 JSON vocabulary, not a production decoder replica.
    let metadata = br#"{"quantiles":null,"compression":10000}"#;
    let digest=br#"{"compression":10000.0,"min":10.0,"max":10.0,"max_processed":20000,"max_unprocessed":80000,"processed_weight":0.0,"unprocessed_weight":1.0,"processed":[],"unprocessed":[{"mean":10.0,"weight":1.0}],"cumulative":[]}"#;
    let mut v3 = vec![0xa2, 3];
    v3.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    v3.extend_from_slice(metadata);
    v3.extend_from_slice(digest);
    assert_eq!(
        floats(
            raw(
                binary(&[Some(v3), Some(singleton())]),
                Arc::new(Float64Array::from(vec![0.5; 2]))
            )
            .unwrap()
        ),
        vec![Some(10.), Some(10.)]
    );
}
