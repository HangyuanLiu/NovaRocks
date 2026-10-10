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

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::SlotId;
use std::sync::Arc;

fn legacy(
    source: ArrayRef,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    // Explicit Physical roots make this a numeric legacy oracle. Carrier-only
    // ExprArena metadata does not establish any nominal-domain authority.
    let source_type = FunctionValueType::new(source.data_type().clone(), true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            source_type.try_to_field("source").unwrap(),
        ])),
        vec![source],
    )
    .unwrap();
    let layout =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, layout);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), source_type.data_type);
    let cast = arena.push_typed(ExprNode::Cast(child, policy), target);
    let frozen = arena.into_immutable().unwrap();
    // The real frozen legacy arena owns the original ALLOW and per-node policy.
    ExprArena::from_immutable(&frozen)
        .unwrap()
        .eval(cast, &chunk)
}
fn signed_values(array: &ArrayRef) -> Vec<Option<i64>> {
    macro_rules! values {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .unwrap()
                .iter()
                .map(|v| v.map(i64::from))
                .collect()
        };
    }
    match array.data_type() {
        DataType::Int8 => values!(Int8Array),
        DataType::Int16 => values!(Int16Array),
        DataType::Int32 => values!(Int32Array),
        DataType::Int64 => values!(Int64Array),
        _ => panic!("the frozen target must remain signed"),
    }
}
fn policies() -> [DecimalOverflowPolicy; 2] {
    [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ]
}
fn target_name(target: &DataType) -> &'static str {
    match target {
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INT",
        DataType::Int64 => "BIGINT",
        _ => unreachable!(),
    }
}
fn range_error(source: &DataType, target: &DataType, value: &str) -> String {
    format!(
        "Expr evaluate meet error: CAST failed: from {source:?} to {target:?}: {value} conflict with range of {}",
        target_name(target)
    )
}
fn source_array(f32_source: bool, values: &[Option<f64>]) -> ArrayRef {
    if f32_source {
        Arc::new(Float32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(Float64Array::from(values.to_vec()))
    }
}

#[test]
fn legacy_float_signed_eight_profiles_preserve_native_boundaries_truncation_nulls_and_frozen_policies()
 {
    // Values and answers are independent of the new prepared recipe. Native
    // f32 bit patterns preserve the actual source precision at i32/i64 limits.
    for f32_source in [false, true] {
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            let (boundaries, maximum, minimum): ([f64; 4], i64, i64) = match (&target, f32_source) {
                (DataType::Int8, _) => ([127.9, -128.5, -129.0, 128.0], 127, -128),
                (DataType::Int16, _) => ([32767.5, -32768.5, -32769.0, 32768.0], 32767, -32768),
                (DataType::Int32, false) => (
                    [2147483647.5, -2147483648.5, -2147483649.0, 2147483648.0],
                    i32::MAX.into(),
                    i32::MIN.into(),
                ),
                (DataType::Int32, true) => (
                    [
                        f32::from_bits(0x4effffff) as f64,
                        f32::from_bits(0xcf000000) as f64,
                        f32::from_bits(0xcf000001) as f64,
                        f32::from_bits(0x4f000000) as f64,
                    ],
                    2147483520,
                    i32::MIN.into(),
                ),
                (DataType::Int64, false) => (
                    [
                        f64::from_bits(0x43dfffffffffffff),
                        f64::from_bits(0xc3e0000000000000),
                        f64::from_bits(0xc3e0000000000001),
                        f64::from_bits(0x43e0000000000000),
                    ],
                    9223372036854774784,
                    i64::MIN,
                ),
                (DataType::Int64, true) => (
                    [
                        f32::from_bits(0x5effffff) as f64,
                        f32::from_bits(0xdf000000) as f64,
                        f32::from_bits(0xdf000001) as f64,
                        f32::from_bits(0x5f000000) as f64,
                    ],
                    9223371487098961920,
                    i64::MIN,
                ),
                _ => unreachable!(),
            };
            let mut values = boundaries.into_iter().map(Some).collect::<Vec<_>>();
            values.extend([
                Some(7.75),
                Some(-7.75),
                Some(0.0),
                Some(-0.0),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
            ]);
            let expected = vec![
                Some(maximum),
                Some(minimum),
                None,
                None,
                Some(7),
                Some(-7),
                Some(0),
                Some(0),
                None,
                None,
                None,
                None,
            ];
            // A nonzero slice excludes the deliberately failing prefix/suffix.
            let mut pool = vec![Some(f64::NAN)];
            pool.extend(values.iter().copied());
            pool.push(Some(f64::INFINITY));
            let source = source_array(f32_source, &pool).slice(1, values.len());
            for policy in policies() {
                let output = legacy(source.clone(), target.clone(), policy, false).unwrap();
                assert_eq!(output.data_type(), &target);
                assert_eq!(signed_values(&output), expected);
                // ALLOW=true changes only failed non-NULL conversions. Original
                // NULL and successful native truncation stay unchanged.
                let success = values
                    .iter()
                    .zip(&expected)
                    .filter(|(value, answer)| value.is_none() || answer.is_some())
                    .map(|(value, _)| *value)
                    .collect::<Vec<_>>();
                let success_expected = values
                    .iter()
                    .zip(&expected)
                    .filter(|(value, answer)| value.is_none() || answer.is_some())
                    .map(|(_, answer)| *answer)
                    .collect::<Vec<_>>();
                assert_eq!(
                    signed_values(
                        &legacy(
                            source_array(f32_source, &success),
                            target.clone(),
                            policy,
                            true
                        )
                        .unwrap()
                    ),
                    success_expected
                );
            }
        }
    }
}

#[test]
fn legacy_float_signed_allow_reports_the_first_original_batch_failure_including_nonfinite() {
    for f32_source in [false, true] {
        let source_type = if f32_source {
            DataType::Float32
        } else {
            DataType::Float64
        };
        for (target, overflow, diagnostic) in [
            (DataType::Int8, 128.0, "128"),
            (DataType::Int16, 32768.0, "32768"),
            (DataType::Int32, 2147483648.0, "2147483648"),
            (
                DataType::Int64,
                f64::from_bits(0x43e0000000000000),
                "9223372036854776000",
            ),
        ] {
            for policy in policies() {
                for (values, first) in [
                    (
                        vec![None, Some(7.75), Some(f64::NAN), Some(overflow)],
                        "NaN",
                    ),
                    (vec![None, Some(overflow), Some(f64::NAN)], diagnostic),
                    (vec![None, Some(f64::INFINITY), Some(overflow)], "inf"),
                    (vec![None, Some(f64::NEG_INFINITY), Some(overflow)], "-inf"),
                ] {
                    assert_eq!(
                        legacy(
                            source_array(f32_source, &values),
                            target.clone(),
                            policy,
                            true
                        )
                        .unwrap_err(),
                        range_error(&source_type, &target, first)
                    );
                    let output = legacy(
                        source_array(f32_source, &values),
                        target.clone(),
                        policy,
                        false,
                    )
                    .unwrap();
                    assert!(output.is_null(0));
                    assert!(
                        output.is_null(
                            values
                                .iter()
                                .position(
                                    |value| value.is_some_and(|v| !v.is_finite() || v == overflow)
                                )
                                .unwrap()
                        )
                    );
                }
            }
        }
    }
}

#[test]
fn legacy_f32_error_expands_actual_native_bits_to_f64_without_changing_range_or_slice_order() {
    // The failing f32 is not 128. Its exact widened value must survive diagnostic
    // formatting, which would differ from f32 Display (128.00002).
    let value = f32::from_bits(0x43000001);
    assert_eq!(f64::from(value), 128.00001525878906);
    let pool = Arc::new(Float32Array::from(vec![
        Some(f32::NAN),
        None,
        Some(7.75),
        Some(value),
        Some(f32::INFINITY),
    ])) as ArrayRef;
    for policy in policies() {
        let selected = pool.slice(1, 4);
        assert_eq!(
            signed_values(&legacy(selected.clone(), DataType::Int8, policy, false).unwrap()),
            vec![None, Some(7), None, None]
        );
        assert_eq!(
            legacy(selected, DataType::Int8, policy, true).unwrap_err(),
            "Expr evaluate meet error: CAST failed: from Float32 to Int8: 128.00001525878906 conflict with range of TINYINT"
        );
        // Advancing the real array slice changes the first failing original row.
        assert_eq!(
            legacy(pool.slice(4, 1), DataType::Int8, policy, true).unwrap_err(),
            "Expr evaluate meet error: CAST failed: from Float32 to Int8: inf conflict with range of TINYINT"
        );
    }
}
