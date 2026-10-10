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
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, NullArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_functions::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, PreparedNullSafeComparisonRecipe,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{sync::Arc, time::Duration};

struct OriginalControl;
impl PureCompileControl for OriginalControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for OriginalControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("scalar comparison does not wait")
    }
}
fn legacy_result(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    // These fixtures explicitly author Physical roots. The old carrier-only
    // arena is a value oracle, not evidence of nominal-domain authority.
    let ty = FunctionValueType::new(left.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![
        ty.try_to_field("left").unwrap(),
        ty.try_to_field("right").unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![left.clone(), right.clone()]).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1), SlotId::new(2)],
    )
    .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), ty.data_type.clone());
    let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), ty.data_type.clone());
    let eq = arena.push_typed(ExprNode::EqForNull(l, r), DataType::Boolean);
    let frozen = arena.into_immutable().unwrap();
    ExprArena::from_immutable(&frozen).unwrap().eval(eq, &chunk)
}
fn compare_real_legacy_and_prepared(left: ArrayRef, right: ArrayRef, expected: &[bool]) {
    let ty = FunctionValueType::new(left.data_type().clone(), true);
    let old = legacy_result(&left, &right).unwrap();
    let old = old.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(old.null_count(), 0);
    assert_eq!(old.iter().map(|v| v.unwrap()).collect::<Vec<_>>(), expected);
    let recipe = PreparedNullSafeComparisonRecipe::try_new(&ty, &ty, &OriginalControl).unwrap();
    assert!(!recipe.nullable_result());
    for (row, &expected) in expected.iter().enumerate() {
        assert_eq!(
            recipe
                .compare_rows(
                    EvaluatedArgument::Column(&left),
                    row,
                    row,
                    EvaluatedArgument::Column(&right),
                    row,
                    row,
                    &OriginalControl
                )
                .unwrap(),
            expected
        );
    }
}

#[test]
fn legacy_nullsafe_flat_profiles_preserve_successful_null_and_exact_typed_values() {
    let mut arrays: Vec<(ArrayRef, ArrayRef)> = vec![];
    macro_rules! primitive {
        ($array:ty, $one:expr, $two:expr) => {
            arrays.push((
                Arc::new(<$array>::from(vec![Some($one), Some($one), None, None])),
                Arc::new(<$array>::from(vec![
                    Some($one),
                    Some($two),
                    None,
                    Some($one),
                ])),
            ));
        };
    }
    primitive!(BooleanArray, true, false);
    primitive!(Int8Array, 1, 2);
    primitive!(Int16Array, 1, 2);
    primitive!(Int32Array, 1, 2);
    primitive!(
        Int64Array,
        9_007_199_254_740_993_i64,
        9_007_199_254_740_992_i64
    );
    primitive!(Float32Array, 1.0, 2.0);
    primitive!(Float64Array, 1.0, 2.0);
    arrays.push((
        Arc::new(StringArray::from(vec![
            Some("é\0"),
            Some("é\0"),
            None,
            None,
        ])),
        Arc::new(StringArray::from(vec![
            Some("é\0"),
            Some("e\0"),
            None,
            Some("é\0"),
        ])),
    ));
    primitive!(Date32Array, -1, 2);
    primitive!(TimestampSecondArray, -1, 2);
    primitive!(TimestampMillisecondArray, -1, 2);
    primitive!(TimestampMicrosecondArray, -1, 2);
    primitive!(TimestampNanosecondArray, -1, 2);
    arrays.push((
        Arc::new(
            Decimal128Array::from(vec![
                Some(9_007_199_254_740_993),
                Some(9_007_199_254_740_993),
                None,
                None,
            ])
            .with_precision_and_scale(38, -2)
            .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![
                Some(9_007_199_254_740_993),
                Some(9_007_199_254_740_992),
                None,
                Some(9_007_199_254_740_993),
            ])
            .with_precision_and_scale(38, -2)
            .unwrap(),
        ),
    ));
    assert_eq!(arrays.len(), 14);
    for (left, right) in arrays {
        let pool_left = arrow::compute::concat(&[left.as_ref(), left.as_ref()]).unwrap();
        let pool_right = arrow::compute::concat(&[right.as_ref(), right.as_ref()]).unwrap();
        compare_real_legacy_and_prepared(
            pool_left.slice(4, 4),
            pool_right.slice(4, 4),
            &[true, false, true, false],
        );
    }
}

#[test]
fn intrinsic_null_success_contract_records_legacy_null_carrier_failure_gap() {
    let left = Arc::new(NullArray::new(4)) as ArrayRef;
    let right = Arc::new(NullArray::new(4)) as ArrayRef;
    // Legacy Array::is_null does not expose the Null carrier's intrinsic SQL
    // NULL. It reaches the unsupported non-NULL scalar branch instead.
    assert_eq!(
        legacy_result(&left, &right).unwrap_err(),
        "list scalar compare unsupported type: Null"
    );
    let ty = FunctionValueType::new(DataType::Null, true);
    let recipe = PreparedNullSafeComparisonRecipe::try_new(&ty, &ty, &OriginalControl).unwrap();
    assert!(!recipe.nullable_result());
    // The accepted intrinsic-NULL contract is independent of this legacy gap.
    for row in 0..4 {
        assert!(
            recipe
                .compare_rows(
                    EvaluatedArgument::Column(&left),
                    row,
                    row,
                    EvaluatedArgument::Column(&right),
                    row,
                    row,
                    &OriginalControl
                )
                .unwrap()
        );
    }
}

#[test]
fn legacy_nullsafe_floats_match_any_nonnull_nan_and_signed_zero_both_directions() {
    // This is the independent legacy contract, deliberately different from
    // ordinary Arrow total-order equality and NaN-payload bit equality.
    let left = vec![
        Some(0.0),
        Some(-0.0),
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        Some(-9.0),
        None,
        Some(f64::NEG_INFINITY),
        Some(3.0),
        None,
        Some(f64::from_bits(0xfff8_0000_0000_0012)),
    ];
    let right = vec![
        Some(-0.0),
        Some(0.0),
        Some(7.0),
        Some(f64::from_bits(0xfff8_0000_0000_0064)),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(4.0),
        None,
        Some(f64::NAN),
    ];
    let expected = [true, true, true, true, false, false, false, true, true];
    for swapped in [false, true] {
        let (l, r) = if swapped {
            (&right, &left)
        } else {
            (&left, &right)
        };
        compare_real_legacy_and_prepared(
            Arc::new(Float64Array::from(l.clone())),
            Arc::new(Float64Array::from(r.clone())),
            &expected,
        );
        compare_real_legacy_and_prepared(
            Arc::new(Float32Array::from(
                l.iter()
                    .map(|v| {
                        v.map(|v| {
                            if v.is_nan() {
                                f32::from_bits(0x7fc0_0042)
                            } else {
                                v as f32
                            }
                        })
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                r.iter()
                    .map(|v| {
                        v.map(|v| {
                            if v.is_nan() {
                                f32::from_bits(0xffc0_0064)
                            } else {
                                v as f32
                            }
                        })
                    })
                    .collect::<Vec<_>>(),
            )),
            &expected,
        );
    }
}

#[test]
fn nullsafe_does_not_infer_nominal_numeric_domains_or_expand_ordinary_flat_capability() {
    use novarocks_type_contract::ValueLogicalType;
    for logical in [ValueLogicalType::LargeInt, ValueLogicalType::Uuid] {
        let mut source = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
        source.logical_type = logical;
        assert!(
            PreparedNullSafeComparisonRecipe::try_new(&source, &source, &OriginalControl).is_err()
        );
    }
    for carrier in [
        DataType::UInt32,
        DataType::Decimal256(38, 0),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
    ] {
        let source = FunctionValueType::new(carrier, true);
        assert!(
            PreparedNullSafeComparisonRecipe::try_new(&source, &source, &OriginalControl).is_err()
        );
    }
    let physical = FunctionValueType::new(DataType::Utf8, true);
    let mut json = physical.clone();
    json.logical_type = ValueLogicalType::Json;
    assert!(PreparedNullSafeComparisonRecipe::try_new(&physical, &json, &OriginalControl).is_err());
}
