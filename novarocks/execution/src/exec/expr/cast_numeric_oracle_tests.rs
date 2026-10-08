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
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use novarocks_types::SlotId;
use std::sync::Arc;
use std::time::Duration;

struct OriginalControl;
impl PureCompileControl for OriginalControl {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for OriginalControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric cast does not wait")
    }
}

fn signed_types() -> [DataType; 4] {
    [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ]
}
fn bounds(ty: &DataType) -> (i64, i64) {
    match ty {
        DataType::Int8 => (i8::MIN.into(), i8::MAX.into()),
        DataType::Int16 => (i16::MIN.into(), i16::MAX.into()),
        DataType::Int32 => (i32::MIN.into(), i32::MAX.into()),
        DataType::Int64 => (i64::MIN, i64::MAX),
        _ => panic!("fixture requires a signed carrier"),
    }
}
fn signed_array(ty: &DataType, values: &[Option<i64>]) -> ArrayRef {
    match ty {
        DataType::Int8 => Arc::new(Int8Array::from(
            values
                .iter()
                .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            values
                .iter()
                .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        _ => panic!("fixture requires a signed carrier"),
    }
}
fn signed_at(array: &dyn Array, row: usize) -> i64 {
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row)
            .into(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row)
            .into(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .into(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row),
        _ => panic!("oracle requires a signed carrier"),
    }
}

fn compare_legacy_rows(
    source: ArrayRef,
    source_nullable: bool,
    target: DataType,
    result_nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> (ArrayRef, Vec<CastRowResult>) {
    // This fixture explicitly authors Physical roots. The legacy carrier-only
    // arena is an independent numeric oracle, not nominal identity evidence.
    let source_type = FunctionValueType::new(source.data_type().clone(), source_nullable);
    let result_type = FunctionValueType::new(target.clone(), result_nullable);
    let recipe = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &source_type,
        &result_type,
        policy,
        allow,
        &OriginalControl,
    )
    .unwrap();
    assert_eq!(recipe.operation(), CastOperation::Carrier);
    assert_eq!(recipe.source_type(), &source_type);
    assert_eq!(recipe.result_type(), &result_type);
    let schema = Arc::new(Schema::new(vec![
        source_type.try_to_field("source").unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![source.clone()]).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), source_type.data_type);
    let cast = arena.push_typed(ExprNode::Cast(input, policy), target.clone());
    let legacy = arena.eval(cast, &chunk).unwrap();
    assert_eq!(legacy.data_type(), &target);
    let rows = (0..source.len())
        .map(|row| {
            recipe
                .evaluate_row(
                    EvaluatedArgument::Column(&source),
                    row,
                    row,
                    &OriginalControl,
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    for (row, value) in rows.iter().enumerate() {
        match value {
            CastRowResult::Null => assert!(legacy.is_null(row)),
            CastRowResult::Signed(value) => {
                assert!(!legacy.is_null(row));
                if let DataType::Date32 = target {
                    assert_eq!(
                        i64::from(
                            legacy
                                .as_any()
                                .downcast_ref::<arrow::array::Date32Array>()
                                .unwrap()
                                .value(row)
                        ),
                        *value
                    );
                } else {
                    assert_eq!(signed_at(legacy.as_ref(), row), *value);
                }
            }
            CastRowResult::Float32(value) => {
                assert!(!legacy.is_null(row));
                assert_eq!(
                    legacy
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap()
                        .value(row)
                        .to_bits(),
                    value.to_bits(),
                );
            }
            CastRowResult::Float64(value) => {
                assert!(!legacy.is_null(row));
                assert_eq!(
                    legacy
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row)
                        .to_bits(),
                    value.to_bits(),
                );
            }
            CastRowResult::RowError(error) => {
                panic!("signed cast must not raise a row error: {error:?}")
            }
            CastRowResult::Boolean(value) => {
                assert!(!legacy.is_null(row));
                assert_eq!(
                    legacy
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .unwrap()
                        .value(row),
                    *value
                );
            }
            CastRowResult::Timestamp(value) => {
                use arrow::array::{
                    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
                    TimestampSecondArray,
                };
                use arrow::datatypes::TimeUnit;
                assert!(!legacy.is_null(row));
                let actual = match &target {
                    DataType::Timestamp(TimeUnit::Second, None) => legacy
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(row),
                    DataType::Timestamp(TimeUnit::Millisecond, None) => legacy
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(row),
                    DataType::Timestamp(TimeUnit::Microsecond, None) => legacy
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(row),
                    DataType::Timestamp(TimeUnit::Nanosecond, None) => legacy
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(row),
                    _ => panic!("timestamp result has a non-timestamp target"),
                };
                assert_eq!(actual, *value);
            }
            CastRowResult::Text(value) => {
                assert!(!legacy.is_null(row));
                assert_eq!(
                    legacy
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap()
                        .value(row),
                    value
                );
            }
            CastRowResult::Unsigned(_) => {
                panic!("signed numeric cast returned an unsigned integer")
            }
        }
    }
    (legacy, rows)
}

#[test]
fn legacy_signed_cast_oracle_matches_all_width_pairs_policies_allow_nulls_and_slices() {
    for source_type in signed_types() {
        let (source_min, source_max) = bounds(&source_type);
        for target_type in signed_types() {
            let (target_min, target_max) = bounds(&target_type);
            let mut values = vec![Some(source_min), Some(source_max), Some(-1), Some(0), None];
            for candidate in [
                Some(target_min),
                target_min.checked_sub(1),
                Some(target_max),
                target_max.checked_add(1),
            ]
            .into_iter()
            .flatten()
            {
                if (source_min..=source_max).contains(&candidate) {
                    values.push(Some(candidate));
                }
            }
            let mut padded = vec![Some(7)];
            padded.extend(values.iter().copied());
            padded.push(Some(-7));
            let source = signed_array(&source_type, &padded).slice(1, values.len());
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let (_, rows) = compare_legacy_rows(
                        source.clone(),
                        true,
                        target_type.clone(),
                        true,
                        policy,
                        allow,
                    );
                    for (value, row) in values.iter().zip(rows) {
                        match value {
                            Some(value) if (target_min..=target_max).contains(value) => {
                                assert!(
                                    matches!(row, CastRowResult::Signed(actual) if actual == *value)
                                );
                            }
                            _ => assert!(matches!(row, CastRowResult::Null)),
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn legacy_signed_to_float_cast_oracle_preserves_direct_width_bits_at_precision_boundaries() {
    for source_type in signed_types() {
        let (min, max) = bounds(&source_type);
        let mut values = vec![Some(min), Some(max), Some(-7), Some(0), Some(7), None];
        for boundary in [1_i64 << 24, 1_i64 << 53] {
            for value in [
                boundary - 1,
                boundary,
                boundary + 1,
                -boundary - 1,
                -boundary,
                -boundary + 1,
            ] {
                if (min..=max).contains(&value) {
                    values.push(Some(value));
                }
            }
        }
        // At this F32 midpoint, an intermediate F64 conversion loses the
        // integer's final bit and can choose the opposite tie result.
        let double_rounding_probe = (1_i64 << 53) + (1_i64 << 29) + 1;
        for value in [double_rounding_probe, -double_rounding_probe] {
            if (min..=max).contains(&value) {
                values.push(Some(value));
            }
        }
        let mut padded = vec![Some(1)];
        padded.extend(values.iter().copied());
        let source = signed_array(&source_type, &padded).slice(1, values.len());
        for target_type in [DataType::Float32, DataType::Float64] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let (_, rows) = compare_legacy_rows(
                        source.clone(),
                        true,
                        target_type.clone(),
                        true,
                        policy,
                        allow,
                    );
                    for (value, row) in values.iter().zip(rows) {
                        match (value, row) {
                            (None, CastRowResult::Null) => {}
                            (Some(value), CastRowResult::Float32(actual)) => {
                                assert_eq!(actual.to_bits(), (*value as f32).to_bits());
                                assert!(actual.is_finite());
                            }
                            (Some(value), CastRowResult::Float64(actual)) => {
                                assert_eq!(actual.to_bits(), (*value as f64).to_bits());
                                assert!(actual.is_finite());
                            }
                            other => panic!("unexpected selected cast result: {other:?}"),
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn legacy_nonnullable_signed_cast_widening_stays_exact_and_narrowing_overflow_is_null_even_with_allow()
 {
    for source_type in signed_types() {
        let (min, max) = bounds(&source_type);
        let source = signed_array(&source_type, &[Some(min), Some(max), Some(-1), Some(0)]);
        for target_type in signed_types() {
            let (target_min, target_max) = bounds(&target_type);
            let narrowing = min < target_min || max > target_max;
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let (legacy, rows) = compare_legacy_rows(
                        source.clone(),
                        false,
                        target_type.clone(),
                        narrowing,
                        policy,
                        allow,
                    );
                    if narrowing {
                        assert!(legacy.is_null(0));
                        assert!(legacy.is_null(1));
                        assert!(matches!(rows[0], CastRowResult::Null));
                        assert!(matches!(rows[1], CastRowResult::Null));
                    } else {
                        assert_eq!(legacy.null_count(), 0);
                        assert!(matches!(rows[0], CastRowResult::Signed(value) if value == min));
                        assert!(matches!(rows[1], CastRowResult::Signed(value) if value == max));
                    }
                }
            }
        }
        for target in [DataType::Float32, DataType::Float64] {
            let (legacy, _) = compare_legacy_rows(
                source.clone(),
                false,
                target,
                false,
                DecimalOverflowPolicy::ReportError,
                true,
            );
            assert_eq!(legacy.null_count(), 0);
        }
    }
}

#[test]
fn legacy_text_to_signed_cast_oracle_matches_widths_allow_and_sparse_source_slices() {
    let source: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
        Some("unused"),
        Some("127"),
        Some("128"),
        Some("-128"),
        Some("-129"),
        Some("32767"),
        Some("32768"),
        Some("2147483647"),
        Some("2147483648"),
        Some("9223372036854775807"),
        Some("-9223372036854775808"),
        Some("9223372036854775808"),
        Some("+0001"),
        Some(" 1"),
        Some("1.5"),
        Some("true"),
        Some(""),
        Some("-"),
        None,
    ]));
    let source = source.slice(1, source.len() - 1);
    for target in signed_types() {
        for allow in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let (_, rows) =
                    compare_legacy_rows(source.clone(), true, target.clone(), true, policy, allow);
                assert_eq!(rows[0], CastRowResult::Signed(127));
                assert_eq!(rows[12], CastRowResult::Null);
                assert_eq!(rows[17], CastRowResult::Null);
            }
        }
    }
}

#[test]
fn legacy_text_to_boolean_cast_oracle_preserves_trim_numeric_and_text_rules() {
    let source: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
        Some(" true "),
        Some("FALSE"),
        Some("+1"),
        Some("-1"),
        Some("0"),
        Some("2147483647"),
        Some("2147483648"),
        Some(""),
        Some("1.5"),
        Some("yes"),
        Some("\u{2003}false\u{2003}"),
        None,
    ]));
    for allow in [false, true] {
        let (_, actual) = compare_legacy_rows(
            source.clone(),
            true,
            DataType::Boolean,
            true,
            DecimalOverflowPolicy::OutputNull,
            allow,
        );
        assert_eq!(
            actual,
            vec![
                CastRowResult::Boolean(true),
                CastRowResult::Boolean(false),
                CastRowResult::Boolean(true),
                CastRowResult::Boolean(true),
                CastRowResult::Boolean(false),
                CastRowResult::Boolean(true),
                CastRowResult::Null,
                CastRowResult::Null,
                CastRowResult::Null,
                CastRowResult::Null,
                CastRowResult::Boolean(false),
                CastRowResult::Null
            ]
        );
    }
}

#[test]
fn legacy_calendar_cast_oracle_matches_compact_integer_and_text_profiles() {
    use arrow::array::StringArray;
    use arrow::datatypes::TimeUnit;
    let integer: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(20240101),
        None,
        Some(690101),
        Some(700101),
        Some(991231),
        Some(20240229),
        Some(20230229),
        Some(101),
        Some(0),
        Some(-1),
        Some(20241008235959),
        Some(99991231),
        Some(i64::MAX),
    ]));
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("2024-10-08 12:30:01.123456789"),
        None,
        Some("1969-12-31 23:59:59.999999999"),
        Some("1970-01-01"),
        Some(""),
        Some("junk"),
        Some("2023-02-29"),
        Some("2024-02-29"),
        Some(" 2024-01-02 "),
        Some("2024-01-03T12:13:14"),
    ]));
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for source in [&integer, &text] {
                for target in [
                    DataType::Date32,
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ] {
                    compare_legacy_rows(source.clone(), true, target, true, policy, allow);
                }
            }
            compare_legacy_rows(
                text.clone(),
                true,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
                policy,
                allow,
            );
        }
    }
}

#[test]
fn legacy_primitive_text_cast_oracle_preserves_widths_extremes_nulls_and_float_spelling() {
    let mut inputs: Vec<ArrayRef> = signed_types()
        .into_iter()
        .map(|ty| {
            let (low, high) = bounds(&ty);
            signed_array(
                &ty,
                &[Some(low), Some(-1), None, Some(0), Some(1), Some(high)],
            )
            .slice(1, 4)
        })
        .collect();
    inputs.push(Arc::new(Float64Array::from(vec![
        Some(-0.0),
        None,
        Some(0.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(1.0),
        Some(1e20),
        Some(1e-20),
    ])));
    inputs.push(Arc::new(Float32Array::from(vec![
        Some(-0.0),
        None,
        Some(f32::NAN),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(1.0),
        Some(1e20),
        Some(1e-20),
    ])));
    inputs.push(Arc::new(arrow::array::BooleanArray::from(vec![
        Some(true),
        None,
        Some(false),
    ])));
    for input in inputs {
        for policy in [
            DecimalOverflowPolicy::ReportError,
            DecimalOverflowPolicy::OutputNull,
        ] {
            for allow in [false, true] {
                compare_legacy_rows(input.clone(), true, DataType::Utf8, true, policy, allow);
            }
        }
    }
    let (_, values) = compare_legacy_rows(
        Arc::new(Float64Array::from(vec![
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1e20,
        ])),
        false,
        DataType::Utf8,
        false,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    assert_eq!(
        values,
        ["0", "nan", "inf", "-inf", "1e+20"].map(|s| CastRowResult::Text(s.into()))
    );
}

#[test]
fn legacy_timestamp_text_cast_oracle_preserves_units_fraction_negative_and_epoch_projection() {
    use arrow::array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    let values = vec![
        Some(i64::MIN),
        Some(-1),
        None,
        Some(0),
        Some(1),
        Some(1_700_000_000),
        Some(i64::MAX),
    ];
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(TimestampSecondArray::from(values.clone())),
        Arc::new(TimestampMillisecondArray::from(values.clone())),
        Arc::new(TimestampMicrosecondArray::from(values.clone())),
        Arc::new(TimestampNanosecondArray::from(values)),
    ];
    for input in inputs {
        compare_legacy_rows(
            input,
            true,
            DataType::Utf8,
            true,
            DecimalOverflowPolicy::ReportError,
            false,
        );
    }
}

#[test]
fn legacy_date_carrier_cast_profiles_match_shared_selected_rows() {
    use arrow::array::Date32Array;
    use arrow::datatypes::TimeUnit;
    for nullable in [false, true] {
        let dates: ArrayRef = Arc::new(Date32Array::from(if nullable {
            vec![Some(-1), None, Some(0), Some(1), Some(19782)]
        } else {
            vec![Some(-1), Some(0), Some(1), Some(19782)]
        }));
        for target in [
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    compare_legacy_rows(
                        dates.clone(),
                        nullable,
                        target.clone(),
                        nullable,
                        policy,
                        allow,
                    );
                }
            }
        }
    }
}
#[test]
fn legacy_timestamp_date_cast_profiles_match_shared_selected_rows() {
    use arrow::array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    for nullable in [false, true] {
        let values = if nullable {
            vec![Some(-1), Some(0), None, Some(1)]
        } else {
            vec![Some(-1), Some(0), Some(1)]
        };
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(TimestampSecondArray::from(values.clone())),
            Arc::new(TimestampMillisecondArray::from(values.clone())),
            Arc::new(TimestampMicrosecondArray::from(values.clone())),
            Arc::new(TimestampNanosecondArray::from(values)),
        ];
        for array in arrays {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    compare_legacy_rows(
                        array.clone(),
                        nullable,
                        DataType::Date32,
                        nullable,
                        policy,
                        allow,
                    );
                }
            }
        }
    }
}
