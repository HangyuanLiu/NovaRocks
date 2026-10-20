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

//! Exact values and schema properties for the selected array CAST port.
use super::*;
use arrow_array::{
    BinaryArray, FixedSizeBinaryArray, Int32Array, LargeBinaryArray, LargeStringArray,
    Time64MicrosecondArray,
};
use arrow_schema::Field;
use std::time::Duration;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("selected CAST does not wait")
    }
}
fn recipe(
    source: DataType,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(source, true),
        &FunctionValueType::new(target, true),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn output<'a>(
    r: &PreparedCastRecipe,
    input: &ArrayRef,
    selection: Selection<'a>,
) -> SelectedValues<'a> {
    assert!(r.uses_selected_array());
    r.evaluate_selected_array(EvaluatedArgument::Column(input), selection, &[], &Control)
        .unwrap()
}
#[test]
fn fixed_bytes_keep_exact_width_and_selected_addresses() {
    let input = Arc::new(BinaryArray::from(vec![
        Some(&b"abcd"[..]),
        Some(&b"xy"[..]),
        Some(&b"wxyz"[..]),
        None,
    ])) as ArrayRef;
    let rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let r = recipe(
        DataType::Binary,
        DataType::FixedSizeBinary(4),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let out = output(&r, &input, selection);
    assert_eq!(out.values().data_type(), &DataType::FixedSizeBinary(4));
    let fixed = out
        .values()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fixed.value(0), b"abcd");
    assert_eq!(fixed.value(1), b"wxyz");
    assert!(fixed.is_null(2));
    let selected = output(&r, &input, Selection::all(4));
    assert!(selected.values().is_null(1));
    assert!(selected.errors().is_empty());
    let r = recipe(
        DataType::FixedSizeBinary(4),
        DataType::Binary,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let restored = output(&r, out.values(), Selection::all(3));
    let bytes = restored
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(bytes.value(0), b"abcd");
    assert_eq!(bytes.value(1), b"wxyz");
    assert!(bytes.is_null(2));
}
#[test]
fn wide_decimal_targets_keep_value_precision_and_policy() {
    for (policy, allow) in [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ] {
        let input = Arc::new(Int64Array::from(vec![Some(12), Some(-12), None])) as ArrayRef;
        let r = recipe(DataType::Int64, DataType::Decimal256(40, 2), policy, allow);
        let out = output(&r, &input, Selection::all(3));
        assert_eq!(out.values().data_type(), &DataType::Decimal256(40, 2));
        let values = out
            .values()
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        assert_eq!(values.value(0), arrow_buffer::i256::from_i128(1200));
        assert_eq!(values.value(1), arrow_buffer::i256::from_i128(-1200));
        assert!(values.is_null(2));
        let bools = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef;
        let r = recipe(
            DataType::Boolean,
            DataType::Decimal256(40, 2),
            policy,
            allow,
        );
        let out = output(&r, &bools, Selection::all(3));
        assert_eq!(out.values().data_type(), &DataType::Decimal256(40, 2));
        let values = out
            .values()
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        assert_eq!(values.value(0), arrow_buffer::i256::from_i128(100));
        assert_eq!(values.value(1), arrow_buffer::i256::ZERO);
        assert!(values.is_null(2));
        let input = Arc::new(Int64Array::from(vec![1000_i64])) as ArrayRef;
        let r = recipe(DataType::Int64, DataType::Decimal256(3, 0), policy, allow);
        let out = output(&r, &input, Selection::all(1));
        assert!(out.values().is_null(0));
        assert_eq!(
            out.errors().len(),
            usize::from(policy == DecimalOverflowPolicy::ReportError)
        );
    }
}
#[test]
fn struct_names_use_complete_by_name_mapping_or_complete_ordinal_mapping() {
    let from = vec![
        Arc::new(Field::new("a", DataType::Int32, false)),
        Arc::new(Field::new("b", DataType::Int32, false)),
    ]
    .into();
    let input = Arc::new(StructArray::new(
        from,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
        None,
    )) as ArrayRef;
    for (names, expected) in [(["b", "a"], [10, 1]), (["new_a", "b"], [1, 10])] {
        let target = DataType::Struct(
            vec![
                Arc::new(Field::new(names[0], DataType::Int32, true)),
                Arc::new(Field::new(names[1], DataType::Int32, true)),
            ]
            .into(),
        );
        let r = recipe(
            input.data_type().clone(),
            target.clone(),
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let out = output(&r, &input, Selection::all(2));
        assert_eq!(out.values().data_type(), &target);
        let values = out.values().as_any().downcast_ref::<StructArray>().unwrap();
        for i in 0..2 {
            assert_eq!(
                values
                    .column(i)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                expected[i]
            );
        }
    }
}
#[test]
fn renamed_struct_keeps_nested_fixed_bytes_without_generic_value_casting() {
    let fixed =
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"abcd", b"wxyz"].into_iter()).unwrap())
            as ArrayRef;
    let input = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "old",
            DataType::FixedSizeBinary(4),
            false,
        ))]
        .into(),
        vec![fixed],
        None,
    )) as ArrayRef;
    let target = DataType::Struct(
        vec![Arc::new(Field::new(
            "new",
            DataType::FixedSizeBinary(4),
            true,
        ))]
        .into(),
    );
    let r = recipe(
        input.data_type().clone(),
        target.clone(),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let rows = [1];
    let out = output(&r, &input, Selection::try_sparse(2, &rows).unwrap());
    assert_eq!(out.values().data_type(), &target);
    let values = out.values().as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        values
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        b"wxyz"
    );
}
#[test]
fn selected_array_route_preserves_inherited_journal() {
    let selection = Selection::all(2);
    let input = SelectedValues::try_new(
        selection,
        &DataType::Binary,
        Arc::new(BinaryArray::from(vec![Some(&b"abcd"[..]), None])),
        vec![RowDataError::new(1, "original child error")].into_boxed_slice(),
    )
    .unwrap();
    let r = recipe(
        DataType::Binary,
        DataType::FixedSizeBinary(4),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let out = r
        .evaluate_selected_array(
            EvaluatedArgument::SelectedColumn(&input),
            selection,
            input.errors(),
            &Control,
        )
        .unwrap();
    assert_eq!(out.errors(), input.errors());
    assert!(out.values().is_null(1));
}

#[test]
fn physical_fixed16_numeric_profile_keeps_big_endian_values_without_nominal_rebinding() {
    let input = Arc::new(Int64Array::from(vec![Some(12_i64), Some(-12_i64), None])) as ArrayRef;
    let r = recipe(
        DataType::Int64,
        DataType::FixedSizeBinary(16),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let out = output(&r, &input, Selection::all(3));
    let fixed = out
        .values()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fixed.value(0), 12_i128.to_be_bytes());
    assert_eq!(fixed.value(1), (-12_i128).to_be_bytes());
    assert!(fixed.is_null(2));
    let r = recipe(
        DataType::FixedSizeBinary(16),
        DataType::Int64,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let restored = output(&r, out.values(), Selection::all(3));
    assert_eq!(
        restored
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        input.as_any().downcast_ref::<Int64Array>().unwrap()
    );
    let text = Arc::new(StringArray::from(vec![
        Some(" 170141183460469231731687303715884105727 "),
        Some("bad"),
        None,
    ])) as ArrayRef;
    let r = recipe(
        DataType::Utf8,
        DataType::FixedSizeBinary(16),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let out = output(&r, &text, Selection::all(3));
    let fixed = out
        .values()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fixed.value(0), i128::MAX.to_be_bytes());
    assert!(fixed.is_null(1));
    assert!(fixed.is_null(2));
}

#[test]
fn selected_scalar_array_families_keep_independent_values_under_every_policy() {
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(UInt64Array::from(vec![Some(12), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(1200), None])
                    .with_precision_and_scale(6, 2)
                    .unwrap(),
            ),
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![Some(1299), None])
                    .with_precision_and_scale(6, 2)
                    .unwrap(),
            ),
            Arc::new(UInt16Array::from(vec![Some(12), None])),
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![Some(123456), None])),
            Arc::new(Int64Array::from(vec![Some(123456), None])),
        ),
        (
            Arc::new(Time64MicrosecondArray::from(vec![Some(123456), None])),
            Arc::new(Int64Array::from(vec![Some(123456), None])),
        ),
        (
            Arc::new(Int64Array::from(vec![Some(123456), None])),
            Arc::new(Time64MicrosecondArray::from(vec![Some(123456), None])),
        ),
        (
            Arc::new(LargeStringArray::from(vec![Some("12"), None])),
            Arc::new(Int32Array::from(vec![Some(12), None])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("alpha"), None])),
            Arc::new(LargeStringArray::from(vec![Some("alpha"), None])),
        ),
        (
            Arc::new(LargeBinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
            Arc::new(BinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
        ),
        (
            Arc::new(Int32Array::from(vec![Some(12), None])),
            Arc::new(BinaryArray::from(vec![
                Some(12_i32.to_ne_bytes().as_slice()),
                None,
            ])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("12.25"), None])),
            Arc::new(
                Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1225)), None])
                    .with_precision_and_scale(40, 2)
                    .unwrap(),
            ),
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![Some(1225), None])
                    .with_precision_and_scale(6, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1225)), None])
                    .with_precision_and_scale(40, 2)
                    .unwrap(),
            ),
        ),
    ];
    for (source, expected) in cases {
        for (policy, allow) in [
            (DecimalOverflowPolicy::OutputNull, false),
            (DecimalOverflowPolicy::OutputNull, true),
            (DecimalOverflowPolicy::ReportError, false),
            (DecimalOverflowPolicy::ReportError, true),
        ] {
            let recipe = recipe(
                source.data_type().clone(),
                expected.data_type().clone(),
                policy,
                allow,
            );
            let actual = output(&recipe, &source, Selection::all(source.len()));
            assert_eq!(
                actual.values().to_data(),
                expected.to_data(),
                "{:?} -> {:?} {policy:?} allow={allow}",
                source.data_type(),
                expected.data_type()
            );
            assert!(actual.errors().is_empty());
        }
    }
}

#[test]
fn fixed_numeric_precision_masks_retain_exact_decimal128_and_decimal256_targets() {
    let input = crate::largeint::array_from_i128(&[Some(12), Some(1000), None]).unwrap();
    for target in [DataType::Decimal128(3, 1), DataType::Decimal256(3, 1)] {
        for (policy, allow) in [
            (DecimalOverflowPolicy::OutputNull, false),
            (DecimalOverflowPolicy::OutputNull, true),
            (DecimalOverflowPolicy::ReportError, false),
            (DecimalOverflowPolicy::ReportError, true),
        ] {
            let r = recipe(DataType::FixedSizeBinary(16), target.clone(), policy, allow);
            let out = output(&r, &input, Selection::all(3));
            assert_eq!(out.values().data_type(), &target);
            match target {
                DataType::Decimal128(..) => {
                    let values = out
                        .values()
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap();
                    assert_eq!(values.value(0), 120);
                }
                DataType::Decimal256(..) => {
                    let values = out
                        .values()
                        .as_any()
                        .downcast_ref::<Decimal256Array>()
                        .unwrap();
                    assert_eq!(values.value(0), arrow_buffer::i256::from_i128(120));
                }
                _ => unreachable!(),
            }
            assert!(out.values().is_null(1));
            assert!(out.values().is_null(2));
            assert_eq!(
                out.errors().len(),
                usize::from(policy == DecimalOverflowPolicy::ReportError)
            );
            if policy == DecimalOverflowPolicy::ReportError {
                assert_eq!(out.errors()[0].selected_ordinal(), 1);
            }
        }
    }
}

#[test]
fn selected_cast_arrow_failure_journal_preserves_only_actual_data_and_its_source() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    for (source, expected) in [
        (
            arrow_schema::ArrowError::CastError("invalid integer".into()),
            crate::DataFailureReason::InvalidValue,
        ),
        (
            arrow_schema::ArrowError::ArithmeticOverflow("coefficient".into()),
            crate::DataFailureReason::NumericOutOfRange,
        ),
    ] {
        let row = ArrayCastFailure::Arrow(source).into_row(7).unwrap();
        assert_eq!(row.selected_ordinal(), 7);
        assert_eq!(row.projected_fact(), expected.failure_fact());
        let mut cause = row.source();
        let mut found = false;
        while let Some(source) = cause {
            found |= source.is::<arrow_schema::ArrowError>();
            cause = source.source();
        }
        assert!(found, "journal retains the actual Arrow source");
    }
    for (source, expected) in [
        (
            arrow_schema::ArrowError::MemoryError("allocation".into()),
            FailureClass::Resource,
        ),
        (
            arrow_schema::ArrowError::OffsetOverflowError(usize::MAX),
            FailureClass::Representation,
        ),
        (
            arrow_schema::ArrowError::SchemaError("carrier".into()),
            FailureClass::Invariant,
        ),
    ] {
        let outer = ArrayCastFailure::Arrow(source).into_row(7).unwrap_err();
        assert_eq!(outer.failure_fact().class(), expected);
        let mut cause = outer.source();
        let mut found = false;
        while let Some(source) = cause {
            found |= source.is::<arrow_schema::ArrowError>();
            cause = source.source();
        }
        assert!(found, "outer failure retains the actual Arrow source");
    }
    let original = KernelFailure::Cancelled;
    assert_eq!(
        ArrayCastFailure::Outer(original.clone())
            .into_row(7)
            .unwrap_err(),
        original
    );
}

fn p05_modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
fn p05_selected_value_case(source: ArrayRef, expected: ArrayRef) {
    for (policy, allow) in p05_modes() {
        let r = recipe(
            source.data_type().clone(),
            expected.data_type().clone(),
            policy,
            allow,
        );
        let result = output(&r, &source, Selection::all(source.len()));
        assert_eq!(
            result.values().to_data(),
            expected.to_data(),
            "{:?} -> {:?}",
            source.data_type(),
            expected.data_type()
        );
        assert!(result.errors().is_empty());
    }
}

#[test]
fn p05_numeric_source_widths_and_nonfinite_to_wide_decimal() {
    use arrow_array::*;
    // Decimal coefficient 1200 at scale 2 denotes exactly integer 12.
    let expected = Arc::new(
        Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1200)), None])
            .with_precision_and_scale(40, 2)
            .unwrap(),
    ) as ArrayRef;
    let sources: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(12), None])),
        Arc::new(Int16Array::from(vec![Some(12), None])),
        Arc::new(Int32Array::from(vec![Some(12), None])),
        Arc::new(Int64Array::from(vec![Some(12), None])),
        Arc::new(UInt8Array::from(vec![Some(12), None])),
        Arc::new(UInt16Array::from(vec![Some(12), None])),
        Arc::new(UInt32Array::from(vec![Some(12), None])),
        Arc::new(UInt64Array::from(vec![Some(12), None])),
        Arc::new(Float32Array::from(vec![Some(12.0), None])),
        Arc::new(Float64Array::from(vec![Some(12.0), None])),
    ];
    for source in sources {
        if matches!(
            source.data_type(),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        ) {
            let narrow = Arc::new(
                Decimal128Array::from(vec![Some(1200), None])
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
            ) as ArrayRef;
            p05_selected_value_case(source.clone(), narrow);
        }
        p05_selected_value_case(source, expected.clone());
    }
    p05_selected_value_case(
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(
            Decimal256Array::from(vec![
                Some(arrow_buffer::i256::ZERO),
                Some(arrow_buffer::i256::ZERO),
                None,
            ])
            .with_precision_and_scale(40, -2)
            .unwrap(),
        ),
    );
    for source in [
        Arc::new(Float32Array::from(vec![
            Some(12.0),
            Some(1000.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            None,
        ])) as ArrayRef,
        Arc::new(Float64Array::from(vec![
            Some(12.0),
            Some(1000.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            None,
        ])) as ArrayRef,
    ] {
        for (policy, allow) in p05_modes() {
            let r = recipe(
                source.data_type().clone(),
                DataType::Decimal256(3, 0),
                policy,
                allow,
            );
            let result = output(&r, &source, Selection::all(5));
            let values = result
                .values()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();
            assert_eq!(values.value(0), arrow_buffer::i256::from_i128(12));
            for row in 1..5 {
                assert!(values.is_null(row));
            }
            assert_eq!(
                result.errors().len(),
                usize::from(policy == DecimalOverflowPolicy::ReportError)
            );
            if policy == DecimalOverflowPolicy::ReportError {
                assert_eq!(result.errors()[0].selected_ordinal(), 1);
            }
        }
    }
}

#[test]
fn p05_decimal_unsigned_width_limits_and_wide_rescale_policy() {
    use arrow_array::*;
    // Integer endpoints are fixed by their widths, independent of Arrow's
    // decimal implementation. Negative-to-unsigned and max+1 cannot fit.
    macro_rules! width {
        ($array:ty, $target:expr, $max:expr) => {{
            let max = $max as i128;
            for source in [
                Arc::new(
                    Decimal128Array::from(vec![Some(0), Some(max), Some(-1), Some(max + 1), None])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(arrow_buffer::i256::ZERO),
                        Some(arrow_buffer::i256::from_i128(max)),
                        Some(arrow_buffer::i256::from_i128(-1)),
                        Some(arrow_buffer::i256::from_i128(max + 1)),
                        None,
                    ])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
                ) as ArrayRef,
            ] {
                for (policy, allow) in p05_modes() {
                    let target = $target;
                    let r = recipe(source.data_type().clone(), target, policy, allow);
                    let result = output(&r, &source, Selection::all(5));
                    let values = result.values().as_any().downcast_ref::<$array>().unwrap();
                    assert_eq!(values.value(0) as u128, 0);
                    assert_eq!(values.value(1) as u128, max as u128);
                    for row in 2..5 {
                        assert!(values.is_null(row));
                    }
                    assert_eq!(
                        result.errors().len(),
                        if policy == DecimalOverflowPolicy::ReportError {
                            2
                        } else {
                            0
                        }
                    );
                    if policy == DecimalOverflowPolicy::ReportError {
                        assert_eq!(
                            result
                                .errors()
                                .iter()
                                .map(|e| e.selected_ordinal())
                                .collect::<Vec<_>>(),
                            vec![2, 3]
                        );
                    }
                }
            }
        }};
    }
    width!(UInt8Array, DataType::UInt8, u8::MAX);
    width!(UInt16Array, DataType::UInt16, u16::MAX);
    width!(UInt32Array, DataType::UInt32, u32::MAX);
    width!(UInt64Array, DataType::UInt64, u64::MAX);
    let source = Arc::new(
        Decimal256Array::from(vec![
            Some(arrow_buffer::i256::from_i128(125)),
            Some(arrow_buffer::i256::from_i128(-125)),
            None,
        ])
        .with_precision_and_scale(40, 2)
        .unwrap(),
    ) as ArrayRef;
    let expected = Arc::new(
        Decimal256Array::from(vec![
            Some(arrow_buffer::i256::from_i128(13)),
            Some(arrow_buffer::i256::from_i128(-13)),
            None,
        ])
        .with_precision_and_scale(40, 1)
        .unwrap(),
    ) as ArrayRef;
    // Decimal rescale rounds the two half ties away from zero.
    p05_selected_value_case(source, expected);
    p05_selected_value_case(
        Arc::new(
            Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(12)), None])
                .with_precision_and_scale(40, -2)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1200)), None])
                .with_precision_and_scale(40, 0)
                .unwrap(),
        ),
    );
    p05_selected_value_case(
        Arc::new(StringArray::from(vec![
            Some("1.25"),
            Some("-1.25"),
            Some(""),
            Some("  "),
            Some("bad"),
            None,
        ])),
        Arc::new(
            Decimal256Array::from(vec![
                Some(arrow_buffer::i256::from_i128(13)),
                Some(arrow_buffer::i256::from_i128(-13)),
                None,
                None,
                None,
                None,
            ])
            .with_precision_and_scale(8, 1)
            .unwrap(),
        ),
    );
    let source = Arc::new(
        Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1000)), None])
            .with_precision_and_scale(40, 0)
            .unwrap(),
    ) as ArrayRef;
    for (policy, allow) in p05_modes() {
        let r = recipe(
            source.data_type().clone(),
            DataType::Decimal256(3, 0),
            policy,
            allow,
        );
        let result = output(&r, &source, Selection::all(2));
        assert_eq!(result.values().data_type(), &DataType::Decimal256(3, 0));
        assert!(result.values().is_null(0));
        assert!(result.values().is_null(1));
        assert_eq!(
            result.errors().len(),
            usize::from(policy == DecimalOverflowPolicy::ReportError || allow)
        );
        if !result.errors().is_empty() {
            assert_eq!(result.errors()[0].selected_ordinal(), 0);
        }
    }
}

#[test]
fn p05_offset_width_and_fixed_bytes_are_exact_under_every_mode() {
    use arrow_array::*;
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(BinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
            Arc::new(LargeBinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
        ),
        (
            Arc::new(LargeStringArray::from(vec![Some("a\0b"), None])),
            Arc::new(StringArray::from(vec![Some("a\0b"), None])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("a\0b"), None])),
            Arc::new(BinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("a\0b"), None])),
            Arc::new(LargeBinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
        ),
        (
            Arc::new(LargeBinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
            Arc::new(LargeStringArray::from(vec![Some("a\0b"), None])),
        ),
    ];
    for (source, expected) in cases {
        p05_selected_value_case(source, expected);
    }
    // Integer-to-binary exposes native-endian bytes of its actual width.
    macro_rules! integer_bytes {
        ($array:ty, $value:expr) => {
            p05_selected_value_case(
                Arc::new(<$array>::from(vec![Some($value), None])),
                Arc::new(BinaryArray::from(vec![
                    Some($value.to_ne_bytes().as_slice()),
                    None,
                ])),
            );
        };
    }
    integer_bytes!(Int8Array, 12_i8);
    integer_bytes!(Int16Array, 12_i16);
    integer_bytes!(Int32Array, 12_i32);
    integer_bytes!(Int64Array, 12_i64);
    integer_bytes!(UInt8Array, 12_u8);
    integer_bytes!(UInt16Array, 12_u16);
    integer_bytes!(UInt32Array, 12_u32);
    integer_bytes!(UInt64Array, 12_u64);
    for width in [4, 16] {
        let bytes = vec![b'x'; width as usize];
        let source = Arc::new(LargeBinaryArray::from(vec![
            Some(bytes.as_slice()),
            Some(b"bad".as_slice()),
            None,
        ])) as ArrayRef;
        for (policy, allow) in p05_modes() {
            let r = recipe(
                DataType::LargeBinary,
                DataType::FixedSizeBinary(width),
                policy,
                allow,
            );
            let fixed = output(&r, &source, Selection::all(3));
            let values = fixed
                .values()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(values.value(0), bytes);
            assert!(values.is_null(1));
            assert!(values.is_null(2));
            assert!(fixed.errors().is_empty());
            let r = recipe(
                DataType::FixedSizeBinary(width),
                DataType::LargeBinary,
                policy,
                allow,
            );
            let restored = output(&r, fixed.values(), Selection::all(3));
            let values = restored
                .values()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert_eq!(values.value(0), bytes);
            assert!(values.is_null(1));
            assert!(values.is_null(2));
            assert!(restored.errors().is_empty());
        }
    }
}

#[test]
fn p05_timestamp_units_and_time_native_carriers_preserve_exact_counts() {
    use arrow_array::*;
    // Timestamp-to-numeric exposes the source unit count, not seconds.
    for source in [
        Arc::new(TimestampSecondArray::from(vec![Some(123456), None])) as ArrayRef,
        Arc::new(TimestampMillisecondArray::from(vec![Some(123456), None])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(123456), None])),
        Arc::new(TimestampNanosecondArray::from(vec![Some(123456), None])),
    ] {
        p05_selected_value_case(
            source.clone(),
            Arc::new(Int64Array::from(vec![Some(123456), None])),
        );
        p05_selected_value_case(
            source,
            Arc::new(Float64Array::from(vec![Some(123456.0), None])),
        );
    }
    for source in [
        Arc::new(Time32SecondArray::from(vec![Some(1), None])) as ArrayRef,
        Arc::new(Time32MillisecondArray::from(vec![Some(1000), None])),
        Arc::new(Time64MicrosecondArray::from(vec![Some(1_000_000), None])),
        Arc::new(Time64NanosecondArray::from(vec![Some(1_000_000_000), None])),
    ] {
        if source.data_type() == &DataType::Time64(TimeUnit::Microsecond) {
            p05_selected_value_case(
                source,
                Arc::new(Time64NanosecondArray::from(vec![Some(1_000_000_000), None])),
            );
        } else {
            p05_selected_value_case(
                source,
                Arc::new(Time64MicrosecondArray::from(vec![Some(1_000_000), None])),
            );
        }
    }
    // Chrono's default time rendering for an exact second has no fraction.
    p05_selected_value_case(
        Arc::new(Time64MicrosecondArray::from(vec![Some(1_000_000), None])),
        Arc::new(StringArray::from(vec![Some("00:00:01"), None])),
    );
}

#[test]
fn p05_list_and_large_list_retag_keep_offsets_nulls_and_metadata_independently() {
    use arrow_array::{LargeListArray, ListArray};
    use std::collections::HashMap;
    let mut metadata = HashMap::new();
    metadata.insert("owner".to_string(), "p05-target".to_string());
    // Independent input rows are [1,2], NULL, [3]. No old cast/take/core
    // produces the expected child values or the expected compact offsets.
    let list = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]);
    let large = LargeListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]);
    for source in [Arc::new(list) as ArrayRef, Arc::new(large) as ArrayRef] {
        let child =
            Arc::new(Field::new("renamed", DataType::Int32, true).with_metadata(metadata.clone()));
        let target = if matches!(source.data_type(), DataType::List(_)) {
            DataType::List(child)
        } else {
            DataType::LargeList(child)
        };
        for (policy, allow) in p05_modes() {
            let r = recipe(source.data_type().clone(), target.clone(), policy, allow);
            assert!(
                r.uses_selected_array(),
                "this exercises the new structural author"
            );
            let selected = [0, 1, 2];
            let result = output(&r, &source, Selection::try_sparse(3, &selected).unwrap());
            assert_eq!(result.values().data_type(), &target);
            assert!(result.values().is_null(1));
            assert!(result.errors().is_empty());
            let (offsets, values): (Vec<i64>, &ArrayRef) =
                if let Some(list) = result.values().as_any().downcast_ref::<ListArray>() {
                    (
                        list.value_offsets().iter().map(|v| *v as i64).collect(),
                        list.values(),
                    )
                } else {
                    let list = result
                        .values()
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .unwrap();
                    (list.value_offsets().to_vec(), list.values())
                };
            assert_eq!(offsets, vec![0, 2, 2, 3]);
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[1, 2, 3]
            );
            let rows = [2];
            let sparse = output(&r, &source, Selection::try_sparse(3, &rows).unwrap());
            assert_eq!(sparse.values().len(), 1);
            assert!(!sparse.values().is_null(0));
            let sparse_values =
                if let Some(list) = sparse.values().as_any().downcast_ref::<ListArray>() {
                    list.values()
                } else {
                    sparse
                        .values()
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .unwrap()
                        .values()
                };
            assert_eq!(
                sparse_values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[3]
            );
        }
    }
}

#[test]
fn p05_struct_parent_null_and_metadata_follow_actual_by_name_permutation() {
    use std::collections::HashMap;
    let mut metadata = HashMap::new();
    metadata.insert("owner".to_string(), "p05-target".to_string());
    let source = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::FixedSizeBinary(4), true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([b"abcd", b"wxyz", b"uvwx"].into_iter())
                    .unwrap(),
            ),
        ],
        Some(arrow_buffer::NullBuffer::from(vec![true, false, true])),
    )) as ArrayRef;
    let target = DataType::Struct(
        vec![
            Arc::new(
                Field::new("b", DataType::FixedSizeBinary(4), true).with_metadata(metadata.clone()),
            ),
            Arc::new(Field::new("a", DataType::Int32, true).with_metadata(metadata)),
        ]
        .into(),
    );
    // Every target name exists: output b,a is a permutation, not ordinal
    // relabeling. The selected NULL parent remains NULL independent of children.
    for (policy, allow) in p05_modes() {
        let r = recipe(source.data_type().clone(), target.clone(), policy, allow);
        let rows = [1, 2];
        let result = output(&r, &source, Selection::try_sparse(3, &rows).unwrap());
        assert_eq!(result.values().data_type(), &target);
        assert!(result.errors().is_empty());
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(values.is_null(0));
        assert!(!values.is_null(1));
        assert_eq!(
            values
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(1),
            b"uvwx"
        );
        assert_eq!(
            values
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            3
        );
    }
}
