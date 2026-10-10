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
//! Expanded exact source-shape witnesses for the ONE dynamic ROUND overload.
//! Safe witnesses here do not close its full domain: round_disagreement_tests
//! separately pins valid overflow rows and original minimum-digits panic.
use super::{
    DifferentialFailure, FloatComparison, ScalarDiffSpec, assert_scalar_matches_v1,
    run_scalar_differential,
};
use arrow::array::{
    types::{Int8Type, Int16Type},
    *,
};
use arrow::datatypes::{DataType, Field, TimeUnit, UnionFields};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;
fn leaves() -> Vec<DataType> {
    let mut types = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit.clone(), None));
        types.push(DataType::Duration(unit));
    }
    types
}
fn value(ty: &DataType) -> ArrayRef {
    let numbers = vec![
        Some(-7),
        Some(-3),
        Some(-1),
        Some(0),
        Some(1),
        Some(3),
        Some(7),
        None,
    ];
    let signed: ArrayRef = Arc::new(Int64Array::from(numbers.clone()));
    let signed32: ArrayRef = Arc::new(Int32Array::from(
        numbers
            .iter()
            .map(|x| x.map(|v| v as i32))
            .collect::<Vec<_>>(),
    ));
    match ty {
        DataType::Null => Arc::new(NullArray::new(8)),
        DataType::Boolean => Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            None,
        ])),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let text: ArrayRef = Arc::new(StringArray::from(vec![
                Some("-2.5"),
                Some("bad"),
                Some("3"),
                Some("0"),
                Some("+1"),
                Some("2.5"),
                Some("inf"),
                None,
            ]));
            arrow::compute::cast(&text, ty).unwrap()
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let f: ArrayRef = Arc::new(Float64Array::from(vec![
                Some(-2.5),
                Some(-0.0),
                Some(1.25),
                Some(0.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ]));
            arrow::compute::cast(&f, ty).unwrap()
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let u: ArrayRef = Arc::new(UInt64Array::from(vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(7),
                Some(127),
                Some(255),
                None,
            ]));
            arrow::compute::cast(&u, ty).unwrap()
        }
        DataType::Date32 | DataType::Time32(_) => arrow::compute::cast(&signed32, ty).unwrap(),
        _ => arrow::compute::cast(&signed, ty).unwrap(),
    }
}
fn check(spec: ScalarDiffSpec) {
    let summary = assert_scalar_matches_v1(
        spec.float_comparison(FloatComparison::Exact)
            .sparse_selections(7, 0x524f554e44),
    );
    assert_eq!(summary.overload.as_str(), "builtin.scalar/round/dynamic-v1");
    assert_eq!(summary.legacy_batch_errors, 0);
    assert_eq!(summary.attributed_row_errors, 0);
}
#[test]
fn pure_differential_round_dynamic_all28_admitted_leaf_value_shapes_unary_binary_slice_null_empty()
{
    let digits: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(0),
        Some(1),
        Some(-2),
        Some(309),
        Some(-309),
        Some(4294967296),
        Some(i64::MAX),
        None,
    ]));
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let sources = leaves()
            .into_iter()
            .filter(|ty| arrow::compute::can_cast_types(ty, &DataType::Float64))
            .collect::<Vec<_>>();
        assert_eq!(sources.len(), 28);
        for ty in sources {
            let source = value(&ty);
            check(
                ScalarDiffSpec::new("round")
                    .column(source.clone())
                    .decimal_overflow(policy),
            );
            // Modest decimal source witnesses avoid an existing, separately pinned failure.
            let d = if matches!(ty, DataType::Decimal128(_, _)) {
                Arc::new(Int64Array::from(vec![
                    Some(0),
                    Some(1),
                    Some(-2),
                    Some(2),
                    Some(0),
                    Some(1),
                    Some(2),
                    None,
                ])) as ArrayRef
            } else {
                digits.clone()
            };
            check(
                ScalarDiffSpec::new("round")
                    .column(source.clone())
                    .column(d.clone())
                    .decimal_overflow(policy),
            );
            check(
                ScalarDiffSpec::new("round")
                    .column(source.slice(1, 6))
                    .column(d.slice(1, 6))
                    .decimal_overflow(policy),
            );
            check(
                ScalarDiffSpec::new("round")
                    .column(new_null_array(&ty, 8))
                    .column(d)
                    .decimal_overflow(policy),
            );
            check(
                ScalarDiffSpec::new("round")
                    .column(new_empty_array(&ty))
                    .decimal_overflow(policy),
            );
        }
    }
}
#[test]
fn pure_differential_round_dynamic_all34_leaf_digits_shapes_columns_and_constant() {
    let source: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.25),
        Some(-1.25),
        Some(-0.0),
        Some(0.0),
        Some(2.5),
        Some(-2.5),
        Some(3.0),
        None,
    ]));
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for ty in leaves() {
            let digits = value(&ty);
            check(
                ScalarDiffSpec::new("round")
                    .column(source.clone())
                    .column(digits.clone())
                    .decimal_overflow(policy),
            );
            check(
                ScalarDiffSpec::new("round")
                    .column(source.clone())
                    .constant_array(digits.slice(3, 1))
                    .decimal_overflow(policy),
            );
            check(
                ScalarDiffSpec::new("round")
                    .constant_array(source.slice(1, 1))
                    .column(digits)
                    .decimal_overflow(policy),
            );
        }
    }
}
#[test]
fn pure_differential_round_dynamic_encoded_cast_paths_original_value_and_digits_sources() {
    let f: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(99.0),
        Some(1.25),
        None,
        Some(-2.5),
    ]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(2), Some(3)]),
            f.clone(),
        )
        .unwrap(),
    );
    let run: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1, 3, 4, 6]),
            f.as_any().downcast_ref::<Float64Array>().unwrap(),
        )
        .unwrap(),
    );
    let list: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float64, true)),
            1,
            f.clone(),
            None,
        )
        .unwrap(),
    );
    let union: ArrayRef = Arc::new(
        UnionArray::try_new(
            UnionFields::try_new(
                [2, 7],
                [
                    Field::new("integer", DataType::Int64, true),
                    Field::new("floating", DataType::Float64, true),
                ],
            )
            .unwrap(),
            vec![2_i8, 7, 7, 2, 7].into(),
            Some(vec![0_i32, 0, 1, 1, 2].into()),
            vec![Arc::new(Int64Array::from(vec![99, 99])), f.slice(1, 3)],
        )
        .unwrap(),
    );
    for array in [dictionary, run, list, union] {
        let len = array.len();
        let digits: ArrayRef = Arc::new(Int64Array::from(vec![Some(1); len]));
        check(ScalarDiffSpec::new("round").column(array.clone()));
        check(
            ScalarDiffSpec::new("round")
                .column(array.clone())
                .column(digits),
        );
        let source: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.25); len]));
        check(ScalarDiffSpec::new("round").column(source).column(array));
    }
}
#[test]
fn pure_differential_round_decimal_result_scale_source_constants_wrapped_and_runtime() {
    for scale in [-2_i8, 0, 2, 6] {
        let source: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12345), Some(-12345), None])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        for digits in [-2_i64, 0, 1, 2, 6, 257] {
            let d: ArrayRef = Arc::new(Int64Array::from(vec![Some(digits); 3]));
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for spec in [
                    ScalarDiffSpec::new("round")
                        .column(source.clone())
                        .column(d.clone())
                        .decimal_overflow(policy),
                    ScalarDiffSpec::new("round")
                        .column(source.clone())
                        .constant_array(d.slice(0, 1))
                        .decimal_overflow(policy),
                ] {
                    if digits == 257 && scale <= 2 {
                        // This legal shape is executed and explicitly recorded as an
                        // existing discrepancy, never skipped or counted as equality.
                        let result = run_scalar_differential(&spec);
                        assert!(
                            matches!(result, Err(DifferentialFailure::Mismatch { .. })),
                            "existing factor overflow gap: {result:?}"
                        );
                    } else {
                        check(spec);
                    }
                }
            }
        }
        check(ScalarDiffSpec::new("round").typed_column(
            FunctionValueType::new(source.data_type().clone(), true),
            source,
        ));
    }
}

#[test]
fn pure_differential_round_exact_dynamic_admission_retains_non_float_temporal_refusals() {
    let refused = leaves()
        .into_iter()
        .filter(|ty| !arrow::compute::can_cast_types(ty, &DataType::Float64))
        .collect::<Vec<_>>();
    assert_eq!(refused.len(), 6);
    for ty in refused {
        let result = run_scalar_differential(&ScalarDiffSpec::new("round").column(value(&ty)));
        assert!(
            matches!(result, Err(DifferentialFailure::Resolution { .. })),
            "actual value capability must remain refused for {ty:?}: {result:?}"
        );
        // These same physical leaves are admitted as digits through the actual
        // Int64 cast author; the preceding digits matrix executes that path.
        assert!(arrow::compute::can_cast_types(&ty, &DataType::Int64));
    }
}

#[test]
fn pure_differential_round_dynamic_all28_by34_admitted_leaf_pair_witnesses() {
    let sources = leaves()
        .into_iter()
        .filter(|ty| arrow::compute::can_cast_types(ty, &DataType::Float64))
        .collect::<Vec<_>>();
    let digits = leaves();
    assert_eq!(sources.len(), 28);
    assert_eq!(digits.len(), 34);
    for source_type in sources {
        for digits_type in &digits {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                // This matrix covers every enumerated carrier pair, with the
                // counterexample domain separately exercised, never excluded
                // from the generic overload's completion assessment.
                let spec = ScalarDiffSpec::new("round")
                    .column(value(&source_type))
                    .column(value(digits_type))
                    .decimal_overflow(policy);
                if matches!(source_type, DataType::Decimal128(_, _))
                    && matches!(
                        digits_type,
                        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                    )
                {
                    // The enumerated digits include 127/255, so the original
                    // 38-scale primary multiplication overflows on real rows.
                    let result = run_scalar_differential(&spec);
                    assert!(
                        matches!(result, Err(DifferentialFailure::Mismatch { .. })),
                        "original required error versus owner policy: {result:?}"
                    );
                } else {
                    check(spec);
                }
            }
        }
    }
}
#[test]
fn pure_differential_round_integer_value_precision_loss_is_original_arrow_float_projection() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(i64::MIN),
        Some(-9007199254740993),
        Some(9007199254740993),
        Some(i64::MAX),
        None,
    ]));
    check(ScalarDiffSpec::new("round").column(source.clone()));
    check(
        ScalarDiffSpec::new("round")
            .column(source)
            .column(Arc::new(Int64Array::from(vec![Some(0); 5]))),
    );
}
