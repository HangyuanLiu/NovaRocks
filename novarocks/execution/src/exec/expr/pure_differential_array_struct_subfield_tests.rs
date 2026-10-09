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
//! Permanent full-FVT profiles for both original subfield authors.
use super::super::legacy_struct_subfield_baseline_tests::{list, structure};
use super::generate::{InputGenerator, InputProfile};
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{Array, ArrayRef, Int32Array, StringArray, StructArray, new_null_array};
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionFields, UnionMode};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
fn check(a: ArrayRef, array: bool) {
    let name = if array {
        "__array_struct_subfield"
    } else {
        "__struct_subfield"
    };
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .typed_column(
                    FunctionValueType::new(a.data_type().clone(), true),
                    a.clone(),
                )
                .constant_array(Arc::new(StringArray::from(vec!["Chosen"])))
                .legacy_constants(form)
                .float_comparison(super::FloatComparison::Exact)
                .sparse_selections(7, 98131),
        );
    }
}
fn both(value: ArrayRef) {
    let n = value.len();
    let s = structure(value, Some((0..n).map(|i| i % 7 != 1).collect()));
    let a = list(
        s,
        (0..=n / 2).map(|i| (i * 2) as i32).collect(),
        Some((0..n / 2).map(|i| i % 5 != 2).collect()),
    );
    check(a, true);
}
#[test]
fn pure_differential_array_struct_subfield_all_generated_flat_child_profiles() {
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
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Date32,
        DataType::Decimal128(38, -2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 6),
    ];
    for u in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(u, None));
    }
    for t in types {
        both(InputGenerator::new(98132).column(
            &FunctionValueType::new(t, true),
            514,
            &InputProfile::default(),
        ));
    }
}
#[test]
fn pure_differential_array_struct_subfield_full_nested_arrow_domain_and_slice_shapes() {
    let f = |ty| {
        Arc::new(
            Field::new("original-child", ty, true)
                .with_metadata([("opaque".into(), "retained".into())].into()),
        )
    };
    let mut ts = vec![
        DataType::Date64,
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::FixedSizeBinary(0),
        DataType::FixedSizeBinary(3),
        DataType::FixedSizeBinary(16),
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::List(f(DataType::Utf8)),
        DataType::LargeList(f(DataType::Int64)),
        DataType::FixedSizeList(f(DataType::Int32), 3),
        DataType::ListView(f(DataType::Utf8)),
        DataType::LargeListView(f(DataType::Utf8)),
        DataType::Struct(vec![f(DataType::Utf8), f(DataType::Int32)].into()),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        DataType::Union(
            UnionFields::new(
                [1, 2],
                [
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ],
            ),
            UnionMode::Dense,
        ),
        DataType::Union(
            UnionFields::new(
                [1, 2],
                [
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ],
            ),
            UnionMode::Sparse,
        ),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            f(DataType::Utf8),
        ),
    ];
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                f(DataType::Utf8),
            ]
            .into(),
        ),
        false,
    );
    ts.push(DataType::Map(Arc::new(entries), false));
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        ts.push(DataType::Duration(unit));
        ts.push(DataType::Timestamp(unit, Some("+09:00".into())));
    }
    for t in ts {
        let a = new_null_array(&t, 8);
        both(a.clone());
        let s = structure(
            a,
            Some(vec![true, false, true, true, true, false, true, true]),
        );
        check(
            list(s, vec![0, 2, 4, 6, 8], Some(vec![true, false, true, true])).slice(1, 2),
            true,
        );
    }
}
#[test]
fn pure_differential_array_struct_subfield_nested_nominal_child_tags_remain_explicit() {
    for logical in [
        ValueLogicalType::Json,
        ValueLogicalType::Variant,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
        ValueLogicalType::Uuid,
    ] {
        let (ty, a): (DataType, ArrayRef) = match logical {
            ValueLogicalType::Json => (
                DataType::Utf8,
                Arc::new(StringArray::from(vec![Some("{}"), None])),
            ),
            ValueLogicalType::Variant => (
                DataType::LargeBinary,
                Arc::new(arrow::array::LargeBinaryArray::from(vec![
                    Some(&b"raw"[..]),
                    None,
                ])),
            ),
            ValueLogicalType::LargeInt | ValueLogicalType::Uuid => (
                DataType::FixedSizeBinary(16),
                novarocks_types::largeint::array_from_i128(&[Some(i128::MIN), None]).unwrap(),
            ),
            _ => (
                DataType::Binary,
                Arc::new(arrow::array::BinaryArray::from(vec![
                    Some(&b"raw"[..]),
                    None,
                ])),
            ),
        };
        let field = FunctionValueType::try_with_logical_type(ty, true, logical)
            .unwrap()
            .try_to_field("Chosen")
            .unwrap();
        let s: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::new(field)].into(),
            vec![a],
            None,
        ));
        check(list(s, vec![0, 1, 2], None), true);
    }
}
#[test]
fn pure_differential_array_struct_subfield_constant_pool_literal_broadcast_and_nonzero_slices() {
    for array in [true] {
        for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
            let s = structure(Arc::new(Int32Array::from(vec![Some(9), None])), None);
            let a = if array {
                list(s, vec![0, 1, 2], None)
            } else {
                s
            };
            let name = if array {
                "__array_struct_subfield"
            } else {
                "__struct_subfield"
            };
            for first in [0, 1] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .constant_array(a.slice(first, 1))
                        .constant_array(Arc::new(StringArray::from(vec!["Chosen"])))
                        .constant_rows(257)
                        .legacy_constants(form)
                        .sparse_selections(7, 98134),
                );
            }
        }
    }
}
#[test]
fn pure_differential_array_struct_subfield_bound_case_insensitive_name_preserves_original_exact_error()
 {
    for array in [true] {
        let s = structure(Arc::new(Int32Array::from(vec![1, 2])), None);
        let a = if array {
            list(s, vec![0, 1, 2], None)
        } else {
            s
        };
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(if array {
                "__array_struct_subfield"
            } else {
                "__struct_subfield"
            })
            .column(a)
            .constant_array(Arc::new(StringArray::from(vec!["chosen"]))),
        );
    }
}

#[test]
fn array_struct_subfield_original_binding_constant_source_and_full_logical_arity() {
    use super::resolve_like_sql;
    use novarocks_functions::FunctionKind;
    let catalog = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let a = list(
        structure(Arc::new(Int32Array::from(vec![Some(1), None])), None),
        vec![0, 1, 2],
        None,
    );
    for field in ["Chosen", "chosen"] {
        for count in [2, 3, 5] {
            let mut spec = ScalarDiffSpec::new("__array_struct_subfield")
                .column(a.clone())
                .constant_array(Arc::new(StringArray::from(vec![field])));
            for _ in 2..count {
                spec = spec.constant_array(Arc::new(Int32Array::from(vec![7])));
            }
            let bound = resolve_like_sql(
                catalog,
                "__array_struct_subfield",
                FunctionKind::Scalar,
                &spec.arguments,
            )
            .unwrap();
            assert_eq!(
                bound.function_id.as_str(),
                "builtin.scalar/__array_struct_subfield/v1"
            );
            assert_eq!(
                bound.selected.overload.as_str(),
                "builtin.scalar/__array_struct_subfield/dynamic-v1"
            );
            assert_eq!(bound.selected.argument_types.len(), count);
        }
    }
    // A field-name column without the original known-constant source fact is
    // not the admitted call, even when its observed values happen to agree.
    let no_constant = ScalarDiffSpec::new("__array_struct_subfield")
        .column(a)
        .column(Arc::new(StringArray::from(vec!["Chosen", "Chosen"])));
    assert!(
        resolve_like_sql(
            catalog,
            "__array_struct_subfield",
            FunctionKind::Scalar,
            &no_constant.arguments
        )
        .is_err()
    );
}
#[test]
fn pure_differential_array_struct_subfield_full_bound_extra_arity_arena_errors() {
    let a = list(
        structure(Arc::new(Int32Array::from(vec![1, 2])), None),
        vec![0, 1, 2],
        None,
    );
    for count in [3, 5] {
        let mut spec = ScalarDiffSpec::new("__array_struct_subfield")
            .column(a.clone())
            .constant_array(Arc::new(StringArray::from(vec!["Chosen"])));
        for _ in 2..count {
            spec = spec.constant_array(Arc::new(Int32Array::from(vec![7])));
        }
        assert_scalar_matches_v1(spec);
    }
}
