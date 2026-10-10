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
//! Permanent single strict-variadic FIELD overload and all actual carrier domains.
use super::super::legacy_field_baseline_tests::{generated, profiles};
use super::*;
use arrow::array::{Float64Array, NullArray, StringArray};
#[test]
fn pure_differential_field_one_actual_variadic_overload_all_registered_carriers() {
    for mut ty in profiles() {
        for nullable in [true, false] {
            if ty.data_type == DataType::Null && !nullable {
                continue;
            }
            ty.nullable = nullable;
            let a = generated(&ty, 257);
            for a in [a.clone(), a.slice(1, 7), a.slice(0, 0)] {
                for arity in [2, 3, 5] {
                    let mut spec = ScalarDiffSpec::new("field").sparse_selections(7, 9933);
                    for _ in 0..arity {
                        spec = spec.typed_column(ty.clone(), a.clone());
                    }
                    assert_scalar_matches_v1(spec);
                }
            }
        }
    }
}
#[test]
fn pure_differential_field_float_ieee_first_match_and_pool_literal_constants() {
    let first: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.),
        Some(-0.),
        Some(f64::NAN),
        Some(f64::INFINITY),
        None,
        Some(3.),
    ]));
    let second: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.),
        Some(0.),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(3.),
        Some(4.),
    ]));
    let third: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.),
        Some(1.),
        Some(0.),
        Some(0.),
        None,
        Some(3.),
    ]));
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("field")
            .column(first)
            .column(second)
            .column(third)
            .sparse_selections(7, 9934),
    );
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for item in [Some("九\0"), None] {
            let value: ArrayRef = Arc::new(StringArray::from(vec![item]));
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("field")
                    .constant_array(value.clone())
                    .constant_array(value.clone())
                    .constant_array(value)
                    .constant_rows(257)
                    .legacy_constants(form)
                    .sparse_selections(7, 9935),
            );
        }
    }
}
#[test]
fn pure_differential_field_actual_sql_null_coercion_and_generic_nominal_tags() {
    // Materialize precisely the original selected argument coercions, as SQL
    // does. A CVal does not independently choose a FIELD comparison strategy.
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(4)),
        Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        Arc::new(StringArray::from(vec!["b", "a", "c", "x"])),
    ];
    let arguments = arrays
        .into_iter()
        .map(|values| DiffArgument::Column {
            value_type: FunctionValueType::new(values.data_type().clone(), true),
            values,
        })
        .collect::<Vec<_>>();
    let bound = resolve_like_sql(
        builtin_engine_function_catalog(),
        "field",
        CatalogKind::Scalar,
        &arguments,
    )
    .unwrap();
    let mut spec = ScalarDiffSpec::new("field").sparse_selections(7, 9936);
    for (arg, target) in arguments.iter().zip(bound.selected.argument_types.iter()) {
        let FunctionArgumentType::Value(ty) = target else {
            panic!("FIELD has value arguments")
        };
        let DiffArgument::Column { values, .. } = arg else {
            unreachable!()
        };
        let actual = arrow::compute::cast(values.as_ref(), &ty.data_type).unwrap();
        spec = spec.typed_column(ty.clone(), actual);
    }
    assert_scalar_matches_v1(spec);
    for logical in [ValueLogicalType::Json, ValueLogicalType::Uuid] {
        let values: ArrayRef = if logical == ValueLogicalType::Json {
            Arc::new(StringArray::from(vec![Some("{}"), Some("raw"), None]))
        } else {
            novarocks_types::largeint::array_from_i128(&[Some(i128::MIN), Some(7), None]).unwrap()
        };
        let ty =
            FunctionValueType::try_with_logical_type(values.data_type().clone(), true, logical)
                .unwrap();
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("field")
                .typed_column(ty.clone(), values.clone())
                .typed_column(ty, values)
                .sparse_selections(7, 9937),
        );
    }
}
