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
//! Permanent full live variadic numeric declaration; exact Binary payload comparison.
use super::generate::{InputGenerator, InputProfile};
use super::*;
use arrow::array::{Decimal128Array, Float64Array};
fn check(ty: FunctionValueType, a: ArrayRef, arity: usize) {
    let mut spec = ScalarDiffSpec::new("percentile_hash").sparse_selections(3, 71231);
    for _ in 0..arity {
        spec = spec.typed_column(ty.clone(), a.clone());
    }
    assert_scalar_matches_v1(spec);
}
#[test]
fn pure_differential_percentile_hash_complete_numeric_variadic_declaration() {
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ] {
        for nullable in [true, false] {
            let ty = FunctionValueType::new(ty.clone(), nullable);
            let a = InputGenerator::new(713).column(&ty, 17, &InputProfile::default());
            for a in [a.clone(), a.slice(1, 7), a.slice(0, 0)] {
                for arity in [1, 2, 5] {
                    check(ty.clone(), a.clone(), arity);
                }
            }
        }
    }
    for logical in [
        ValueLogicalType::Physical,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Uuid,
    ] {
        for nullable in [true, false] {
            let ty = FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                nullable,
                logical,
            )
            .unwrap();
            let a = novarocks_functions::largeint::array_from_i128(&[
                Some(i128::MIN),
                Some(i128::MAX),
                if nullable { None } else { Some(0) },
                Some(-1),
                Some(0),
            ])
            .unwrap();
            for arity in [1, 2, 5] {
                check(ty.clone(), a.clone(), arity);
            }
        }
    }
}
#[test]
fn pure_differential_percentile_hash_all_decimal_precision_signed_scale_and_nullability() {
    // Cover every legal signed scale and precision on complementary axes rather
    // than replaying the unchanged numeric reader as a massive Cartesian suite.
    let mut shapes = std::collections::BTreeSet::new();
    for p in 1..=38 {
        for s in [-128, -76, -38, -1, 0, p as i8] {
            shapes.insert((p, s));
        }
    }
    for s in i8::MIN..=38 {
        shapes.insert((38, s));
    }
    for (p, s) in shapes {
        for nullable in [true, false] {
            let ty = FunctionValueType::new(DataType::Decimal128(p, s), nullable);
            let a = Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(i128::MAX),
                    Some(-1),
                    Some(0),
                    if nullable { None } else { Some(1) },
                ])
                .with_precision_and_scale(p, s)
                .unwrap(),
            ) as ArrayRef;
            for arity in [1, 3] {
                check(ty.clone(), a.clone(), arity);
            }
        }
    }
}
#[test]
fn pure_differential_percentile_hash_ieee_state_bytes_constants_literal_and_pool() {
    let a = Arc::new(Float64Array::from(vec![
        Some(0.),
        Some(-0.),
        Some(f64::NAN),
        Some(f64::from_bits(0x7ff8_0000_0000_0123)),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(16777217.),
        None,
    ])) as ArrayRef;
    for arity in [1, 2, 5] {
        check(
            FunctionValueType::new(DataType::Float64, true),
            a.clone(),
            arity,
        );
    }
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for v in [
            None,
            Some(0.),
            Some(-0.),
            Some(16777217.),
            Some(f64::INFINITY),
            Some(f64::NAN),
        ] {
            for arity in [1, 2, 5] {
                let mut spec = ScalarDiffSpec::new("percentile_hash")
                    .legacy_constants(form)
                    .sparse_selections(3, 71233);
                for _ in 0..arity {
                    spec = spec.constant_array(Arc::new(Float64Array::from(vec![v])));
                }
                assert_scalar_matches_v1(spec);
            }
        }
    }
}
