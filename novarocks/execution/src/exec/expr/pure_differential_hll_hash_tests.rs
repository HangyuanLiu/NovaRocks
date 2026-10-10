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
//! Permanent Native N1 original carrier-domain equality. Unsupported raw types stay separate.
use super::generate::{InputGenerator, InputProfile};
use super::*;
use arrow::array::*;
fn check(ty: FunctionValueType, a: ArrayRef) {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("hll_hash")
            .typed_column(ty, a)
            .sparse_selections(3, 792143),
    );
}
#[test]
fn pure_differential_hll_hash_native_n1_full_carriers_and_nominal_types() {
    for ty in [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Date32,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
    ] {
        for nullable in [true, false] {
            let ty = FunctionValueType::new(ty.clone(), nullable);
            let a = InputGenerator::new(712391).column(&ty, 17, &InputProfile::default());
            for a in [a.clone(), a.slice(1, 7), a.slice(0, 0)] {
                check(ty.clone(), a);
            }
        }
    }
    for unit in [
        arrow::datatypes::TimeUnit::Second,
        arrow::datatypes::TimeUnit::Millisecond,
        arrow::datatypes::TimeUnit::Microsecond,
        arrow::datatypes::TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("UTC".into()), Some("America/New_York".into())] {
            let dtype = DataType::Timestamp(unit, zone);
            let a = arrow::compute::cast(
                &Int64Array::from(vec![Some(i64::MIN), Some(0), Some(i64::MAX), None]),
                &dtype,
            )
            .unwrap();
            check(FunctionValueType::new(dtype, true), a);
        }
    }
    for width in [0, 1, 15, 16, 17, 32] {
        let v = vec![0xff; width as usize];
        let a = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(v.as_slice()), None, Some(v.as_slice())].into_iter(),
                width,
            )
            .unwrap(),
        ) as ArrayRef;
        check(
            FunctionValueType::new(DataType::FixedSizeBinary(width), true),
            a.clone(),
        );
        if width == 16 {
            for logical in [ValueLogicalType::LargeInt, ValueLogicalType::Uuid] {
                check(
                    FunctionValueType::try_with_logical_type(
                        DataType::FixedSizeBinary(16),
                        true,
                        logical,
                    )
                    .unwrap(),
                    a.clone(),
                );
            }
        }
    }
}
#[test]
fn pure_differential_hll_hash_native_n1_ieee_unicode_full_decimal_shapes() {
    check(
        FunctionValueType::new(DataType::Float32, true),
        Arc::new(Float32Array::from(vec![
            Some(0.),
            Some(-0.),
            Some(f32::from_bits(0x7fc0_0123)),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            None,
        ])),
    );
    check(
        FunctionValueType::new(DataType::Float64, true),
        Arc::new(Float64Array::from(vec![
            Some(0.),
            Some(-0.),
            Some(f64::from_bits(0x7ff8_0000_0000_0123)),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
        ])),
    );
    check(
        FunctionValueType::new(DataType::Utf8, true),
        Arc::new(StringArray::from(vec![
            Some(""),
            Some("é雪\0"),
            None,
            Some("é雪"),
        ])),
    );
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
        let a = Arc::new(
            Decimal128Array::from(vec![
                Some(i128::MIN),
                Some(i128::MAX),
                None,
                Some(-1),
                Some(0),
            ])
            .with_precision_and_scale(p, s)
            .unwrap(),
        ) as ArrayRef;
        check(FunctionValueType::new(DataType::Decimal128(p, s), true), a);
    }
}
#[test]
fn pure_differential_hll_hash_native_n1_literal_pool_constants_and_nonzero_ordinal() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for a in [
            Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec!["é雪"])),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("hll_hash")
                    .constant_array(a)
                    .legacy_constants(form)
                    .constant_rows(5)
                    .sparse_selections(3, 792144),
            );
        }
    }
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let backing = Arc::new(StringArray::from(vec!["unused", "é雪"])) as ArrayRef;
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("constant").unwrap()),
        ty,
        backing.to_data(),
        constant_policy(),
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap();
    assert_eq!(pool.value(1).unwrap().ordinal(), 1);
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("hll_hash")
            .constant(pool.value(1).unwrap())
            .legacy_constants(LegacyConstantForm::Pool)
            .constant_rows(5)
            .sparse_selections(3, 792145),
    );
}
