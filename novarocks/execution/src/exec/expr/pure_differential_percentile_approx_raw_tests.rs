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
//! Permanent whole declared ANY x ANY scalar comparison, including required error rows.
use super::*;
use arrow::array::*;
use arrow::datatypes::Field;
fn binary(v: &[Option<Vec<u8>>]) -> ArrayRef {
    Arc::new(BinaryArray::from_iter(v.iter().map(|v| v.as_deref())))
}
fn payload() -> Vec<u8> {
    novarocks_functions::approx_percentile_core::encode_single_value(10.)
}
fn check(p: ArrayRef, q: ArrayRef, nullable: bool) {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("percentile_approx_raw")
            .typed_column(FunctionValueType::new(p.data_type().clone(), nullable), p)
            .typed_column(FunctionValueType::new(q.data_type().clone(), nullable), q)
            .sparse_selections(7, 641997),
    );
}
#[test]
fn pure_differential_percentile_approx_raw_all_original_numeric_quantile_carriers() {
    let qs: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(0), None, Some(1)])),
        Arc::new(Int16Array::from(vec![Some(0), None, Some(1)])),
        Arc::new(Int32Array::from(vec![Some(0), None, Some(1)])),
        Arc::new(Int64Array::from(vec![Some(0), None, Some(1)])),
        Arc::new(Float32Array::from(vec![Some(0.5), None, Some(1.)])),
        Arc::new(Float64Array::from(vec![Some(0.5), None, Some(1.)])),
        Arc::new(
            Decimal128Array::from(vec![Some(50), None, Some(100)])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ),
        novarocks_functions::largeint::array_from_i128(&[Some(0), None, Some(1)]).unwrap(),
    ];
    for q in qs {
        let p = binary(&[Some(payload()), None, Some(payload())]);
        check(p.clone(), q.clone(), true);
        check(p.slice(1, 2), q.slice(1, 2), true);
        check(p.slice(0, 0), q.slice(0, 0), false);
    }
    for precision in 1..=38 {
        for scale in [-128, -1, 0, precision as i8] {
            let q = Arc::new(
                Decimal128Array::from(vec![Some(0), None, Some(1)])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            check(binary(&[Some(payload()), None, Some(payload())]), q, true);
        }
    }
}
#[test]
fn pure_differential_percentile_approx_raw_all_payload_carriers_and_malformed_states() {
    for nullable in [false, true] {
        let rows = [
            Some(payload()),
            Some(vec![]),
            Some(b"bad".to_vec()),
            if nullable { None } else { Some(payload()) },
            Some(vec![0; 11]),
        ];
        let qs = Arc::new(Float64Array::from(vec![0.5; 5])) as ArrayRef;
        check(binary(&rows), qs.clone(), nullable);
        check(
            Arc::new(LargeBinaryArray::from_iter(
                rows.iter().map(|v| v.as_deref()),
            )),
            qs.clone(),
            nullable,
        );
        for p in [
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("bad"),
                Some("雪"),
                if nullable { None } else { Some("") },
                Some("long malformed"),
            ])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                Some(""),
                Some("bad"),
                Some("雪"),
                if nullable { None } else { Some("") },
                Some("long malformed"),
            ])),
        ] {
            check(p, qs.clone(), nullable);
        }
    }
}
#[test]
fn pure_differential_percentile_approx_raw_nulls_do_not_hide_unsupported_input_shapes() {
    let long = DataType::Struct(
        (0..80)
            .map(|i| Field::new(format!("original_nested_{i}"), DataType::Utf8, true))
            .collect(),
    );
    for ty in [
        DataType::Null,
        DataType::UInt32,
        DataType::Boolean,
        DataType::Decimal256(76, 2),
        DataType::List(Arc::new(Field::new("child", DataType::Utf8, true))),
        long,
    ] {
        let unsupported = new_null_array(&ty, 3);
        check(
            unsupported.clone(),
            Arc::new(Float64Array::from(vec![Some(0.5), None, Some(0.)])),
            true,
        );
        check(binary(&[None, None, None]), unsupported.clone(), true);
        check(
            unsupported.slice(0, 0),
            Arc::new(Float64Array::from(Vec::<f64>::new())),
            // Frozen NULL lift requires nullable source and target even when empty.
            ty == DataType::Null,
        );
    }
}
#[test]
fn pure_differential_percentile_approx_raw_exact_float_domain_and_hidden_payload_null() {
    let qs = Arc::new(Float64Array::from(vec![
        Some(-0.),
        Some(0.),
        Some(1.),
        Some(2.),
        Some(-1.),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
    ]));
    check(
        binary(&(0..9).map(|_| Some(payload())).collect::<Vec<_>>()),
        qs,
        true,
    );
    // Malformed bytes underneath a physical NULL are not decoded.
    let p = Arc::new(BinaryArray::new(
        arrow_buffer::OffsetBuffer::new(vec![0i32, 3, 3].into()),
        arrow_buffer::Buffer::from(b"bad".as_slice()),
        Some(arrow_buffer::NullBuffer::from(vec![false, true])),
    )) as ArrayRef;
    check(p, Arc::new(Float64Array::from(vec![0.5; 2])), true);
}
#[test]
fn pure_differential_percentile_approx_raw_actual_constant_ordinals_and_v3_payload() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for p in [None, Some(payload()), Some(vec![]), Some(b"bad".to_vec())] {
            let one = binary(&[p]);
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("percentile_approx_raw")
                    .constant_array(one)
                    .constant_array(Arc::new(Float64Array::from(vec![0.5])))
                    .constant_rows(321)
                    .legacy_constants(form)
                    .sparse_selections(7, 645031),
            );
        }
    }
    let metadata = br#"{"quantiles":null,"compression":10000}"#;
    let mut v3 = vec![0xa2, 3];
    v3.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    v3.extend_from_slice(metadata);
    check(
        binary(&[Some(v3), Some(payload()), None]),
        Arc::new(Float64Array::from(vec![Some(0.5), Some(1.), None])),
        true,
    );
}
