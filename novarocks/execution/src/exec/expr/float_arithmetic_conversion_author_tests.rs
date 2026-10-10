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

//! Exact original Arrow cast versus already-installed selected Cast authors.
use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array,
    },
    datatypes::DataType,
};
use novarocks_functions::*;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric conversion never waits")
    }
}
fn assert_original_conversion(array: ArrayRef) {
    let expected = arrow::compute::cast(&array, &DataType::Float64).unwrap();
    let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let source = FunctionValueType::new(array.data_type().clone(), true);
            let target = FunctionValueType::new(DataType::Float64, true);
            let recipe = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &target,
                policy,
                allow,
                &Control,
            )
            .unwrap();
            for row in 0..array.len() {
                let actual = recipe
                    .evaluate_row(EvaluatedArgument::Column(&array), row, row, &Control)
                    .unwrap();
                if expected.is_null(row) {
                    assert_eq!(actual, CastRowResult::Null);
                } else {
                    let CastRowResult::Float64(value) = actual else {
                        panic!("exact F64 result required: {actual:?}")
                    };
                    assert_eq!(
                        value.to_bits(),
                        expected.value(row).to_bits(),
                        "{:?} row {row}",
                        array.data_type()
                    );
                }
            }
        }
    }
}
#[test]
fn float_arithmetic_conversion_original_arrow_signed_and_ieee_widths() {
    let signed = [
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
        None,
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(
            signed
                .iter()
                .map(|x| x.map(|x| x as i8))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int16Array::from(
            signed
                .iter()
                .map(|x| x.map(|x| x as i16))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            signed
                .iter()
                .map(|x| x.map(|x| x as i32))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(signed.to_vec())),
        Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(0x7f800001)),
            Some(f32::from_bits(0xffc12345)),
            Some(f32::MAX),
            Some(f32::MIN_POSITIVE),
            Some(f32::from_bits(1)),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(0x7ff0000000000001)),
            Some(f64::from_bits(0xfff8123456789abc)),
            Some(f64::MAX),
            Some(f64::MIN_POSITIVE),
            Some(f64::from_bits(1)),
            None,
        ])),
    ];
    for array in arrays {
        assert_original_conversion(array.clone());
        assert_original_conversion(array.slice(1, array.len() - 2));
        assert_original_conversion(array.slice(0, 0));
    }
}
#[test]
fn float_arithmetic_conversion_original_arrow_all_decimal128_metadata_and_raw_extremes() {
    // Raw payload precision is not silently validated or narrowed by arithmetic's original Arrow cast.
    let values = vec![
        Some(i128::MIN),
        Some(i128::MAX),
        Some(-1),
        Some(0),
        Some(1),
        None,
    ];
    for precision in 1..=38u8 {
        for scale in i8::MIN..=precision as i8 {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(values.clone())
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            assert_original_conversion(array);
        }
    }
}
