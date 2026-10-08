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

//! Original Decimal128 Arrow and Decimal256 Types Float64 value authors.
//! These are distinct original formulas. No overflow policy or context is read.
use arrow_array::{Decimal128Array, Float64Array, types::Float64Type};
use arrow_buffer::i256;
use num_traits::ToPrimitive;

/// Original Arrow 58.2.0 cast_decimal_to_float Float64 closure. Division at
/// negative scales is intentional; replacing it with multiplication changes bits.
pub fn decimal128_to_f64(value: i128, scale: i8) -> f64 {
    value as f64 / 10_f64.powi(scale as i32)
}

/// Original Arrow primitive unary also transforms hidden NULL backing payloads.
/// The selected owner consumes only demanded non-NULL values using the same author.
pub fn decimal128_array_to_f64(array: &Decimal128Array, scale: i8) -> Float64Array {
    array.unary::<_, Float64Type>(|value| decimal128_to_f64(value, scale))
}

pub fn decimal256_to_f64(value: i256, scale: i8) -> f64 {
    // Convert i256 to f64 using the same arithmetic approach as StarRocks BE:
    // (double)unscaled / (double)scale_factor.
    // This matches StarRocks's to_float() implementation in decimalv3.h which does:
    //   *to_value = static_cast<To>(static_cast<double>(value) / static_cast<double>(scale_factor));
    let unscaled_f64 = value.to_f64().unwrap_or(f64::NAN);
    if scale <= 0 {
        let factor = 10f64.powi((-scale) as i32);
        unscaled_f64 * factor
    } else {
        let factor = 10f64.powi(scale as i32);
        unscaled_f64 / factor
    }
}
