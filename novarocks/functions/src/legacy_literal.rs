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
//! Original v1 literal value and builder, and the sole native NEGATE zero author.
use arrow_buffer::i256;
#[derive(Clone, Debug)]
pub enum LegacyLiteralValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Utf8(String),
    Binary(Vec<u8>),
    Date32(i32),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        value: i256,
        precision: u8,
        scale: i8,
    },
}

use crate::largeint;
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, NullArray,
    StringArray,
};
use std::sync::Arc;

pub fn eval(value: &LegacyLiteralValue, len: usize) -> Result<ArrayRef, String> {
    match value {
        LegacyLiteralValue::Null => Ok(Arc::new(NullArray::new(len))),
        LegacyLiteralValue::Int8(v) => {
            let arr = Int8Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Int16(v) => {
            let arr = Int16Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Int32(v) => {
            let arr = Int32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Bool(v) => {
            let arr = BooleanArray::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Int64(v) => {
            let arr = Int64Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::LargeInt(v) => {
            let values = vec![Some(*v); len];
            largeint::array_from_i128(&values)
        }
        LegacyLiteralValue::Float32(v) => {
            let arr = Float32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Float64(v) => {
            let arr = Float64Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Utf8(v) => {
            let arr = StringArray::from(vec![v.as_str(); len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Binary(v) => {
            let values = std::iter::repeat_n(v.as_slice(), len).collect::<Vec<&[u8]>>();
            let arr = BinaryArray::from_vec(values);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Date32(v) => {
            let arr = Date32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let arr = Decimal128Array::from(vec![*value; len])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(arr))
        }
        LegacyLiteralValue::Decimal256 {
            value,
            precision,
            scale,
        } => {
            let arr = Decimal256Array::from(vec![*value; len])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(arr))
        }
    }
}

pub fn native_negate_zero(
    data_type: &arrow_schema::DataType,
) -> Result<LegacyLiteralValue, String> {
    use arrow_schema::DataType;
    let literal = match data_type {
        DataType::Int8 => LegacyLiteralValue::Int8(0),
        DataType::Int16 => LegacyLiteralValue::Int16(0),
        DataType::Int32 => LegacyLiteralValue::Int32(0),
        DataType::Int64 => LegacyLiteralValue::Int64(0),
        DataType::Float32 => LegacyLiteralValue::Float32(0.0),
        DataType::Float64 => LegacyLiteralValue::Float64(0.0),
        DataType::Decimal128(precision, scale) => LegacyLiteralValue::Decimal128 {
            value: 0,
            precision: *precision,
            scale: *scale,
        },
        DataType::Decimal256(precision, scale) => LegacyLiteralValue::Decimal256 {
            value: i256::ZERO,
            precision: *precision,
            scale: *scale,
        },
        dt if crate::largeint::is_largeint_data_type(dt) => LegacyLiteralValue::LargeInt(0),
        _ => {
            return Err(format!(
                "NEGATE is not supported for data type {data_type:?}"
            ));
        }
    };
    Ok(literal)
}
