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
//! Original scalar HLL_HASH carrier reader and array projection. No expression/name/environment access.
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_array::builder::BinaryBuilder;
use arrow_schema::DataType;
use std::{fmt, sync::Arc};
#[derive(Clone, Copy)]
pub struct UnsupportedCarrier<'a>(pub &'a DataType);
impl fmt::Display for UnsupportedCarrier<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hll_hash expects scalar input, got {:?}", self.0)
    }
}
/// One original NULL/hash/encoder leaf. Source owners project their original
/// scalar bytes before this call; this function chooses no type/coercion policy.
pub fn encode_scalar_bytes_observed<E>(
    bytes: Option<&[u8]>,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<Vec<u8>, E> {
    use crate::hll::{MURMUR_SEED, encode_hll_empty, encode_hll_single, murmur_hash64a_observed};
    let Some(bytes) = bytes else {
        return Ok(encode_hll_empty());
    };
    let hash = murmur_hash64a_observed(bytes, MURMUR_SEED, observe)?;
    Ok(encode_hll_single(hash))
}
pub fn encode_scalar_bytes(bytes: Option<&[u8]>) -> Vec<u8> {
    match encode_scalar_bytes_observed::<std::convert::Infallible>(bytes, &mut || Ok(())) {
        Ok(bytes) => bytes,
        Err(impossible) => match impossible {},
    }
}
pub enum Input<'a> {
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Date32(&'a Date32Array),
    TimestampSecond(&'a TimestampSecondArray),
    TimestampMillisecond(&'a TimestampMillisecondArray),
    TimestampMicrosecond(&'a TimestampMicrosecondArray),
    TimestampNanosecond(&'a TimestampNanosecondArray),
    Decimal128(&'a Decimal128Array),
    FixedSizeBinary(&'a FixedSizeBinaryArray),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
}
impl<'a> Input<'a> {
    /// Exact original downcast order, including admission before NULL/empty traversal.
    pub fn try_new(input: &'a ArrayRef) -> Result<Self, UnsupportedCarrier<'a>> {
        if let Some(a) = input.as_any().downcast_ref::<BooleanArray>() {
            return Ok(Self::Boolean(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<Int8Array>() {
            return Ok(Self::Int8(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<Int16Array>() {
            return Ok(Self::Int16(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<Int32Array>() {
            return Ok(Self::Int32(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
            return Ok(Self::Int64(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<Float32Array>() {
            return Ok(Self::Float32(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
            return Ok(Self::Float64(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<Date32Array>() {
            return Ok(Self::Date32(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<TimestampSecondArray>() {
            return Ok(Self::TimestampSecond(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<TimestampMillisecondArray>() {
            return Ok(Self::TimestampMillisecond(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            return Ok(Self::TimestampMicrosecond(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Ok(Self::TimestampNanosecond(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<Decimal128Array>() {
            return Ok(Self::Decimal128(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            return Ok(Self::FixedSizeBinary(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Utf8(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Self::LargeUtf8(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<BinaryArray>() {
            return Ok(Self::Binary(a));
        }
        if let Some(a) = input.as_any().downcast_ref::<LargeBinaryArray>() {
            return Ok(Self::LargeBinary(a));
        }
        Err(UnsupportedCarrier(input.data_type()))
    }
    pub fn row_observed<E>(
        &self,
        row: usize,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Vec<u8>, E> {
        macro_rules! hash {
            ($a:expr,$bytes:expr) => {{
                if $a.is_null(row) {
                    return encode_scalar_bytes_observed(None, observe);
                }
                encode_scalar_bytes_observed(Some($bytes.as_ref()), observe)
            }};
        }
        match self {
            Self::Boolean(a) => hash!(a, [if a.value(row) { 1u8 } else { 0u8 }]),
            Self::Int8(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::Int16(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::Int32(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::Int64(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::Float32(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::Float64(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::Date32(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::TimestampSecond(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::TimestampMillisecond(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::TimestampMicrosecond(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::TimestampNanosecond(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::Decimal128(a) => hash!(a, a.value(row).to_le_bytes()),
            Self::FixedSizeBinary(a) => hash!(a, a.value(row)),
            Self::Utf8(a) => hash!(a, a.value(row).as_bytes()),
            Self::LargeUtf8(a) => hash!(a, a.value(row).as_bytes()),
            Self::Binary(a) => hash!(a, a.value(row)),
            Self::LargeBinary(a) => hash!(a, a.value(row)),
        }
    }
    pub fn row(&self, row: usize) -> Vec<u8> {
        match self.row_observed::<std::convert::Infallible>(row, &mut || Ok(())) {
            Ok(value) => value,
            Err(impossible) => match impossible {},
        }
    }
}
pub fn array(input: &ArrayRef) -> Result<ArrayRef, String> {
    // Preserve original output builder creation before the type admission.
    let mut builder = BinaryBuilder::new();
    let reader = Input::try_new(input).map_err(|failure| failure.to_string())?;
    for row in 0..input.len() {
        builder.append_value(reader.row(row));
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BinaryArray, Float32Array, Float64Array, NullArray};
    fn payloads(a: ArrayRef) -> Vec<Vec<u8>> {
        a.as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_vec())
            .collect()
    }
    #[test]
    fn hll_hash_original_core_ieee_bits_and_typed_null() {
        for a in [
            Arc::new(Float32Array::from(vec![
                Some(0.),
                Some(-0.),
                Some(f32::from_bits(0x7fc00123)),
                None,
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(0.),
                Some(-0.),
                Some(f64::from_bits(0x7ff8000000000123)),
                None,
            ])),
        ] {
            let out = payloads(array(&a).unwrap());
            assert_ne!(out[0], out[1]);
            assert_eq!(out[3], vec![0]);
        }
    }
    #[test]
    fn hll_hash_original_core_admission_before_empty() {
        for n in [0, 1] {
            let a = Arc::new(NullArray::new(n)) as ArrayRef;
            assert_eq!(
                array(&a).unwrap_err(),
                "hll_hash expects scalar input, got Null"
            );
        }
    }
    #[test]
    fn hll_hash_original_core_observed_murmur_is_same_result() {
        let a = Arc::new(StringArray::from(vec!["雪".repeat(300)])) as ArrayRef;
        let input = Input::try_new(&a).unwrap_or_else(|_| panic!("original string carrier"));
        let mut calls = 0;
        let seen = input
            .row_observed::<()>(0, &mut || {
                calls += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, input.row(0));
        assert!(calls > 100);
    }
}
