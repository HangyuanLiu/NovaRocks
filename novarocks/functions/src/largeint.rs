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
use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{ArrayRef, FixedSizeBinaryArray};
use std::sync::Arc;

pub use novarocks_type_contract::{LARGEINT_BYTE_WIDTH, is_largeint_data_type};

pub fn i128_to_be_bytes(value: i128) -> [u8; 16] {
    value.to_be_bytes()
}

pub fn i128_from_be_bytes(bytes: &[u8]) -> Result<i128, String> {
    if bytes.len() != LARGEINT_BYTE_WIDTH as usize {
        return Err(format!(
            "invalid LARGEINT byte length: expected {}, got {}",
            LARGEINT_BYTE_WIDTH,
            bytes.len()
        ));
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Ok(i128::from_be_bytes(buf))
}

#[derive(Clone, Copy, Debug)]
pub enum LargeIntObservation {
    Step,
    OpaqueBoundary,
}
pub fn array_from_i128(values: &[Option<i128>]) -> Result<ArrayRef, String> {
    match array_from_i128_observed(values, &mut |_| Ok::<(), std::convert::Infallible>(())) {
        Ok(result) => result,
        Err(never) => match never {},
    }
}
/// ONE original builder body; raw callers use an infallible no-op observer,
/// selected callers preserve their exact typed refusal cause.
pub fn array_from_i128_observed<E>(
    values: &[Option<i128>],
    observe: &mut dyn FnMut(LargeIntObservation) -> Result<(), E>,
) -> Result<Result<ArrayRef, String>, E> {
    observe(LargeIntObservation::OpaqueBoundary)?;
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), LARGEINT_BYTE_WIDTH);
    observe(LargeIntObservation::OpaqueBoundary)?;
    for value in values {
        observe(LargeIntObservation::Step)?;
        match value {
            Some(v) => {
                if let Err(error) = builder.append_value(i128_to_be_bytes(*v)) {
                    return Ok(Err(error.to_string()));
                }
            }
            None => builder.append_null(),
        }
    }
    observe(LargeIntObservation::OpaqueBoundary)?;
    let out = Arc::new(builder.finish()) as ArrayRef;
    observe(LargeIntObservation::OpaqueBoundary)?;
    Ok(Ok(out))
}

pub fn as_fixed_size_binary_array<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<&'a FixedSizeBinaryArray, String> {
    let arr = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| format!("{context}: expected FixedSizeBinaryArray"))?;
    if arr.value_length() != LARGEINT_BYTE_WIDTH {
        return Err(format!(
            "{context}: expected FixedSizeBinary({}), got FixedSizeBinary({})",
            LARGEINT_BYTE_WIDTH,
            arr.value_length()
        ));
    }
    Ok(arr)
}

pub fn value_at(arr: &FixedSizeBinaryArray, row: usize) -> Result<i128, String> {
    i128_from_be_bytes(arr.value(row))
}
