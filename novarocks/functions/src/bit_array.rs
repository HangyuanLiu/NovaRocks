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
//! Original evaluated-array bit conversion and shift loop shared by both ABIs.
//! Runtime computation takes an explicit operation; diagnostic labels stay outside.
use crate::{Selection, largeint};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int64Array, UInt64Array,
};
use arrow_cast::cast;
use arrow_schema::DataType;
use std::sync::Arc;

#[derive(Debug)]
pub enum BitArrayError {
    CastArgument {
        index: usize,
        cause: String,
    },
    ArgumentCarrier {
        index: usize,
        expected: &'static str,
    },
    CastOutput(String),
    Raw(String),
}
impl BitArrayError {
    /// Compatibility projection only: the label never selects computation.
    pub fn legacy_message(self, label: &str) -> String {
        match self {
            Self::CastArgument { index, cause } => {
                format!("{label}: failed to cast arg{index} to BIGINT: {cause}")
            }
            Self::ArgumentCarrier { index, expected } => {
                format!("{label}: arg{index} is not {expected}")
            }
            Self::CastOutput(cause) => format!("{label}: failed to cast output: {cause}"),
            Self::Raw(cause) => cause,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum BitArrayObservation {
    Step,
    OpaqueBoundary,
}

pub fn to_i64_array(array: &ArrayRef, arg_idx: usize) -> Result<Int64Array, BitArrayError> {
    let casted = cast(array, &DataType::Int64).map_err(|e| BitArrayError::CastArgument {
        index: arg_idx,
        cause: e.to_string(),
    })?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or(BitArrayError::ArgumentCarrier {
            index: arg_idx,
            expected: "BIGINT",
        })
}

pub fn to_i128_values(
    array: &ArrayRef,
    arg_idx: usize,
) -> Result<Vec<Option<i128>>, BitArrayError> {
    match array.data_type() {
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or(BitArrayError::ArgumentCarrier {
                    index: arg_idx,
                    expected: "LARGEINT",
                })?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(
                        largeint::i128_from_be_bytes(arr.value(row)).map_err(BitArrayError::Raw)?,
                    ));
                }
            }
            Ok(out)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().ok_or(
                BitArrayError::ArgumentCarrier {
                    index: arg_idx,
                    expected: "UINT64",
                },
            )?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(arr.value(row) as i128));
                }
            }
            Ok(out)
        }
        DataType::Decimal128(_, scale) if *scale == 0 => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().ok_or(
                BitArrayError::ArgumentCarrier {
                    index: arg_idx,
                    expected: "DECIMAL128",
                },
            )?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(arr.value(row)));
                }
            }
            Ok(out)
        }
        DataType::Null => Ok(vec![None; array.len()]),
        _ => {
            let casted = to_i64_array(array, arg_idx)?;
            let mut out = Vec::with_capacity(casted.len());
            for row in 0..casted.len() {
                if casted.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(casted.value(row) as i128));
                }
            }
            Ok(out)
        }
    }
}

/// Original NULL-propagating evaluated-value loop for unary and binary bitwise calls.
/// The read adapter retains the caller's exact row domain and carrier validation.
pub fn map_values_observed<I, T, E>(
    selection: Selection<'_>,
    mut read_value: impl FnMut(usize, usize) -> Result<Option<I>, E>,
    func: impl Fn(I) -> T,
    observe: &mut dyn FnMut(BitArrayObservation) -> Result<(), E>,
) -> Result<Vec<Option<T>>, E> {
    observe(BitArrayObservation::OpaqueBoundary)?;
    let mut out = Vec::with_capacity(selection.len());
    observe(BitArrayObservation::OpaqueBoundary)?;
    for (ordinal, row) in selection.iter().enumerate() {
        observe(BitArrayObservation::Step)?;
        out.push(read_value(ordinal, row)?.map(&func));
    }
    Ok(out)
}

mod count_domain {
    pub trait Sealed {}
    impl Sealed for i64 {}
    impl Sealed for i128 {}
}
/// The original BIGINT and LARGEINT paths normalize counts independently.
/// In particular the raw LARGEINT count need not fit i64 before u32 projection.
pub trait ShiftCount: count_domain::Sealed {
    fn wrapping_u32(self) -> u32;
}
impl ShiftCount for i64 {
    fn wrapping_u32(self) -> u32 {
        self as u32
    }
}
impl ShiftCount for i128 {
    fn wrapping_u32(self) -> u32 {
        self as u32
    }
}

/// Original nullable shift loop. Readers own exact selected mappings/NULL
/// validation or legacy array indexing; the u32 projection is authored here.
pub fn shift_values_observed<T, C: ShiftCount, E>(
    selection: Selection<'_>,
    mut read_pair: impl FnMut(usize, usize) -> Result<Option<(T, C)>, E>,
    func: impl Fn(T, u32) -> T,
    observe: &mut dyn FnMut(BitArrayObservation) -> Result<(), E>,
) -> Result<Vec<Option<T>>, E> {
    observe(BitArrayObservation::OpaqueBoundary)?;
    let mut out = Vec::with_capacity(selection.len());
    observe(BitArrayObservation::OpaqueBoundary)?;
    for (ordinal, row) in selection.iter().enumerate() {
        observe(BitArrayObservation::Step)?;
        out.push(match read_pair(ordinal, row)? {
            Some((value, count)) => Some(func(value, count.wrapping_u32())),
            None => None,
        });
    }
    Ok(out)
}

pub fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
) -> Result<ArrayRef, BitArrayError> {
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    cast(&out, target).map_err(|e| BitArrayError::CastOutput(e.to_string()))
}
pub fn cast_output_observed<E>(
    out: ArrayRef,
    output_type: Option<&DataType>,
    observe: &mut dyn FnMut(BitArrayObservation) -> Result<(), E>,
) -> Result<Result<ArrayRef, BitArrayError>, E> {
    observe(BitArrayObservation::OpaqueBoundary)?;
    let result = cast_output(out, output_type);
    observe(BitArrayObservation::OpaqueBoundary)?;
    Ok(result)
}
pub fn finish_i64_observed<E>(
    values: Vec<Option<i64>>,
    output_type: Option<&DataType>,
    observe: &mut dyn FnMut(BitArrayObservation) -> Result<(), E>,
) -> Result<Result<ArrayRef, BitArrayError>, E> {
    observe(BitArrayObservation::OpaqueBoundary)?;
    let out = Arc::new(Int64Array::from(values)) as ArrayRef;
    observe(BitArrayObservation::OpaqueBoundary)?;
    cast_output_observed(out, output_type, observe)
}

pub fn cast_largeint_output(
    values: &[Option<i128>],
    output_type: Option<&DataType>,
) -> Result<ArrayRef, BitArrayError> {
    match cast_largeint_output_observed(values, output_type, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    }) {
        Ok(result) => result,
        Err(never) => match never {},
    }
}
pub fn cast_largeint_output_observed<E>(
    values: &[Option<i128>],
    output_type: Option<&DataType>,
    observe: &mut dyn FnMut(BitArrayObservation) -> Result<(), E>,
) -> Result<Result<ArrayRef, BitArrayError>, E> {
    match output_type {
        None => build_largeint(values, observe),
        Some(t) if largeint::is_largeint_data_type(t) => build_largeint(values, observe),
        Some(t) => {
            // Original unchecked i128 -> i64 projection; no numeric cast policy
            // is substituted for this legacy physical output path.
            observe(BitArrayObservation::OpaqueBoundary)?;
            let mut out_i64 = Vec::with_capacity(values.len());
            for value in values {
                observe(BitArrayObservation::Step)?;
                out_i64.push(value.map(|x| x as i64));
            }
            finish_i64_observed(out_i64, Some(t), observe)
        }
    }
}
fn build_largeint<E>(
    values: &[Option<i128>],
    observe: &mut dyn FnMut(BitArrayObservation) -> Result<(), E>,
) -> Result<Result<ArrayRef, BitArrayError>, E> {
    crate::largeint::array_from_i128_observed(values, &mut |event| {
        observe(match event {
            crate::largeint::LargeIntObservation::Step => BitArrayObservation::Step,
            crate::largeint::LargeIntObservation::OpaqueBoundary => {
                BitArrayObservation::OpaqueBoundary
            }
        })
    })
    .map(|result| result.map_err(BitArrayError::Raw))
}
