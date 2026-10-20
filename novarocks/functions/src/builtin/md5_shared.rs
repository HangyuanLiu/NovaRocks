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
//! ONE original byte admission, MD5 stream and result assembly; no name dispatch.
use crate::{
    KernelFailure, Selection,
    kernel_input::EvaluationCheckpoints,
    largeint::{self, LargeIntObservation},
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
};
use arrow_schema::DataType;
use md5::{Digest, Md5};
use std::sync::Arc;
#[derive(Clone, Copy)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum CoreError {
    BytesRequired {
        arg_idx: usize,
    },
    InputCast {
        arg_idx: usize,
        source: arrow_schema::ArrowError,
    },
    OutputCast(arrow_schema::ArrowError),
    Contract(novarocks_failure_contract::BoundedDetail),
    Kernel(KernelFailure),
}
impl From<KernelFailure> for CoreError {
    fn from(failure: KernelFailure) -> Self {
        Self::Kernel(failure)
    }
}
type Observer<'a> = &'a mut dyn FnMut(Observation) -> Result<(), KernelFailure>;
#[derive(Clone)]
pub enum OwnedBytesArray {
    Utf8(StringArray),
    Binary(BinaryArray),
    LargeUtf8(LargeStringArray),
    LargeBinary(LargeBinaryArray),
    Null { len: usize },
}

impl OwnedBytesArray {
    pub fn len(&self) -> usize {
        match self {
            Self::Utf8(arr) => arr.len(),
            Self::Binary(arr) => arr.len(),
            Self::LargeUtf8(arr) => arr.len(),
            Self::LargeBinary(arr) => arr.len(),
            Self::Null { len } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(arr) => arr.is_null(row),
            Self::Binary(arr) => arr.is_null(row),
            Self::LargeUtf8(arr) => arr.is_null(row),
            Self::LargeBinary(arr) => arr.is_null(row),
            Self::Null { .. } => true,
        }
    }

    pub fn bytes(&self, row: usize) -> &[u8] {
        match self {
            Self::Utf8(arr) => arr.value(row).as_bytes(),
            Self::Binary(arr) => arr.value(row),
            Self::LargeUtf8(arr) => arr.value(row).as_bytes(),
            Self::LargeBinary(arr) => arr.value(row),
            Self::Null { .. } => &[],
        }
    }

    pub fn utf8(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(arr) if !arr.is_null(row) => Some(arr.value(row)),
            Self::LargeUtf8(arr) if !arr.is_null(row) => Some(arr.value(row)),
            _ => None,
        }
    }
}

pub fn to_owned_bytes_array_observed(
    array: ArrayRef,
    arg_idx: usize,
    observe: Observer<'_>,
) -> Result<OwnedBytesArray, CoreError> {
    observe(Observation::Step)?;
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(OwnedBytesArray::Utf8(arr.clone()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(OwnedBytesArray::Binary(arr.clone()));
    }
    // Digest readers borrow bytes directly in either offset width. No small
    // Arrow layout is required to calculate a hash or inspect an AES input.
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(OwnedBytesArray::LargeBinary(array.clone()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(OwnedBytesArray::LargeUtf8(array.clone()));
    }
    if matches!(array.data_type(), DataType::Null) {
        return Ok(OwnedBytesArray::Null { len: array.len() });
    }
    Err(CoreError::BytesRequired { arg_idx })
}

pub fn to_owned_bytes_array_with_varchar_cast_observed(
    array: ArrayRef,
    arg_idx: usize,
    observe: Observer<'_>,
) -> Result<OwnedBytesArray, CoreError> {
    match to_owned_bytes_array_observed(array.clone(), arg_idx, observe) {
        Ok(bytes) => Ok(bytes),
        Err(CoreError::BytesRequired { .. }) => {
            observe(Observation::OpaqueBoundary)?;
            let casted = arrow_cast::cast(&array, &DataType::Utf8)
                .map_err(|source| CoreError::InputCast { arg_idx, source })?;
            observe(Observation::OpaqueBoundary)?;
            to_owned_bytes_array_observed(casted, arg_idx, observe)
        }
        Err(other) => Err(other),
    }
}

pub fn cast_output_observed(
    out: ArrayRef,
    output_type: Option<&DataType>,
    observe: Observer<'_>,
) -> Result<ArrayRef, CoreError> {
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    observe(Observation::OpaqueBoundary)?;
    let result = arrow_cast::cast(&out, target).map_err(CoreError::OutputCast)?;
    observe(Observation::OpaqueBoundary)?;
    Ok(result)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Md5,
    Md5sum,
    Md5sumNumeric,
}
fn vector<T>(rows: usize, observe: Observer<'_>) -> Result<Vec<T>, CoreError> {
    super::string_failure::array_layout::<T>(rows)?;
    observe(Observation::OpaqueBoundary)?;
    let mut out = Vec::new();
    out.try_reserve_exact(rows)
        .map_err(|source| super::string_failure::reservation::<T>(rows, source))?;
    observe(Observation::OpaqueBoundary)?;
    Ok(out)
}
/// The caller supplies evaluated, normalized columns and an exact address projection.
pub fn evaluate_observed(
    op: Operation,
    inputs: &[OwnedBytesArray],
    selection: Selection<'_>,
    mut row_for: impl FnMut(usize, usize, usize) -> usize,
    target: Option<&DataType>,
    observe: Observer<'_>,
) -> Result<ArrayRef, CoreError> {
    if op == Operation::Md5 && inputs.len() != 1 {
        return Err(CoreError::Contract(
            novarocks_failure_contract::BoundedDetail::truncating(
                "MD5 requires exactly one byte input",
            ),
        ));
    }
    enum Rows {
        Hex(Vec<Option<String>>),
        Large(Vec<Option<i128>>),
        Narrow(Vec<Option<i64>>),
    }
    let large =
        op == Operation::Md5sumNumeric && target.is_some_and(largeint::is_largeint_data_type);
    let mut output = if op != Operation::Md5sumNumeric {
        Rows::Hex(vector(selection.len(), observe)?)
    } else if large {
        Rows::Large(vector(selection.len(), observe)?)
    } else {
        Rows::Narrow(vector(selection.len(), observe)?)
    };
    let mut output_offset = 0i32;
    for (ordinal, row) in selection.iter().enumerate() {
        observe(Observation::Step)?;
        let first_is_null = if op == Operation::Md5 {
            let source_row = row_for(0, ordinal, row);
            if source_row >= inputs[0].len() {
                return Err(address_failure(0, source_row));
            }
            inputs[0].is_null(source_row)
        } else {
            false
        };
        if first_is_null {
            if let Rows::Hex(out) = &mut output {
                out.push(None);
            }
            continue;
        }
        observe(Observation::OpaqueBoundary)?;
        let mut hasher = Md5::new();
        observe(Observation::OpaqueBoundary)?;
        for (idx, input) in inputs.iter().enumerate() {
            observe(Observation::Step)?;
            let source_row = row_for(idx, ordinal, row);
            if source_row >= input.len() {
                return Err(address_failure(idx, source_row));
            }
            if input.is_null(source_row) {
                continue;
            }
            let bytes = input.bytes(source_row);
            for _ in bytes {
                observe(Observation::Step)?;
            }
            observe(Observation::OpaqueBoundary)?;
            hasher.update(bytes);
            observe(Observation::OpaqueBoundary)?;
        }
        observe(Observation::OpaqueBoundary)?;
        let digest = hasher.finalize();
        observe(Observation::OpaqueBoundary)?;
        match &mut output {
            Rows::Hex(out) => {
                output_offset =
                    crate::aggregate_scalar::checked_utf8_output_offset(output_offset, 32)?;
                observe(Observation::OpaqueBoundary)?;
                let text = hex::encode(digest);
                observe(Observation::OpaqueBoundary)?;
                for _ in 0..32 {
                    observe(Observation::Step)?;
                }
                out.push(Some(text));
            }
            Rows::Large(out) => {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&digest[..16]);
                out.push(Some(i128::from_be_bytes(bytes)));
            }
            Rows::Narrow(out) => {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&digest[..16]);
                out.push(Some(u128::from_be_bytes(bytes) as i64));
            }
        }
    }
    observe(Observation::OpaqueBoundary)?;
    let out = match output {
        Rows::Hex(out) => Arc::new(StringArray::from(out)) as ArrayRef,
        Rows::Large(out) => largeint::array_from_i128_observed(&out, &mut |event| {
            observe(match event {
                LargeIntObservation::Step => Observation::Step,
                LargeIntObservation::OpaqueBoundary => Observation::OpaqueBoundary,
            })
        })?,
        Rows::Narrow(out) => Arc::new(Int64Array::from(out)) as ArrayRef,
    };
    observe(Observation::OpaqueBoundary)?;
    if op == Operation::Md5sumNumeric && !large {
        cast_output_observed(out, target, observe)
    } else {
        Ok(out)
    }
}
pub(crate) fn observe(
    work: &mut EvaluationCheckpoints<'_>,
    event: Observation,
) -> Result<(), KernelFailure> {
    match event {
        Observation::Step => work.step(),
        Observation::OpaqueBoundary => work.flush(),
    }
}
fn address_failure(arg_idx: usize, row: usize) -> CoreError {
    CoreError::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
            "digest arg{arg_idx} row {row} is outside its byte carrier"
        )),
    )
}
pub(crate) fn diagnostic_label(op: Operation) -> &'static str {
    match op {
        Operation::Md5 => "md5",
        Operation::Md5sum => "md5sum",
        Operation::Md5sumNumeric => "md5sum_numeric",
    }
}
impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BytesRequired { arg_idx } => {
                write!(f, "arg{arg_idx} must be VARCHAR or VARBINARY")
            }
            Self::InputCast { arg_idx, source } => {
                write!(f, "failed to cast arg{arg_idx} to VARCHAR: {source}")
            }
            Self::OutputCast(source) => write!(f, "failed to cast output: {source}"),
            Self::Contract(detail) => detail.fmt(f),
            Self::Kernel(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for CoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InputCast { source, .. } | Self::OutputCast(source) => Some(source),
            Self::Kernel(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for CoreError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let fact = |class, code| {
            FailureFact::new(
                class,
                FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
            )
        };
        match self {
            Self::InputCast {
                source: arrow_schema::ArrowError::MemoryError(_),
                ..
            }
            | Self::OutputCast(arrow_schema::ArrowError::MemoryError(_)) => {
                fact(FailureClass::Resource, 371)
            }
            Self::InputCast { .. } | Self::OutputCast(_) => fact(FailureClass::Invariant, 370),
            Self::Kernel(source) => source.failure_fact(),
            _ => fact(FailureClass::Invariant, 369),
        }
    }
}
#[derive(Debug)]
struct RequiredDigestFailure {
    label: novarocks_failure_contract::BoundedDetail,
    cause: CoreError,
}
impl std::fmt::Display for RequiredDigestFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.label, self.cause)
    }
}
impl std::error::Error for RequiredDigestFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
impl novarocks_failure_contract::FailureProjection for RequiredDigestFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.cause)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for RequiredDigestFailure {}
pub(crate) fn into_kernel(cause: CoreError, label: &str) -> KernelFailure {
    match cause {
        CoreError::Kernel(source) => source,
        cause => KernelFailure::from_domain(RequiredDigestFailure {
            label: novarocks_failure_contract::BoundedDetail::truncating(label),
            cause,
        }),
    }
}
/// Required byte consumers preserve the original domain cause and bounded label.
pub fn to_owned_bytes_array(
    array: ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<OwnedBytesArray, KernelFailure> {
    to_owned_bytes_array_observed(array, arg_idx, &mut |_| Ok(()))
        .map_err(|source| into_kernel(source, fn_name))
}
pub fn to_owned_bytes_array_with_varchar_cast(
    array: ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<OwnedBytesArray, KernelFailure> {
    to_owned_bytes_array_with_varchar_cast_observed(array, arg_idx, &mut |_| Ok(()))
        .map_err(|source| into_kernel(source, fn_name))
}
pub fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, KernelFailure> {
    cast_output_observed(out, output_type, &mut |_| Ok(()))
        .map_err(|source| into_kernel(source, fn_name))
}
pub fn evaluate_required(
    op: Operation,
    inputs: &[OwnedBytesArray],
    rows: usize,
    target: Option<&DataType>,
) -> Result<ArrayRef, KernelFailure> {
    evaluate_observed(
        op,
        inputs,
        Selection::all(rows),
        |_, _, row| row,
        target,
        &mut |_| Ok(()),
    )
    .map_err(|source| into_kernel(source, diagnostic_label(op)))
}
#[cfg(test)]
#[path = "md5_shared_tests.rs"]
mod tests;
