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
use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray};
use arrow_schema::DataType;
use md5::{Digest, Md5};
use std::{alloc::Layout, sync::Arc};
#[derive(Clone, Copy)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum CoreError {
    BytesRequired { arg_idx: usize },
    OutputCast(arrow_schema::ArrowError),
    LargeIntCarrier(String),
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
}

impl OwnedBytesArray {
    pub fn len(&self) -> usize {
        match self {
            Self::Utf8(arr) => arr.len(),
            Self::Binary(arr) => arr.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(arr) => arr.is_null(row),
            Self::Binary(arr) => arr.is_null(row),
        }
    }

    pub fn bytes(&self, row: usize) -> &[u8] {
        match self {
            Self::Utf8(arr) => arr.value(row).as_bytes(),
            Self::Binary(arr) => arr.value(row),
        }
    }

    pub fn utf8(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(arr) if !arr.is_null(row) => Some(arr.value(row)),
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
    // Iceberg-backed VARBINARY/VARCHAR columns are materialized in the "Large"
    // Arrow layout (LargeBinary / LargeUtf8). Normalize them to the small
    // layout the extractor understands via a zero-copy-ish arrow cast, then
    // re-enter. The casted array is Binary/Utf8 so this does not recurse again.
    if matches!(
        array.data_type(),
        DataType::LargeBinary | DataType::LargeUtf8
    ) {
        let target = if matches!(array.data_type(), DataType::LargeUtf8) {
            DataType::Utf8
        } else {
            DataType::Binary
        };
        observe(Observation::OpaqueBoundary)?;
        let casted = arrow_cast::cast(&array, &target);
        observe(Observation::OpaqueBoundary)?;
        if let Ok(casted) = casted {
            return to_owned_bytes_array_observed(casted, arg_idx, observe);
        }
    }
    // A typed-Null literal arrives as a NullArray; treat it as a VARCHAR
    // column whose every row is NULL so the per-row logic below collapses
    // the result to NULL rather than failing the static check.
    if matches!(array.data_type(), DataType::Null) {
        let len = array.len();
        let mut all_null = Vec::with_capacity(len);
        for _ in 0..len {
            observe(Observation::Step)?;
            all_null.push(None::<&str>);
        }
        observe(Observation::OpaqueBoundary)?;
        let array = StringArray::from(all_null);
        observe(Observation::OpaqueBoundary)?;
        return Ok(OwnedBytesArray::Utf8(array));
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
        Err(original @ CoreError::BytesRequired { .. }) => {
            observe(Observation::OpaqueBoundary)?;
            let casted = arrow_cast::cast(&array, &DataType::Utf8);
            observe(Observation::OpaqueBoundary)?;
            let casted = casted.map_err(|_| original)?;
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
    let result = arrow_cast::cast(&out, target).map_err(CoreError::OutputCast);
    observe(Observation::OpaqueBoundary)?;
    result
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Md5,
    Md5sum,
    Md5sumNumeric,
}
fn vector<T>(rows: usize, raw: bool, observe: Observer<'_>) -> Result<Vec<T>, CoreError> {
    if raw {
        return Ok(Vec::with_capacity(rows));
    }
    Layout::array::<T>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    observe(Observation::OpaqueBoundary)?;
    let mut out = Vec::new();
    out.try_reserve_exact(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
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
    raw: bool,
    observe: Observer<'_>,
) -> Result<ArrayRef, CoreError> {
    enum Rows {
        Hex(Vec<Option<String>>),
        Large(Vec<Option<i128>>),
        Narrow(Vec<Option<i64>>),
    }
    let large =
        op == Operation::Md5sumNumeric && target.is_some_and(largeint::is_largeint_data_type);
    let mut output = if op != Operation::Md5sumNumeric {
        Rows::Hex(vector(selection.len(), raw, observe)?)
    } else if large {
        Rows::Large(vector(selection.len(), raw, observe)?)
    } else {
        Rows::Narrow(vector(selection.len(), raw, observe)?)
    };
    for (ordinal, row) in selection.iter().enumerate() {
        observe(Observation::Step)?;
        if op == Operation::Md5 && inputs[0].is_null(row_for(0, ordinal, row)) {
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
        })?
        .map_err(CoreError::LargeIntCarrier)?,
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
/// Diagnostic labels live in error projection, not in admission or digest calculation.
pub(crate) mod compatibility {
    use super::{CoreError, Operation};
    pub(crate) fn diagnostic_label(op: Operation) -> &'static str {
        match op {
            Operation::Md5 => "md5",
            Operation::Md5sum => "md5sum",
            Operation::Md5sumNumeric => "md5sum_numeric",
        }
    }
    pub(crate) fn error_text(error: CoreError, label: &str) -> String {
        match error {
            CoreError::BytesRequired { arg_idx } => {
                format!("{}: arg{} must be VARCHAR or VARBINARY", label, arg_idx)
            }
            CoreError::OutputCast(cause) => format!("{}: failed to cast output: {}", label, cause),
            CoreError::LargeIntCarrier(message) => message,
            CoreError::Kernel(failure) => failure.to_string(),
        }
    }
    pub(super) fn legacy<T>(result: Result<T, CoreError>, label: &str) -> Result<T, String> {
        result.map_err(|error| error_text(error, label))
    }
}
/// Original v1/AES API; labels format errors only.
pub fn to_owned_bytes_array(
    array: ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<OwnedBytesArray, String> {
    compatibility::legacy(
        to_owned_bytes_array_observed(array, arg_idx, &mut |_| Ok(())),
        fn_name,
    )
}
pub fn to_owned_bytes_array_with_varchar_cast(
    array: ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<OwnedBytesArray, String> {
    compatibility::legacy(
        to_owned_bytes_array_with_varchar_cast_observed(array, arg_idx, &mut |_| Ok(())),
        fn_name,
    )
}
pub fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    compatibility::legacy(
        cast_output_observed(out, output_type, &mut |_| Ok(())),
        fn_name,
    )
}
pub fn evaluate_legacy(
    op: Operation,
    inputs: &[OwnedBytesArray],
    rows: usize,
    target: Option<&DataType>,
) -> Result<ArrayRef, String> {
    compatibility::legacy(
        evaluate_observed(
            op,
            inputs,
            Selection::all(rows),
            |_, _, row| row,
            target,
            true,
            &mut |_| Ok(()),
        ),
        compatibility::diagnostic_label(op),
    )
}
#[cfg(test)]
#[path = "md5_shared_tests.rs"]
mod tests;
