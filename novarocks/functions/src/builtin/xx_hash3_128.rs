// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//! Original seed-zero XXH3 stream and byte-layout normalization shared by both callers.
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    largeint::{self, LargeIntObservation},
};
use arrow_array::{Array, ArrayRef, BinaryArray, StringArray, UInt64Array};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, cell::RefCell};
use twox_hash::xxhash3_128::Hasher;
pub enum BytesArray {
    Utf8(StringArray),
    Binary(BinaryArray),
}
impl BytesArray {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(arr) => arr.is_null(row),
            Self::Binary(arr) => arr.is_null(row),
        }
    }
    fn bytes(&self, row: usize) -> &[u8] {
        match self {
            Self::Utf8(arr) => arr.value(row).as_bytes(),
            Self::Binary(arr) => arr.value(row),
        }
    }
}
type ErrorBoundary<'a> = Option<&'a dyn Fn(&str) -> Result<(), KernelFailure>>;
fn raw_error(message: String, boundary: ErrorBoundary<'_>) -> KernelFailure {
    if let Some(boundary) = boundary {
        if let Err(failure) = boundary(&message) {
            return failure;
        }
    }
    internal(&message)
}
/// The original normalization: Large offsets are converted by Arrow, and a
/// failed cast retains the original generic admission error rather than its cause.
fn normalize_bytes(
    array: ArrayRef,
    arg_idx: usize,
    work: &mut EvaluationCheckpoints<'_>,
    boundary: ErrorBoundary<'_>,
) -> Result<BytesArray, KernelFailure> {
    work.step()?;
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(BytesArray::Utf8(arr.clone()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(BytesArray::Binary(arr.clone()));
    }
    if matches!(
        array.data_type(),
        DataType::LargeBinary | DataType::LargeUtf8
    ) {
        let target = if matches!(array.data_type(), DataType::LargeUtf8) {
            DataType::Utf8
        } else {
            DataType::Binary
        };
        work.flush()?;
        let casted = arrow_cast::cast(&array, &target);
        work.flush()?;
        if let Ok(casted) = casted {
            return normalize_bytes(casted, arg_idx, work, boundary);
        }
    }
    Err(raw_error(
        format!("xx_hash3_128: arg{} must be VARCHAR or VARBINARY", arg_idx),
        boundary,
    ))
}
/// V1 must call this immediately after each child evaluation, preserving the
/// first unsupported argument before attempting to evaluate a later child.
pub fn prepare_legacy_input(array: ArrayRef, arg_idx: usize) -> Result<BytesArray, String> {
    let raw = RefCell::new(None);
    let boundary = |message: &str| {
        *raw.borrow_mut() = Some(message.to_string());
        Err(KernelFailure::InstanceFailed)
    };
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    normalize_bytes(array, arg_idx, &mut work, Some(&boundary))
        .map_err(|failure| raw.into_inner().unwrap_or_else(|| legacy_failure(failure)))
}
fn reserve<T>(
    len: usize,
    legacy: bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<T>, KernelFailure> {
    if legacy {
        return Ok(Vec::with_capacity(len));
    }
    Layout::array::<T>(len).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut out = Vec::new();
    out.try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(out)
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let arguments = &contract.selected().argument_types;
    let target = contract.result_type();
    if arguments.is_empty()
        || target.data_type != DataType::FixedSizeBinary(16)
        || target.logical_type != ValueLogicalType::LargeInt
        || !target.nullable
    {
        return Err(invalid(
            "xx_hash3_128 requires nonempty arguments and its exact nullable LARGEINT result",
        ));
    }
    for source in arguments.iter() {
        observe()?;
        let FunctionArgumentType::Value(source) = source else {
            return Err(invalid("xx_hash3_128 requires byte value arguments"));
        };
        if !matches!(
            source.data_type,
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary
        ) {
            return Err(invalid(
                "xx_hash3_128 source carrier has no installed v1 byte projection",
            ));
        }
        // Binding already validates the explicit logical identity and carrier.
        // Opaque byte hashing produces LARGEINT without relabelling the source.
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(input.contract(), || work.step())?;
        let selection = input.selection();
        let mut arrays = reserve(input.arguments().len(), false, &mut work)?;
        let mut compact = reserve(input.arguments().len(), false, &mut work)?;
        for (idx, arg) in input.arguments().iter().enumerate() {
            work.step()?;
            let mut array = arg.array().clone();
            let large = matches!(
                array.data_type(),
                DataType::LargeUtf8 | DataType::LargeBinary
            );
            if large {
                // Only demanded addresses enter the original Large->small normalizer.
                // Arrow take preserves the Large source domain; it does no text decoding.
                let mut indices = reserve(selection.len(), false, &mut work)?;
                for (ordinal, row) in selection.iter().enumerate() {
                    work.step()?;
                    indices.push(arg.value_row(ordinal, row) as u64);
                }
                work.flush()?;
                let indices = UInt64Array::from(indices);
                work.flush()?;
                array =
                    arrow_select::take::take(array.as_ref(), &indices, None).map_err(|error| {
                        internal(&format!(
                            "xx_hash3_128 selected byte projection failed: {error}"
                        ))
                    })?;
                work.flush()?;
            }
            arrays.push(normalize_bytes(array, idx, &mut work, None)?);
            compact.push(large);
        }
        let out = hash_values(
            &arrays,
            selection,
            |arg, ordinal, row| {
                if compact[arg] {
                    ordinal
                } else {
                    input.arguments()[arg].value_row(ordinal, row)
                }
            },
            false,
            &mut work,
            None,
        )?;
        SelectedValues::try_new_observed(
            selection,
            &input.contract().result_type().data_type,
            out,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
/// Hash the already-normalized legacy values; preserve its optional raw output cast.
pub fn evaluate_legacy(
    inputs: &[BytesArray],
    rows: usize,
    target: Option<&DataType>,
) -> Result<ArrayRef, String> {
    let raw = RefCell::new(None);
    let boundary = |message: &str| {
        *raw.borrow_mut() = Some(message.to_string());
        Err(KernelFailure::InstanceFailed)
    };
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    let result = (|| {
        let out = hash_values(
            inputs,
            Selection::all(rows),
            |_, _, row| row,
            true,
            &mut work,
            Some(&boundary),
        )?;
        let Some(target) = target else {
            return Ok(out);
        };
        if out.data_type() == target || largeint::is_largeint_data_type(target) {
            return Ok(out);
        }
        work.flush()?;
        let out = arrow_cast::cast(&out, target).map_err(|error| {
            raw_error(
                format!("xx_hash3_128: failed to cast output: {}", error),
                Some(&boundary),
            )
        })?;
        work.flush()?;
        Ok(out)
    })();
    result.map_err(|failure| raw.into_inner().unwrap_or_else(|| legacy_failure(failure)))
}
fn hash_values(
    inputs: &[BytesArray],
    selection: Selection<'_>,
    mut row_for: impl FnMut(usize, usize, usize) -> usize,
    legacy: bool,
    work: &mut EvaluationCheckpoints<'_>,
    boundary: ErrorBoundary<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let mut out = reserve(selection.len(), legacy, work)?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let mut is_null = false;
        for (idx, input) in inputs.iter().enumerate() {
            work.step()?;
            if input.is_null(row_for(idx, ordinal, row)) {
                is_null = true;
                break;
            }
        }
        if is_null {
            out.push(None);
            continue;
        }
        work.flush()?;
        let mut hasher = Hasher::with_seed(0);
        work.flush()?;
        for (idx, input) in inputs.iter().enumerate() {
            work.step()?;
            // Original Hasher write calls append one contiguous stream. Chunking
            // changes cooperative boundaries without adding argument delimiters.
            for bytes in input.bytes(row_for(idx, ordinal, row)).chunks(256) {
                work.flush()?;
                hasher.write(bytes);
                work.flush()?;
                for _ in bytes {
                    work.step()?;
                }
            }
        }
        work.flush()?;
        out.push(Some(hasher.finish_128() as i128));
        work.flush()?;
    }
    if !legacy {
        Layout::array::<u8>(
            selection
                .len()
                .checked_mul(16)
                .ok_or(KernelFailure::ResourceExhausted)?,
        )
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    }
    largeint::array_from_i128_observed(&out, &mut |event| match event {
        LargeIntObservation::Step => work.step(),
        LargeIntObservation::OpaqueBoundary => work.flush(),
    })?
    .map_err(|message| raw_error(message, boundary))
}
struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("pure scalar calculation never waits")
    }

    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
}
fn legacy_failure(failure: KernelFailure) -> String {
    match failure {
        KernelFailure::InvalidProgram(message)
        | KernelFailure::Internal(message)
        | KernelFailure::Operational(message) => message.message().to_string(),
        other => other.to_string(),
    }
}
#[cfg(test)]
#[path = "xx_hash3_128_tests.rs"]
mod tests;
