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

//! Selected original uppercase-only percent decoding with required row errors.
//! Output and error layouts are checked before reservations. Standard UTF-8
//! checks inspect bounded borrowed blocks under opaque entry/exit observation;
//! allocator/library internals and formal host memory grants remain open.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput,
    SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{
    alloc::Layout,
    fmt::{self, Write},
    sync::Arc,
};

#[derive(Clone, Copy)]
enum DecodeError {
    ShortPercent,
    HexPair(u8, u8),
    Utf8 { index: usize, length: Option<usize> },
}
fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
fn walk_decoded(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(u8, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
) -> Result<Result<usize, DecodeError>, KernelFailure> {
    let source = text.as_bytes();
    let mut at = 0usize;
    let mut length = 0usize;
    while at < source.len() {
        let byte = source[at];
        work.step()?;
        if byte == b'%' {
            if source.len() - at < 3 {
                return Ok(Err(DecodeError::ShortPercent));
            }
            let left = source[at + 1];
            work.step()?;
            let right = source[at + 2];
            work.step()?;
            let (Some(left_hex), Some(right_hex)) = (hex(left), hex(right)) else {
                return Ok(Err(DecodeError::HexPair(left, right)));
            };
            emit((left_hex << 4) | right_hex, work)?;
            at += 3;
        } else {
            // A literal plus is unchanged, exactly as in the original body.
            emit(byte, work)?;
            at += 1;
        }
        length += 1; // decoded length never exceeds the original Utf8 extent.
    }
    Ok(Ok(length))
}

struct Utf8Scan {
    bytes: [u8; 256],
    length: usize,
    processed: usize,
    failure: Option<DecodeError>,
}
impl Utf8Scan {
    fn new() -> Self {
        Self {
            bytes: [0; 256],
            length: 0,
            processed: 0,
            failure: None,
        }
    }
    fn push(
        &mut self,
        byte: u8,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if self.failure.is_some() {
            return Ok(());
        }
        self.bytes[self.length] = byte;
        self.length += 1;
        work.step()?;
        if self.length == self.bytes.len() {
            self.check(false, work)?;
        }
        Ok(())
    }
    fn check(
        &mut self,
        final_block: bool,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if self.failure.is_some() || self.length == 0 {
            return Ok(());
        }
        work.flush()?;
        let error = std::str::from_utf8(&self.bytes[..self.length]).err();
        work.flush()?;
        match error {
            None => {
                self.processed += self.length;
                self.length = 0;
            }
            Some(error) if final_block || error.error_len().is_some() => {
                self.failure = Some(DecodeError::Utf8 {
                    index: self.processed + error.valid_up_to(),
                    length: error.error_len(),
                });
            }
            Some(error) => {
                // The original UTF-8 owner identifies an incomplete final
                // scalar. Carry at most three bytes into the next block.
                let valid = error.valid_up_to();
                let tail = self.length - valid;
                if tail > 3 {
                    return Err(internal("url_decode UTF-8 block tail exceeds one scalar"));
                }
                for index in 0..tail {
                    self.bytes[index] = self.bytes[valid + index];
                    work.step()?;
                }
                self.processed += valid;
                self.length = tail;
            }
        }
        Ok(())
    }
}
fn inspect(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<usize, DecodeError>, KernelFailure> {
    let mut utf8 = Utf8Scan::new();
    let length = walk_decoded(text, work, |byte, work| utf8.push(byte, work))?;
    // Malformed percent anywhere wins over invalid decoded UTF-8, because the
    // original algorithm finishes percent parsing before String::from_utf8.
    let Ok(length) = length else {
        return Ok(length);
    };
    utf8.check(true, work)?;
    Ok(match utf8.failure {
        Some(error) => Err(error),
        None => Ok(length),
    })
}

// Static text and at most two byte-derived chars use fewer than 96 bytes.
// A usize decimal index uses no more than usize::BITS digits. This fixed
// stack bound avoids allocating a temporary diagnostic String.
const DIAGNOSTIC_CAPACITY: usize = 96 + usize::BITS as usize;
struct Diagnostic<'a, 'c> {
    bytes: [u8; DIAGNOSTIC_CAPACITY],
    length: usize,
    work: &'a mut EvaluationCheckpoints<'c>,
    refusal: Option<KernelFailure>,
}
impl Write for Diagnostic<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if text.len() > self.bytes.len() - self.length {
            return Err(fmt::Error);
        }
        for byte in text.bytes() {
            self.bytes[self.length] = byte;
            self.length += 1;
            if let Err(cause) = self.work.step() {
                self.refusal = Some(cause);
                return Err(fmt::Error);
            }
        }
        Ok(())
    }
}
fn row_error(
    error: DecodeError,
    ordinal: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<RowDataError, KernelFailure> {
    let mut diagnostic = Diagnostic {
        bytes: [0; DIAGNOSTIC_CAPACITY],
        length: 0,
        work,
        refusal: None,
    };
    let result = match error {
        DecodeError::ShortPercent => {
            write!(diagnostic, "decode string contains illegal hex chars: %")
        }
        DecodeError::HexPair(left, right) => write!(
            diagnostic,
            "decode string contains illegal hex chars: {}{}",
            char::from(left),
            char::from(right)
        ),
        DecodeError::Utf8 {
            index,
            length: Some(length),
        } => write!(
            diagnostic,
            "url_decode utf8 decode failed: invalid utf-8 sequence of {length} bytes from index {index}"
        ),
        DecodeError::Utf8 {
            index,
            length: None,
        } => write!(
            diagnostic,
            "url_decode utf8 decode failed: incomplete utf-8 byte sequence from index {index}"
        ),
    };
    if let Some(cause) = diagnostic.refusal {
        return Err(cause);
    }
    result.map_err(|_| internal("url_decode diagnostic exceeds its source-derived stack bound"))?;
    diagnostic.work.flush()?;
    let text = std::str::from_utf8(&diagnostic.bytes[..diagnostic.length])
        .map_err(|_| internal("url_decode diagnostic is not UTF-8"))?;
    diagnostic.work.flush()?;
    let error = RowDataError::new(ordinal, text);
    diagnostic.work.flush()?;
    Ok(error)
}
fn output_capacity(rows: usize, bytes: usize, errors: usize) -> Result<(), KernelFailure> {
    let error_slots = Layout::array::<RowDataError>(errors)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let messages = errors
        .checked_mul(crate::MAX_ROW_ERROR_MESSAGE_BYTES)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(messages).map_err(|_| KernelFailure::ResourceExhausted)?;
    error_slots
        .checked_add(messages)
        .ok_or(KernelFailure::ResourceExhausted)?;
    i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let offsets = rows
        .checked_add(1)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let offsets = Layout::array::<i32>(offsets)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|bits| bits / 64)
        .and_then(|words| words.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|value| value.checked_add(bitmap))
        .and_then(|value| value.checked_add(error_slots))
        .and_then(|value| value.checked_add(messages))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

pub(super) fn evaluate_string_url_decode<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid("URL decoding requires one checked value argument"));
        };
        let target = input.contract().result_type();
        if source.logical_type != ValueLogicalType::Physical
            || source.data_type != DataType::Utf8
            || target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "URL decoding differs from its exact installed profile",
            ));
        }
        let values = argument
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("URL decoding selected carrier is not Utf8"))?;
        let selection = input.selection();
        output_capacity(selection.len(), 0, 0)?;
        let mut error_count = 0usize;
        let mut output_bytes = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            work.step()?;
            if row >= values.len() {
                return Err(internal("URL decoding selected row is outside its carrier"));
            }
            if values.is_null(row) {
                if !source.nullable {
                    return Err(internal(
                        "URL decoding non-null source contains selected SQL NULL",
                    ));
                }
                continue;
            }
            let selected = values.value(row);
            match inspect(selected, &mut work)? {
                Ok(length) => {
                    output_bytes = output_bytes
                        .checked_add(length)
                        .ok_or(KernelFailure::ResourceExhausted)?;
                }
                Err(_) => {
                    error_count = error_count
                        .checked_add(1)
                        .ok_or(KernelFailure::ResourceExhausted)?;
                }
            }
        }
        output_capacity(selection.len(), output_bytes, error_count)?;
        work.flush()?;
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(error_count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(output_bytes)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(selection.len() + 1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0i32);
        let mut has_null = false;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            if values.is_null(row) {
                has_null = true;
                validity.append(false);
            } else {
                let selected = values.value(row);
                match inspect(selected, &mut work)? {
                    Err(error) => {
                        has_null = true;
                        validity.append(false);
                        errors.push(row_error(error, ordinal, &mut work)?);
                        work.step()?;
                    }
                    Ok(length) => {
                        if length > output_bytes - bytes.len() {
                            return Err(internal("url_decode exceeded its measured output extent"));
                        }
                        let start = bytes.len();
                        let copied = walk_decoded(selected, &mut work, |byte, work| {
                            bytes.push(byte);
                            work.step()
                        })?
                        .map_err(|_| {
                            internal("url_decode immutable source changed after inspection")
                        })?;
                        if copied != length || bytes.len() - start != length {
                            return Err(internal(
                                "url_decode differs from its measured row extent",
                            ));
                        }
                        validity.append(true);
                    }
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != output_bytes || errors.len() != error_count {
            return Err(internal(
                "URL decoding differs from its measured output extent",
            ));
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
        work.flush()?;
        let errors = errors.into_boxed_slice();
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            array,
            errors,
            || work.step(),
        )
    })();
    if matches!(
        &result,
        Err(KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
#[path = "string_url_decode_tests.rs"]
mod tests;
