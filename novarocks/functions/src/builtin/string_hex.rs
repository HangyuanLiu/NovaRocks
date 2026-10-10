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

//! Selected HEX over the three installed Utf8, Binary and Int64 profiles.
//! Two passes measure and write uppercase ASCII; no per-row String is allocated.
//! Layout checks describe representation, not a formal host memory grant.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let offsets = Layout::array::<i32>(
        rows.checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?,
    )
    .map_err(|_| KernelFailure::ResourceExhausted)?
    .size();
    let bitmap = rows
        .checked_add(63)
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}
fn byte_hex_length(bytes: usize) -> Result<usize, KernelFailure> {
    bytes.checked_mul(2).ok_or(KernelFailure::ResourceExhausted)
}
fn integer_hex_length(value: u64) -> usize {
    if value == 0 {
        1
    } else {
        ((64 - value.leading_zeros()) as usize).div_ceil(4)
    }
}
#[derive(Clone, Copy)]
enum Row<'a> {
    Bytes(&'a [u8]),
    Integer(u64),
}
impl Row<'_> {
    fn length(self) -> Result<usize, KernelFailure> {
        match self {
            Self::Bytes(bytes) => byte_hex_length(bytes.len()),
            Self::Integer(value) => Ok(integer_hex_length(value)),
        }
    }
    fn emit(
        self,
        bytes: &mut Vec<u8>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        match self {
            Self::Bytes(input) => {
                for byte in input {
                    bytes.push(HEX[usize::from(byte >> 4)]);
                    work.step()?;
                    bytes.push(HEX[usize::from(byte & 15)]);
                    work.step()?;
                }
            }
            Self::Integer(value) => {
                for index in (0..integer_hex_length(value)).rev() {
                    bytes.push(HEX[((value >> (index * 4)) & 15) as usize]);
                    work.step()?;
                }
            }
        }
        Ok(())
    }
}
enum Source<'a> {
    Utf8(&'a StringArray),
    Binary(&'a BinaryArray),
    Int64(&'a Int64Array),
}
impl<'a> Source<'a> {
    fn row(&self, row: usize) -> Row<'a> {
        match self {
            Self::Utf8(array) => Row::Bytes(array.value(row).as_bytes()),
            Self::Binary(array) => Row::Bytes(array.value(row)),
            // Rust UpperHex on negative i64 renders this exact two's-complement
            // unsigned bit pattern, without adding a minus sign.
            Self::Int64(array) => Row::Integer(array.value(row) as u64),
        }
    }
}

pub(super) fn evaluate_string_hex<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 1 || arguments.len() != 1 {
            return Err(invalid("hex requires its exact one checked argument"));
        }
        let FunctionArgumentType::Value(source) = &types[0] else {
            return Err(invalid("hex requires a value argument"));
        };
        let exact = source.logical_type == ValueLogicalType::Physical
            && matches!(
                source.data_type,
                DataType::Utf8 | DataType::Binary | DataType::Int64
            );
        work.step()?;
        if !exact {
            return Err(invalid(
                "hex differs from its exact installed argument profile",
            ));
        }
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "hex differs from its exact installed result profile",
            ));
        }
        let array = arguments[0].array();
        let concrete = match source.data_type {
            DataType::Utf8 => Source::Utf8(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("hex selected source is not Utf8"))?,
            ),
            DataType::Binary => Source::Binary(
                array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| internal("hex selected source is not Binary"))?,
            ),
            DataType::Int64 => Source::Int64(
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| internal("hex selected source is not Int64"))?,
            ),
            _ => {
                return Err(invalid(
                    "hex selected source is outside the installed profiles",
                ));
            }
        };
        let selected = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<Row<'_>>, KernelFailure> {
            let row = arguments[0].value_row(ordinal, batch_row);
            work.step()?;
            if row >= array.len() {
                return Err(internal("hex selected argument row is out of bounds"));
            }
            if array.is_null(row) {
                if !source.nullable {
                    return Err(internal("hex non-null argument contains selected SQL NULL"));
                }
                Ok(None)
            } else {
                Ok(Some(concrete.row(row)))
            }
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, row) in selection.iter().enumerate() {
            if let Some(value) = selected(ordinal, row, &mut work)? {
                total = total
                    .checked_add(value.length()?)
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            work.step()?;
        }
        output_capacity(selection.len(), total)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
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
        for (ordinal, row) in selection.iter().enumerate() {
            match selected(ordinal, row, &mut work)? {
                None => {
                    validity.append(false);
                    has_null = true;
                }
                Some(value) => {
                    if value.length()? > total - bytes.len() {
                        return Err(internal("hex exceeded its measured output extent"));
                    }
                    value.emit(&mut bytes, &mut work)?;
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal("hex differs from its measured output extent"));
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            array,
            Box::default(),
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
#[path = "string_hex_tests.rs"]
mod tests;
