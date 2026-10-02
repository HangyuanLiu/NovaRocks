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

//! Exact selected CRC32 for the installed physical Utf8 profile.
//! Output capacity checks do not replace formal host memory admission.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StringArray, builder::Int64Builder};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

pub(super) fn evaluate_crc32<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let ([FunctionArgumentType::Value(source)], [argument]) = (
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) else {
        return Err(invalid("crc32 requires exactly one checked value argument"));
    };
    let target = input.contract().result_type();
    if source.logical_type != ValueLogicalType::Physical
        || source.data_type != DataType::Utf8
        || target.logical_type != ValueLogicalType::Physical
        || target.data_type != DataType::Int64
        || !target.nullable
    {
        return Err(invalid("crc32 differs from its exact installed profile"));
    }
    if argument.array().data_type() != &DataType::Utf8 {
        return Err(internal("crc32 carrier differs from its checked argument"));
    }
    let values = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("crc32 selected Utf8 carrier cannot be downcast"))?;
    let selection = input.selection();
    output_capacity(selection.len())?;
    let mut builder = Int64Builder::with_capacity(selection.len());
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        if row >= values.len() {
            return Err(internal(
                "crc32 selected row is outside its checked carrier",
            ));
        }
        if values.is_null(row) {
            if !source.nullable {
                return Err(internal(
                    "crc32 non-null argument contains selected SQL NULL",
                ));
            }
            builder.append_null();
        } else {
            // Preserve the original UTF-8 bytes without normalization or
            // signed reinterpretation of the final unsigned 32-bit checksum.
            builder.append_value(i64::from(checksum(
                values.value(row).as_bytes(),
                &mut work,
            )?));
        }
    }
    let output = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    SelectedValues::try_new(selection, &target.data_type, output, Box::default())
        .map_err(|_| internal("crc32 compact output violates its selected contract"))
}

fn checksum(bytes: &[u8], work: &mut EvaluationCheckpoints<'_>) -> Result<u32, KernelFailure> {
    let mut crc = 0xffff_ffff_u32;
    for &byte in bytes {
        work.step()?;
        crc ^= u32::from(byte);
        // Eight fixed bit operations are one bounded byte work unit. The
        // polynomial is IEEE CRC-32 (zlib), not the CRC32C workspace library.
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    Ok(crc ^ 0xffff_ffff)
}

/// Allocation representability only; this does not authorize host memory.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(values).map_err(|_| KernelFailure::ResourceExhausted)?;
    isize::try_from(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
#[path = "crc32_tests.rs"]
mod tests;
