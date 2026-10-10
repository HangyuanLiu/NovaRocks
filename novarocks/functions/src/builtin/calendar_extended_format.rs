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

//! Original MySQL-to-Chrono date formatting over exactly selected rows.

use super::calendar_extended::DateInput;
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

pub(super) fn mysql_format_to_chrono(
    fmt: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, KernelFailure> {
    let capacity = fmt
        .len()
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(capacity).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        work.step()?;
        if c == '%' {
            if let Some(n) = chars.next() {
                work.step()?;
                match n {
                    'Y' => out.push_str("%Y"),
                    'y' => out.push_str("%y"),
                    'm' | 'c' => out.push_str("%m"),
                    'd' | 'e' => out.push_str("%d"),
                    'H' => out.push_str("%H"),
                    'h' | 'I' => out.push_str("%I"),
                    'i' => out.push_str("%M"),
                    's' | 'S' => out.push_str("%S"),
                    'f' => out.push_str("%f"),
                    'T' => out.push_str("%H:%M:%S"),
                    _ => {
                        out.push('%');
                        out.push(n);
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}
fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<Option<String>>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<i32>(
        rows.checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?,
    )
    .map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    rows.checked_add(63)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}
pub(super) fn evaluate_date_format<'a>(
    input: super::calendar_extended_shared::CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let (
            [
                FunctionArgumentType::Value(date_type),
                FunctionArgumentType::Value(format_type),
            ],
            [date_arg, format_arg],
        ) = (input.types, input.arguments())
        else {
            return Err(invalid(
                "date_format requires its exact two value arguments",
            ));
        };
        let target = input.target;
        if format_type.logical_type != ValueLogicalType::Physical
            || format_type.data_type != DataType::Utf8
            || format_arg.array().data_type() != &DataType::Utf8
            || target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "date_format differs from its exact installed profile",
            ));
        }
        let dates = DateInput::for_input(date_type, date_arg.array().as_ref(), input)?;
        let formats = format_arg
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("date_format format cannot be downcast"))?;
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        work.flush()?;
        let mut outputs = Vec::new();
        outputs
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut bytes_count = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let date_row = date_arg.value_row(ordinal, batch_row);
            let format_row = format_arg.value_row(ordinal, batch_row);
            work.step()?;
            if date_row >= date_arg.array().len() || format_row >= formats.len() {
                return Err(internal("date_format selected row is out of bounds"));
            }
            let date_null = date_arg.array().is_null(date_row);
            let format_null = formats.is_null(format_row);
            if date_null && !date_type.nullable || format_null && !format_type.nullable {
                return Err(internal(
                    "date_format non-null source contains selected SQL NULL",
                ));
            }
            let output = if date_null || format_null {
                None
            } else {
                let dt = dates.read(date_row, &mut work)?;
                let format = mysql_format_to_chrono(formats.value(format_row), &mut work)?;
                work.flush()?;
                // Keep the original full-format replacement after mapping.
                let format = format.replace("%f", "%6f");
                work.flush()?;
                work.flush()?;
                let value = dt.and_then(|date| {
                    let value = date.format(&format).to_string();
                    (value.len() <= 128).then_some(value)
                });
                work.flush()?;
                value
            };
            if let Some(text) = &output {
                bytes_count = bytes_count
                    .checked_add(text.len())
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            outputs.push(output);
            work.step()?;
        }
        output_capacity(selection.len(), bytes_count)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(bytes_count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(selection.len() + 1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        let mut has_null = false;
        offsets.push(0i32);
        for output in outputs {
            if let Some(text) = output {
                for byte in text.bytes() {
                    bytes.push(byte);
                    work.step()?;
                }
                validity.append(true);
            } else {
                validity.append(false);
                has_null = true;
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
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
    work.finish_result(result)
}
