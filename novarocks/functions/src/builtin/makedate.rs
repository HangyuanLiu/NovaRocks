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

//! Exact selected MAKEDATE over its two canonical Int64 arguments.
//! The year/day algorithm has one shared source owner. Output Layout checks
//! establish representability, not a formal memory grant.

use crate::{
    FunctionArgumentType, FunctionValueType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Date32Array, Int64Array};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::DataType;
use chrono::Datelike;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

fn checked_input<'a>(
    source: &FunctionValueType,
    array: &'a dyn Array,
) -> Result<&'a Int64Array, KernelFailure> {
    if source.logical_type != ValueLogicalType::Physical || source.data_type != DataType::Int64 {
        return Err(invalid(
            "makedate requires its exact Physical Int64 source domain",
        ));
    }
    if array.data_type() != &source.data_type {
        return Err(internal(
            "makedate carrier differs from its exact selected source",
        ));
    }
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| internal("makedate exact selected carrier cannot be downcast to Int64Array"))
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = Layout::array::<i32>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|bits| bits / 64)
        .and_then(|words| words.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

pub(super) fn evaluate_makedate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let (
            [
                FunctionArgumentType::Value(year_type),
                FunctionArgumentType::Value(day_type),
            ],
            [year_argument, day_argument],
        ) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        )
        else {
            return Err(invalid(
                "makedate requires two exact checked value arguments",
            ));
        };
        let target = input.contract().result_type();
        let exact = target.logical_type == ValueLogicalType::Physical
            && target.data_type == DataType::Date32
            && target.nullable;
        work.step()?;
        if !exact {
            return Err(invalid(
                "makedate differs from its exact nullable Date32 result",
            ));
        }
        // Both concrete classes are checked before any strict NULL short circuit.
        let year_values = checked_input(year_type, year_argument.array().as_ref());
        work.step()?;
        let year_values = year_values?;
        let day_values = checked_input(day_type, day_argument.array().as_ref());
        work.step()?;
        let day_values = day_values?;
        let arguments = [year_argument, day_argument];
        let types = [year_type, day_type];
        let selection = input.selection();
        output_capacity(selection.len())?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        let mut has_null = false;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let mut rows = [0usize; 2];
            let mut null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                let valid_address = row < argument.array().len();
                work.step()?;
                if !valid_address {
                    return Err(internal("makedate selected row is out of bounds"));
                }
                let source_null = argument.array().is_null(row);
                work.step()?;
                if source_null && !types[index].nullable {
                    return Err(internal(
                        "makedate non-null source contains selected SQL NULL",
                    ));
                }
                null |= source_null;
            }
            let date32 = if null {
                None
            } else {
                let year = year_values.value(rows[0]);
                let day = day_values.value(rows[1]);
                work.step()?;
                // Chrono construction is a finite opaque dependency operation;
                // its internals are not claimed to observe our work quantum.
                work.flush()?;
                let date = crate::calendar_numeric::makedate_from_year_day(year, day);
                work.flush()?;
                let date32 = date.map(|date| {
                    // The shared helper restricts years to 0..=9999, so this
                    // original CE-day subtraction fits the Date32 carrier.
                    date.num_days_from_ce() - crate::datetime_value::UNIX_EPOCH_DAY_OFFSET
                });
                work.step()?;
                date32
            };
            values.push(date32.unwrap_or(0));
            validity.append(date32.is_some());
            has_null |= date32.is_none();
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(Date32Array::new(
            values.into(),
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
#[path = "makedate_tests.rs"]
mod tests;
