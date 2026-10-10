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

//! The installed to_days profiles use the original forward Julian arithmetic.
//! Date parsing and value conversion are shared with their exact source authors.
//! Output Layout checks are representation facts, not formal memory admission.

use crate::{
    FunctionArgumentType, FunctionValueType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues,
    datetime_value::{self, DateParseObservation},
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::{DataType, TimeUnit};
use chrono::NaiveDate;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CalendarDayNumberOp {
    ToDays,
}

enum DateInput<'a> {
    Date(&'a Date32Array),
    Timestamp(&'a TimestampMicrosecondArray),
    Text(&'a StringArray),
}
impl<'a> DateInput<'a> {
    fn checked(source: &FunctionValueType, array: &'a dyn Array) -> Result<Self, KernelFailure> {
        if source.logical_type != ValueLogicalType::Physical {
            return Err(invalid(
                "calendar day number requires its exact Physical source domain",
            ));
        }
        if array.data_type() != &source.data_type {
            return Err(internal(
                "calendar day number carrier differs from its exact selected source",
            ));
        }
        match &source.data_type {
            DataType::Date32 => array.as_any().downcast_ref().map(Self::Date),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                array.as_any().downcast_ref().map(Self::Timestamp)
            }
            DataType::Utf8 => array.as_any().downcast_ref().map(Self::Text),
            _ => {
                return Err(invalid(
                    "calendar day number differs from its three exact installed profiles",
                ));
            }
        }
        .ok_or_else(|| internal("calendar day number exact selected carrier cannot be downcast"))
    }
    fn read(
        &self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<NaiveDate>, KernelFailure> {
        match self {
            Self::Date(values) => {
                let value = values.value(row);
                work.step()?;
                work.flush()?;
                let date = datetime_value::date32_to_naive(value);
                work.flush()?;
                Ok(date)
            }
            Self::Timestamp(values) => {
                let value = values.value(row);
                work.step()?;
                work.flush()?;
                let date = datetime_value::timestamp_to_naive(&TimeUnit::Microsecond, value)
                    .map(|value| value.date());
                work.flush()?;
                Ok(date)
            }
            Self::Text(values) => {
                let text = values.value(row);
                let mut observe = |event| match event {
                    DateParseObservation::Step => work.step(),
                    DateParseObservation::OpaqueBoundary => work.flush(),
                };
                if let Some(value) = datetime_value::parse_datetime_observed(text, &mut observe)? {
                    let date = value.date();
                    observe(DateParseObservation::Step)?;
                    return Ok(Some(date));
                }
                datetime_value::parse_date_observed(text, &mut observe)
            }
        }
    }
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = Layout::array::<i64>(rows)
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

pub(super) fn evaluate_calendar_day_number<'a>(
    op: CalendarDayNumberOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let CalendarDayNumberOp::ToDays = op;
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid(
                "calendar day number requires one exact checked value argument",
            ));
        };
        let target = input.contract().result_type();
        let exact = target.logical_type == ValueLogicalType::Physical
            && target.data_type == DataType::Int64
            && target.nullable;
        work.step()?;
        if !exact {
            return Err(invalid(
                "calendar day number differs from its exact nullable Int64 result",
            ));
        }
        let reader = DateInput::checked(source, argument.array().as_ref())?;
        work.step()?;
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
            let row = argument.value_row(ordinal, batch_row);
            let valid_address = row < argument.array().len();
            work.step()?;
            if !valid_address {
                return Err(internal(
                    "calendar day number selected row is out of bounds",
                ));
            }
            let null = argument.array().is_null(row);
            work.step()?;
            if null && !source.nullable {
                return Err(internal(
                    "calendar day number non-null source contains selected SQL NULL",
                ));
            }
            let parsed = if null {
                None
            } else {
                reader.read(row, &mut work)?
            };
            let number = match parsed {
                Some(date) => {
                    // This exact finite helper deliberately retains signed Rust
                    // division for negative years; it is not CE-day rebasing.
                    work.flush()?;
                    let number = crate::calendar_julian::day_number_from_date(date);
                    work.flush()?;
                    Some(number)
                }
                None => None,
            };
            values.push(number.unwrap_or(0));
            validity.append(number.is_some());
            has_null |= number.is_none();
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(Int64Array::new(
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
#[path = "calendar_day_number_tests.rs"]
mod tests;
