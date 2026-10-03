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

//! Date-only difference for the two actual three-profile calendar functions.
//! Parsers and timestamp/date value authors are shared, never reinterpreted.
//! Output Layout gates are representation facts, not formal memory admission.

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
pub(super) enum CalendarDiffOp {
    Difference,
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
                "calendar difference requires its exact Physical source domain",
            ));
        }
        if array.data_type() != &source.data_type {
            return Err(internal(
                "calendar difference carrier differs from its exact selected source",
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
                    "calendar difference differs from its three exact installed profiles",
                ));
            }
        }
        .ok_or_else(|| internal("calendar difference exact selected carrier cannot be downcast"))
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

pub(super) fn evaluate_calendar_diff<'a>(
    op: CalendarDiffOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let CalendarDiffOp::Difference = op;
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let (
            [
                FunctionArgumentType::Value(left_type),
                FunctionArgumentType::Value(right_type),
            ],
            [left, right],
        ) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        )
        else {
            return Err(invalid(
                "calendar difference requires two exact checked value arguments",
            ));
        };
        let target = input.contract().result_type();
        let exact = target.logical_type == ValueLogicalType::Physical
            && target.data_type == DataType::Int64
            && target.nullable
            && left_type.data_type == right_type.data_type;
        work.step()?;
        if !exact {
            return Err(invalid(
                "calendar difference differs from its exact homogeneous source and nullable Int64 result",
            ));
        }
        // Validate both concrete classes before any strict NULL short circuit.
        let readers = [
            DateInput::checked(left_type, left.array().as_ref())?,
            DateInput::checked(right_type, right.array().as_ref())?,
        ];
        work.step()?;
        let arguments = [left, right];
        let types = [left_type, right_type];
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
                    return Err(internal(
                        "calendar difference selected row is out of bounds",
                    ));
                }
                let source_null = argument.array().is_null(row);
                work.step()?;
                if source_null && !types[index].nullable {
                    return Err(internal(
                        "calendar difference non-null argument contains selected SQL NULL",
                    ));
                }
                null |= source_null;
            }
            let difference = if null {
                None
            } else {
                // Both date conversions are eager, including a right parser when
                // the left conversion yields a successful invalid-value NULL.
                let left = readers[0].read(rows[0], &mut work)?;
                let right = readers[1].read(rows[1], &mut work)?;
                work.flush()?;
                let difference = match (left, right) {
                    (Some(left), Some(right)) => Some((left - right).num_days()),
                    _ => None,
                };
                work.flush()?;
                difference
            };
            values.push(difference.unwrap_or(0));
            validity.append(difference.is_some());
            has_null |= difference.is_none();
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
#[path = "calendar_diff_tests.rs"]
mod tests;
