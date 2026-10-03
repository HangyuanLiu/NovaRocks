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

//! Selected date extraction through the existing pure calendar/parser owner.
//! Canonical selected targets are Date32, Timestamp(Microsecond,None), Utf8.
//! Timestamp conversion retains the old UTC-naive value algorithm, without
//! a session or host timezone lookup. Chrono 0.4.42's parse_from_str uses
//! borrowed StrftimeItems and stack Parsed; the flexible helper uses fixed
//! arrays and observes its actual owned scan work. Chrono/std calls retain
//! opaque before/after observations, without a formal allocation/funding grant.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    datetime_value::{
        DateParseObservation, UNIX_EPOCH_DAY_OFFSET, date32_to_naive, parse_date_observed,
        parse_datetime_observed, timestamp_to_naive,
    },
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDate};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = Layout::array::<i32>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}
enum DateInput<'a> {
    Date(&'a Date32Array),
    Timestamp(&'a TimestampMicrosecondArray),
    Text(&'a StringArray),
}
impl DateInput<'_> {
    fn date(
        &self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<NaiveDate>, KernelFailure> {
        let result = match self {
            Self::Date(array) => date32_to_naive(array.value(row)),
            Self::Timestamp(array) => {
                timestamp_to_naive(&TimeUnit::Microsecond, array.value(row)).map(|dt| dt.date())
            }
            Self::Text(array) => {
                let text = array.value(row);
                // Retain the sole parser and its exact fallback order. Observe
                // its completed owned scans and opaque library boundaries.
                let mut observe = |event| match event {
                    DateParseObservation::Step => work.step(),
                    DateParseObservation::OpaqueBoundary => work.flush(),
                };
                if let Some(datetime) = parse_datetime_observed(text, &mut observe)? {
                    Some(datetime.date())
                } else {
                    parse_date_observed(text, &mut observe)?
                }
            }
        };
        work.step()?;
        Ok(result)
    }
}

pub(super) fn evaluate_date<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let args = input.arguments();
        if types.len() != 1 || args.len() != 1 {
            return Err(invalid("date requires its installed single argument"));
        }
        let FunctionArgumentType::Value(source) = &types[0] else {
            return Err(invalid("date requires a checked value"));
        };
        if source.logical_type != ValueLogicalType::Physical {
            return Err(invalid(
                "date source differs from its installed logical domain",
            ));
        }
        let array = args[0].array();
        let reader = match &source.data_type {
            DataType::Date32 => DateInput::Date(
                array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| internal("date carrier is not Date32"))?,
            ),
            DataType::Timestamp(TimeUnit::Microsecond, None) => DateInput::Timestamp(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| internal("date carrier is not TimestampMicrosecond"))?,
            ),
            DataType::Utf8 => DateInput::Text(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("date carrier is not Utf8"))?,
            ),
            _ => {
                return Err(invalid(
                    "date source differs from its canonical selected profile",
                ));
            }
        };
        work.step()?;
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Date32
            || !target.nullable
        {
            return Err(invalid(
                "date differs from its installed nullable Date32 result",
            ));
        }
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
            let row = args[0].value_row(ordinal, batch_row);
            work.step()?;
            if row >= array.len() {
                return Err(internal("date selected row is out of bounds"));
            }
            let date = if array.is_null(row) {
                if !source.nullable {
                    return Err(internal("date non-null source contains selected SQL NULL"));
                }
                None
            } else {
                reader.date(row, &mut work)?
            };
            if let Some(date) = date {
                values.push(
                    date.num_days_from_ce()
                        .checked_sub(UNIX_EPOCH_DAY_OFFSET)
                        .ok_or_else(|| internal("date result exceeds its epoch representation"))?,
                );
                validity.append(true);
            } else {
                values.push(0);
                validity.append(false);
                has_null = true;
            }
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
#[path = "date_tests.rs"]
mod tests;
