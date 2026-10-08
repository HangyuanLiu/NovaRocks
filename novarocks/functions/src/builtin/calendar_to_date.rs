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

//! One original date extraction projection over the existing temporal reader.
//! Raw v1 admission and selected owner admission are explicit input contracts.
use super::calendar_extended::DateInput;
use super::calendar_extended_shared::CalendarInput;
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, SelectedValues,
    datetime_value::UNIX_EPOCH_DAY_OFFSET,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Date32Array};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::DataType;
use chrono::Datelike;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc, time::Duration};
pub(super) fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
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
pub(super) fn evaluate_to_date<'a>(
    input: CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.types;
        let args = input.arguments();
        if types.len() != 1 || args.len() != 1 {
            return Err(invalid("date requires its installed single argument"));
        }
        let FunctionArgumentType::Value(source) = &types[0] else {
            return Err(invalid("date requires a checked value"));
        };
        let array = args[0].array();
        let reader = DateInput::date_for_input(source, array.as_ref(), input)?;
        work.step()?;
        let target = input.target;
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
                reader.read(row, &mut work)?.map(|value| value.date())
            };
            if let Some(date) = date {
                values.push(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
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

struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        unreachable!("date extraction never waits")
    }
}
/// The arena caller supplies one evaluated child and all original child rows.
/// This adapter has no live date, timezone, name lookup or alternate parser.
pub fn evaluate_legacy_to_date(array: &ArrayRef) -> Result<ArrayRef, String> {
    use super::calendar_extended::CalendarCarrierError;
    use crate::{EvaluatedArgument, FunctionValueType, Selection};
    let types = [FunctionArgumentType::Value(FunctionValueType::new(
        array.data_type().clone(),
        true,
    ))];
    let arguments = [EvaluatedArgument::Column(array)];
    let target = FunctionValueType::new(DataType::Date32, true);
    let failure = std::cell::RefCell::new(None);
    let carrier_error_boundary = |error: CalendarCarrierError<'_>| {
        *failure.borrow_mut() = Some(error.legacy_message());
        internal("legacy date carrier failure")
    };
    let output = evaluate_to_date(
        CalendarInput {
            types: &types,
            target: &target,
            carrier: &DataType::Date32,
            arguments: &arguments,
            selected: Selection::all(array.len()),
            row_error_boundary: None,
            carrier_error_boundary: Some(&carrier_error_boundary),
            legacy: true,
        },
        &LegacyControl,
    )
    .map_err(|error| {
        failure.take().unwrap_or_else(|| match error {
            KernelFailure::InvalidProgram(message)
            | KernelFailure::Internal(message)
            | KernelFailure::Operational(message) => message.message().to_string(),
            other => other.to_string(),
        })
    })?;
    Ok(output.values().clone())
}
