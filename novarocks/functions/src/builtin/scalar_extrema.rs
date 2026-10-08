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
//! The original greatest/least values shared by evaluated legacy and selected calls.
use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallContract, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    math_numeric::{
        MathNumericError, MathNumericObservation, NumericArrayView, cast_output,
        cast_output_observed, value_at_f64,
    },
};
use arrow_array::{Array, ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, cell::RefCell, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub enum ExtremaOperation {
    Greatest,
    Least,
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
fn reserve<T>(count: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}
fn temporal(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8 | DataType::Null
    )
}
fn numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Null
    )
}
/// The original short temporal argument reads NULL, while len-one broadcasts.
pub fn datetime_value_at(
    values: &[Option<NaiveDateTime>],
    row: usize,
    len: usize,
) -> Option<NaiveDateTime> {
    let idx = if values.len() == 1 && len > 1 { 0 } else { row };
    values.get(idx).copied().flatten()
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let arguments = &contract.selected().argument_types;
    if arguments.is_empty()
        || !contract.result_type().nullable
        || contract.result_type().logical_type != ValueLogicalType::Physical
    {
        return Err(invalid(
            "extrema requires a nonempty exact nullable Physical result profile",
        ));
    }
    let mut is_temporal = true;
    let mut is_numeric = true;
    for argument in arguments.iter() {
        observe()?;
        let FunctionArgumentType::Value(value) = argument else {
            return Err(invalid("extrema requires value arguments"));
        };
        if value.logical_type != ValueLogicalType::Physical
            && !(value.logical_type == ValueLogicalType::Json && value.data_type == DataType::Utf8)
        {
            return Err(invalid(
                "extrema has no value-preserving implementation for this logical domain",
            ));
        }
        is_temporal &= temporal(&value.data_type);
        is_numeric &= numeric(&value.data_type);
    }
    let target = &contract.result_type().data_type;
    if *target == DataType::Null {
        return Err(invalid(
            "greatest/least has no v1 value result for an exact Null target",
        ));
    }
    let target_admitted = if is_temporal {
        matches!(
            target,
            DataType::Utf8 | DataType::Date32 | DataType::Timestamp(_, _) | DataType::Null
        )
    } else if is_numeric {
        numeric(target) || matches!(target, DataType::Decimal256(_, _))
    } else {
        false
    };
    if !target_admitted {
        return Err(invalid(
            "extrema source/result carriers are not an installed v1 calculation profile",
        ));
    }
    // Target zone adjustment belongs to the static Arrow conversion, not a session clock.
    // The owner validates library support before admitting that exact target.
    Ok(())
}
/// Check a temporal target's exact static conversion, including timezone support.
pub(super) fn validate_target(contract: &ScalarCallContract) -> Result<(), KernelFailure> {
    if matches!(
        contract.result_type().data_type,
        DataType::Timestamp(_, Some(_))
    ) {
        let empty =
            Arc::new(TimestampMicrosecondArray::from(Vec::<Option<i64>>::new())) as ArrayRef;
        cast_output(empty, Some(&contract.result_type().data_type))
            .map_err(|message| invalid(&message))?;
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    operation: ExtremaOperation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(input.contract(), || work.step())?;
        let out = evaluate_values(
            operation,
            input.arguments(),
            input.selection(),
            Some(&input.contract().result_type().data_type),
            false,
            &mut work,
            None,
        )?;
        SelectedValues::try_new_observed(
            input.selection(),
            &input.contract().result_type().data_type,
            out,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
/// The caller owns eager child evaluation. This boundary receives values only
/// and projects each original raw diagnostic before the bounded owner boundary.
pub fn evaluate_legacy(
    operation: ExtremaOperation,
    arrays: &[ArrayRef],
    rows: usize,
    target: Option<&DataType>,
) -> Result<ArrayRef, String> {
    let raw = RefCell::new(None);
    let boundary = |message: &str| {
        *raw.borrow_mut() = Some(message.to_string());
        Err(KernelFailure::InstanceFailed)
    };
    let arguments = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    match evaluate_values(
        operation,
        &arguments,
        Selection::all(rows),
        target,
        true,
        &mut work,
        Some(&boundary),
    ) {
        Ok(output) => Ok(output),
        Err(failure) => Err(raw.into_inner().unwrap_or_else(|| legacy_failure(failure))),
    }
}
fn evaluate_values(
    operation: ExtremaOperation,
    args: &[EvaluatedArgument<'_>],
    selection: Selection<'_>,
    target: Option<&DataType>,
    legacy: bool,
    work: &mut EvaluationCheckpoints<'_>,
    boundary: ErrorBoundary<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let greatest = matches!(operation, ExtremaOperation::Greatest);
    let mut temporal_arguments = !args.is_empty();
    for arg in args {
        work.step()?;
        if !temporal(arg.array().data_type()) {
            temporal_arguments = false;
            break;
        }
    }
    if temporal_arguments {
        let mut values_per_arg = reserve(args.len(), work)?;
        for arg in args {
            work.step()?;
            let array = arg.array();
            let count = if legacy { array.len() } else { selection.len() };
            let mut decoded = reserve(count, work)?;
            if matches!(array.data_type(), DataType::Null) {
                for _ in 0..count {
                    work.step()?;
                    decoded.push(None);
                }
            } else {
                let reader = super::calendar_extended::DateInput::raw(array.as_ref())
                    .map_err(|e| raw_error(e.legacy_message(), boundary))?;
                for ordinal in 0..count {
                    work.step()?;
                    let row = if legacy {
                        ordinal
                    } else {
                        arg.value_row(ordinal, selection.row(ordinal).expect("selection ordinal"))
                    };
                    decoded.push(if array.is_null(row) {
                        None
                    } else {
                        reader.read(row, work)?
                    });
                }
            }
            values_per_arg.push(decoded);
        }
        let mut values = reserve(selection.len(), work)?;
        for ordinal in 0..selection.len() {
            work.step()?;
            let row = selection.row(ordinal).expect("selection ordinal");
            let mut acc: Option<NaiveDateTime> = None;
            for decoded in &values_per_arg {
                work.step()?;
                let value = if legacy {
                    datetime_value_at(decoded, row, selection.batch_rows())
                } else {
                    decoded.get(ordinal).copied().flatten()
                };
                match (acc, value) {
                    (Some(a), Some(b)) => {
                        acc = Some(if greatest {
                            if a >= b { a } else { b }
                        } else if a <= b {
                            a
                        } else {
                            b
                        })
                    }
                    (None, Some(b)) => acc = Some(b),
                    (_, None) => {
                        acc = None;
                        break;
                    }
                }
            }
            values.push(acc.map(crate::datetime_value::naive_to_timestamp_micros));
        }
        work.flush()?;
        let ts = Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef;
        work.flush()?;
        let fallback = DataType::Timestamp(TimeUnit::Microsecond, None);
        let output_type = target.unwrap_or(&fallback);
        if matches!(output_type, DataType::Utf8) {
            let ts = ts
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    raw_error(
                        "failed to downcast datetime extrema result".to_string(),
                        boundary,
                    )
                })?;
            let mut out = reserve(ts.len(), work)?;
            for row in 0..ts.len() {
                work.step()?;
                let value = if ts.is_null(row) {
                    None
                } else {
                    work.flush()?;
                    let value = crate::datetime_value::timestamp_to_naive(
                        &TimeUnit::Microsecond,
                        ts.value(row),
                    )
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string());
                    work.flush()?;
                    value
                };
                out.push(value);
            }
            work.flush()?;
            let out = Arc::new(StringArray::from(out)) as ArrayRef;
            work.flush()?;
            return Ok(out);
        }
        return cast(ts, Some(output_type), work, boundary);
    }
    let mut views = reserve(args.len(), work)?;
    for arg in args {
        work.step()?;
        views.push(NumericArrayView::new(arg.array()).map_err(|e| raw_error(e, boundary))?);
    }
    let mut values = reserve(selection.len(), work)?;
    for ordinal in 0..selection.len() {
        work.step()?;
        let row = selection.row(ordinal).expect("selection ordinal");
        let mut acc: Option<f64> = None;
        for (arg, view) in args.iter().zip(&views) {
            work.step()?;
            work.flush()?;
            let v = if legacy {
                value_at_f64(view, row, selection.batch_rows())
            } else {
                view.value_f64(arg.value_row(ordinal, row))
            }
            .filter(|v| v.is_finite());
            work.flush()?;
            match (acc, v) {
                (Some(a), Some(b)) => acc = Some(if greatest { a.max(b) } else { a.min(b) }),
                (None, Some(b)) => acc = Some(b),
                (_, None) => {
                    acc = None;
                    break;
                }
            }
        }
        values.push(acc);
    }
    work.flush()?;
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    work.flush()?;
    cast(out, target, work, boundary)
}
fn cast(
    out: ArrayRef,
    target: Option<&DataType>,
    work: &mut EvaluationCheckpoints<'_>,
    boundary: ErrorBoundary<'_>,
) -> Result<ArrayRef, KernelFailure> {
    work.flush()?;
    let out = cast_output_observed(out, target, &mut |event| match event {
        MathNumericObservation::Step => work.step(),
        MathNumericObservation::OpaqueBoundary => work.flush(),
    })
    .map_err(|error| match error {
        MathNumericError::Legacy(message) => raw_error(message, boundary),
        MathNumericError::Kernel(failure) => failure,
    })?;
    work.flush()?;
    Ok(out)
}
struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn wait(&self, _duration: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("extrema evaluation must not request a wait")
    }

    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
}
fn legacy_failure(failure: KernelFailure) -> String {
    match failure {
        KernelFailure::InvalidProgram(e)
        | KernelFailure::Internal(e)
        | KernelFailure::Operational(e) => e.message().to_string(),
        other => format!("extrema shared value calculation failed: {other:?}"),
    }
}

#[cfg(test)]
#[path = "scalar_extrema_tests.rs"]
mod tests;
