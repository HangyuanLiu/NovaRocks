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

//! Original SPLIT computation shared by full-array v1 and selected calls.
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues, Selection,
};
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum CoreError {
    FirstStringRequired,
    SecondStringRequired,
    LengthMismatch,
    Kernel(KernelFailure),
}
impl From<KernelFailure> for CoreError {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FirstStringRequired => {
                f.write_str("split: first argument must be a string array")
            }
            Self::SecondStringRequired => {
                f.write_str("split: second argument must be a string array")
            }
            Self::LengthMismatch => f.write_str("split: argument length mismatch"),
            Self::Kernel(cause) => write!(f, "{cause}"),
        }
    }
}
fn sources<'a>(
    left: &'a dyn Array,
    right: &'a dyn Array,
) -> Result<(&'a StringArray, &'a StringArray), CoreError> {
    let left = left
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(CoreError::FirstStringRequired)?;
    let right = right
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(CoreError::SecondStringRequired)?;
    Ok((left, right))
}
fn evaluate_rows(
    left: &StringArray,
    right: &StringArray,
    selection: Selection<'_>,
    mut address: impl FnMut(usize, usize) -> (usize, usize),
    observe: &mut impl FnMut(Observation) -> Result<(), KernelFailure>,
) -> Result<ArrayRef, CoreError> {
    observe(Observation::OpaqueBoundary)?;
    let value_builder = StringBuilder::new();
    let mut list_builder = ListBuilder::new(value_builder);
    observe(Observation::OpaqueBoundary)?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let (row, delimiter_row) = address(ordinal, batch_row);
        observe(Observation::Step)?;
        if row >= left.len() || delimiter_row >= right.len() {
            return Err(internal("split selected address is out of bounds").into());
        }
        if left.is_null(row) || right.is_null(delimiter_row) {
            observe(Observation::OpaqueBoundary)?;
            list_builder.append(false);
            observe(Observation::OpaqueBoundary)?;
            continue;
        }
        let haystack = left.value(row);
        let delimiter = right.value(delimiter_row);
        if delimiter.is_empty() {
            for ch in haystack.chars() {
                observe(Observation::Step)?;
                let mut buf = [0u8; 4];
                let s = ch.encode_utf8(&mut buf);
                observe(Observation::OpaqueBoundary)?;
                list_builder.values().append_value(s);
                observe(Observation::OpaqueBoundary)?;
            }
            observe(Observation::OpaqueBoundary)?;
            list_builder.append(true);
            observe(Observation::OpaqueBoundary)?;
            continue;
        }
        let mut start = 0usize;
        loop {
            observe(Observation::OpaqueBoundary)?;
            let found = haystack[start..].find(delimiter);
            observe(Observation::OpaqueBoundary)?;
            let Some(pos) = found else {
                break;
            };
            let end = start + pos;
            observe(Observation::OpaqueBoundary)?;
            list_builder.values().append_value(&haystack[start..end]);
            observe(Observation::OpaqueBoundary)?;
            start = end + delimiter.len();
            observe(Observation::Step)?;
        }
        observe(Observation::OpaqueBoundary)?;
        list_builder.values().append_value(&haystack[start..]);
        list_builder.append(true);
        observe(Observation::OpaqueBoundary)?;
    }
    observe(Observation::OpaqueBoundary)?;
    let result = Arc::new(list_builder.finish()) as ArrayRef;
    observe(Observation::OpaqueBoundary)?;
    Ok(result)
}
pub fn evaluate_legacy(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    let (left, right) = sources(left.as_ref(), right.as_ref()).map_err(|e| e.to_string())?;
    if right.len() != left.len() {
        return Err(CoreError::LengthMismatch.to_string());
    }
    evaluate_rows(
        left,
        right,
        Selection::all(left.len()),
        |_, row| (row, row),
        &mut |_| Ok(()),
    )
    .map_err(|e| e.to_string())
}
pub fn evaluate_observed(
    left: &dyn Array,
    right: &dyn Array,
    selection: Selection<'_>,
    address: impl FnMut(usize, usize) -> (usize, usize),
    observe: &mut impl FnMut(Observation) -> Result<(), KernelFailure>,
) -> Result<ArrayRef, CoreError> {
    let (left, right) = sources(left, right)?;
    evaluate_rows(left, right, selection, address, observe)
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let types = &contract.selected().argument_types;
    if types.len() != 2 {
        return Err(invalid("split requires its two exact checked arguments"));
    }
    let mut nullable = false;
    for ty in types.iter() {
        step()?;
        let FunctionArgumentType::Value(source) = ty else {
            return Err(invalid("split requires value arguments"));
        };
        if source.logical_type != ValueLogicalType::Physical || source.data_type != DataType::Utf8 {
            return Err(invalid("split requires its exact Physical Utf8 arguments"));
        }
        nullable |= source.nullable;
    }
    let target = contract.result_type();
    step()?;
    if target.logical_type != ValueLogicalType::Physical
        || target.data_type != DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        || (nullable && !target.nullable)
    {
        return Err(invalid(
            "split differs from its exact List Utf8 result contract",
        ));
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(input.contract(), || work.step())?;
        if input.arguments().len() != 2 {
            return Err(invalid("split selected argument arity differs"));
        }
        let selection = input.selection();
        for (arg, ty) in input
            .arguments()
            .iter()
            .zip(input.contract().selected().argument_types.iter())
        {
            let FunctionArgumentType::Value(source) = ty else {
                unreachable!("validated values")
            };
            work.flush()?;
            let nulls = arg.array().logical_nulls();
            work.flush()?;
            for (ordinal, row) in selection.iter().enumerate() {
                let row = arg.value_row(ordinal, row);
                work.step()?;
                if row >= arg.array().len() {
                    return Err(internal("split selected address is out of bounds"));
                }
                if !source.nullable && nulls.as_ref().is_some_and(|n| n.is_null(row)) {
                    return Err(internal("split non-null selected source contains SQL NULL"));
                }
            }
        }
        let out = evaluate_observed(
            input.arguments()[0].array().as_ref(),
            input.arguments()[1].array().as_ref(),
            selection,
            |ordinal, row| {
                (
                    input.arguments()[0].value_row(ordinal, row),
                    input.arguments()[1].value_row(ordinal, row),
                )
            },
            &mut |event| match event {
                Observation::Step => work.step(),
                Observation::OpaqueBoundary => work.flush(),
            },
        )
        .map_err(|error| match error {
            CoreError::Kernel(cause) => cause,
            other => internal(&other.to_string()),
        })?;
        SelectedValues::try_new_observed(
            selection,
            &input.contract().result_type().data_type,
            out,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
#[cfg(test)]
#[path = "string_split_tests.rs"]
mod tests;
