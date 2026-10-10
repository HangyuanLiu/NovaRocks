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

//! Original NULL_OR_EMPTY computation shared by full-array v1 and selected calls.
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues, Selection,
};
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, NullArray, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum CoreError {
    Unsupported(DataType),
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
            Self::Unsupported(ty) => write!(f, "null_or_empty expects string or array, got {ty:?}"),
            Self::Kernel(cause) => write!(f, "{cause}"),
        }
    }
}
enum Source<'a> {
    Null,
    Text(&'a StringArray),
    List(&'a ListArray),
    AllNull,
}
fn source(input: &dyn Array) -> Result<Source<'_>, CoreError> {
    if input.as_any().downcast_ref::<NullArray>().is_some() {
        return Ok(Source::Null);
    }
    if let Some(array) = input.as_any().downcast_ref::<StringArray>() {
        return Ok(Source::Text(array));
    }
    if let Some(array) = input.as_any().downcast_ref::<ListArray>() {
        return Ok(Source::List(array));
    }
    // This is the original whole-array fact, including empty unsupported arrays.
    // Pure preparation does not admit these unstable unsupported profiles.
    if input.null_count() == input.len() {
        return Ok(Source::AllNull);
    }
    Err(CoreError::Unsupported(input.data_type().clone()))
}
pub fn evaluate_observed(
    input: &dyn Array,
    selection: Selection<'_>,
    mut address: impl FnMut(usize, usize) -> usize,
    observe: &mut impl FnMut(Observation) -> Result<(), KernelFailure>,
) -> Result<ArrayRef, CoreError> {
    observe(Observation::OpaqueBoundary)?;
    let source = source(input)?;
    observe(Observation::OpaqueBoundary)?;
    observe(Observation::OpaqueBoundary)?;
    let mut out = Vec::with_capacity(selection.len());
    observe(Observation::OpaqueBoundary)?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let row = address(ordinal, batch_row);
        observe(Observation::Step)?;
        if row >= input.len() {
            return Err(internal("null_or_empty selected address is out of bounds").into());
        }
        let value = match &source {
            Source::Null | Source::AllNull => true,
            Source::Text(array) => {
                if array.is_null(row) {
                    true
                } else {
                    array.value(row).is_empty()
                }
            }
            Source::List(array) => {
                if array.is_null(row) {
                    true
                } else {
                    observe(Observation::OpaqueBoundary)?;
                    let empty = array.value(row).is_empty();
                    observe(Observation::OpaqueBoundary)?;
                    empty
                }
            }
        };
        out.push(Some(value));
        observe(Observation::Step)?;
    }
    observe(Observation::OpaqueBoundary)?;
    let result = Arc::new(BooleanArray::from(out)) as ArrayRef;
    observe(Observation::OpaqueBoundary)?;
    Ok(result)
}
pub fn evaluate_legacy(input: &ArrayRef) -> Result<ArrayRef, String> {
    evaluate_observed(
        input.as_ref(),
        Selection::all(input.len()),
        |_, row| row,
        &mut |_| Ok(()),
    )
    .map_err(|e| e.to_string())
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let [FunctionArgumentType::Value(source)] = contract.selected().argument_types.as_ref() else {
        return Err(invalid(
            "null_or_empty requires its one exact value argument",
        ));
    };
    step()?;
    let exact = match (&source.logical_type, &source.data_type) {
        (ValueLogicalType::Physical, DataType::Null | DataType::Utf8 | DataType::List(_)) => true,
        (ValueLogicalType::Json, DataType::Utf8) => true,
        _ => false,
    };
    if !exact {
        return Err(invalid(
            "null_or_empty source has no stable exact string or List profile",
        ));
    }
    let target = contract.result_type();
    step()?;
    if target.logical_type != ValueLogicalType::Physical || target.data_type != DataType::Boolean {
        return Err(invalid(
            "null_or_empty differs from its exact Boolean result contract",
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
        if input.arguments().len() != 1 {
            return Err(invalid("null_or_empty selected argument arity differs"));
        }
        let selection = input.selection();
        let arg = &input.arguments()[0];
        let FunctionArgumentType::Value(source) = &input.contract().selected().argument_types[0]
        else {
            unreachable!("validated value")
        };
        work.flush()?;
        let nulls = arg.array().logical_nulls();
        work.flush()?;
        for (ordinal, row) in selection.iter().enumerate() {
            let row = arg.value_row(ordinal, row);
            work.step()?;
            if row >= arg.array().len() {
                return Err(internal("null_or_empty selected address is out of bounds"));
            }
            if !source.nullable && nulls.as_ref().is_some_and(|n| n.is_null(row)) {
                return Err(internal(
                    "null_or_empty non-null selected source contains SQL NULL",
                ));
            }
        }
        let out = evaluate_observed(
            arg.array().as_ref(),
            selection,
            |ordinal, row| arg.value_row(ordinal, row),
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
#[path = "string_null_or_empty_tests.rs"]
mod tests;
