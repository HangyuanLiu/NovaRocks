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

//! Exact selected addressing for the ONE original array append core.
use super::{
    array_append_core::{
        AppendFailure, AppendOutputFacts, AppendRows, ArrayAppendInputs, append_observed,
    },
    array_literal_core::CollectionObservation,
    collection_selected::copy_error,
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    selected_copy,
};
use arrow_schema::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::{alloc::Layout, cell::RefCell};
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let [
        FunctionArgumentType::Value(list),
        FunctionArgumentType::Value(value),
    ] = contract.selected().argument_types.as_ref()
    else {
        return Err(invalid(
            "array_append requires its two exact value arguments",
        ));
    };
    observe()?;
    let result = contract.result_type();
    if list.logical_type != ValueLogicalType::Physical
        || result.logical_type != ValueLogicalType::Physical
        || !result.nullable
    {
        return Err(invalid(
            "array_append requires a Physical List and its real nullable List result",
        ));
    }
    let (DataType::List(source), DataType::List(output)) = (&list.data_type, &result.data_type)
    else {
        return Err(invalid(
            "array_append requires exact frozen List source/result fields",
        ));
    };
    let source = FunctionValueType::try_from_field(source)
        .map_err(|_| invalid("array_append has an invalid frozen source item identity"))?;
    let output = FunctionValueType::try_from_field(output)
        .map_err(|_| invalid("array_append has an invalid frozen result item identity"))?;
    if source.logical_type != value.logical_type
        || source.logical_type != output.logical_type
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            &source.data_type,
            &value.data_type,
            &mut observe,
        )?
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            &source.data_type,
            &output.data_type,
            &mut observe,
        )?
    {
        return Err(invalid(
            "array_append requires already-materialized exact item arguments and result",
        ));
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let result = (|| {
        validate_profile(input.contract(), || work.borrow_mut().step())?;
        let result = input.contract().result_type();
        let DataType::List(field) = &result.data_type else {
            unreachable!("checked List result")
        };
        let [list, target] = input.arguments() else {
            return Err(invalid("array_append requires two evaluated arguments"));
        };
        let selection = input.selection();
        let offsets = selection
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        Layout::array::<i32>(offsets).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut observer = |event| match event {
            CollectionObservation::Step => work.borrow_mut().step(),
            CollectionObservation::OpaqueBoundary => work.borrow_mut().flush(),
        };
        let inputs = ArrayAppendInputs::new(
            list.array().clone(),
            target.array().clone(),
            AppendOutputFacts::FrozenList(field),
            &mut observer,
        )
        .map_err(failure)?;
        let values = append_observed(
            &inputs,
            selection,
            |ordinal, row| {
                let list_row = list.value_row(ordinal, row);
                let target_row = target.value_row(ordinal, row);
                if list_row >= list.array().len() || target_row >= target.array().len() {
                    return Err(internal(
                        "array_append mapping is outside its checked carriers",
                    ));
                }
                Ok(AppendRows {
                    list: list_row,
                    target: target_row,
                })
            },
            |source, start, len, current| {
                let current =
                    usize::try_from(current).map_err(|_| KernelFailure::ResourceExhausted)?;
                let capacity = current
                    .checked_add(len)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                i32::try_from(capacity).map_err(|_| KernelFailure::ResourceExhausted)?;
                // This checks the actual single extension. Combined recursive multi-source
                // payload representability remains the explicit shared-copy obligation.
                selected_copy::preflight_extend(source, start, len, 0, capacity, |boundary| {
                    if boundary {
                        work.borrow_mut().flush()
                    } else {
                        work.borrow_mut().step()
                    }
                })
                .map_err(copy_error)
            },
            &mut observer,
        )
        .map_err(failure)?;
        SelectedValues::try_new(selection, &result.data_type, values, Box::default())
            .map_err(|_| internal("array_append compact output violates its frozen contract"))
    })();
    work.into_inner().finish_result(result)
}
fn failure(error: AppendFailure<KernelFailure>) -> KernelFailure {
    match error {
        AppendFailure::Control(cause) => cause,
        AppendFailure::Data(message) => internal(&message),
    }
}
#[cfg(test)]
#[path = "array_append_selected_tests.rs"]
mod tests;
