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
//! Exact selected addressing for the ONE original MAP_SIZE core.
use super::{
    array_literal_core::CollectionObservation,
    collection_offset_count::OffsetCountFailure,
    map_size_core::{count_observed, map_input},
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::cell::RefCell;
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let [FunctionArgumentType::Value(source)] = contract.selected().argument_types.as_ref() else {
        return Err(invalid("map_size requires its one exact value argument"));
    };
    observe()?;
    if source.logical_type() != ValueLogicalType::Physical
        || !matches!(source.data_type(), DataType::Map(_, _))
    {
        return Err(invalid(
            "map_size requires its exact frozen Physical Map source",
        ));
    }
    let result = contract.result_type();
    observe()?;
    if result.logical_type() != ValueLogicalType::Physical
        || result.data_type() != &DataType::Int32
        || !result.nullable()
    {
        return Err(invalid(
            "map_size requires its real nullable Physical Int32 result",
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
        let [argument] = input.arguments() else {
            return Err(invalid("map_size requires one evaluated argument"));
        };
        let map = map_input(argument.array().as_ref())?;
        work.borrow_mut().step()?;
        let selection = input.selection();
        let mut observer = |event| match event {
            CollectionObservation::Step => work.borrow_mut().step(),
            CollectionObservation::OpaqueBoundary => work.borrow_mut().flush(),
        };
        let values = count_observed(
            map,
            selection,
            |ordinal, row| {
                let address = argument.value_row(ordinal, row);
                if address >= argument.array().len() {
                    Err(internal("map_size mapping is outside its checked carrier"))
                } else {
                    Ok(address)
                }
            },
            &mut observer,
        )
        .map_err(|failure| match failure {
            OffsetCountFailure::Control(cause) | OffsetCountFailure::Operation(cause) => cause,
        })?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            input.contract().result_type().data_type(),
            values,
            Box::default(),
            || work.borrow_mut().step(),
        )
    })();
    work.into_inner().finish_result(result)
}
#[cfg(test)]
#[path = "map_size_selected_tests.rs"]
mod tests;
