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
//! Exact selected demand, real row data failures and shared copy admission.
use super::{
    array_literal_core::CollectionObservation,
    collection_selected::{copy_error, reserve},
    map_projection_core::{MapPart, ProjectionFailure, project_observed},
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallContract,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    selected_copy,
};
use arrow_array::{Array, MapArray};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, cell::RefCell};
pub(super) fn validate_profile(
    part: MapPart,
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let [FunctionArgumentType::Value(source)] = contract.selected().argument_types.as_ref() else {
        return Err(invalid(
            "map projection requires its one exact value argument",
        ));
    };
    observe()?;
    if source.logical_type != ValueLogicalType::Physical {
        return Err(invalid(
            "map projection requires its exact Physical Map source",
        ));
    }
    let DataType::Map(entries, _) = &source.data_type else {
        return Err(invalid(
            "map projection requires its frozen Map source fields",
        ));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(invalid("map projection requires its real Struct entries"));
    };
    observe()?;
    if fields.len() != 2 {
        return Err(invalid(
            "map projection requires its two real key/value fields",
        ));
    }
    let selected = match part {
        MapPart::Keys => &fields[0],
        MapPart::Values => &fields[1],
    };
    let result = contract.result_type();
    let DataType::List(item) = &result.data_type else {
        return Err(invalid(
            "map projection requires its real frozen List result field",
        ));
    };
    observe()?;
    if result.logical_type != ValueLogicalType::Physical || !result.nullable {
        return Err(invalid(
            "map projection requires its real nullable Physical List result",
        ));
    }
    // Read the already validated field tag through its sole neutral author, then
    // compare frozen nested data types with the existing observed author.
    let source_tag = novarocks_type_contract::field_logical_type(selected)
        .map_err(|_| invalid("map projection source field has invalid logical identity"))?;
    let result_tag = novarocks_type_contract::field_logical_type(item)
        .map_err(|_| invalid("map projection result field has invalid logical identity"))?;
    observe()?;
    if source_tag != result_tag
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            selected.data_type(),
            item.data_type(),
            &mut observe,
        )?
    {
        return Err(invalid(
            "map projection result differs from its actual selected key/value domain",
        ));
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    part: MapPart,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let result = (|| {
        validate_profile(part, input.contract(), || work.borrow_mut().step())?;
        let [argument] = input.arguments() else {
            return Err(invalid(
                "map projection requires one evaluated Map argument",
            ));
        };
        let map = argument
            .array()
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| internal("map projection exact Map source has foreign carrier class"))?;
        let selection = input.selection();
        let result = input.contract().result_type();
        let DataType::List(field) = &result.data_type else {
            unreachable!("validated result")
        };
        Layout::array::<i32>(
            selection
                .len()
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?,
        )
        .map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut errors = reserve::<RowDataError>(selection.len(), &work)?;
        work.borrow_mut().flush()?;
        let nulls = RefCell::new(NullBufferBuilder::new(selection.len()));
        work.borrow_mut().flush()?;
        let mut observer = |e| match e {
            CollectionObservation::Step => work.borrow_mut().step(),
            CollectionObservation::OpaqueBoundary => work.borrow_mut().flush(),
        };
        let mut row_error = |ordinal, text: String| {
            work.borrow_mut().step()?;
            errors.push(RowDataError::new(ordinal, &text));
            Ok(())
        };
        let values = project_observed(
            map,
            part,
            field.clone(),
            selection,
            |ordinal, row| {
                let row = argument.value_row(ordinal, row);
                if row >= map.len() {
                    Err(internal(
                        "map projection mapping is outside its checked carrier",
                    ))
                } else {
                    Ok(row)
                }
            },
            Some(&mut row_error),
            |_, _, is_null| {
                work.borrow_mut().step()?;
                if is_null {
                    nulls.borrow_mut().append_null()
                } else {
                    nulls.borrow_mut().append_non_null()
                }
                Ok(())
            },
            || nulls.borrow_mut().finish(),
            |values, indices| {
                let mut exact = reserve::<Option<u64>>(indices.len(), &work)?;
                for &index in indices {
                    work.borrow_mut().step()?;
                    exact.push(Some(u64::from(index)));
                }
                selected_copy::preflight_take(values, &exact, |boundary| {
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
        .map_err(|e| match e {
            ProjectionFailure::Control(cause) => cause,
            ProjectionFailure::Data(text) => internal(&text),
            ProjectionFailure::Take(text) => internal(&text),
        })?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &result.data_type,
            values,
            errors.into_boxed_slice(),
            || work.borrow_mut().step(),
        )
    })();
    work.into_inner().finish_result(result)
}
#[cfg(test)]
#[path = "map_projection_selected_tests.rs"]
mod tests;
