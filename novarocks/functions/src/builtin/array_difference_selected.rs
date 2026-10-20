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

//! Reuse exact selected collection projection and the original cast and adjacent subtraction algorithm.
use super::{
    array_difference_core::{self, DifferenceFailure, Operation},
    array_literal_core::CollectionObservation,
    collection_selected::compact,
};
use crate::{
    KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput, SelectedValues,
    kernel_control::internal, kernel_input::EvaluationCheckpoints,
};
use arrow_array::new_empty_array;
use std::cell::RefCell;
pub(super) fn evaluate<'a>(
    _operation: Operation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let refusal: RefCell<Option<KernelFailure>> = RefCell::new(None);
    let result = (|| {
        let [argument] = input.arguments() else {
            return Err(internal(
                "array difference exact invocation requires one evaluated argument",
            ));
        };
        let selection = input.selection();
        let target = input.contract().result_type().data_type();
        if selection.is_empty() {
            return SelectedValues::try_new(
                selection,
                target,
                new_empty_array(target),
                Box::default(),
            )
            .map_err(|_| internal("array difference empty result violated its exact type"));
        }
        let array = compact(*argument, selection, &work)?;
        let mut observer = |event| {
            if let Some(cause) = refusal.borrow().as_ref() {
                return Err(cause.clone());
            }
            let result = match event {
                CollectionObservation::Step => work.borrow_mut().step(),
                CollectionObservation::OpaqueBoundary => control.checkpoint(0),
            };
            if let Err(cause) = &result {
                *refusal.borrow_mut() = Some(cause.clone());
            }
            result
        };
        let mut errors = Vec::new();
        let values = array_difference_core::difference_observed(
            &array,
            Some(target),
            selection.len(),
            &mut observer,
            &mut |cause| {
                // Only this explicit arithmetic row outcome may enter a journal.
                // SDK, shape, allocation and format failures use Operation.
                work.borrow_mut().step()?;
                if errors.len() == errors.capacity() {
                    work.borrow_mut().flush()?;
                    errors.try_reserve(1).map_err(|source| {
                        KernelFailure::from_domain(
                            crate::allocation_failure::AllocationRefusal::new(
                                "array_difference row journal",
                                source,
                            ),
                        )
                    })?;
                    work.borrow_mut().flush()?;
                }
                errors.push(RowDataError::with_reason(
                    cause.row,
                    crate::DataFailureReason::NumericOutOfRange,
                    &cause.to_string(),
                ));
                Ok(())
            },
        )
        .map_err(|source| match source {
            DifferenceFailure::Operation(source) | DifferenceFailure::Caller(source) => source,
        })?;
        let errors = errors.into_boxed_slice();
        SelectedValues::try_new(selection, target, values, errors)
            .map_err(|_| internal("array difference selected result violated its exact contract"))
    })();
    if let Some(cause) = refusal.into_inner() {
        return Err(cause);
    }
    work.into_inner().finish_result(result)
}
