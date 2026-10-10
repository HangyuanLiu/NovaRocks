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
    collection_selected::{compact, reserve},
};
use crate::{
    KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput, SelectedValues,
    kernel_control::internal, kernel_input::EvaluationCheckpoints,
};
use arrow_array::{new_empty_array, new_null_array};
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
        let target = &input.contract().result_type().data_type;
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
        let result = array_difference_core::difference_observed(
            &array,
            Some(target),
            selection.len(),
            &mut observer,
        );
        let (values, errors) = match result {
            Ok(values) => (values, Box::default()),
            Err(DifferenceFailure::Control(cause)) => return Err(cause),
            // Normalizer errors precede all parent decisions in the original vector body.
            Err(DifferenceFailure::Data(message)) => {
                let mut errors = reserve::<RowDataError>(selection.len(), &work)?;
                for ordinal in 0..selection.len() {
                    work.borrow_mut().step()?;
                    errors.push(RowDataError::new(ordinal, &message));
                }
                (
                    new_null_array(target, selection.len()),
                    errors.into_boxed_slice(),
                )
            }
        };
        SelectedValues::try_new(selection, target, values, errors)
            .map_err(|_| internal("array difference selected result violated its exact contract"))
    })();
    if let Some(cause) = refusal.into_inner() {
        return Err(cause);
    }
    work.into_inner().finish_result(result)
}
