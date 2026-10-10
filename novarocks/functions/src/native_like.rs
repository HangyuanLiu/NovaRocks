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
//! Original Unicode/backslash LIKE math. No arena, decoder or environment.
//! Observation grants no capacity; raw wrappers preserve full legacy errors.
use arrow_array::{Array, ArrayRef, BooleanArray, StringArray};
use std::{convert::Infallible, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub enum LikeObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum LikeError<E> {
    Data(&'static str),
    Host(E),
}
fn event<E>(
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
    e: LikeObservation,
) -> Result<(), LikeError<E>> {
    observe(e).map_err(LikeError::Host)
}
fn legacy<T>(
    run: impl FnOnce(
        &mut dyn FnMut(LikeObservation) -> Result<(), Infallible>,
    ) -> Result<T, LikeError<Infallible>>,
) -> Result<T, String> {
    match run(&mut |_| Ok(())) {
        Ok(v) => Ok(v),
        Err(LikeError::Data(s)) => Err(s.into()),
        Err(LikeError::Host(never)) => match never {},
    }
}
pub fn like_match(text: &str, pattern: &str) -> bool {
    legacy(|observe| like_match_observed(text, pattern, observe)).unwrap()
}
pub fn like_match_observed<E>(
    text: &str,
    pattern: &str,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    event(observe, LikeObservation::OpaqueBoundary)?;
    // One original chars decoder; observation is injected into this iterator.
    let text_chars: Vec<char> = text
        .chars()
        .map(|c| {
            event(observe, LikeObservation::Step)?;
            Ok(c)
        })
        .collect::<Result<_, LikeError<E>>>()?;
    event(observe, LikeObservation::OpaqueBoundary)?;
    let pattern_chars: Vec<char> = pattern
        .chars()
        .map(|c| {
            event(observe, LikeObservation::Step)?;
            Ok(c)
        })
        .collect::<Result<_, LikeError<E>>>()?;
    event(observe, LikeObservation::OpaqueBoundary)?;
    like_match_recursive(&text_chars, 0, &pattern_chars, 0, observe)
}
fn match_literal<E>(
    text: &[char],
    text_idx: usize,
    pattern: &[char],
    next_pattern_idx: usize,
    literal: char,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    if text_idx >= text.len() || text[text_idx] != literal {
        return Ok(false);
    }
    like_match_recursive(text, text_idx + 1, pattern, next_pattern_idx, observe)
}
fn like_match_recursive<E>(
    text: &[char],
    text_idx: usize,
    pattern: &[char],
    pattern_idx: usize,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    event(observe, LikeObservation::Step)?;
    if pattern_idx >= pattern.len() {
        return Ok(text_idx >= text.len());
    }
    match pattern[pattern_idx] {
        '%' => {
            if like_match_recursive(text, text_idx, pattern, pattern_idx + 1, observe)? {
                return Ok(true);
            }
            for i in text_idx..text.len() {
                if like_match_recursive(text, i + 1, pattern, pattern_idx + 1, observe)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        '_' => {
            if text_idx >= text.len() {
                return Ok(false);
            }
            like_match_recursive(text, text_idx + 1, pattern, pattern_idx + 1, observe)
        }
        '\\' => {
            if pattern_idx + 1 >= pattern.len() {
                return match_literal(text, text_idx, pattern, pattern_idx + 1, '\\', observe);
            }
            let escaped = pattern[pattern_idx + 1];
            match_literal(text, text_idx, pattern, pattern_idx + 2, escaped, observe)
        }
        c => match_literal(text, text_idx, pattern, pattern_idx + 1, c, observe),
    }
}
fn row_observed<E>(
    text: &StringArray,
    text_row: usize,
    pattern: &StringArray,
    pattern_row: usize,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<Option<bool>, LikeError<E>> {
    event(observe, LikeObservation::Step)?;
    if text.is_null(text_row) || pattern.is_null(pattern_row) {
        return Ok(None);
    }
    Ok(Some(like_match_observed(
        text.value(text_row),
        pattern.value(pattern_row),
        observe,
    )?))
}
pub fn evaluate_legacy(text: &ArrayRef, pattern: &ArrayRef) -> Result<ArrayRef, String> {
    legacy(|observe| evaluate_observed(text, pattern, observe))
}
pub fn evaluate_observed<E>(
    text: &ArrayRef,
    pattern: &ArrayRef,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<ArrayRef, LikeError<E>> {
    let len = text.len();
    let text = text
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(LikeError::Data(
            "like: first argument must be a string array",
        ))?;
    let pattern = pattern
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(LikeError::Data(
            "like: second argument must be a string array",
        ))?;
    event(observe, LikeObservation::OpaqueBoundary)?;
    let result_values: Vec<Option<bool>> = (0..len)
        .map(|i| row_observed(text, i, pattern, i, observe))
        .collect::<Result<_, _>>()?;
    event(observe, LikeObservation::OpaqueBoundary)?;
    let result = BooleanArray::from_iter(result_values);
    event(observe, LikeObservation::OpaqueBoundary)?;
    Ok(Arc::new(result))
}

/// Exact already-bound Utf8-only original native operator domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedNativeLikeRecipe {
    negated: bool,
    text: novarocks_type_contract::FunctionValueType,
    pattern: novarocks_type_contract::FunctionValueType,
    result: novarocks_type_contract::FunctionValueType,
}
impl PreparedNativeLikeRecipe {
    pub fn try_new(
        negated: bool,
        text: &novarocks_type_contract::FunctionValueType,
        pattern: &novarocks_type_contract::FunctionValueType,
        result: &novarocks_type_contract::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, crate::ComparisonPrepareError> {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueLogicalType};
        use arrow_schema::DataType;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome = (|| {
            for ty in [text, pattern, result] {
                crate::kernel_input::validate_type_observed(ty, &mut work)
                    .map_err(crate::ComparisonPrepareError::Kernel)?;
                work.step()?;
            }
            if [text, pattern].iter().any(|ty| {
                ty.logical_type != ValueLogicalType::Physical || ty.data_type != DataType::Utf8
            }) {
                return Err(crate::ComparisonPrepareError::Unsupported);
            }
            if result.logical_type != ValueLogicalType::Physical
                || result.data_type != DataType::Boolean
                || ((text.nullable || pattern.nullable) && !result.nullable)
            {
                return Err(crate::ComparisonPrepareError::TypeMismatch);
            }
            work.flush()?;
            Ok(Self {
                negated,
                text: text.clone(),
                pattern: pattern.clone(),
                result: result.clone(),
            })
        })();
        if outcome
            .as_ref()
            .err()
            .is_some_and(|e: &crate::ComparisonPrepareError| e.control_error().is_some())
        {
            return outcome;
        }
        work.finish()?;
        outcome
    }
    pub fn negated(&self) -> bool {
        self.negated
    }
    pub fn text_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.text
    }
    pub fn pattern_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.pattern
    }
    pub fn result_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.result
    }
    pub fn own_effects(
        &self,
        context: novarocks_type_contract::ExpressionEffectContext,
    ) -> crate::ScopedExpressionEffects {
        crate::ScopedExpressionEffects::pure_value(context)
    }
    pub fn evaluate_selected<'a>(
        &self,
        text: crate::EvaluatedArgument<'a>,
        pattern: crate::EvaluatedArgument<'a>,
        selection: crate::Selection<'a>,
        truth_only: bool,
        control: &dyn crate::KernelEvaluationControl,
    ) -> Result<crate::SelectedValues<'a>, crate::KernelFailure> {
        use crate::{EvaluatedArgument, KernelEvaluationControl, KernelFailure};
        use crate::kernel_control::{invalid, internal};
        let observed = crate::KernelControlObservation::new(control);
        observed.checkpoint(0)?;
        let mut work = crate::EvaluationCheckpoints::new(&observed);
        let outcome = (|| {
            for (arg, expected) in [(text, &self.text), (pattern, &self.pattern)] {
                // Header identity belongs to the checked invocation even when
                // every demanded row carries an inherited error placeholder.
                if arg.array().data_type() != &expected.data_type
                    || !arg.array().as_any().is::<StringArray>()
                {
                    return Err(invalid("LIKE exact Utf8 carrier is absent"));
                }
                work.step()?;
                if let EvaluatedArgument::Constant(value) = arg {
                    if value.value_type().logical_type != expected.logical_type
                        || (value.value_type().nullable && !expected.nullable)
                    {
                        return Err(invalid("LIKE constant differs from its exact logical type"));
                    }
                    work.step()?;
                }
                if let EvaluatedArgument::SelectedColumn(value) = arg {
                    if !value
                        .selection()
                        .same_rows_observed(selection, || work.step())?
                    {
                        return Err(invalid(
                            "LIKE compact argument has a foreign selected domain",
                        ));
                    }
                } else {
                    arg.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
                }
            }
            if selection.len() == 0 {
                crate::kernel_input::validate_argument_observed(
                    text, selection, &self.text, &observed,
                )?;
                crate::kernel_input::validate_argument_observed(
                    pattern,
                    selection,
                    &self.pattern,
                    &observed,
                )?;
            }
            let mut left_errors = match text {
                EvaluatedArgument::SelectedColumn(v) => v.errors(),
                _ => &[],
            }
            .iter()
            .peekable();
            let mut right_errors = match pattern {
                EvaluatedArgument::SelectedColumn(v) => v.errors(),
                _ => &[],
            }
            .iter()
            .peekable();
            let mut errors = Vec::new();
            let mut values = Vec::with_capacity(selection.len());
            work.flush()?;
            for (ordinal, row) in selection.iter().enumerate() {
                work.step()?;
                let left = if left_errors
                    .peek()
                    .is_some_and(|e| e.selected_ordinal() == ordinal)
                {
                    left_errors.next()
                } else {
                    None
                };
                let right = if right_errors
                    .peek()
                    .is_some_and(|e| e.selected_ordinal() == ordinal)
                {
                    right_errors.next()
                } else {
                    None
                };
                if let Some(error) = left.or(right) {
                    errors.push(error.clone());
                    values.push(None);
                    work.step()?;
                    continue;
                }
                for (arg, ty) in [(text, &self.text), (pattern, &self.pattern)] {
                    crate::arithmetic::checked_row(
                        arg,
                        ordinal,
                        row,
                        ty,
                        |array| {
                            if array.as_any().is::<StringArray>() {
                                Ok(())
                            } else {
                                Err(invalid("LIKE exact Utf8 carrier is absent"))
                            }
                        },
                        &mut work,
                    )?;
                }
                let left = text
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("LIKE source carrier differs"))?;
                let right = pattern
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("LIKE pattern carrier differs"))?;
                let value = row_observed(
                    left,
                    text.value_row(ordinal, row),
                    right,
                    pattern.value_row(ordinal, row),
                    &mut |event| match event {
                        LikeObservation::Step => work.step(),
                        LikeObservation::OpaqueBoundary => work.flush(),
                    },
                )
                .map_err(|error| match error {
                    LikeError::Host(cause) => cause,
                    LikeError::Data(message) => internal(message),
                })?;
                values.push(value);
                work.step()?;
            }
            work.flush()?;
            let output = BooleanArray::from_iter(values);
            work.flush()?;
            // The original native negated expansion invokes this same Arrow
            // Boolean NOT author after the complete positive LIKE result.
            let output = if self.negated {
                let out =
                    arrow_arith::boolean::not(&output).map_err(|e| internal(&e.to_string()))?;
                work.flush()?;
                out
            } else {
                output
            };
            let output = if truth_only {
                let mut failed = errors.iter().peekable();
                let mut rows = Vec::with_capacity(output.len());
                for (ordinal, value) in output.iter().enumerate() {
                    work.step()?;
                    rows.push(
                        if failed
                            .peek()
                            .is_some_and(|error| error.selected_ordinal() == ordinal)
                        {
                            failed.next();
                            None
                        } else {
                            Some(value.unwrap_or(false))
                        },
                    );
                }
                work.flush()?;
                let output = BooleanArray::from_iter(rows);
                work.flush()?;
                output
            } else {
                output
            };
            crate::SelectedValues::try_new_observed(
                selection,
                &self.result.data_type,
                Arc::new(output),
                errors.into_boxed_slice(),
                || work.step(),
            )
        })();
        observed.finish(work.finish_result(outcome))
    }
}
#[cfg(test)]
#[path = "native_like_tests.rs"]
mod tests;
