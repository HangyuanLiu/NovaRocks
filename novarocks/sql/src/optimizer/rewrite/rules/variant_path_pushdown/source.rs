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

//! Move actual request data at the original descriptor creation boundary.
//! Retention does not certify source provenance or interpret path grammar.

use std::{alloc::Layout, sync::Arc};

use arrow::datatypes::DataType;
use novarocks_functions::{
    ConstantError, ConstantValue, FunctionArgument, MAX_CALL_EFFECT_ARGUMENTS,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionKind, FunctionValueType, PureCompileControl,
};

use crate::{
    binding::{
        LogicalCallArgumentCaptureError, SqlFunctionBinding, move_authored_call_arguments_observed,
    },
    common::{LiteralValue, variant_source::DerivedVariantSource},
    compiler::SqlCompileError,
    optimizer::scalar::{ScalarArena, ScalarId, ScalarNode},
};

/// The caller supplies a valid original arena call and accepted canonical
/// descriptor text. Its source and allocation admission remain caller-owned.
pub(super) fn capture_variant_source_observed(
    arena: &ScalarArena,
    call: ScalarId,
    binding: &SqlFunctionBinding,
    canonical_path: &str,
    type_literal: &str,
    control: &dyn PureCompileControl,
) -> Result<Arc<DerivedVariantSource>, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let original = arena.node(call);
        work.step()?;
        let ScalarNode::FunctionCall {
            args,
            distinct,
            binding: original_binding,
            ..
        } = original
        else {
            return Err(invalid("variant source is not an original scalar call"));
        };
        let same_binding = std::ptr::eq(original_binding.resolved(), binding.resolved());
        work.step()?;
        if !same_binding {
            return Err(invalid(
                "variant source binding is not its original immutable loan",
            ));
        }
        let exact_count = args.len() == 3
            && args.len() <= MAX_CALL_EFFECT_ARGUMENTS
            && !*distinct
            && binding.kind == FunctionKind::Scalar
            && binding.logical_argument_count == 3
            && binding.selected.argument_types.len() == 3;
        work.step()?;
        if !exact_count {
            return Err(invalid("variant source has inconsistent original channels"));
        }

        // Retain the existing nominal/string/CV source gate. The shared scalar
        // argument author below must not bypass this reader's complete FVT check.
        for &argument in &args[1..] {
            let selected = super::string_literal_value_scalar(arena, argument, &mut work);
            if selected.as_ref().is_err_and(is_control) {
                return Err(selected.unwrap_err());
            }
            work.step()?;
            if selected?.is_none() {
                return Err(invalid(
                    "variant path or type source is not an accepted string constant",
                ));
            }
        }

        let request_layout = Layout::array::<FunctionArgument>(3);
        work.step()?;
        request_layout.map_err(|_| SqlCompileError::ResourceExhausted)?;
        work.flush()?;
        let mut arguments = Vec::new();
        arguments
            .try_reserve_exact(3)
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        work.step()?;
        work.flush()?;
        for &argument in args {
            let authored =
                crate::optimizer::scalar::function_argument(arena, argument, work.control());
            if authored.as_ref().is_err_and(is_control) {
                return Err(authored.unwrap_err());
            }
            work.step()?;
            arguments.push(authored?);
            work.step()?;
            work.flush()?;
        }
        let emission_type = FunctionValueType::new(DataType::Utf8, false);
        let path = emission_constant(
            &arguments[1],
            canonical_path,
            &emission_type,
            arena,
            &mut work,
        )?;
        let target = emission_constant(
            &arguments[2],
            type_literal,
            &emission_type,
            arena,
            &mut work,
        )?;
        work.flush()?;
        let captured = move_authored_call_arguments_observed(
            binding,
            3,
            arguments,
            arena.constant_policy(),
            work.control(),
        )
        .map_err(capture_error);
        if captured.as_ref().is_err_and(is_control) {
            return Err(captured.unwrap_err());
        }
        work.step()?;
        let captured = captured?;
        work.flush()?;
        let source = DerivedVariantSource::new_observed(captured, path, target, work.control())?;
        work.step()?;
        work.flush()?;
        let source = Arc::new(source);
        work.step()?;
        Ok(source)
    })();
    if result.as_ref().is_err_and(is_control) {
        return result;
    }
    work.finish()?;
    result
}

fn emission_constant(
    original: &FunctionArgument,
    text: &str,
    expected: &FunctionValueType,
    arena: &ScalarArena,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantValue, SqlCompileError> {
    if let FunctionArgument::Value {
        constant: Some(value),
        ..
    } = original
    {
        work.flush()?;
        let same_type = value
            .value_type()
            .exactly_equals_observed::<ConstantError>(expected, || {
                work.step().map_err(ConstantError::from)
            })
            .map_err(SqlCompileError::from)?;
        work.flush()?;
        if same_type {
            let selected = value
                .try_utf8_borrowed_observed(CompilePhase::FunctionSpecialization, work.control())
                .map_err(SqlCompileError::from)?;
            work.step()?;
            work.flush()?;
            if let Some(selected) = selected
                && text_equal(selected, text, work)?
            {
                let value = value.clone();
                work.step()?;
                return Ok(value);
            }
        }
    }
    // This is a distinct canonical emission value, not a re-admission of the
    // original request CV. Check the actual String representation before copy.
    let representable = text.len() <= i32::MAX as usize;
    work.step()?;
    if !representable {
        return Err(SqlCompileError::ResourceExhausted);
    }
    let request_layout = Layout::array::<u8>(text.len());
    work.step()?;
    request_layout.map_err(|_| SqlCompileError::ResourceExhausted)?;
    work.flush()?;
    let mut copied = String::new();
    copied
        .try_reserve_exact(text.len())
        .map_err(|_| SqlCompileError::ResourceExhausted)?;
    work.step()?;
    for character in text.chars() {
        // A Unicode scalar copies at most four bytes within the reserved extent.
        copied.push(character);
        work.step()?;
    }
    let literal = LiteralValue::String(copied);
    work.step()?;
    work.flush()?;
    let value = crate::constant::admit_syntax_constant(
        &literal,
        expected,
        arena.constant_policy(),
        work.control(),
    )
    .map_err(SqlCompileError::from);
    if value.as_ref().is_err_and(is_control) {
        return Err(value.unwrap_err());
    }
    work.step()?;
    work.flush()?;
    value
}

fn text_equal(
    actual: &str,
    expected: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    let same_length = actual.len() == expected.len();
    work.step()?;
    if !same_length {
        return Ok(false);
    }
    for (actual, expected) in actual.bytes().zip(expected.bytes()) {
        let equal = actual == expected;
        work.step()?;
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn capture_error(error: LogicalCallArgumentCaptureError) -> SqlCompileError {
    match error {
        LogicalCallArgumentCaptureError::Control(cause) => cause.into(),
        LogicalCallArgumentCaptureError::Binding(error) => error.into(),
        LogicalCallArgumentCaptureError::InvalidSource(detail) => invalid(detail),
    }
}

fn invalid(detail: &str) -> SqlCompileError {
    SqlCompileError::InvalidRequest(detail.to_owned())
}

fn is_control(error: &SqlCompileError) -> bool {
    matches!(
        error,
        SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted
    )
}
