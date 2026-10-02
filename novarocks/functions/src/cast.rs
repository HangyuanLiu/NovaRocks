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

//! Immutable selected casts for the exact Physical signed scalar domain.
//! The caller retains original policies and full types; this recipe performs no
//! output allocation, registry lookup, coercion or memory admission.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, logical_is_null, validate_type_observed};
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError,
    ScopedExpressionEffects,
};
use arrow_array::{Array, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow_cast::cast::num_cast;
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    ExpressionEffectContext, ExpressionEffects, FunctionValueType, PureCompileControl,
    ValueLogicalType,
};
use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CastOperation {
    Carrier,
    Time,
    TimeFromDatetime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CastPrepareError {
    Control(CompileControlError),
    Kernel(KernelFailure),
    Unsupported,
    TypeMismatch,
}
impl CastPrepareError {
    pub fn control_error(&self) -> Option<CompileControlError> {
        match self {
            Self::Control(error) => Some(*error),
            Self::Kernel(KernelFailure::Cancelled) => Some(CompileControlError::Cancelled),
            Self::Kernel(KernelFailure::DeadlineExceeded) => {
                Some(CompileControlError::DeadlineExceeded)
            }
            Self::Kernel(KernelFailure::ResourceExhausted) => {
                Some(CompileControlError::ResourceExhausted)
            }
            _ => None,
        }
    }
}
impl From<CompileControlError> for CastPrepareError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for CastPrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Kernel(error) => error.fmt(f),
            Self::Unsupported => {
                f.write_str("cast requires an implemented exact Physical numeric domain")
            }
            Self::TypeMismatch => {
                f.write_str("cast result violates its frozen successful-NULL contract")
            }
        }
    }
}
impl Error for CastPrepareError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Kernel(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum SignedWidth {
    I8,
    I16,
    I32,
    I64,
}
impl SignedWidth {
    fn from_type(ty: &DataType) -> Option<Self> {
        match ty {
            DataType::Int8 => Some(Self::I8),
            DataType::Int16 => Some(Self::I16),
            DataType::Int32 => Some(Self::I32),
            DataType::Int64 => Some(Self::I64),
            _ => None,
        }
    }
    fn validate(self, array: &dyn Array) -> bool {
        match self {
            Self::I8 => array.as_any().is::<Int8Array>(),
            Self::I16 => array.as_any().is::<Int16Array>(),
            Self::I32 => array.as_any().is::<Int32Array>(),
            Self::I64 => array.as_any().is::<Int64Array>(),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Target {
    Signed(SignedWidth),
    F32,
    F64,
}
impl Target {
    fn from_type(ty: &DataType) -> Option<Self> {
        SignedWidth::from_type(ty).map(Self::Signed).or(match ty {
            DataType::Float32 => Some(Self::F32),
            DataType::Float64 => Some(Self::F64),
            _ => None,
        })
    }
}

/// Successful narrowing failures are NULL, never row errors for these 24 pairs.
/// RowError is reserved for subsequent explicitly authored cast families.
#[derive(Clone, Debug, PartialEq)]
pub enum CastRowResult {
    Null,
    Signed(i64),
    Float32(f32),
    Float64(f64),
    RowError(RowDataError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedCastRecipe {
    operation: CastOperation,
    source: FunctionValueType,
    result: FunctionValueType,
    source_width: SignedWidth,
    target: Target,
    decimal_overflow_policy: DecimalOverflowPolicy,
    allow_throw_exception: bool,
}
impl PreparedCastRecipe {
    pub fn try_new(
        operation: CastOperation,
        source: &FunctionValueType,
        result: &FunctionValueType,
        policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Self, CastPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome = (|| {
            for ty in [source, result] {
                validate_type_observed(ty, &mut work).map_err(CastPrepareError::Kernel)?;
            }
            let physical = source.logical_type == ValueLogicalType::Physical
                && result.logical_type == ValueLogicalType::Physical;
            work.step()?;
            if operation != CastOperation::Carrier || !physical {
                return Err(CastPrepareError::Unsupported);
            }
            let source_width =
                SignedWidth::from_type(&source.data_type).ok_or(CastPrepareError::Unsupported)?;
            let target =
                Target::from_type(&result.data_type).ok_or(CastPrepareError::Unsupported)?;
            work.step()?;
            let narrowing = matches!(target, Target::Signed(width) if width < source_width);
            let valid_nullable = result.nullable || (!source.nullable && !narrowing);
            work.step()?;
            if !valid_nullable {
                return Err(CastPrepareError::TypeMismatch);
            }
            // All admitted carriers are closed scalar types with no retained graph.
            let recipe = Self {
                operation,
                source: source.clone(),
                result: result.clone(),
                source_width,
                target,
                decimal_overflow_policy: policy,
                allow_throw_exception,
            };
            work.step()?;
            Ok(recipe)
        })();
        if outcome
            .as_ref()
            .err()
            .is_some_and(|error| error.control_error().is_some())
        {
            return outcome;
        }
        work.finish()?;
        outcome
    }
    pub fn operation(&self) -> CastOperation {
        self.operation
    }
    pub fn source_type(&self) -> &FunctionValueType {
        &self.source
    }
    pub fn result_type(&self) -> &FunctionValueType {
        &self.result
    }
    pub fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub fn policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }
    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        ScopedExpressionEffects::primitive(context, ExpressionEffects::PURE_VALUE)
    }
    /// The host excludes inherited errors before this selected-row operation.
    /// Both the actual address and concrete scalar carrier are checked before NULL.
    pub fn evaluate_row(
        &self,
        argument: EvaluatedArgument<'_>,
        ordinal: usize,
        logical_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<CastRowResult, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let outcome = (|| {
            let row = self.checked_row(argument, ordinal, logical_row, &mut work)?;
            if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                return if self.source.nullable {
                    Ok(CastRowResult::Null)
                } else {
                    Err(invalid("non-null cast argument contains a selected NULL"))
                };
            }
            macro_rules! convert {
                ($array:ty, $native:ty) => {{
                    let source = argument
                        .array()
                        .as_any()
                        .downcast_ref::<$array>()
                        .ok_or_else(|| internal("cast carrier has a foreign array implementation"))?
                        .value(row);
                    let output = match self.target {
                        Target::Signed(SignedWidth::I8) => num_cast::<$native, i8>(source)
                            .map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I16) => num_cast::<$native, i16>(source)
                            .map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I32) => num_cast::<$native, i32>(source)
                            .map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I64) => {
                            num_cast::<$native, i64>(source).map(CastRowResult::Signed)
                        }
                        Target::F32 => num_cast::<$native, f32>(source).map(CastRowResult::Float32),
                        Target::F64 => num_cast::<$native, f64>(source).map(CastRowResult::Float64),
                    };
                    work.step()?;
                    output.unwrap_or(CastRowResult::Null)
                }};
            }
            Ok(match self.source_width {
                SignedWidth::I8 => convert!(Int8Array, i8),
                SignedWidth::I16 => convert!(Int16Array, i16),
                SignedWidth::I32 => convert!(Int32Array, i32),
                SignedWidth::I64 => convert!(Int64Array, i64),
            })
        })();
        // The same work latch returns any original callback cause without replay.
        work.finish()?;
        outcome
    }
    fn checked_row(
        &self,
        argument: EvaluatedArgument<'_>,
        ordinal: usize,
        logical_row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<usize, KernelFailure> {
        if let EvaluatedArgument::Constant(value) = argument {
            let actual = value.value_type();
            let matches = actual.logical_type == self.source.logical_type
                && (!actual.nullable || self.source.nullable);
            work.step()?;
            if !matches {
                return Err(invalid("cast constant differs from its frozen value type"));
            }
        }
        let array = argument.array();
        let matches = array.data_type() == &self.source.data_type;
        work.step()?;
        if !matches {
            return Err(invalid(
                "cast argument differs from its frozen numeric carrier",
            ));
        }
        let shape = match argument {
            EvaluatedArgument::Scalar(array) => array.len() == 1,
            EvaluatedArgument::SelectedColumn(values) => {
                values.selection().row(ordinal) == Some(logical_row)
            }
            _ => true,
        };
        work.step()?;
        if !shape {
            return Err(invalid(
                "cast argument has a foreign scalar or compact address",
            ));
        }
        if let EvaluatedArgument::SelectedColumn(values) = argument {
            let (mut start, mut end) = (0, values.errors().len());
            while start < end {
                let middle = start + (end - start) / 2;
                let actual = values.errors()[middle].selected_ordinal();
                work.step()?;
                match actual.cmp(&ordinal) {
                    std::cmp::Ordering::Less => start = middle + 1,
                    std::cmp::Ordering::Greater => end = middle,
                    std::cmp::Ordering::Equal => {
                        return Err(invalid(
                            "cast argument contains an unresolved selected row error",
                        ));
                    }
                }
            }
        }
        let row = argument.value_row(ordinal, logical_row);
        let in_bounds = row < array.len();
        work.step()?;
        if !in_bounds {
            return Err(invalid("cast selected address is outside its array"));
        }
        let concrete = self.source_width.validate(array.as_ref());
        work.step()?;
        if !concrete {
            return Err(internal("cast carrier has a foreign array implementation"));
        }
        Ok(row)
    }
}

#[cfg(test)]
#[path = "cast_tests.rs"]
mod tests;
