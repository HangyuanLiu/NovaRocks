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

//! Immutable selected casts for exact Physical signed and floating scalar domains.
//! The caller retains original policies and full types; this recipe performs no
//! output allocation, registry lookup, coercion or memory admission.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, logical_is_null, validate_type_observed};
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError,
    ScopedExpressionEffects,
};
use arrow_array::{
    Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
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
enum Source {
    Signed(SignedWidth),
    F32,
    F64,
}
impl Source {
    fn from_type(ty: &DataType) -> Option<Self> {
        SignedWidth::from_type(ty).map(Self::Signed).or(match ty {
            DataType::Float32 => Some(Self::F32),
            DataType::Float64 => Some(Self::F64),
            _ => None,
        })
    }
    fn validate(self, array: &dyn Array) -> bool {
        match self {
            Self::Signed(width) => width.validate(array),
            Self::F32 => array.as_any().is::<Float32Array>(),
            Self::F64 => array.as_any().is::<Float64Array>(),
        }
    }
    const fn is_float(self) -> bool {
        matches!(self, Self::F32 | Self::F64)
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

/// Signed narrowing failures are successful NULLs. Floating-to-signed failures
/// use the original ALLOW policy independently of the decimal overflow policy.
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
    source_kind: Source,
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
            let source_kind =
                Source::from_type(&source.data_type).ok_or(CastPrepareError::Unsupported)?;
            let target =
                Target::from_type(&result.data_type).ok_or(CastPrepareError::Unsupported)?;
            work.step()?;
            if source_kind.is_float() && !matches!(target, Target::Signed(_)) {
                return Err(CastPrepareError::Unsupported);
            }
            let successful_null = match (source_kind, target) {
                (Source::Signed(source), Target::Signed(target)) => target < source,
                (Source::F32 | Source::F64, Target::Signed(_)) => !allow_throw_exception,
                _ => false,
            };
            let valid_nullable = result.nullable || (!source.nullable && !successful_null);
            work.step()?;
            if !valid_nullable {
                return Err(CastPrepareError::TypeMismatch);
            }
            // All admitted carriers are closed scalar types with no retained graph.
            let recipe = Self {
                operation,
                source: source.clone(),
                result: result.clone(),
                source_kind,
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
        ScopedExpressionEffects::primitive(
            context,
            ExpressionEffects {
                may_raise_row_error: self.source_kind.is_float() && self.allow_throw_exception,
                ..ExpressionEffects::PURE_VALUE
            },
        )
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
            macro_rules! convert_float {
                ($array:ty, $native:ty) => {{
                    let source = argument.array().as_any().downcast_ref::<$array>()
                        .ok_or_else(|| internal("cast carrier has a foreign array implementation"))?
                        .value(row);
                    let converted = match self.target {
                        Target::Signed(SignedWidth::I8) => num_cast::<$native, i8>(source).map(i64::from),
                        Target::Signed(SignedWidth::I16) => num_cast::<$native, i16>(source).map(i64::from),
                        Target::Signed(SignedWidth::I32) => num_cast::<$native, i32>(source).map(i64::from),
                        Target::Signed(SignedWidth::I64) => num_cast::<$native, i64>(source),
                        _ => return Err(internal("floating cast contains a foreign frozen target")),
                    };
                    work.step()?;
                    match converted {
                        Some(value) => CastRowResult::Signed(value),
                        None if !self.allow_throw_exception => CastRowResult::Null,
                        None => {
                            let name = match self.target {
                                Target::Signed(SignedWidth::I8) => "TINYINT",
                                Target::Signed(SignedWidth::I16) => "SMALLINT",
                                Target::Signed(SignedWidth::I32) => "INT",
                                Target::Signed(SignedWidth::I64) => "BIGINT",
                                _ => return Err(internal("floating cast contains a foreign frozen target")),
                            };
                            work.step()?;
                            work.flush()?;
                            // The frozen scalar types and one f64 bound the diagnostic.
                            // String allocation/formatting internals are opaque, not a
                            // formal host memory grant or an internal work proof.
                            let message = format!(
                                "Expr evaluate meet error: CAST failed: from {:?} to {:?}: {} conflict with range of {}",
                                self.source.data_type, self.result.data_type, source as f64, name,
                            );
                            let error = RowDataError::new(ordinal, &message);
                            work.flush()?;
                            CastRowResult::RowError(error)
                        }
                    }
                }};
            }
            Ok(match self.source_kind {
                Source::Signed(SignedWidth::I8) => convert!(Int8Array, i8),
                Source::Signed(SignedWidth::I16) => convert!(Int16Array, i16),
                Source::Signed(SignedWidth::I32) => convert!(Int32Array, i32),
                Source::Signed(SignedWidth::I64) => convert!(Int64Array, i64),
                Source::F32 => convert_float!(Float32Array, f32),
                Source::F64 => convert_float!(Float64Array, f64),
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
        let concrete = self.source_kind.validate(array.as_ref());
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

#[cfg(test)]
#[path = "cast_float_tests.rs"]
mod float_tests;
