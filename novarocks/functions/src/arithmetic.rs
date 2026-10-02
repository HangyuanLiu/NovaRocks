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

//! Immutable selected arithmetic for exact Physical signed integer operands.
//! Preparation freezes both input widths, result, and authored policies. This
//! primitive neither allocates output arrays nor grants their memory ownership.

use arrow_array::{Array, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArithmeticOperator, CompileCheckpoints, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, ExpressionEffectContext, ExpressionEffects, FunctionValueType,
    PureCompileControl, ValueLogicalType, arithmetic_result_value_type_with_op,
};
use std::{error::Error, fmt};

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, validate_type_observed};
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError,
    ScopedExpressionEffects,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArithmeticPrepareError {
    Control(CompileControlError),
    Kernel(KernelFailure),
    Unsupported,
    TypeMismatch,
}
impl ArithmeticPrepareError {
    pub fn control_error(&self) -> Option<CompileControlError> {
        match self {
            Self::Control(cause) => Some(*cause),
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
impl From<CompileControlError> for ArithmeticPrepareError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl fmt::Display for ArithmeticPrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Kernel(error) => error.fmt(f),
            Self::Unsupported => {
                f.write_str("arithmetic requires exact Physical signed integer operands")
            }
            Self::TypeMismatch => {
                f.write_str("arithmetic result differs from its frozen nullable value type")
            }
        }
    }
}
impl Error for ArithmeticPrepareError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Kernel(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignedWidth {
    I8,
    I16,
    I32,
    I64,
}
impl SignedWidth {
    fn from_type(ty: &FunctionValueType) -> Option<Self> {
        if ty.logical_type != ValueLogicalType::Physical {
            return None;
        }
        Self::from_carrier(&ty.data_type)
    }
    fn from_carrier(carrier: &DataType) -> Option<Self> {
        Some(match carrier {
            DataType::Int8 => Self::I8,
            DataType::Int16 => Self::I16,
            DataType::Int32 => Self::I32,
            DataType::Int64 => Self::I64,
            _ => return None,
        })
    }
    fn validate(self, array: &dyn Array) -> bool {
        match self {
            Self::I8 => array.as_any().is::<Int8Array>(),
            Self::I16 => array.as_any().is::<Int16Array>(),
            Self::I32 => array.as_any().is::<Int32Array>(),
            Self::I64 => array.as_any().is::<Int64Array>(),
        }
    }
    fn read(self, array: &dyn Array, row: usize) -> Result<i64, KernelFailure> {
        macro_rules! read {
            ($array:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(|array| i64::from(array.value(row)))
                    .ok_or_else(|| {
                        internal("signed arithmetic carrier has a foreign array implementation")
                    })
            };
        }
        match self {
            Self::I8 => read!(Int8Array),
            Self::I16 => read!(Int16Array),
            Self::I32 => read!(Int32Array),
            Self::I64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|array| array.value(row))
                .ok_or_else(|| {
                    internal("signed arithmetic carrier has a foreign array implementation")
                }),
        }
    }
}

/// A row error uses the supplied left compact ordinal. The controller must
/// remap it when publishing into another output Selection; this is not a
/// source logical row number. No output vector or deferred replay is owned here.
#[derive(Clone, Debug, PartialEq)]
pub enum ArithmeticRowResult {
    Null,
    Signed(i64),
    Float(f64),
    RowError(RowDataError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedArithmeticRecipe {
    operator: ArithmeticOperator,
    left: FunctionValueType,
    right: FunctionValueType,
    result: FunctionValueType,
    left_width: SignedWidth,
    right_width: SignedWidth,
    decimal_overflow_policy: DecimalOverflowPolicy,
    allow_throw_exception: bool,
}
impl PreparedArithmeticRecipe {
    pub fn try_new(
        operator: ArithmeticOperator,
        left: &FunctionValueType,
        right: &FunctionValueType,
        result: &FunctionValueType,
        decimal_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ArithmeticPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome = (|| {
            for ty in [left, right, result] {
                validate_type_observed(ty, &mut work).map_err(ArithmeticPrepareError::Kernel)?;
            }
            let left_width = SignedWidth::from_type(left);
            let right_width = SignedWidth::from_type(right);
            work.step()?;
            let (Some(left_width), Some(right_width)) = (left_width, right_width) else {
                return Err(ArithmeticPrepareError::Unsupported);
            };
            let expected = arithmetic_result_value_type_with_op(left, right, operator)
                .ok_or(ArithmeticPrepareError::TypeMismatch)?;
            let matches = result.nullable
                && result.logical_type == expected.logical_type
                && result.data_type == expected.data_type;
            work.step()?;
            if !matches {
                return Err(ArithmeticPrepareError::TypeMismatch);
            }
            // All admitted carriers are now closed scalar types: these copies
            // contain no field graph, metadata, timezone or dynamic backing.
            let recipe = Self {
                operator,
                left: left.clone(),
                right: right.clone(),
                result: result.clone(),
                left_width,
                right_width,
                decimal_overflow_policy: decimal_policy,
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
    pub fn operator(&self) -> ArithmeticOperator {
        self.operator
    }
    pub fn left_type(&self) -> &FunctionValueType {
        &self.left
    }
    pub fn right_type(&self) -> &FunctionValueType {
        &self.right
    }
    pub fn result_type(&self) -> &FunctionValueType {
        &self.result
    }
    pub fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }
    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        let may_raise_row_error = match self.operator {
            ArithmeticOperator::Modulo => true,
            ArithmeticOperator::Divide => false,
            ArithmeticOperator::Add
            | ArithmeticOperator::Subtract
            | ArithmeticOperator::Multiply => {
                self.left_width == SignedWidth::I64 || self.right_width == SignedWidth::I64
            }
        };
        ScopedExpressionEffects::primitive(
            context,
            ExpressionEffects {
                may_raise_row_error,
                ..ExpressionEffects::PURE_VALUE
            },
        )
    }

    /// Validate both original addresses before strict SQL NULL. Inherited row
    /// errors must have been excluded by the controller; an unresolved compact
    /// error is rejected rather than misread as a successful NULL.
    #[expect(
        clippy::too_many_arguments,
        reason = "Preserve both actual compact/logical addresses and original control"
    )]
    pub fn evaluate_row(
        &self,
        left: EvaluatedArgument<'_>,
        left_ordinal: usize,
        left_logical_row: usize,
        right: EvaluatedArgument<'_>,
        right_ordinal: usize,
        right_logical_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArithmeticRowResult, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let outcome = (|| {
            let l = checked_row(
                left,
                left_ordinal,
                left_logical_row,
                &self.left,
                self.left_width,
                &mut work,
            )?;
            let r = checked_row(
                right,
                right_ordinal,
                right_logical_row,
                &self.right,
                self.right_width,
                &mut work,
            )?;
            let (Some(l), Some(r)) = (l, r) else {
                return Ok(ArithmeticRowResult::Null);
            };
            use ArithmeticOperator::*;
            let output = match self.operator {
                Divide if r == 0 => ArithmeticRowResult::Null,
                Divide => ArithmeticRowResult::Float((l as f64) / (r as f64)),
                Modulo if r == 0 => ArithmeticRowResult::RowError(RowDataError::new(
                    left_ordinal,
                    "Divide by zero error",
                )),
                Modulo => ArithmeticRowResult::Signed(l.wrapping_rem(r)),
                Add | Subtract | Multiply => {
                    let (value, symbol) = match self.operator {
                        Add => (l.checked_add(r), '+'),
                        Subtract => (l.checked_sub(r), '-'),
                        Multiply => (l.checked_mul(r), '*'),
                        _ => unreachable!("checked signed arithmetic operation"),
                    };
                    match value {
                        Some(value) => ArithmeticRowResult::Signed(value),
                        None => ArithmeticRowResult::RowError(RowDataError::new(
                            left_ordinal,
                            &format!(
                                "Arithmetic overflow: Overflow happened on: {l:?} {symbol} {r:?}"
                            ),
                        )),
                    }
                }
            };
            work.step()?;
            Ok(output)
        })();
        // EvaluationCheckpoints preserves any primary callback refusal, even
        // an Internal/Operational/InvalidProgram cause; it never rechecks it.
        work.finish()?;
        outcome
    }
}

fn checked_row(
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    logical_row: usize,
    expected: &FunctionValueType,
    width: SignedWidth,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<i64>, KernelFailure> {
    if let EvaluatedArgument::Constant(value) = argument {
        let actual = value.value_type();
        let matches =
            actual.logical_type == expected.logical_type && (!actual.nullable || expected.nullable);
        work.step()?;
        if !matches {
            return Err(invalid(
                "arithmetic constant differs from its frozen value type",
            ));
        }
    }
    let array = argument.array();
    let matches = SignedWidth::from_carrier(array.data_type()) == Some(width);
    work.step()?;
    if !matches {
        return Err(invalid(
            "arithmetic argument differs from its frozen signed carrier",
        ));
    }
    let shape_matches = match argument {
        EvaluatedArgument::Scalar(array) => array.len() == 1,
        EvaluatedArgument::SelectedColumn(values) => {
            values.selection().row(ordinal) == Some(logical_row)
        }
        _ => true,
    };
    work.step()?;
    if !shape_matches {
        return Err(invalid(
            "arithmetic argument has a foreign scalar or compact address",
        ));
    }
    if let EvaluatedArgument::SelectedColumn(values) = argument {
        // The checked errors are sorted and unique. Search only this actual
        // compact ordinal without scanning another row or building a bitmap.
        let (mut start, mut end) = (0, values.errors().len());
        while start < end {
            let middle = start + (end - start) / 2;
            let error_ordinal = values.errors()[middle].selected_ordinal();
            work.step()?;
            match error_ordinal.cmp(&ordinal) {
                std::cmp::Ordering::Less => start = middle + 1,
                std::cmp::Ordering::Greater => end = middle,
                std::cmp::Ordering::Equal => {
                    return Err(invalid(
                        "arithmetic argument contains an unresolved selected row error",
                    ));
                }
            }
        }
    }
    let row = argument.value_row(ordinal, logical_row);
    let in_bounds = row < array.len();
    work.step()?;
    if !in_bounds {
        return Err(invalid("arithmetic selected address is outside its array"));
    }
    let valid_implementation = width.validate(array.as_ref());
    work.step()?;
    if !valid_implementation {
        return Err(internal(
            "signed arithmetic carrier has a foreign array implementation",
        ));
    }
    let is_null = array.is_null(row);
    work.step()?;
    if is_null {
        return if expected.nullable {
            Ok(None)
        } else {
            Err(invalid(
                "non-null arithmetic argument contains a selected NULL",
            ))
        };
    }
    let value = width.read(array.as_ref(), row);
    work.step()?;
    Ok(Some(value?))
}

#[cfg(test)]
#[path = "arithmetic_tests.rs"]
mod tests;
