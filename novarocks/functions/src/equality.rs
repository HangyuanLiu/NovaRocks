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

//! Immutable equality for already-materialized flat value domains with explicit logical identity.
//! This primitive has no instance state, registry lookup, coercion or memory grant.

use arrow_array::{types::*, *};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueLogicalType, arrow_data_types_exact_observed,
};
use std::{error::Error, fmt};

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, validate_type_observed};
use crate::{EvaluatedArgument, KernelEvaluationControl, KernelFailure};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EqualityPrepareError {
    Control(CompileControlError),
    Kernel(KernelFailure),
    Unsupported,
    TypeMismatch,
}
impl EqualityPrepareError {
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
impl fmt::Display for EqualityPrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Kernel(error) => error.fmt(f),
            Self::Unsupported => {
                f.write_str("equality requires its exact supported flat logical domain")
            }
            Self::TypeMismatch => {
                f.write_str("equality arguments differ in their frozen value domain")
            }
        }
    }
}
impl Error for EqualityPrepareError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Kernel(error) => Some(error),
            _ => None,
        }
    }
}
impl From<CompileControlError> for EqualityPrepareError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for EqualityPrepareError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Kernel(error.into())
    }
}

/// Scalar Arrow equality, including floating-point bit equality. Authored UUID
/// and LARGEINT compare their exact 16-byte representation, without inferring
/// either identity from a carrier. Nested and null-safe equality have different
/// existing algorithms and are not admitted.
/// The compiler retains this checked recipe against the actual expression use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedEqualityRecipe {
    left: FunctionValueType,
    right: FunctionValueType,
    leaf: FlatLeaf,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlatLeaf {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Date32,
    Date64,
    Time32Second,
    Time32Millisecond,
    Time64Microsecond,
    Time64Nanosecond,
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
    DurationSecond,
    DurationMillisecond,
    DurationMicrosecond,
    DurationNanosecond,
    IntervalYearMonth,
    IntervalDayTime,
    IntervalMonthDayNano,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    FixedSizeBinary,
}
impl FlatLeaf {
    fn from_type(ty: &DataType) -> Option<Self> {
        Some(match ty {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Decimal32(..) => Self::Decimal32,
            DataType::Decimal64(..) => Self::Decimal64,
            DataType::Decimal128(..) => Self::Decimal128,
            DataType::Decimal256(..) => Self::Decimal256,
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Time32(TimeUnit::Second) => Self::Time32Second,
            DataType::Time32(TimeUnit::Millisecond) => Self::Time32Millisecond,
            DataType::Time64(TimeUnit::Microsecond) => Self::Time64Microsecond,
            DataType::Time64(TimeUnit::Nanosecond) => Self::Time64Nanosecond,
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => Self::TimestampSecond,
                TimeUnit::Millisecond => Self::TimestampMillisecond,
                TimeUnit::Microsecond => Self::TimestampMicrosecond,
                TimeUnit::Nanosecond => Self::TimestampNanosecond,
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => Self::DurationSecond,
                TimeUnit::Millisecond => Self::DurationMillisecond,
                TimeUnit::Microsecond => Self::DurationMicrosecond,
                TimeUnit::Nanosecond => Self::DurationNanosecond,
            },
            DataType::Interval(IntervalUnit::YearMonth) => Self::IntervalYearMonth,
            DataType::Interval(IntervalUnit::DayTime) => Self::IntervalDayTime,
            DataType::Interval(IntervalUnit::MonthDayNano) => Self::IntervalMonthDayNano,
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::FixedSizeBinary(_) => Self::FixedSizeBinary,
            _ => return None,
        })
    }
}
impl PreparedEqualityRecipe {
    pub fn try_new(
        left: &FunctionValueType,
        right: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<Self, EqualityPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            validate_type_observed(left, &mut work).map_err(EqualityPrepareError::Kernel)?;
            validate_type_observed(right, &mut work).map_err(EqualityPrepareError::Kernel)?;
            if left.logical_type != right.logical_type {
                return Err(EqualityPrepareError::TypeMismatch);
            }
            let allowed = match left.logical_type {
                ValueLogicalType::Physical => true,
                ValueLogicalType::LargeInt | ValueLogicalType::Uuid => {
                    left.data_type == DataType::FixedSizeBinary(16)
                        && right.data_type == DataType::FixedSizeBinary(16)
                }
                _ => false,
            };
            if !allowed {
                return Err(EqualityPrepareError::Unsupported);
            }
            let leaf =
                FlatLeaf::from_type(&left.data_type).ok_or(EqualityPrepareError::Unsupported)?;
            if !arrow_data_types_exact_observed::<EqualityPrepareError>(
                &left.data_type,
                &right.data_type,
                || work.step().map_err(Into::into),
            )? {
                return Err(EqualityPrepareError::TypeMismatch);
            }
            work.step()?;
            // Only flat types remain; timestamp timezone backing is Arc-shared.
            work.flush()?;
            let prepared = Self {
                left: left.clone(),
                right: right.clone(),
                leaf,
            };
            work.step()?;
            Ok(prepared)
        })();
        if result
            .as_ref()
            .err()
            .is_some_and(|error| error.control_error().is_some())
        {
            return result;
        }
        work.finish()?;
        result
    }
    pub fn left_type(&self) -> &FunctionValueType {
        &self.left
    }
    pub fn right_type(&self) -> &FunctionValueType {
        &self.right
    }
    pub fn nullable_result(&self) -> bool {
        self.left.nullable || self.right.nullable || self.leaf == FlatLeaf::Null
    }

    /// The host supplies an actual required row and excludes inherited errors.
    /// This verifies only these two addresses, never scans a whole Selection.
    /// The caller flushes its own pending work before entering this scope.
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep both actual source addresses and the caller-owned control explicit"
    )]
    pub fn compare_rows(
        &self,
        left: EvaluatedArgument<'_>,
        left_ordinal: usize,
        left_batch_row: usize,
        right: EvaluatedArgument<'_>,
        right_ordinal: usize,
        right_batch_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<bool>, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let l = checked_row(left, left_ordinal, left_batch_row, &self.left, &mut work)?;
            let r = checked_row(
                right,
                right_ordinal,
                right_batch_row,
                &self.right,
                &mut work,
            )?;
            let la = left.array();
            let ra = right.array();
            // Check both operands before a successful SQL NULL can short-circuit.
            if la.is_null(l) && !self.left.nullable || ra.is_null(r) && !self.right.nullable {
                return Err(invalid(
                    "non-null equality argument contains a selected NULL",
                ));
            }
            work.step()?;
            self.compare_values(la.as_ref(), l, ra.as_ref(), r, &mut work)
        })();
        if matches!(
            result,
            Err(KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted)
        ) {
            return result;
        }
        work.finish()?;
        result
    }

    fn compare_values(
        &self,
        left: &dyn Array,
        l: usize,
        right: &dyn Array,
        r: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<bool>, KernelFailure> {
        macro_rules! p {
            ($ty:ty) => {
                primitive::<$ty>(left, l, right, r, |a, b| a == b, work)
            };
        }
        macro_rules! f {
            ($ty:ty) => {
                primitive::<$ty>(left, l, right, r, |a, b| a.to_bits() == b.to_bits(), work)
            };
        }
        macro_rules! b {
            ($ty:ty, $bytes:expr) => {{
                let left = cast::<$ty>(left)?;
                let right = cast::<$ty>(right)?;
                if left.is_null(l) || right.is_null(r) {
                    Ok(None)
                } else {
                    let bytes: fn(&$ty, usize) -> &[u8] = $bytes;
                    equal_bytes(bytes(left, l), bytes(right, r), work).map(Some)
                }
            }};
        }
        match self.leaf {
            FlatLeaf::Null => {
                cast::<NullArray>(left)?;
                cast::<NullArray>(right)?;
                Ok(None)
            }
            FlatLeaf::Boolean => {
                let left = cast::<BooleanArray>(left)?;
                let right = cast::<BooleanArray>(right)?;
                if left.is_null(l) || right.is_null(r) {
                    Ok(None)
                } else {
                    let eq = left.value(l) == right.value(r);
                    work.step()?;
                    Ok(Some(eq))
                }
            }
            FlatLeaf::Int8 => p!(Int8Type),
            FlatLeaf::Int16 => p!(Int16Type),
            FlatLeaf::Int32 => p!(Int32Type),
            FlatLeaf::Int64 => p!(Int64Type),
            FlatLeaf::UInt8 => p!(UInt8Type),
            FlatLeaf::UInt16 => p!(UInt16Type),
            FlatLeaf::UInt32 => p!(UInt32Type),
            FlatLeaf::UInt64 => p!(UInt64Type),
            FlatLeaf::Float16 => f!(Float16Type),
            FlatLeaf::Float32 => f!(Float32Type),
            FlatLeaf::Float64 => f!(Float64Type),
            FlatLeaf::Decimal32 => p!(Decimal32Type),
            FlatLeaf::Decimal64 => p!(Decimal64Type),
            FlatLeaf::Decimal128 => p!(Decimal128Type),
            FlatLeaf::Decimal256 => p!(Decimal256Type),
            FlatLeaf::Date32 => p!(Date32Type),
            FlatLeaf::Date64 => p!(Date64Type),
            FlatLeaf::Time32Second => p!(Time32SecondType),
            FlatLeaf::Time32Millisecond => p!(Time32MillisecondType),
            FlatLeaf::Time64Microsecond => p!(Time64MicrosecondType),
            FlatLeaf::Time64Nanosecond => p!(Time64NanosecondType),
            FlatLeaf::TimestampSecond => p!(TimestampSecondType),
            FlatLeaf::TimestampMillisecond => p!(TimestampMillisecondType),
            FlatLeaf::TimestampMicrosecond => p!(TimestampMicrosecondType),
            FlatLeaf::TimestampNanosecond => p!(TimestampNanosecondType),
            FlatLeaf::DurationSecond => p!(DurationSecondType),
            FlatLeaf::DurationMillisecond => p!(DurationMillisecondType),
            FlatLeaf::DurationMicrosecond => p!(DurationMicrosecondType),
            FlatLeaf::DurationNanosecond => p!(DurationNanosecondType),
            FlatLeaf::IntervalYearMonth => p!(IntervalYearMonthType),
            FlatLeaf::IntervalDayTime => p!(IntervalDayTimeType),
            FlatLeaf::IntervalMonthDayNano => p!(IntervalMonthDayNanoType),
            FlatLeaf::Utf8 => b!(StringArray, |a, i| a.value(i).as_bytes()),
            FlatLeaf::LargeUtf8 => b!(LargeStringArray, |a, i| a.value(i).as_bytes()),
            FlatLeaf::Binary => b!(BinaryArray, |a, i| a.value(i)),
            FlatLeaf::LargeBinary => b!(LargeBinaryArray, |a, i| a.value(i)),
            FlatLeaf::FixedSizeBinary => b!(FixedSizeBinaryArray, |a, i| a.value(i)),
        }
    }
}

fn cast<T: Array + 'static>(array: &dyn Array) -> Result<&T, KernelFailure> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| internal("exact equality carrier cannot be downcast"))
}
fn primitive<T: ArrowPrimitiveType>(
    left: &dyn Array,
    l: usize,
    right: &dyn Array,
    r: usize,
    equal: impl FnOnce(T::Native, T::Native) -> bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<bool>, KernelFailure> {
    let left = cast::<PrimitiveArray<T>>(left)?;
    let right = cast::<PrimitiveArray<T>>(right)?;
    if left.is_null(l) || right.is_null(r) {
        return Ok(None);
    }
    let equal = equal(left.value(l), right.value(r));
    work.step()?;
    Ok(Some(equal))
}
fn equal_bytes(
    left: &[u8],
    right: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    if left.len() != right.len() {
        work.step()?;
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        let same = left == right;
        work.step()?;
        if !same {
            return Ok(false);
        }
    }
    Ok(true)
}
fn flat_type_matches(
    expected: &DataType,
    actual: &DataType,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    // Timezone labels are bounded by the preparation owner but still observed
    // byte by byte when their Arc backing is not shared.
    if let (DataType::Timestamp(eu, ez), DataType::Timestamp(au, az)) = (expected, actual) {
        if eu != au {
            work.step()?;
            return Ok(false);
        }
        match (ez, az) {
            (None, None) => Ok(true),
            (Some(e), Some(a)) => equal_bytes(e.as_bytes(), a.as_bytes(), work),
            _ => Ok(false),
        }
    } else {
        let same = expected == actual;
        work.step()?;
        Ok(same)
    }
}
fn checked_row(
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    batch_row: usize,
    expected: &FunctionValueType,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    if let EvaluatedArgument::Constant(value) = argument {
        let ty = value.value_type();
        if ty.logical_type != expected.logical_type || (ty.nullable && !expected.nullable) {
            return Err(invalid(
                "equality constant differs from its frozen value type",
            ));
        }
    }
    if !flat_type_matches(&expected.data_type, argument.array().data_type(), work)? {
        return Err(invalid(
            "equality argument differs from its frozen flat carrier",
        ));
    }
    match argument {
        EvaluatedArgument::Scalar(array) if array.len() != 1 => {
            return Err(invalid("equality scalar has invalid cardinality"));
        }
        EvaluatedArgument::SelectedColumn(values)
            if values.selection().row(ordinal) != Some(batch_row) =>
        {
            return Err(invalid(
                "equality compact address differs from its actual selection",
            ));
        }
        _ => {}
    }
    let row = argument.value_row(ordinal, batch_row);
    if row >= argument.array().len() {
        return Err(invalid("equality selected address is outside its array"));
    }
    work.step()?;
    Ok(row)
}

#[cfg(test)]
#[path = "equality/tests.rs"]
mod tests;
