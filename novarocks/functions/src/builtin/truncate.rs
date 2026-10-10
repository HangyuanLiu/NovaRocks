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

//! Selected TRUNCATE computation, preserving its installed floating algorithm.

use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
    builder::{Decimal128Builder, Float64Builder, Int64Builder},
    types::{Decimal128Type, DecimalType},
};
use arrow_cast::cast::DecimalCast;
use arrow_schema::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, ValueLogicalType};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TruncateOp {
    Unary,
    Binary,
}
impl TruncateOp {
    fn arity(self) -> usize {
        match self {
            Self::Unary => 1,
            Self::Binary => 2,
        }
    }
}

enum NumericInput<'a> {
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Decimal128(&'a Decimal128Array, f64),
    Null,
}
impl<'a> NumericInput<'a> {
    fn checked(array: &'a ArrayRef, source: &FunctionValueType) -> Result<Self, KernelFailure> {
        if source.logical_type != ValueLogicalType::Physical
            || array.data_type() != &source.data_type
        {
            return Err(invalid(
                "truncate input differs from its exact numeric carrier",
            ));
        }
        macro_rules! downcast {
            ($array:ty, $variant:ident) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(Self::$variant)
                    .ok_or_else(|| internal("truncate numeric carrier cannot be downcast"))
            };
        }
        match &source.data_type {
            DataType::Int8 => downcast!(Int8Array, Int8),
            DataType::Int16 => downcast!(Int16Array, Int16),
            DataType::Int32 => downcast!(Int32Array, Int32),
            DataType::Int64 => downcast!(Int64Array, Int64),
            DataType::Float32 => downcast!(Float32Array, Float32),
            DataType::Float64 => downcast!(Float64Array, Float64),
            DataType::Decimal128(_, scale) => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(|array| Self::Decimal128(array, 10_f64.powi(i32::from(*scale))))
                .ok_or_else(|| internal("truncate decimal carrier cannot be downcast")),
            DataType::Null => Ok(Self::Null),
            _ => Err(invalid(
                "truncate input is not an installed numeric carrier",
            )),
        }
    }
    fn value(&self, row: usize) -> f64 {
        match self {
            Self::Int8(a) => f64::from(a.value(row)),
            Self::Int16(a) => f64::from(a.value(row)),
            Self::Int32(a) => f64::from(a.value(row)),
            Self::Int64(a) => a.value(row) as f64,
            Self::Float32(a) => f64::from(a.value(row)),
            Self::Float64(a) => a.value(row),
            // Keep the original i128->f64 conversion before division. This is
            // deliberately not exact integer decimal truncation.
            Self::Decimal128(a, divisor) => (a.value(row) as f64) / divisor,
            Self::Null => unreachable!("Null carriers never produce a non-null row"),
        }
    }
    fn digits(&self, row: usize) -> Option<i64> {
        match self {
            Self::Int8(a) => Some(i64::from(a.value(row))),
            Self::Int16(a) => Some(i64::from(a.value(row))),
            Self::Int32(a) => Some(i64::from(a.value(row))),
            Self::Int64(a) => Some(a.value(row)),
            Self::Float32(a) => Some(f64::from(a.value(row)))
                .filter(|v| v.is_finite())
                .map(|v| v as i64),
            Self::Float64(a) => Some(a.value(row))
                .filter(|v| v.is_finite())
                .map(|v| v as i64),
            // The installed Decimal digits reader saturates after the same
            // floating conversion; it is not ROUND's checked Arrow cast.
            Self::Decimal128(..) => Some(self.value(row) as i64),
            Self::Null => None,
        }
    }
}
struct NumericValue<'a> {
    argument: EvaluatedArgument<'a>,
    view: NumericInput<'a>,
    nullable: bool,
}
impl<'a> NumericValue<'a> {
    fn checked(
        argument: EvaluatedArgument<'a>,
        source: &FunctionValueType,
    ) -> Result<Self, KernelFailure> {
        Ok(Self {
            argument,
            view: NumericInput::checked(argument.array(), source)?,
            nullable: source.nullable,
        })
    }
    fn row(&self, ordinal: usize, batch_row: usize) -> Result<Option<usize>, KernelFailure> {
        let row = self.argument.value_row(ordinal, batch_row);
        if row >= self.argument.array().len() {
            return Err(internal("truncate selected row is outside its carrier"));
        }
        if self.argument.array().is_null(row) || matches!(self.view, NumericInput::Null) {
            if !self.nullable {
                return Err(internal(
                    "truncate non-null input contains selected SQL NULL",
                ));
            }
            Ok(None)
        } else {
            Ok(Some(row))
        }
    }
}

enum Output {
    Integer(Int64Builder),
    Float(Float64Builder),
    Decimal {
        builder: Decimal128Builder,
        multiplier: f64,
        precision: u8,
        policy: DecimalOverflowPolicy,
    },
}
impl Output {
    fn new(
        target: &FunctionValueType,
        rows: usize,
        policy: DecimalOverflowPolicy,
    ) -> Result<Self, KernelFailure> {
        if target.logical_type != ValueLogicalType::Physical || !target.nullable {
            return Err(invalid(
                "truncate requires its exact nullable Physical result",
            ));
        }
        match target.data_type {
            DataType::Int64 => Ok(Self::Integer(Int64Builder::with_capacity(rows))),
            DataType::Float64 => Ok(Self::Float(Float64Builder::with_capacity(rows))),
            DataType::Decimal128(38, scale) => {
                let builder = Decimal128Builder::with_capacity(rows)
                    .with_precision_and_scale(38, scale)
                    .map_err(|_| {
                        invalid("truncate selected decimal result parameters are invalid")
                    })?;
                Ok(Self::Decimal {
                    builder,
                    multiplier: 10_f64.powi(i32::from(scale)),
                    precision: 38,
                    policy,
                })
            }
            _ => Err(invalid(
                "truncate result is not an installed output carrier",
            )),
        }
    }
    fn append(&mut self, value: Option<f64>, ordinal: usize, errors: &mut Vec<RowDataError>) {
        match self {
            // This is Arrow's checked numeric f64->i64 conversion, including
            // its asymmetric inclusive lower/exclusive upper boundary.
            Self::Integer(builder) => {
                builder.append_option(value.and_then(<i64 as DecimalCast>::from_f64))
            }
            Self::Float(builder) => builder.append_option(value),
            Self::Decimal {
                builder,
                multiplier,
                precision,
                policy,
            } => {
                let converted = value.and_then(|value| {
                    <i128 as DecimalCast>::from_f64((*multiplier * value).round())
                        .filter(|raw| Decimal128Type::is_valid_decimal_precision(*raw, *precision))
                });
                if value.is_some()
                    && converted.is_none()
                    && *policy == DecimalOverflowPolicy::ReportError
                {
                    errors.push(RowDataError::new(
                        ordinal,
                        "decimal overflow in truncate output",
                    ));
                }
                builder.append_option(converted);
            }
        }
    }
    fn finish(self) -> ArrayRef {
        match self {
            Self::Integer(mut b) => Arc::new(b.finish()),
            Self::Float(mut b) => Arc::new(b.finish()),
            Self::Decimal { mut builder, .. } => Arc::new(builder.finish()),
        }
    }
}

pub(super) fn evaluate_truncate<'a>(
    op: TruncateOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    if input.arguments().len() != op.arity()
        || input.contract().selected().argument_types.len() != op.arity()
    {
        return Err(invalid(
            "truncate arguments differ from the frozen operation arity",
        ));
    }
    let (value, digits) = match (
        op,
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) {
        (TruncateOp::Unary, [FunctionArgumentType::Value(source)], [argument]) => {
            (NumericValue::checked(*argument, source)?, None)
        }
        (
            TruncateOp::Binary,
            [
                FunctionArgumentType::Value(left),
                FunctionArgumentType::Value(right),
            ],
            [l, r],
        ) => (
            NumericValue::checked(*l, left)?,
            Some(NumericValue::checked(*r, right)?),
        ),
        _ => {
            return Err(invalid(
                "truncate selected call is not an exact value profile",
            ));
        }
    };
    let selection = input.selection();
    output_capacity(selection.len())?;
    let target = input.contract().result_type();
    let mut output = Output::new(
        target,
        selection.len(),
        input.contract().decimal_overflow_policy(),
    )?;
    let mut errors = Vec::new();
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let left = value
            .row(ordinal, batch_row)?
            .map(|row| value.view.value(row));
        let result = match &digits {
            None => left.map(f64::trunc),
            Some(digits) => {
                let right = digits
                    .row(ordinal, batch_row)?
                    .and_then(|row| digits.view.digits(row));
                match (left, right) {
                    (Some(value), Some(digits)) if digits >= 0 => {
                        let factor = 10_f64.powi(digits as i32);
                        Some((value * factor).trunc() / factor)
                    }
                    (Some(value), Some(digits)) => {
                        // Unsigned magnitude fixes i64::MIN negation without
                        // changing the installed exponent's i32 projection.
                        let factor = 10_f64.powi(digits.unsigned_abs() as i32);
                        Some((value / factor).trunc() * factor)
                    }
                    _ => None,
                }
            }
        }
        .filter(|value| value.is_finite());
        output.append(result, ordinal, &mut errors);
    }
    let values = output.finish();
    work.finish()?;
    SelectedValues::try_new(
        selection,
        &target.data_type,
        values,
        errors.into_boxed_slice(),
    )
    .map_err(|_| internal("truncate compact output violates its selected contract"))
}
// Representation limits are not a memory grant. Formal host admission remains
// required before the output builders and bounded row-error storage allocate.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    for width in [
        16,
        std::mem::size_of::<RowDataError>() + crate::MAX_ROW_ERROR_MESSAGE_BYTES,
    ] {
        let bytes = rows
            .checked_mul(width)
            .ok_or(KernelFailure::ResourceExhausted)?;
        isize::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    }
    rows.checked_add(7)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
#[path = "truncate_tests.rs"]
mod tests;
