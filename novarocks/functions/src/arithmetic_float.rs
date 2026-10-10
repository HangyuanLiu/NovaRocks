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

//! Exact original Float arithmetic adapters. The installed CAST author owns
//! conversion; locked ArrowNativeTypeOp owns the original primitive mathematics.
use super::{ArithmeticPrepareError, ArithmeticRowResult};
use crate::kernel_control::internal;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    CastOperation, CastPrepareError, CastRowResult, EvaluatedArgument, KernelEvaluationControl,
    KernelFailure, PreparedCastRecipe,
};
use arrow_array::ArrowNativeTypeOp;
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArithmeticOperator, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
    ValueLogicalType,
};
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FloatArithmetic {
    left: PreparedCastRecipe,
    right: PreparedCastRecipe,
    operator: ArithmeticOperator,
    mask_right_zero: bool,
}
impl FloatArithmetic {
    pub(super) fn prepare(
        operator: ArithmeticOperator,
        left: &FunctionValueType,
        right: &FunctionValueType,
        policy: DecimalOverflowPolicy,
        allow: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Option<Self>, ArithmeticPrepareError> {
        let successful = |ty: &FunctionValueType| {
            ty.logical_type == ValueLogicalType::Physical
                && matches!(
                    ty.data_type,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Decimal128(..)
                )
        };
        let is_float =
            |ty: &FunctionValueType| matches!(ty.data_type, DataType::Float32 | DataType::Float64);
        // LARGEINT/Float is a legitimate frozen FVT but its original Arrow setup
        // is invocation-wide Data. This kernel-only slice does not claim that port.
        if !successful(left) || !successful(right) || !(is_float(left) || is_float(right)) {
            return Ok(None);
        }
        let target = FunctionValueType::new(DataType::Float64, true);
        let prepare = |source: &FunctionValueType| {
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                source,
                &target,
                policy,
                allow,
                control,
            )
            .map_err(|error| match error {
                CastPrepareError::Control(cause) => ArithmeticPrepareError::Control(cause),
                CastPrepareError::Kernel(cause) => ArithmeticPrepareError::Kernel(cause),
                CastPrepareError::Unsupported => ArithmeticPrepareError::Unsupported,
                CastPrepareError::TypeMismatch => ArithmeticPrepareError::TypeMismatch,
            })
        };
        let left = prepare(left)?;
        let right_cast = prepare(right)?;
        // Original nullify_zeros deliberately omits F32 and Decimal128; their
        // zero values reach the same original unchecked IEEE division author.
        let mask_right_zero = crate::legacy_arithmetic::division_masks_zeros(&right.data_type);
        Ok(Some(Self {
            left,
            right: right_cast,
            operator,
            mask_right_zero,
        }))
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Retain both actual compact and logical addresses"
    )]
    pub(super) fn evaluate_row(
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
        // Both conversions validate their original full address/type/NULL
        // contract before the strict binary NULL projection. No new decoder.
        let left = self
            .left
            .evaluate_row(left, left_ordinal, left_logical_row, control)?;
        let right = self
            .right
            .evaluate_row(right, right_ordinal, right_logical_row, control)?;
        let mut work = EvaluationCheckpoints::new(control);
        let outcome = (|| {
            work.step()?;
            if matches!(left, CastRowResult::Null) || matches!(right, CastRowResult::Null) {
                return Ok(ArithmeticRowResult::Null);
            }
            let (CastRowResult::Float64(left), CastRowResult::Float64(right)) = (left, right)
            else {
                return Err(internal(
                    "floating arithmetic CAST returned a foreign original result",
                ));
            };
            let output = if self.operator == ArithmeticOperator::Divide
                && self.mask_right_zero
                && right.is_zero()
            {
                ArithmeticRowResult::Null
            } else {
                // Sole original mathematics: Arrow numeric::float_op dispatches
                // to exactly these methods in locked Arrow 58.2.0 as well.
                ArithmeticRowResult::Float(match self.operator {
                    ArithmeticOperator::Add => left.add_wrapping(right),
                    ArithmeticOperator::Subtract => left.sub_wrapping(right),
                    ArithmeticOperator::Multiply => left.mul_wrapping(right),
                    ArithmeticOperator::Divide => left.div_wrapping(right),
                    ArithmeticOperator::Modulo => left.mod_wrapping(right),
                })
            };
            work.step()?;
            Ok(output)
        })();
        work.finish()?;
        outcome
    }
}
#[cfg(test)]
#[path = "arithmetic_float_tests.rs"]
mod tests;
