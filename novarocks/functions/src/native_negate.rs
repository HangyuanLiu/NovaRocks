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
//! Prepared native NEGATE using the sole original typed-zero/Sub author.
//! One-row invocations bound every owned original loop independently of output demand.
use crate::arithmetic::ArithmeticPrepareError;
use crate::kernel_control::invalid;
use crate::legacy_literal::{LegacyLiteralValue, native_negate_zero};
use crate::{
    EvaluatedArgument, EvaluationCheckpoints, KernelEvaluationControl, KernelFailure, RowDataError,
    ScopedExpressionEffects,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, ExpressionEffectContext,
    ExpressionEffects, FunctionValueType, PureCompileControl, ValueLogicalType,
};

/// Complete admission fact for the original native zero/Sub implementation.
/// This does not change an original SQL result declaration or run row work.
/// Carrier values that Arrow can hold outside decimal precision are retained
/// in the original input domain: OutputNull remains a successful NULL there.
pub fn native_negate_computed_result_type(
    source: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<FunctionValueType, ArithmeticPrepareError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let outcome = (|| {
        work.flush()?;
        let mut result = source.clone();
        work.step()?;
        result.nullable |= matches!(
            source.data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        );
        work.step()?;
        let prepared = PreparedNativeNegateRecipe::try_new(source, &result, control)?;
        Ok(prepared.result)
    })();
    if outcome
        .as_ref()
        .err()
        .is_some_and(|error: &ArithmeticPrepareError| error.control_error().is_some())
    {
        return outcome;
    }
    work.finish()?;
    outcome
}

#[derive(Clone, Debug)]
pub struct PreparedNativeNegateRecipe {
    source: FunctionValueType,
    result: FunctionValueType,
    zero: LegacyLiteralValue,
}
#[derive(Clone, Debug)]
pub enum NativeNegateRowResult {
    Value(ArrayRef),
    RowError(RowDataError),
}
impl PreparedNativeNegateRecipe {
    pub fn try_new(
        source: &FunctionValueType,
        result: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ArithmeticPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome = (|| {
            crate::kernel_input::validate_type_observed(source, &mut work)
                .map_err(ArithmeticPrepareError::Kernel)?;
            crate::kernel_input::validate_type_observed(result, &mut work)
                .map_err(ArithmeticPrepareError::Kernel)?;
            // The original native zero author supplies both supported types and
            // full refusal messages. Numeric classification alone is not admission.
            let zero = native_negate_zero(&source.data_type)
                .map_err(|message| ArithmeticPrepareError::Kernel(invalid(&message)))?;
            let actual_domain = source.logical_type == ValueLogicalType::Physical
                && !matches!(source.data_type, DataType::FixedSizeBinary(_))
                || source.logical_type == ValueLogicalType::LargeInt
                    && source.data_type == DataType::FixedSizeBinary(16);
            work.step()?;
            if !actual_domain
                || source.logical_type != result.logical_type
                || source.data_type != result.data_type
                || (source.nullable && !result.nullable)
            {
                return Err(ArithmeticPrepareError::TypeMismatch);
            }
            // Original narrow MIN produces a successful NULL on Arrow output
            // cast. Its nonnullable physical result receipt is not truthful.
            if !result.nullable
                && matches!(
                    source.data_type,
                    DataType::Int8 | DataType::Int16 | DataType::Int32
                )
            {
                return Err(ArithmeticPrepareError::Kernel(invalid(
                    "native NEGATE narrow signed result requires its actual nullable output domain",
                )));
            }
            if !result.nullable
                && matches!(
                    source.data_type,
                    DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
                )
            {
                return Err(ArithmeticPrepareError::Kernel(invalid(
                    "native NEGATE full decimal carrier requires its actual nullable output domain",
                )));
            }
            work.step()?;
            Ok(Self {
                source: source.clone(),
                result: result.clone(),
                zero,
            })
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
    pub fn source_type(&self) -> &FunctionValueType {
        &self.source
    }
    pub fn result_type(&self) -> &FunctionValueType {
        &self.result
    }
    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        ScopedExpressionEffects::primitive(
            context,
            ExpressionEffects {
                may_raise_row_error: self.source.data_type == DataType::Int64,
                ..ExpressionEffects::PURE_VALUE
            },
        )
    }
    /// Assemble only demanded rows. Child row errors remain required and are
    /// never mistaken for successful strict NULLs. The original Arrow concat
    /// builds the exact scalar output; no second numeric builder is authored.
    pub fn evaluate_selected<'a>(
        &self,
        argument: EvaluatedArgument<'a>,
        selection: crate::Selection<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<crate::SelectedValues<'a>, KernelFailure> {
        let observed = crate::KernelControlObservation::new(control);
        observed.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(&observed);
        let outcome = (|| {
            if let EvaluatedArgument::SelectedColumn(values) = argument {
                if !values
                    .selection()
                    .same_rows_observed(selection, || work.step())?
                {
                    return Err(invalid(
                        "native NEGATE compact argument has a foreign selection",
                    ));
                }
            } else {
                argument.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
            }
            let inherited = match argument {
                EvaluatedArgument::SelectedColumn(values) => values.errors(),
                _ => &[],
            };
            let mut next_error = inherited.iter().peekable();
            let mut rows = Vec::with_capacity(selection.len());
            let mut errors = Vec::new();
            work.flush()?;
            for ordinal in 0..selection.len() {
                let row = selection
                    .row(ordinal)
                    .ok_or_else(|| invalid("native NEGATE selection has a missing address"))?;
                work.step()?;
                if next_error
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == ordinal)
                {
                    let error = next_error
                        .next()
                        .ok_or_else(|| invalid("native NEGATE inherited error is missing"))?;
                    errors.push(error.clone());
                    rows.push(arrow_array::new_null_array(&self.result.data_type, 1));
                    work.step()?;
                } else {
                    work.flush()?;
                    match self.evaluate_row(argument, ordinal, row, &observed)? {
                        NativeNegateRowResult::Value(value) => rows.push(value),
                        NativeNegateRowResult::RowError(error) => {
                            errors.push(error);
                            rows.push(arrow_array::new_null_array(&self.result.data_type, 1));
                        }
                    }
                    work.step()?;
                }
            }
            work.flush()?;
            let output = if rows.is_empty() {
                arrow_array::new_empty_array(&self.result.data_type)
            } else {
                let mut arrays = Vec::with_capacity(rows.len());
                for array in &rows {
                    arrays.push(array.as_ref());
                    work.step()?;
                }
                work.flush()?;
                arrow_select::concat::concat(&arrays).map_err(|error| {
                    crate::kernel_control::internal(&format!(
                        "native NEGATE selected assembly: {error}"
                    ))
                })?
            };
            work.flush()?;
            crate::SelectedValues::try_new_observed(
                selection,
                &self.result.data_type,
                output,
                errors.into_boxed_slice(),
                || work.step(),
            )
        })();
        observed.finish(work.finish_result(outcome))
    }

    /// The host first removes inherited row errors. This method preserves
    /// actual Pool/Scalar/Batch/Compact addresses and executes no unselected row.
    pub fn evaluate_row(
        &self,
        argument: EvaluatedArgument<'_>,
        ordinal: usize,
        logical_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<NativeNegateRowResult, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let outcome = (|| {
            crate::arithmetic::checked_row(
                argument,
                ordinal,
                logical_row,
                &self.source,
                |_| Ok(()),
                &mut work,
            )?;
            let row = argument.value_row(ordinal, logical_row);
            // Flush before slices and original builders. Memory authorization
            // remains the host's responsibility, independent of observation.
            work.flush()?;
            let operand = argument.array().slice(row, 1);
            let zero =
                crate::legacy_literal::eval(&self.zero, 1).map_err(|message| invalid(&message))?;
            work.flush()?;
            // Same source/result dtype and one row bound the original owned
            // loops: D128 factors stop at the 39th checked multiply, twice;
            // D256 precision uses at most 76 multiplies and scale factors are
            // identity. This 256-unit observation is a work bound, never a grant.
            for _ in 0..256 {
                work.step()?;
            }
            let answer = crate::legacy_arithmetic::eval_sub_arrays(
                zero,
                operand,
                self.result.data_type.clone(),
                false,
                DecimalOverflowPolicy::OutputNull,
            );
            work.flush()?;
            match answer {
                Ok(array) => Ok(NativeNegateRowResult::Value(array)),
                Err(message) => Ok(NativeNegateRowResult::RowError(RowDataError::new(
                    ordinal, &message,
                ))),
            }
        })();
        work.finish_result(outcome)
    }
}

#[cfg(test)]
#[path = "native_negate_tests.rs"]
mod tests;
