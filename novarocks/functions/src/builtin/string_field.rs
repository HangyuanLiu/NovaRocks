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
//! FIELD exact selected input shapes around the original init/step/final author.
use crate::field_shared::{FieldState, Observation};
use crate::kernel_control::invalid;
use crate::kernel_input::{EvaluationCheckpoints, validate_argument_observed};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
};
use arrow_array::{Array, ArrayRef, Int32Array};
use arrow_schema::DataType;
use novarocks_type_contract::{ValueLogicalType, arrow_data_types_exact_observed};
use std::sync::Arc;
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let types = contract.selected().argument_types.as_ref();
    // The prepared owner has already refused unsupported argument shapes.
    // Repeat the ONE original count check for the checked selected contract;
    // this is ABI validation, never projection of a legacy data error.
    super::string_field_owner::check_pure_arity(types.len())
        .map_err(|error| invalid(&error.to_string()))?;
    let FunctionArgumentType::Value(first) = &types[0] else {
        return Err(invalid("FIELD requires exact value arguments"));
    };
    for ty in types {
        let FunctionArgumentType::Value(ty) = ty else {
            return Err(invalid("FIELD requires exact value arguments"));
        };
        step()?;
        let admitted = matches!(
            ty.data_type,
            DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Date32
                | DataType::Timestamp(..)
        ) || novarocks_type_contract::is_largeint_data_type(&ty.data_type);
        if !admitted
            || first.logical_type != ty.logical_type
            || !arrow_data_types_exact_observed::<KernelFailure>(
                &first.data_type,
                &ty.data_type,
                &mut step,
            )?
        {
            return Err(invalid(
                "FIELD selected comparison profile differs from its strict original binding",
            ));
        }
    }
    let target = contract.result_type();
    step()?;
    if target.logical_type != ValueLogicalType::Physical
        || target.data_type != DataType::Int32
        || target.nullable
    {
        return Err(invalid(
            "FIELD exact result is a non-null Int32 first-match index",
        ));
    }
    Ok(())
}
fn concrete(a: &dyn Array) -> bool {
    use arrow_array::*;
    match a.data_type() {
        DataType::Null => a.as_any().is::<NullArray>(),
        DataType::Boolean => a.as_any().is::<BooleanArray>(),
        DataType::Int8 => a.as_any().is::<Int8Array>(),
        DataType::Int16 => a.as_any().is::<Int16Array>(),
        DataType::Int32 => a.as_any().is::<Int32Array>(),
        DataType::Int64 => a.as_any().is::<Int64Array>(),
        DataType::Float32 => a.as_any().is::<Float32Array>(),
        DataType::Float64 => a.as_any().is::<Float64Array>(),
        DataType::Decimal128(..) => a.as_any().is::<Decimal128Array>(),
        DataType::Decimal256(..) => a.as_any().is::<Decimal256Array>(),
        DataType::FixedSizeBinary(16) => a.as_any().is::<FixedSizeBinaryArray>(),
        DataType::Utf8 => a.as_any().is::<StringArray>(),
        DataType::LargeUtf8 => a.as_any().is::<LargeStringArray>(),
        DataType::Date32 => a.as_any().is::<Date32Array>(),
        DataType::Timestamp(unit, _) => match unit {
            arrow_schema::TimeUnit::Second => a.as_any().is::<TimestampSecondArray>(),
            arrow_schema::TimeUnit::Millisecond => a.as_any().is::<TimestampMillisecondArray>(),
            arrow_schema::TimeUnit::Microsecond => a.as_any().is::<TimestampMicrosecondArray>(),
            arrow_schema::TimeUnit::Nanosecond => a.as_any().is::<TimestampNanosecondArray>(),
        },
        _ => false,
    }
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let boundary = crate::kernel_control::KernelControlObservation::new(control);
    let result = (|| {
        boundary.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(&boundary);
        let result = (|| {
            validate_profile(input.contract(), || work.step())?;
            let args = input.arguments();
            let types = input.contract().selected().argument_types.as_ref();
            if args.len() != types.len() {
                return Err(invalid(
                    "FIELD selected argument count differs from its checked call",
                ));
            }
            for (arg, ty) in args.iter().zip(types) {
                let FunctionArgumentType::Value(ty) = ty else {
                    unreachable!("validated value");
                };
                work.flush()?;
                validate_argument_observed(*arg, input.selection(), ty, &boundary)?;
                work.step()?;
                if !concrete(arg.array().as_ref()) {
                    return Err(invalid(
                        "FIELD selected carrier has a foreign concrete array implementation",
                    ));
                }
            }
            let mut values = Vec::new();
            work.flush()?;
            values
                .try_reserve_exact(input.selection().len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.flush()?;
            for (ordinal, row) in input.selection().iter().enumerate() {
                let mut project =
                    |arg: crate::EvaluatedArgument<'_>| -> Result<ArrayRef, KernelFailure> {
                        let actual = arg.value_row(ordinal, row);
                        work.step()?;
                        if actual >= arg.array().len() {
                            return Err(invalid(
                                "FIELD selected address is outside its concrete input",
                            ));
                        }
                        work.flush()?;
                        let out = arg.array().slice(actual, 1);
                        work.flush()?;
                        Ok(out)
                    };
                let first = project(args[0])?;
                // Typed guards above prove exactly the registered comparable
                // carriers and address shape. Raw-only data guards remain raw;
                // a forged ABI violation is not a maskable SQL data error.
                let mut state = FieldState::new_observed(
                    &first,
                    args.len(),
                    Some(&mut |event| match event {
                        Observation::Step => work.step(),
                        Observation::OpaqueBoundary => work.flush(),
                    }),
                )?
                .map_err(|_| {
                    invalid("FIELD checked ABI violated its original initialization guard")
                })?;
                for (index, arg) in args[1..].iter().enumerate() {
                    let actual = arg.value_row(ordinal, row);
                    work.step()?;
                    work.flush()?;
                    let candidate = arg.array().slice(actual, 1);
                    work.flush()?;
                    state
                        .step_observed(
                            index,
                            &candidate,
                            Some(&mut |event| match event {
                                Observation::Step => work.step(),
                                Observation::OpaqueBoundary => work.flush(),
                            }),
                        )?
                        .map_err(|_| {
                            invalid("FIELD checked ABI violated its original comparison guard")
                        })?;
                }
                let output = state.finish_observed(Some(&mut |event| match event {
                    Observation::Step => work.step(),
                    Observation::OpaqueBoundary => work.flush(),
                }))?;
                let value = output
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        invalid("FIELD original core returned a different concrete output")
                    })?;
                values.push(value.value(0));
                work.step()?;
            }
            work.flush()?;
            let output = Arc::new(Int32Array::from(values)) as ArrayRef;
            work.flush()?;
            SelectedValues::try_new_observed::<KernelFailure>(
                input.selection(),
                &DataType::Int32,
                output,
                Box::default(),
                || work.step(),
            )
        })();
        work.finish_result(result)
    })();
    boundary.finish(result)
}
