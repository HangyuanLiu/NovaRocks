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

//! Mandatory complete definition types owned with the same resolved program.
//! This checks carrier/signature correspondence, not primitive operation
//! typing, lexical capture closure, or correct lowering from a physical plan.
//! Source NULL/logical facts are explicit; legacy optional schemas never fill
//! an absent entry. A TruthOnly use does not change its definition's value type.

use crate::{
    MAX_STATIC_EXPRESSIONS, ProgramCallSite, ProgramExprId, ProgramExpressionArena,
    ProgramResolvedCalls, StaticExprKind,
};
use novarocks_functions::{
    FunctionArgumentType, FunctionResultType, FunctionValueType, KernelFailure,
    validate_function_value_type_observed,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl,
    ValueLogicalType, ValueTypeError, arrow_data_types_exact_observed,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

#[derive(Clone, Debug)]
pub struct ProgramTypedExpressions {
    calls: ProgramResolvedCalls,
    types: Arc<BTreeMap<ProgramExpressionArena, Arc<[FunctionArgumentType]>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramExpressionTypeError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Kernel(KernelFailure),
    TooManyDefinitions,
    IncompleteCoverage,
    WrongKind,
    WrongLambda,
    TypeMismatch,
    WrongDemand,
}
impl From<CompileControlError> for ProgramExpressionTypeError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ValueTypeError> for ProgramExpressionTypeError {
    fn from(value: ValueTypeError) -> Self {
        Self::ValueType(value)
    }
}
impl fmt::Display for ProgramExpressionTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid complete local expression types: {self:?}")
    }
}
impl std::error::Error for ProgramExpressionTypeError {}

impl ProgramTypedExpressions {
    pub fn try_new(
        calls: ProgramResolvedCalls,
        types: BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramExpressionTypeError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let arenas = calls.snapshot().roots().arenas();
        if types.len() != arenas.len() {
            return Err(ProgramExpressionTypeError::IncompleteCoverage);
        }
        // All actual definition positions share one local arena-entry budget.
        // Reusing an Arc under a different scope does not erase that position.
        let mut count = 0usize;
        for (arena, entries) in &types {
            work.step()?;
            let definitions = arenas
                .get(arena)
                .ok_or(ProgramExpressionTypeError::IncompleteCoverage)?;
            if definitions.nodes().len() != entries.len() {
                return Err(ProgramExpressionTypeError::IncompleteCoverage);
            }
            count = count
                .checked_add(entries.len())
                .filter(|n| *n <= MAX_STATIC_EXPRESSIONS)
                .ok_or(ProgramExpressionTypeError::TooManyDefinitions)?;
        }
        for (arena, entries) in &types {
            let definitions = &arenas[arena];
            for (definition, ty) in definitions.nodes().iter().zip(entries) {
                work.step()?;
                match (definition.kind(), ty) {
                    (
                        StaticExprKind::LambdaFunction {
                            body, arg_slots, ..
                        },
                        FunctionArgumentType::Lambda {
                            parameter_types,
                            result_type,
                        },
                    ) => {
                        if arg_slots.len() != parameter_types.len() {
                            return Err(ProgramExpressionTypeError::WrongLambda);
                        }
                        validate(result_type, &mut work)?;
                        same_carrier(definition.data_type(), &result_type.data_type, &mut work)?;
                        let Some(FunctionArgumentType::Value(body_type)) =
                            entries.get(body.index())
                        else {
                            return Err(ProgramExpressionTypeError::WrongLambda);
                        };
                        same_value(body_type, result_type, false, &mut work)?;
                        for parameter in parameter_types {
                            validate(parameter, &mut work)?;
                        }
                    }
                    (StaticExprKind::LambdaFunction { .. }, _)
                    | (_, FunctionArgumentType::Lambda { .. }) => {
                        return Err(ProgramExpressionTypeError::WrongKind);
                    }
                    (_, FunctionArgumentType::Value(value)) => {
                        validate(value, &mut work)?;
                        same_carrier(definition.data_type(), &value.data_type, &mut work)?;
                        if let StaticExprKind::Constant(constant) = definition.kind() {
                            same_value(constant.value_type(), value, false, &mut work)?;
                        }
                    }
                }
            }
        }
        for (arena, flow) in calls.snapshot().flows() {
            let entries = &types[arena];
            for invocation in flow.uses().values() {
                work.step()?;
                let result = match &entries[invocation.definition.index()] {
                    FunctionArgumentType::Value(value) => value,
                    // LambdaBody demand belongs to its final body result;
                    // the wrapper itself is a lexical callable, not a value.
                    FunctionArgumentType::Lambda { result_type, .. } => result_type,
                };
                if invocation.context.demand == EvaluationDemand::TruthOnly
                    && (result.data_type != arrow_schema::DataType::Boolean
                        || result.logical_type != ValueLogicalType::Physical)
                {
                    return Err(ProgramExpressionTypeError::WrongDemand);
                }
            }
        }
        for (site, resolved) in calls.calls() {
            work.step()?;
            let ProgramCallSite::Expression(occurrence) = site else {
                continue;
            };
            let flow = &calls.snapshot().flows()[&occurrence.arena];
            let invocation = &flow.uses()[&occurrence.use_id];
            let definitions = &arenas[&occurrence.arena];
            let entries = &types[&occurrence.arena];
            let definition = definitions
                .node(invocation.definition)
                .expect("resolved occurrence was checked");
            let args = match definition.kind() {
                StaticExprKind::FunctionCall { args, .. } | StaticExprKind::BoundCall { args } => {
                    args
                }
                _ => unreachable!("checked call site"),
            };
            let call = resolved.call_contract();
            let FunctionResultType::Scalar(result) = &call.selected().result_type else {
                return Err(ProgramExpressionTypeError::WrongKind);
            };
            let FunctionArgumentType::Value(actual_result) =
                &entries[invocation.definition.index()]
            else {
                return Err(ProgramExpressionTypeError::WrongKind);
            };
            same_value(actual_result, result, false, &mut work)?;
            for (argument, expected) in args.iter().zip(&call.selected().argument_types) {
                work.step()?;
                match (&entries[argument.index()], expected) {
                    (
                        FunctionArgumentType::Value(actual),
                        FunctionArgumentType::Value(expected),
                    ) => {
                        same_value(actual, expected, true, &mut work)?;
                    }
                    (
                        FunctionArgumentType::Lambda {
                            parameter_types: actual_parameters,
                            result_type: actual_result,
                        },
                        FunctionArgumentType::Lambda {
                            parameter_types: expected_parameters,
                            result_type: expected_result,
                        },
                    ) => {
                        if actual_parameters.len() != expected_parameters.len() {
                            return Err(ProgramExpressionTypeError::WrongLambda);
                        }
                        for (actual, expected) in actual_parameters.iter().zip(expected_parameters)
                        {
                            same_value(actual, expected, false, &mut work)?;
                        }
                        same_value(actual_result, expected_result, false, &mut work)?;
                    }
                    _ => return Err(ProgramExpressionTypeError::WrongKind),
                }
            }
        }
        work.finish()?;
        Ok(Self {
            calls,
            types: Arc::new(
                types
                    .into_iter()
                    .map(|(arena, entries)| (arena, Arc::from(entries)))
                    .collect(),
            ),
        })
    }
    pub const fn resolved_calls(&self) -> &ProgramResolvedCalls {
        &self.calls
    }
    pub fn types(&self) -> &BTreeMap<ProgramExpressionArena, Arc<[FunctionArgumentType]>> {
        &self.types
    }
    pub fn definition_type(
        &self,
        arena: ProgramExpressionArena,
        definition: ProgramExprId,
    ) -> Option<&FunctionArgumentType> {
        self.types.get(&arena)?.get(definition.index())
    }
}
fn validate(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    validate_function_value_type_observed(value, work).map_err(|error| match error {
        KernelFailure::Cancelled => {
            ProgramExpressionTypeError::Control(CompileControlError::Cancelled)
        }
        KernelFailure::DeadlineExceeded => {
            ProgramExpressionTypeError::Control(CompileControlError::DeadlineExceeded)
        }
        KernelFailure::ResourceExhausted => {
            ProgramExpressionTypeError::Control(CompileControlError::ResourceExhausted)
        }
        error => ProgramExpressionTypeError::Kernel(error),
    })
}
fn same_carrier(
    actual: &arrow_schema::DataType,
    expected: &arrow_schema::DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    if arrow_data_types_exact_observed::<ProgramExpressionTypeError>(actual, expected, || {
        work.step().map_err(Into::into)
    })? {
        Ok(())
    } else {
        Err(ProgramExpressionTypeError::TypeMismatch)
    }
}
fn same_value(
    actual: &FunctionValueType,
    expected: &FunctionValueType,
    argument: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    work.step()?;
    let nullability = if argument {
        expected.nullable || !actual.nullable
    } else {
        actual.nullable == expected.nullable
    };
    if actual.logical_type != expected.logical_type || !nullability {
        return Err(ProgramExpressionTypeError::TypeMismatch);
    }
    // Materialized runtime carriers must be exact, including nested metadata
    // and dictionary identity; only top-level argument NULL admission widens.
    same_carrier(&actual.data_type, &expected.data_type, work)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod call_tests;
