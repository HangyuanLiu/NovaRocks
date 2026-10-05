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

//! Neutral original call requests and exact binding failures.
//!
//! A request retains the original static argument facts independently of its
//! selected signature and runtime expression shape. The constant carrier may
//! be an admitted value or a transport owner's checked pool reference. This
//! data grants no SQL provenance, effects, installed implementation or memory.

use novarocks_constant_contract::ConstantValue;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionArgumentType,
    FunctionOverloadId, FunctionValueType, PureCompileControl,
};
use std::fmt;

/// Original static arguments, independent of runtime expression shape.
/// `None` is nonconstant. The default admitted-value carrier retains typed
/// NULL, selected ordinal, original Field and shared backing. A transport
/// owner may instead carry a checked reference into its sole pool namespace.
/// Lambdas cannot carry a scalar constant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionArgument<C = ConstantValue> {
    Value {
        value_type: FunctionValueType,
        constant: Option<C>,
    },
    Lambda {
        parameter_types: Box<[FunctionValueType]>,
        result_type: FunctionValueType,
    },
}

impl FunctionArgument {
    /// Compare actual checked values and complete argument types. Pool IDs,
    /// unused rows and backing addresses are not semantic value equality.
    pub fn equals_observed(
        &self,
        other: &Self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<bool, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = (|| match (self, other) {
            (
                Self::Value {
                    value_type: left,
                    constant: left_constant,
                },
                Self::Value {
                    value_type: right,
                    constant: right_constant,
                },
            ) => {
                if !left.exactly_equals_observed(right, || {
                    work.step().map_err(FunctionBindingError::from)
                })? {
                    return Ok(false);
                }
                match (left_constant, right_constant) {
                    (None, None) => Ok(true),
                    (Some(left), Some(right)) => {
                        work.flush()?;
                        let equal = left.equals_observed(right, phase, work.control())?;
                        work.flush()?;
                        Ok(equal)
                    }
                    _ => Ok(false),
                }
            }
            (
                Self::Lambda {
                    parameter_types: left,
                    result_type: left_result,
                },
                Self::Lambda {
                    parameter_types: right,
                    result_type: right_result,
                },
            ) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (left, right) in left.iter().zip(right) {
                    if !left.exactly_equals_observed(right, || {
                        work.step().map_err(FunctionBindingError::from)
                    })? {
                        return Ok(false);
                    }
                }
                left_result.exactly_equals_observed(right_result, || {
                    work.step().map_err(FunctionBindingError::from)
                })
            }
            _ => Ok(false),
        })();
        finish_binding_work(result, work)
    }
}

impl<C> FunctionArgument<C> {
    pub fn argument_type(&self) -> FunctionArgumentType {
        match self {
            Self::Value { value_type, .. } => FunctionArgumentType::Value(value_type.clone()),
            Self::Lambda {
                parameter_types,
                result_type,
            } => FunctionArgumentType::Lambda {
                parameter_types: parameter_types.clone(),
                result_type: result_type.clone(),
            },
        }
    }

    /// Match the selected type using the owner's original nullability rules.
    /// The caller owns entry/completion and admission for the same meter.
    /// This proves neither constant provenance nor installed capability.
    pub fn matches_type_observed(
        &self,
        expected: &FunctionArgumentType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, FunctionBindingError> {
        match (self, expected) {
            (Self::Value { value_type, .. }, FunctionArgumentType::Value(expected)) => {
                work.step()?;
                if value_type.logical_type != expected.logical_type
                    || (value_type.nullable && !expected.nullable)
                {
                    return Ok(false);
                }
                novarocks_type_contract::fits_nested_nullability_observed(
                    &value_type.data_type,
                    &expected.data_type,
                    || work.step().map_err(FunctionBindingError::from),
                )
            }
            (
                Self::Lambda {
                    parameter_types,
                    result_type,
                },
                FunctionArgumentType::Lambda {
                    parameter_types: expected_parameters,
                    result_type: expected_result,
                },
            ) => {
                if parameter_types.len() != expected_parameters.len() {
                    return Ok(false);
                }
                for (actual, expected) in parameter_types.iter().zip(expected_parameters) {
                    if !actual.exactly_equals_observed(expected, || {
                        work.step().map_err(FunctionBindingError::from)
                    })? {
                        return Ok(false);
                    }
                }
                result_type.exactly_equals_observed(expected_result, || {
                    work.step().map_err(FunctionBindingError::from)
                })
            }
            _ => Ok(false),
        }
    }
}

#[derive(Debug)]
pub struct FunctionBindingRequest<'a, C = ConstantValue> {
    /// Logical arguments followed by aggregate-owned ORDER BY update channels.
    pub arguments: &'a [FunctionArgument<C>],
    /// Equals arguments.len() for every non-aggregate function.
    pub logical_argument_count: usize,
    /// An explicit syntax result constraint for an owner that declares a
    /// context-typed result, such as a zero-element typed array. This never
    /// authorizes a consumer to replace an already selected result type.
    pub expected_result_type: Option<&'a FunctionValueType>,
}

impl<C> Copy for FunctionBindingRequest<'_, C> {}
impl<C> Clone for FunctionBindingRequest<'_, C> {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionBindingError {
    Control(CompileControlError),
    MissingEffectDeclaration(FunctionOverloadId),
    UnknownFunction,
    HiddenFunction,
    MissingBindingDeclaration,
    UnknownOverload(FunctionOverloadId),
    DuplicateOverload(FunctionOverloadId),
    NoMatchingOverload,
    AmbiguousOverload,
    InvalidBinding(Box<str>),
}

fn invalid(message: &str) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(message.into())
}

impl fmt::Display for FunctionBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::MissingEffectDeclaration(identity) => write!(
                formatter,
                "selected overload `{}` has no complete effect declaration",
                identity.as_str()
            ),
            Self::UnknownFunction => formatter.write_str("function is not registered"),
            Self::HiddenFunction => formatter.write_str("function is hidden from user SQL"),
            Self::MissingBindingDeclaration => {
                formatter.write_str("function has no exact binding declaration")
            }
            Self::UnknownOverload(identity) => write!(
                formatter,
                "unknown selected overload `{}`",
                identity.as_str()
            ),
            Self::DuplicateOverload(identity) => write!(
                formatter,
                "duplicate overload identity `{}`",
                identity.as_str()
            ),
            Self::NoMatchingOverload => formatter.write_str("no matching declared overload"),
            Self::AmbiguousOverload => {
                formatter.write_str("multiple declared overloads match ambiguously")
            }
            Self::InvalidBinding(message) => {
                write!(formatter, "invalid function binding: {message}")
            }
        }
    }
}

impl std::error::Error for FunctionBindingError {}

impl From<CompileControlError> for FunctionBindingError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl FunctionBindingError {
    pub fn control_error(&self) -> Option<CompileControlError> {
        match self {
            Self::Control(error) => Some(*error),
            _ => None,
        }
    }
}

impl From<novarocks_type_contract::ValueTypeError> for FunctionBindingError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        invalid(&error.to_string())
    }
}

impl From<novarocks_constant_contract::ConstantError> for FunctionBindingError {
    fn from(error: novarocks_constant_contract::ConstantError) -> Self {
        match error {
            novarocks_constant_contract::ConstantError::Control(error) => Self::Control(error),
            novarocks_constant_contract::ConstantError::Limit(_) => {
                Self::Control(CompileControlError::ResourceExhausted)
            }
            error => invalid(&error.to_string()),
        }
    }
}

fn finish_binding_work<T>(
    result: Result<T, FunctionBindingError>,
    work: CompileCheckpoints<'_>,
) -> Result<T, FunctionBindingError> {
    if matches!(result, Err(FunctionBindingError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
mod tests;
