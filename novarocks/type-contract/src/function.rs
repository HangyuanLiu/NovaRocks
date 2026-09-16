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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use arrow_schema::DataType;

const MAX_FUNCTION_IDENTITY_BYTES: usize = 1024;

macro_rules! stable_identity {
    ($name:ident, $kind:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Box<str>);

        impl $name {
            pub fn try_new(value: impl AsRef<str>) -> Result<Self, FunctionIdentityError> {
                validate_identity($kind, value.as_ref()).map(|()| Self(value.as_ref().into()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

stable_identity!(FunctionId, "function");
stable_identity!(FunctionOverloadId, "function overload");

/// Stable identity of an aggregate's serialized intermediate state.
///
/// The delimiter exclusions keep catalog digests structurally unambiguous.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AggregateStateFormatId(Box<str>);

impl AggregateStateFormatId {
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, FunctionIdentityError> {
        let value = value.as_ref();
        validate_identity("aggregate state format", value)?;
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'|' | b','))
        {
            return Err(FunctionIdentityError::InvalidCharacters {
                kind: "aggregate state format",
            });
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FunctionValueType {
    pub data_type: DataType,
    pub nullable: bool,
}

impl FunctionValueType {
    pub const fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }
}

/// Exact shape of one bound function argument.
///
/// A lambda is executable syntax with its own parameter contract, rather than
/// a scalar value typed as its body.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FunctionArgumentType {
    Value(FunctionValueType),
    Lambda {
        parameter_types: Box<[FunctionValueType]>,
        result_type: FunctionValueType,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
    Window,
    Table,
}

/// Stability of a result within and across workers for the same logical input.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionVolatility {
    #[default]
    Immutable,
    Stable,
    Volatile,
}

impl FunctionVolatility {
    pub const fn is_volatile(self) -> bool {
        matches!(self, Self::Volatile)
    }

    pub const fn is_replica_deterministic(self) -> bool {
        matches!(self, Self::Immutable)
    }
}

/// Whether an implementation may decide which argument expressions to run.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionArgumentEvaluation {
    Eager,
    ShortCircuit,
}

/// How failures produced by the selected implementation are exposed.
///
/// This does not change failures raised while evaluating argument expressions.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionFailureBehavior {
    Propagate,
    ReturnsNull,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionIdentityError {
    Empty { kind: &'static str },
    TooLong { kind: &'static str, actual: usize },
    InvalidCharacters { kind: &'static str },
}

impl fmt::Display for FunctionIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { kind } => write!(formatter, "{kind} identity is empty"),
            Self::TooLong { kind, actual } => write!(
                formatter,
                "{kind} identity is {actual} bytes, exceeding {MAX_FUNCTION_IDENTITY_BYTES}"
            ),
            Self::InvalidCharacters { kind } => {
                write!(
                    formatter,
                    "{kind} identity contains non-canonical characters"
                )
            }
        }
    }
}

impl std::error::Error for FunctionIdentityError {}

fn validate_identity(kind: &'static str, value: &str) -> Result<(), FunctionIdentityError> {
    if value.is_empty() {
        return Err(FunctionIdentityError::Empty { kind });
    }
    if value.len() > MAX_FUNCTION_IDENTITY_BYTES {
        return Err(FunctionIdentityError::TooLong {
            kind,
            actual: value.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AggregateStateFormatId, FunctionId, FunctionIdentityError};

    #[test]
    fn stable_function_identity_is_bounded() {
        assert_eq!(
            FunctionId::try_new("builtin/lower/v1").unwrap().as_str(),
            "builtin/lower/v1"
        );
        assert!(matches!(
            FunctionId::try_new(""),
            Err(FunctionIdentityError::Empty { kind: "function" })
        ));
        assert!(matches!(
            FunctionId::try_new("x".repeat(1025)),
            Err(FunctionIdentityError::TooLong { actual: 1025, .. })
        ));
    }

    #[test]
    fn aggregate_state_format_is_canonical_and_unambiguous() {
        assert!(AggregateStateFormatId::try_new("builtin/sum/state-v1").is_ok());
        for invalid in ["state with space", "state|v1", "state,v1"] {
            assert!(matches!(
                AggregateStateFormatId::try_new(invalid),
                Err(FunctionIdentityError::InvalidCharacters { .. })
            ));
        }
    }
}
