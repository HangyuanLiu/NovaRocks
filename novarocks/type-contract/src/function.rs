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

use arrow_schema::{DataType, Field};

const MAX_FUNCTION_IDENTITY_BYTES: usize = 1024;

macro_rules! stable_identity {
    ($name:ident, $kind:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Box<str>);

        impl $name {
            /// Validate a borrowed identity with the constructor's exact grammar.
            /// This performs no allocation and does not retain the input.
            pub fn validate_str(value: &str) -> Result<(), FunctionIdentityError> {
                validate_identity($kind, value)
            }

            pub fn try_new(value: impl AsRef<str>) -> Result<Self, FunctionIdentityError> {
                let value = value.as_ref();
                Self::validate_str(value)?;
                Ok(Self(value.into()))
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
    /// Validate a borrowed state identity with the constructor's exact grammar.
    /// This performs no allocation and does not retain the input.
    pub fn validate_str(value: &str) -> Result<(), FunctionIdentityError> {
        Self::validate_str_observed(value, || Ok(()))
    }

    /// Observe each byte actually inspected by the same identity grammar.
    /// Length rejection precedes the scan; a callback refusal is returned
    /// directly before any later byte or identity error is produced.
    pub fn validate_str_observed<E: From<FunctionIdentityError>>(
        value: &str,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        validate_identity("aggregate state format", value)?;
        for byte in value.bytes() {
            let allowed = byte.is_ascii_graphic() && !matches!(byte, b'|' | b',');
            observe()?;
            if !allowed {
                return Err(FunctionIdentityError::InvalidCharacters {
                    kind: "aggregate state format",
                }
                .into());
            }
        }
        Ok(())
    }

    pub fn try_new(value: impl AsRef<str>) -> Result<Self, FunctionIdentityError> {
        let value = value.as_ref();
        Self::validate_str(value)?;
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct FunctionValueType {
    pub data_type: DataType,
    pub nullable: bool,
    pub logical_type: crate::ValueLogicalType,
}

impl PartialEq for FunctionValueType {
    fn eq(&self, other: &Self) -> bool {
        self.nullable == other.nullable
            && self.logical_type == other.logical_type
            && crate::arrow_data_types_exact(&self.data_type, &other.data_type)
    }
}
impl Eq for FunctionValueType {}
impl std::hash::Hash for FunctionValueType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Arrow's coarser dictionary equality/hash may collide for different
        // frozen dictionary identities; exact equality still distinguishes them.
        std::hash::Hash::hash(&self.data_type, state);
        std::hash::Hash::hash(&self.nullable, state);
        std::hash::Hash::hash(&self.logical_type, state);
    }
}

impl FunctionValueType {
    pub const fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
            logical_type: crate::ValueLogicalType::Physical,
        }
    }

    /// Read an explicitly authored root domain from its actual Arrow field.
    /// Missing metadata declares Physical; no carrier confers another identity.
    pub fn try_from_field(field: &arrow_schema::Field) -> Result<Self, crate::ValueTypeError> {
        Self::try_with_logical_type(
            field.data_type().clone(),
            field.is_nullable(),
            crate::field_logical_type(field)?,
        )
    }

    /// Materialize a new schema field from this exact authored value type.
    /// Root identity is explicit metadata; nested fields remain unchanged.
    pub fn try_to_field(&self, name: impl Into<String>) -> Result<Field, crate::ValueTypeError> {
        self.validate()?;
        let field = Field::new(name, self.data_type.clone(), self.nullable);
        Ok(match self.logical_type.metadata_value() {
            Some(tag) => field
                .with_metadata([(crate::NR_LOGICAL_TYPE_KEY.to_owned(), tag.to_owned())].into()),
            None => field,
        })
    }

    pub fn try_with_logical_type(
        data_type: DataType,
        nullable: bool,
        logical_type: crate::ValueLogicalType,
    ) -> Result<Self, crate::ValueTypeError> {
        let value = Self {
            data_type,
            nullable,
            logical_type,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), crate::ValueTypeError> {
        self.logical_type.validate_carrier(&self.data_type)?;
        crate::validate_nested_logical_types(&self.data_type)
    }

    pub fn fits_value_type(&self, expected: &Self) -> bool {
        self.logical_type == expected.logical_type
            && (expected.nullable || !self.nullable)
            && fits_nested_nullability(&self.data_type, &expected.data_type)
            && self.validate().is_ok()
            && expected.validate().is_ok()
    }

    /// Compare the exact frozen value type, including nested provider and
    /// dictionary facts. This observes the borrowed traversal; it does not
    /// replace the owner's metadata admission or allocation policy.
    pub fn exactly_equals_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        if self.nullable != other.nullable || self.logical_type != other.logical_type {
            return Ok(false);
        }
        crate::arrow_data_types_exact_observed(&self.data_type, &other.data_type, observe)
    }

    pub fn same_value_domain_observed<E: From<crate::ValueTypeError>>(
        &self,
        other: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        self.logical_type.validate_carrier(&self.data_type)?;
        other.logical_type.validate_carrier(&other.data_type)?;
        if self.logical_type != other.logical_type {
            return Ok(false);
        }
        Ok(
            fits_nested_nullability_observed(&self.data_type, &other.data_type, &mut observe)?
                && fits_nested_nullability_observed(
                    &other.data_type,
                    &self.data_type,
                    &mut observe,
                )?,
        )
    }

    pub fn same_value_domain(&self, other: &Self) -> bool {
        self.logical_type == other.logical_type
            && fits_nested_nullability(&self.data_type, &other.data_type)
            && fits_nested_nullability(&other.data_type, &self.data_type)
            && self.validate().is_ok()
            && other.validate().is_ok()
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

/// Externally visible errors owned by a selected row implementation on legally
/// bound inputs. Child-expression, frozen-contract, resource and lifecycle errors
/// are outside this fact; it does not depend on `FunctionFailureBehavior`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionIntrinsicRowError {
    NoRowError,
    MayRaise,
    /// Aggregate/window owners do not evaluate a scalar row implementation.
    NotRowEvaluated,
}

impl FunctionIntrinsicRowError {
    pub const fn is_valid_for_kind(self, kind: FunctionKind) -> bool {
        match kind {
            FunctionKind::Scalar | FunctionKind::Table => !matches!(self, Self::NotRowEvaluated),
            FunctionKind::Aggregate | FunctionKind::Window => {
                matches!(self, Self::NotRowEvaluated)
            }
        }
    }
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

/// Whether a value of `actual` stands where `expected` is asked for, allowing
/// only that its nested fields admit less.
///
/// The two are the same type, except that a nested field the value never
/// writes null into may stand where one that may be null is asked for -- the
/// same direction nullability travels everywhere else. A field that may be
/// null standing where a non-null one is asked for is the mismatch.
pub fn fits_nested_nullability(actual: &DataType, expected: &DataType) -> bool {
    fits_nested_nullability_observed::<crate::ValueTypeError>(actual, expected, || Ok(()))
        .unwrap_or(false)
}

/// Preserve the same value-domain/nullability rule while observing recursive
/// schemas and pair comparisons. Nonlogical field annotations do not change
/// this semantic domain; exact physical-carrier comparison remains separate.
pub fn fits_nested_nullability_observed<E: From<crate::ValueTypeError>>(
    actual: &DataType,
    expected: &DataType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    crate::validate_nested_logical_types_observed(actual, &mut observe)?;
    crate::validate_nested_logical_types_observed(expected, &mut observe)?;
    #[allow(deprecated)]
    fn field_fits<E>(
        actual: &Field,
        expected: &Field,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        if actual.name() != expected.name()
            || (!expected.is_nullable() && actual.is_nullable())
            || actual.dict_id() != expected.dict_id()
            || actual.dict_is_ordered() != expected.dict_is_ordered()
            || crate::field_logical_type(actual) != crate::field_logical_type(expected)
        {
            return Ok(false);
        }
        fits(actual.data_type(), expected.data_type(), observe)
    }
    fn fits<E>(
        actual: &DataType,
        expected: &DataType,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        match (actual, expected) {
            (DataType::List(actual), DataType::List(expected))
            | (DataType::LargeList(actual), DataType::LargeList(expected))
            | (DataType::ListView(actual), DataType::ListView(expected))
            | (DataType::LargeListView(actual), DataType::LargeListView(expected)) => {
                field_fits(actual, expected, observe)
            }
            (DataType::FixedSizeList(actual, al), DataType::FixedSizeList(expected, el)) => {
                if al != el {
                    Ok(false)
                } else {
                    field_fits(actual, expected, observe)
                }
            }
            (DataType::Struct(actual), DataType::Struct(expected)) => {
                if actual.len() != expected.len() {
                    return Ok(false);
                }
                for (actual, expected) in actual.iter().zip(expected) {
                    if !field_fits(actual, expected, observe)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Map(actual, asorted), DataType::Map(expected, esorted)) => {
                if asorted != esorted {
                    Ok(false)
                } else {
                    field_fits(actual, expected, observe)
                }
            }
            (DataType::Union(actual, amode), DataType::Union(expected, emode)) => {
                if amode != emode || actual.len() != expected.len() {
                    return Ok(false);
                }
                for ((ai, actual), (ei, expected)) in actual.iter().zip(expected.iter()) {
                    if ai != ei || !field_fits(actual, expected, observe)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Dictionary(ak, av), DataType::Dictionary(ek, ev)) => {
                Ok(fits(ak, ek, observe)? && fits(av, ev, observe)?)
            }
            (DataType::RunEndEncoded(ar, av), DataType::RunEndEncoded(er, ev)) => {
                Ok(field_fits(ar, er, observe)? && field_fits(av, ev, observe)?)
            }
            (actual, expected) => Ok(actual == expected),
        }
    }
    fits(actual, expected, &mut observe)
}

#[cfg(test)]
mod tests {
    use super::{AggregateStateFormatId, FunctionId, FunctionIdentityError};

    #[test]
    fn observed_value_type_equality_preserves_exact_frozen_identity() {
        use crate::{FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType, ValueTypeError};
        use arrow_schema::{DataType, Field};
        #[allow(deprecated)]
        let nested = |dict_id, ordered, nullable, annotation: &str, logical: &str| {
            FunctionValueType::new(
                DataType::Struct(
                    vec![
                        Field::new_dict(
                            "dictionary",
                            DataType::Dictionary(
                                Box::new(DataType::Int8),
                                Box::new(DataType::Utf8),
                            ),
                            nullable,
                            dict_id,
                            ordered,
                        )
                        .with_metadata([("provider".to_owned(), annotation.to_owned())].into()),
                        if logical.is_empty() {
                            Field::new("payload", DataType::Utf8, false)
                        } else {
                            Field::new("payload", DataType::Utf8, false).with_metadata(
                                [(NR_LOGICAL_TYPE_KEY.to_owned(), logical.to_owned())].into(),
                            )
                        },
                    ]
                    .into(),
                ),
                false,
            )
        };
        let value = nested(7, false, true, "source", "json");
        for other in [
            value.clone(),
            nested(8, false, true, "source", "json"),
            nested(7, true, true, "source", "json"),
            nested(7, false, false, "source", "json"),
            nested(7, false, true, "other", "json"),
            nested(7, false, true, "source", ""),
        ] {
            assert_eq!(
                value
                    .exactly_equals_observed::<ValueTypeError>(&other, || Ok(()))
                    .unwrap(),
                value == other,
            );
        }
        let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
        let uuid = FunctionValueType::try_with_logical_type(
            physical.data_type.clone(),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap();
        assert!(
            !physical
                .exactly_equals_observed::<ValueTypeError>(&uuid, || Ok(()))
                .unwrap()
        );
        assert!(
            !physical
                .exactly_equals_observed::<ValueTypeError>(
                    &FunctionValueType::new(physical.data_type.clone(), true),
                    || Ok(()),
                )
                .unwrap()
        );
    }

    #[test]
    fn observed_value_type_equality_preserves_control_failure_at_entry_and_interior() {
        use crate::{CompileControlError, FunctionValueType, ValueTypeError};
        use arrow_schema::{DataType, Field};
        #[derive(Debug, PartialEq)]
        enum Failure {
            Type(ValueTypeError),
            Control(CompileControlError),
        }
        impl From<ValueTypeError> for Failure {
            fn from(value: ValueTypeError) -> Self {
                Self::Type(value)
            }
        }
        let value = FunctionValueType::new(
            DataType::Struct(
                (0..320)
                    .map(|index| Field::new(index.to_string(), DataType::Int32, false))
                    .collect(),
            ),
            false,
        );
        let mut visits = 0;
        assert!(
            value
                .exactly_equals_observed::<ValueTypeError>(&value, || {
                    visits += 1;
                    Ok(())
                })
                .unwrap()
        );
        assert!(visits > 960);
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop in [1, 256, visits] {
                let mut observed = 0;
                let result = value.exactly_equals_observed::<Failure>(&value, || {
                    observed += 1;
                    if observed == stop {
                        Err(Failure::Control(failure))
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(result, Err(Failure::Control(failure)));
                assert_eq!(observed, stop);
            }
        }
    }

    #[test]
    fn source_field_value_domain_is_explicit_and_validated() {
        use crate::{FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType};
        use arrow_schema::{DataType, Field};
        let physical = Field::new("bytes", DataType::FixedSizeBinary(16), false);
        assert_eq!(
            FunctionValueType::try_from_field(&physical).unwrap(),
            FunctionValueType::new(DataType::FixedSizeBinary(16), false)
        );
        let uuid = physical
            .clone()
            .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), "uuid".to_owned())].into());
        let exact_uuid = FunctionValueType::try_from_field(&uuid).unwrap();
        assert_eq!(exact_uuid.logical_type, ValueLogicalType::Uuid);
        assert_eq!(
            FunctionValueType::try_from_field(&exact_uuid.try_to_field("roundtrip").unwrap())
                .unwrap(),
            exact_uuid
        );
        assert!(
            FunctionValueType::try_from_field(&uuid.with_data_type(DataType::FixedSizeBinary(15)))
                .is_err()
        );
        let unknown =
            physical.with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), "unknown".to_owned())].into());
        assert!(FunctionValueType::try_from_field(&unknown).is_err());
        let item = Field::new("provider_item", DataType::Utf8, false).with_metadata(
            [
                (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
                ("provider.field_id".to_owned(), "91".to_owned()),
            ]
            .into(),
        );
        let nested = Field::new(
            "values",
            DataType::LargeList(std::sync::Arc::new(item)),
            true,
        );
        assert_eq!(
            FunctionValueType::try_from_field(&nested).unwrap(),
            FunctionValueType::new(nested.data_type().clone(), true)
        );
    }

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

#[cfg(test)]
#[path = "function/identity_tests.rs"]
mod identity_tests;
