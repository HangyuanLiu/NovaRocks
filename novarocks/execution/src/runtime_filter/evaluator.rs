// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Execution-owned runtime-filter row evaluation.
//!
//! The artifact query is deliberately narrower than an evaluator: a Backend
//! adapter can expose immutable, validated artifact facts, but it never sees
//! Arrow arrays and cannot make a fragment-local row or scan decision.
// Design: ADR-0042 (docs/adr/ADR-0042-runtime-filter-artifact-query-and-evaluator-boundary.md)

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_spi::connector::ConnectorScalarValue;

use super::{
    LogicalVersion, RuntimeFilterBindingId, RuntimeFilterContractViolation,
    RuntimeFilterContractViolationKind,
};

/// A borrowed, execution-owned scalar extracted from an Arrow input column.
///
/// The containing [`RuntimeFilterArtifactQuery`] has already frozen the Arrow
/// data type.  This value therefore intentionally carries no lossy type tag:
/// callers must never infer a timestamp unit, decimal scale, or LargeInt width
/// from its payload.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RuntimeFilterScalarRef<'a> {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Utf8(&'a str),
    Date32(i32),
    TimestampSecond(i64),
    TimestampMillisecond(i64),
    TimestampMicrosecond(i64),
    TimestampNanosecond(i64),
    Decimal128(i128),
}

/// Closed failure categories for an immutable artifact query.
///
/// `ContractViolation` means the retained artifact no longer matches the
/// fragment contract and is fail-fast.  The other variants are ordinary
/// fail-open evaluation outcomes owned by Execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterArtifactQueryError {
    Unsupported,
    ResourceUnavailable,
    ContractViolation,
}

/// Neutral, immutable queries over one retained runtime-filter artifact.
///
/// Implementations belong to the participant owner.  This trait deliberately
/// accepts neither Arrow batches nor connector scan facts and returns neither
/// a Boolean mask nor an evaluation outcome/effect.  Execution alone combines
/// those artifact facts with fragment data and decides fail-open behavior.
pub trait RuntimeFilterArtifactQuery: Send + Sync {
    /// The exact, frozen Arrow data type accepted by this artifact.
    fn data_type(&self) -> &DataType;

    /// Whether a null input value can match the artifact under its frozen null
    /// semantics.
    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError>;

    /// Whether the artifact contains any non-null matches.
    fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError>;

    /// Whether one non-null scalar can match the artifact.
    fn non_null_value_may_match(
        &self,
        value: RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, RuntimeFilterArtifactQueryError>;

    /// Whether an inclusive closed connector range can match the artifact.
    /// This is a primitive only; Execution owns all scan-facts validation and
    /// final scan-unit outcomes.
    fn non_null_range_may_match(
        &self,
        inclusive_min: &ConnectorScalarValue,
        inclusive_max: &ConnectorScalarValue,
    ) -> Result<bool, RuntimeFilterArtifactQueryError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterRowNotEvaluatedReason {
    DataTypeUnsupported,
    ArtifactQueryUnsupported,
    ResourceUnavailable,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeFilterRowEvaluation {
    Evaluated {
        mask: BooleanArray,
        logical_version: LogicalVersion,
    },
    NotEvaluated {
        reason: RuntimeFilterRowNotEvaluatedReason,
        observed_version: LogicalVersion,
    },
}

/// The binding-local result of evaluating one Arrow input column.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterRowOutcome {
    binding_id: RuntimeFilterBindingId,
    evaluation: RuntimeFilterRowEvaluation,
}

impl RuntimeFilterRowOutcome {
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }

    pub const fn evaluation(&self) -> &RuntimeFilterRowEvaluation {
        &self.evaluation
    }

    /// Effects are derived from an evaluated mask only.  In particular, a
    /// fail-open outcome cannot fabricate profile deltas for a version it did
    /// not evaluate.
    pub fn effect(&self) -> Option<RuntimeFilterRowEffect> {
        let RuntimeFilterRowEvaluation::Evaluated {
            mask,
            logical_version,
        } = &self.evaluation
        else {
            return None;
        };
        Some(RuntimeFilterRowEffect {
            binding_id: self.binding_id,
            logical_version: *logical_version,
            input_rows: mask.len() as u64,
            output_rows: mask.values().iter().filter(|matched| *matched).count() as u64,
        })
    }
}

/// A profile/event fact that can only be obtained from an evaluated row mask.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterRowEffect {
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    input_rows: u64,
    output_rows: u64,
}

impl RuntimeFilterRowEffect {
    pub const fn binding_id(self) -> RuntimeFilterBindingId {
        self.binding_id
    }

    pub const fn logical_version(self) -> LogicalVersion {
        self.logical_version
    }

    pub const fn input_rows(self) -> u64 {
        self.input_rows
    }

    pub const fn output_rows(self) -> u64 {
        self.output_rows
    }
}

/// Evaluates one Arrow input column through an immutable artifact query.
///
/// The input type must exactly equal the artifact's frozen type.  A mismatch
/// is an invalid fragment contract, not a best-effort conversion.  Supported
/// Arrow values are converted into [`RuntimeFilterScalarRef`] one row at a
/// time, so participant adapters never receive Arrow memory.
pub fn evaluate_rows(
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    artifact: &dyn RuntimeFilterArtifactQuery,
    input: &ArrayRef,
) -> Result<RuntimeFilterRowOutcome, RuntimeFilterContractViolation> {
    if input.data_type() != artifact.data_type() {
        return Err(violation(
            "runtime-filter row input type differs from the immutable artifact type",
        ));
    }

    let evaluation = match input.data_type() {
        DataType::Boolean => {
            evaluate_primitive::<BooleanArray>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Boolean(array.value(index)))
            })
        }
        DataType::Int8 => {
            evaluate_primitive::<Int8Array>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Int8(array.value(index)))
            })
        }
        DataType::Int16 => {
            evaluate_primitive::<Int16Array>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Int16(array.value(index)))
            })
        }
        DataType::Int32 => {
            evaluate_primitive::<Int32Array>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Int32(array.value(index)))
            })
        }
        DataType::Int64 => {
            evaluate_primitive::<Int64Array>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Int64(array.value(index)))
            })
        }
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            evaluate_primitive::<FixedSizeBinaryArray>(
                artifact,
                input,
                logical_version,
                |array, index| {
                    // FixedSizeBinary(16) guarantees a complete i128 payload.  The
                    // only remaining failure would mean Arrow violated its own
                    // declared width and is therefore a contract violation.
                    let value =
                        novarocks_types::largeint::i128_from_be_bytes(array.value(index))
                            .map_err(|_| violation("invalid FixedSizeBinary(16) LargeInt value"))?;
                    Ok(RuntimeFilterScalarRef::LargeInt(value))
                },
            )
        }
        DataType::Float32 => {
            evaluate_primitive::<Float32Array>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Float32(array.value(index)))
            })
        }
        DataType::Float64 => {
            evaluate_primitive::<Float64Array>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Float64(array.value(index)))
            })
        }
        DataType::Utf8 => {
            evaluate_primitive::<StringArray>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Utf8(array.value(index)))
            })
        }
        DataType::Date32 => {
            evaluate_primitive::<Date32Array>(artifact, input, logical_version, |array, index| {
                Ok(RuntimeFilterScalarRef::Date32(array.value(index)))
            })
        }
        DataType::Timestamp(TimeUnit::Second, _) => evaluate_primitive::<TimestampSecondArray>(
            artifact,
            input,
            logical_version,
            |array, index| Ok(RuntimeFilterScalarRef::TimestampSecond(array.value(index))),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            evaluate_primitive::<TimestampMillisecondArray>(
                artifact,
                input,
                logical_version,
                |array, index| {
                    Ok(RuntimeFilterScalarRef::TimestampMillisecond(
                        array.value(index),
                    ))
                },
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            evaluate_primitive::<TimestampMicrosecondArray>(
                artifact,
                input,
                logical_version,
                |array, index| {
                    Ok(RuntimeFilterScalarRef::TimestampMicrosecond(
                        array.value(index),
                    ))
                },
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            evaluate_primitive::<TimestampNanosecondArray>(
                artifact,
                input,
                logical_version,
                |array, index| {
                    Ok(RuntimeFilterScalarRef::TimestampNanosecond(
                        array.value(index),
                    ))
                },
            )
        }
        DataType::Decimal128(_, _) => evaluate_primitive::<Decimal128Array>(
            artifact,
            input,
            logical_version,
            |array, index| Ok(RuntimeFilterScalarRef::Decimal128(array.value(index))),
        ),
        _ => Ok(RuntimeFilterRowEvaluation::NotEvaluated {
            reason: RuntimeFilterRowNotEvaluatedReason::DataTypeUnsupported,
            observed_version: logical_version,
        }),
    }?;

    Ok(RuntimeFilterRowOutcome {
        binding_id,
        evaluation,
    })
}

fn evaluate_primitive<A>(
    artifact: &dyn RuntimeFilterArtifactQuery,
    input: &ArrayRef,
    logical_version: LogicalVersion,
    scalar: impl Fn(&A, usize) -> Result<RuntimeFilterScalarRef<'_>, RuntimeFilterContractViolation>,
) -> Result<RuntimeFilterRowEvaluation, RuntimeFilterContractViolation>
where
    A: Array + 'static,
{
    let typed = input.as_any().downcast_ref::<A>().ok_or_else(|| {
        violation("runtime-filter Arrow array implementation differs from its declared type")
    })?;
    let mut mask = Vec::new();
    if mask.try_reserve_exact(typed.len()).is_err() {
        return Ok(RuntimeFilterRowEvaluation::NotEvaluated {
            reason: RuntimeFilterRowNotEvaluatedReason::ResourceUnavailable,
            observed_version: logical_version,
        });
    }
    for index in 0..typed.len() {
        let result = if typed.is_null(index) {
            artifact.matches_null()
        } else {
            artifact.non_null_value_may_match(scalar(typed, index)?)
        };
        match result {
            Ok(matched) => mask.push(matched),
            Err(RuntimeFilterArtifactQueryError::Unsupported) => {
                return Ok(RuntimeFilterRowEvaluation::NotEvaluated {
                    reason: RuntimeFilterRowNotEvaluatedReason::ArtifactQueryUnsupported,
                    observed_version: logical_version,
                });
            }
            Err(RuntimeFilterArtifactQueryError::ResourceUnavailable) => {
                return Ok(RuntimeFilterRowEvaluation::NotEvaluated {
                    reason: RuntimeFilterRowNotEvaluatedReason::ResourceUnavailable,
                    observed_version: logical_version,
                });
            }
            Err(RuntimeFilterArtifactQueryError::ContractViolation) => {
                return Err(violation(
                    "runtime-filter artifact query rejected its immutable artifact",
                ));
            }
        }
    }
    Ok(RuntimeFilterRowEvaluation::Evaluated {
        mask: BooleanArray::from(mask),
        logical_version,
    })
}

fn violation(detail: &'static str) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(
        RuntimeFilterContractViolationKind::ContractMismatch,
        detail,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array};

    use super::*;

    struct Query {
        data_type: DataType,
        null_matches: Result<bool, RuntimeFilterArtifactQueryError>,
        value_matches: Result<bool, RuntimeFilterArtifactQueryError>,
    }

    impl RuntimeFilterArtifactQuery for Query {
        fn data_type(&self) -> &DataType {
            &self.data_type
        }

        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            self.null_matches
        }

        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }

        fn non_null_value_may_match(
            &self,
            _: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            self.value_matches
        }

        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }
    }

    fn query(data_type: DataType) -> Query {
        Query {
            data_type,
            null_matches: Ok(true),
            value_matches: Ok(false),
        }
    }

    #[test]
    fn evaluates_nulls_and_derives_effect_only_from_evaluated_mask() {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(2)]));
        let outcome = evaluate_rows(
            RuntimeFilterBindingId::new(7),
            LogicalVersion::new(3),
            &query(DataType::Int32),
            &input,
        )
        .unwrap();

        let RuntimeFilterRowEvaluation::Evaluated {
            mask,
            logical_version,
        } = outcome.evaluation()
        else {
            panic!("expected evaluated outcome");
        };
        assert_eq!(
            mask.values().iter().collect::<Vec<_>>(),
            vec![false, true, false]
        );
        assert_eq!(*logical_version, LogicalVersion::new(3));
        assert_eq!(
            outcome.effect(),
            Some(RuntimeFilterRowEffect {
                binding_id: RuntimeFilterBindingId::new(7),
                logical_version: LogicalVersion::new(3),
                input_rows: 3,
                output_rows: 1,
            })
        );
    }

    #[test]
    fn unsupported_or_resource_query_is_fail_open_without_an_effect() {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        for (error, reason) in [
            (
                RuntimeFilterArtifactQueryError::Unsupported,
                RuntimeFilterRowNotEvaluatedReason::ArtifactQueryUnsupported,
            ),
            (
                RuntimeFilterArtifactQueryError::ResourceUnavailable,
                RuntimeFilterRowNotEvaluatedReason::ResourceUnavailable,
            ),
        ] {
            let mut artifact = query(DataType::Int32);
            artifact.value_matches = Err(error);
            let outcome = evaluate_rows(
                RuntimeFilterBindingId::new(1),
                LogicalVersion::new(9),
                &artifact,
                &input,
            )
            .unwrap();
            assert_eq!(
                outcome.evaluation(),
                &RuntimeFilterRowEvaluation::NotEvaluated {
                    reason,
                    observed_version: LogicalVersion::new(9),
                }
            );
            assert_eq!(outcome.effect(), None);
        }
    }

    #[test]
    fn frozen_type_mismatch_and_contract_query_failure_are_fail_fast() {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let mismatch = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::FIRST,
            &query(DataType::Int64),
            &input,
        )
        .unwrap_err();
        assert_eq!(
            mismatch.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );

        let mut artifact = query(DataType::Int32);
        artifact.value_matches = Err(RuntimeFilterArtifactQueryError::ContractViolation);
        let failure = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::FIRST,
            &artifact,
            &input,
        )
        .unwrap_err();
        assert_eq!(
            failure.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
    }

    #[test]
    fn unsupported_arrow_type_is_typed_not_evaluated() {
        let input: ArrayRef = Arc::new(arrow::array::BinaryArray::from(vec![b"one".as_slice()]));
        let outcome = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::new(4),
            &query(DataType::Binary),
            &input,
        )
        .unwrap();
        assert_eq!(
            outcome.evaluation(),
            &RuntimeFilterRowEvaluation::NotEvaluated {
                reason: RuntimeFilterRowNotEvaluatedReason::DataTypeUnsupported,
                observed_version: LogicalVersion::new(4),
            }
        );
    }

    #[test]
    fn floating_values_are_forwarded_without_lossy_conversion() {
        struct FloatQuery(Query);
        impl RuntimeFilterArtifactQuery for FloatQuery {
            fn data_type(&self) -> &DataType {
                self.0.data_type()
            }
            fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.matches_null()
            }
            fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.has_non_null_matches()
            }
            fn non_null_value_may_match(
                &self,
                value: RuntimeFilterScalarRef<'_>,
            ) -> Result<bool, RuntimeFilterArtifactQueryError> {
                assert!(matches!(value, RuntimeFilterScalarRef::Float64(value) if value.is_nan()));
                Ok(true)
            }
            fn non_null_range_may_match(
                &self,
                min: &ConnectorScalarValue,
                max: &ConnectorScalarValue,
            ) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.non_null_range_may_match(min, max)
            }
        }
        let input: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
        let outcome = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::FIRST,
            &FloatQuery(query(DataType::Float64)),
            &input,
        )
        .unwrap();
        assert_eq!(outcome.effect().unwrap().output_rows(), 1);
    }
}
