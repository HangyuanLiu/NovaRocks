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

//! Immutable result-affecting parameters. Runtime deadlines, topology, batch
//! sizing and resource policies are deliberately outside this vocabulary.

use crate::{CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Stable semantic keys, independent of a parameter's fragment-local ID.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SemanticParameterKey {
    StatementStartUtc,
    TimeZone,
    AllowThrowException,
    DecimalOverflowToDouble,
    GroupConcatLegacy,
    GroupConcatMaxLen,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SemanticParameterId(u32);

impl SemanticParameterId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// An exact dependency on one frozen value in its lexical setting scope.
/// Different scopes may bind the same stable key to different sparse IDs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SemanticParameterRef {
    pub id: SemanticParameterId,
    pub expected_key: SemanticParameterKey,
}

pub const MAX_SEMANTIC_PARAMETERS: usize = 4096;

/// The value variant fixes its semantic key and logical type. A codec must
/// reject a different key/type instead of coercing or supplying a default.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SemanticParameterValue {
    /// Microseconds since the UTC Unix epoch, captured at statement entry.
    StatementStartUtc(i64),
    /// An exact rule name or fixed-offset spelling from the admitted setting.
    TimeZone(Box<str>),
    AllowThrowException(bool),
    DecimalOverflowToDouble(bool),
    GroupConcatLegacy(bool),
    /// Raw admitted setting; specialization applies the existing minimum clamp.
    GroupConcatMaxLen(i64),
}

impl SemanticParameterValue {
    pub const fn key(&self) -> SemanticParameterKey {
        match self {
            Self::StatementStartUtc(_) => SemanticParameterKey::StatementStartUtc,
            Self::TimeZone(_) => SemanticParameterKey::TimeZone,
            Self::AllowThrowException(_) => SemanticParameterKey::AllowThrowException,
            Self::DecimalOverflowToDouble(_) => SemanticParameterKey::DecimalOverflowToDouble,
            Self::GroupConcatLegacy(_) => SemanticParameterKey::GroupConcatLegacy,
            Self::GroupConcatMaxLen(_) => SemanticParameterKey::GroupConcatMaxLen,
        }
    }
}

/// One authoritative immutable table. Sparse IDs never determine allocation
/// size. Projection preserves both identities and exact values across retries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SemanticParameters {
    entries: BTreeMap<SemanticParameterId, SemanticParameterValue>,
}

impl SemanticParameters {
    pub fn try_new(
        entries: impl IntoIterator<Item = (SemanticParameterId, SemanticParameterValue)>,
    ) -> Result<Self, SemanticParameterError> {
        match Self::construct(entries, None) {
            Ok(table) => Ok(table),
            Err(SemanticParameterProjectionError::Parameter(error)) => Err(error),
            // Observation is absent on this path, so it cannot refuse control.
            Err(SemanticParameterProjectionError::Control(_)) => unreachable!(),
        }
    }

    /// Construct using the caller's original phase and checkpoints. The caller
    /// admits iterator copies and tree allocation requests before entering, and
    /// owns the ordinary/success tail. This port neither admits resources nor
    /// creates or finishes another control scope.
    pub fn try_new_observed(
        entries: impl IntoIterator<Item = (SemanticParameterId, SemanticParameterValue)>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, SemanticParameterProjectionError> {
        Self::construct(entries, Some(work))
    }

    fn construct(
        entries: impl IntoIterator<Item = (SemanticParameterId, SemanticParameterValue)>,
        mut work: Option<&mut CompileCheckpoints<'_>>,
    ) -> Result<Self, SemanticParameterProjectionError> {
        let mut table = Self::default();
        let mut has_statement_start = false;
        let mut entries = entries.into_iter();
        loop {
            // A lazy iterator may allocate one bounded value or latch a codec
            // error. Observe its actual pull, including termination, without
            // requiring the iterator to borrow this same mutable checkpoint.
            construction_flush(&mut work)?;
            let next = entries.next();
            construction_step(&mut work)?;
            construction_flush(&mut work)?;
            let Some((id, value)) = next else {
                return Ok(table);
            };
            let key = value.key();
            construction_step(&mut work)?;
            construction_flush(&mut work)?;
            let duplicate = table.entries.contains_key(&id);
            construction_step(&mut work)?;
            construction_flush(&mut work)?;
            if duplicate {
                return Err(SemanticParameterProjectionError::Parameter(
                    SemanticParameterError::DuplicateId(id),
                ));
            }
            let too_many = table.entries.len() >= MAX_SEMANTIC_PARAMETERS;
            construction_step(&mut work)?;
            if too_many {
                return Err(SemanticParameterProjectionError::Parameter(
                    SemanticParameterError::TooManyParameters,
                ));
            }
            let statement_start = key == SemanticParameterKey::StatementStartUtc;
            construction_step(&mut work)?;
            if statement_start {
                let duplicate = has_statement_start;
                construction_step(&mut work)?;
                if duplicate {
                    return Err(SemanticParameterProjectionError::Parameter(
                        SemanticParameterError::DuplicateStatementStart,
                    ));
                }
                has_statement_start = true;
            }
            if let SemanticParameterValue::TimeZone(zone) = &value {
                let invalid_length = zone.is_empty() || zone.len() > 255;
                construction_step(&mut work)?;
                if invalid_length {
                    return Err(SemanticParameterProjectionError::Parameter(
                        SemanticParameterError::InvalidTimeZone,
                    ));
                }
                for character in zone.chars() {
                    let invalid = character.is_control();
                    construction_step(&mut work)?;
                    if invalid {
                        return Err(SemanticParameterProjectionError::Parameter(
                            SemanticParameterError::InvalidTimeZone,
                        ));
                    }
                }
            }
            construction_flush(&mut work)?;
            table.entries.insert(id, value);
            construction_step(&mut work)?;
            construction_flush(&mut work)?;
        }
    }

    pub fn entries(&self) -> &BTreeMap<SemanticParameterId, SemanticParameterValue> {
        &self.entries
    }

    pub fn get(&self, id: SemanticParameterId) -> Option<&SemanticParameterValue> {
        self.entries.get(&id)
    }

    pub fn require(
        &self,
        reference: SemanticParameterRef,
    ) -> Result<&SemanticParameterValue, SemanticParameterError> {
        let value = self
            .entries
            .get(&reference.id)
            .ok_or(SemanticParameterError::MissingId(reference.id))?;
        if value.key() != reference.expected_key {
            return Err(SemanticParameterError::KeyMismatch(reference));
        }
        Ok(value)
    }

    /// Extract exact dependencies, preserving scoped values and sparse IDs.
    /// A shared definition is cloned once; every reference's key is checked.
    pub fn project(
        &self,
        required: impl IntoIterator<Item = SemanticParameterRef>,
    ) -> Result<Self, SemanticParameterError> {
        let mut selected = BTreeSet::new();
        let mut entries = Vec::new();
        for reference in required {
            let value = self.require(reference)?;
            if selected.insert(reference.id) {
                entries.push((reference.id, value.clone()));
            }
        }
        Self::try_new(entries)
    }

    /// Observe every reference lookup and bounded value clone. Definitions
    /// come exclusively from this already validated immutable table; a subset
    /// needs no unobserved second validation or temporary entries vector.
    pub fn project_observed(
        &self,
        required: impl IntoIterator<Item = SemanticParameterRef>,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, SemanticParameterProjectionError> {
        let mut work = CompileCheckpoints::try_new(control, phase)
            .map_err(SemanticParameterProjectionError::Control)?;
        let mut entries = BTreeMap::new();
        for reference in required {
            let value = self
                .require(reference)
                .map_err(SemanticParameterProjectionError::Parameter)?;
            entries.entry(reference.id).or_insert_with(|| value.clone());
            work.step()
                .map_err(SemanticParameterProjectionError::Control)?;
        }
        work.finish()
            .map_err(SemanticParameterProjectionError::Control)?;
        Ok(Self { entries })
    }
}

fn construction_flush(
    work: &mut Option<&mut CompileCheckpoints<'_>>,
) -> Result<(), SemanticParameterProjectionError> {
    if let Some(work) = work.as_deref_mut() {
        work.flush()
            .map_err(SemanticParameterProjectionError::Control)?;
    }
    Ok(())
}

fn construction_step(
    work: &mut Option<&mut CompileCheckpoints<'_>>,
) -> Result<(), SemanticParameterProjectionError> {
    if let Some(work) = work.as_deref_mut() {
        work.step()
            .map_err(SemanticParameterProjectionError::Control)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SemanticParameterProjectionError {
    Parameter(SemanticParameterError),
    Control(CompileControlError),
}
impl fmt::Display for SemanticParameterProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parameter(error) => error.fmt(f),
            Self::Control(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SemanticParameterProjectionError {}

#[cfg(test)]
#[path = "semantics/projection_tests.rs"]
mod projection_tests;

#[cfg(test)]
#[path = "semantics/construction_tests.rs"]
mod construction_tests;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SemanticParameterError {
    DuplicateId(SemanticParameterId),
    DuplicateStatementStart,
    MissingId(SemanticParameterId),
    KeyMismatch(SemanticParameterRef),
    TooManyParameters,
    InvalidTimeZone,
}

impl fmt::Display for SemanticParameterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateId(id) => write!(f, "duplicate semantic parameter ID {}", id.get()),
            Self::DuplicateStatementStart => {
                f.write_str("statement start has multiple authorities")
            }
            Self::MissingId(id) => write!(f, "missing semantic parameter ID {}", id.get()),
            Self::KeyMismatch(reference) => write!(
                f,
                "semantic parameter ID {} does not have expected key {:?}",
                reference.id.get(),
                reference.expected_key
            ),
            Self::TooManyParameters => {
                f.write_str("semantic parameter table exceeds its definition bound")
            }
            Self::InvalidTimeZone => f.write_str(
                "semantic time zone must contain 1..255 bytes and no control characters",
            ),
        }
    }
}

impl std::error::Error for SemanticParameterError {}

/// Observability belongs to a use edge, not to a shared expression definition.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EvaluationDemand {
    Value,
    TruthOnly,
}

/// A successful Boolean value; an unevaluated row or a row error is separate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BooleanValue {
    True,
    False,
    Null,
}

impl EvaluationDemand {
    pub const fn and_decided_by(self, value: BooleanValue) -> bool {
        matches!(value, BooleanValue::False)
            || matches!((self, value), (Self::TruthOnly, BooleanValue::Null))
    }

    pub const fn or_decided_by(self, value: BooleanValue) -> bool {
        matches!(value, BooleanValue::True)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_keeps_sparse_identity_and_frozen_values() {
        let clock = SemanticParameterId::new(u32::MAX);
        let table = SemanticParameters::try_new([
            (clock, SemanticParameterValue::StatementStartUtc(-17)),
            (
                SemanticParameterId::new(2),
                SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
            ),
            (
                SemanticParameterId::new(9),
                SemanticParameterValue::AllowThrowException(true),
            ),
        ])
        .unwrap();
        let projected = table
            .project([
                SemanticParameterRef {
                    id: clock,
                    expected_key: SemanticParameterKey::StatementStartUtc,
                },
                SemanticParameterRef {
                    id: clock,
                    expected_key: SemanticParameterKey::StatementStartUtc,
                },
            ])
            .unwrap();
        assert_eq!(projected.entries().len(), 1);
        assert_eq!(
            projected.get(clock),
            Some(&SemanticParameterValue::StatementStartUtc(-17))
        );
        assert_eq!(projected.clone(), projected);
        assert!(matches!(
            projected.require(SemanticParameterRef {
                id: SemanticParameterId::new(2),
                expected_key: SemanticParameterKey::TimeZone
            }),
            Err(SemanticParameterError::MissingId(_))
        ));
    }

    #[test]
    fn duplicate_identity_or_authority_is_rejected() {
        for entries in [
            [
                (
                    SemanticParameterId::new(1),
                    SemanticParameterValue::AllowThrowException(true),
                ),
                (
                    SemanticParameterId::new(1),
                    SemanticParameterValue::GroupConcatLegacy(false),
                ),
            ],
            [
                (
                    SemanticParameterId::new(1),
                    SemanticParameterValue::StatementStartUtc(1),
                ),
                (
                    SemanticParameterId::new(2),
                    SemanticParameterValue::StatementStartUtc(2),
                ),
            ],
        ] {
            assert!(SemanticParameters::try_new(entries).is_err());
        }
    }

    #[test]
    fn lexical_scopes_keep_distinct_values_for_the_same_key() {
        let first = SemanticParameterRef {
            id: SemanticParameterId::new(7),
            expected_key: SemanticParameterKey::AllowThrowException,
        };
        let second = SemanticParameterRef {
            id: SemanticParameterId::new(u32::MAX),
            expected_key: SemanticParameterKey::AllowThrowException,
        };
        let table = SemanticParameters::try_new([
            (first.id, SemanticParameterValue::AllowThrowException(true)),
            (
                second.id,
                SemanticParameterValue::AllowThrowException(false),
            ),
            (
                SemanticParameterId::new(2),
                SemanticParameterValue::GroupConcatMaxLen(-121),
            ),
        ])
        .unwrap();
        let projected = table.project([first, second, first]).unwrap();
        assert_eq!(projected.entries().len(), 2);
        assert_eq!(
            projected.require(first).unwrap(),
            &SemanticParameterValue::AllowThrowException(true)
        );
        assert_eq!(
            projected.require(second).unwrap(),
            &SemanticParameterValue::AllowThrowException(false)
        );
        assert_eq!(
            table.get(SemanticParameterId::new(2)),
            Some(&SemanticParameterValue::GroupConcatMaxLen(-121))
        );
        assert!(matches!(
            table.project([
                first,
                SemanticParameterRef {
                    id: first.id,
                    expected_key: SemanticParameterKey::TimeZone
                }
            ]),
            Err(SemanticParameterError::KeyMismatch(_))
        ));
    }

    #[test]
    fn time_zone_spelling_is_bounded_without_environment_lookup() {
        for zone in [String::new(), "x".repeat(256), "UTC\n".into()] {
            assert_eq!(
                SemanticParameters::try_new([(
                    SemanticParameterId::new(0),
                    SemanticParameterValue::TimeZone(zone.into())
                )]),
                Err(SemanticParameterError::InvalidTimeZone)
            );
        }
        assert!(
            SemanticParameters::try_new([(
                SemanticParameterId::new(0),
                SemanticParameterValue::TimeZone("x".repeat(255).into())
            )])
            .is_ok()
        );
    }

    #[test]
    fn definition_bound_is_independent_of_sparse_ids() {
        let entries = (0..MAX_SEMANTIC_PARAMETERS)
            .map(|index| {
                (
                    SemanticParameterId::new(u32::MAX - index as u32),
                    SemanticParameterValue::AllowThrowException(index % 2 == 0),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            SemanticParameters::try_new(entries.clone())
                .unwrap()
                .entries()
                .len(),
            MAX_SEMANTIC_PARAMETERS
        );
        let mut over = entries;
        over.push((
            SemanticParameterId::new(0),
            SemanticParameterValue::GroupConcatMaxLen(-1),
        ));
        assert_eq!(
            SemanticParameters::try_new(over),
            Err(SemanticParameterError::TooManyParameters)
        );
    }

    #[test]
    fn boolean_decision_depends_on_the_use_edge() {
        use BooleanValue::*;
        for (value, value_and, truth_and, or) in [
            (True, false, false, true),
            (False, true, true, false),
            (Null, false, true, false),
        ] {
            assert_eq!(EvaluationDemand::Value.and_decided_by(value), value_and);
            assert_eq!(EvaluationDemand::TruthOnly.and_decided_by(value), truth_and);
            for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
                assert_eq!(demand.or_decided_by(value), or);
            }
        }
    }
}
