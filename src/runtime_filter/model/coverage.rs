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

use std::collections::BTreeSet;

use super::contract::CoverageWitnessId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Coverage {
    Leaf(CoverageWitnessId),
    AllOf(Vec<Coverage>),
    AnyOf(Vec<Coverage>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoverageShapeError {
    EmptyAllOf,
    EmptyAnyOf,
    DuplicateChild,
}

impl Coverage {
    pub(crate) fn validate_shape(&self) -> Result<(), CoverageShapeError> {
        let (children, empty_error) = match self {
            Self::Leaf(_) => return Ok(()),
            Self::AllOf(children) => (children, CoverageShapeError::EmptyAllOf),
            Self::AnyOf(children) => (children, CoverageShapeError::EmptyAnyOf),
        };

        if children.is_empty() {
            return Err(empty_error);
        }

        for child in children {
            child.validate_shape()?;
        }

        let mut canonical_children = BTreeSet::new();
        for child in children {
            if !canonical_children.insert(CanonicalCoverage::from(child)) {
                return Err(CoverageShapeError::DuplicateChild);
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CanonicalCoverage {
    Leaf(CoverageWitnessId),
    AllOf(Vec<CanonicalCoverage>),
    AnyOf(Vec<CanonicalCoverage>),
}

impl From<&Coverage> for CanonicalCoverage {
    fn from(coverage: &Coverage) -> Self {
        match coverage {
            Coverage::Leaf(witness_id) => Self::Leaf(*witness_id),
            Coverage::AllOf(children) => Self::AllOf(canonical_children(children)),
            Coverage::AnyOf(children) => Self::AnyOf(canonical_children(children)),
        }
    }
}

fn canonical_children(children: &[Coverage]) -> Vec<CanonicalCoverage> {
    let mut canonical = children
        .iter()
        .map(CanonicalCoverage::from)
        .collect::<Vec<_>>();
    canonical.sort();
    canonical
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_rejects_empty_composites() {
        assert_eq!(
            Coverage::AllOf(vec![]).validate_shape(),
            Err(CoverageShapeError::EmptyAllOf)
        );
        assert_eq!(
            Coverage::AnyOf(vec![]).validate_shape(),
            Err(CoverageShapeError::EmptyAnyOf)
        );
    }

    #[test]
    fn coverage_rejects_repeated_witness_in_one_normalized_subtree() {
        let coverage = Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(1)),
        ]);

        assert_eq!(
            coverage.validate_shape(),
            Err(CoverageShapeError::DuplicateChild)
        );
    }

    #[test]
    fn coverage_rejects_duplicate_children_after_canonical_comparison() {
        let coverage = Coverage::AllOf(vec![
            Coverage::AnyOf(vec![
                Coverage::Leaf(CoverageWitnessId::new(1)),
                Coverage::Leaf(CoverageWitnessId::new(2)),
            ]),
            Coverage::AnyOf(vec![
                Coverage::Leaf(CoverageWitnessId::new(2)),
                Coverage::Leaf(CoverageWitnessId::new(1)),
            ]),
        ]);

        assert_eq!(
            coverage.validate_shape(),
            Err(CoverageShapeError::DuplicateChild)
        );
    }

    #[test]
    fn coverage_accepts_nested_all_of_any_of() {
        let coverage = Coverage::AllOf(vec![
            Coverage::AnyOf(vec![
                Coverage::Leaf(CoverageWitnessId::new(1)),
                Coverage::Leaf(CoverageWitnessId::new(2)),
            ]),
            Coverage::Leaf(CoverageWitnessId::new(3)),
        ]);
        assert_eq!(coverage.validate_shape(), Ok(()));
    }
}
