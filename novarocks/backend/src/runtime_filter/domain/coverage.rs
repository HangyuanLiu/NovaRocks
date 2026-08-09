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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

macro_rules! id {
    ($name:ident, $raw:ty) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub(crate) struct $name($raw);

        impl $name {
            pub(crate) const fn new(raw: $raw) -> Self {
                Self(raw)
            }

            pub(crate) const fn get(self) -> $raw {
                self.0
            }
        }
    };
}

id!(BackendCoverageWitnessId, u32);

/// A finite, validated coverage expression installed with one Backend
/// participant. It describes readiness only and does not encode a reduction
/// or any fragment value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendCoverage {
    Witness(BackendCoverageWitnessId),
    AllOf(Vec<Self>),
    AnyOf(Vec<Self>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendCoverageError {
    EmptyComposite,
}

impl fmt::Display for BackendCoverageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter coverage: {self:?}"
        )
    }
}

impl std::error::Error for BackendCoverageError {}

impl BackendCoverage {
    pub(crate) const fn witness(witness: BackendCoverageWitnessId) -> Self {
        Self::Witness(witness)
    }

    pub(crate) fn all_of(
        children: impl IntoIterator<Item = Self>,
    ) -> Result<Self, BackendCoverageError> {
        Self::composite(children, Self::AllOf)
    }

    pub(crate) fn any_of(
        children: impl IntoIterator<Item = Self>,
    ) -> Result<Self, BackendCoverageError> {
        Self::composite(children, Self::AnyOf)
    }

    pub(crate) fn witnesses(&self) -> BTreeSet<BackendCoverageWitnessId> {
        let mut witnesses = BTreeSet::new();
        self.collect_witnesses(&mut witnesses);
        witnesses
    }

    fn composite(
        children: impl IntoIterator<Item = Self>,
        make: impl FnOnce(Vec<Self>) -> Self,
    ) -> Result<Self, BackendCoverageError> {
        let children = children.into_iter().collect::<Vec<_>>();
        if children.is_empty() {
            return Err(BackendCoverageError::EmptyComposite);
        }
        Ok(make(children))
    }

    fn collect_witnesses(&self, output: &mut BTreeSet<BackendCoverageWitnessId>) {
        match self {
            Self::Witness(witness) => {
                output.insert(*witness);
            }
            Self::AllOf(children) | Self::AnyOf(children) => {
                for child in children {
                    child.collect_witnesses(output);
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendCoverageWitnessProgress {
    Pending,
    Satisfied,
    Impossible,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendCoverageProgress {
    Pending,
    Satisfied,
    Impossible,
}

/// Mutable readiness state. A terminal witness may be recorded once only,
/// which makes duplicate delivery idempotent and prevents terminal regression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendCoverageState {
    witnesses: BTreeMap<BackendCoverageWitnessId, BackendCoverageWitnessProgress>,
}

impl BackendCoverageState {
    pub(crate) fn new(coverage: &BackendCoverage) -> Result<Self, BackendCoverageError> {
        let witnesses = coverage.witnesses();
        if witnesses.is_empty() {
            return Err(BackendCoverageError::EmptyComposite);
        }
        Ok(Self {
            witnesses: witnesses
                .into_iter()
                .map(|witness| (witness, BackendCoverageWitnessProgress::Pending))
                .collect(),
        })
    }

    pub(crate) fn mark_satisfied(&mut self, witness: BackendCoverageWitnessId) -> bool {
        self.advance(witness, BackendCoverageWitnessProgress::Satisfied)
    }

    pub(crate) fn mark_impossible(&mut self, witness: BackendCoverageWitnessId) -> bool {
        self.advance(witness, BackendCoverageWitnessProgress::Impossible)
    }

    pub(crate) fn progress(&self, coverage: &BackendCoverage) -> BackendCoverageProgress {
        match coverage {
            BackendCoverage::Witness(witness) => match self
                .witnesses
                .get(witness)
                .expect("coverage state is initialized from its validated expression")
            {
                BackendCoverageWitnessProgress::Pending => BackendCoverageProgress::Pending,
                BackendCoverageWitnessProgress::Satisfied => BackendCoverageProgress::Satisfied,
                BackendCoverageWitnessProgress::Impossible => BackendCoverageProgress::Impossible,
            },
            BackendCoverage::AllOf(children) => {
                let mut all_satisfied = true;
                for child in children {
                    match self.progress(child) {
                        BackendCoverageProgress::Impossible => {
                            return BackendCoverageProgress::Impossible;
                        }
                        BackendCoverageProgress::Pending => all_satisfied = false,
                        BackendCoverageProgress::Satisfied => {}
                    }
                }
                if all_satisfied {
                    BackendCoverageProgress::Satisfied
                } else {
                    BackendCoverageProgress::Pending
                }
            }
            BackendCoverage::AnyOf(children) => {
                let mut all_impossible = true;
                for child in children {
                    match self.progress(child) {
                        BackendCoverageProgress::Satisfied => {
                            return BackendCoverageProgress::Satisfied;
                        }
                        BackendCoverageProgress::Pending => all_impossible = false,
                        BackendCoverageProgress::Impossible => {}
                    }
                }
                if all_impossible {
                    BackendCoverageProgress::Impossible
                } else {
                    BackendCoverageProgress::Pending
                }
            }
        }
    }

    fn advance(
        &mut self,
        witness: BackendCoverageWitnessId,
        next: BackendCoverageWitnessProgress,
    ) -> bool {
        let Some(progress) = self.witnesses.get_mut(&witness) else {
            return false;
        };
        if *progress != BackendCoverageWitnessProgress::Pending {
            return false;
        }
        *progress = next;
        true
    }
}
