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

//! Mandatory structural declarations for derived provider-domain conditions.
//! An empty table grants no pruning authority. Exact scan-owned predicates keep
//! their independent contract; FE semantic validation must classify every
//! derived domain and prove implication before runtime can consume it.

use crate::pruning_structure::{PruningConsumerIndex, PruningWork};
use crate::{
    FragmentId, FragmentPackage, PruningDomainField, PruningDomainStructure, PruningDomainWitness,
    PruningStructureError,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, MAX_CONTROL_DEPTH,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl,
};
use std::{collections::BTreeSet, fmt, sync::Arc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenFragmentPruning {
    fragment: FragmentId,
    witnesses: Arc<[PruningDomainWitness]>,
}

impl FrozenFragmentPruning {
    pub fn try_new(
        fragment: FragmentId,
        witnesses: Vec<PruningDomainWitness>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FrozenPruningError> {
        count_items(fragment, &witnesses, control)?;
        Ok(Self {
            fragment,
            witnesses: witnesses.into(),
        })
    }

    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }
    pub fn witnesses(&self) -> &[PruningDomainWitness] {
        &self.witnesses
    }

    /// Count every actual source path/reference under one combined bound, not
    /// one independently renewed allowance per witness. No semantic permission
    /// is inferred from the existence or absence of these declarations.
    pub fn dynamic_items_observed(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<usize, FrozenPruningError> {
        count_items(self.fragment, &self.witnesses, control)
    }

    pub(crate) fn validate_package(
        &self,
        package: &FragmentPackage,
        control: &dyn PureCompileControl,
    ) -> Result<(), FrozenPruningError> {
        if self.fragment != package.fragment().id() {
            return Err(FrozenPruningError::WrongFragment);
        }
        if self.witnesses.is_empty() {
            return Ok(());
        }
        // One actual consumer index and one combined proof-work allowance for
        // all declarations. Later FE semantic validation still checks effects
        // and complete global consumers before granting pruning authority.
        let mut work = PruningWork::try_new(control)?;
        let index = PruningConsumerIndex::try_new(package, &mut work)?;
        for witness in self.witnesses.iter() {
            PruningDomainStructure::try_new_indexed(package, witness, &index, control, &mut work)?;
        }
        work.finish()?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenPruningError {
    Control(CompileControlError),
    Structure(PruningStructureError),
    WrongFragment,
    DuplicateTarget,
    TooLarge,
    EmptySources,
}
impl fmt::Display for FrozenPruningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid frozen pruning declarations: {self:?}")
    }
}
impl std::error::Error for FrozenPruningError {}
impl From<CompileControlError> for FrozenPruningError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<PruningStructureError> for FrozenPruningError {
    fn from(error: PruningStructureError) -> Self {
        match error {
            PruningStructureError::Control(error) => Self::Control(error),
            error => Self::Structure(error),
        }
    }
}

fn count_items(
    fragment: FragmentId,
    witnesses: &[PruningDomainWitness],
    control: &dyn PureCompileControl,
) -> Result<usize, FrozenPruningError> {
    let mut observed = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let mut items = 0usize;
    let mut targets = BTreeSet::new();
    add_items(&mut items, witnesses.len())?;
    for witness in witnesses {
        if witness.target.fragment != fragment {
            return Err(FrozenPruningError::WrongFragment);
        }
        let field = match witness.target.field {
            PruningDomainField::Enforced => 0u8,
            PruningDomainField::Unenforced => 1u8,
        };
        if !targets.insert((witness.target.scan, witness.target.occurrence, field)) {
            return Err(FrozenPruningError::DuplicateTarget);
        }
        if witness.sources.is_empty() {
            return Err(FrozenPruningError::EmptySources);
        }
        add_items(&mut items, witness.sources.len())?;
        for source in witness.sources.iter() {
            if source.conjunct_path.len() >= MAX_CONTROL_DEPTH {
                return Err(FrozenPruningError::TooLarge);
            }
            for size in [
                source.conjunct_path.len(),
                source.input_path.len(),
                source.columns.len(),
            ] {
                add_items(&mut items, size)?;
            }
            // Length gates precede traversal; every source and column has an
            // observed bounded step, including empty path/column collections.
            for column in source.columns.iter() {
                add_items(&mut items, column.values.len())?;
                observed.step()?;
            }
            observed.step()?;
        }
        observed.step()?;
    }
    observed.finish()?;
    Ok(items)
}
fn add_items(items: &mut usize, add: usize) -> Result<(), FrozenPruningError> {
    *items = items.checked_add(add).ok_or(FrozenPruningError::TooLarge)?;
    if *items > MAX_CONTROL_USE_REFERENCES {
        return Err(FrozenPruningError::TooLarge);
    }
    Ok(())
}
