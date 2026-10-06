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

    /// Original target-set and final Arc geometry, usable before projection.
    pub fn construction_resources(
        count: usize,
    ) -> Result<novarocks_type_contract::ControlOwnedResourceFacts, FrozenPruningError> {
        let mut resources = novarocks_type_contract::ControlResourceCounter::default();
        resources
            .arc::<PruningDomainWitness>(count)
            .map_err(resource_error)?;
        resources
            .tree::<(crate::NodeId, crate::ProviderReadOccurrenceId, u8), ()>(count)
            .map_err(resource_error)?;
        Ok(resources.facts())
    }

    /// Construct the same declarations in the caller's original scope. The
    /// cumulative facts bound actual target-set and final Arc backing requests;
    /// they do not establish implication or complete Package consumer closure.
    pub fn try_new_in(
        fragment: FragmentId,
        witnesses: Vec<PruningDomainWitness>,
        admit: &mut impl FnMut(
            &novarocks_type_contract::ControlOwnedResourceFacts,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FrozenPruningError> {
        let mut resources = novarocks_type_contract::ControlResourceCounter::default();
        resources
            .merge(Self::construction_resources(witnesses.len())?)
            .map_err(resource_error)?;
        admit(&resources.facts())?;
        count_items_core(fragment, &witnesses, Some((&mut resources, admit)), work)?;
        work.flush()?;
        let value = Self {
            fragment,
            witnesses: witnesses.into(),
        };
        work.step()?;
        work.flush()?;
        Ok(value)
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

    /// Count the same original declaration grammar without renewing a scope.
    /// Ordinary and successful tails are the caller's responsibility.
    pub fn dynamic_items_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, FrozenPruningError> {
        count_items_core(self.fragment, &self.witnesses, None, work)
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
        // One actual consumer index and one combined proof-work allowance.
        let mut work = PruningWork::try_new(control)?;
        self.validate_package_core(package, control, &mut work)?;
        work.finish()?;
        Ok(())
    }

    /// Retain the original consumer/index and witness laws on the caller's
    /// scope. No empty table grants pruning or complete Package authority.
    pub(crate) fn validate_package_in(
        &self,
        package: &FragmentPackage,
        observed: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FrozenPruningError> {
        if self.fragment != package.fragment().id() {
            return Err(FrozenPruningError::WrongFragment);
        }
        if self.witnesses.is_empty() {
            return Ok(());
        }
        let control = observed.control();
        let mut work = PruningWork::borrowed(observed);
        self.validate_package_core(package, control, &mut work)
    }
    fn validate_package_core(
        &self,
        package: &FragmentPackage,
        control: &dyn PureCompileControl,
        work: &mut PruningWork<'_, '_>,
    ) -> Result<(), FrozenPruningError> {
        let index = PruningConsumerIndex::try_new(package, work)?;
        for witness in self.witnesses.iter() {
            PruningDomainStructure::try_new_indexed(package, witness, &index, control, work)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenPruningError {
    Control(CompileControlError),
    ResourceSource(&'static str),
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
    let items = count_items_core(fragment, witnesses, None, &mut observed)?;
    observed.finish()?;
    Ok(items)
}

fn resource_error(error: novarocks_type_contract::ControlResourceError) -> FrozenPruningError {
    match error {
        novarocks_type_contract::ControlResourceError::Control(cause) => {
            FrozenPruningError::Control(cause)
        }
        novarocks_type_contract::ControlResourceError::SourceModel(message) => {
            FrozenPruningError::ResourceSource(message)
        }
    }
}

type PruningAdmission<'a> = dyn FnMut(&novarocks_type_contract::ControlOwnedResourceFacts) -> Result<(), CompileControlError>
    + 'a;
type PruningResources<'a> = Option<(
    &'a mut novarocks_type_contract::ControlResourceCounter,
    &'a mut PruningAdmission<'a>,
)>;
fn charge_walk(
    resources: &mut PruningResources<'_>,
    count: usize,
) -> Result<(), FrozenPruningError> {
    if let Some((counter, admit)) = resources.as_mut() {
        // Each captured header/path/value contributes a bounded allowance for
        // this original constructor's arithmetic, local laws and completed
        // steps. Tree movement and Arc copying are separate fixed requests.
        counter
            .work(novarocks_type_contract::control_resource_mul(count, 32).map_err(resource_error)?)
            .map_err(resource_error)?;
        admit(&counter.facts())?;
    }
    Ok(())
}

fn count_items_core(
    fragment: FragmentId,
    witnesses: &[PruningDomainWitness],
    mut resources: PruningResources<'_>,
    observed: &mut CompileCheckpoints<'_>,
) -> Result<usize, FrozenPruningError> {
    let observed_resources = resources.is_some();
    let mut items = 0usize;
    let mut targets = BTreeSet::new();
    add_items(&mut items, witnesses.len())?;
    charge_walk(&mut resources, witnesses.len())?;
    for witness in witnesses {
        if witness.target.fragment != fragment {
            return Err(FrozenPruningError::WrongFragment);
        }
        let field = match witness.target.field {
            PruningDomainField::Enforced => 0u8,
            PruningDomainField::Unenforced => 1u8,
        };
        charge_walk(&mut resources, witness.sources.len())?;
        if observed_resources {
            observed.flush()?;
        }
        if !targets.insert((witness.target.scan, witness.target.occurrence, field)) {
            return Err(FrozenPruningError::DuplicateTarget);
        }
        if observed_resources {
            observed.step()?;
            observed.flush()?;
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
            let captured = novarocks_type_contract::control_resource_add(
                source.conjunct_path.len(),
                source.input_path.len(),
            )
            .and_then(|n| novarocks_type_contract::control_resource_add(n, source.columns.len()))
            .map_err(resource_error)?;
            charge_walk(&mut resources, captured)?;
            // Length gates precede traversal; every source and column has an
            // observed bounded step, including empty path/column collections.
            for column in source.columns.iter() {
                add_items(&mut items, column.values.len())?;
                charge_walk(&mut resources, column.values.len())?;
                observed.step()?;
            }
            observed.step()?;
        }
        observed.step()?;
    }
    Ok(items)
}
fn add_items(items: &mut usize, add: usize) -> Result<(), FrozenPruningError> {
    *items = items.checked_add(add).ok_or(FrozenPruningError::TooLarge)?;
    if *items > MAX_CONTROL_USE_REFERENCES {
        return Err(FrozenPruningError::TooLarge);
    }
    Ok(())
}
