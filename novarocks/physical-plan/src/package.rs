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

//! One carrier-neutral fragment input. Its constructor proves local structure;
//! exact installed function and provider capabilities are checked separately.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_connector_contract::{
    ConnectorWriteRecipeDraft, FrozenConnectorRead, WriteTargetOrdinal,
};
use novarocks_type_contract::{SemanticParameterError, SemanticParameterRef, SemanticParameters};

use crate::{
    AnnotationSubject, Fragment, FragmentCuts, FragmentId, NodeId, PhysicalPlan, PlanAnnotation,
    PlanVersionId, ProviderReadOccurrenceId, RequiredContracts, ResultPort, ValidationErrors,
    derive_fragment_cuts,
};

/// An owned input for the same checked constructor on FE and BE. It contains
/// neither generated DTOs nor runtime handles. Sparse IDs remain map keys.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentPackageInput {
    pub version: PlanVersionId,
    pub required: RequiredContracts,
    pub fragment: Fragment,
    pub cuts: FragmentCuts,
    pub result: Option<ResultPort>,
    pub parameters: SemanticParameters,
    /// Additional provider public facts paired with exact physical scan nodes.
    /// The recipe is a structurally checked draft, not a capability proof.
    pub scans: BTreeMap<NodeId, FrozenConnectorRead>,
    pub writes: BTreeMap<NodeId, ConnectorWriteRecipeDraft>,
    pub annotations: Box<[PlanAnnotation]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FragmentPackage(FragmentPackageInput);

impl FragmentPackage {
    pub fn try_new(input: FragmentPackageInput) -> Result<Self, ValidationErrors> {
        crate::validation::validate_package(&input)?;
        Ok(Self(input))
    }

    pub const fn version(&self) -> PlanVersionId {
        self.0.version
    }

    pub const fn required(&self) -> RequiredContracts {
        self.0.required
    }

    pub const fn fragment(&self) -> &Fragment {
        &self.0.fragment
    }

    pub const fn cuts(&self) -> &FragmentCuts {
        &self.0.cuts
    }

    pub const fn result(&self) -> Option<&ResultPort> {
        self.0.result.as_ref()
    }

    pub const fn parameters(&self) -> &SemanticParameters {
        &self.0.parameters
    }

    pub fn scans(&self) -> &BTreeMap<NodeId, FrozenConnectorRead> {
        &self.0.scans
    }

    pub fn writes(&self) -> &BTreeMap<NodeId, ConnectorWriteRecipeDraft> {
        &self.0.writes
    }

    pub fn annotations(&self) -> &[PlanAnnotation] {
        &self.0.annotations
    }

    pub fn into_input(self) -> FragmentPackageInput {
        self.0
    }
}

/// Extract from the immutable complete-plan authority. No second global plan
/// validation or executable peer graph is performed here. Boundary derivation
/// is indexed once for all fragments; each output uses the BE constructor.
pub fn extract_fragment_packages(
    plan: &PhysicalPlan,
    scans: &BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>,
    parameters: &SemanticParameters,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
) -> Result<BTreeMap<FragmentId, FragmentPackage>, FragmentPackageExtractionError> {
    let mut cuts =
        derive_fragment_cuts(plan).ok_or(FragmentPackageExtractionError::BoundaryDerivation)?;
    // Display/statistics annotations for the whole plan remain FE-owned. Index
    // local subjects once so unrelated peer statistics cannot grow a package.
    let mut annotations_by_fragment: BTreeMap<FragmentId, Vec<PlanAnnotation>> = BTreeMap::new();
    for annotation in plan.annotations() {
        let id = match annotation.subject {
            AnnotationSubject::Plan => continue,
            AnnotationSubject::Fragment(id)
            | AnnotationSubject::Node(id, _)
            | AnnotationSubject::Value(id, _) => id,
        };
        annotations_by_fragment
            .entry(id)
            .or_default()
            .push(annotation.clone());
    }
    let mut outputs = BTreeMap::new();
    let mut consumed = std::collections::BTreeSet::new();
    let mut consumed_writes = std::collections::BTreeSet::new();
    for fragment in plan.fragments().values() {
        let mut local_scans = BTreeMap::new();
        let mut local_writes = BTreeMap::new();
        for node in fragment.nodes().values() {
            if let crate::NodeKind::Scan { occurrence, .. } = &node.kind {
                let fact = scans
                    .get(occurrence)
                    .ok_or(FragmentPackageExtractionError::MissingScan(*occurrence))?;
                consumed.insert(*occurrence);
                local_scans.insert(node.id, fact.clone());
            }
        }
        for node in fragment.nodes().values() {
            if let crate::NodeKind::TableWriter { target } = &node.kind {
                let fact = writes.get(&target.write_target_ordinal).ok_or(
                    FragmentPackageExtractionError::MissingWrite(target.write_target_ordinal),
                )?;
                consumed_writes.insert(target.write_target_ordinal);
                local_writes.insert(node.id, fact.clone());
            }
        }
        let annotations = annotations_by_fragment
            .remove(&fragment.id())
            .unwrap_or_default()
            .into_boxed_slice();
        let package = FragmentPackage::try_new(FragmentPackageInput {
            version: plan.version(),
            required: plan.required(),
            fragment: fragment.clone(),
            cuts: cuts
                .remove(&fragment.id())
                .ok_or(FragmentPackageExtractionError::BoundaryDerivation)?,
            result: plan
                .result_port()
                .filter(|result| result.fragment == fragment.id())
                .cloned(),
            parameters: parameters
                .project(fragment_parameter_references(fragment))
                .map_err(FragmentPackageExtractionError::Parameter)?,
            scans: local_scans,
            writes: local_writes,
            annotations,
        })
        .map_err(FragmentPackageExtractionError::Local)?;
        outputs.insert(fragment.id(), package);
    }
    if consumed.len() != scans.len() {
        return Err(FragmentPackageExtractionError::UnusedScan);
    }
    if consumed_writes.len() != writes.len() {
        return Err(FragmentPackageExtractionError::UnusedWrite);
    }
    Ok(outputs)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentPackageExtractionError {
    BoundaryDerivation,
    MissingScan(ProviderReadOccurrenceId),
    UnusedScan,
    MissingWrite(WriteTargetOrdinal),
    UnusedWrite,
    Local(ValidationErrors),
    Parameter(SemanticParameterError),
}

impl fmt::Display for FragmentPackageExtractionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BoundaryDerivation => f.write_str("complete plan boundary derivation failed"),
            Self::MissingScan(id) => write!(
                f,
                "frozen scan facts are missing for occurrence {}",
                id.get()
            ),
            Self::UnusedScan => f.write_str("frozen scan facts contain an unused occurrence"),
            Self::MissingWrite(id) => {
                write!(f, "frozen write facts are missing for target {}", id.get())
            }
            Self::UnusedWrite => f.write_str("frozen write facts contain an unused target"),
            Self::Local(errors) => errors.fmt(f),
            Self::Parameter(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for FragmentPackageExtractionError {}

/// Dependencies belong to exact call definitions, never to a function name or
/// a process setting. Projection visits each definition once and preserves IDs.
pub(crate) fn fragment_parameter_references(fragment: &Fragment) -> Vec<SemanticParameterRef> {
    let mut references = Vec::new();
    for (_, expression) in fragment.expressions().iter() {
        match &expression.kind {
            crate::ExprKind::FunctionCall { function, .. } => {
                references.extend_from_slice(&function.semantic_parameters)
            }
            crate::ExprKind::WindowCall {
                function,
                aggregate_binding,
                ..
            } => {
                references.extend_from_slice(&function.semantic_parameters);
                if let Some(binding) = aggregate_binding {
                    references.extend_from_slice(&binding.function.semantic_parameters);
                }
            }
            _ => {}
        }
    }
    for node in fragment.nodes().values() {
        if let Some((_, calls)) = node.kind.aggregate_contract() {
            for call in calls {
                references.extend_from_slice(&call.binding.function.semantic_parameters);
            }
        }
        match &node.kind {
            crate::NodeKind::TableFunction { function, .. } => {
                references.extend_from_slice(&function.semantic_parameters)
            }
            crate::NodeKind::TableWriter { target } => {
                for call in &target.partial_aggregates {
                    references.extend_from_slice(&call.binding.function.semantic_parameters);
                }
            }
            crate::NodeKind::TableFinish(finish) => {
                for call in &finish.final_aggregates {
                    references.extend_from_slice(&call.binding.function.semantic_parameters);
                }
            }
            _ => {}
        }
    }
    references
}
