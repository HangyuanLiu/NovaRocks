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

//! Owned receiving input for the original unpublished structural stage.

use crate::validation::{ValidationContext, validate_fragment_construction_after_admission};
use crate::{
    ExprArena, Fragment, FragmentId, FragmentParts, FragmentSink, FragmentStructureError, NodeId,
    PhysicalNode, PipelineDopDomain, PlanLimits, RuntimeFilterId, ValidationError,
    ValidationErrors, ValueDef, ValueId,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::collections::BTreeMap;

/// Exact sparse owners produced by a receiving composition. This input is not
/// a published Package or a substitute for its original requests and proofs.
/// The caller admits these maps, arrays and validator scratch before entry.
#[derive(Clone, Debug)]
pub struct FragmentStructureInput {
    pub id: FragmentId,
    pub root: NodeId,
    pub values: BTreeMap<ValueId, ValueDef>,
    pub expressions: ExprArena,
    pub nodes: BTreeMap<NodeId, PhysicalNode>,
    pub sink: FragmentSink,
    pub dop_domain: PipelineDopDomain,
    pub runtime_filters: Box<[RuntimeFilterId]>,
}

impl Fragment {
    /// Move original sparse namespaces into the same checked construction stage.
    /// No next-ID allocator or guessed definition is involved. As with a
    /// builder's structural finish, exact call requests must be installed and
    /// complete roots/calls/properties/cuts validated before Package publication.
    /// Resource/graph validation remains the original bounded opaque author;
    /// these boundaries do not attest its internal cooperative work or MEM grant.
    pub fn try_from_structure_observed(
        input: FragmentStructureInput,
        limits: PlanLimits,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FragmentStructureError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            let mut errors = ValidationContext::for_construction(limits);
            crate::builder::admit_structure_counts(
                input.nodes.len(),
                input.values.len(),
                input.expressions.len(),
                input.runtime_filters.len(),
                limits,
                &mut errors,
                &mut work,
            )?;
            if !errors.is_empty() {
                return Err(FragmentStructureError::Structure(
                    ValidationErrors::from_collector(errors),
                ));
            }
            // Public maps preserve sparse keys, but each key must still name
            // its original definition. Mutable builders established this while
            // inserting; receiving ownership establishes it here before use.
            for (id, value) in &input.values {
                let matches = *id == value.id;
                work.step()?;
                if !matches {
                    errors.push(ValidationError::new(
                        "fragment.structure.values",
                        "value map key differs from its definition identity",
                    ));
                }
                if errors.is_saturated() {
                    break;
                }
            }
            for (id, node) in &input.nodes {
                let matches = *id == node.id;
                work.step()?;
                if !matches {
                    errors.push(ValidationError::new(
                        "fragment.structure.nodes",
                        "node map key differs from its definition identity",
                    ));
                }
                if errors.is_saturated() {
                    break;
                }
            }
            if !errors.is_empty() {
                return Err(FragmentStructureError::Structure(
                    ValidationErrors::from_collector(errors),
                ));
            }
            let fragment = Fragment::from(FragmentParts {
                id: input.id,
                root: input.root,
                values: input.values,
                expressions: input.expressions,
                nodes: input.nodes,
                sink: input.sink,
                dop_domain: input.dop_domain,
                runtime_filters: input.runtime_filters,
                call_requests: crate::FragmentCallRequests::unpublished_empty(input.id),
            });
            work.flush()?;
            let mut usage = crate::resource::CutResourcePreflight::new();
            usage.add_fragment(&fragment, &mut errors);
            usage.validate("fragment.structure.resources", &mut errors);
            work.step()?;
            work.flush()?;
            if !errors.is_empty() {
                return Err(FragmentStructureError::Structure(
                    ValidationErrors::from_collector(errors),
                ));
            }
            work.flush()?;
            let validation = validate_fragment_construction_after_admission(&fragment, limits);
            work.step()?;
            work.flush()?;
            validation?;
            Ok(fragment)
        })();
        if matches!(&result, Err(FragmentStructureError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
}

#[cfg(test)]
#[path = "structure_input/tests.rs"]
mod tests;
