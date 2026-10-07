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

//! The single owner of plan-global runtime-filter binding identities.
//!
//! Runtime deployment keys every runtime-filter endpoint by its binding
//! identity: the facts an attempt deploys from, participant installation and
//! scan-source matching all name an endpoint by it. The numbering spans the
//! whole plan, so a fragment package carries its own slice of it in its cuts
//! rather than recomputing one it cannot see.

use std::fmt;

use crate::{
    FragmentId, NodeId, PhysicalPlan, RuntimeFilterBindingCut, RuntimeFilterBindingRole,
    RuntimeFilterId,
};

/// One plan-global runtime-filter binding identity, and the endpoint it names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterBinding {
    pub binding_id: u32,
    pub filter: RuntimeFilterId,
    pub fragment: FragmentId,
    pub node: NodeId,
    pub role: RuntimeFilterBindingRole,
}

impl RuntimeFilterBinding {
    /// This binding as the cuts of its own fragment carry it.
    pub const fn cut(&self) -> RuntimeFilterBindingCut {
        RuntimeFilterBindingCut {
            binding_id: self.binding_id,
            filter: self.filter,
            role: self.role,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterBindingError {
    /// A fragment attaches a runtime filter the plan does not define.
    AbsentRuntimeFilter {
        fragment: FragmentId,
        filter: RuntimeFilterId,
    },
    /// The plan has more endpoints than the `u32` identity space numbers.
    IdentitySpaceExhausted,
}

impl fmt::Display for RuntimeFilterBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AbsentRuntimeFilter { fragment, filter } => write!(
                formatter,
                "fragment {} references absent runtime filter {}",
                fragment.get(),
                filter.get()
            ),
            Self::IdentitySpaceExhausted => {
                formatter.write_str("runtime-filter binding identity space exhausted")
            }
        }
    }
}

impl std::error::Error for RuntimeFilterBindingError {}

/// Number every runtime-filter binding of one plan, once.
///
/// The numbering is a property of the plan: fragments in id order, each
/// fragment's attached filters in its own order, producers before consumers,
/// each role in its endpoint order, starting at 1. Everything that needs a
/// binding identity -- a wire encoder, the scan sources, the facts an attempt
/// deploys from, and the cuts of every fragment package -- reads this one
/// derivation, because two derivations of one numbering disagree the moment
/// either changes, and the disagreement would surface as a plan that cannot
/// be deployed rather than as the numbering bug it is.
///
/// One fragment's bindings are therefore consecutive identities, which is
/// what lets a package check its own slice without the rest of the plan.
pub fn runtime_filter_bindings(
    plan: &PhysicalPlan,
) -> Result<Vec<RuntimeFilterBinding>, RuntimeFilterBindingError> {
    let mut bindings = Vec::new();
    let mut next_binding = 1_u32;
    let mut mint = |filter: RuntimeFilterId,
                    fragment: FragmentId,
                    node: NodeId,
                    role: RuntimeFilterBindingRole|
     -> Result<(), RuntimeFilterBindingError> {
        let binding_id = next_binding;
        next_binding = next_binding
            .checked_add(1)
            .ok_or(RuntimeFilterBindingError::IdentitySpaceExhausted)?;
        bindings.push(RuntimeFilterBinding {
            binding_id,
            filter,
            fragment,
            node,
            role,
        });
        Ok(())
    };
    for fragment in plan.fragments().values() {
        for filter_id in fragment.runtime_filters() {
            let filter = plan.runtime_filters().get(filter_id).ok_or(
                RuntimeFilterBindingError::AbsentRuntimeFilter {
                    fragment: fragment.id(),
                    filter: *filter_id,
                },
            )?;
            for (index, producer) in filter.producers.iter().enumerate() {
                if producer.endpoint.fragment == fragment.id() {
                    mint(
                        filter.id,
                        fragment.id(),
                        producer.endpoint.node,
                        RuntimeFilterBindingRole::Producer(index),
                    )?;
                }
            }
            for (index, consumer) in filter.consumers.iter().enumerate() {
                if consumer.endpoint.fragment == fragment.id() {
                    mint(
                        filter.id,
                        fragment.id(),
                        consumer.endpoint.node,
                        RuntimeFilterBindingRole::Consumer(index),
                    )?;
                }
            }
        }
    }
    Ok(bindings)
}
