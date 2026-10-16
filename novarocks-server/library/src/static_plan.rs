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

//! Server-owned choice of the one static plan interpreter.
//!
//! Every role in one binary agrees on how a fragment's static plan travels:
//! the frontend freezes one carrier and every backend interprets exactly that
//! carrier. Production composes the plan-tree interpreter. The debug-only
//! `physical-wire-v2-candidate` binary composes the compiled-package
//! interpreter instead, and the choice enters the Native compatibility
//! material, so a candidate process and a production process never share an
//! island. Nothing per task or per statement selects an interpreter.

use novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet;
use novarocks_native_adapter::backend_application::BackendStaticPlanInterpreter;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use novarocks_version::StaticPlanInterpreter;

use crate::provider_manifest::ServerProviderManifest;

#[cfg(all(feature = "physical-wire-v2-candidate", not(debug_assertions)))]
compile_error!("physical-wire-v2-candidate is only supported by debug and dev-opt builds");

/// The static plan interpreter this binary composes, resolved once per
/// process before compatibility material and before any role composes.
pub struct ServerStaticPlan {
    choice: Choice,
}

enum Choice {
    PlanTree,
    #[cfg(feature = "physical-wire-v2-candidate")]
    CompiledPackage(crate::physical_wire_candidate::CandidateCompiledPackage),
}

/// The static carrier this process's frontend must freeze for its island.
#[derive(Clone, Copy, Debug)]
pub enum FrontendStaticPlanCarrier {
    /// The plan tree, read by the production interpreter.
    PlanTree,
    /// The physical package, read by the candidate compiled-package
    /// interpreter. The admission is the one the backend receiver uses.
    #[cfg(feature = "physical-wire-v2-candidate")]
    CompiledPackage {
        admission: novarocks_physical_plan::FragmentPackageAdmission,
        limits: novarocks_plan_codec::physical_package_v2::PackageEncodeLimits,
    },
}

impl ServerStaticPlan {
    /// The production plan-tree interpreter.
    pub fn plan_tree() -> Self {
        Self {
            choice: Choice::PlanTree,
        }
    }

    /// Composes this binary's interpreter from the process's sealed function
    /// set and provider manifest. A production build ignores both: its
    /// interpreter needs neither a pure catalogue nor pure provider ports.
    pub fn compose(
        function_set: &SealedExecutionFunctionSet,
        provider_manifest: &ServerProviderManifest,
    ) -> anyhow::Result<Self> {
        #[cfg(feature = "physical-wire-v2-candidate")]
        let choice = Choice::CompiledPackage(
            crate::physical_wire_candidate::CandidateCompiledPackage::compose(
                function_set,
                provider_manifest,
            )?,
        );
        #[cfg(not(feature = "physical-wire-v2-candidate"))]
        let choice = {
            let _ = (function_set, provider_manifest);
            Choice::PlanTree
        };
        Ok(Self { choice })
    }

    /// The interpreter component of this process's compatibility material.
    pub fn compatibility_component(&self) -> StaticPlanInterpreter {
        match &self.choice {
            Choice::PlanTree => StaticPlanInterpreter::PlanTree,
            #[cfg(feature = "physical-wire-v2-candidate")]
            Choice::CompiledPackage(candidate) => StaticPlanInterpreter::CompiledPackage {
                pure_function_catalog_digest: candidate.pure_function_catalog_digest(),
            },
        }
    }

    /// The backend interpreter. A compiled-package process builds its one
    /// package decode model here, so the backend composes this exactly once.
    pub fn backend_interpreter(&self) -> anyhow::Result<BackendStaticPlanInterpreter> {
        match &self.choice {
            Choice::PlanTree => Ok(BackendStaticPlanInterpreter::PlanTree),
            #[cfg(feature = "physical-wire-v2-candidate")]
            Choice::CompiledPackage(candidate) => candidate.backend_interpreter(),
        }
    }

    /// The carrier the frontend must freeze; see the frontend composition
    /// hook in `composition::compose_frontend_role_config`.
    pub fn frontend_carrier(&self) -> FrontendStaticPlanCarrier {
        match &self.choice {
            Choice::PlanTree => FrontendStaticPlanCarrier::PlanTree,
            #[cfg(feature = "physical-wire-v2-candidate")]
            Choice::CompiledPackage(candidate) => candidate.frontend_carrier(),
        }
    }
}

/// Startup composition runs once, before any listener opens, over statically
/// sized inputs; nothing could cancel it, so this control never refuses.
pub(crate) struct CompositionControl;

impl PureCompileControl for CompositionControl {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

/// The candidate composition is tested beside its inputs, in
/// `physical_wire_candidate`.
#[cfg(all(test, not(feature = "physical-wire-v2-candidate")))]
mod tests {
    use super::*;

    /// A production build composes the plan tree in every role, and its
    /// compatibility component is the constant plan-tree component.
    #[test]
    fn a_production_binary_composes_the_plan_tree_interpreter() {
        let function_set = crate::composition::compose_process_function_set()
            .expect("sealed process function set");
        let manifest = ServerProviderManifest::seal().expect("server provider manifest");
        let plan = ServerStaticPlan::compose(&function_set, &manifest).expect("static plan");
        assert_eq!(
            plan.compatibility_component(),
            StaticPlanInterpreter::PlanTree
        );
        assert!(matches!(
            plan.frontend_carrier(),
            FrontendStaticPlanCarrier::PlanTree
        ));
        assert!(matches!(
            plan.backend_interpreter().expect("backend interpreter"),
            BackendStaticPlanInterpreter::PlanTree
        ));
    }
}
