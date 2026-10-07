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

//! Runtime-filter consumers of a compiled LocalProgram node.
//!
//! The consumer state is the same arena-free state an ExprArena plan uses:
//! bindings, subscriptions, the gate and the row masks. Only the keys differ.
//! Each binding's key is the node's `RuntimeFilter { binding }` root, and every
//! driver evaluates it through its own `CompiledExpressionInstance`. The root
//! reads exactly its frozen input port, so a chunk is filtered as it arrives:
//! nothing is hydrated or retyped before a key is evaluated.

use std::sync::Arc;

use arrow::array::ArrayRef;
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
};

use super::{RuntimeFilterConsumerState, RuntimeFilterKeyProvider};
use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::node::runtime_filter::RuntimeFilterExecutionContract;
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, evaluate_all, instances, root_value_type,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::RuntimeErrorState;
use crate::runtime_filter as execution;

/// The runtime-filter consumers of one compiled node, shared by its drivers:
/// the consumer state and the key root of every binding, in binding order.
pub(crate) struct CompiledRuntimeFilterConsumers {
    state: RuntimeFilterConsumerState,
    program: Arc<LocalProgram>,
    sites: Arc<[ProgramExpressionRootSite]>,
    error: Arc<RuntimeErrorState>,
}

impl CompiledRuntimeFilterConsumers {
    /// `contracts` are the runtime contracts of `node`'s runtime-filter
    /// bindings in their frozen order; binding `i` is keyed by the node's
    /// `RuntimeFilter { binding: i }` root, whose static type must be exactly
    /// the binding's membership key type.
    pub(crate) fn try_new(
        owner: &'static str,
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        contracts: Vec<execution::RuntimeFilterConsumerContract>,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let sites = (0..contracts.len())
            .map(|binding| {
                let binding = u32::try_from(binding)
                    .map_err(|_| "compiled runtime-filter binding count exceeds u32")?;
                Ok(ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::RuntimeFilter { binding },
                })
            })
            .collect::<Result<Arc<[_]>, String>>()?;
        let state = RuntimeFilterConsumerState::from_contracts(
            owner,
            contracts,
            |index, contract| {
                let site = sites[index];
                let key = root_value_type(&program, site)?;
                let RuntimeFilterExecutionContract::Membership(schema) = contract.contract() else {
                    return Err(format!(
                        "native {owner} runtime-filter binding_id={} requires a membership SetUnion contract",
                        contract.binding_id().get()
                    ));
                };
                if &key.data_type != schema.data_type() {
                    return Err(format!(
                        "compiled {owner} runtime-filter binding_id={} key root at local node {} has type {:?}, not its membership type {:?}",
                        contract.binding_id().get(),
                        node.index(),
                        key.data_type,
                        schema.data_type()
                    ));
                }
                Ok(())
            },
        )?;
        Ok(Self {
            state,
            program,
            sites,
            error,
        })
    }

    /// The consumer state every driver of the node shares.
    pub(crate) fn state(&self) -> &RuntimeFilterConsumerState {
        &self.state
    }

    /// The key roots one driver evaluates: its own instances, created on its
    /// first filtered chunk.
    pub(crate) fn driver_keys(&self) -> CompiledRuntimeFilterKeys {
        CompiledRuntimeFilterKeys {
            program: Arc::clone(&self.program),
            sites: Arc::clone(&self.sites),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
        }
    }
}

/// One driver's instances of a compiled consumer's key roots.
pub(crate) struct CompiledRuntimeFilterKeys {
    program: Arc<LocalProgram>,
    sites: Arc<[ProgramExpressionRootSite]>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
}

impl RuntimeFilterKeyProvider for CompiledRuntimeFilterKeys {
    type Error = ExecutionFailure;

    /// The frozen root port is exact: the chunk is evaluated as delivered.
    fn prepare_input(&mut self, chunk: Chunk) -> ExecutionResult<Chunk> {
        Ok(chunk)
    }

    fn evaluate_key(&mut self, binding: usize, input: &Chunk) -> ExecutionResult<ArrayRef> {
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let site = *self
            .sites
            .get(binding)
            .ok_or("compiled runtime-filter binding index drifted")?;
        let instance = self
            .instances
            .as_mut()
            .and_then(|instances| instances.get_mut(binding))
            .ok_or("compiled runtime-filter key instance is missing")?;
        evaluate_all(instance, site, &input.batch, &self.control)
    }
}
