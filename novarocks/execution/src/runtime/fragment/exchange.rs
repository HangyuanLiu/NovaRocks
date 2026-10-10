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

use crate::exec::pipeline::binding::{ExchangeBinding, ExchangeBindings};
use crate::runtime::fragment::instance::{ExchangeInputAssignments, FragmentInstanceSpec};
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::exec::chunk::ChunkSchema;
use crate::exec::fragment::program::{FragmentNodeId, FragmentProgram};
use crate::runtime::exchange::{ExchangeColumnBinding, ExchangeKey};
use crate::runtime::fragment::io::{
    ExchangeReceiverKey, ExchangeReceiverPort, ExchangeReceiverRegistration,
};
use novarocks_types::UniqueId;

/// Materialize per-node exchange bindings from the validated instance spec.
/// Program exchange contracts (schema) were already cross-checked by
/// `FragmentSubmission::try_new`; here we only project the dynamic parts
/// (sender count + this instance's `ExchangeKey`) into an exec-level carrier.
pub(crate) fn materialize_exchange_bindings(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
    receiver_port: Arc<dyn ExchangeReceiverPort>,
) -> ExchangeBindings {
    let finst = instance.fragment_instance_id().get();
    let mut bindings = ExchangeBindings::default();
    for node_id in program.exchange_inputs().keys() {
        let assignment = instance
            .exchange_inputs()
            .get(node_id)
            .expect("submission validation guarantees an exchange assignment per contract");
        let key = ExchangeKey {
            finst_id_hi: finst.high(),
            finst_id_lo: finst.low(),
            node_id: node_id.get(),
        };
        bindings.insert(
            node_id.get(),
            ExchangeBinding {
                key,
                expected_senders: assignment.sender_count().get(),
                receiver_port: Arc::clone(&receiver_port),
            },
        );
    }
    bindings
}

/// The receivers one compiled fragment instance registers before it runs,
/// and the pipeline bindings its exchange sources pull from.
pub(crate) struct CompiledExchangeReceivers {
    pub(crate) registrations: Vec<ExchangeReceiverRegistration>,
    pub(crate) bindings: ExchangeBindings,
}

/// Project a compiled program's exchange inputs into receiver registrations
/// and pipeline bindings for one fragment instance.
///
/// A compiled receiver is addressed by its edge's physical destination node,
/// the same id the sender routes to, so `assignments` is keyed by that node
/// and must cover exactly the program's receivers. Its expected schema is the
/// compiled ExchangeSource layout, and it binds wire columns by position:
/// compiled sender and receiver slot ids are allocated per fragment and are
/// never comparable.
pub(crate) fn materialize_compiled_exchange_receivers(
    program: &novarocks_local_program::LocalProgram,
    fragment_instance_id: UniqueId,
    assignments: &ExchangeInputAssignments,
    receiver_port: Arc<dyn ExchangeReceiverPort>,
) -> Result<CompiledExchangeReceivers, String> {
    let nodes = program.graph().nodes();
    let mut receivers = BTreeSet::new();
    let mut registrations = Vec::with_capacity(program.exchange_inputs().len());
    let mut bindings = ExchangeBindings::default();
    for (id, input) in program.exchange_inputs() {
        let node = nodes.get(id.index()).ok_or_else(|| {
            format!(
                "compiled exchange input names missing local node {}",
                id.index()
            )
        })?;
        if !matches!(
            node.kind(),
            novarocks_local_program::ProgramNodeKind::ExchangeSource { .. }
        ) {
            return Err(format!(
                "compiled exchange input at local node {} is not an exchange source",
                id.index()
            ));
        }
        let receiver = i32::try_from(input.receiver_node).map_err(|_| {
            format!(
                "compiled exchange receiver node {} exceeds i32",
                input.receiver_node
            )
        })?;
        if !receivers.insert(receiver) {
            return Err(format!(
                "compiled exchange receiver node {receiver} is addressed by more than one source"
            ));
        }
        let assignment = assignments
            .get(&FragmentNodeId::new(receiver))
            .ok_or_else(|| {
                format!("missing exchange assignment for compiled receiver node {receiver}")
            })?;
        let expected_senders = assignment.sender_count().get();
        let key = ExchangeReceiverKey {
            fragment_instance_id,
            node_id: receiver,
        };
        registrations.push(ExchangeReceiverRegistration {
            key,
            expected_senders,
            expected_chunk_schema: ChunkSchema::from_compiled_layout(node.output_layout())?,
            column_binding: ExchangeColumnBinding::Positional,
        });
        bindings.insert(
            receiver,
            ExchangeBinding {
                key: ExchangeKey {
                    finst_id_hi: fragment_instance_id.high(),
                    finst_id_lo: fragment_instance_id.low(),
                    node_id: receiver,
                },
                expected_senders,
                receiver_port: Arc::clone(&receiver_port),
            },
        );
    }
    if let Some((extra, _)) = assignments
        .iter()
        .find(|(node_id, _)| !receivers.contains(&node_id.get()))
    {
        return Err(format!(
            "exchange assignment for node {} has no compiled exchange source",
            extra.get()
        ));
    }
    Ok(CompiledExchangeReceivers {
        registrations,
        bindings,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgram,
        FragmentProgramOptions, FragmentSinkSpec, RuntimeFilterContract,
    };
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::exchange::ExchangeKey;
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
        FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::query_options::QueryOptions;
    use novarocks_types::QueryId;
    use novarocks_types::UniqueId;

    use super::materialize_exchange_bindings;

    fn values_program(
        exchange_inputs: BTreeMap<FragmentNodeId, ExchangeInputContract>,
    ) -> FragmentProgram {
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id: 1,
                }),
            },
        };
        let profile = plan
            .local_compile_profile(NonZeroUsize::new(1).unwrap(), None)
            .unwrap();
        let (local, runtime) = plan
            .into_local_program_and_bindings(
                profile,
                BTreeMap::new(),
                Vec::new(),
                novarocks_local_program::StaticSinkProgram::Noop,
            )
            .unwrap();
        assert_eq!(runtime.scan_count(), 0);
        FragmentProgram::try_new(
            Arc::new(local),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            exchange_inputs,
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        )
        .unwrap()
    }

    fn instance_with_exchange(
        exchange_inputs: ExchangeInputAssignments,
        fragment_instance_id: UniqueId,
    ) -> FragmentInstanceSpec {
        FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId::new(1, 2),
            FragmentInstanceId::new(fragment_instance_id),
            ScanAssignments::default(),
            exchange_inputs,
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).expect("non-zero DOP"),
            BackendNum::try_new(1).expect("backend number"),
        )
    }

    #[test]
    fn exchange_binding_takes_sender_count_from_instance_and_key_from_finst_id() {
        let node_id = FragmentNodeId::new(5);
        let program = values_program(BTreeMap::from([(
            node_id,
            ExchangeInputContract::new(Arc::new(ChunkSchema::empty())),
        )]));
        let instance = instance_with_exchange(
            ExchangeInputAssignments::new(BTreeMap::from([(
                node_id,
                ExchangeInputAssignment::new(NonZeroUsize::new(3).expect("non-zero sender count")),
            )])),
            UniqueId::new(11, 22),
        );

        let bindings = materialize_exchange_bindings(
            &program,
            &instance,
            Arc::new(crate::runtime::fragment::io::UnavailableExchangeReceiverPort),
        );

        let binding = bindings.get(5).expect("binding for exchange node 5");
        assert_eq!(binding.expected_senders, 3);
        assert_eq!(
            binding.key,
            ExchangeKey {
                finst_id_hi: 11,
                finst_id_lo: 22,
                node_id: 5,
            }
        );
    }
}
