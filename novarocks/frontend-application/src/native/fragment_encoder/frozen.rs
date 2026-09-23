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

//! One fragment's static plan, frozen exactly once.
//!
//! Every task of a fragment is created from the same static plan, and every
//! resend of a create repeats it. So the frontend encodes it once, here, into
//! immutable bytes that every task and every resend share, and the generated
//! message is dropped as soon as those bytes exist.
//!
//! The few typed facts the frontend needs afterwards -- which edges the sink
//! branches bind to, what the sink does, the root node an operator profile is
//! keyed by, whether the fragment writes -- are read from the very plan this
//! call encodes, in the same call. That is what makes it impossible to pair
//! these bytes with facts describing some other plan, and it is why nothing
//! later ever parses the bytes or reaches back into a generated message: the
//! plan leaves this module as bytes and facts only.
// Design: ADR-0158 (docs/adr/ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md)

use std::sync::Arc;

use novarocks_execution::task_execution::{FragmentContractVersion, FrozenBytes};
use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId};
use novarocks_proto_models::{novarocks as wire, plan};
use prost::Message;

use crate::metrics::task_creation::{RetainedPayload, static_fragment_frozen};
use crate::query_execution::artifact::FragmentId;

/// One static sink branch, in the order the static plan declares it.
///
/// The execution kernel binds its n-th sink branch to the n-th edge a task's
/// assignment names, so this order is part of the contract and is read, not
/// reconstructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StaticSinkTarget {
    target_fragment_id: FragmentId,
    target_exchange_node_id: i32,
}

impl StaticSinkTarget {
    pub(crate) const fn target_fragment_id(self) -> FragmentId {
        self.target_fragment_id
    }

    pub(crate) const fn target_exchange_node_id(self) -> i32 {
        self.target_exchange_node_id
    }
}

/// The typed facts of one frozen fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FragmentFacts {
    fragment_id: FragmentId,
    dop_domain: PipelineDopDomain,
    sink_targets: Box<[StaticSinkTarget]>,
    root_plan_node_id: i32,
    declares_table_writer: bool,
    carries_runtime_filter_bindings: bool,
}

impl FragmentFacts {
    pub(crate) const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) const fn dop_domain(&self) -> PipelineDopDomain {
        self.dop_domain
    }

    /// The static sink branches, in declared order.
    pub(crate) fn sink_targets(&self) -> &[StaticSinkTarget] {
        &self.sink_targets
    }

    /// The plan-node id of this fragment's root (output) node.
    ///
    /// It is the identity EXPLAIN ANALYZE keys a fragment by: the renderer
    /// looks each `PLAN FRAGMENT` up by its sealed root node id, and the
    /// encoder copied that id onto the frozen plan unchanged.
    pub(crate) const fn root_plan_node_id(&self) -> i32 {
        self.root_plan_node_id
    }

    /// Whether this fragment's plan contains a connector table writer.
    ///
    /// Read off the frozen plan rather than inferred from the intent: a
    /// distributed write's writer set is what decides whether the write
    /// completed, and an exchange or scan fragment of a write plan is not a
    /// writer.
    pub(crate) const fn declares_table_writer(&self) -> bool {
        self.declares_table_writer
    }

    /// Whether this fragment was encoded with its runtime-filter binding
    /// table. The backend requires the table on every fragment, empty or not,
    /// so this is not the same question as whether the plan declares a filter.
    pub(crate) const fn carries_runtime_filter_bindings(&self) -> bool {
        self.carries_runtime_filter_bindings
    }
}

/// What a frozen fragment carries besides its plan.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StaticFragmentHeader {
    pub(crate) plan_version: PlanVersionId,
    pub(crate) plan_contract_revision: u32,
    pub(crate) dop_domain: PipelineDopDomain,
}

/// One fragment's static plan as immutable bytes, with its typed facts.
///
/// Its only constructor is [`FragmentArtifact::freeze`], so the bytes and the
/// facts always come from one plan. Tasks and resends hold it by `Arc` and
/// clone the bytes' shared backing; nothing re-encodes it.
#[derive(Debug)]
pub(crate) struct FragmentArtifact {
    content: FrozenBytes,
    facts: FragmentFacts,
    /// This plan's share of the retained static-plan gauges, returned when
    /// the last task, resend or template holding the plan drops it.
    _retained: RetainedPayload,
}

impl FragmentArtifact {
    /// Encodes one fragment's static plan, once.
    ///
    /// The plan must carry its root and a known static sink: a plan without
    /// either could never be installed, and the backend would refuse it only
    /// after a task had been placed and sent. The generated message is
    /// consumed; what survives is the bytes and the facts read from it.
    pub(crate) fn freeze(
        plan: plan::PlanFragment,
        header: StaticFragmentHeader,
    ) -> Result<Arc<Self>, String> {
        let fragment_id = plan.fragment_id;
        let root = plan
            .root
            .as_ref()
            .ok_or_else(|| format!("native fragment {fragment_id} carries no root node"))?;
        let root_plan_node_id = root.node_id;
        let declares_table_writer = contains_writer(root);
        let carries_runtime_filter_bindings = plan.runtime_filter_bindings.is_some();
        let kind = plan
            .sink
            .as_ref()
            .and_then(|sink| sink.kind.as_ref())
            .ok_or_else(|| format!("native fragment {fragment_id} carries no sink"))?;
        let sink_targets = static_sink_targets(kind);
        let facts = FragmentFacts {
            fragment_id,
            dop_domain: header.dop_domain,
            sink_targets,
            root_plan_node_id,
            declares_table_writer,
            carries_runtime_filter_bindings,
        };
        let frozen = wire::FrozenFragment {
            plan_version: header.plan_version.as_bytes().to_vec(),
            plan_contract_revision: header.plan_contract_revision,
            fragment_contract_version: u32::from(FragmentContractVersion::CURRENT.get()),
            pipeline_dop_domain: Some(wire::PipelineDopDomain {
                min: header.dop_domain.min,
                max: header.dop_domain.max,
                requires_power_of_two: header.dop_domain.requires_power_of_two,
            }),
            plan: Some(plan),
        };
        let content = FrozenBytes::freeze(frozen.encode_to_vec().into());
        #[cfg(test)]
        tests::record_freeze();
        let retained = static_fragment_frozen(content.len());
        Ok(Arc::new(Self {
            content,
            facts,
            _retained: retained,
        }))
    }

    /// The frozen bytes every task of this fragment is created from.
    pub(crate) const fn content(&self) -> &FrozenBytes {
        &self.content
    }

    pub(crate) const fn facts(&self) -> &FragmentFacts {
        &self.facts
    }
}

fn contains_writer(node: &plan::DistributedNode) -> bool {
    matches!(
        node.payload.as_ref(),
        Some(plan::distributed_node::Payload::TableWriter(_))
    ) || node.children.iter().any(contains_writer)
}

/// The static sink branches, in the order the plan declares them. A sink
/// that delivers nowhere -- the result, or a sink that discards -- has none.
fn static_sink_targets(kind: &plan::data_sink::Kind) -> Box<[StaticSinkTarget]> {
    let target = |target_fragment_id, target_exchange_node_id| StaticSinkTarget {
        target_fragment_id,
        target_exchange_node_id,
    };
    match kind {
        plan::data_sink::Kind::Result(_) | plan::data_sink::Kind::Noop(_) => Box::default(),
        plan::data_sink::Kind::DataStream(stream) => {
            Box::new([target(stream.target_fragment_id, stream.dest_node_id)])
        }
        plan::data_sink::Kind::MultiCastDataStream(multicast) => multicast
            .sinks
            .iter()
            .map(|stream| target(stream.target_fragment_id, stream.dest_node_id))
            .collect(),
        plan::data_sink::Kind::ChangeStreamRouter(router) => router
            .routes
            .iter()
            .map(|route| target(route.target_fragment_id, route.target_exchange_node_id))
            .collect(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::Cell;

    use super::*;

    thread_local! {
        static FREEZES: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) fn record_freeze() {
        FREEZES.with(|count| count.set(count.get() + 1));
    }

    /// How many static plans this thread has frozen. Tests read the delta
    /// around the call they measure, so parallel tests cannot interfere.
    pub(crate) fn freezes_on_this_thread() -> usize {
        FREEZES.with(Cell::get)
    }

    fn header() -> StaticFragmentHeader {
        StaticFragmentHeader {
            plan_version: PlanVersionId::try_new([7; 16]).expect("nonzero version"),
            plan_contract_revision: 1,
            dop_domain: PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: false,
            },
        }
    }

    fn fragment(sink: plan::data_sink::Kind) -> plan::PlanFragment {
        plan::PlanFragment {
            fragment_id: 3,
            root: Some(plan::DistributedNode {
                node_id: 11,
                ..Default::default()
            }),
            sink: Some(plan::DataSink { kind: Some(sink) }),
            ..Default::default()
        }
    }

    #[test]
    fn freezing_encodes_the_plan_once_and_reads_its_facts_from_the_same_plan() {
        let before = freezes_on_this_thread();
        let artifact = FragmentArtifact::freeze(
            fragment(plan::data_sink::Kind::MultiCastDataStream(
                plan::MultiCastDataStreamSink {
                    sinks: vec![
                        plan::DataStreamSink {
                            dest_node_id: 20,
                            target_fragment_id: 5,
                            ..Default::default()
                        },
                        plan::DataStreamSink {
                            dest_node_id: 21,
                            target_fragment_id: 4,
                            ..Default::default()
                        },
                    ],
                },
            )),
            header(),
        )
        .expect("a frozen fragment");
        assert_eq!(freezes_on_this_thread() - before, 1);
        let facts = artifact.facts();
        assert_eq!(facts.fragment_id(), 3);
        assert_eq!(facts.root_plan_node_id(), 11);
        assert_eq!(
            facts
                .sink_targets()
                .iter()
                .map(|target| (
                    target.target_fragment_id(),
                    target.target_exchange_node_id()
                ))
                .collect::<Vec<_>>(),
            vec![(5, 20), (4, 21)],
            "branch order is the static plan's own"
        );
        assert!(!facts.declares_table_writer());
        assert!(!facts.carries_runtime_filter_bindings());

        let decoded = wire::FrozenFragment::decode(artifact.content().bytes().clone())
            .expect("the frozen bytes are one FrozenFragment");
        assert_eq!(decoded.plan_version, vec![7; 16]);
        assert_eq!(decoded.plan.expect("plan").fragment_id, 3);
    }

    #[test]
    fn a_plan_without_a_root_or_a_sink_cannot_be_frozen() {
        let mut no_root = fragment(plan::data_sink::Kind::Result(true));
        no_root.root = None;
        assert!(FragmentArtifact::freeze(no_root, header()).is_err());
        let mut no_sink = fragment(plan::data_sink::Kind::Result(true));
        no_sink.sink = None;
        assert!(FragmentArtifact::freeze(no_sink, header()).is_err());
    }
}
