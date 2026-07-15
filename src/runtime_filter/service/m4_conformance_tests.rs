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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::coordinator::scheduler::{
    FragmentInstancePlacement, LiveBackendSnapshot, SchedulingPlan,
};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::RuntimeFilterDeploymentPolicy;
use crate::runtime_filter::deployment::compiler;
use crate::runtime_filter::materializer::codec::{
    ArtifactDecodeExpectations, decode_leaf, encode_physical_leaf,
};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder, NullSemantics,
    OrderContract, OrderKeyContract, PlanFragmentId, PlanNodeId, ReductionRequirement,
    RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
    SortDirection, TopKSummaryRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::graph::{
    ApplyPoint, ConsumerRequirement, PlanLocation, ProducerRequirement, RuntimeFilterBindingRole,
    RuntimeFilterBindingSpec, RuntimeFilterChannelSpec, RuntimeFilterGraph,
};
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    PhysicalArtifact,
};
use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    MaterializationPolicy, RuntimeFilterCoreBudget, RuntimeFilterInstallView,
};
use crate::runtime_filter::port::ordered_bound::{
    COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
    RuntimeOrderContract, comparator_digest_for_test,
};
use crate::runtime_filter::port::producer::{
    InstallOutcome, OrderedBoundProducerAdapter, ProducerAdapter, ProducerHandle, ProducerPortKind,
    RuntimeContractViolation, SubmitOutcome, TopKSummaryProducerAdapter,
};
use crate::runtime_filter::port::subscription::{
    BlockingSnapshotSubscription, LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription,
    SubscriptionHandle, SubscriptionKind,
};
use crate::runtime_filter::port::support::{
    ArtifactRetainedBudget, RuntimeFilterClock, RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::topk_summary::{RuntimeTopKSummaryContract, TopKSummary};
use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};
use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
use crate::sql::planner::distributed::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
};

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;

const CHANNEL: ChannelId = ChannelId::new(1);
const PRODUCER_A: BindingId = BindingId::new(10);
const PRODUCER_B: BindingId = BindingId::new(20);
const CONSUMER: BindingId = BindingId::new(30);
const WITNESS_A: CoverageWitnessId = CoverageWitnessId::new(101);
const WITNESS_B: CoverageWitnessId = CoverageWitnessId::new(102);
const PRODUCER_FRAGMENT_A: PlanFragmentId = PlanFragmentId::new(1);
const PRODUCER_FRAGMENT_B: PlanFragmentId = PlanFragmentId::new(2);
const CONSUMER_FRAGMENT: PlanFragmentId = PlanFragmentId::new(3);
const INSTANCE_A: UniqueId = UniqueId { hi: 94, lo: 10 };
const INSTANCE_B: UniqueId = UniqueId { hi: 94, lo: 20 };
const CONSUMER_INSTANCE: UniqueId = UniqueId { hi: 94, lo: 30 };
const PARTICIPANT: RuntimeFilterParticipantId = RuntimeFilterParticipantId::new(0);

struct ProducerFixture {
    binding: BindingId,
    witness: CoverageWitnessId,
    fragment: PlanFragmentId,
    instance: UniqueId,
}

struct MembershipHarness {
    service: Arc<RuntimeFilterService>,
    blocking: Arc<dyn BlockingSnapshotSubscription>,
}

struct MembershipProducer {
    port: Arc<dyn ProducerAdapter>,
}

impl MembershipProducer {
    fn submit_values(
        &self,
        partition: u32,
        sequence: u64,
        values: impl IntoIterator<Item = i64>,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.port.submit(
            PartitionId::new(partition),
            ProducerSequence::new(sequence),
            ValueDomainDelta::new(MembershipValues::int64(values), false),
        )
    }

    fn close(
        &self,
        partition: u32,
        terminal_sequence: u64,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.port.close_partition(
            PartitionId::new(partition),
            ProducerSequence::new(terminal_sequence),
        )
    }
}

impl MembershipHarness {
    fn producer(&self, binding: BindingId, instance: UniqueId) -> MembershipProducer {
        let ProducerHandle::Membership(port) = self
            .service
            .open_producer(binding, instance, 1, ProducerPortKind::Membership)
            .expect("compiler-installed producer is authorized")
        else {
            panic!("membership graph must install only the Membership producer port")
        };
        MembershipProducer { port }
    }
}

struct DeterministicClock(Instant);

impl RuntimeFilterClock for DeterministicClock {
    fn now(&self) -> Instant {
        self.0
    }
}

#[derive(Default)]
struct RecordingEvents(Mutex<Vec<RuntimeFilterEvent>>);

impl RuntimeFilterEventSink for RecordingEvents {
    fn record(&self, event: RuntimeFilterEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn producer_fixtures() -> [ProducerFixture; 2] {
    [
        ProducerFixture {
            binding: PRODUCER_A,
            witness: WITNESS_A,
            fragment: PRODUCER_FRAGMENT_A,
            instance: INSTANCE_A,
        },
        ProducerFixture {
            binding: PRODUCER_B,
            witness: WITNESS_B,
            fragment: PRODUCER_FRAGMENT_B,
            instance: INSTANCE_B,
        },
    ]
}

fn expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        data_type: DataType::Int64,
        nullable: false,
    }
}

fn membership_graph(
    coverage: Coverage,
    producers: &[ProducerFixture],
    activation: ConsumerActivation,
) -> RuntimeFilterGraph {
    let capabilities = BTreeSet::from([
        ArtifactCapability::Membership,
        ArtifactCapability::EmptyDomain,
    ]);
    let contributions = BTreeSet::from([
        ContributionKind::ValueDomainDelta,
        ContributionKind::ProducerClosed,
    ]);
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: CHANNEL,
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
        })
        .unwrap();
    for (index, producer) in producers.iter().enumerate() {
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: producer.binding,
                channel_id: CHANNEL,
                coverage_witness_id: Some(producer.witness),
                location: PlanLocation {
                    fragment_id: producer.fragment,
                    node_id: PlanNodeId::new(index as i32 + 1),
                },
                expression: expression(),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                    contribution_kinds: contributions.clone(),
                    completion_requirement: CompletionRequirement::ProducerClosed,
                }),
            })
            .unwrap();
    }
    graph
        .insert_binding(RuntimeFilterBindingSpec {
            binding_id: CONSUMER,
            channel_id: CHANNEL,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: CONSUMER_FRAGMENT,
                node_id: PlanNodeId::new(30),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities,
                activation,
            }),
        })
        .unwrap();
    graph
        .validate()
        .expect("M4 fixture graph must pass RFD-1 validation before compilation");
    graph
}

fn placement(
    fragment: PlanFragmentId,
    instance_index: usize,
    instance: UniqueId,
    endpoint: SocketAddr,
) -> FragmentInstancePlacement {
    FragmentInstancePlacement {
        fragment_id: fragment.get(),
        instance_index,
        finst_id: instance,
        backend_idx: PARTICIPANT.get() as usize,
        endpoint: RuntimeEndpoint::from_socket_addr(endpoint),
        scan_ranges: BTreeMap::new(),
        destinations: Vec::new(),
        runtime_filter_prober_params: BTreeMap::new(),
        per_exch_num_senders: BTreeMap::new(),
    }
}

fn scheduling_plan(producers: &[ProducerFixture]) -> SchedulingPlan {
    let endpoint = fixture_endpoint();
    let mut by_fragment = producers
        .iter()
        .map(|producer| {
            (
                producer.fragment.get(),
                vec![placement(producer.fragment, 0, producer.instance, endpoint)],
            )
        })
        .collect::<BTreeMap<_, _>>();
    by_fragment.insert(
        CONSUMER_FRAGMENT.get(),
        vec![placement(CONSUMER_FRAGMENT, 0, CONSUMER_INSTANCE, endpoint)],
    );
    SchedulingPlan {
        root_fragment_id: CONSUMER_FRAGMENT.get(),
        by_fragment,
        root_finst_id: CONSUMER_INSTANCE,
        root_backend_idx: PARTICIPANT.get() as usize,
    }
}

fn fixture_endpoint() -> SocketAddr {
    "127.0.0.1:9060".parse().unwrap()
}

fn fragment_edges(producers: &[ProducerFixture]) -> Vec<FragmentEdge> {
    producers
        .iter()
        .enumerate()
        .map(|(index, producer)| FragmentEdge {
            source_fragment_id: producer.fragment.get(),
            target_fragment_id: CONSUMER_FRAGMENT.get(),
            target_exchange_node_id: index as i32 + 1,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        })
        .collect()
}

fn compile_install_view(
    graph: &RuntimeFilterGraph,
    scheduling: &SchedulingPlan,
    edges: &[FragmentEdge],
    participant: RuntimeFilterParticipantId,
) -> RuntimeFilterInstallView {
    let backends = LiveBackendSnapshot::from_endpoints(vec![fixture_endpoint()]);
    let policy = RuntimeFilterDeploymentPolicy {
        core_budget: RuntimeFilterCoreBudget::new(16 * 1024),
        replica_redundancy: backends.entries().len() as u32,
        materialization: MaterializationPolicy::for_test(),
    };
    compiler::compile(
        graph,
        scheduling,
        edges,
        &backends,
        &policy,
        DeploymentEpoch::new(1),
    )
    .expect("valid graph and live placement must compile")
    .install_views
    .remove(&participant)
    .expect("compiler must project the colocated participant install view")
}

fn install_service(view: RuntimeFilterInstallView) -> Arc<RuntimeFilterService> {
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        UniqueId { hi: 94, lo: 0 },
        Arc::new(DeterministicClock(Instant::now())),
        Arc::new(RecordingEvents::default()),
        MemTrackerMemoryAccount::new_root_for_test("m4-join-conformance"),
    ));
    assert_eq!(service.install(view).unwrap(), InstallOutcome::Installed);
    service
}

fn join_harness(coverage: Coverage) -> MembershipHarness {
    let producers = producer_fixtures();
    let graph = membership_graph(coverage, &producers, ConsumerActivation::BlockingSnapshot);
    let scheduling = scheduling_plan(&producers);
    let edges = fragment_edges(&producers);
    let service = install_service(compile_install_view(
        &graph,
        &scheduling,
        &edges,
        PARTICIPANT,
    ));
    for producer in &producers {
        let ProducerHandle::Membership(_) = service
            .open_producer(
                producer.binding,
                producer.instance,
                1,
                ProducerPortKind::Membership,
            )
            .expect("all scheduled producer instances open before execution")
        else {
            panic!("membership graph must install only Membership producer ports")
        };
    }
    let SubscriptionHandle::Blocking(blocking) = service
        .subscribe(
            CONSUMER,
            CONSUMER_INSTANCE,
            SubscriptionKind::BlockingSnapshot,
        )
        .expect("compiler-installed blocking consumer is authorized")
    else {
        panic!("blocking graph consumer must install only BlockingSnapshot")
    };
    MembershipHarness { service, blocking }
}

fn join_allof_harness() -> MembershipHarness {
    join_harness(Coverage::AllOf(vec![
        Coverage::Leaf(WITNESS_A),
        Coverage::Leaf(WITNESS_B),
    ]))
}

fn join_anyof_harness() -> MembershipHarness {
    join_harness(Coverage::AnyOf(vec![
        Coverage::Leaf(WITNESS_A),
        Coverage::Leaf(WITNESS_B),
    ]))
}

fn publish_membership(
    harness: &MembershipHarness,
    binding: BindingId,
    instance: UniqueId,
    values: &[i64],
) {
    let producer = harness.producer(binding, instance);
    producer
        .submit_values(0, 0, values.iter().copied())
        .unwrap();
    producer.close(0, 1).unwrap();
}

fn membership_payload(artifact: &PhysicalArtifact) -> &[u8] {
    let bytes = artifact.canonical_bytes();
    assert_eq!(&bytes[..4], b"NRFL");
    let schema_len = u16::from_be_bytes(bytes[39..41].try_into().unwrap()) as usize;
    let mut cursor = 41 + schema_len;
    assert_eq!(
        LogicalVersion::new(u64::from_be_bytes(
            bytes[cursor..cursor + 8].try_into().unwrap()
        )),
        artifact.version()
    );
    cursor += 8;
    let flags = bytes[cursor];
    assert_eq!(flags & 1 != 0, artifact.contains_null());
    cursor += 1;
    assert_eq!(bytes[cursor], 0, "membership ValueSet has no hash contract");
    cursor += 1;
    let payload_len = u64::from_be_bytes(bytes[cursor..cursor + 8].try_into().unwrap()) as usize;
    cursor += 8;
    assert_eq!(cursor + payload_len, bytes.len());
    &bytes[cursor..]
}

fn assert_membership_values(bundle: &ArtifactBundle, expected: &[i64]) {
    let [(ArtifactKind::ValueSet, artifact)] = bundle.artifacts() else {
        panic!("non-empty Int64 membership must publish one ValueSet leaf")
    };
    let payload = membership_payload(artifact);
    assert_eq!(payload[0], 5, "canonical membership payload must be Int64");
    let count = u64::from_be_bytes(payload[1..9].try_into().unwrap()) as usize;
    assert_eq!(payload.len(), 9 + count * 8);
    let values = payload[9..]
        .chunks_exact(8)
        .map(|bytes| i64::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(values, expected);
}

fn membership_profile() -> ConsumerArtifactProfile {
    ConsumerArtifactProfile::new(
        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
        None,
    )
    .unwrap()
}

fn assert_fixture_remote_equivalent(local: &ArtifactBundle) {
    let [(kind, local_leaf)] = local.artifacts() else {
        panic!("Join fixture publishes one physical membership leaf")
    };
    let profile = membership_profile();
    assert_eq!(local.profile_id(), profile.id());

    let schema =
        ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
    let encoded = encode_physical_leaf(
        *kind,
        &schema,
        local_leaf.version(),
        local_leaf.contains_null(),
        None,
        membership_payload(local_leaf),
    )
    .unwrap();
    assert_eq!(encoded, local_leaf.canonical_bytes());

    let retained_bytes = PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap();
    let decoded_memory: Arc<dyn RuntimeFilterMemoryAccount> =
        MemTrackerMemoryAccount::new_root_for_test("m4-fixture-remote-decode");
    let remote_leaf = decode_leaf(
        &encoded,
        ArtifactDecodeExpectations {
            expected_kind: *kind,
            expected_schema_digest: local_leaf.schema_digest(),
            expected_logical_version: local.version(),
            expected_hash_contract: None,
        },
        encoded.len(),
        Arc::new(ArtifactRetainedBudget::new(retained_bytes)),
        decoded_memory,
    )
    .unwrap();
    let remote = ArtifactBundle::new(
        local.channel_id(),
        local.version(),
        &profile,
        vec![(*kind, remote_leaf)],
        local.encoded_bytes(),
    )
    .unwrap();

    assert_eq!(local.artifacts()[0].0, remote.artifacts()[0].0);
    assert_eq!(local.profile_id(), remote.profile_id());
    assert_eq!(local.version(), remote.version());
    assert_eq!(local.canonical_digest(), remote.canonical_digest());
}

fn order_plan(
    direction: SortDirection,
    null_order: NullOrder,
) -> (OrderContract, Arc<RuntimeOrderContract>) {
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction,
        null_order,
    }];
    let plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
    (plan, contract)
}

fn plan_from_runtime_contract(contract: &RuntimeOrderContract) -> OrderContract {
    OrderContract {
        keys: contract
            .keys()
            .iter()
            .map(|key| OrderKeyContract {
                data_type: key.data_type().clone(),
                direction: key.direction(),
                null_order: key.null_order(),
            })
            .collect(),
        inclusive: true,
        comparator_digest: contract.plan_comparator_digest(),
    }
}

fn ordered_binding(
    producer: &ProducerFixture,
    contributions: &BTreeSet<ContributionKind>,
    node_id: i32,
) -> RuntimeFilterBindingSpec {
    RuntimeFilterBindingSpec {
        binding_id: producer.binding,
        channel_id: CHANNEL,
        coverage_witness_id: Some(producer.witness),
        location: PlanLocation {
            fragment_id: producer.fragment,
            node_id: PlanNodeId::new(node_id),
        },
        expression: expression(),
        apply_point: ApplyPoint::NodeOutput,
        role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
            contribution_kinds: contributions.clone(),
            completion_requirement: CompletionRequirement::ProducerClosed,
        }),
    }
}

fn ordered_consumer_binding() -> RuntimeFilterBindingSpec {
    RuntimeFilterBindingSpec {
        binding_id: CONSUMER,
        channel_id: CHANNEL,
        coverage_witness_id: None,
        location: PlanLocation {
            fragment_id: CONSUMER_FRAGMENT,
            node_id: PlanNodeId::new(30),
        },
        expression: expression(),
        apply_point: ApplyPoint::NodeInput,
        role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
            capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            activation: ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
        }),
    }
}

fn ordered_graph(
    contract: &RuntimeOrderContract,
    producer: &ProducerFixture,
) -> RuntimeFilterGraph {
    let contributions = BTreeSet::from([
        ContributionKind::OrderedBoundUpdate,
        ContributionKind::ProducerClosed,
    ]);
    let coverage = Coverage::Leaf(producer.witness);
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: CHANNEL,
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(plan_from_runtime_contract(
                contract,
            )),
            lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::TightenOrderedBound,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
        })
        .unwrap();
    graph
        .insert_binding(ordered_binding(producer, &contributions, 1))
        .unwrap();
    graph.insert_binding(ordered_consumer_binding()).unwrap();
    graph
        .validate()
        .expect("direct TopN graph must pass RFD-1 validation before compilation");
    graph
}

fn topk_graph(
    plan: OrderContract,
    requirement: TopKSummaryRequirement,
    producers: &[ProducerFixture],
) -> RuntimeFilterGraph {
    let contributions = BTreeSet::from([
        ContributionKind::TopKSummary,
        ContributionKind::ProducerClosed,
    ]);
    let coverage = Coverage::AllOf(
        producers
            .iter()
            .map(|producer| Coverage::Leaf(producer.witness))
            .collect(),
    );
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: CHANNEL,
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(plan),
            lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::MergeTopKSummary(requirement),
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
        })
        .unwrap();
    for (index, producer) in producers.iter().enumerate() {
        graph
            .insert_binding(ordered_binding(producer, &contributions, index as i32 + 1))
            .unwrap();
    }
    graph.insert_binding(ordered_consumer_binding()).unwrap();
    graph
        .validate()
        .expect("TopKSummary graph must pass RFD-1 validation before compilation");
    graph
}

struct DirectTopNHarness {
    _service: Arc<RuntimeFilterService>,
    producer: Arc<dyn OrderedBoundProducerAdapter>,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

fn direct_topn_harness(contract: Arc<RuntimeOrderContract>) -> DirectTopNHarness {
    let producers = producer_fixtures();
    let direct = &producers[0];
    let graph = ordered_graph(&contract, direct);
    let scheduling = scheduling_plan(std::slice::from_ref(direct));
    let edges = fragment_edges(std::slice::from_ref(direct));
    let service = install_service(compile_install_view(
        &graph,
        &scheduling,
        &edges,
        PARTICIPANT,
    ));
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(
            direct.binding,
            direct.instance,
            1,
            ProducerPortKind::OrderedBound,
        )
        .expect("compiler-installed direct TopN producer is authorized")
    else {
        panic!("direct TopN graph must install only the OrderedBound producer port")
    };
    let SubscriptionHandle::Live(live) = service
        .subscribe(
            CONSUMER,
            CONSUMER_INSTANCE,
            SubscriptionKind::NonBlockingLive,
        )
        .expect("compiler-installed ordered consumer is authorized")
    else {
        panic!("ordered graph consumer must install only NonBlockingLive")
    };
    DirectTopNHarness {
        _service: service,
        producer,
        live,
    }
}

struct TopNHeapAdapter {
    k: usize,
    contract: Arc<RuntimeOrderContract>,
    producer: Arc<dyn OrderedBoundProducerAdapter>,
    candidates: Vec<OrderedTuple>,
    next_sequence: u64,
    published: Vec<LogicalVersion>,
}

impl TopNHeapAdapter {
    fn new(
        k: usize,
        contract: Arc<RuntimeOrderContract>,
        producer: Arc<dyn OrderedBoundProducerAdapter>,
    ) -> Self {
        assert!(k > 0, "TopN adapter requires a positive limit");
        Self {
            k,
            contract,
            producer,
            candidates: Vec::new(),
            next_sequence: 0,
            published: Vec::new(),
        }
    }

    fn push(
        &mut self,
        row: OrderedTuple,
    ) -> Result<Option<LogicalVersion>, RuntimeContractViolation> {
        let previous_kth = self.candidates.get(self.k - 1).cloned();
        self.candidates.push(row);
        self.candidates.sort_by(|left, right| {
            self.contract
                .compare(left, right)
                .expect("TopN candidates match their runtime order contract")
        });
        self.candidates.truncate(self.k);
        let Some(current_kth) = self.candidates.get(self.k - 1).cloned() else {
            return Ok(None);
        };
        if previous_kth.as_ref().is_some_and(|previous| {
            self.contract
                .compare(&current_kth, previous)
                .expect("TopN candidates match their runtime order contract")
                != Ordering::Less
        }) {
            return Ok(None);
        }

        let outcome = self.producer.submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(self.next_sequence),
            OrderedBoundUpdate::new(&self.contract, current_kth)
                .expect("TopN kth tuple matches its runtime order contract"),
        )?;
        assert_eq!(
            outcome,
            SubmitOutcome::Published,
            "a genuinely tighter direct TopN bound must publish"
        );
        self.next_sequence += 1;
        let version = self
            .published
            .last()
            .copied()
            .and_then(LogicalVersion::checked_next)
            .unwrap_or(LogicalVersion::FIRST);
        self.published.push(version);
        Ok(Some(version))
    }

    fn published_versions(&self) -> &[LogicalVersion] {
        &self.published
    }
}

struct TopNCase {
    contract: Arc<RuntimeOrderContract>,
    rows: Vec<OrderedTuple>,
    final_topn: Vec<OrderedTuple>,
    expected_publications: Vec<bool>,
}

fn tuple(contract: &RuntimeOrderContract, value: Option<i64>) -> OrderedTuple {
    OrderedTuple::try_new(contract, [value.map(OrderedScalar::Int64)])
        .expect("finite TopN sample matches the Int64 order contract")
}

fn oracle_topn(
    contract: &RuntimeOrderContract,
    rows: &[OrderedTuple],
    k: usize,
) -> Vec<OrderedTuple> {
    let mut ranked = rows.to_vec();
    ranked.sort_by(|left, right| {
        contract
            .compare(left, right)
            .expect("oracle rows match their runtime order contract")
    });
    ranked.truncate(k);
    ranked
}

fn oracle_publication_schedule(
    contract: &RuntimeOrderContract,
    rows: &[OrderedTuple],
    k: usize,
) -> Vec<bool> {
    let mut prefix = Vec::with_capacity(rows.len());
    let mut previous_bound: Option<OrderedTuple> = None;
    rows.iter()
        .map(|row| {
            prefix.push(row.clone());
            let ranked = oracle_topn(contract, &prefix, k);
            let Some(bound) = ranked.get(k - 1).cloned() else {
                return false;
            };
            let publish = previous_bound.as_ref().is_none_or(|previous| {
                contract.compare(&bound, previous).unwrap() == Ordering::Less
            });
            previous_bound = Some(bound);
            publish
        })
        .collect()
}

fn topn_case(
    direction: SortDirection,
    null_order: NullOrder,
    values: impl IntoIterator<Item = Option<i64>>,
) -> TopNCase {
    let (_, contract) = order_plan(direction, null_order);
    let rows = values
        .into_iter()
        .map(|value| tuple(&contract, value))
        .collect::<Vec<_>>();
    let final_topn = oracle_topn(&contract, &rows, 3);
    let expected_publications = oracle_publication_schedule(&contract, &rows, 3);
    TopNCase {
        contract,
        rows,
        final_topn,
        expected_publications,
    }
}

fn topn_cases_with_fixed_seed() -> Vec<TopNCase> {
    let mut cases = vec![
        topn_case(
            SortDirection::Ascending,
            NullOrder::Last,
            [
                Some(30),
                Some(20),
                Some(10),
                Some(100),
                Some(30),
                Some(5),
                Some(20),
                Some(1),
                None,
            ],
        ),
        topn_case(
            SortDirection::Descending,
            NullOrder::First,
            [
                Some(10),
                Some(20),
                Some(30),
                Some(-100),
                Some(10),
                Some(40),
                Some(20),
                Some(50),
                None,
            ],
        ),
    ];
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    for index in 0..62 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let base = ((state >> 32) % 20_000) as i64 - 10_000;
        if index % 2 == 0 {
            cases.push(topn_case(
                SortDirection::Ascending,
                NullOrder::Last,
                [
                    Some(base + 30),
                    Some(base + 20),
                    Some(base + 10),
                    Some(base + 100),
                    Some(base + 30),
                    Some(base + 5),
                    Some(base + 20),
                    Some(base + 1),
                ],
            ));
        } else {
            cases.push(topn_case(
                SortDirection::Descending,
                NullOrder::First,
                [
                    Some(base + 10),
                    Some(base + 20),
                    Some(base + 30),
                    Some(base - 100),
                    Some(base + 10),
                    Some(base + 40),
                    Some(base + 20),
                    Some(base + 50),
                ],
            ));
        }
    }
    assert_eq!(cases.len(), 64);
    cases
}

fn assert_bound_is_sound_for_final_topn(
    live: &Arc<dyn NonBlockingLiveSubscription>,
    version: LogicalVersion,
    final_topn: &[OrderedTuple],
) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: None,
    } = live.poll_after(None)
    else {
        panic!("direct TopN tightening must be visible as a non-terminal live update")
    };
    assert_eq!(bundle.version(), version);
    let [(ArtifactKind::Range, artifact)] = bundle.artifacts() else {
        panic!("ordered TopN must materialize exactly one Range artifact")
    };
    let range = artifact.range().expect("Range leaf owns ordered data");
    for row in final_topn {
        assert_ne!(
            range.contract().compare(row, range.bound()).unwrap(),
            Ordering::Greater,
            "visible TopN bound must not eliminate a row in the final TopN"
        );
    }
    bundle
}

fn assert_immutable_version_history(
    observed: &[(Arc<ArtifactBundle>, LogicalVersion, [u8; 32])],
    expected_versions: &[LogicalVersion],
) {
    assert_eq!(observed.len(), expected_versions.len());
    assert!(observed.len() >= 2);
    for (record, expected) in observed.iter().zip(expected_versions) {
        assert_eq!(record.0.version(), record.1);
        assert_eq!(record.0.version(), *expected);
        assert_eq!(record.0.canonical_digest(), record.2);
    }
    for pair in observed.windows(2) {
        assert!(pair[0].1 < pair[1].1);
        assert!(!Arc::ptr_eq(&pair[0].0, &pair[1].0));
    }
}

struct TopKSummaryHarness {
    service: Arc<RuntimeFilterService>,
    contract: Arc<RuntimeTopKSummaryContract>,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

impl TopKSummaryHarness {
    fn producer(
        &self,
        binding: BindingId,
        instance: UniqueId,
    ) -> Arc<dyn TopKSummaryProducerAdapter> {
        let ProducerHandle::TopKSummary(producer) = self
            .service
            .open_producer(binding, instance, 1, ProducerPortKind::TopKSummary)
            .expect("compiler-installed TopKSummary producer is authorized")
        else {
            panic!("TopKSummary graph must install only the TopKSummary producer port")
        };
        producer
    }

    fn submit_summary(
        &self,
        binding: BindingId,
        instance: UniqueId,
        values: &[i64],
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let mut candidates = values
            .iter()
            .map(|value| tuple(self.contract.order(), Some(*value)))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| self.contract.order().compare(left, right).unwrap());
        self.producer(binding, instance).submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            TopKSummary::try_new(&self.contract, candidates)
                .expect("TopKSummary fixture constructs canonical candidates"),
        )
    }

    fn close_all(&self) -> Result<(), RuntimeContractViolation> {
        let visible_version = self
            .live
            .snapshot()
            .expect("TopKSummary bound is visible before close")
            .version();
        let first = self
            .producer(PRODUCER_A, INSTANCE_A)
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))?;
        assert_ne!(first, SubmitOutcome::Completed);
        assert!(matches!(
            self.live.poll_after(Some(visible_version)),
            LivePollOutcome::Idle {
                latest_version: Some(latest),
                terminal: None,
            } if latest == visible_version
        ));
        let second = self
            .producer(PRODUCER_B, INSTANCE_B)
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))?;
        assert_eq!(second, SubmitOutcome::Completed);
        Ok(())
    }
}

fn topk_allof_harness(k: u32) -> TopKSummaryHarness {
    let producers = producer_fixtures();
    let (plan, _) = order_plan(SortDirection::Ascending, NullOrder::Last);
    let requirement = TopKSummaryRequirement::try_new(k).expect("TopK requires positive K");
    let contract = Arc::new(
        RuntimeTopKSummaryContract::try_from_plan(&plan, requirement)
            .expect("TopKSummary plan contract is valid"),
    );
    let graph = topk_graph(plan, requirement, &producers);
    let scheduling = scheduling_plan(&producers);
    let edges = fragment_edges(&producers);
    let service = install_service(compile_install_view(
        &graph,
        &scheduling,
        &edges,
        PARTICIPANT,
    ));
    let SubscriptionHandle::Live(live) = service
        .subscribe(
            CONSUMER,
            CONSUMER_INSTANCE,
            SubscriptionKind::NonBlockingLive,
        )
        .expect("compiler-installed TopKSummary consumer is authorized")
    else {
        panic!("TopKSummary consumer must install only NonBlockingLive")
    };
    TopKSummaryHarness {
        service,
        contract,
        live,
    }
}

fn expect_live_update(outcome: LivePollOutcome) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: None,
    } = outcome
    else {
        panic!("expected a non-terminal live range update")
    };
    bundle
}

fn assert_sound_topk_bound(bundle: &ArtifactBundle, final_topk: &[i64]) {
    let [(ArtifactKind::Range, artifact)] = bundle.artifacts() else {
        panic!("TopKSummary must materialize exactly one Range artifact")
    };
    let range = artifact.range().expect("Range leaf owns ordered data");
    let [Some(OrderedScalar::Int64(actual_bound))] = range.bound().values() else {
        panic!("TopKSummary fixture expects one non-null Int64 bound")
    };
    assert_eq!(Some(actual_bound), final_topk.last());
    for value in final_topk {
        let row = tuple(range.contract(), Some(*value));
        assert_ne!(
            range.contract().compare(&row, range.bound()).unwrap(),
            Ordering::Greater,
            "merged TopK bound must not eliminate a final TopK row"
        );
    }
}

fn assert_completed_without_new_unsound_version(
    live: &Arc<dyn NonBlockingLiveSubscription>,
    version: LogicalVersion,
) {
    assert!(matches!(
        live.poll_after(Some(version)),
        LivePollOutcome::Idle {
            latest_version: Some(latest),
            terminal: Some(LiveTerminal::Completed),
        } if latest == version
    ));
    assert_eq!(live.snapshot().unwrap().version(), version);
}

#[test]
fn m4_join_conformance_uses_graph_compiler_public_ports_and_route_equivalent_artifacts() {
    let all_of = join_allof_harness();
    let first = all_of.producer(PRODUCER_A, INSTANCE_A);
    first.submit_values(0, 0, [1]).unwrap();
    first.close(0, 1).unwrap();
    assert!(all_of.blocking.snapshot().is_none());
    let second = all_of.producer(PRODUCER_B, INSTANCE_B);
    second.submit_values(0, 0, [2]).unwrap();
    second.close(0, 1).unwrap();
    let local = all_of
        .blocking
        .snapshot()
        .expect("AllOf publishes after both witnesses");
    assert_eq!(local.version(), LogicalVersion::FIRST);
    assert_membership_values(&local, &[1, 2]);
    assert_fixture_remote_equivalent(&local);

    let any_of = join_anyof_harness();
    publish_membership(&any_of, PRODUCER_A, INSTANCE_A, &[7]);
    let first = any_of
        .blocking
        .snapshot()
        .expect("first valid replica publishes");
    publish_membership(&any_of, PRODUCER_B, INSTANCE_B, &[9]);
    let after_late = any_of.blocking.snapshot().expect("winner remains visible");
    assert_eq!(first.version(), after_late.version());
    assert_eq!(first.canonical_digest(), after_late.canonical_digest());
}

#[test]
fn m4_direct_topn_conformance_delays_until_n_and_preserves_sound_monotonic_bounds() {
    for case in topn_cases_with_fixed_seed() {
        let harness = direct_topn_harness(case.contract.clone());
        let mut adapter = TopNHeapAdapter::new(3, case.contract.clone(), harness.producer.clone());
        assert!(!case.expected_publications[0]);
        assert!(!case.expected_publications[1]);
        assert!(adapter.push(case.rows[0].clone()).unwrap().is_none());
        assert!(adapter.push(case.rows[1].clone()).unwrap().is_none());
        assert!(matches!(
            harness.live.poll_after(None),
            LivePollOutcome::Idle { .. }
        ));
        let mut observed = Vec::new();
        for (index, row) in case.rows.into_iter().enumerate().skip(2) {
            let published = adapter.push(row).unwrap();
            assert_eq!(published.is_some(), case.expected_publications[index]);
            if let Some(version) = published {
                let bundle =
                    assert_bound_is_sound_for_final_topn(&harness.live, version, &case.final_topn);
                observed.push((bundle.clone(), bundle.version(), bundle.canonical_digest()));
            }
        }
        assert!(adapter.published_versions().len() >= 2);
        assert_immutable_version_history(&observed, adapter.published_versions());
    }
}

#[test]
fn m4_topk_summary_conformance_merges_incomplete_shards_only_after_allof() {
    let harness = topk_allof_harness(3);
    harness
        .submit_summary(PRODUCER_A, INSTANCE_A, &[1, 4])
        .unwrap();
    assert!(matches!(
        harness.live.poll_after(None),
        LivePollOutcome::Idle { .. }
    ));
    harness
        .submit_summary(PRODUCER_B, INSTANCE_B, &[2, 3])
        .unwrap();
    let first = expect_live_update(harness.live.poll_after(None));
    assert_sound_topk_bound(&first, &[1, 2, 3]);
    harness.close_all().unwrap();
    assert_completed_without_new_unsound_version(&harness.live, first.version());
}
