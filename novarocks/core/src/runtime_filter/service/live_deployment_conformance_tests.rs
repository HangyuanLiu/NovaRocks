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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::coordinator::cluster::LiveBackendSnapshot;
use crate::coordinator::dispatch::{FetchOutcome, FragmentDispatcher, NativeFragmentEnvelope};
use crate::coordinator::ports::RuntimeFilterDeploymentControlPort;
use crate::coordinator::runtime_filter_deployment::{
    DeploymentEpochAllocator, NativeRuntimeFilterDeploymentPolicyProvider,
    RuntimeFilterInstallBarrier, prepare_runtime_filter_deployment,
};
use crate::coordinator::scheduler::{FragmentInstancePlacement, SchedulingPlan};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::QueryId;
use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
use crate::runtime_filter::deployment::compiler;
use crate::runtime_filter::deployment::extension::RuntimeFilterDeploymentExtension;
use crate::runtime_filter::deployment::participant_id_for_backend;
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, NullSemantics, PlanFragmentId, PlanNodeId,
    ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
    RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::graph::{
    ApplyPoint, ConsumerBindingTarget, ConsumerRequirement, PlanLocation, ProducerRequirement,
    RuntimeFilterBindingRole, RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
    RuntimeFilterGraph,
};
use crate::runtime_filter::port::events::{RuntimeFilterEvent, TransportEventKind};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, PartitionId, ProducerSequence, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::RuntimeFilterParticipantInstall;
use crate::runtime_filter::port::producer::{
    ProducerPortKind, RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::port::subscription::{ArtifactAcquireOutcome, SubscriptionKind};
use crate::runtime_filter::port::transport::RuntimeFilterAcceptStatus;
use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};
use crate::service::grpc_fragment_dispatcher::GrpcRuntimeFilterDeploymentControl;
use crate::service::grpc_server::IndependentGrpcRuntimeFilterNode;
use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder;
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentStreamKind, PlanFragment,
};
use crate::sql::planner::payload::PlanValuesNode;
use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

use super::RuntimeFilterService;

const QUERY: UniqueId = UniqueId {
    hi: 0x6a,
    lo: 0x800,
};
const CHANNEL: ChannelId = ChannelId::new(80);
const PRODUCER_BINDING: BindingId = BindingId::new(81);
const CONSUMER_BINDING: BindingId = BindingId::new(82);
const WITNESS: CoverageWitnessId = CoverageWitnessId::new(83);
const PRODUCER_FRAGMENT: u32 = 1;
const PRODUCER_NODE: i32 = 810;
const CONSUMER_FRAGMENT: u32 = 0;
const CONSUMER_NODE: i32 = 820;
const MAX_WAIT: Duration = Duration::from_secs(5);
static LIVE_CONFORMANCE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug)]
enum ConformanceTopology {
    AnyOfDirect,
    AllOfAggregate,
}

impl ConformanceTopology {
    fn coverage(self) -> Coverage {
        match self {
            Self::AnyOfDirect => Coverage::AnyOf(vec![Coverage::Leaf(WITNESS)]),
            Self::AllOfAggregate => Coverage::AllOf(vec![Coverage::Leaf(WITNESS)]),
        }
    }
}

#[derive(Default)]
struct RecordingFragmentDispatcher {
    submit_count: AtomicUsize,
    submissions: Mutex<Vec<(usize, u32, UniqueId)>>,
}

impl RecordingFragmentDispatcher {
    fn submit_count(&self) -> usize {
        self.submit_count.load(Ordering::SeqCst)
    }

    fn submissions(&self) -> Vec<(usize, u32, UniqueId)> {
        self.submissions.lock().unwrap().clone()
    }
}

impl FragmentDispatcher for RecordingFragmentDispatcher {
    fn submit_fragment(
        &self,
        backend_idx: usize,
        submission: NativeFragmentEnvelope,
    ) -> Result<(), String> {
        let finst_id = submission.fragment_instance_id()?;
        self.submissions
            .lock()
            .unwrap()
            .push((backend_idx, submission.fragment_id(), finst_id));
        self.submit_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn fetch_result(
        &self,
        _backend_idx: usize,
        _finst_id: UniqueId,
        _max_wait_ms: i64,
        _expected_chunk_schema: Option<&crate::exec::chunk::ChunkSchemaRef>,
    ) -> Result<FetchOutcome, String> {
        Ok(FetchOutcome::Eof)
    }

    fn cancel_fragments(&self, _backend_idx: usize, _finst_ids: &[UniqueId]) {}

    fn backend_count(&self) -> usize {
        1
    }
}

struct AckGatedDeploymentControl {
    inner: Arc<GrpcRuntimeFilterDeploymentControl>,
    installed: mpsc::SyncSender<RuntimeFilterParticipantId>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl RuntimeFilterDeploymentControlPort for AckGatedDeploymentControl {
    async fn install(
        &self,
        query_id: UniqueId,
        lifecycle: crate::protocol::native::RuntimeFilterQueryLifecycleOptions,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<(), String> {
        self.inner
            .install(query_id, lifecycle, deadline, participant, install)
            .await?;
        self.installed
            .send(participant)
            .map_err(|_| "live install ACK observation receiver closed".to_string())?;
        tokio::time::timeout(MAX_WAIT, self.release.acquire())
            .await
            .map_err(|_| "live install ACK gate timed out".to_string())?
            .map_err(|_| "live install ACK gate closed".to_string())?
            .forget();
        Ok(())
    }

    async fn abort(
        &self,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
    ) -> Result<(), String> {
        self.inner
            .abort(query_id, epoch, deadline, participant)
            .await
    }
}

fn stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 0.0,
        row_count_confidence: PlannerConfidence::Fallback,
        column_statistics: Default::default(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn output_column() -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(1),
        name: "k".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    }
}

fn expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        data_type: DataType::Int64,
        nullable: false,
    }
}

fn runtime_filter_graph(topology: ConformanceTopology) -> RuntimeFilterGraph {
    let coverage = topology.coverage();
    let contributions = BTreeSet::from([
        ContributionKind::ValueDomainDelta,
        ContributionKind::ProducerClosed,
    ]);
    let capabilities = BTreeSet::from([
        ArtifactCapability::Membership,
        ArtifactCapability::EmptyDomain,
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
                max_contribution_bytes: 1024,
                max_artifact_bytes: 4096,
                deadline_ms: 4_000,
                max_retries: 2,
            },
        })
        .expect("insert conformance channel");
    graph
        .insert_binding(RuntimeFilterBindingSpec {
            binding_id: PRODUCER_BINDING,
            channel_id: CHANNEL,
            coverage_witness_id: Some(WITNESS),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(PRODUCER_FRAGMENT),
                node_id: PlanNodeId::new(PRODUCER_NODE),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                contribution_kinds: contributions,
                completion_requirement: CompletionRequirement::ProducerClosed,
                join_key_ordinal: 0,
            }),
        })
        .expect("insert conformance producer binding");
    graph
        .insert_binding(RuntimeFilterBindingSpec {
            binding_id: CONSUMER_BINDING,
            channel_id: CHANNEL,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(CONSUMER_FRAGMENT),
                node_id: PlanNodeId::new(CONSUMER_NODE),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities,
                activation: ConsumerActivation::BlockingSnapshot,
                target: ConsumerBindingTarget::SourceBoundary,
            }),
        })
        .expect("insert conformance consumer binding");
    graph
}

fn sealed_plan(topology: ConformanceTopology) -> crate::sql::planner::distributed::DistributedPlan {
    let column = output_column();
    let producer = PlanFragment {
        fragment_id: PRODUCER_FRAGMENT,
        root: DistributedNode {
            node_id: PRODUCER_NODE,
            fragment_id: PRODUCER_FRAGMENT,
            tuple_ids: vec![PRODUCER_NODE],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: vec![PRODUCER_BINDING],
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![column.clone()],
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: vec![column.clone()],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let consumer = PlanFragment {
        fragment_id: CONSUMER_FRAGMENT,
        root: DistributedNode {
            node_id: CONSUMER_NODE,
            fragment_id: CONSUMER_FRAGMENT,
            tuple_ids: vec![CONSUMER_NODE],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: vec![CONSUMER_BINDING],
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: PRODUCER_FRAGMENT,
                output_columns: vec![column.clone()],
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: vec![column],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    DistributedPlanDraftBuilder::new(
        vec![producer, consumer],
        Some(CONSUMER_FRAGMENT),
        vec![FragmentEdge {
            source_fragment_id: PRODUCER_FRAGMENT,
            target_fragment_id: CONSUMER_FRAGMENT,
            target_exchange_node_id: CONSUMER_NODE,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![1],
        }],
        runtime_filter_graph(topology),
    )
    .seal()
    .expect("conformance fixture must pass the production distributed-plan seal")
}

fn placement(
    fragment_id: u32,
    instance_index: usize,
    backend_idx: usize,
    finst_id: UniqueId,
    endpoint: std::net::SocketAddr,
) -> FragmentInstancePlacement {
    FragmentInstancePlacement {
        fragment_id,
        instance_index,
        finst_id,
        backend_idx,
        endpoint: RuntimeEndpoint::from_socket_addr(endpoint),
        scan_ranges: BTreeMap::new(),
        destinations: Vec::new(),
        per_exch_num_senders: BTreeMap::new(),
    }
}

fn scheduling(endpoints: &[std::net::SocketAddr; 3]) -> SchedulingPlan {
    SchedulingPlan {
        root_fragment_id: CONSUMER_FRAGMENT,
        by_fragment: BTreeMap::from([
            (
                PRODUCER_FRAGMENT,
                vec![
                    placement(
                        PRODUCER_FRAGMENT,
                        0,
                        0,
                        UniqueId {
                            hi: QUERY.hi,
                            lo: 0x101,
                        },
                        endpoints[0],
                    ),
                    placement(
                        PRODUCER_FRAGMENT,
                        1,
                        1,
                        UniqueId {
                            hi: QUERY.hi,
                            lo: 0x102,
                        },
                        endpoints[1],
                    ),
                ],
            ),
            (
                CONSUMER_FRAGMENT,
                vec![placement(
                    CONSUMER_FRAGMENT,
                    0,
                    2,
                    UniqueId {
                        hi: QUERY.hi,
                        lo: 0x201,
                    },
                    endpoints[2],
                )],
            ),
        ]),
        root_finst_id: UniqueId {
            hi: QUERY.hi,
            lo: 0x201,
        },
        root_backend_idx: 2,
    }
}

fn native_submissions(
    plan: &crate::sql::planner::distributed::DistributedPlan,
    scheduling: &SchedulingPlan,
) -> Vec<(usize, NativeFragmentEnvelope)> {
    let prepared = crate::coordinator::prepare::prepare_fragments(
        plan,
        &crate::connector::ConnectorRegistry::new(),
        None,
    )
    .expect("prepare the sealed live conformance plan");
    let native = crate::protocol::native::encode::encode_native_fragment_bundle(plan, &prepared)
        .expect("encode the sealed live conformance plan");
    scheduling
        .by_fragment
        .iter()
        .flat_map(|(fragment_id, placements)| {
            let template = native
                .get(*fragment_id)
                .unwrap_or_else(|| panic!("native fragment {fragment_id} is present"))
                .clone();
            placements.iter().map(move |placement| {
                (
                    placement.backend_idx,
                    NativeFragmentEnvelope::new(
                        template.clone(),
                        crate::proto::novarocks::InstanceParams {
                            query_id: Some(crate::proto::common::UniqueId {
                                hi: QUERY.hi,
                                lo: QUERY.lo,
                            }),
                            fragment_instance_id: Some(crate::proto::common::UniqueId {
                                hi: placement.finst_id.hi,
                                lo: placement.finst_id.lo,
                            }),
                            ..Default::default()
                        },
                    ),
                )
            })
        })
        .collect()
}

fn wait_for_transport_ack(
    sender: &RuntimeFilterService,
    sender_participant: RuntimeFilterParticipantId,
    route_edge_ids: &BTreeSet<crate::runtime_filter::port::identity::RouteEdgeId>,
) {
    let deadline = std::time::Instant::now() + MAX_WAIT;
    let query = QueryKey::from_hi_lo(QUERY.hi, QUERY.lo);
    loop {
        let accepted = RuntimeFilterLifecycleRegistry::global()
            .snapshot(query)
            .is_some_and(|snapshot| {
                snapshot.channel_events.values().flatten().any(|event| {
                    matches!(
                        event,
                        RuntimeFilterEvent::TransportEnvelope {
                            identity,
                            kind: TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted),
                            ..
                        } if identity.common().participant_id() == sender_participant
                            && identity.common().channel_id() == CHANNEL
                            && route_edge_ids.contains(&identity.route_edge_id())
                    )
                })
            });
        if sender.transport_pending_len_for_test() == 0 && accepted {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sender transport did not reach pending=0 plus Acked(Accepted) within {MAX_WAIT:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn assert_zero_fragment_submits(dispatchers: &[Arc<RecordingFragmentDispatcher>]) {
    for (backend_idx, dispatcher) in dispatchers.iter().enumerate() {
        assert_eq!(
            dispatcher.submit_count(),
            0,
            "backend {backend_idx} submitted a fragment before the install barrier ACKed"
        );
    }
}

fn run_live_conformance(topology: ConformanceTopology) {
    let _serial = LIVE_CONFORMANCE_LOCK.lock().unwrap();
    let lifecycle_query = QueryKey::from_hi_lo(QUERY.hi, QUERY.lo);
    RuntimeFilterLifecycleRegistry::global().remove_query(lifecycle_query);
    let mut nodes = [
        IndependentGrpcRuntimeFilterNode::start().expect("start independent BE zero"),
        IndependentGrpcRuntimeFilterNode::start().expect("start independent BE one"),
        IndependentGrpcRuntimeFilterNode::start().expect("start independent BE two"),
    ];
    let endpoints = [
        nodes[0].endpoint(),
        nodes[1].endpoint(),
        nodes[2].endpoint(),
    ];
    let backends = LiveBackendSnapshot::new(endpoints.into_iter().enumerate().collect());
    let sealed = sealed_plan(topology);
    let scheduling = scheduling(&endpoints);
    let deployment = prepare_runtime_filter_deployment(
        sealed.runtime_filter_graph(),
        &backends,
        &NativeRuntimeFilterDeploymentPolicyProvider::new(2),
        &DeploymentEpochAllocator,
    )
    .expect("derive live deployment policy")
    .expect("the conformance graph is nonempty");
    let compiled = compiler::compile(
        sealed.runtime_filter_graph(),
        &scheduling,
        sealed.edges(),
        &backends,
        &deployment.policy.compiler,
        deployment.epoch,
    )
    .expect("compile live three-BE runtime filter deployment");
    let installs = RuntimeFilterDeploymentExtension::new()
        .participant_installs(&compiled)
        .expect("project per-participant installs");
    assert_eq!(installs.len(), 3, "every BE has an authorized role");

    let dispatchers = Arc::new(
        (0..3)
            .map(|_| Arc::new(RecordingFragmentDispatcher::default()))
            .collect::<Vec<_>>(),
    );
    let install_ack_observations = Arc::new(AtomicUsize::new(0));
    for node in &nodes {
        let dispatchers = Arc::clone(&dispatchers);
        let observations = Arc::clone(&install_ack_observations);
        node.manager()
            .set_before_runtime_filter_installed_publish_hook_for_test(Arc::new(move || {
                assert_zero_fragment_submits(dispatchers.as_slice());
                observations.fetch_add(1, Ordering::SeqCst);
            }));
    }

    let grpc_control = Arc::new(
        GrpcRuntimeFilterDeploymentControl::new(backends.entries())
            .expect("construct production gRPC deployment control"),
    );
    let (installed_tx, installed_rx) = mpsc::sync_channel(3);
    let ack_release = Arc::new(tokio::sync::Semaphore::new(0));
    let control = Arc::new(AckGatedDeploymentControl {
        inner: grpc_control,
        installed: installed_tx,
        release: Arc::clone(&ack_release),
    });
    let submissions = native_submissions(&sealed, &scheduling);
    let dispatchers_for_submission = Arc::clone(&dispatchers);
    let installs_for_barrier = installs.clone();
    let lifecycle = crate::protocol::native::RuntimeFilterQueryLifecycleOptions {
        delivery_expire: MAX_WAIT,
        query_expire: Duration::from_secs(30),
        transport_retry_interval: deployment.policy.transport.retry_interval,
        transport_max_attempts: deployment.policy.transport.max_attempts,
        transport_deadline: deployment.policy.transport.deadline,
        transport_max_pending_entries: deployment.policy.transport.max_pending_entries,
        transport_max_pending_bytes: deployment.policy.transport.max_pending_bytes,
    };
    let (barrier_done_tx, barrier_done_rx) = mpsc::sync_channel(1);
    let barrier_thread = std::thread::spawn(move || {
        let result = RuntimeFilterInstallBarrier::new(control)
            .install_all_or_rollback(
                QUERY,
                deployment.epoch,
                lifecycle,
                deployment.policy.install_rpc_deadline,
                installs_for_barrier,
            )
            .and_then(|installed| {
                installed.release();
                for (backend_idx, submission) in submissions {
                    dispatchers_for_submission[backend_idx]
                        .submit_fragment(backend_idx, submission)?;
                }
                Ok(())
            });
        let _ = barrier_done_tx.send(result);
    });
    let mut installed_participants = BTreeSet::new();
    for _ in 0..3 {
        installed_participants.insert(
            installed_rx
                .recv_timeout(MAX_WAIT)
                .expect("real install RPC completes before the ACK gate"),
        );
    }
    assert_eq!(installed_participants.len(), 3);
    assert_zero_fragment_submits(dispatchers.as_slice());
    assert_eq!(
        install_ack_observations.load(Ordering::SeqCst),
        3,
        "every install handler observed the zero-submit pre-ACK invariant"
    );
    ack_release.add_permits(3);
    barrier_done_rx
        .recv_timeout(MAX_WAIT)
        .expect("install barrier and post-release submission finish within the bound")
        .expect("all three real install RPCs ACK and scheduled fragments submit");
    barrier_thread.join().expect("install barrier thread joins");

    let mut actual_submissions = dispatchers
        .iter()
        .flat_map(|dispatcher| dispatcher.submissions())
        .collect::<Vec<_>>();
    actual_submissions.sort_unstable();
    let mut expected_submissions = scheduling
        .by_fragment
        .iter()
        .flat_map(|(fragment_id, placements)| {
            placements
                .iter()
                .map(|placement| (placement.backend_idx, *fragment_id, placement.finst_id))
        })
        .collect::<Vec<_>>();
    expected_submissions.sort_unstable();
    assert_eq!(
        actual_submissions, expected_submissions,
        "the released install lease dispatches every real scheduled fragment exactly once"
    );

    let query = QueryId {
        hi: QUERY.hi,
        lo: QUERY.lo,
    };
    let expected_installs = installs.into_iter().collect::<BTreeMap<_, _>>();
    let mut services = Vec::new();
    for (backend_idx, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.manager().fragment_counts_for_test(query),
            Some((0, 0)),
            "install must not register fragments on backend {backend_idx}"
        );
        let service = node
            .manager()
            .runtime_filter_service_for_ingress(query)
            .expect("install ACK exposes the query-scoped service");
        let participant = participant_id_for_backend(backend_idx).expect("valid backend id");
        assert_eq!(
            service
                .installed_participant_install_for_test()
                .expect("service has an active installation"),
            expected_installs[&participant],
            "backend {backend_idx} installed only its compiler-authorized Core/routing roles"
        );
        services.push(service);
    }

    let producer_finsts = [
        scheduling.by_fragment[&PRODUCER_FRAGMENT][0].finst_id,
        scheduling.by_fragment[&PRODUCER_FRAGMENT][1].finst_id,
    ];
    let consumer_finst = scheduling.by_fragment[&CONSUMER_FRAGMENT][0].finst_id;
    let mut producers = Vec::new();
    for backend_idx in 0..2 {
        let producer = services[backend_idx]
            .open_producer(
                PRODUCER_BINDING,
                producer_finsts[backend_idx],
                1,
                ProducerPortKind::Membership,
            )
            .expect("producer role is authorized only on its installed BE")
            .into_membership()
            .expect("membership producer port");
        assert_eq!(
            services[backend_idx]
                .subscribe(
                    CONSUMER_BINDING,
                    consumer_finst,
                    SubscriptionKind::BlockingSnapshot,
                )
                .expect_err("producer-only BE must reject consumer authorization")
                .kind(),
            RuntimeContractViolationKind::UnauthorizedBinding
        );
        producers.push(producer);
    }
    assert_eq!(
        services[2]
            .open_producer(
                PRODUCER_BINDING,
                producer_finsts[0],
                1,
                ProducerPortKind::Membership,
            )
            .expect_err("consumer-only BE must reject producer authorization")
            .kind(),
        RuntimeContractViolationKind::UnauthorizedBinding
    );
    let subscription = services[2]
        .subscribe(
            CONSUMER_BINDING,
            consumer_finst,
            SubscriptionKind::BlockingSnapshot,
        )
        .expect("consumer role is authorized on BE two")
        .into_blocking()
        .expect("blocking snapshot consumer");

    let submit_and_close =
        |producer: &Arc<dyn crate::runtime_filter::port::producer::ProducerAdapter>, value: i64| {
            let submit = producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([value]), false),
                )
                .expect("inject contribution through the installed producer Service");
            assert!(matches!(
                submit,
                SubmitOutcome::Applied
                    | SubmitOutcome::Published
                    | SubmitOutcome::StreamAcceptedNoGlobalChange
            ));
            let _close = producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .expect("close installed producer stream");
        };
    match topology {
        ConformanceTopology::AnyOfDirect => submit_and_close(&producers[0], 11),
        ConformanceTopology::AllOfAggregate => {
            // RFD-6A validates the query-global aggregator Core and the aggregate
            // final artifact's cross-BE delivery. Remote producer-contribution
            // transport is explicitly deferred to RFD-6B, so both authorized
            // streams enter through the actual BE0 aggregator Service here.
            let aggregate_remote_producer = services[0]
                .open_producer(
                    PRODUCER_BINDING,
                    producer_finsts[1],
                    1,
                    ProducerPortKind::Membership,
                )
                .expect("aggregator Core owns the remote producer stream")
                .into_membership()
                .expect("membership producer port");
            submit_and_close(&producers[0], 11);
            submit_and_close(&aggregate_remote_producer, 22);
        }
    }

    let ArtifactAcquireOutcome::Published(bundle) = subscription.acquire(MAX_WAIT) else {
        panic!("remote consumer did not receive a published artifact within {MAX_WAIT:?}");
    };
    assert!(
        !bundle.artifacts().is_empty(),
        "the remotely delivered artifact bundle is nonempty"
    );
    let sender_participant = participant_id_for_backend(0).expect("valid sender backend id");
    let sender_routes = expected_installs[&sender_participant]
        .core_view()
        .channels()[&CHANNEL]
        .outbound_materialization_groups()
        .values()
        .flat_map(|group| group.route_edge_ids().iter().copied())
        .collect::<BTreeSet<_>>();
    assert!(!sender_routes.is_empty(), "sender owns a delivery route");
    wait_for_transport_ack(&services[0], sender_participant, &sender_routes);
    let rejected = RuntimeFilterLifecycleRegistry::global()
        .snapshot(QueryKey::from_hi_lo(QUERY.hi, QUERY.lo))
        .is_some_and(|snapshot| {
            snapshot.channel_events.values().flatten().any(|event| {
                matches!(
                    event,
                    RuntimeFilterEvent::TransportEnvelope {
                        identity,
                        kind: TransportEventKind::Acked(RuntimeFilterAcceptStatus::Rejected),
                        ..
                    } if identity.common().participant_id() == sender_participant
                        && identity.common().channel_id() == CHANNEL
                        && sender_routes.contains(&identity.route_edge_id())
                )
            })
        });
    assert!(
        !rejected,
        "the sender route must not complete with Rejected"
    );

    for node in &mut nodes {
        node.shutdown().expect("shutdown independent gRPC BE");
    }
    RuntimeFilterLifecycleRegistry::global().remove_query(lifecycle_query);
}

#[test]
fn live_three_be_anyof_direct_install_ack_and_delivery() {
    run_live_conformance(ConformanceTopology::AnyOfDirect);
}

#[test]
fn live_three_be_allof_aggregate_install_ack_and_delivery() {
    run_live_conformance(ConformanceTopology::AllOfAggregate);
}
