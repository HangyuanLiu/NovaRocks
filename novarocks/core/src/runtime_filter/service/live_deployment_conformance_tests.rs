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

use crate::catalog::schema::ColumnDef;
use crate::common::types::UniqueId;
use crate::connector::iceberg::scan_model::{
    IcebergDataFileBinding, IcebergDataFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef,
    IcebergTableInfo,
};
use crate::coordinator::cluster::LiveBackendSnapshot;
use crate::coordinator::dispatch::{FetchOutcome, FragmentDispatcher, NativeFragmentEnvelope};
use crate::coordinator::execution::ExecutionCoordinator;
use crate::coordinator::ports::{
    CoordinatorExecutionPorts, CoordinatorObserver, RuntimeFilterDeploymentControlPort,
};
use crate::coordinator::runtime_filter_deployment::NativeRuntimeFilterDeploymentPolicyProvider;
use crate::coordinator::scheduler::FragmentScheduler;
use crate::coordinator::write::handle_fragment_report_exec_status;
use crate::coordinator::write::report::FragmentExecStatusReport;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::QueryId;
use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
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
use crate::runtime_filter::port::producer::{ProducerPortKind, SubmitOutcome};
use crate::runtime_filter::port::subscription::{ArtifactAcquireOutcome, SubscriptionKind};
use crate::runtime_filter::port::transport::RuntimeFilterAcceptStatus;
use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};
use crate::service::grpc_fragment_dispatcher::GrpcRuntimeFilterDeploymentControl;
use crate::service::grpc_server::IndependentGrpcRuntimeFilterNode;
use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder;
use crate::sql::planner::distributed::write::sink::{
    IcebergWriteFragmentSink, IcebergWriteInputBinding,
};
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
use crate::sql::planner::table::{ScanSource, TableDef};

use super::RuntimeFilterService;

const CHANNEL: ChannelId = ChannelId::new(80);
const PRODUCER_BINDING: BindingId = BindingId::new(81);
const CONSUMER_BINDING: BindingId = BindingId::new(82);
const WITNESS: CoverageWitnessId = CoverageWitnessId::new(83);
const PRODUCER_FRAGMENT: u32 = 0;
const PRODUCER_NODE: i32 = 810;
const CONSUMER_FRAGMENT: u32 = 0;
const CONSUMER_NODE: i32 = PRODUCER_NODE;
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
        let query_id = submission.query_id()?;
        let finst_id = submission.fragment_instance_id()?;
        let backend_num = submission.instance_params_for_test().backend_num;
        self.submissions
            .lock()
            .unwrap()
            .push((backend_idx, submission.fragment_id(), finst_id));
        self.submit_count.fetch_add(1, Ordering::SeqCst);
        handle_fragment_report_exec_status(FragmentExecStatusReport {
            query_id,
            fragment_instance_id: finst_id,
            backend_num,
            done: true,
            status: crate::proto::common::Status {
                code: 0,
                message: String::new(),
            },
            iceberg_commits: vec![crate::proto::novarocks::IcebergCommitInfo {
                iceberg_data_file: Some(crate::proto::novarocks::IcebergDataFile {
                    path: Some(format!("s3://live-conformance/be-{backend_idx}.parquet")),
                    record_count: Some(1),
                    file_size_in_bytes: Some(1),
                    file_content: crate::proto::novarocks::IcebergFileContent::Data as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            load_counters: BTreeMap::new(),
            loaded_rows: 1,
            loaded_bytes: 1,
            filtered_rows: 0,
        })
        .map_err(|error| format!("recording dispatcher writer completion failed: {error}"))?;
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
        3
    }
}

#[derive(Default)]
struct RecordingCoordinatorObserver(AtomicUsize);

impl RecordingCoordinatorObserver {
    fn scheduled_count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl CoordinatorObserver for RecordingCoordinatorObserver {
    fn fragment_scheduled(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct InstallObservation {
    query_id: UniqueId,
    participant: RuntimeFilterParticipantId,
    install: RuntimeFilterParticipantInstall,
}

struct AckGatedDeploymentControl {
    inner: Arc<GrpcRuntimeFilterDeploymentControl>,
    installed: mpsc::SyncSender<InstallObservation>,
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
        let observation = InstallObservation {
            query_id,
            participant,
            install: install.clone(),
        };
        self.inner
            .install(query_id, lifecycle, deadline, participant, install)
            .await?;
        self.installed
            .send(observation)
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
        data_type: DataType::Int32,
        nullable: false,
        is_internal: false,
    }
}

fn expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        data_type: DataType::Int32,
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
                value_type: DataType::Int32,
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

fn source_column() -> ColumnDef {
    ColumnDef {
        name: "k".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        write_default: None,
        logical_type: None,
    }
}

fn iceberg_table() -> IcebergTableInfo {
    IcebergTableInfo {
        catalog: "live_conformance".to_string(),
        namespace: "default".to_string(),
        table: "source".to_string(),
        table_uuid: Some("00000000-0000-0000-0000-00000000006a".to_string()),
        current_snapshot_id: Some(1),
        schema_id: 1,
        location: "s3://live-conformance/source".to_string(),
        schema: IcebergSchemaDef {
            fields: vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "k".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            }],
        },
        serialized_metadata: None,
        serialized_metadata_rows: None,
    }
}

fn sealed_plan(topology: ConformanceTopology) -> crate::sql::planner::distributed::DistributedPlan {
    let column = output_column();
    let table = iceberg_table();
    let source = TableDef {
        name: "source".to_string(),
        columns: vec![source_column()],
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: ScanSource::IcebergDataFiles {
            table,
            files: (0..3)
                .map(|index| {
                    IcebergDataFileInfo::for_test(
                        &format!("s3://live-conformance/source/file-{index}.parquet"),
                        1,
                        1,
                    )
                })
                .collect(),
            cloud_properties: BTreeMap::new(),
            binding: IcebergDataFileBinding::ExplicitFiles,
        },
    };
    let fragment = PlanFragment {
        fragment_id: PRODUCER_FRAGMENT,
        root: DistributedNode {
            node_id: PRODUCER_NODE,
            fragment_id: PRODUCER_FRAGMENT,
            tuple_ids: vec![PRODUCER_NODE],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: vec![PRODUCER_BINDING, CONSUMER_BINDING],
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table: source,
                alias: None,
                columns: vec![column.clone()],
                predicates: Vec::new(),
                required_columns: Some(vec!["k".to_string()]),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::IcebergWrite(IcebergWriteFragmentSink {
            descriptor_database: "default".to_string(),
            spec: crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec(),
            input: IcebergWriteInputBinding::RootOutputByOrdinal,
        }),
        output_exprs: None,
        output_columns: vec![column],
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    DistributedPlanDraftBuilder::new(
        vec![fragment],
        Some(CONSUMER_FRAGMENT),
        Vec::new(),
        runtime_filter_graph(topology),
    )
    .seal()
    .expect("conformance fixture must pass the production distributed-plan seal")
}

fn wait_for_transport_ack(
    query_id: UniqueId,
    sender: &RuntimeFilterService,
    sender_participant: RuntimeFilterParticipantId,
    route_edge_ids: &BTreeSet<crate::runtime_filter::port::identity::RouteEdgeId>,
) {
    let deadline = std::time::Instant::now() + MAX_WAIT;
    let query = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
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

fn assert_zero_fragment_submits(dispatcher: &RecordingFragmentDispatcher) {
    assert_eq!(
        dispatcher.submit_count(),
        0,
        "the coordinator submitted a fragment before the install barrier ACKed"
    );
}

fn run_live_conformance(topology: ConformanceTopology) {
    let _serial = LIVE_CONFORMANCE_LOCK.lock().unwrap();
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
    let mut connectors = crate::connector::ConnectorRegistry::new();
    connectors.register_scan_planner(Arc::new(
        crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
    ));
    let prepared = crate::coordinator::prepare::prepare_fragments(&sealed, &connectors, None)
        .expect("prepare the sealed live conformance plan");
    let expected_prepared =
        crate::coordinator::prepare::prepare_fragments(&sealed, &connectors, None)
            .expect("prepare the expected live scheduling projection");
    let native_bundle =
        crate::protocol::native::encode::encode_native_fragment_bundle(&sealed, &prepared)
            .expect("encode the sealed live conformance plan");
    let scheduler = Arc::new(FragmentScheduler::from_live_backend_snapshot(
        backends.clone(),
    ));
    let dispatcher = Arc::new(RecordingFragmentDispatcher::default());
    let observer = Arc::new(RecordingCoordinatorObserver::default());
    let install_ack_observations = Arc::new(AtomicUsize::new(0));
    for node in &nodes {
        let dispatcher = Arc::clone(&dispatcher);
        let observations = Arc::clone(&install_ack_observations);
        node.manager()
            .set_before_runtime_filter_installed_publish_hook_for_test(Arc::new(move || {
                assert_zero_fragment_submits(dispatcher.as_ref());
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
    let mut execution_ports = CoordinatorExecutionPorts::new(
        dispatcher.clone(),
        RuntimeEndpoint::from_socket_addr(endpoints[0]),
        observer.clone(),
        control,
    );
    execution_ports.runtime_filter_policy_provider =
        Arc::new(NativeRuntimeFilterDeploymentPolicyProvider::new(2));
    let coordinator = ExecutionCoordinator::new(
        prepared,
        native_bundle,
        execution_ports,
        Arc::clone(&scheduler),
        None,
    );
    let (coordinator_done_tx, coordinator_done_rx) = mpsc::sync_channel(1);
    let coordinator_thread = std::thread::spawn(move || {
        let _ = coordinator_done_tx.send(coordinator.execute());
    });

    let mut query_id = None;
    let mut expected_installs = BTreeMap::new();
    for _ in 0..3 {
        let observation = installed_rx
            .recv_timeout(MAX_WAIT)
            .expect("real coordinator install RPC completes before the ACK gate");
        match query_id {
            Some(expected) => assert_eq!(observation.query_id, expected),
            None => query_id = Some(observation.query_id),
        }
        assert_eq!(
            observation.install.local_participant_id(),
            observation.participant
        );
        assert!(
            expected_installs
                .insert(observation.participant, observation.install)
                .is_none(),
            "coordinator installs every participant exactly once"
        );
    }
    let query_id = query_id.expect("coordinator generated query id");
    let lifecycle_query = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
    assert_eq!(
        expected_installs.len(),
        3,
        "all three live BEs participate in the compiled install"
    );
    assert_eq!(
        expected_installs
            .values()
            .map(RuntimeFilterParticipantInstall::epoch)
            .collect::<BTreeSet<_>>()
            .len(),
        1,
        "all participants install the same deployment epoch"
    );
    assert_zero_fragment_submits(dispatcher.as_ref());
    assert_eq!(observer.scheduled_count(), 0);
    assert_eq!(
        install_ack_observations.load(Ordering::SeqCst),
        3,
        "every install handler observed the zero-submit pre-ACK invariant"
    );
    ack_release.add_permits(3);
    coordinator_done_rx
        .recv_timeout(MAX_WAIT)
        .expect("production coordinator terminates within the bound")
        .expect("production coordinator crosses install, assembly, submit, and write completion");
    coordinator_thread.join().expect("coordinator thread joins");

    let scheduling = scheduler
        .schedule(expected_prepared.scheduling_view(), query_id)
        .expect("replay the production scheduler projection for exact comparison");
    let mut actual_submissions = dispatcher.submissions();
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
        "ExecutionCoordinator dispatches every production-scheduled placement exactly once"
    );
    assert_eq!(
        actual_submissions.len(),
        3,
        "exactly three fragments submit"
    );
    assert_eq!(observer.scheduled_count(), 3);
    assert_eq!(
        actual_submissions
            .iter()
            .map(|(backend_idx, _, _)| *backend_idx)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 2]),
        "the three scheduled scan placements cover all live BEs"
    );

    let query = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
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

    let producer_finsts = scheduling.by_fragment[&PRODUCER_FRAGMENT]
        .iter()
        .map(|placement| (placement.backend_idx, placement.finst_id))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(producer_finsts.len(), 3);
    let consumer_finst = producer_finsts[&2];
    let mut producers = Vec::new();
    for backend_idx in 0..3 {
        let producer = services[backend_idx]
            .open_producer(
                PRODUCER_BINDING,
                producer_finsts[&backend_idx],
                1,
                ProducerPortKind::Membership,
            )
            .expect("producer role is authorized on every scheduled BE")
            .into_membership()
            .expect("membership producer port");
        producers.push(producer);
    }
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
        |producer: &Arc<dyn crate::runtime_filter::port::producer::ProducerAdapter>, value: i32| {
            let submit = producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int32([value]), false),
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
            // transport is explicitly deferred to RFD-6B, so all authorized
            // streams enter through the actual BE0 aggregator Service here.
            submit_and_close(&producers[0], 11);
            for (backend_idx, value) in [(1, 22), (2, 33)] {
                let aggregate_remote_producer = services[0]
                    .open_producer(
                        PRODUCER_BINDING,
                        producer_finsts[&backend_idx],
                        1,
                        ProducerPortKind::Membership,
                    )
                    .expect("aggregator Core owns every remote producer stream")
                    .into_membership()
                    .expect("membership producer port");
                submit_and_close(&aggregate_remote_producer, value);
            }
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
    wait_for_transport_ack(query_id, &services[0], sender_participant, &sender_routes);
    let rejected = RuntimeFilterLifecycleRegistry::global()
        .snapshot(lifecycle_query)
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
