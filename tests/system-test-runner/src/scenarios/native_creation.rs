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

//! Frozen task creation across the real 1FE+3BE process boundary.
//!
//! A create is decided by its exact task identity: the first legal round wins
//! and prepares the task, and every later request for that identity is
//! answered from the task that round created, whatever body it carries. The
//! frontend freezes each static plan once per statement, freezes a create's
//! metadata once when the create is first admitted, resends exactly those
//! bytes, and lets go of them once the create is answered.
//!
//! The backend half is observed through its own receipts and the create
//! markers it prints; the frontend half through the task-creation gauges it
//! exports, which fall only when a payload's last holder drops it.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use novarocks_native_trust::NativeEndpointConnector;
use novarocks_proto_models::{common, novarocks as proto, plan};
use novarocks_types::identity::{BackendProcessId, FrontendProcessId};

use crate::actors::mysql as mysql_actor;
use crate::actors::mysql_stream::MysqlStream;
use crate::scenario::{Scenario, ScenarioContext};

use super::native_compatibility::{
    HEARTBEAT_PATH, RawUnaryResponse, authorization_header, decode_hex_32, only_successful_receipt,
    raw_acquire_admission_ticket, raw_apply_task_operations, raw_establish, raw_operation_envelope,
    raw_unary, raw_unary_response,
};
use super::query_lifecycle::{
    NID2_FENCE_QUERY, arm_on_every_backend, assert_two_sleep_rows, await_backend_exit,
    await_fresh_task_create, await_resource_convergence, await_terminal_snapshot,
    await_token_scoped_marker, latest_execution_id, resource_snapshot,
};

const REQUIRED_BACKENDS: usize = 3;
const BASELINE_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// A release is a small control operation, which only the control method
/// carries.
const CONTROL_PATH: &str = "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations";

const CREATE_APPLIED: &str = "NOVAROCKS_TASK_CREATE_APPLIED";
const CREATE_IDEMPOTENT: &str = "NOVAROCKS_TASK_CREATE_IDEMPOTENT";
const CREATE_ACK_DROPPED: &str = "NOVAROCKS_TASK_CREATE_ACK_DROPPED";
const LEASE_RENEWED: &str = "NOVAROCKS_TASK_LEASE_RENEWED";

const STATIC_FRAGMENTS_FROZEN: &str = "novarocks_task_static_fragments_frozen_total";
const STATIC_FRAGMENTS_RETAINED: &str = "novarocks_task_static_fragments_retained";
const CREATES_PRICED: &str = "novarocks_task_creates_priced_total";
const CREATES_FROZEN: &str = "novarocks_task_creates_frozen_total";
const CREATE_PAYLOADS_RETAINED: &str = "novarocks_task_create_payloads_retained";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(FrozenReplayAndMembership),
        Box::new(CreationPayloadLifetime),
        Box::new(FixedPlanRecovery),
    ]
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let actual = context.handle().be_count();
    ensure!(
        actual == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, but the runner launched 1FE+{actual}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}

// ---------------------------------------------------------------------------
// native-creation/frozen-replay-and-membership
// ---------------------------------------------------------------------------

/// Same-identity creates across the real authenticated boundary.
///
/// One backend receives, over its own Native listener, a legal create, the
/// identical create again, and a create for the same identity whose every
/// body fact differs. The first wins; both replays are answered with the
/// winner's original receipt and nothing is applied, interpreted or renewed a
/// second time. A request under another frontend process, another attempt or
/// another backend's identity never reads that receipt. A create whose initial
/// domain names a member its descriptor never froze is refused without
/// holding its identity, so the next legal create of that identity wins. After
/// the context is released, a replay still cannot bring a task back.
struct FrozenReplayAndMembership;

impl Scenario for FrozenReplayAndMembership {
    fn name(&self) -> &'static str {
        "native-creation/frozen-replay-and-membership"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let session = RawBackendSession::establish(context, 0)?;
        let applied = |context: &mut ScenarioContext| session.marker_count(context, CREATE_APPLIED);
        let idempotent =
            |context: &mut ScenarioContext| session.marker_count(context, CREATE_IDEMPOTENT);
        let renewed_before = context.handle().be_log_count(0, LEASE_RENEWED)?;
        context.action(format!(
            "established raw query context {} on BE[0] through its authenticated Native listener",
            session.execution_label()
        ));

        // The winner.
        let first = RawCreate::values(&session, 1);
        let accepted = session.apply(first.operation(), "first CreateTask")?;
        ensure!(
            outcome(&accepted) == proto::TaskOperationOutcome::Accepted,
            "the first legal create must win, got {accepted:?}"
        );
        let original = accepted
            .ack
            .clone()
            .context("the winning create returned no acknowledgement")?;
        ensure!(
            matches!(original, proto::task_operation_receipt::Ack::CreateTask(_)),
            "the winning create acknowledged something other than a task: {original:?}"
        );
        await_count(context, "the winner's create marker", 1, applied)?;
        context.action("the first legal create won and applied exactly one task on BE[0]");

        // The identical request, and the same identity with every body fact
        // changed: parallelism and its frozen domain, kernel key, instance
        // ordinal and sink.
        let exact = session.apply(first.operation(), "exact CreateTask replay")?;
        let mut variant = first.clone();
        variant.kernel_key_low = 2;
        variant.pipeline_dop = 3;
        variant.sink = plan::data_sink::Kind::Result(true);
        variant.instance_ordinal = 9;
        let changed = session.apply(variant.operation(), "changed-body CreateTask replay")?;
        for (label, replay) in [("exact", &exact), ("changed-body", &changed)] {
            ensure!(
                outcome(replay) == proto::TaskOperationOutcome::Idempotent,
                "an {label} replay of a created identity must be idempotent, got {replay:?}"
            );
            ensure!(
                replay.ack.as_ref() == Some(&original),
                "an {label} replay must carry the winner's original receipt, got {replay:?}"
            );
        }
        await_count(context, "both replays' idempotent markers", 2, idempotent)?;
        ensure!(
            applied(context)? == 1,
            "a replay applied a second creation for one identity"
        );
        ensure!(
            context.handle().be_log_count(0, LEASE_RENEWED)? == renewed_before,
            "a create replay renewed the context lease"
        );
        context.action(
            "exact and changed-body replays both returned the original receipt; nothing was applied, interpreted or renewed again",
        );

        // Another scope never reads this identity's receipt.
        let mut foreign_frontend = first.clone();
        foreign_frontend.context = session.context_under(FrontendProcessId::new_v7(), 1);
        let mut next_attempt = first.clone();
        next_attempt.context = session.context_under(session.frontend, 2);
        let mut foreign_backend = first.clone();
        let stranger = BackendProcessId::new_v7();
        foreign_backend.backend_override = Some(stranger);
        let mut scope_outcomes = Vec::new();
        for (label, mut request) in [
            ("another frontend process", foreign_frontend),
            ("another attempt", next_attempt),
            ("another backend's identity", foreign_backend),
        ] {
            request.max_wait_millis = 50;
            let receipt = session.apply(request.operation(), label)?;
            let verdict = outcome(&receipt);
            ensure!(
                !matches!(
                    verdict,
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::Idempotent
                ) && receipt.ack.is_none(),
                "a create under {label} read or created a task: {receipt:?}"
            );
            scope_outcomes.push(format!("{label}={verdict:?}"));
        }
        ensure!(
            applied(context)? == 1,
            "a create under another scope applied a task"
        );
        context.action(format!(
            "creates under other scopes read no receipt and applied nothing: {}",
            scope_outcomes.join(", ")
        ));

        // Membership is the winner's own check, and a refused round holds
        // nothing: a later legal create of the same identity wins.
        let mut refused = RawCreate::values(&session, 2);
        refused.initial_domains = vec![proto::TaskDomainUpdate {
            domain: Some(proto::task_domain_update::Domain::OpenExchangeEdges(
                proto::OpenExchangeEdgesDomain {
                    version: 1,
                    edge_ids: vec![7],
                },
            )),
        }];
        let refusal = session.apply(refused.operation(), "CreateTask naming an unfrozen edge")?;
        ensure!(
            outcome(&refusal) == proto::TaskOperationOutcome::InvalidStateOrRequest
                && refusal.ack.is_none(),
            "an initial domain naming an edge the descriptor never froze must be refused, got {refusal:?}"
        );
        let mut legal = refused.clone();
        legal.initial_domains.clear();
        let won = session.apply(legal.operation(), "legal CreateTask after a refused round")?;
        ensure!(
            outcome(&won) == proto::TaskOperationOutcome::Accepted,
            "a legal create after a refused round must win, got {won:?}"
        );
        await_count(context, "the second identity's create marker", 2, applied)?;
        context.action(
            "an initial domain naming an unfrozen edge was refused, and the next legal create of that identity won",
        );

        // After release nothing is created again.
        let released = session.release_when_ready(context)?;
        let after = session.apply(first.operation(), "CreateTask replay after release")?;
        let verdict = outcome(&after);
        ensure!(
            verdict != proto::TaskOperationOutcome::Accepted,
            "a replay after release created a task again: {after:?}"
        );
        if verdict == proto::TaskOperationOutcome::Idempotent {
            ensure!(
                after.ack.as_ref() == Some(&original),
                "a retained answer after release must be the original receipt, got {after:?}"
            );
        }
        ensure!(
            applied(context)? == 2,
            "a replay after release applied a creation"
        );
        context.action(format!(
            "released the context ({released:?}); a later replay answered {verdict:?} and applied nothing"
        ));
        Ok(())
    }
}

/// One raw, authenticated Native session against one backend, holding an
/// established query context of its own.
struct RawBackendSession {
    index: usize,
    connector: NativeEndpointConnector,
    authorization: String,
    backend: BackendProcessId,
    frontend: FrontendProcessId,
    query_id: common::UniqueId,
}

impl RawBackendSession {
    fn establish(context: &mut ScenarioContext, index: usize) -> Result<Self> {
        let port = context.handle().runtime().be[index].grpc;
        let rows = context.handle().frontend_backend_topology()?;
        let row = rows
            .iter()
            .find(|row| row.grpc_port == port)
            .with_context(|| format!("SHOW BACKENDS omitted BE[{index}]"))?;
        ensure!(
            row.is_eligible_live(),
            "BE[{index}] must be eligible and live, row={row:?}"
        );
        let backend = row
            .process_id
            .parse::<BackendProcessId>()
            .context("parse the target backend process identity")?;
        let compatibility = decode_hex_32(&row.native_compatibility_id)?;
        let endpoint = context.handle().native_be_endpoint(index)?;
        let mode = context.handle().native_trust_mode();
        let connector = context.handle().native_probe_connector(endpoint, mode)?;
        let trust = context.handle().native_probe_trust()?;
        let authorization = authorization_header(&trust)?;
        let heartbeat: proto::HeartbeatResponse = raw_unary(
            connector.clone(),
            HEARTBEAT_PATH,
            &authorization,
            proto::HeartbeatRequest {
                expected_process_id: Some(proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                }),
            },
        )?;
        let admission_epoch = heartbeat
            .admission_epoch_capability
            .context("the target heartbeat omitted its admission epoch capability")?;
        let session = Self {
            index,
            connector,
            authorization,
            backend,
            frontend: FrontendProcessId::new_v7(),
            // A query id of this scenario's own, so every marker it counts is
            // this session's and no other case's.
            query_id: common::UniqueId {
                hi: 0x5b5b,
                lo: i64::from(std::process::id()),
            },
        };
        let query_context = session.context_under(session.frontend, 1);
        let acquisition = session.apply(
            raw_acquire_admission_ticket(query_context.clone(), compatibility, admission_epoch),
            "admission ticket acquisition",
        )?;
        let Some(proto::task_operation_receipt::Ack::QueryContextAdmissionTicket(ticket)) =
            acquisition.ack
        else {
            bail!(
                "admission acquisition was not granted: {:?}",
                acquisition.outcome
            );
        };
        let ticket = ticket
            .ticket_id
            .context("the admission acknowledgement omitted its ticket id")?;
        let establish = session.apply(
            raw_establish(query_context, ticket, Some(compatibility.to_vec())),
            "Establish",
        )?;
        ensure!(
            outcome(&establish) == proto::TaskOperationOutcome::Accepted,
            "the raw Establish must be accepted, got {establish:?}"
        );
        Ok(session)
    }

    /// This session's query context, under `frontend` and `attempt`.
    fn context_under(&self, frontend: FrontendProcessId, attempt: u64) -> proto::QueryContextRef {
        proto::QueryContextRef {
            query_execution_id: Some(proto::QueryExecutionId {
                query_id: Some(self.query_id),
                attempt_id: attempt,
            }),
            frontend_process_id: Some(proto::FrontendProcessId {
                value: frontend.to_bytes().to_vec(),
            }),
            backend_process_id: Some(proto::BackendProcessId {
                value: self.backend.to_bytes().to_vec(),
            }),
        }
    }

    fn execution_label(&self) -> String {
        format!("{}:{}", self.query_id.hi, self.query_id.lo)
    }

    /// How many `marker` lines this backend printed for this session's first
    /// attempt.
    fn marker_count(&self, context: &mut ScenarioContext, marker: &str) -> Result<usize> {
        let needle = format!("{marker} execution_id={}:1 ", self.execution_label());
        context.handle().be_log_count(self.index, &needle)
    }

    fn apply(
        &self,
        operation: proto::TaskOperation,
        subject: &str,
    ) -> Result<proto::TaskOperationReceipt> {
        only_successful_receipt(
            raw_apply_task_operations(&self.connector, &self.authorization, vec![operation])?,
            subject,
        )
    }

    /// Releases this session's context once every local task is terminal.
    fn release_when_ready(
        &self,
        context: &mut ScenarioContext,
    ) -> Result<proto::ReleaseQueryContextOutcome> {
        loop {
            let response: RawUnaryResponse<proto::ApplyTaskOperationsResponse> =
                raw_unary_response(
                    &self.connector,
                    CONTROL_PATH,
                    &self.authorization,
                    proto::ApplyTaskControlOperationsRequest {
                        operations: vec![proto::TaskControlOperation {
                            envelope: Some(raw_operation_envelope(1_000)),
                            control: Some(
                                proto::task_control_operation::Control::ReleaseQueryContext(
                                    proto::ReleaseQueryContextRequest {
                                        query_context: Some(self.context_under(self.frontend, 1)),
                                    },
                                ),
                            ),
                        }],
                    },
                )?;
            let receipt = only_successful_receipt(response, "ReleaseQueryContext")?;
            let Some(proto::task_operation_receipt::Ack::ReleaseQueryContext(ack)) = receipt.ack
            else {
                bail!("the release returned no release acknowledgement: {receipt:?}");
            };
            let released = proto::ReleaseQueryContextOutcome::try_from(ack.outcome)
                .context("decode the release outcome")?;
            if released != proto::ReleaseQueryContextOutcome::NotReady {
                return Ok(released);
            }
            let remaining = context.remaining("release the raw query context")?;
            thread::sleep(remaining.min(POLL_INTERVAL));
        }
    }
}

/// Every fact one raw create carries, so a test can vary exactly one body
/// fact and keep the identity.
#[derive(Clone)]
struct RawCreate {
    context: proto::QueryContextRef,
    task_id: u32,
    backend_override: Option<BackendProcessId>,
    kernel_key_low: i64,
    pipeline_dop: u32,
    sink: plan::data_sink::Kind,
    instance_ordinal: u32,
    initial_domains: Vec<proto::TaskDomainUpdate>,
    max_wait_millis: u64,
}

impl RawCreate {
    /// A decodable, self-contained task: one VALUES node into a NOOP sink,
    /// frozen for exactly its own parallelism.
    fn values(session: &RawBackendSession, task_id: u32) -> Self {
        Self {
            context: session.context_under(session.frontend, 1),
            task_id,
            backend_override: None,
            kernel_key_low: i64::from(task_id) * 100 + 1,
            pipeline_dop: 2,
            sink: plan::data_sink::Kind::Noop(true),
            instance_ordinal: 0,
            initial_domains: Vec::new(),
            max_wait_millis: 15_000,
        }
    }

    fn operation(&self) -> proto::TaskOperation {
        use prost::Message;

        let execution = self
            .context
            .query_execution_id
            .expect("a raw context carries its execution");
        let mut context = self.context.clone();
        let backend = match self.backend_override {
            Some(backend) => {
                let backend = proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                };
                context.backend_process_id = Some(backend.clone());
                backend
            }
            None => context
                .backend_process_id
                .clone()
                .expect("a raw context carries its backend"),
        };
        let frozen = proto::FrozenFragment {
            plan_version: vec![0x5b; 16],
            plan_contract_revision: 1,
            fragment_contract_version: 1,
            pipeline_dop_domain: Some(proto::PipelineDopDomain {
                min: self.pipeline_dop,
                max: self.pipeline_dop,
                requires_power_of_two: false,
            }),
            plan: Some(plan::PlanFragment {
                fragment_id: 1,
                root: Some(plan::DistributedNode {
                    node_id: 10,
                    fragment_id: 1,
                    limit: -1,
                    payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                        output_columns: Vec::new(),
                        kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                            rows: Vec::new(),
                            columns: Vec::new(),
                        })),
                    })),
                    ..Default::default()
                }),
                sink: Some(plan::DataSink {
                    kind: Some(self.sink.clone()),
                }),
                runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                    fragment_id: 1,
                    bindings: Vec::new(),
                }),
                ..Default::default()
            }),
        };
        let metadata = proto::CreationMetadata {
            query_context: Some(context),
            descriptor: Some(proto::TaskDescriptor {
                identity: Some(proto::TaskIdentity {
                    query_execution_id: Some(execution),
                    stage_id: 1,
                    task_id: self.task_id,
                    backend_process_id: Some(backend),
                }),
                fragment_instance_id: Some(common::UniqueId {
                    hi: 0x5b,
                    lo: self.kernel_key_low,
                }),
                pipeline_dop: self.pipeline_dop,
                split_plan_nodes: Vec::new(),
                topology: Some(Default::default()),
            }),
            initial_domains: self.initial_domains.clone(),
            assignment: Some(proto::TaskAssignment {
                instance_ordinal: self.instance_ordinal,
                initial_scan_ranges: Vec::new(),
                sink_edge_ids: Vec::new(),
            }),
        };
        proto::TaskOperation {
            envelope: Some(raw_operation_envelope(self.max_wait_millis)),
            operation: Some(proto::task_operation::Operation::CreateTask(
                proto::CreateTaskRequest {
                    frozen_fragment: frozen.encode_to_vec().into(),
                    creation_metadata: metadata.encode_to_vec().into(),
                },
            )),
        }
    }
}

fn outcome(receipt: &proto::TaskOperationReceipt) -> proto::TaskOperationOutcome {
    proto::TaskOperationOutcome::try_from(receipt.outcome)
        .unwrap_or(proto::TaskOperationOutcome::Unspecified)
}

/// Polls `count` until it reaches `expected`: backend output reaches the
/// harness through an asynchronous pump, so one read can be a little early.
fn await_count(
    context: &mut ScenarioContext,
    subject: &str,
    expected: usize,
    count: impl Fn(&mut ScenarioContext) -> Result<usize>,
) -> Result<()> {
    loop {
        let current = count(context)?;
        ensure!(
            current <= expected,
            "{subject}: observed {current}, more than the expected {expected}"
        );
        if current == expected {
            return Ok(());
        }
        let remaining = context.remaining(&format!("observe {subject}"))?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}

// ---------------------------------------------------------------------------
// native-creation/creation-payload-lifetime
// ---------------------------------------------------------------------------

/// Where the frontend's creation payloads are, through the real statement
/// path.
///
/// An exactly answered create is released while its statement still runs;
/// the statement's static plans stay until the statement ends. A create
/// whose acknowledgement is lost keeps its payload, is resent as the same
/// frozen bytes, is answered by its identity, and is never frozen or applied
/// twice. A cancelled statement releases everything it froze.
struct CreationPayloadLifetime;

impl Scenario for CreationPayloadLifetime {
    fn name(&self) -> &'static str {
        "native-creation/creation-payload-lifetime"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        let idle = FrontendCreationGauges::scrape(context)?;
        context.action(format!("idle frontend creation gauges: {idle:?}"));

        // Exact acknowledgements release the payload mid-statement.
        let pending = PendingRead::start(context, NID2_FENCE_QUERY)?;
        let mut peak_payloads = 0;
        loop {
            let now = FrontendCreationGauges::scrape(context)?;
            peak_payloads = peak_payloads.max(now.create_payloads_retained);
            if now.creates_frozen > idle.creates_frozen
                && now.create_payloads_retained == idle.create_payloads_retained
                && now.static_fragments_retained > idle.static_fragments_retained
            {
                ensure!(
                    pending.still_running(),
                    "the statement ended before its create payloads were observed released"
                );
                context.action(format!(
                    "while the statement still ran, every answered create released its payload \
                     (peak {peak_payloads} retained) and its static plans stayed retained ({} alive)",
                    now.static_fragments_retained
                ));
                break;
            }
            ensure!(
                pending.still_running(),
                "the statement ended before its create payloads were observed released: {now:?}"
            );
            let remaining =
                context.remaining("observe answered creates releasing their payloads")?;
            thread::sleep(remaining.min(POLL_INTERVAL));
        }
        let rows = pending.finish(context)?;
        ensure!(
            rows.len() == 2,
            "the delayed read returned {rows:?} instead of two rows"
        );
        await_idle_gauges(context, &idle, "a completed statement")?;
        context.action("the completed statement released its static plans");

        // A lost acknowledgement: the exact bytes are resent and answered by
        // identity; nothing is frozen or applied twice.
        let before = FrontendCreationGauges::scrape(context)?;
        let applied_before = backend_marker_total(context, CREATE_APPLIED)?;
        let idempotent_before = backend_marker_total(context, CREATE_IDEMPOTENT)?;
        let tokens = arm_on_every_backend(context, "create-task-ack-drop")?;
        let result = run_baseline_query(context);
        let cleared = context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear create-task-ack-drop tokens");
        result?;
        cleared?;
        let dropped_on = await_token_scoped_marker(context, CREATE_ACK_DROPPED, &tokens)?;
        let after = FrontendCreationGauges::scrape(context)?;
        let applied = backend_marker_total(context, CREATE_APPLIED)? - applied_before;
        let replayed = backend_marker_total(context, CREATE_IDEMPOTENT)? - idempotent_before;
        let frozen = after.creates_frozen - before.creates_frozen;
        let priced = after.creates_priced - before.creates_priced;
        ensure!(
            replayed >= 1,
            "a create whose acknowledgement was dropped was never answered by its identity"
        );
        ensure!(
            frozen == i64::try_from(applied)? && priced == frozen,
            "every task must be priced, frozen and applied exactly once despite a lost \
             acknowledgement: priced={priced} frozen={frozen} applied={applied}"
        );
        await_idle_gauges(
            context,
            &idle,
            "a statement whose create acknowledgement was lost",
        )?;
        context.action(format!(
            "BE[{dropped_on}] dropped a create acknowledgement; the frontend resent the same frozen \
             create, {replayed} replay(s) were answered idempotently, and {applied} task(s) were \
             priced, frozen and applied once each"
        ));

        // Cancellation releases everything the statement froze.
        let created_before = (0..context.handle().be_count())
            .map(|index| context.handle().be_log_count(index, CREATE_APPLIED))
            .collect::<Result<Vec<_>>>()?;
        let pending = PendingRead::start(context, NID2_FENCE_QUERY)?;
        await_fresh_task_create(context, &created_before)?;
        let deadline = context.deadline();
        context
            .handle()
            .kill_query_until(pending.connection_id, deadline)
            .context("cancel the delayed read")?;
        match pending.finish(context) {
            Ok(rows) => bail!("a cancelled statement returned {rows:?}"),
            Err(error) => {
                context.action(format!("the cancelled statement failed as asked: {error}"))
            }
        }
        await_idle_gauges(context, &idle, "a cancelled statement")?;
        await_resource_convergence(context, &baseline)?;
        context.action("the cancelled statement released every payload it froze");
        Ok(())
    }
}

/// The frontend's task-creation gauges and counters at one instant.
#[derive(Clone, Copy, Debug)]
struct FrontendCreationGauges {
    static_fragments_frozen: i64,
    static_fragments_retained: i64,
    creates_priced: i64,
    creates_frozen: i64,
    create_payloads_retained: i64,
}

impl FrontendCreationGauges {
    fn scrape(context: &mut ScenarioContext) -> Result<Self> {
        let port = context.handle().runtime().fe_http_port;
        let body = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?
            .get(format!("http://127.0.0.1:{port}/metrics"))
            .send()
            .context("scrape FE /metrics")?
            .error_for_status()
            .context("FE /metrics status")?
            .text()
            .context("read FE /metrics")?;
        Ok(Self {
            static_fragments_frozen: metric(&body, STATIC_FRAGMENTS_FROZEN)?,
            static_fragments_retained: metric(&body, STATIC_FRAGMENTS_RETAINED)?,
            creates_priced: metric(&body, CREATES_PRICED)?,
            creates_frozen: metric(&body, CREATES_FROZEN)?,
            create_payloads_retained: metric(&body, CREATE_PAYLOADS_RETAINED)?,
        })
    }
}

/// The one unlabelled sample of `name` in a Prometheus text body.
fn metric(body: &str, name: &str) -> Result<i64> {
    let samples = body
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (metric, value) = line.split_once(' ')?;
            (metric == name).then_some(value.trim())
        })
        .collect::<Vec<_>>();
    let [value] = samples.as_slice() else {
        bail!("FE /metrics must carry exactly one {name} sample, found {samples:?}");
    };
    let value = value
        .parse::<f64>()
        .with_context(|| format!("parse {name} sample {value:?}"))?;
    Ok(value as i64)
}

/// Waits until the frontend holds no more creation payload or static plan
/// than it did while idle.
fn await_idle_gauges(
    context: &mut ScenarioContext,
    idle: &FrontendCreationGauges,
    subject: &str,
) -> Result<()> {
    loop {
        let now = FrontendCreationGauges::scrape(context)?;
        if now.create_payloads_retained == idle.create_payloads_retained
            && now.static_fragments_retained == idle.static_fragments_retained
        {
            return Ok(());
        }
        let remaining = context.remaining(&format!(
            "observe {subject} releasing its payloads: {now:?}"
        ))?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}

fn backend_marker_total(context: &mut ScenarioContext, marker: &str) -> Result<usize> {
    let mut total = 0;
    for index in 0..context.handle().be_count() {
        total += context.handle().be_log_count(index, marker)?;
    }
    Ok(total)
}

fn run_baseline_query(context: &mut ScenarioContext) -> Result<()> {
    let mut connection = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining("connect the baseline read")?,
    )?;
    let rows: Vec<i64> = connection
        .query(BASELINE_QUERY)
        .context("run the baseline read")?;
    ensure!(
        rows == vec![1, 2],
        "the baseline read returned {rows:?} instead of [1, 2]"
    );
    Ok(())
}

/// One statement running on its own connection, cancellable by id.
struct PendingRead {
    thread: thread::JoinHandle<Result<()>>,
    connection_id: u32,
    done: mpsc::Receiver<Result<Vec<i64>, mysql::Error>>,
}

impl PendingRead {
    fn start(context: &mut ScenarioContext, sql: &'static str) -> Result<Self> {
        let (id_tx, id_rx) = mpsc::sync_channel(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let connect = context.remaining("connect the delayed read")?;
        let thread = thread::Builder::new()
            .name("native-creation-read".to_owned())
            .spawn(move || -> Result<()> {
                let mut connection = mysql_actor::connect_for_cancellation(&user, port, connect)?;
                id_tx
                    .send(connection.connection_id())
                    .context("publish the delayed read connection id")?;
                let outcome = connection.query(sql);
                done_tx
                    .send(outcome)
                    .context("publish the delayed read result")
            })
            .context("start the delayed read")?;
        let connection_id = id_rx
            .recv_timeout(context.remaining("receive the delayed read connection id")?)
            .context("the delayed read ended before publishing its connection id")?;
        Ok(Self {
            thread,
            connection_id,
            done,
        })
    }

    fn still_running(&self) -> bool {
        matches!(self.done.try_recv(), Err(mpsc::TryRecvError::Empty))
    }

    fn finish(self, context: &mut ScenarioContext) -> Result<Vec<i64>> {
        let outcome = self
            .done
            .recv_timeout(context.remaining("await the delayed read")?)
            .context("the delayed read did not finish")?;
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("the delayed read thread panicked"))??;
        outcome.context("the delayed read failed")
    }
}

// ---------------------------------------------------------------------------
// native-creation/fixed-plan-recovery
// ---------------------------------------------------------------------------

/// A recovered statement runs the plan it was activated with.
///
/// The same delayed read runs once cleanly and once with the backend that
/// admitted its first task killed before any result was read. The second
/// completes on replacement attempt 2 through the real actor authorization
/// and recovery path, and the frontend freezes exactly as many static plans
/// for it as for the clean run: the recovery attempt encoded none of its own.
struct FixedPlanRecovery;

impl Scenario for FixedPlanRecovery {
    fn name(&self) -> &'static str {
        "native-creation/fixed-plan-recovery"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        let idle = FrontendCreationGauges::scrape(context)?;

        // The reference: how many static plans one clean run freezes.
        let before_clean = latest_execution_id(context)?;
        let pending = PendingRead::start(context, NID2_FENCE_QUERY)?;
        let rows = pending.finish(context)?;
        ensure!(rows.len() == 2, "the clean read returned {rows:?}");
        let clean_terminal = await_terminal_snapshot(context, before_clean.as_deref())?;
        ensure!(
            clean_terminal.attempt_id == 1,
            "the clean read unexpectedly ran attempt {}",
            clean_terminal.attempt_id
        );
        let after_clean = FrontendCreationGauges::scrape(context)?;
        let clean_static = after_clean.static_fragments_frozen - idle.static_fragments_frozen;
        ensure!(
            clean_static > 0,
            "the clean read froze no static plan: {idle:?} -> {after_clean:?}"
        );
        await_idle_gauges(context, &idle, "the clean read")?;
        context.action(format!(
            "a clean run froze {clean_static} static plan(s) on attempt 1"
        ));

        // The recovery: kill the backend that admitted a task before any row
        // is read, so the statement must complete on a replacement attempt.
        let before_recovery = latest_execution_id(context)?;
        let frozen_before = FrontendCreationGauges::scrape(context)?;
        let created_before = (0..context.handle().be_count())
            .map(|index| context.handle().be_log_count(index, CREATE_APPLIED))
            .collect::<Result<Vec<_>>>()?;
        let mut stream = MysqlStream::query(
            context.mysql_user(),
            context.mysql_port(),
            NID2_FENCE_QUERY,
            context.remaining("open the recovered read")?,
        )?;
        let target = await_fresh_task_create(context, &created_before)?;
        context
            .handle()
            .kill_be(target)
            .with_context(|| format!("kill admitted BE[{target}]"))?;
        await_backend_exit(context, target)?;
        assert_two_sleep_rows(&mut stream)?;
        let terminal = await_terminal_snapshot(context, before_recovery.as_deref())?;
        ensure!(
            terminal.attempt_id == 2,
            "the read must complete on replacement attempt 2, got attempt {}",
            terminal.attempt_id
        );
        let recovered = FrontendCreationGauges::scrape(context)?;
        let recovered_static =
            recovered.static_fragments_frozen - frozen_before.static_fragments_frozen;
        ensure!(
            recovered_static == clean_static,
            "a recovered statement froze {recovered_static} static plan(s) where a clean run \
             freezes {clean_static}: the recovery attempt encoded its own"
        );
        await_idle_gauges(context, &idle, "the recovered read")?;
        await_resource_convergence(context, &baseline)?;
        context.action(format!(
            "the read recovered on attempt 2 after BE[{target}] exited and froze {recovered_static} \
             static plan(s), exactly as the clean run did"
        ));

        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(target, deadline)
            .with_context(|| format!("restore BE[{target}]"))?;
        Ok(())
    }
}
