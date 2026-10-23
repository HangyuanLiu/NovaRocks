// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One real producer keeps sending to B after A's exact normal departure.

use std::path::Path;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};
use arrow_array::{Array, Int64Array};
use novarocks_cluster_harness::ServerHandle;
use novarocks_cluster_harness::test_sessions::{
    ClusterTestSessionPlan, RoleLaunchReference, RoleSlot,
};
use novarocks_failpoint::{session::DomainGrant, target_binding::RoleProcessReceipt};
use novarocks_proto_models::{common, novarocks as proto};
use novarocks_types::{BackendProcessId, FrontendProcessId};
use prost::Message;
use serde_json::json;

use super::native_compatibility::{raw_operation_envelope, raw_unary};
use super::native_creation::{
    RawBackendSession, RawCreate, RawSink, compiled_fragment, observe_task_terminal_and_stopped,
    outcome, receipt_verdict, session_control,
};
use super::query_lifecycle::{await_resource_convergence, resource_snapshot};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};

const ROWS: [i64; 5] = [7, -11, 0, i64::MAX, i64::MIN];
const POLL: Duration = Duration::from_millis(50);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(SameProducerNormalClose {
            multicast: false,
            observation: ExchangeObservationOwner::default(),
        }),
        Box::new(SameProducerNormalClose {
            multicast: true,
            observation: ExchangeObservationOwner::default(),
        }),
    ]
}

struct SameProducerNormalClose {
    multicast: bool,
    observation: ExchangeObservationOwner,
}

struct FrozenExchangeObservation {
    identity: (common::UniqueId, FrontendProcessId),
    references: Vec<RoleLaunchReference>,
    deadline: Instant,
    selected: bool,
}
#[derive(Default)]
struct ExchangeObservationOwner(Mutex<Option<FrozenExchangeObservation>>);
impl ExchangeObservationOwner {
    fn freeze(
        &self,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<ClusterTestSessionPlan> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("exchange observation owner poisoned"))?;
        ensure!(owner.is_none(), "exchange observation already frozen");
        ensure!(
            cluster_size == 3,
            "exchange observations require native 1FE+3BE"
        );
        // This is the fixture producer's original request identity, allocated
        // once before launch and then used unchanged in all three contexts.
        let identity = RawBackendSession::declare_identity();
        let mut plan = ClusterTestSessionPlan::new(deadline)?;
        let root = plan.reserve_transport("exact-exchange-observations")?;
        let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
        plan.authorize_ordinary_launch(&frontend, binary.to_path_buf())?;
        let mut references = Vec::new();
        for index in 0..cluster_size {
            let reference = plan.reserve_launch(RoleSlot::Backend(index))?;
            plan.authorize_launch(
                &reference,
                vec![DomainGrant {
                    domain: "native-observation".into(),
                    grant_id: format!("exchange-q1-be{index}"),
                    parameters: json!({"action":"exchange-snapshot", "root":root,
                    "backend_index":index, "target_session_id":reference.session_id,
                    "max_events":16384, "query_execution":{
                        "query_id_hi":identity.0.hi, "query_id_lo":identity.0.lo, "attempt_id":1}}),
                }],
                vec![
                    ("novarocks-server".into(), "test-controls".into()),
                    ("novarocks-native-adapter".into(), "test-controls".into()),
                ],
            )?;
            plan.set_launch_binary(&reference, binary.to_path_buf())?;
            references.push(reference);
        }
        plan.validate_selected_intents(cluster_size)?;
        *owner = Some(FrozenExchangeObservation {
            identity,
            references,
            deadline,
            selected: false,
        });
        Ok(plan)
    }

    fn take_identity(&self, deadline: Instant) -> Result<(common::UniqueId, FrontendProcessId)> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("exchange observation owner poisoned"))?;
        let frozen = owner
            .as_mut()
            .context("exchange observation was not frozen before launch")?;
        ensure!(!frozen.selected, "exchange identity already selected");
        ensure!(
            frozen.deadline == deadline && Instant::now() < deadline,
            "exchange original deadline changed or expired"
        );
        frozen.selected = true;
        Ok(frozen.identity)
    }

    fn verify_backend(&self, index: usize, actual: BackendProcessId) -> Result<()> {
        let owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("exchange observation owner poisoned"))?;
        let frozen = owner
            .as_ref()
            .context("exchange observation was not frozen")?;
        ensure!(
            frozen.selected && Instant::now() < frozen.deadline,
            "exchange identity is not live and selected"
        );
        let reference = frozen
            .references
            .get(index)
            .context("exchange backend is outside the finite plan")?;
        let receipt = RoleProcessReceipt::read_private(&reference.process_receipt)?;
        ensure!(
            receipt.session_id == reference.session_id && receipt.backend_process()? == actual,
            "exchange observation differs from its declared backend session/process"
        );
        Ok(())
    }
}

impl Scenario for SameProducerNormalClose {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "exchange_normal_close",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        if self.multicast {
            "task-execution/multicast-normal-close-keeps-sibling"
        } else {
            "task-execution/same-edge-normal-close-keeps-sibling"
        }
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        Ok(Some(self.observation.freeze(
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "exchange evidence requires native 1FE+3BE"
        );
        let baseline = resource_snapshot(context)?;
        let mut sessions = Vec::new();
        let result = (|| -> Result<()> {
            let (query_id, frontend) = self.observation.take_identity(context.deadline())?;
            sessions.push(RawBackendSession::establish_declared(
                context, 0, query_id, frontend,
            )?);
            self.observation.verify_backend(0, sessions[0].backend)?;
            sessions.push(RawBackendSession::establish_same_attempt(
                context,
                1,
                &sessions[0],
            )?);
            sessions.push(RawBackendSession::establish_same_attempt(
                context,
                2,
                &sessions[0],
            )?);
            self.observation.verify_backend(1, sessions[1].backend)?;
            self.observation.verify_backend(2, sessions[2].backend)?;
            let a = &sessions[0];
            let b = &sessions[1];
            let p = &sessions[2];
            let producer_base = RawCreate::values(p, 3);
            let producer_identity = proto::TaskIdentity {
                query_execution_id: producer_base.context.query_execution_id,
                stage_id: 2,
                task_id: 3,
                backend_process_id: producer_base.context.backend_process_id.clone(),
            };
            let producer_finst = common::UniqueId {
                hi: 0x5b,
                lo: producer_base.kernel_key_low,
            };
            let receiver_a = receiver(
                a,
                1,
                false,
                &producer_identity,
                producer_finst,
                self.multicast,
            )?;
            let receiver_b = receiver(
                b,
                2,
                true,
                &producer_identity,
                producer_finst,
                self.multicast,
            )?;
            let producer = producer(
                &producer_base,
                producer_identity,
                producer_finst,
                &receiver_a,
                &receiver_b,
                context,
                self.multicast,
            )?;
            // Submit the actual multicast package first so an unsupported sink
            // is reported by the compiler, rather than a fabricated verdict.
            if self.multicast {
                producer.accept_and_await_installed(context, p)?;
            }
            receiver_a.accept_and_await_installed(context, a)?;
            receiver_b.accept_and_await_installed(context, b)?;
            if !self.multicast {
                producer.accept_and_await_installed(context, p)?;
            }
            context.action(format!(
                "froze attempt={:?} P={:?} finst={producer_finst:?} A={:?} B={:?}; P Installed with edges Closed; multicast={}",
                producer.identity.query_execution_id, producer.identity, receiver_a.identity, receiver_b.identity, self.multicast,
            ));
            context.action(if self.multicast {
                "frozen P edge 1 -> exact A; P edge 2 -> exact B; both edges use the same P source finst and sender ordinal 0/count 1"
            } else {
                "frozen P edge 1 -> exact A and B; both destinations use the same P source finst and sender ordinal 0/count 1"
            });
            let cancel = session_control(
                a,
                proto::task_control_operation::Control::CancelTask(proto::CancelTaskRequest {
                    identity: Some(receiver_a.identity.clone()),
                    reason: proto::TaskCancelReason::UpstreamNoLongerNeeded as i32,
                }),
            )?;
            ensure!(
                matches!(
                    outcome(&cancel)?,
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::Idempotent
                ),
                "exact normal Cancel failed: {cancel:?}"
            );
            // RawApplication declares this Task no longer needs input. This
            // is an explicit test authority, not a business unknown-A source.
            observe_task_terminal_and_stopped(
                context,
                a,
                &receiver_a.identity,
                proto::TaskState::Canceled,
            )?;
            let edge_ids = if self.multicast { vec![1, 2] } else { vec![1] };
            let open = p.apply(
                proto::TaskOperation {
                    envelope: Some(raw_operation_envelope(1_000)),
                    operation: Some(proto::task_operation::Operation::UpdateTask(
                        proto::UpdateTaskRequest {
                            identity: Some(producer.identity.clone()),
                            domains: vec![proto::TaskDomainUpdate {
                                domain: Some(proto::task_domain_update::Domain::OpenExchangeEdges(
                                    proto::OpenExchangeEdgesDomain {
                                        version: 1,
                                        edge_ids,
                                    },
                                )),
                            }],
                        },
                    )),
                },
                "open the frozen producer after A actually stopped",
            )?;
            ensure!(
                matches!(
                    outcome(&open)?,
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::Idempotent
                ),
                "producer open failed: {open:?}"
            );
            let rows = fetch_rows(context, b, &receiver_b.identity)?;
            ensure!(
                rows == ROWS,
                "B lost or changed rows after A closed: {rows:?}"
            );
            await_normal_close_marker(context, p, &receiver_a, &producer)?;
            observe_task_terminal_and_stopped(
                context,
                b,
                &receiver_b.identity,
                proto::TaskState::Finished,
            )?;
            observe_task_terminal_and_stopped(
                context,
                p,
                &producer.identity,
                proto::TaskState::Finished,
            )?;
            context.action(format!("same frozen P received A's validated typed normal-close reply; B received exact BIGINT rows {rows:?} plus EOS; P and B Finished and actually stopped"));
            Ok(())
        })();
        // A failed fixture may still own a Closed producer or waiting B.
        // Abort first drives those live Tasks down; Quiesce alone only fences
        // admission. Successful evidence keeps the normal Release path.
        let mut cleanup_error = None;
        if result.is_err() {
            for session in &sessions {
                if let Err(error) = session_control(
                    session,
                    proto::task_control_operation::Control::AbortQueryContext(
                        proto::AbortQueryContextRequest {
                            query_context: Some(session.context_under(session.frontend, 1)),
                            cause: proto::QueryContextAbortCause::QueryFailed as i32,
                        },
                    ),
                ) {
                    cleanup_error.get_or_insert(error);
                }
            }
        }
        for session in &sessions {
            if let Err(error) = session.release_when_ready(context) {
                cleanup_error.get_or_insert(error);
            }
        }
        result?;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        await_resource_convergence(context, &baseline)?;
        Ok(())
    }
}

struct FrozenTask {
    operation: proto::TaskOperation,
    identity: proto::TaskIdentity,
    finst: common::UniqueId,
}

impl FrozenTask {
    fn accept_and_await_installed(
        &self,
        context: &mut ScenarioContext,
        session: &RawBackendSession,
    ) -> Result<()> {
        let accepted = session.apply(self.operation.clone(), "create exact exchange fixture")?;
        ensure!(
            outcome(&accepted)? == proto::TaskOperationOutcome::Accepted,
            "fixture Create was not Accepted: {accepted:?}"
        );
        loop {
            let receipt = session.apply(
                self.operation.clone(),
                "observe frozen fixture installation",
            )?;
            let Some(proto::task_operation_verdict::Ack::CreateTask(ack)) =
                receipt_verdict(&receipt)?.ack.clone()
            else {
                bail!("Create replay omitted ack: {receipt:?}");
            };
            let status = ack
                .current_status
                .context("Create omitted current status")?;
            ensure!(
                status.identity.as_ref() == Some(&self.identity),
                "Create changed frozen identity: {status:?}"
            );
            ensure!(
                !matches!(
                    proto::TaskState::try_from(status.state)?,
                    proto::TaskState::Failed
                        | proto::TaskState::Aborted
                        | proto::TaskState::Canceled
                ),
                "fixture task failed before normal close: {status:?}"
            );
            if status.installed == Some(true) {
                return Ok(());
            }
            thread::sleep(
                context
                    .remaining("await exchange fixture Installed")?
                    .min(POLL),
            );
        }
    }
}

fn carriers(base: &RawCreate) -> Result<(proto::FrozenFragment, proto::CreationMetadata)> {
    let operation = base.operation();
    let Some(proto::task_operation::Operation::CreateTask(create)) = operation.operation else {
        unreachable!()
    };
    Ok((
        proto::FrozenFragment::decode(create.frozen_fragment)?,
        proto::CreationMetadata::decode(create.creation_metadata)?,
    ))
}

fn freeze(frozen: proto::FrozenFragment, metadata: proto::CreationMetadata) -> FrozenTask {
    let descriptor = metadata.descriptor.as_ref().expect("fixture descriptor");
    FrozenTask {
        identity: descriptor.identity.clone().expect("fixture identity"),
        finst: descriptor.fragment_instance_id.expect("fixture finst"),
        operation: proto::TaskOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            operation: Some(proto::task_operation::Operation::CreateTask(
                proto::CreateTaskRequest {
                    frozen_fragment: frozen.encode_to_vec().into(),
                    creation_metadata: metadata.encode_to_vec().into(),
                },
            )),
        },
    }
}

fn receiver(
    session: &RawBackendSession,
    id: u32,
    result: bool,
    source: &proto::TaskIdentity,
    source_finst: common::UniqueId,
    multicast: bool,
) -> Result<FrozenTask> {
    let mut base = RawCreate::values(session, id);
    base.pipeline_dop = 1;
    base.exchange_input = true;
    let (_, mut metadata) = carriers(&base)?;
    let fragment_id = if multicast && result { 3 } else { 1 };
    let edge = if multicast && result { 2 } else { 1 };
    let frozen = compiled_fragment(
        fragment_id,
        1,
        Some((edge, multicast)),
        if result {
            RawSink::Result
        } else {
            RawSink::CountOnly
        },
        &[],
    );
    metadata
        .descriptor
        .as_mut()
        .expect("fixture descriptor")
        .identity
        .as_mut()
        .expect("fixture identity")
        .stage_id = fragment_id;
    let inbound = &mut metadata
        .descriptor
        .as_mut()
        .expect("fixture descriptor")
        .topology
        .as_mut()
        .expect("fixture topology")
        .inbound[0];
    inbound.sources = vec![proto::TaskExchangeSource {
        task: Some(source.clone()),
        fragment_instance_id: Some(source_finst),
        sender_ordinal: 0,
    }];
    Ok(freeze(frozen, metadata))
}

fn producer(
    base: &RawCreate,
    identity: proto::TaskIdentity,
    finst: common::UniqueId,
    a: &FrozenTask,
    b: &FrozenTask,
    context: &mut ScenarioContext,
    multicast: bool,
) -> Result<FrozenTask> {
    let mut base = base.clone();
    base.pipeline_dop = 1;
    let (_, mut metadata) = carriers(&base)?;
    let frozen = compiled_fragment(2, 1, None, RawSink::Stream { multicast }, &ROWS);
    let descriptor = metadata.descriptor.as_mut().expect("fixture descriptor");
    descriptor.identity = Some(identity);
    descriptor.fragment_instance_id = Some(finst);
    let mut destination = |task: &FrozenTask, index: usize| -> proto::TaskExchangeDestination {
        proto::TaskExchangeDestination {
            task: Some(task.identity.clone()),
            fragment_instance_id: Some(task.finst),
            destination_node_id: 10,
            endpoint: Some(proto::QueryControlEndpoint {
                host: "127.0.0.1".into(),
                port: u32::from(context.handle().runtime().be[index].grpc),
            }),
        }
    };
    let edge = |edge_id, destinations| proto::TaskExchangeEdge {
        edge_id,
        destination_node_id: 10,
        partitioning: proto::ExchangePartitioning::Unpartitioned as i32,
        destinations,
        sender_ordinal: 0,
        sender_count: 1,
    };
    descriptor
        .topology
        .as_mut()
        .expect("fixture topology")
        .outbound = if multicast {
        vec![
            edge(1, vec![destination(a, 0)]),
            edge(2, vec![destination(b, 1)]),
        ]
    } else {
        vec![edge(1, vec![destination(a, 0), destination(b, 1)])]
    };
    metadata
        .assignment
        .as_mut()
        .expect("fixture assignment")
        .sink_edge_ids = if multicast { vec![1, 2] } else { vec![1] };
    Ok(freeze(frozen, metadata))
}

fn fetch_rows(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    identity: &proto::TaskIdentity,
) -> Result<Vec<i64>> {
    let mut rows = Vec::new();
    let mut sequence = 0;
    let mut acknowledged_packet_sequence = None;
    let mut accepted_eos = false;
    loop {
        context.remaining("fetch exact B data and EOS")?;
        let response: proto::FetchResultResponse = raw_unary(
            session.connector.clone(),
            "/novarocks.NovaRocksGrpc/FetchTaskResult",
            &session.authorization,
            proto::FetchTaskResultRequest {
                root_task: Some(identity.clone()),
                max_wait_millis: 100,
                acknowledged_packet_sequence,
                max_result_bytes: 1024 * 1024,
            },
            context.deadline(),
        )?;
        ensure!(
            response.status != proto::fetch_result_response::Status::Error as i32,
            "B result failed: {}",
            response.message
        );
        if response.status == proto::fetch_result_response::Status::NotReady as i32 {
            continue;
        }
        if response.status == proto::fetch_result_response::Status::Eof as i32 {
            ensure!(
                accepted_eos
                    && response.eos
                    && response.result_arrow_ipc.is_empty()
                    && acknowledged_packet_sequence == Some(u64::try_from(response.packet_seq)?),
                "B EOF did not settle the exact acknowledged EOS packet: {response:?}"
            );
            return Ok(rows);
        }
        ensure!(
            !accepted_eos && response.status == proto::fetch_result_response::Status::Ready as i32,
            "B returned an invalid result status: {response:?}"
        );
        ensure!(
            response.packet_seq == sequence,
            "B packet sequence changed: expected {sequence}, got {}",
            response.packet_seq
        );
        sequence += 1;
        if !response.result_arrow_ipc.is_empty() {
            // Decode the public NRX1 result envelope independently of the
            // execution implementation, then use the standard Arrow client.
            let wire = response.result_arrow_ipc.as_ref();
            ensure!(
                wire.len() >= 14 && &wire[..4] == b"NRX1",
                "B result envelope is missing"
            );
            ensure!(
                wire[4] == 1 && wire[5] == 1,
                "B result envelope version or flags differ"
            );
            ensure!(
                u32::from_le_bytes(wire[6..10].try_into()?) == 1,
                "B returned another schema"
            );
            ensure!(
                // LocalCompiler allocates the sole receiver channel from slot zero.
                u32::from_le_bytes(wire[10..14].try_into()?) == 0,
                "B result slot differs"
            );
            let chunks =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(&wire[14..]), None)?;
            for chunk in chunks {
                let chunk = chunk?;
                ensure!(chunk.columns().len() == 1, "B returned another schema");
                let values = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("B column is not BIGINT")?;
                ensure!(values.null_count() == 0, "B introduced NULLs");
                rows.extend(values.values().iter().copied());
            }
        }
        // Acknowledge only after exact sequence and typed payload validation.
        // The final EOS is also acknowledged before waiting on real stop.
        acknowledged_packet_sequence = Some(u64::try_from(response.packet_seq)?);
        accepted_eos = response.eos;
    }
}

fn await_normal_close_marker(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    a: &FrozenTask,
    p: &FrozenTask,
) -> Result<()> {
    loop {
        let log = context.handle().be_log_contents(session.index)?;
        let destination = format!(
            "dest_finst={} ",
            novarocks_types::UniqueId::new(a.finst.hi, a.finst.lo)
        );
        let source = format!(
            "sender_finst={} ",
            novarocks_types::UniqueId::new(p.finst.hi, p.finst.lo)
        );
        let query = a
            .identity
            .query_execution_id
            .expect("fixture execution")
            .query_id
            .expect("fixture query");
        let process = novarocks_types::identity::BackendProcessId::try_from_bytes(
            a.identity
                .backend_process_id
                .as_ref()
                .expect("fixture backend")
                .value
                .as_slice()
                .try_into()
                .expect("backend UUID width"),
        )
        .map_err(anyhow::Error::msg)?;
        let task = format!(
            "destination_task={}/stage-{}/task-{}@{} ",
            novarocks_types::UniqueId::new(query.hi, query.lo),
            a.identity.stage_id,
            a.identity.task_id,
            process
        );
        let attempt = format!(
            "attempt={} ",
            a.identity
                .query_execution_id
                .expect("fixture execution")
                .attempt_id
        );
        if log.lines().any(|line| {
            line.contains("event=normal_close_validated")
                && line.contains(&destination)
                && line.contains(&source)
                && line.contains(&task)
                && line.contains(&attempt)
                && line.contains("node_id=10 ")
                && line.contains("sender_ordinal=0 ")
                && line.contains("sender_count=1 ")
        }) {
            return Ok(());
        }
        thread::sleep(
            context
                .remaining("observe P Native validated A normal-close proof")?
                .min(POLL),
        );
    }
}

#[cfg(test)]
mod observation_authorization_tests {
    use super::*;

    #[test]
    fn exchange_request_identity_is_predeclared_once_and_each_process_is_exact() {
        let owner = ExchangeObservationOwner::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        let binary = std::env::current_exe().unwrap();
        let plan = owner.freeze(3, &binary, deadline).unwrap();
        plan.validate_selected_intents(3).unwrap();
        let identity = owner.0.lock().unwrap().as_ref().unwrap().identity;
        assert!(
            owner
                .take_identity(deadline + Duration::from_millis(1))
                .is_err()
        );
        assert_eq!(owner.take_identity(deadline).unwrap(), identity);
        assert!(owner.take_identity(deadline).is_err());
        assert!(owner.freeze(3, &binary, deadline).is_err());
        let references = owner.0.lock().unwrap().as_ref().unwrap().references.clone();
        for (index, reference) in references.iter().enumerate() {
            let process = BackendProcessId::new_v7();
            RoleProcessReceipt {
                schema_version: 1,
                session_id: reference.session_id.clone(),
                launch_id: format!("original-be{index}"),
                role: novarocks_failpoint::session::SessionRole::Be,
                process_id: process.to_string(),
            }
            .write_private(&reference.process_receipt)
            .unwrap();
            assert!(owner.verify_backend(index, process).is_ok());
            assert!(
                owner
                    .verify_backend(index, BackendProcessId::new_v7())
                    .is_err()
            );
        }
        assert!(owner.verify_backend(3, BackendProcessId::new_v7()).is_err());
    }

    #[test]
    fn normal_close_producer_keeps_original_bigint_source_and_exact_constant_references() {
        use arrow_schema::{DataType, Field};
        use novarocks_physical_plan::{ConstantPoolId, ExprKind, FragmentId, NodeKind};
        use novarocks_plan_codec::{
            physical_package_v2::{PackageDecodeLimits, decode_fragment_package},
            resource_preflight_v2::FragmentDecodeResourceModel,
        };
        use novarocks_type_contract::{
            CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
        };
        struct FixtureCompileControl(Instant);
        impl PureCompileControl for FixtureCompileControl {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                if Instant::now() >= self.0 {
                    Err(CompileControlError::DeadlineExceeded)
                } else {
                    Ok(())
                }
            }
        }
        let control = FixtureCompileControl(Instant::now() + Duration::from_secs(10));
        let model = FragmentDecodeResourceModel::try_new(&control).unwrap();
        for multicast in [false, true] {
            // Decode the same checked compiled-only source the scenario sends.
            // No legacy tree or second, handwritten constant namespace is used.
            let frozen = compiled_fragment(2, 1, None, RawSink::Stream { multicast }, &ROWS);
            let wire = novarocks_proto_models::physical_package_v2::FragmentPackage::decode(
                frozen.package.as_ref(),
            )
            .unwrap();
            let package = decode_fragment_package(
                frozen.package.as_ref(),
                &model,
                &PackageDecodeLimits::FROZEN,
                &control,
            )
            .unwrap();
            let ty = FunctionValueType::new(DataType::Int64, false);
            let fragment = package.fragment();
            assert_eq!(fragment.id(), FragmentId::new(2));
            assert_eq!(fragment.dop_domain().min, 1);
            assert_eq!(fragment.dop_domain().max, 1);
            let root = fragment.root();
            let node = &fragment.nodes()[&root];
            let [output] = node.output.columns.as_ref() else {
                panic!("the original producer has one BIGINT output");
            };
            assert_eq!(fragment.values()[output].ty, ty);
            assert!(package.calls().entries().is_empty());
            assert_eq!(package.cuts().outbound.len(), if multicast { 2 } else { 1 });
            let fields = wire.source_fields.as_ref().unwrap();
            assert!(fields.shape.is_some());
            assert_eq!(fields.field_facts.len(), 1);
            assert!(
                wire.fragment
                    .as_ref()
                    .unwrap()
                    .call_requests
                    .as_ref()
                    .unwrap()
                    .entries
                    .is_empty()
            );
            assert!(wire.calls.as_ref().unwrap().entries.is_empty());
            let [record] = wire.constants.as_slice() else {
                panic!("the original five rows must share one actual constant pool");
            };
            assert_eq!(record.id, 0);
            assert_eq!(fields.field_facts[0].field_id, record.source_field_id);
            assert!(record.value_type_id.is_some());
            assert!(record.source_field_id.is_some());
            let mut reader = arrow_ipc::reader::StreamReader::try_new(
                std::io::Cursor::new(&record.arrow_ipc[..]),
                None,
            )
            .unwrap();
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(
                batch.schema().field(0),
                &Field::new("v", DataType::Int64, false)
            );
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.null_count(), 0);
            assert_eq!(values.values().as_ref(), &ROWS);
            assert!(reader.next().is_none());
            let pools = package.constants().entries();
            assert_eq!(pools.len(), 1);
            let pool = &pools[&ConstantPoolId::new(record.id)];
            assert_eq!(pool.value_type(), &ty);
            assert_eq!(
                pool.field_ref().as_ref(),
                &Field::new("v", DataType::Int64, false)
            );
            let NodeKind::Values { rows } = &node.kind else {
                panic!("the exact producer must remain an actual VALUES source");
            };
            assert_eq!(rows.len(), ROWS.len());
            for (ordinal, row) in rows.iter().enumerate() {
                let [id] = row.as_ref() else {
                    panic!("each original row has one BIGINT expression");
                };
                let value = fragment.expressions().get(*id).unwrap();
                assert_eq!(value.owner, root);
                assert_eq!(value.ty, ty);
                let ExprKind::Constant(reference) = &value.kind else {
                    panic!("the typed v2 decoder must keep the actual admitted constant reference");
                };
                assert_eq!(reference.pool, ConstantPoolId::new(record.id));
                assert_eq!(reference.ordinal, u32::try_from(ordinal).unwrap());
            }
        }
    }
}
