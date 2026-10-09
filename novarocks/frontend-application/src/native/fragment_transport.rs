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

//! Native task-result transport and submission DTO.
//!
//! Task observation reads and the bounded V1 root relay address the exact
//! task and process frozen by this attempt. Root bodies stay encoded until
//! their admitted domain owner consumes them.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::domain::DomainVersion;
use novarocks_execution::task_execution::operation::FetchTaskDynamicFilters;
use novarocks_execution::task_execution::{
    FinalTaskInfo, OperationOutcome, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::BackendProcessDescriptor;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_query_application::{
    api::{QueryExecutionError, QueryExecutionErrorKind},
    coordination::{AttemptFailureClass, RootResultFetchFailure},
};
use novarocks_task_codec::operation::{
    decode_operation_outcome, encode_fetch_dynamic_filters, encode_get_final_task_info,
};
use novarocks_task_codec::status::decode_final_task_info;
use novarocks_types::identity::BackendProcessId;

use crate::runtime_filter::feedback::TaskRuntimeFilterFeedback;

use super::data_runtime::FrontendDataRuntime;
use super::transport::Client;

/// What a final task info read answered.
///
/// Losing final info costs diagnostics only, so absence is a reported category
/// rather than an error: success and failure are decided by `TaskStatus`
/// alone.
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
pub enum FinalTaskInfoRead {
    Available(FinalTaskInfo),
    Unavailable(OperationOutcome),
}

/// The longest one dynamic filter read may hold the coordinator's turn.
///
/// The read is answered immediately by the backend -- it is a projection of a
/// retained payload, not a long poll -- so this only bounds a backend that has
/// stopped answering. It has to be bounded here because the request carries no
/// wait field of its own, and the same thread that makes this call also settles
/// acknowledgements and opens exchange edges: an unbounded read would stop the
/// whole attempt to chase a pruning optimization.
const DYNAMIC_FILTER_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// How much longer than the wait it asked for a task-addressed read may take
/// before the backend is treated as having stopped answering.
///
/// A root result poll tells the backend how long it may block, so an answer
/// that has not arrived by that wait plus this allowance is not a slow answer,
/// it is no answer. Bounding it is not an optimization: the coordinator thread
/// that makes this call is the same one that settles acknowledgements, folds
/// status and opens exchange edges, so an unbounded call stops the whole
/// attempt -- past its own statement deadline, silently, with no fact named.
///
/// The allowance is the transport's own frontend queue residence rather than a
/// number invented here. That is already how long this attempt lets a released
/// operation sit before it calls it lost, and a second number would put two
/// answers on one question.
#[derive(Copy, Clone, Debug)]
pub(crate) struct TaskReadGrace(std::time::Duration);

impl TaskReadGrace {
    pub(crate) const fn new(grace: std::time::Duration) -> Self {
        Self(grace)
    }

    /// The deadline for a read that asked the backend to block for `wait`.
    fn deadline_for(self, wait: std::time::Duration) -> std::time::Duration {
        wait.saturating_add(self.0)
    }
}

/// What one dynamic filter read answered.
///
/// A version of `None` is the settled "nothing to fetch": either the task has
/// advertised nothing at all, or it went terminal and no longer retains the
/// payload it advertised. Both are answers, not failures -- the split source's
/// own wait cap degrades to unpruned enumeration -- so neither is an error
/// here.
#[derive(Clone, Debug)]
pub struct DynamicFilterRead {
    version: Option<DomainVersion>,
    feedback: Vec<TaskRuntimeFilterFeedback>,
}

impl DynamicFilterRead {
    /// The transport builds this from a decoded response; only a test that
    /// scripts a read builds one directly.
    #[cfg(test)]
    pub(crate) const fn new(
        version: Option<DomainVersion>,
        feedback: Vec<TaskRuntimeFilterFeedback>,
    ) -> Self {
        Self { version, feedback }
    }

    pub const fn version(&self) -> Option<DomainVersion> {
        self.version
    }

    pub fn feedback(&self) -> &[TaskRuntimeFilterFeedback] {
        &self.feedback
    }
}

/// Why one dynamic filter read produced no answer.
///
/// The two are acted on differently and must not be collapsed. `Unavailable`
/// means the read did not complete -- the next turn asks again, and the split
/// source keeps waiting inside its own cap. `Refused` means the backend
/// answered that the request itself is not legal against the task it names,
/// which is a real disagreement between this frontend's view of the task and
/// the backend's, and is never repaired by asking again.
#[derive(Clone, Debug)]
pub enum DynamicFilterReadError {
    Unavailable(String),
    Refused(String),
}

impl fmt::Display for DynamicFilterReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(detail) | Self::Refused(detail) => formatter.write_str(detail),
        }
    }
}

/// The task protocol's terminal-info and dynamic-filter observations.
///
/// Both are addressed by exact [`TaskIdentity`]; none creates a task,
/// advances a status, or renews a lease.
#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
pub trait TaskResultTransport: Send + Sync + 'static {
    /// Reads one terminal task's bounded final info.
    fn final_task_info(&self, identity: TaskIdentity) -> Result<FinalTaskInfoRead, String>;

    /// Reads whatever one task advertised above the reader's own cursor.
    fn dynamic_filters(
        &self,
        identity: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError>;
}

/// The native implementation over one frozen backend process set.
///
/// The set is frozen at construction from one live topology snapshot, exactly
/// as the operation transport freezes its own: a task identity naming a
/// process that is not in it is not looked up elsewhere, because a replaced
/// process is a different process.
#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
pub(crate) struct NativeTaskResultTransport {
    clients: BTreeMap<BackendProcessId, Client>,
    endpoints: BTreeMap<BackendProcessId, RuntimeEndpoint>,
    data_runtime: FrontendDataRuntime,
    grace: TaskReadGrace,
}

impl fmt::Debug for NativeTaskResultTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeTaskResultTransport")
            .field("backends", &self.clients.len())
            .finish_non_exhaustive()
    }
}

#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
impl NativeTaskResultTransport {
    pub(crate) fn new(
        backends: &[BackendProcessDescriptor],
        data_runtime: FrontendDataRuntime,
        grace: TaskReadGrace,
    ) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("the root result transport requires at least one backend".to_owned());
        }
        let mut clients = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        for descriptor in backends {
            let process_id = descriptor.process_id();
            let client = Client::for_backend(descriptor.clone(), data_runtime.clone());
            if clients.insert(process_id, client).is_some() {
                return Err(format!("duplicate backend process {process_id}"));
            }
            endpoints.insert(process_id, descriptor.endpoint().clone());
        }
        Ok(Self {
            clients,
            endpoints,
            data_runtime,
            grace,
        })
    }

    fn client_of(&self, identity: TaskIdentity) -> Result<(&Client, String), String> {
        let process = identity.backend_process_id();
        let client = self.clients.get(&process).ok_or_else(|| {
            format!("task {identity} names a backend process this attempt did not freeze")
        })?;
        let address = self.endpoints[&process].to_string();
        Ok((client, address))
    }
}

/// What a `tonic::Status` is hiding, which its own `Display` never prints.
///
/// The classification below already reads `source()` to tell an HTTP/2
/// failure from a service-readiness one, and then the message threw it away.
/// So a broken connection reached every log and every client as the fixed
/// text `transport error`, and the io or h2 error that actually broke it --
/// a reset, a refused connection, a TLS alert -- was not recoverable from
/// outside the process. Whoever hits this next should not have to guess.
///
/// Transport-layer sources carry addresses and syscall errors, never
/// credential material; the status message itself is unchanged, so nothing
/// that was already redacted becomes visible here.
fn transport_error_chain(error: &tonic::Status) -> String {
    /// Enough to reach the io error under tonic and h2, bounded so a cyclic
    /// or pathological chain cannot turn one diagnostic into a flood.
    const MAX_DEPTH: usize = 8;

    let mut chain = String::new();
    let mut source = std::error::Error::source(error);
    for _ in 0..MAX_DEPTH {
        let Some(current) = source else {
            break;
        };
        chain.push_str(&format!("; caused by: {current}"));
        source = current.source();
    }
    if source.is_some() {
        chain.push_str("; caused by: ...");
    }
    chain
}

fn classify_fetch_task_result_rpc_status(error: tonic::Status) -> NativeRootResultFetchError {
    let unknown_is_transport = error.code() == tonic::Code::Unknown
        && (std::error::Error::source(&error).is_some()
            || error.message().starts_with("Service was not ready: "));
    // A backend that stops gracefully sends GOAWAY, and the streams it cuts
    // arrive here as `Cancelled` carrying the transport error that caused it.
    // That is the same fact as the torn socket this used to see as `Unknown`:
    // the endpoint could not complete the request. Reading it any other way
    // would make an orderly stop less recoverable than an abrupt one. The
    // source is required for the same reason it is required above -- a
    // source-less remote `Cancelled` is the peer's own answered refusal.
    let cancelled_is_transport =
        error.code() == tonic::Code::Cancelled && std::error::Error::source(&error).is_some();
    let detail = format!(
        "fetch_task_result rpc failed: {error}{}",
        transport_error_chain(&error)
    );
    match error.code() {
        // These statuses state that the exact backend endpoint or its HTTP/2
        // transport could not complete the request in this attempt's bounded
        // transport window.
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            NativeRootResultFetchError::infrastructure(detail)
        }
        tonic::Code::Unknown if unknown_is_transport => {
            NativeRootResultFetchError::infrastructure(detail)
        }
        tonic::Code::Cancelled if cancelled_is_transport => {
            NativeRootResultFetchError::infrastructure(detail)
        }
        // A peer that explicitly refuses work for capacity reasons has made a
        // resource-governance decision rather than disappearing.
        tonic::Code::ResourceExhausted => NativeRootResultFetchError::resource_governance(detail),
        // The FetchResult response carries every application-owned refusal in
        // band. Tonic's generated client maps service-readiness failures to a
        // source-less Unknown with a fixed generated prefix; HTTP/2 failures
        // retain an error source. A source-less remote Unknown is ambiguous and
        // therefore remains a contract failure. Every other gRPC status is an
        // answered protocol, identity, authorization, or server-contract
        // refusal. Attempt execution failure is learned from the accepted Task
        // status projection, never inferred from this transport status.
        _ => NativeRootResultFetchError::contract(detail),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeRootResultFetchErrorClass {
    Infrastructure,
    ResourceGovernance,
    ContractViolation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeRootResultFetchError {
    class: NativeRootResultFetchErrorClass,
    detail: String,
}

impl NativeRootResultFetchError {
    fn infrastructure(detail: impl Into<String>) -> Self {
        Self {
            class: NativeRootResultFetchErrorClass::Infrastructure,
            detail: detail.into(),
        }
    }

    fn resource_governance(detail: impl Into<String>) -> Self {
        Self {
            class: NativeRootResultFetchErrorClass::ResourceGovernance,
            detail: detail.into(),
        }
    }

    fn contract(detail: impl Into<String>) -> Self {
        Self {
            class: NativeRootResultFetchErrorClass::ContractViolation,
            detail: detail.into(),
        }
    }

    fn into_pump_failure_for_backend(
        self,
        backend: novarocks_types::identity::BackendProcessId,
    ) -> RootResultFetchFailure {
        let requirement = if self.class == NativeRootResultFetchErrorClass::Infrastructure {
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                backend,
            )
        } else {
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        };
        self.into_pump_failure()
            .with_topology_requirement(requirement)
    }

    fn into_pump_failure(self) -> RootResultFetchFailure {
        let (class, kind) = match self.class {
            NativeRootResultFetchErrorClass::Infrastructure => (
                AttemptFailureClass::RecoverableInfrastructure,
                QueryExecutionErrorKind::Failed,
            ),
            NativeRootResultFetchErrorClass::ResourceGovernance => (
                AttemptFailureClass::ResourceGovernance,
                QueryExecutionErrorKind::Rejected,
            ),
            NativeRootResultFetchErrorClass::ContractViolation => (
                AttemptFailureClass::ContractViolation,
                QueryExecutionErrorKind::InvalidRequest,
            ),
        };
        RootResultFetchFailure::new(class, QueryExecutionError::new(kind, self.detail))
    }
}

impl fmt::Display for NativeRootResultFetchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for NativeRootResultFetchError {}

impl TaskResultTransport for NativeTaskResultTransport {
    fn final_task_info(&self, identity: TaskIdentity) -> Result<FinalTaskInfoRead, String> {
        let (client, address) = self.client_of(identity)?;
        let request = encode_get_final_task_info(identity);
        // Final info is a projection of a retained record, answered
        // immediately, so this bound only covers a backend that stopped
        // answering. It has to be bounded for the same reason as the poll
        // above, and more sharply: this read runs inside the attempt's drain,
        // whose whole point is not to hold a client-visible completion.
        let deadline = self.grace.deadline_for(std::time::Duration::ZERO);
        let response = self.data_runtime.block_on(async {
            let expires_at = tokio::time::Instant::now() + deadline;
            let mut grpc = tokio::time::timeout_at(
                expires_at,
                client.grpc_with_channel_error(NativeRpcMethod::GetFinalTaskInfo),
            )
            .await
            .map_err(|_| {
                format!(
                    "{address}: final info read for task {identity} could not acquire a \
                         channel within {deadline:?}"
                )
            })?
            .map_err(|error| error.to_string())?;
            tokio::time::timeout_at(expires_at, grpc.get_final_task_info(request))
                .await
                .map_err(|_| {
                    format!(
                        "{address}: final info read for task {identity} did not answer within \
                         {deadline:?}"
                    )
                })?
                .map(tonic::Response::into_inner)
                .map_err(|error| format!("get_final_task_info rpc failed: {error}"))
        })??;
        let result = response
            .result
            .ok_or_else(|| format!("{address}: final info response carries no result"))?;
        match result {
            novarocks_proto_models::novarocks::get_final_task_info_response::Result::Info(info) => {
                decode_final_task_info(identity, &info, FieldPath::root("final_task_info"))
                    .map(FinalTaskInfoRead::Available)
                    .map_err(|error| format!("{address}: {error}"))
            }
            novarocks_proto_models::novarocks::get_final_task_info_response::Result::Unavailable(
                outcome,
            ) => decode_operation_outcome(outcome, FieldPath::root("final_task_info"))
                .map(FinalTaskInfoRead::Unavailable)
                .map_err(|error| format!("{address}: {error}")),
        }
    }

    fn dynamic_filters(
        &self,
        identity: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        let (client, address) = self
            .client_of(identity)
            .map_err(DynamicFilterReadError::Refused)?;
        // The operation identity is minted per call and never replayed: this
        // read creates nothing, so there is nothing for a replay to be
        // idempotent against.
        let request = encode_fetch_dynamic_filters(FetchTaskDynamicFilters::new(
            TaskOperationId::new_v7(),
            identity,
            acknowledged,
        ));
        let response = self
            .data_runtime
            .block_on(async {
                let expires_at = tokio::time::Instant::now() + DYNAMIC_FILTER_READ_TIMEOUT;
                let mut grpc = tokio::time::timeout_at(
                    expires_at,
                    client.grpc_with_channel_error(NativeRpcMethod::FetchTaskDynamicFilters),
                )
                .await
                .map_err(|_| {
                    DynamicFilterReadError::Unavailable(format!(
                        "{address}: dynamic filter read could not acquire a channel in time"
                    ))
                })?
                .map_err(|error| DynamicFilterReadError::Unavailable(error.to_string()))?;
                tokio::time::timeout_at(expires_at, grpc.fetch_task_dynamic_filters(request))
                    .await
                    .map_err(|_| {
                        DynamicFilterReadError::Unavailable(format!(
                            "{address}: dynamic filter read did not answer in time"
                        ))
                    })?
                    .map(tonic::Response::into_inner)
                    .map_err(|error| classify_dynamic_filter_status(&address, &error))
            })
            .map_err(DynamicFilterReadError::Unavailable)??;
        let answered = response
            .identity
            .as_ref()
            .map(|answered| {
                novarocks_task_codec::identity::decode_task_identity(
                    answered,
                    FieldPath::root("fetch_task_dynamic_filters").field("identity"),
                )
            })
            .transpose()
            .map_err(|error| DynamicFilterReadError::Refused(format!("{address}: {error}")))?
            .ok_or_else(|| {
                DynamicFilterReadError::Refused(format!(
                    "{address}: dynamic filter read answered without a task identity"
                ))
            })?;
        // A read that agrees with itself proves nothing. This is the identity
        // this frontend asked about, so an answer naming another task is
        // refused rather than admitted as that task's feedback.
        if answered != identity {
            return Err(DynamicFilterReadError::Refused(format!(
                "{address}: dynamic filter read for {identity} answered for {answered}"
            )));
        }
        let version = DomainVersion::new(response.version).ok();
        if version.is_none() && !response.domains.is_empty() {
            return Err(DynamicFilterReadError::Refused(format!(
                "{address}: dynamic filter read carries domains under version zero"
            )));
        }
        let mut feedback = Vec::with_capacity(response.domains.len());
        for domain in &response.domains {
            let envelope = domain.envelope.as_ref().ok_or_else(|| {
                DynamicFilterReadError::Refused(format!(
                    "{address}: dynamic filter domain carries no envelope"
                ))
            })?;
            feedback.push(
                TaskRuntimeFilterFeedback::parse(envelope).map_err(|error| {
                    DynamicFilterReadError::Refused(format!("{address}: {error}"))
                })?,
            );
        }
        Ok(DynamicFilterRead { version, feedback })
    }
}

/// The Frontend side of `FetchRootResult` for one frozen attempt: bounded
/// root reads relayed without decode. Each reply retains the caller's window
/// alias until its delivery exits. The result window granted at admission is
/// this read's carrier commitment, so no separate fetch permit is taken.
pub(crate) struct NativeBoundedRootReadPort {
    transport: Arc<NativeTaskResultTransport>,
    sealed: std::sync::atomic::AtomicBool,
}

impl NativeBoundedRootReadPort {
    pub(crate) fn new(transport: Arc<NativeTaskResultTransport>) -> Self {
        Self {
            transport,
            sealed: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl novarocks_query_application::api::BoundedRootReadPort for NativeBoundedRootReadPort {
    fn read(
        &self,
        request: novarocks_execution_contract::root_result::RootResultRead,
        physical_guard: novarocks_workload_control::ResultWindowAlias,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        novarocks_query_application::api::RetainedRootReply,
                        RootResultFetchFailure,
                    >,
                > + Send
                + 'static,
        >,
    > {
        let root = request.root_task();
        let backend = root.backend_process_id();
        let sealed = self.sealed.load(std::sync::atomic::Ordering::Acquire);
        let route = self
            .transport
            .client_of(root)
            .map(|(client, address)| (client.clone(), address));
        let grace = self.transport.grace;
        Box::pin(async move {
            if sealed {
                return Err(NativeRootResultFetchError::contract(format!(
                    "root reads for task {root} are sealed"
                ))
                .into_pump_failure());
            }
            let (client, address) = route
                .map_err(|error| NativeRootResultFetchError::contract(error).into_pump_failure())?;
            let wire = novarocks_task_codec::root_result::encode_read(&request);
            let wait = request.max_wait();
            let deadline = grace.deadline_for(wait);
            let expires_at = tokio::time::Instant::now() + deadline;
            let mut grpc = tokio::time::timeout_at(
                expires_at,
                client.grpc_with_channel_error(NativeRpcMethod::FetchRootResult),
            )
            .await
            .map_err(|_| {
                NativeRootResultFetchError::infrastructure(format!(
                    "{address}: root read for task {root} could not acquire a channel within \
                     {deadline:?}"
                ))
                .into_pump_failure_for_backend(backend)
            })?
            .map_err(|error| {
                NativeRootResultFetchError::infrastructure(error.to_string())
                    .into_pump_failure_for_backend(backend)
            })?;
            let response = tokio::time::timeout_at(expires_at, grpc.fetch_root_result(wire))
                .await
                .map_err(|_| {
                    NativeRootResultFetchError::infrastructure(format!(
                        "{address}: root read for task {root} did not answer within \
                         {deadline:?}; it was asked to wait at most {wait:?}"
                    ))
                    .into_pump_failure_for_backend(backend)
                })?
                .map(tonic::Response::into_inner)
                .map_err(|status| {
                    classify_fetch_task_result_rpc_status(status)
                        .into_pump_failure_for_backend(backend)
                })?;
            let reply = novarocks_task_codec::root_result::decode_reply(
                response,
                &request,
                request.consumed(),
                FieldPath::root("fetch_root_result"),
            )
            .map_err(|error| {
                NativeRootResultFetchError::contract(format!("{address}: {error}"))
                    .into_pump_failure()
            })?;
            // The decoded body and its message envelope live together until
            // the delivery exits; both are covered by the window grant.
            let live = match &reply.outcome {
                novarocks_execution_contract::root_result::RootReadOutcome::Data(data) => {
                    data.body().len() as u64
                }
                _ => 0,
            } + novarocks_result_contract::RootProfileV1::ENVELOPE_BYTES as u64;
            novarocks_query_application::api::RetainedRootReply::try_new(
                reply,
                physical_guard,
                live,
            )
            .map_err(|error| {
                NativeRootResultFetchError::contract(format!(
                    "{address}: root reply exceeds its window: {error}"
                ))
                .into_pump_failure()
            })
        })
    }

    fn seal(
        &self,
        _sealed: novarocks_execution_contract::root_lifetime::RootReadSealed,
    ) -> Result<(), QueryExecutionError> {
        self.sealed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

/// Classifies one dynamic filter read failure by type.
///
/// A status that leaves the read unfinished is `Unavailable`, and the next turn
/// asks again. Everything else is the backend having answered that the request
/// is illegal against the task it names, which asking again cannot repair.
fn classify_dynamic_filter_status(address: &str, status: &tonic::Status) -> DynamicFilterReadError {
    let detail = format!(
        "{address}: fetch_task_dynamic_filters rpc failed: {}",
        status.message()
    );
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::ResourceExhausted => DynamicFilterReadError::Unavailable(detail),
        _ => DynamicFilterReadError::Refused(detail),
    }
}

#[cfg(test)]
mod tests {
    use novarocks_query_application::coordination::AttemptFailureClass;

    use super::{NativeRootResultFetchError, classify_fetch_task_result_rpc_status};

    #[test]
    fn query_adapter_classifies_native_transport_failures_without_guessing() {
        let contract =
            NativeRootResultFetchError::contract("malformed response").into_pump_failure();
        assert_eq!(contract.class(), AttemptFailureClass::ContractViolation);
        let unavailable =
            NativeRootResultFetchError::infrastructure("backend unavailable").into_pump_failure();
        assert_eq!(
            unavailable.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        let capacity = NativeRootResultFetchError::resource_governance("fetch intake closed")
            .into_pump_failure();
        assert_eq!(capacity.class(), AttemptFailureClass::ResourceGovernance);
    }

    #[test]
    fn fetch_transport_loss_excludes_only_its_exact_root_process() {
        use novarocks_query_application::api::NativeAttemptTopologyRequirement;
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let lost = NativeRootResultFetchError::infrastructure("endpoint lost")
            .into_pump_failure_for_backend(backend);
        assert_eq!(
            lost.topology_requirement(),
            NativeAttemptTopologyRequirement::ExcludeProcess(backend)
        );
        let refusal = NativeRootResultFetchError::contract("answered refusal")
            .into_pump_failure_for_backend(backend);
        assert_eq!(
            refusal.topology_requirement(),
            NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }

    #[test]
    fn grpc_status_classification_retries_only_endpoint_unavailability() {
        for code in [tonic::Code::Unavailable, tonic::Code::DeadlineExceeded] {
            let failure = classify_fetch_task_result_rpc_status(tonic::Status::new(code, "lost"))
                .into_pump_failure();
            assert_eq!(
                failure.class(),
                AttemptFailureClass::RecoverableInfrastructure
            );
        }

        let generated_readiness = classify_fetch_task_result_rpc_status(tonic::Status::unknown(
            "Service was not ready: transport error",
        ))
        .into_pump_failure();
        assert_eq!(
            generated_readiness.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        let sourced_transport =
            classify_fetch_task_result_rpc_status(tonic::Status::from_error(Box::new(
                std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
            )))
            .into_pump_failure();
        assert_eq!(
            sourced_transport.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        // A backend that stops gracefully sends GOAWAY, so its cut streams
        // arrive as `Cancelled` carrying the transport error. That has to stay
        // as recoverable as the torn socket it replaced, or an orderly stop
        // would be less recoverable than an abrupt one.
        let mut graceful_stop =
            tonic::Status::new(tonic::Code::Cancelled, "operation was canceled");
        graceful_stop.set_source(std::sync::Arc::new(std::io::Error::other(
            "connection closed",
        )));
        assert_eq!(
            classify_fetch_task_result_rpc_status(graceful_stop)
                .into_pump_failure()
                .class(),
            AttemptFailureClass::RecoverableInfrastructure
        );

        let remote_unknown = classify_fetch_task_result_rpc_status(tonic::Status::unknown(
            "remote application returned an unknown status",
        ))
        .into_pump_failure();
        assert_eq!(
            remote_unknown.class(),
            AttemptFailureClass::ContractViolation
        );

        let capacity = classify_fetch_task_result_rpc_status(tonic::Status::new(
            tonic::Code::ResourceExhausted,
            "full",
        ))
        .into_pump_failure();
        assert_eq!(capacity.class(), AttemptFailureClass::ResourceGovernance);

        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::FailedPrecondition,
            tonic::Code::Unauthenticated,
            tonic::Code::PermissionDenied,
            tonic::Code::Internal,
            tonic::Code::Cancelled,
        ] {
            let failure =
                classify_fetch_task_result_rpc_status(tonic::Status::new(code, "refused"))
                    .into_pump_failure();
            assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        }
    }

    #[test]
    fn a_transport_failure_reports_what_actually_broke_the_connection() {
        // `tonic::Status` prints only its own code and message, so every
        // broken connection reached logs and clients as the fixed text
        // "transport error" while the io error underneath went nowhere. The
        // classification already reads that source to decide the failure
        // class; reporting it costs nothing and is the difference between a
        // diagnosable incident and a guess.
        let status = tonic::Status::from_error(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset by peer",
        )));
        let detail = classify_fetch_task_result_rpc_status(status).detail;
        assert!(
            detail.contains("connection reset by peer"),
            "the cause must survive into the reported detail: {detail}"
        );
        assert!(detail.contains("caused by"), "{detail}");
    }

    #[test]
    fn a_status_with_no_cause_reports_no_chain() {
        let status = tonic::Status::new(tonic::Code::ResourceExhausted, "full");
        let detail = classify_fetch_task_result_rpc_status(status).detail;
        assert!(!detail.contains("caused by"), "{detail}");
    }
}
