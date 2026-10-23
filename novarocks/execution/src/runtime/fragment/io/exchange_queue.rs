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
/// Queue ownership/control facts, independent of client cancellation policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeSendQueueFailure {
    OwnerStopped,
    ObservationPoisoned,
    ObservationRebound,
}
impl std::fmt::Display for ExchangeSendQueueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::OwnerStopped => "exchange send owner was cancelled",
            Self::ObservationPoisoned => "exchange tracker observation lock failed",
            Self::ObservationRebound => "exchange tracker already bound to another test observer",
        })
    }
}
impl std::error::Error for ExchangeSendQueueFailure {}
impl novarocks_failure_contract::FailureProjection for ExchangeSendQueueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, reason) = match self {
            Self::OwnerStopped => (
                FailureClass::Control,
                ExecutionReason::ExchangeQueueOwnerStopped,
            ),
            Self::ObservationPoisoned => (
                FailureClass::Unexpected,
                ExecutionReason::ExchangeObservationPoisoned,
            ),
            Self::ObservationRebound => (
                FailureClass::Invariant,
                ExecutionReason::ExchangeObservationRebound,
            ),
        };
        FailureFact::new(class, reason.reason())
    }
}
impl From<ExchangeSendQueueFailure> for crate::runtime::fragment::ExecutionFailure {
    fn from(source: ExchangeSendQueueFailure) -> Self {
        Self::from_domain(source)
    }
}

use super::exchange_edge::{EdgeSendGate, ExchangeDestinationKey};
use super::{
    ExchangeFrame, ExchangePermitAcquire, ExchangeStreamTransmitter, ExchangeTransmitRejection,
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation,
};
use crate::{
    exec::pipeline::{operator::InputConstructionOwner, schedule::observer::Observable},
    runtime::{
        fragment::{ExecutionFailure, ExecutionResult},
        io::IoExecutor,
        profile::{OperatorProfiles, ProfileUnit, clamp_u128_to_i64},
        runtime_state::RuntimeErrorState,
    },
};
use novarocks_execution_contract::{
    ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeFrameGeometry,
    ExchangeFrameKind, ExchangeFrameSequence, ExchangePermitRequest,
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    alloc::Layout,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// The exact async/CPU scopes, independently of logical terminal publication.
/// Stop requests do not decrement these counters or destroy an active job.
pub struct ExchangeSendTracker {
    active_tasks: AtomicUsize,
    inflight_tasks: AtomicUsize,
    inflight_bytes: AtomicUsize,
    cancelled: CancellationToken,
    #[cfg(novarocks_dev_controls)]
    test_observer: Mutex<Option<Arc<dyn crate::runtime::exchange::TestEventObserver>>>,
}
impl ExchangeSendTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active_tasks: AtomicUsize::new(0),
            inflight_tasks: AtomicUsize::new(0),
            inflight_bytes: AtomicUsize::new(0),
            cancelled: CancellationToken::new(),
            #[cfg(novarocks_dev_controls)]
            test_observer: Mutex::new(None),
        })
    }
    pub fn is_idle(&self) -> bool {
        self.active_tasks.load(Ordering::Acquire) == 0
            && self.inflight_tasks.load(Ordering::Acquire) == 0
    }
    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes.load(Ordering::Acquire)
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.is_cancelled()
    }
    fn begin(self: &Arc<Self>, notify: Arc<Observable>) -> SendTaskActivity {
        self.active_tasks.fetch_add(1, Ordering::AcqRel);
        SendTaskActivity {
            tracker: self.clone(),
            notify,
        }
    }
    #[cfg(novarocks_dev_controls)]
    pub fn install_test_observer(
        &self,
        observer: Arc<dyn crate::runtime::exchange::TestEventObserver>,
    ) -> Result<(), ExchangeSendQueueFailure> {
        let mut slot = self
            .test_observer
            .lock()
            .map_err(|_| ExchangeSendQueueFailure::ObservationPoisoned)?;
        if let Some(old) = slot.as_ref() {
            if Arc::ptr_eq(old, &observer) {
                return Ok(());
            }
            return Err(ExchangeSendQueueFailure::ObservationRebound);
        }
        *slot = Some(observer);
        Ok(())
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn test_observer(
        &self,
    ) -> Option<Arc<dyn crate::runtime::exchange::TestEventObserver>> {
        self.test_observer.lock().ok()?.clone()
    }
}
struct SendTaskActivity {
    tracker: Arc<ExchangeSendTracker>,
    notify: Arc<Observable>,
}
impl Drop for SendTaskActivity {
    fn drop(&mut self) {
        self.tracker.active_tasks.fetch_sub(1, Ordering::AcqRel);
        self.notify.defer_notify().arm();
    }
}

/// An admitted local source, never an eagerly encoded parked payload. Its
/// geometry and encoding come from the same concrete creator. The sender's
/// peak includes writer scratch and every independent payload copy.
pub trait ExchangeEncodeSource: Send + 'static {
    fn geometry(&self) -> ExchangeFrameGeometry;
    fn sender_peak_bytes(&self) -> usize;
    fn encode(self: Box<Self>, packet_cap: usize) -> ExecutionResult<Vec<u8>>;
}
struct StreamState {
    next: ExchangeFrameSequence,
    busy: bool,
    ended: bool,
    failed: bool,
}
/// One shared sequencer for every producer driver of this exact stream.
/// The native capability already owns the preparation-reserved flow position.
pub struct ExchangeSendStream {
    transmitter: Arc<dyn ExchangeStreamTransmitter>,
    state: Mutex<StreamState>,
    observable: Arc<Observable>,
    _storage: ValueTypeStorageLifetime,
}
impl ExchangeSendStream {
    pub fn bind(
        transmitter: Arc<dyn ExchangeStreamTransmitter>,
        contributors: usize,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Arc<Self>> {
        let waiters = contributors
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| capacity("exchange sender observer count overflow"))?;
        let backing =
            Observable::bounded_backing_bytes(waiters).map_err(ExecutionFailure::from_domain)?;
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Self>())
                .map_err(crate::exec::pipeline::operator::ConstructionFailure::from)?,
        )?;
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                Layout::new::<Observable>(),
            )
            .map_err(crate::exec::pipeline::operator::ConstructionFailure::from)?,
        )?;
        owner.observe_opaque_upper_bound(backing)?;
        let observable =
            Arc::new(Observable::try_bounded(waiters).map_err(ExecutionFailure::from_domain)?);
        Ok(Arc::new(Self {
            transmitter,
            state: Mutex::new(StreamState {
                next: ExchangeFrameSequence::FIRST,
                busy: false,
                ended: false,
                failed: false,
            }),
            observable,
            _storage: owner.storage_lifetime(),
        }))
    }
    pub fn target(&self) -> &super::ExchangeStreamTarget {
        self.transmitter.target()
    }
    pub fn observable(&self) -> Arc<Observable> {
        self.observable.clone()
    }
    pub fn available(&self) -> bool {
        let s = self.state.lock().expect("exchange stream lock");
        !s.busy && !s.ended && !s.failed
    }
}

pub struct ExchangeSendTask {
    pub stream: Arc<ExchangeSendStream>,
    pub kind: ExchangeFrameKind,
    pub source: Option<Box<dyn ExchangeEncodeSource>>,
    pub profiles: Option<OperatorProfiles>,
    pub notify: Arc<Observable>,
    pub error_state: Arc<RuntimeErrorState>,
    pub tracker: Arc<ExchangeSendTracker>,
    pub edge_gate: Option<Arc<EdgeSendGate>>,
}
pub enum ExchangeSendEnqueue {
    Enqueued,
    NoCapacity(ExchangeSendTask),
}
/// S covers encoding, queued and actual in-flight request lifetime. The FIFO
/// semaphore has at most one waiter per preparation-reserved exact stream.
/// Q/R are acquired by the native capability before this semaphore is touched.
pub struct ExchangeSendQueue {
    staging: Arc<tokio::sync::Semaphore>,
    max_inflight_bytes: usize,
    inflight_bytes: Arc<AtomicUsize>,
    codec: Arc<IoExecutor>,
}
fn io(kind: FragmentIoErrorKind, detail: impl Into<String>) -> FragmentIoError {
    FragmentIoError::new(FragmentIoOperation::ExchangeTransmit, kind, detail)
}
fn contract(detail: impl Into<String>) -> ExecutionFailure {
    io(FragmentIoErrorKind::InvalidResponse, detail).into()
}
fn capacity(detail: impl Into<String>) -> ExecutionFailure {
    io(FragmentIoErrorKind::ResourceExhausted, detail).into()
}
impl ExchangeSendQueue {
    pub fn new(max_inflight_bytes: usize, codec: Arc<IoExecutor>) -> Self {
        // The owning ExecutionRuntime validates this same library domain before
        // constructing the process queue; there is no saturating capacity.
        assert!(
            max_inflight_bytes > 0 && max_inflight_bytes <= tokio::sync::Semaphore::MAX_PERMITS
        );
        Self {
            staging: Arc::new(tokio::sync::Semaphore::new(max_inflight_bytes)),
            max_inflight_bytes,
            inflight_bytes: Arc::new(AtomicUsize::new(0)),
            codec,
        }
    }
    pub fn max_inflight_bytes(&self) -> usize {
        self.max_inflight_bytes
    }
    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes.load(Ordering::Acquire)
    }
    pub fn cancel_tracker(&self, tracker: &Arc<ExchangeSendTracker>) {
        tracker.cancelled.cancel();
    }
    pub fn close_destination_normally(&self, gate: &Arc<EdgeSendGate>) -> (bool, usize) {
        (gate.close_for_normal_cancellation(), 0)
    }
    pub fn try_submit(
        self: &Arc<Self>,
        task: ExchangeSendTask,
    ) -> ExecutionResult<ExchangeSendEnqueue> {
        if task.tracker.is_cancelled() {
            return Err(ExchangeSendQueueFailure::OwnerStopped.into());
        }
        if task.kind == ExchangeFrameKind::End && task.source.is_some()
            || task.kind != ExchangeFrameKind::End && task.source.is_none()
        {
            return Err(contract("exchange End/source shape differs"));
        }
        let geometry = task.source.as_ref().map(|s| s.geometry()).unwrap_or(
            ExchangeFrameGeometry::try_new(0, 0, 0).map_err(|e| contract(e.to_string()))?,
        );
        let source_bytes = task
            .source
            .as_ref()
            .map(|s| s.sender_peak_bytes())
            .unwrap_or(0);
        if source_bytes > self.max_inflight_bytes || u32::try_from(source_bytes).is_err() {
            return Err(capacity(
                "one exchange frame exceeds the sender staging window",
            ));
        }
        let request = {
            let mut state = task.stream.state.lock().expect("exchange stream lock");
            if state.failed || state.ended {
                return Err(contract("exchange frame follows a failed or ended stream"));
            }
            if state.busy {
                drop(state);
                return Ok(ExchangeSendEnqueue::NoCapacity(task));
            }
            let request = ExchangePermitRequest {
                stream: task.stream.target().stream,
                sequence: state.next,
                kind: task.kind,
                geometry,
            };
            request.validate().map_err(|e| contract(e.to_string()))?;
            if task.kind != ExchangeFrameKind::End {
                state
                    .next
                    .checked_next()
                    .map_err(|e| contract(e.to_string()))?;
            }
            state.busy = true;
            request
        };
        task.tracker.inflight_tasks.fetch_add(1, Ordering::AcqRel);
        let activity = task.tracker.begin(task.notify.clone());
        let stream = task.stream.clone();
        let queue = self.clone();
        let scope = SendScope {
            task: Some(task),
            sequence: request.sequence,
            completed: false,
            _activity: activity,
        };
        stream.transmitter.spawn(Box::pin(async move {
            let mut scope = scope;
            let result = queue
                .run(scope.task.as_mut().expect("owned exchange task"), request)
                .await;
            scope.complete(result);
        }));
        Ok(ExchangeSendEnqueue::Enqueued)
    }
    async fn run(
        self: &Arc<Self>,
        task: &mut ExchangeSendTask,
        request: ExchangePermitRequest,
    ) -> Result<(), ExchangeTransmitRejection> {
        if let Some(gate) = task.edge_gate.as_ref() {
            if gate.is_normally_canceled() {
                return if task.stream.target().delivery == ExchangeDeliveryRequirement::DemandDriven
                {
                    Err(ExchangeTransmitRejection::DestinationCanceled(
                        crate::task_execution::status::CancelReason::UpstreamNoLongerNeeded,
                    ))
                } else {
                    Err(io(
                        FragmentIoErrorKind::InvalidResponse,
                        "MustDrain destination withdrew before its frame",
                    )
                    .into())
                };
            }
        }
        let admission = task
            .stream
            .transmitter
            .acquire(request, task.tracker.cancelled.clone())
            .await?;
        let grant = match admission {
            ExchangePermitAcquire::Granted(grant) => {
                if grant.request != request {
                    return Err(io(
                        FragmentIoErrorKind::InvalidResponse,
                        "exchange grant differs from its complete request",
                    )
                    .into());
                }
                grant
            }
            ExchangePermitAcquire::Closed(close) => {
                if close.stream != request.stream {
                    return Err(io(
                        FragmentIoErrorKind::InvalidResponse,
                        "exchange close belongs to another stream",
                    )
                    .into());
                }
                return match (task.stream.target().delivery, close.disposition) {
                    (
                        ExchangeDeliveryRequirement::DemandDriven,
                        ExchangeCloseDisposition::Withdrawn,
                    ) => Err(ExchangeTransmitRejection::DestinationCanceled(
                        crate::task_execution::status::CancelReason::UpstreamNoLongerNeeded,
                    )),
                    (_, ExchangeCloseDisposition::Consumed { final_sequence })
                        if request.kind == ExchangeFrameKind::End
                            && final_sequence == request.sequence =>
                    {
                        Ok(())
                    }
                    _ => Err(io(
                        FragmentIoErrorKind::InvalidResponse,
                        "exchange close does not prove this exact final End was consumed",
                    )
                    .into()),
                };
            }
        };
        let encoded = if let Some(source) = task.source.take() {
            let bytes = source.sender_peak_bytes();
            let permit = tokio::select! { biased;
                _=task.tracker.cancelled.cancelled()=>return Err(io(FragmentIoErrorKind::Cancelled,"exchange staging wait cancelled").into()),
                result=self.staging.clone().acquire_many_owned(u32::try_from(bytes).map_err(|_|io(FragmentIoErrorKind::ResourceExhausted,"exchange staging request exceeds permit domain"))?)=>result.map_err(|_|io(FragmentIoErrorKind::Internal,"exchange sender staging closed"))?,
            };
            self.inflight_bytes.fetch_add(bytes, Ordering::AcqRel);
            task.tracker
                .inflight_bytes
                .fetch_add(bytes, Ordering::AcqRel);
            let staging = SendStaging {
                _permit: Some(permit),
                bytes,
                process: self.inflight_bytes.clone(),
                tracker: task.tracker.clone(),
                notify: task.notify.clone(),
            };
            let activity = task.tracker.begin(task.notify.clone());
            let control = task.error_state.clone();
            let cap = usize::try_from(request.geometry.wire_bytes()).map_err(|_| {
                io(
                    FragmentIoErrorKind::ResourceExhausted,
                    "exchange wire extent exceeds usize",
                )
            })?;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.codec.submit(move |context| {
                let _activity = activity;
                let result = if context.is_cancelled() || control.error().is_some() {
                    Err(io(
                        FragmentIoErrorKind::Cancelled,
                        "exchange codec stopped before work",
                    )
                    .into())
                } else {
                    source.encode(cap)
                };
                let result = result.map(|payload| EncodedSend {
                    payload,
                    _staging: staging,
                });
                let _ = sender.send(result);
            });
            // Do not drop a running codec scope or its staging on cancellation.
            // The actual closure owns both; even a dropped receiver cannot
            // return those reservations before closure exit.
            receiver
                .await
                .map_err(|_| {
                    io(
                        FragmentIoErrorKind::Internal,
                        "exchange codec exited without a result",
                    )
                })?
                .map_err(|cause| {
                    ExchangeTransmitRejection::Failed(FragmentIoError::from_domain(
                        FragmentIoOperation::ExchangeTransmit,
                        FragmentIoErrorKind::Internal,
                        cause,
                    ))
                })?
        } else {
            EncodedSend {
                payload: Vec::new(),
                _staging: SendStaging::empty(
                    task.tracker.clone(),
                    task.notify.clone(),
                    self.inflight_bytes.clone(),
                ),
            }
        };
        if encoded.payload.len() as u128 > u128::from(request.geometry.wire_bytes()) {
            return Err(io(
                FragmentIoErrorKind::InvalidResponse,
                "encoded frame exceeds its receiver wire grant",
            )
            .into());
        }
        let start = Instant::now();
        let bytes = encoded.payload.len();
        let EncodedSend { payload, _staging } = encoded;
        let result = task
            .stream
            .transmitter
            .transmit(
                ExchangeFrame {
                    target: task.stream.target().clone(),
                    grant,
                    payload,
                },
                task.tracker.cancelled.clone(),
            )
            .await;
        drop(_staging);
        if let Some(profile) = task.profiles.as_ref() {
            profile
                .common
                .counter_add("RequestSent", ProfileUnit::Unit, 1);
            profile.common.counter_add(
                "BytesSent",
                ProfileUnit::Bytes,
                clamp_u128_to_i64(bytes as u128),
            );
            profile.unique.counter_add(
                "NetworkTime",
                ProfileUnit::TimeNs,
                clamp_u128_to_i64(start.elapsed().as_nanos()),
            );
        }
        if result.is_ok() {
            super::exchange_metrics::observe_exchange_shuffle_bytes(bytes);
        }
        result
    }
}
struct EncodedSend {
    payload: Vec<u8>,
    _staging: SendStaging,
}
struct SendStaging {
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    bytes: usize,
    process: Arc<AtomicUsize>,
    tracker: Arc<ExchangeSendTracker>,
    notify: Arc<Observable>,
}
impl SendStaging {
    fn empty(
        tracker: Arc<ExchangeSendTracker>,
        notify: Arc<Observable>,
        process: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            _permit: None,
            bytes: 0,
            process,
            tracker,
            notify,
        }
    }
}
impl Drop for SendStaging {
    fn drop(&mut self) {
        drop(self._permit.take());
        self.process.fetch_sub(self.bytes, Ordering::AcqRel);
        self.tracker
            .inflight_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        self.notify.defer_notify().arm();
    }
}
struct SendScope {
    task: Option<ExchangeSendTask>,
    sequence: ExchangeFrameSequence,
    completed: bool,
    _activity: SendTaskActivity,
}
impl SendScope {
    fn complete(&mut self, result: Result<(), ExchangeTransmitRejection>) {
        let task = self.task.as_ref().expect("exchange scope task");
        let success = result.is_ok();
        match result {
            Ok(()) => {}
            Err(ExchangeTransmitRejection::DestinationCanceled(reason)) => {
                let target = task.stream.target();
                let expected = ExchangeDestinationKey::new(
                    target.stream.destination_fragment_instance_id(),
                    target.stream.destination_node().get(),
                );
                match task.edge_gate.as_ref() {
                    Some(gate)
                        if target.delivery == ExchangeDeliveryRequirement::DemandDriven
                            && gate.destination() == expected =>
                    {
                        gate.close_for_normal_cancellation();
                    }
                    _ => task.error_state.set_error(io(
                        FragmentIoErrorKind::InvalidResponse,
                        format!("exchange {reason} has no matching DemandDriven gate"),
                    )),
                }
            }
            Err(ExchangeTransmitRejection::Failed(cause)) => task.error_state.set_error(cause),
        }
        let mut state = task.stream.state.lock().expect("exchange stream lock");
        if success {
            if task.kind == ExchangeFrameKind::End {
                state.ended = true;
            } else {
                state.next = self
                    .sequence
                    .checked_next()
                    .expect("checked before frame admission");
            }
        } else {
            state.failed = true;
        }
        state.busy = false;
        drop(state);
        self.completed = true;
    }
}
impl Drop for SendScope {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            if !self.completed {
                let mut state = task.stream.state.lock().expect("exchange stream lock");
                state.failed = true;
                state.busy = false;
                drop(state);
                task.error_state.set_error(io(
                    FragmentIoErrorKind::Internal,
                    "exchange async scope exited without its outcome",
                ));
            }
            task.tracker.inflight_tasks.fetch_sub(1, Ordering::AcqRel);
            task.stream.observable.defer_notify().arm();
            task.notify.defer_notify().arm();
        }
    }
}
