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

//! Same-ticket original Native action, selected local offer and paid reply plan.
use super::super::{
    data_runtime::CachedNativeChannel,
    transport::{NativeChannelSource, NativeDialAttempt, NativeDialHandoff},
};
use super::*;
use novarocks_native_adapter::native_lane::ReservedNativeCall;
use novarocks_proto_models::resource_layout::{CloneBackingVisitor, GeneratedCloneBacking};
use novarocks_workload_control::{
    WorkError,
    joint_admission::{JointLane, NativeAsyncActionAdmission, NativeAsyncActionQuote},
};
use std::{
    alloc::Layout,
    task::{Context, Poll, Waker},
};

pub(crate) struct NativeBatchAdmission {
    pub(crate) remaining: usize,
    pub(super) admission: NativeAsyncActionAdmission,
}
impl std::fmt::Debug for NativeBatchAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalNativeBatchAdmission")
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}
pub(crate) struct NativePhysicalOffer {
    pub(in crate::native) encoding:
        Option<super::super::transport_supervisor::NativeTransportEncodingPermit>,
    pub(in crate::native) call: Option<NativeCallSource>,
}
pub(in crate::native) enum NativeCallSource {
    Warm {
        acquired: CachedNativeChannel,
        call: ReservedNativeCall,
    },
    Cold(NativeDialHandoff),
}
struct CloneInvoice {
    owned: usize,
}
impl CloneInvoice {
    fn add(&mut self, bytes: usize) -> Result<(), WorkError> {
        self.owned = self
            .owned
            .checked_add(bytes)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(())
    }
    fn array<T>(&mut self, len: usize) -> Result<(), WorkError> {
        self.add(
            Layout::array::<T>(len)
                .map_err(|_| WorkError::ArithmeticOverflow)?
                .size(),
        )
    }
    fn visit<T: GeneratedCloneBacking>(&mut self, value: &T) -> Result<(), WorkError> {
        value.visit_clone_backing(self)
    }
}
impl CloneBackingVisitor for CloneInvoice {
    type Error = WorkError;
    fn vector<T>(&mut self, len: usize) -> Result<(), WorkError> {
        self.array::<T>(len)
    }
    fn string(&mut self, len: usize) -> Result<(), WorkError> {
        self.add(len)
    }
    fn boxed<T>(&mut self) -> Result<(), WorkError> {
        self.add(size_of::<T>())
    }
    fn shared_bytes(&mut self, _: &bytes::Bytes) -> Result<(), WorkError> {
        // This aliases the original immutable input retained by the same
        // queue/source. Encoding output is separately included below.
        Ok(())
    }
    fn btree<K, V>(&mut self, _: usize) -> Result<(), WorkError> {
        Err(WorkError::Conflict)
    }
    fn unsupported(&mut self, _: &'static str) -> Result<(), WorkError> {
        Err(WorkError::Conflict)
    }
}
fn stored<T: GeneratedCloneBacking + prost::Message + Default + 'static>(
    invoice: &mut CloneInvoice,
    content: &dyn novarocks_execution_contract::domain::CodecOwnedContent,
) -> Result<(), WorkError> {
    invoice.visit(stored_message::<T>(content).ok_or(WorkError::Conflict)?)
}
fn credential(
    invoice: &mut CloneInvoice,
    material: &dyn novarocks_execution_contract::domain::ConfidentialContent,
) -> Result<(), WorkError> {
    let material = stored_credential(material).ok_or(WorkError::Conflict)?;
    invoice.array::<proto::CredentialLeaseDescriptor>(material.descriptors().len())?;
    for descriptor in material.descriptors() {
        invoice.visit(descriptor)?
    }
    Ok(())
}
fn context_domain(
    invoice: &mut CloneInvoice,
    domain: &novarocks_execution::task_execution::QueryContextDomainUpdate,
) -> Result<(), WorkError> {
    use novarocks_execution::task_execution::QueryContextDomainUpdate as D;
    match domain {
        D::CatalogBinding { payload, .. } => stored::<CatalogSet>(invoice, payload.as_ref()),
        D::SharedDynamicFilter { payload, .. } => {
            stored::<proto::RuntimeFilterContribution>(invoice, payload.as_ref())
        }
        D::Credential(value) => credential(invoice, value.material().as_ref()),
    }
}
fn clone_input_backing(operations: &[OperationIntent]) -> Result<usize, WorkError> {
    let mut invoice = CloneInvoice { owned: 0 };
    for operation in operations {
        // Every operation envelope/target projection allocates at most four
        // original 16-byte process/capability identities plus the compatibility
        // digest. This is new byte backing, not a charge of the old input twice.
        invoice.add(4 * 16 + 32)?;
        match operation {
            OperationIntent::EstablishQueryContext(value) => {
                stored::<CatalogSet>(&mut invoice, value.catalog_binding().as_ref())?;
                stored::<proto::RuntimeFilterContribution>(
                    &mut invoice,
                    value.initial_runtime_filter().as_ref(),
                )?;
                credential(&mut invoice, value.initial_credential().material().as_ref())?;
            }
            OperationIntent::UpdateQueryContext(value) => match value.as_ref() {
                UpdateQueryContext::Establish(value) => {
                    stored::<CatalogSet>(&mut invoice, value.catalog_binding().as_ref())?;
                    stored::<proto::RuntimeFilterContribution>(
                        &mut invoice,
                        value.initial_runtime_filter().as_ref(),
                    )?;
                    credential(&mut invoice, value.initial_credential().material().as_ref())?;
                }
                UpdateQueryContext::AdvanceDomain(value) => {
                    context_domain(&mut invoice, value.domain())?
                }
                UpdateQueryContext::RenewLease(_) => {}
            },
            OperationIntent::UpdateTask(value) => {
                invoice.array::<proto::TaskDomainUpdate>(value.domains().len())?;
                for domain in value.domains() {
                    use novarocks_execution::task_execution::TaskDomainUpdate as D;
                    match domain {
                        D::SplitAssignment(value) => stored::<
                            novarocks_proto_models::connector_read::SplitAssignment,
                        >(
                            &mut invoice, value.payload().as_ref()
                        )?,
                        D::TaskDynamicFilter { payload, .. } => stored::<
                            novarocks_proto_models::filter::RuntimeFilterEnvelope,
                        >(
                            &mut invoice, payload.as_ref()
                        )?,
                        D::OpenExchangeEdges { edges, .. } => invoice.array::<u32>(edges.len())?,
                        D::CloseExchangeDestination { .. } => invoice.add(16)?,
                    }
                }
            }
            _ => {}
        }
    }
    Ok(invoice.owned)
}
fn nonzero(value: usize) -> Result<std::num::NonZeroU64, WorkError> {
    std::num::NonZeroU64::new(u64::try_from(value).map_err(|_| WorkError::ArithmeticOverflow)?)
        .ok_or(WorkError::Conflict)
}
fn make_quote(
    items: usize,
    input_bytes: usize,
    budget: TransportBudget,
) -> Result<NativeAsyncActionQuote, WorkError> {
    let layouts = known_input_layouts_for_composition(
        std::num::NonZeroUsize::new(items).ok_or(WorkError::Conflict)?,
        budget,
    )?;
    let mut metadata = 0usize;
    for layout in [
        layouts.original.future,
        layouts.original.input,
        layouts.operation_slots,
        layouts.method_runs,
        layouts.receipts,
        layouts.unencodable_slots,
        layouts.queue_pointers,
        layouts.one_run_operations,
        layouts.one_control_run,
        layouts.reply_ids,
        layouts.reply_headers,
        layouts.establishes,
    ] {
        metadata = metadata
            .checked_add(layout.size())
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    let (tail, frame, body) =
        novarocks_native_adapter::native_lane::native_request_custody_layouts()
            .map_err(|_| WorkError::ArithmeticOverflow)?;
    for layout in [tail, frame, body] {
        metadata = metadata
            .checked_add(layout.size())
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    let reply_meta = Layout::array::<std::sync::OnceLock<OperationAcknowledgement>>(items)
        .map_err(|_| WorkError::ArithmeticOverflow)?
        .size();
    let plan =
        novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_transport(budget);
    let reply_bytes = novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES
        .checked_add(plan.peak_bytes())
        .ok_or(WorkError::ArithmeticOverflow)?;
    Ok(NativeAsyncActionQuote::for_original_native_transport(
        nonzero(items)?,
        nonzero(metadata)?,
        nonzero(input_bytes.max(1))?,
        nonzero(budget.max_batch_encoded_bytes())?,
        nonzero(reply_meta)?,
        nonzero(reply_bytes)?,
    ))
}
impl NativeTaskOperationSink {
    pub(super) fn quote_original_batch(
        &self,
        batch: &DispatchBatch,
    ) -> Result<NativeAsyncActionQuote, WorkError> {
        let clone = clone_input_backing(batch.operations())?;
        // The original Encoder may retain its previous request buffer while
        // growing the next output. Two full configured requests cover this
        // original source phase; original immutable input is charged separately.
        let wire = self
            .transport
            .max_batch_encoded_bytes()
            .checked_mul(2)
            .ok_or(WorkError::ArithmeticOverflow)?;
        make_quote(
            batch.operations().len(),
            clone
                .checked_add(wire)
                .ok_or(WorkError::ArithmeticOverflow)?,
            self.transport,
        )
    }
}

pub(super) enum NativeAdmissionFailure {
    Governance(WorkError),
    Unsent(String),
}
impl From<WorkError> for NativeAdmissionFailure {
    fn from(error: WorkError) -> Self {
        Self::Governance(error)
    }
}
pub(super) struct AcceptedNativeAction {
    pub(super) slot: super::super::apply_send_owner::ApplySlot,
    pub(super) publisher: super::super::apply_send_owner::OriginalReplyPublisher,
    pub(super) notifications: [novarocks_workload_control::WorkloadProgressNotification; 3],
}
impl NativeTaskOperationSink {
    pub(super) fn admit_original_batch(
        &self,
        batch: &mut DispatchBatch,
        client: &Client,
    ) -> Result<Option<AcceptedNativeAction>, NativeAdmissionFailure> {
        self.data_runtime
            .apply_send_port()
            .check_role_admission()
            .map_err(|failure| NativeAdmissionFailure::Unsent(failure.to_string()))?;
        if batch.native_pending.is_none() {
            let quote = self.quote_original_batch(batch)?;
            let lane = if batch
                .operations()
                .iter()
                .any(OperationIntent::requires_control_progress)
            {
                JointLane::Settlement
            } else {
                JointLane::Foreground
            };
            let admission = self
                .authority
                .action_source(lane, quote)?
                .begin()
                .map_err(|error| error.error)?;
            batch.native_pending = Some(NativeBatchAdmission {
                remaining: batch.operations().len(),
                admission,
            });
        }
        let waker = Waker::from(Arc::clone(&self.acks.inner));
        let mut cx = Context::from_waker(&waker);
        // Read-only physical facts borrow the batch before the paid ticket's
        // offer borrows its original field; no ticket is taken into a temporary.
        let backend = batch.backend();
        let supervisor_lane = supervisor_lane(batch);
        let method = batch.operations()[0].native_method();
        let items = batch.operations().len();
        let pending = batch
            .native_pending
            .as_mut()
            .expect("same original batch ticket");
        let mut offer = match pending.admission.poll_local_offer(&mut cx) {
            Poll::Pending => return Ok(None),
            Poll::Ready(Err(error)) => return Err(error.error.into()),
            Poll::Ready(Ok(offer)) => offer,
        };
        let slot = match self
            .data_runtime
            .apply_send_port()
            .reserve_original_slot(items)
        {
            Ok(slot) => slot,
            Err(failure) => {
                let notification = offer.reobserve_after_drop(&mut None::<NativePhysicalOffer>)?;
                drop(offer);
                notification.notify();
                return if matches!(
                    failure,
                    super::super::apply_send_owner::RegistrationFailure::Closed
                ) {
                    Err(NativeAdmissionFailure::Unsent(failure.to_string()))
                } else {
                    Ok(None)
                };
            }
        };
        let mut physical = match self.try_native_physical_selected(
            client,
            backend,
            supervisor_lane,
            method,
            &mut cx,
        ) {
            Poll::Pending => {
                drop(slot);
                let notification = offer.reobserve_after_drop(&mut None::<NativePhysicalOffer>)?;
                drop(offer);
                notification.notify();
                return Ok(None);
            }
            Poll::Ready(Err(error)) => {
                drop(slot);
                let notification = offer.reobserve_after_drop(&mut None::<NativePhysicalOffer>)?;
                drop(offer);
                notification.notify();
                return Err(NativeAdmissionFailure::Unsent(error.to_string()));
            }
            Poll::Ready(Ok(physical)) => Some(physical),
        };
        let accepted = match slot.accept_native_in(&mut offer, &mut physical) {
            Ok(notification) => notification,
            Err(error) => {
                let notification = offer.reobserve_after_drop(&mut physical)?;
                drop(offer);
                drop(slot);
                notification.notify();
                return Err(error.into());
            }
        };
        drop(offer);
        // All foreign Wake/Drop paths above observe the same stable original
        // row. Typed adoption returns silent notices; original commit owns F
        // and physical/source before the caller publishes any of them.
        let (publisher, action, reply) = slot.prepare_native_in(&pending.admission, items)?;
        Ok(Some(AcceptedNativeAction {
            slot,
            publisher,
            notifications: [accepted, action, reply],
        }))
    }
    fn try_native_physical_selected(
        &self,
        client: &Client,
        backend: BackendProcessId,
        lane: NativeTransportLane,
        method: NativeRpcMethod,
        cx: &mut Context<'_>,
    ) -> Poll<Result<NativePhysicalOffer, ChannelAcquisitionError>> {
        let encoding = match self
            .data_runtime
            .task_transport_supervisor()
            .try_reserve_encoding(&self.transport_waiter, backend, lane)
        {
            Ok(value) => value,
            Err(_) => return Poll::Pending,
        };
        let mut selected = Some(client.prepare_channel_source(method)?);
        let mut handoff = None;
        match NativeChannelSource::try_reserve_in(&mut selected, &mut handoff)? {
            NativeDialAttempt::Pending => return Poll::Pending,
            NativeDialAttempt::Installed => {}
        }
        let call = match handoff.take().expect("original Native local handoff") {
            NativeDialHandoff::Ready(acquired) => {
                let call = match acquired.channel.select_local_call().try_reserve(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => {
                        return Poll::Ready(Err(ChannelAcquisitionError::retryable_network(
                            error.to_string(),
                        )));
                    }
                    Poll::Ready(Ok(call)) => call,
                };
                if acquired.is_retired() {
                    return Poll::Ready(Err(ChannelAcquisitionError::retryable_network(
                        "selected Native generation retired",
                    )));
                }
                NativeCallSource::Warm { acquired, call }
            }
            cold @ NativeDialHandoff::Reserved(_) => NativeCallSource::Cold(cold),
        };
        Poll::Ready(Ok(NativePhysicalOffer {
            encoding: Some(encoding),
            call: Some(call),
        }))
    }
}
