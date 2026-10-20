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

//! Owned, transport-independent Native root reads. No RPC is installed here.
//!
//! The consumer must retain the returned owner beside every independently
//! allocated wire backing, through Body and the last physical DATA alias.
//! Pure protobuf projection or handler return is not a send-exit receipt.

use std::alloc::Layout;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_execution_contract::root_result::{
    RootReadDeferral, RootReadDeferred, RootReadRefusal, RootResultRead, RootResultReply,
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_result_contract::RootProfileV1;
use novarocks_worker::TaskExecutionRegistry;
use novarocks_worker::root_result_channel::{
    ContextRootRoute, RootChannelError, RootDeliveryOwner, RootMetadataReservation,
};

/// Closed-route responses are inline facts, not newly admitted context reads.
pub enum NativeRootReadResponse {
    Owned(NativeRootResultReply),
    AwaitTerminalControl { accepted_consumed: u64 },
    Deferred(RootReadDeferred),
    Refused(RootReadRefusal),
}

/// Full copy credit follows the send owner; logical encoded length cannot
/// shrink it. The same owner keeps the existing root delivery position live.
pub struct NativeRootSendOwnership {
    resources: Option<Arc<NativeRootSendResources>>,
}

struct NativeRootSendResources {
    // Physical wire backings live in the caller's wrapper, before this owner.
    // Credit and delivery exit precede the fixed wrapper metadata reservation.
    _delivery: RootDeliveryOwner,
    backing: NativeRootSendBacking,
    _metadata: Option<RootMetadataReservation>,
}

enum NativeRootSendBacking {
    Payload(ResultWriteCredit),
    // ACK-only has no data-position dependency. Its complete two-copy
    // envelope and wrapper share one original fixed metadata reservation.
    ControlMetadata(RootMetadataReservation),
}

impl NativeRootSendOwnership {
    pub fn backing_capacity_bytes(&self) -> usize {
        match &self
            .resources
            .as_ref()
            .expect("live root send owner")
            .backing
        {
            NativeRootSendBacking::Payload(credit) => credit.bytes(),
            NativeRootSendBacking::ControlMetadata(_reservation) => {
                2 * RootProfileV1::ENVELOPE_BYTES
            }
        }
    }
}

impl Clone for NativeRootSendOwnership {
    fn clone(&self) -> Self {
        Self {
            resources: Some(Arc::clone(
                self.resources.as_ref().expect("live root send owner"),
            )),
        }
    }
}

impl Drop for NativeRootSendOwnership {
    fn drop(&mut self) {
        // No Weak, raw Arc, or Deref escapes this strong-only handle. Pinned
        // std Arc::into_inner drops its implicit Weak (freeing the allocation)
        // before returning the last Resources value. Only then can its copy
        // credit, delivery position and metadata reservation physically exit.
        let resources = Arc::into_inner(self.resources.take().expect("live root send owner"));
        drop(resources);
    }
}

/// Declaration order destroys the actual reply/Bytes before its send grant.
/// Exporting ownership is for the future physical transport wrapper; this
/// module neither constructs a Tonic response nor allocates encoded bytes.
pub struct NativeRootResultReply {
    reply: RootResultReply,
    ownership: NativeRootSendOwnership,
}

impl NativeRootResultReply {
    pub fn reply(&self) -> &RootResultReply {
        &self.reply
    }

    /// Clones no payload. Keep this owner until every backing using its
    /// pregrant has actually dropped; a response future is not that proof.
    pub fn ownership(&self) -> NativeRootSendOwnership {
        self.ownership.clone()
    }
}

/// Exact local wrapper layout under the workspace's pinned Rust 1.98.1 Arc
/// representation. No callback, schema tree, or opaque allocator is guessed.
/// Worker owns and covers its existing delivery/credit issuer scaffolds.
pub fn native_root_send_metadata_bytes() -> usize {
    #[repr(C, align(2))]
    struct ArcHeader {
        strong: AtomicUsize,
        weak: AtomicUsize,
    }
    let ownership = Layout::new::<ArcHeader>()
        .extend(Layout::new::<NativeRootSendResources>())
        .expect("fixed root send Arc layout fits usize")
        .0
        .pad_to_align();
    ownership
        .size()
        .checked_add(Layout::new::<NativeRootReadResponse>().size())
        .expect("fixed root reply layout fits usize")
}

/// A lightweight composition handle. Registry lookup mints the read under
/// the original context fence, independent of the retired task's horizon.
pub struct NativeRootResultReader {
    registry: Arc<TaskExecutionRegistry>,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
}

impl NativeRootResultReader {
    pub fn new(registry: Arc<TaskExecutionRegistry>) -> Self {
        Self {
            registry,
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_controls(
        mut self,
        controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    ) -> Self {
        self.test_controls = controls;
        self
    }

    /// A finite original-role action holds only the second read, before any
    /// additional Data offer. The runner enables it after parsing the first
    /// complete client row; that observation does not invent a Backend ACK.
    #[cfg(novarocks_dev_controls)]
    async fn fail_after_visible_prefix(
        &self,
        request: &RootResultRead,
        admitted: &novarocks_worker::root_result_channel::RootChannelRead,
    ) -> bool {
        use novarocks_execution_contract::ConclusionDecision;
        if request.wanted().map(|sequence| sequence.get()) != Some(2) {
            return false;
        }
        let Some(controls) = self.test_controls.as_ref() else {
            return false;
        };
        let identity = request.root_task();
        let Some(control) = self.registry.live_task_execution_control_for_test(identity) else {
            return false;
        };
        let claim = match controls.claim_dormant_after_visible_prefix(
            identity.query_execution_id(),
            identity.backend_process_id(),
        ) {
            Ok(Some(claim)) => claim,
            Ok(None) => return false,
            Err(error) => {
                tracing::error!(%error, "visible-prefix failure action could not be claimed");
                return false;
            }
        };
        let cancellation = claim.cancellation();
        loop {
            if !cancellation.is_active() || control.is_stopped() {
                return false;
            }
            match claim.action_ended() {
                Ok(false) => {}
                Ok(true) => return false,
                Err(error) => {
                    tracing::error!(%error, "visible-prefix failure action end could not be observed");
                    return false;
                }
            }
            match controls.after_visible_prefix_enabled(&claim) {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => {
                    tracing::error!(%error, "visible-prefix failure activation could not be observed");
                    return false;
                }
            }
            // Only this finite role-granted read is suspended. Its original
            // scope lifetime and the caller's future remain the owners of
            // cancellation; no Task failure or ACK is synthesized on exit.
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return false,
                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {},
            }
        }
        if !cancellation.is_active() || control.is_stopped() {
            return false;
        }
        let original = crate::task_protocol_fault::injected_task_execution_failure();
        let receipt = control.report_failure(original.clone());
        if !matches!(
            receipt.decision,
            ConclusionDecision::Accept | ConclusionDecision::RefineDerived
        ) || !receipt
            .accepted
            .originating_failure()
            .is_some_and(|accepted| std::ptr::eq(accepted.cause(), original.cause()))
        {
            return false;
        }
        if let Err(error) =
            crate::task_protocol_fault::record_task_execution_failure(identity, &claim)
        {
            control.report_cleanup(&error);
            return true;
        }
        let execution = identity.query_execution_id();
        eprintln!(
            "NOVAROCKS_TASK_EXECUTION_FAILURE_AFTER_VISIBLE_PREFIX_ACCEPTED execution_id={}:{}:{} stage={} task={} backend={} token={} accepted_consumed={}",
            execution.query_id().high(),
            execution.query_id().low(),
            execution.attempt_id().get(),
            identity.stage_id().get(),
            identity.task_id().get(),
            identity.backend_process_id(),
            claim.token,
            admitted.accepted_consumed()
        );
        true
    }

    pub async fn read(&self, request: &RootResultRead) -> NativeRootReadResponse {
        self.read_with_transport_metadata(request, 0).await
    }

    /// Pregrant the transport's exact post-admission allocation inventory
    /// from the same fixed root metadata envelope, before ACK or projection.
    /// Pre-decode lane/stream allocations remain the caller's responsibility.
    pub(crate) async fn read_with_transport_metadata(
        &self,
        request: &RootResultRead,
        transport_metadata_bytes: usize,
    ) -> NativeRootReadResponse {
        let admitted = match self.registry.context_root_result_route(request) {
            ContextRootRoute::Read(read) => read,
            ContextRootRoute::AwaitTerminalControl { accepted_consumed } => {
                return NativeRootReadResponse::AwaitTerminalControl { accepted_consumed };
            }
            ContextRootRoute::UnknownRoot => {
                return refusal(request, RootChannelError::UnknownRoot);
            }
            ContextRootRoute::Mismatch => return refusal(request, RootChannelError::Identity),
            ContextRootRoute::Preparing => return deferred(request, RootReadDeferral::Preparing),
            ContextRootRoute::Busy(facts) => {
                return deferred(request, RootReadDeferral::ReadHoldersBusy(facts));
            }
            ContextRootRoute::Refused(cause) => return refusal(request, cause),
        };
        // ACK retires queue ownership before data-copy admission. It cannot
        // wait for the data budget that its own retirement makes available.
        // Metadata precedes ACK and every reply/Arc projection; ACK-only's
        // complete two-copy envelope is included in this same reservation.
        let ack_only = request.wanted().is_none();
        let metadata_bytes = native_root_send_metadata_bytes()
            .checked_add(if ack_only {
                2 * RootProfileV1::ENVELOPE_BYTES
            } else {
                0
            })
            .and_then(|bytes| bytes.checked_add(transport_metadata_bytes));
        let Some(metadata_bytes) = metadata_bytes else {
            return refusal(request, RootChannelError::SendGeometry);
        };
        let metadata = match admitted.try_reserve_native_send_metadata(metadata_bytes) {
            Ok(metadata) => metadata,
            Err(RootChannelError::Closed) => {
                return NativeRootReadResponse::AwaitTerminalControl {
                    accepted_consumed: admitted.accepted_consumed(),
                };
            }
            Err(RootChannelError::Capacity(cause)) => {
                return deferred(
                    request,
                    RootReadDeferral::SendMetadataBusy(cause.resource_facts()),
                );
            }
            Err(error) => return refusal(request, error),
        };
        if let Err(error) = admitted.apply_consumed() {
            return if error == RootChannelError::Closed {
                NativeRootReadResponse::AwaitTerminalControl {
                    accepted_consumed: admitted.accepted_consumed(),
                }
            } else {
                refusal(request, error)
            };
        }
        #[cfg(novarocks_dev_controls)]
        if self.fail_after_visible_prefix(request, &admitted).await {
            return NativeRootReadResponse::AwaitTerminalControl {
                accepted_consumed: admitted.accepted_consumed(),
            };
        }
        let (backing, metadata) = if ack_only {
            (NativeRootSendBacking::ControlMetadata(metadata), None)
        } else {
            let credit = match admitted.try_reserve_native_send_backing() {
                Ok(ResultWriteAdmission::Granted(credit)) => credit,
                Ok(ResultWriteAdmission::Blocked) => {
                    return match admitted.native_send_backing_refusal() {
                        Ok(cause) => deferred(
                            request,
                            RootReadDeferral::SendBackingBusy(cause.resource_facts()),
                        ),
                        Err(error) => refusal(request, error),
                    };
                }
                Err(RootChannelError::Closed) => {
                    return NativeRootReadResponse::AwaitTerminalControl {
                        accepted_consumed: admitted.accepted_consumed(),
                    };
                }
                Err(error) => return refusal(request, error),
            };
            (NativeRootSendBacking::Payload(credit), Some(metadata))
        };
        let delivery = match admitted.read().await {
            Ok(delivery) => delivery,
            Err(error) => return refusal(request, error),
        };
        let (reply, guard) = delivery.into_parts();
        NativeRootReadResponse::Owned(NativeRootResultReply {
            reply,
            ownership: NativeRootSendOwnership {
                resources: Some(Arc::new(NativeRootSendResources {
                    backing,
                    _delivery: guard,
                    _metadata: metadata,
                })),
            },
        })
    }
}

fn refusal(request: &RootResultRead, cause: RootChannelError) -> NativeRootReadResponse {
    let fact = cause.failure_fact();
    let detail = SafeDetail::truncating("root read was refused by its owning channel");
    NativeRootReadResponse::Refused(RootReadRefusal::for_request(request, fact, detail))
}
fn deferred(request: &RootResultRead, reason: RootReadDeferral) -> NativeRootReadResponse {
    NativeRootReadResponse::Deferred(RootReadDeferred::for_request(request, reason))
}
