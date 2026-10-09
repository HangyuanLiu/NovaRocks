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
use novarocks_execution_contract::root_result::{RootResultRead, RootResultReply};
use novarocks_result_contract::RootProfileV1;
use novarocks_worker::TaskExecutionRegistry;
use novarocks_worker::root_result_channel::{
    ContextRootRoute, RootChannelError, RootDeliveryOwner, RootMetadataReservation,
};

/// Refusals carry no allocation or fallback into the retired result reader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeRootReadRefusal {
    UnknownRoot,
    Mismatch,
    Preparing,
    Busy,
    BackingCapacity,
    MetadataCapacity,
    Channel(RootChannelError),
}

/// Closed-route responses are inline facts, not newly admitted context reads.
pub enum NativeRootReadResponse {
    Owned(NativeRootResultReply),
    AwaitTerminalControl { accepted_consumed: u64 },
    Refused(NativeRootReadRefusal),
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

/// Exact local wrapper layout under the workspace's pinned Rust 1.92 Arc
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
}

impl NativeRootResultReader {
    pub fn new(registry: Arc<TaskExecutionRegistry>) -> Self {
        Self { registry }
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
            ContextRootRoute::UnknownRoot => return refusal(NativeRootReadRefusal::UnknownRoot),
            ContextRootRoute::Mismatch => return refusal(NativeRootReadRefusal::Mismatch),
            ContextRootRoute::Preparing => return refusal(NativeRootReadRefusal::Preparing),
            ContextRootRoute::Busy => return refusal(NativeRootReadRefusal::Busy),
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
            return refusal(NativeRootReadRefusal::MetadataCapacity);
        };
        let metadata = match admitted.try_reserve_native_send_metadata(metadata_bytes) {
            Ok(metadata) => metadata,
            Err(RootChannelError::Closed) => {
                return NativeRootReadResponse::AwaitTerminalControl {
                    accepted_consumed: admitted.accepted_consumed(),
                };
            }
            Err(RootChannelError::Capacity) => {
                return refusal(NativeRootReadRefusal::MetadataCapacity);
            }
            Err(error) => return refusal(NativeRootReadRefusal::Channel(error)),
        };
        if let Err(error) = admitted.apply_consumed() {
            return if error == RootChannelError::Closed {
                NativeRootReadResponse::AwaitTerminalControl {
                    accepted_consumed: admitted.accepted_consumed(),
                }
            } else {
                refusal(NativeRootReadRefusal::Channel(error))
            };
        }
        let (backing, metadata) = if ack_only {
            (NativeRootSendBacking::ControlMetadata(metadata), None)
        } else {
            let credit = match admitted.try_reserve_native_send_backing() {
                Ok(ResultWriteAdmission::Granted(credit)) => credit,
                Ok(ResultWriteAdmission::Blocked) => {
                    return refusal(NativeRootReadRefusal::BackingCapacity);
                }
                Err(RootChannelError::Closed) => {
                    return NativeRootReadResponse::AwaitTerminalControl {
                        accepted_consumed: admitted.accepted_consumed(),
                    };
                }
                Err(error) => return refusal(NativeRootReadRefusal::Channel(error)),
            };
            (NativeRootSendBacking::Payload(credit), Some(metadata))
        };
        let delivery = match admitted.read().await {
            Ok(delivery) => delivery,
            Err(error) => return refusal(NativeRootReadRefusal::Channel(error)),
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

fn refusal(reason: NativeRootReadRefusal) -> NativeRootReadResponse {
    NativeRootReadResponse::Refused(reason)
}
