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

//! Post-admission unary root encoding and physical DATA ownership.
//!
//! This helper installs no RPC. Its caller must own bounded pre-decode
//! lane/stream/request/header/future resources. Closed and refused routes use
//! that control capacity. A concrete response body must reach Hyper without
//! an unaccounted outer Box; H2's independent connection copies need their
//! own connection owner. Those composition gates remain separate.

use std::alloc::Layout;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes};
use hyper::body::{Body, Frame, SizeHint};
use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultReply};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as wire;
use novarocks_result_contract::RootProfileV1;
use novarocks_task_codec::root_result::{decode_read, encode_reply};
use prost::Message;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBody, EncodeBuf, Encoder};
use tonic::codegen::http;
use tonic::server::UnaryService;
use tonic::{Code, Request, Response, Status};

use crate::root_result_reader::{
    NativeRootReadRefusal, NativeRootReadResponse, NativeRootResultReader, NativeRootSendOwnership,
};

const GRPC_PREFIX: usize = 5;
const PAYLOAD_CAPACITY: usize = RootProfileV1::SEGMENT_BYTES + RootProfileV1::ENVELOPE_BYTES;

struct OwnedWireReply {
    // The DTO (including the original offered alias) exits before its owner.
    message: wire::FetchRootResultResponse,
    _ownership: Option<NativeRootSendOwnership>,
}

type RootEncodedBody = EncodeBody<RootEncoder, tokio_stream::Once<Result<OwnedWireReply, Status>>>;

/// Exact post-admission allocations introduced by this locked Tonic path.
/// The outer Body is inline; Tonic's inner EncodeBody Box, split/freeze Shared
/// header, one owned DATA wrapper and the 16-byte backend UUID are covered.
/// Byte buffers use the separate full-copy grant; no logical-length credit.
pub fn native_root_unary_metadata_bytes() -> usize {
    Layout::new::<RootEncodedBody>()
        .size()
        .checked_add(novarocks_worker::guarded_bytes::BYTES_MUT_SHARED_HEADER_BYTES)
        .and_then(|bytes| {
            bytes.checked_add(novarocks_worker::guarded_bytes::owner_wrapper_bytes::<
                Bytes,
                NativeRootSendOwnership,
            >())
        })
        .and_then(|bytes| bytes.checked_add(16))
        .expect("fixed root unary metadata fits usize")
}

/// Actual Tonic unary decode/call/encode, followed in the same poll by a
/// concrete body handoff. No await can expose the intermediate BoxBody after
/// the admitted service returns. The finite lane owner is supplied by the
/// eventual listener, not manufactured from a late root admission here.
pub async fn root_result_unary<B>(
    reader: &NativeRootResultReader,
    request: http::Request<B>,
) -> http::Response<NativeRootUnaryBody>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send,
{
    let handoff = Mutex::new(None);
    // Pin the Darwin lazy mutex allocation before giving shared references
    // to codec/service. This pre-admission allocation belongs to the lane.
    drop(handoff.lock().expect("root unary handoff lock"));
    let codec = RootCodec { handoff: &handoff };
    let service = RootService {
        reader,
        handoff: &handoff,
    };
    // No compression is enabled: there is one independently allocated
    // encoder buffer, not an additional uncompression buffer or reencode.
    let mut grpc = tonic::server::Grpc::new(codec)
        .max_decoding_message_size(RootProfileV1::ENVELOPE_BYTES)
        .max_encoding_message_size(PAYLOAD_CAPACITY - GRPC_PREFIX);
    let response = grpc.unary(service, request).await;
    let ownership = handoff.lock().expect("root unary handoff lock").take();
    let (parts, inner) = response.into_parts();
    http::Response::from_parts(
        parts,
        NativeRootUnaryBody {
            inner,
            ownership,
            emitted_data: false,
        },
    )
}

struct RootService<'a> {
    reader: &'a NativeRootResultReader,
    handoff: &'a Mutex<Option<NativeRootSendOwnership>>,
}
impl<'a> UnaryService<wire::FetchRootResultRequest> for RootService<'a> {
    type Response = OwnedWireReply;
    // This finite future allocation precedes read admission and is part of
    // the caller's lane/stream inventory, not the root's later metadata.
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::Response>, Status>> + Send + 'a>>;

    fn call(&mut self, request: Request<wire::FetchRootResultRequest>) -> Self::Future {
        let reader = self.reader;
        let handoff = self.handoff;
        Box::pin(async move {
            let read =
                decode_read(request.get_ref(), FieldPath::root("root_read")).map_err(|_| {
                    Status::new(Code::InvalidArgument, "invalid frozen root read")
                })?;
            let (message, ownership) = match reader
                .read_with_transport_metadata(&read, native_root_unary_metadata_bytes())
                .await
            {
                NativeRootReadResponse::Owned(reply) => {
                    let ownership = reply.ownership();
                    let message = encode_reply(reply.reply()).map_err(|_| {
                        Status::new(Code::Internal, "invalid owned root reply")
                    })?;
                    (message, Some(ownership))
                }
                NativeRootReadResponse::AwaitTerminalControl { accepted_consumed } => {
                    let message = encode_reply(&RootResultReply {
                        root_task: read.root_task(),
                        profile: read.profile(),
                        kind: read.kind(),
                        accepted_consumed,
                        outcome: RootReadOutcome::AwaitTerminalControl,
                    })
                    .map_err(|_| {
                        Status::new(Code::Internal, "invalid sealed root reply")
                    })?;
                    (message, None)
                }
                NativeRootReadResponse::Refused(reason) => return Err(refusal_status(reason)),
            };
            // Projection follows the same metadata pregrant. Publish the
            // strong handoff before returning Ready to Tonic's map_response,
            // which allocates its encoding buffer before the first body poll.
            *handoff.lock().expect("root unary handoff lock") = ownership.clone();
            Ok(Response::new(OwnedWireReply {
                message,
                _ownership: ownership,
            }))
        })
    }
}

struct RootCodec<'a> {
    handoff: &'a Mutex<Option<NativeRootSendOwnership>>,
}
impl Codec for RootCodec<'_> {
    type Encode = OwnedWireReply;
    type Decode = wire::FetchRootResultRequest;
    type Encoder = RootEncoder;
    type Decoder = RootDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        // Tonic 0.12.3 calls codec.encoder() before EncodedBytes::new.
        let ownership = self
            .handoff
            .lock()
            .expect("root unary handoff lock")
            .clone();
        let capacity = ownership
            .as_ref()
            .map_or(RootProfileV1::ENVELOPE_BYTES, |owner| {
                owner.backing_capacity_bytes() / 2
            });
        RootEncoder {
            capacity,
            _ownership: ownership,
        }
    }
    fn decoder(&mut self) -> Self::Decoder {
        RootDecoder
    }
}
struct RootDecoder;
impl Decoder for RootDecoder {
    type Item = wire::FetchRootResultRequest;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Message::decode(src)
            .map(Some)
            .map_err(|_| Status::new(Code::InvalidArgument, "invalid root read protobuf"))
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(RootProfileV1::ENVELOPE_BYTES, RootProfileV1::ENVELOPE_BYTES)
    }
}
struct RootEncoder {
    capacity: usize,
    _ownership: Option<NativeRootSendOwnership>,
}
impl Encoder for RootEncoder {
    type Item = OwnedWireReply;
    type Error = Status;
    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        let length = item.message.encoded_len();
        if length
            .checked_add(GRPC_PREFIX)
            .is_none_or(|n| n > self.capacity)
        {
            return Err(Status::new(
                Code::ResourceExhausted,
                "root reply exceeds fixed send envelope",
            ));
        }
        // remaining_mut is usize::MAX for BytesMut, not actual capacity.
        // The single unary item has already consumed the five-byte prefix;
        // chunk_mut exposes the actual preallocated contiguous remainder.
        if dst.chunk_mut().len() < length {
            return Err(Status::new(
                Code::Internal,
                "root encoder lost its preallocated backing",
            ));
        }
        item.message
            .encode(dst)
            .map_err(|_| Status::new(Code::Internal, "root protobuf encoding failed"))
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(self.capacity, self.capacity)
    }
}

/// Keep this concrete value through the framework's body handoff. Field
/// order drops Tonic's inner Box/buffer before the independent send owner.
/// Every owned DATA frame retains that owner through all Bytes slices.
pub struct NativeRootUnaryBody {
    inner: tonic::body::BoxBody,
    ownership: Option<NativeRootSendOwnership>,
    emitted_data: bool,
}
impl Body for NativeRootUnaryBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => {
                    if this.emitted_data {
                        return Poll::Ready(Some(Err(Status::new(
                            Code::Internal,
                            "root unary emitted multiple DATA backings",
                        ))));
                    }
                    this.emitted_data = true;
                    let data = if let Some(owner) = &this.ownership {
                        novarocks_worker::guarded_bytes::bytes_with_exit_guard(data, owner.clone())
                    } else {
                        data
                    };
                    Poll::Ready(Some(Ok(Frame::data(data))))
                }
                Err(frame) => Poll::Ready(Some(Ok(frame))),
            },
            result => result,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn refusal_status(reason: NativeRootReadRefusal) -> Status {
    match reason {
        NativeRootReadRefusal::UnknownRoot => {
            Status::new(Code::NotFound, "unknown context root")
        }
        NativeRootReadRefusal::Mismatch => {
            Status::new(Code::FailedPrecondition, "frozen root read mismatch")
        }
        NativeRootReadRefusal::Preparing => {
            Status::new(Code::Unavailable, "root installation in progress")
        }
        NativeRootReadRefusal::Busy => {
            Status::new(Code::ResourceExhausted, "root read holder capacity")
        }
        NativeRootReadRefusal::BackingCapacity => {
            Status::new(Code::ResourceExhausted, "root send backing capacity")
        }
        NativeRootReadRefusal::MetadataCapacity => {
            Status::new(Code::ResourceExhausted, "root send metadata capacity")
        }
        NativeRootReadRefusal::Channel(_) => {
            Status::new(Code::FailedPrecondition, "root channel read refused")
        }
    }
}
