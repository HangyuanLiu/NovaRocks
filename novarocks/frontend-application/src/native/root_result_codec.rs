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

//! Root-only Native wire validation before prost constructs response fields.
//!
//! Message limits describe protobuf bytes, not allocation capacities. The
//! original caller owns the window across Tonic's buffers, decoded aliases and
//! delivery. This codec neither mints that ownership nor derives a refund from
//! the final DTO's logical body length.

use bytes::Buf;
use novarocks_proto_models::novarocks as wire;
use novarocks_result_contract::RootProfileV1;
use prost::Message;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

pub(super) const ROOT_REQUEST_MAX_BYTES: usize = RootProfileV1::ENVELOPE_BYTES;
pub(super) const ROOT_RESPONSE_MAX_BYTES: usize =
    RootProfileV1::SEGMENT_BYTES + RootProfileV1::ENVELOPE_BYTES;
const GRPC_PREFIX_BYTES: usize = 5;
const ROOT_INITIAL_BUFFER_BYTES: usize = ROOT_REQUEST_MAX_BYTES + GRPC_PREFIX_BYTES;
const PREFLIGHT_REFUSAL: &str = "root response violates bounded wire resource shape";

#[derive(Debug)]
pub(super) struct RootWireProtocolError;
impl std::fmt::Display for RootWireProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("root response violates its wire contract")
    }
}
impl std::error::Error for RootWireProtocolError {}
fn protocol_status(message: &'static str) -> Status {
    let mut status = Status::data_loss(message);
    status.set_source(std::sync::Arc::new(RootWireProtocolError));
    status
}

#[derive(Clone, Copy, Default)]
pub(super) struct RootResultCodec;

impl Codec for RootResultCodec {
    type Encode = wire::FetchRootResultRequest;
    type Decode = wire::FetchRootResultResponse;
    type Encoder = RootRequestEncoder;
    type Decoder = RootResponseDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RootRequestEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RootResponseDecoder
    }
}

pub(super) struct RootRequestEncoder;

impl Encoder for RootRequestEncoder {
    type Item = wire::FetchRootResultRequest;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        // Tonic adds its five-byte header before calling the encoder; its
        // max_encoding_message_size applies only to these protobuf bytes.
        if item.encoded_len() > ROOT_REQUEST_MAX_BYTES {
            return Err(Status::invalid_argument(
                "root request exceeds its fixed wire envelope",
            ));
        }
        item.encode(dst)
            .map_err(|_| Status::internal("root request protobuf encoding failed"))
    }

    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(ROOT_INITIAL_BUFFER_BYTES, ROOT_INITIAL_BUFFER_BYTES)
    }
}

pub(super) struct RootResponseDecoder;

impl Decoder for RootResponseDecoder {
    type Item = wire::FetchRootResultResponse;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        // Locked Tonic 0.12.3 DecodeBuf is a bounded view of one contiguous
        // BytesMut. No protobuf object is constructed before this preflight.
        let raw = src.chunk();
        if raw.len() != src.remaining() {
            return Err(Status::internal(
                "root response decode buffer is not contiguous",
            ));
        }
        novarocks_task_codec::resource_preflight::check_root_result_reply(raw)
            .map_err(|_| protocol_status(PREFLIGHT_REFUSAL))?;
        Message::decode(src)
            .map(Some)
            .map_err(|_| protocol_status("invalid root response protobuf"))
    }

    fn buffer_settings(&self) -> BufferSettings {
        // Keep empty polls small. Tonic may reserve/grow before this decoder
        // runs; this initial size is not a physical backing bound or receipt.
        BufferSettings::new(ROOT_INITIAL_BUFFER_BYTES, ROOT_INITIAL_BUFFER_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, Bytes};
    use tonic::Code;
    use tonic::codegen::http::StatusCode;

    fn field(tag: u32, value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(value.len() as u64, &mut out);
        out.extend_from_slice(value);
        out
    }

    fn response_stream(raw: &[u8]) -> tonic::Streaming<wire::FetchRootResultResponse> {
        let mut framed = Vec::with_capacity(GRPC_PREFIX_BYTES + raw.len());
        framed.put_u8(0);
        framed.put_u32(u32::try_from(raw.len()).expect("bounded fixture size"));
        framed.extend_from_slice(raw);
        tonic::Streaming::new_response(
            RootResponseDecoder,
            axum::body::Body::from(framed),
            StatusCode::OK,
            None,
            Some(ROOT_RESPONSE_MAX_BYTES),
        )
    }

    #[tokio::test]
    async fn actual_tonic_decoder_rejects_merged_data_before_prost() {
        // Two singular Reply messages merge in prost; the original resource
        // format rejects their two Data objects before allocating the DTO.
        let one_reply = field(4, &field(6, &[]));
        let mut raw = one_reply.clone();
        raw.extend_from_slice(&one_reply);
        let prost_merged = wire::FetchRootResultResponse::decode(raw.as_slice())
            .expect("the raw fixture is a legal merged protobuf");
        assert!(matches!(
            prost_merged.result,
            Some(wire::fetch_root_result_response::Result::Reply(_))
        ));
        let error = response_stream(&raw).message().await.unwrap_err();
        assert_eq!(error.code(), Code::DataLoss);
        assert_eq!(error.message(), PREFLIGHT_REFUSAL);
    }

    #[tokio::test]
    async fn actual_tonic_decoder_keeps_the_existing_repeated_body_shape() {
        let mut data = field(2, &[b'a'; 64]);
        data.extend_from_slice(&field(2, b"x"));
        let raw = field(4, &field(6, &data));
        let decoded = response_stream(&raw).message().await.unwrap().unwrap();
        let Some(wire::fetch_root_result_response::Result::Reply(reply)) = decoded.result else {
            panic!("expected actual Reply");
        };
        let Some(wire::root_read_receipt::Outcome::Data(data)) = reply.outcome else {
            panic!("expected actual Data");
        };
        assert_eq!(data.body, Bytes::from_static(b"x"));
        // This logical length proves no allocation capacity or refund.
    }

    #[tokio::test]
    async fn protobuf_limits_exclude_the_actual_tonic_prefix() {
        let body = vec![0; RootProfileV1::SEGMENT_BYTES];
        let mut raw = field(4, &field(6, &field(2, &body)));
        let padding_len = (0..RootProfileV1::ENVELOPE_BYTES)
            .find(|len| raw.len() + field(127, &vec![0; *len]).len() == ROOT_RESPONSE_MAX_BYTES)
            .expect("exactly fill the original metadata envelope");
        raw.extend_from_slice(&field(127, &vec![0; padding_len]));
        assert_eq!(raw.len(), ROOT_RESPONSE_MAX_BYTES);
        let decoded = response_stream(&raw).message().await.unwrap().unwrap();
        let Some(wire::fetch_root_result_response::Result::Reply(reply)) = decoded.result else {
            panic!("expected actual Reply");
        };
        let Some(wire::root_read_receipt::Outcome::Data(data)) = reply.outcome else {
            panic!("expected actual Data");
        };
        assert_eq!(data.body.len(), RootProfileV1::SEGMENT_BYTES);
        raw.extend_from_slice(&[0x78, 0]);
        let error = response_stream(&raw).message().await.unwrap_err();
        assert_eq!(error.code(), Code::OutOfRange);

        let exact_request = (0..=ROOT_REQUEST_MAX_BYTES)
            .map(|bytes| wire::FetchRootResultRequest {
                root_task: Some(wire::TaskIdentity {
                    backend_process_id: Some(wire::BackendProcessId {
                        value: vec![0; bytes],
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .find(|request| request.encoded_len() == ROOT_REQUEST_MAX_BYTES)
            .expect("wire-only fixture exactly fills the request envelope");
        let mut too_large = exact_request.clone();
        too_large
            .root_task
            .as_mut()
            .unwrap()
            .backend_process_id
            .as_mut()
            .unwrap()
            .value
            .push(0);
        assert_eq!(too_large.encoded_len(), ROOT_REQUEST_MAX_BYTES + 1);
        let body = tonic::codec::EncodeBody::new_client(
            RootRequestEncoder,
            tokio_stream::iter([Ok(exact_request)]),
            None,
            Some(ROOT_REQUEST_MAX_BYTES),
        );
        let framed = axum::body::to_bytes(axum::body::Body::new(body), ROOT_INITIAL_BUFFER_BYTES)
            .await
            .unwrap();
        assert_eq!(framed.len(), ROOT_REQUEST_MAX_BYTES + GRPC_PREFIX_BYTES);
        assert_eq!(&framed[..GRPC_PREFIX_BYTES], &[0, 0, 0, 16, 0]);
        let body = tonic::codec::EncodeBody::new_client(
            RootRequestEncoder,
            tokio_stream::iter([Ok(too_large)]),
            None,
            Some(ROOT_REQUEST_MAX_BYTES),
        );
        assert!(
            axum::body::to_bytes(axum::body::Body::new(body), ROOT_INITIAL_BUFFER_BYTES,)
                .await
                .is_err()
        );
    }
}
