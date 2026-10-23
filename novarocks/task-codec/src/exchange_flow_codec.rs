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

//! Pure exchange identity and permit projection; this module grants no capacity.

use novarocks_execution_contract::exchange_flow::{
    ExchangeCloseDisposition, ExchangeFrameGeometry, ExchangeFrameKind, ExchangeFrameSequence,
    ExchangePermitGrant, ExchangePermitRequest, ExchangePermitToken, ExchangeStreamClose,
    ExchangeStreamIdentity,
};
use novarocks_execution_contract::{ExchangeSource, FragmentNodeId};
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::novarocks as wire;
use novarocks_types::UniqueId;

use crate::identity::{decode_task_identity, encode_task_identity};
use crate::{inconsistent, invalid, invalid_enum, missing};

pub fn decode_stream(
    value: &wire::ExchangeStreamIdentity,
    path: FieldPath,
) -> Result<ExchangeStreamIdentity, ProtocolError> {
    let source = decode_task_identity(
        value.source_task.as_ref().ok_or_else(|| {
            missing(
                path.field("source_task"),
                "exchange source Task is required",
            )
        })?,
        path.field("source_task"),
    )?;
    let destination = decode_task_identity(
        value.destination_task.as_ref().ok_or_else(|| {
            missing(
                path.field("destination_task"),
                "exchange destination Task is required",
            )
        })?,
        path.field("destination_task"),
    )?;
    ExchangeStreamIdentity::try_new(
        ExchangeSource::new(
            source,
            UniqueId::new(value.source_finst_id_hi, value.source_finst_id_lo),
            value.sender_ordinal,
        ),
        destination,
        UniqueId::new(value.destination_finst_id_hi, value.destination_finst_id_lo),
        FragmentNodeId::new(value.destination_node_id),
    )
    .map_err(|error| invalid(path, error.to_string()))
}

pub fn encode_stream(value: ExchangeStreamIdentity) -> wire::ExchangeStreamIdentity {
    wire::ExchangeStreamIdentity {
        source_task: Some(encode_task_identity(value.source().task())),
        source_finst_id_hi: value.source().fragment_instance_id().high(),
        source_finst_id_lo: value.source().fragment_instance_id().low(),
        sender_ordinal: value.source().sender_ordinal(),
        destination_task: Some(encode_task_identity(value.destination())),
        destination_finst_id_hi: value.destination_fragment_instance_id().high(),
        destination_finst_id_lo: value.destination_fragment_instance_id().low(),
        destination_node_id: value.destination_node().get(),
    }
}

pub fn decode_sequence(
    value: i64,
    path: FieldPath,
) -> Result<ExchangeFrameSequence, ProtocolError> {
    ExchangeFrameSequence::try_new(value).map_err(|error| invalid(path, error.to_string()))
}

fn decode_kind(value: i32, path: FieldPath) -> Result<ExchangeFrameKind, ProtocolError> {
    match wire::ExchangeFrameKind::try_from(value) {
        Ok(wire::ExchangeFrameKind::Data) => Ok(ExchangeFrameKind::Data),
        Ok(wire::ExchangeFrameKind::LogicalSummary) => Ok(ExchangeFrameKind::LogicalSummary),
        Ok(wire::ExchangeFrameKind::WriterResults) => Ok(ExchangeFrameKind::WriterResults),
        Ok(wire::ExchangeFrameKind::End) => Ok(ExchangeFrameKind::End),
        Ok(wire::ExchangeFrameKind::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "exchange frame kind must be a known non-default value",
        )),
    }
}

fn encode_kind(value: ExchangeFrameKind) -> i32 {
    let encoded = match value {
        ExchangeFrameKind::Data => wire::ExchangeFrameKind::Data,
        ExchangeFrameKind::LogicalSummary => wire::ExchangeFrameKind::LogicalSummary,
        ExchangeFrameKind::WriterResults => wire::ExchangeFrameKind::WriterResults,
        ExchangeFrameKind::End => wire::ExchangeFrameKind::End,
    };
    encoded as i32
}

pub fn decode_permit_request(
    value: &wire::AcquireExchangePermitRequest,
    path: FieldPath,
) -> Result<ExchangePermitRequest, ProtocolError> {
    let stream = decode_stream(
        value
            .stream
            .as_ref()
            .ok_or_else(|| missing(path.field("stream"), "exchange permit stream is required"))?,
        path.field("stream"),
    )?;
    let geometry = value.geometry.as_ref().ok_or_else(|| {
        missing(
            path.field("geometry"),
            "exchange frame geometry is required",
        )
    })?;
    let request = ExchangePermitRequest {
        stream,
        sequence: decode_sequence(value.sequence, path.field("sequence"))?,
        kind: decode_kind(value.frame_kind, path.field("frame_kind"))?,
        geometry: ExchangeFrameGeometry::try_new(
            geometry.wire_bytes,
            geometry.decoded_bytes,
            geometry.decode_scratch_bytes,
        )
        .map_err(|error| invalid(path.field("geometry"), error.to_string()))?,
    };
    request
        .validate()
        .map_err(|error| invalid(path.field("geometry"), error.to_string()))?;
    Ok(request)
}

pub fn encode_permit_request(value: ExchangePermitRequest) -> wire::AcquireExchangePermitRequest {
    wire::AcquireExchangePermitRequest {
        stream: Some(encode_stream(value.stream)),
        sequence: value.sequence.get(),
        frame_kind: encode_kind(value.kind),
        geometry: Some(wire::ExchangeFrameGeometry {
            wire_bytes: value.geometry.wire_bytes(),
            decoded_bytes: value.geometry.decoded_bytes(),
            decode_scratch_bytes: value.geometry.decode_scratch_bytes(),
        }),
    }
}

pub fn decode_token(value: &[u8], path: FieldPath) -> Result<ExchangePermitToken, ProtocolError> {
    let bytes = value.try_into().map_err(|_| {
        invalid(
            path.clone(),
            "exchange permit token must be exactly 16 bytes",
        )
    })?;
    ExchangePermitToken::try_from_bytes(bytes).map_err(|error| invalid(path, error.to_string()))
}

/// Checks a reply against the exact outstanding request, without granting capacity.
pub fn decode_permit_grant(
    value: &wire::ExchangePermitGrant,
    expected: ExchangePermitRequest,
    path: FieldPath,
) -> Result<ExchangePermitGrant, ProtocolError> {
    let request = decode_permit_request(
        value
            .request
            .as_ref()
            .ok_or_else(|| missing(path.field("request"), "exchange grant request is required"))?,
        path.field("request"),
    )?;
    if request != expected {
        return Err(inconsistent(
            path.field("request"),
            "exchange grant names a different request",
        ));
    }
    Ok(ExchangePermitGrant {
        request,
        token: decode_token(&value.token, path.field("token"))?,
    })
}

pub fn encode_permit_grant(value: ExchangePermitGrant) -> wire::ExchangePermitGrant {
    wire::ExchangePermitGrant {
        request: Some(encode_permit_request(value.request)),
        token: value.token.to_bytes().to_vec(),
    }
}

/// Exact structural facts of a frame, without a capacity or token-use grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedExchangeFrameEnvelope {
    pub sequence: ExchangeFrameSequence,
    pub kind: ExchangeFrameKind,
    pub token: ExchangePermitToken,
}

/// Validate the original frame envelope even when its receiver is closed.
/// Retirement does not excuse malformed identities, tokens, kinds or EOS data.
pub fn validate_frame_envelope(
    value: &wire::ExchangeRequest,
    expected_stream: ExchangeStreamIdentity,
    expected_sender_count: u32,
    path: FieldPath,
) -> Result<ValidatedExchangeFrameEnvelope, ProtocolError> {
    let stream = decode_stream(
        value
            .stream
            .as_ref()
            .ok_or_else(|| missing(path.field("stream"), "exchange frame stream is required"))?,
        path.field("stream"),
    )?;
    if stream != expected_stream
        || value.finst_id_hi != stream.destination_fragment_instance_id().high()
        || value.finst_id_lo != stream.destination_fragment_instance_id().low()
        || value.node_id != stream.destination_node().get()
        || value.source_finst_id_hi != stream.source().fragment_instance_id().high()
        || value.source_finst_id_lo != stream.source().fragment_instance_id().low()
        || value.sender_ordinal != stream.source().sender_ordinal()
        || expected_sender_count == 0
        || value.sender_count != expected_sender_count
        || value.sender_ordinal >= expected_sender_count
    {
        return Err(inconsistent(
            path.clone(),
            "exchange frame does not match its frozen stream",
        ));
    }
    let sequence = decode_sequence(value.sequence, path.field("sequence"))?;
    let kind = decode_kind(value.frame_kind, path.field("frame_kind"))?;
    if value.eos != (kind == ExchangeFrameKind::End) {
        return Err(inconsistent(
            path.field("eos"),
            "exchange frame kind differs from EOS",
        ));
    }
    if kind == ExchangeFrameKind::End && !value.payload.is_empty() {
        return Err(inconsistent(
            path.field("payload"),
            "exchange End frame must have an empty payload",
        ));
    }
    let token = decode_token(&value.permit_token, path.field("permit_token"))?;
    Ok(ValidatedExchangeFrameEnvelope {
        sequence,
        kind,
        token,
    })
}

/// Structural validation only. Token single-claim state and actual IPC geometry
/// belong to the receiving owner and must still be checked before allocation.
pub fn validate_permitted_frame(
    value: &wire::ExchangeRequest,
    expected: ExchangePermitGrant,
    expected_sender_count: u32,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let envelope = validate_frame_envelope(
        value,
        expected.request.stream,
        expected_sender_count,
        path.clone(),
    )?;
    if envelope.sequence != expected.request.sequence {
        return Err(inconsistent(
            path.field("sequence"),
            "exchange frame sequence does not match its grant",
        ));
    }
    if envelope.kind != expected.request.kind {
        return Err(inconsistent(
            path.field("frame_kind"),
            "exchange frame kind does not match its grant",
        ));
    }
    if envelope.token != expected.token {
        return Err(inconsistent(
            path.field("permit_token"),
            "exchange permit token does not match its grant",
        ));
    }
    expected
        .request
        .validate()
        .map_err(|error| invalid(path.field("geometry"), error.to_string()))?;
    let payload_bytes = u64::try_from(value.payload.len()).map_err(|_| {
        invalid(
            path.field("payload"),
            "exchange payload length exceeds the wire byte domain",
        )
    })?;
    if payload_bytes > expected.request.geometry.wire_bytes() {
        return Err(inconsistent(
            path.field("payload"),
            "exchange payload exceeds its grant",
        ));
    }
    Ok(())
}

/// The old close carrier omits source Task, so the frozen caller stream supplies
/// that identity; every address carried by the reply must match it exactly.
pub fn decode_normal_close(
    value: &wire::ExchangeNormalClosed,
    expected: ExchangeStreamIdentity,
    expected_sender_count: u32,
    path: FieldPath,
) -> Result<ExchangeStreamClose, ProtocolError> {
    let destination = decode_task_identity(
        value.destination_task.as_ref().ok_or_else(|| {
            missing(
                path.field("destination_task"),
                "normal close requires the exact destination Task",
            )
        })?,
        path.field("destination_task"),
    )?;
    if destination != expected.destination()
        || value.destination_finst_id_hi != expected.destination_fragment_instance_id().high()
        || value.destination_finst_id_lo != expected.destination_fragment_instance_id().low()
        || value.destination_node_id != expected.destination_node().get()
        || value.source_finst_id_hi != expected.source().fragment_instance_id().high()
        || value.source_finst_id_lo != expected.source().fragment_instance_id().low()
        || value.sender_ordinal != expected.source().sender_ordinal()
        || expected_sender_count == 0
        || value.sender_count != expected_sender_count
        || value.sender_ordinal >= expected_sender_count
    {
        return Err(inconsistent(
            path.clone(),
            "normal close does not match its frozen exchange stream",
        ));
    }
    let disposition = match value.disposition.as_ref().ok_or_else(|| {
        missing(
            path.field("disposition"),
            "normal close requires an explicit disposition",
        )
    })? {
        wire::exchange_normal_closed::Disposition::Consumed(consumed) => {
            ExchangeCloseDisposition::Consumed {
                final_sequence: decode_sequence(
                    consumed.final_sequence,
                    path.field("consumed").field("final_sequence"),
                )?,
            }
        }
        wire::exchange_normal_closed::Disposition::Withdrawn(_) => {
            ExchangeCloseDisposition::Withdrawn
        }
    };
    Ok(ExchangeStreamClose {
        stream: expected,
        disposition,
    })
}

pub fn encode_normal_close(
    value: ExchangeStreamClose,
    sender_count: u32,
) -> wire::ExchangeNormalClosed {
    let stream = value.stream;
    wire::ExchangeNormalClosed {
        destination_task: Some(encode_task_identity(stream.destination())),
        destination_finst_id_hi: stream.destination_fragment_instance_id().high(),
        destination_finst_id_lo: stream.destination_fragment_instance_id().low(),
        destination_node_id: stream.destination_node().get(),
        source_finst_id_hi: stream.source().fragment_instance_id().high(),
        source_finst_id_lo: stream.source().fragment_instance_id().low(),
        sender_ordinal: stream.source().sender_ordinal(),
        sender_count,
        disposition: Some(match value.disposition {
            ExchangeCloseDisposition::Consumed { final_sequence } => {
                wire::exchange_normal_closed::Disposition::Consumed(wire::ExchangeConsumed {
                    final_sequence: final_sequence.get(),
                })
            }
            ExchangeCloseDisposition::Withdrawn => {
                wire::exchange_normal_closed::Disposition::Withdrawn(wire::ExchangeWithdrawn {})
            }
        }),
    }
}

#[cfg(test)]
#[path = "exchange_flow_codec_tests.rs"]
mod tests;
