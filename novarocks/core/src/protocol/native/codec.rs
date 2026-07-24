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

use std::any::TypeId;
use std::marker::PhantomData;

use bytes::Buf;
use prost::Message;
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

const PLAN_FIELD: u32 = 1;
const INSTANCE_PARAMS_FIELD: u32 = 2;
const LEGACY_RUNTIME_FILTER_PARAMS_FIELD: u32 = 7;
const PLAN_RUNTIME_FILTER_BINDINGS_FIELD: u32 = 10;
const RUNTIME_FILTER_TABLE_BINDING_FIELD: u32 = 2;
const RUNTIME_FILTER_BINDING_PRODUCER_FIELD: u32 = 8;
const PRODUCER_JOIN_BUILD_KEY_FIELD: u32 = 3;
const PRODUCER_AGGREGATE_TOPN_KEY_FIELD: u32 = 4;

/// Native protobuf codec used by generated NovaRocks clients and servers.
///
/// Prost intentionally discards unknown fields. That behavior is correct for
/// every native message except the retired `InstanceParams` tag 7 and ambiguous
/// producer target oneofs: accepting either would make invalid raw wire appear
/// to satisfy the current contract after unknown-field or oneof information is
/// discarded. The decoder therefore inspects only the affected paths in raw
/// `SubmitFragmentRequest` bytes before delegating to Prost. No legacy payload
/// is decoded or carried.
#[derive(Debug, Clone)]
pub(crate) struct NativeProstCodec<T, U> {
    marker: PhantomData<(T, U)>,
}

impl<T, U> Default for NativeProstCodec<T, U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T, U> Codec for NativeProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = NativeProstEncoder<T>;
    type Decoder = NativeProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        NativeProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        NativeProstDecoder::default()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeProstEncoder<T> {
    marker: PhantomData<T>,
    buffer_settings: BufferSettings,
}

impl<T> Default for NativeProstEncoder<T> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

impl<T: Message> Encoder for NativeProstEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        item.encode(destination)
            .expect("Message only errors if not enough space");
        Ok(())
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeProstDecoder<U> {
    marker: PhantomData<U>,
    buffer_settings: BufferSettings,
}

impl<U> Default for NativeProstDecoder<U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

impl<U> Decoder for NativeProstDecoder<U>
where
    U: Message + Default + Send + 'static,
{
    type Item = U;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        if TypeId::of::<U>() == TypeId::of::<crate::proto::novarocks::SubmitFragmentRequest>() {
            let bytes = source.chunk();
            if bytes.len() != source.remaining() {
                return Err(Status::internal(
                    "SubmitFragmentRequest protobuf is not contiguous",
                ));
            }
            match scan_submit_fragment_request(bytes) {
                Ok(()) | Err(WireScanError::Decode(_)) => {}
                Err(WireScanError::RetiredInstanceParamsField) => {
                    return Err(retired_instance_params_field_status());
                }
                Err(WireScanError::AmbiguousProducerBindingTarget) => {
                    return Err(ambiguous_producer_binding_target_status());
                }
            }
        }

        U::decode(source)
            .map(Some)
            .map_err(|error| Status::internal(error.to_string()))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

pub(crate) fn validate_submit_fragment_request_wire(bytes: &[u8]) -> Result<(), Status> {
    match scan_submit_fragment_request(bytes) {
        Ok(()) => Ok(()),
        Err(WireScanError::RetiredInstanceParamsField) => {
            Err(retired_instance_params_field_status())
        }
        Err(WireScanError::AmbiguousProducerBindingTarget) => {
            Err(ambiguous_producer_binding_target_status())
        }
        Err(WireScanError::Decode(error)) => Err(Status::internal(error.to_string())),
    }
}

fn retired_instance_params_field_status() -> Status {
    Status::invalid_argument(
        "retired InstanceParams tag 7 runtime_filter_params is not accepted by native submission",
    )
}

fn ambiguous_producer_binding_target_status() -> Status {
    Status::invalid_argument(
        "native runtime-filter producer target carries both join_build_key and aggregate_topn_key",
    )
}

fn scan_submit_fragment_request(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if wire_type == WireType::LengthDelimited {
            match field {
                PLAN_FIELD => {
                    let plan = take_length_delimited(&mut cursor)?;
                    scan_plan_fragment(plan)?;
                    continue;
                }
                INSTANCE_PARAMS_FIELD => {
                    let instance_params = take_length_delimited(&mut cursor)?;
                    scan_instance_params(instance_params)?;
                    continue;
                }
                _ => {}
            }
        }
        skip_field(wire_type, field, &mut cursor, context.clone())?;
    }
    Ok(())
}

fn scan_plan_fragment(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == PLAN_RUNTIME_FILTER_BINDINGS_FIELD && wire_type == WireType::LengthDelimited {
            let table = take_length_delimited(&mut cursor)?;
            scan_runtime_filter_binding_table(table)?;
        } else {
            skip_field(wire_type, field, &mut cursor, context.clone())?;
        }
    }
    Ok(())
}

fn scan_runtime_filter_binding_table(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == RUNTIME_FILTER_TABLE_BINDING_FIELD && wire_type == WireType::LengthDelimited {
            let binding = take_length_delimited(&mut cursor)?;
            scan_runtime_filter_binding(binding)?;
        } else {
            skip_field(wire_type, field, &mut cursor, context.clone())?;
        }
    }
    Ok(())
}

fn scan_runtime_filter_binding(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    let mut target_field = None;
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == RUNTIME_FILTER_BINDING_PRODUCER_FIELD && wire_type == WireType::LengthDelimited
        {
            let producer = take_length_delimited(&mut cursor)?;
            scan_runtime_filter_producer_role(producer, &mut target_field)?;
        } else {
            skip_field(wire_type, field, &mut cursor, context.clone())?;
        }
    }
    Ok(())
}

fn scan_runtime_filter_producer_role(
    bytes: &[u8],
    target_field: &mut Option<u32>,
) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if matches!(
            field,
            PRODUCER_JOIN_BUILD_KEY_FIELD | PRODUCER_AGGREGATE_TOPN_KEY_FIELD
        ) && wire_type == WireType::LengthDelimited
        {
            if target_field.is_some_and(|seen| seen != field) {
                return Err(WireScanError::AmbiguousProducerBindingTarget);
            }
            *target_field = Some(field);
        }
        skip_field(wire_type, field, &mut cursor, context.clone())?;
    }
    Ok(())
}

fn take_length_delimited<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8], WireScanError> {
    let length = decode_varint(cursor)?;
    if length > cursor.remaining() as u64 {
        return Err(prost::DecodeError::new("buffer underflow").into());
    }
    let length = length as usize;
    let (payload, remaining) = cursor.split_at(length);
    *cursor = remaining;
    Ok(payload)
}

fn scan_instance_params(bytes: &[u8]) -> Result<(), WireScanError> {
    let mut cursor = bytes;
    let context = DecodeContext::default();
    while cursor.has_remaining() {
        let (field, wire_type) = decode_key(&mut cursor)?;
        if field == LEGACY_RUNTIME_FILTER_PARAMS_FIELD {
            return Err(WireScanError::RetiredInstanceParamsField);
        }
        skip_field(wire_type, field, &mut cursor, context.clone())?;
    }
    Ok(())
}

#[derive(Debug)]
enum WireScanError {
    Decode(prost::DecodeError),
    RetiredInstanceParamsField,
    AmbiguousProducerBindingTarget,
}

impl From<prost::DecodeError> for WireScanError {
    fn from(error: prost::DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl std::fmt::Display for WireScanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(error) => error.fmt(formatter),
            Self::RetiredInstanceParamsField => formatter.write_str(
                "retired InstanceParams tag 7 runtime_filter_params is not accepted by native submission",
            ),
            Self::AmbiguousProducerBindingTarget => formatter.write_str(
                "native runtime-filter producer target carries both join_build_key and aggregate_topn_key",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validate_submit_fragment_request_wire;
    use prost::Message;

    fn submit_with_instance(instance: &[u8]) -> Vec<u8> {
        assert!(instance.len() < 128);
        let mut request = vec![0x12, instance.len() as u8];
        request.extend_from_slice(instance);
        request
    }

    fn length_delimited(field: u32, payload: &[u8]) -> Vec<u8> {
        assert!(field <= 15);
        assert!(payload.len() < 128);
        let mut wire = vec![((field << 3) | 2) as u8, payload.len() as u8];
        wire.extend_from_slice(payload);
        wire
    }

    #[test]
    fn legacy_instance_params_tag_seven_is_rejected_for_every_valid_wire_type() {
        for wire_type in 0_u8..=5 {
            let key = ((7_u32 << 3) | u32::from(wire_type)) as u8;
            let request = submit_with_instance(&[key]);
            let error = validate_submit_fragment_request_wire(&request)
                .expect_err("tag 7 must be rejected before its value is decoded");
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert!(error.message().contains("tag 7"), "{error}");
        }
    }

    #[test]
    fn scanner_skips_supported_wire_types_without_interpreting_payloads() {
        let instance = [
            0x08, 0x96, 0x01, // field 1, varint
            0x11, 1, 2, 3, 4, 5, 6, 7, 8, // field 2, fixed64
            0x1a, 0x03, 9, 10, 11, // field 3, length-delimited
            0x25, 12, 13, 14, 15, // field 4, fixed32
        ];
        validate_submit_fragment_request_wire(&submit_with_instance(&instance))
            .expect("supported fields must delegate to Prost");
    }

    #[test]
    fn unknown_group_is_transparent_to_scanner_and_prost() {
        let request = submit_with_instance(&[
            0x5b, 0x08, 1, 0x5c, // unknown field 11, group containing field 1
        ]);
        validate_submit_fragment_request_wire(&request)
            .expect("unknown group must be skipped by the guard");
        crate::proto::novarocks::SubmitFragmentRequest::decode(request.as_slice())
            .expect("unknown groups must remain transparent to Prost");
    }

    #[test]
    fn repeated_instance_params_rejects_if_any_occurrence_contains_legacy_tag() {
        let mut request = submit_with_instance(&[0x08, 1]);
        request.extend_from_slice(&submit_with_instance(&[0x3a, 0]));
        let error = validate_submit_fragment_request_wire(&request)
            .expect_err("every InstanceParams occurrence must be inspected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn outer_tag_seven_is_not_confused_with_retired_instance_params_tag() {
        let mut request = vec![0x3a, 0];
        request.extend_from_slice(&submit_with_instance(&[0x08, 1]));
        validate_submit_fragment_request_wire(&request)
            .expect("only direct InstanceParams tag 7 is retired");
    }

    #[test]
    fn producer_binding_target_rejects_ambiguous_raw_oneof_before_prost_collapse() {
        let mut producer = length_delimited(3, &[]);
        producer.extend_from_slice(&length_delimited(4, &[]));
        let binding = length_delimited(8, &producer);
        let table = length_delimited(2, &binding);
        let plan = length_delimited(10, &table);
        let request = length_delimited(1, &plan);

        let decoded = crate::proto::novarocks::SubmitFragmentRequest::decode(request.as_slice())
            .expect("Prost accepts both oneof target tags");
        let target = decoded
            .plan
            .and_then(|plan| plan.runtime_filter_bindings)
            .and_then(|table| table.bindings.into_iter().next())
            .and_then(|binding| binding.role)
            .and_then(|role| match role {
                crate::proto::plan::runtime_filter_binding::Role::Producer(role) => role.target,
                crate::proto::plan::runtime_filter_binding::Role::Consumer(_) => None,
            });
        assert!(matches!(
            target,
            Some(crate::proto::plan::runtime_filter_producer_role::Target::AggregateTopnKey(_))
        ));

        let error = validate_submit_fragment_request_wire(&request)
            .expect_err("raw wire carrying both producer target arms must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("join_build_key"), "{error}");
        assert!(error.message().contains("aggregate_topn_key"), "{error}");
    }

    #[test]
    fn malformed_submit_wire_is_bounded_and_never_panics() {
        let malformed = [
            vec![0x12, 0x80],
            vec![0x12, 0x04, 0x08],
            vec![0x12, 0x01, 0x0c],
            vec![0x12, 0x02, 0x0b, 0x14],
            vec![0x00],
            vec![0x80; 11],
            vec![0x12, 0x01, 0x0e],
        ];
        for bytes in malformed {
            let outcome =
                std::panic::catch_unwind(|| validate_submit_fragment_request_wire(&bytes));
            let result = outcome.expect("wire scanner must not panic");
            let error = result.expect_err("malformed bytes must retain Prost rejection semantics");
            assert_eq!(error.code(), tonic::Code::Internal, "bytes={bytes:?}");
        }
    }
}
