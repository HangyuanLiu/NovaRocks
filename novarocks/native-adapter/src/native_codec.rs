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
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::resource_preflight;
use prost::Message;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

/// Native protobuf codec used by generated NovaRocks clients and servers.
#[derive(Debug, Clone)]
pub struct NativeProstCodec<T, U> {
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
pub struct NativeProstEncoder<T> {
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
pub struct NativeProstDecoder<U> {
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
        // Tonic has assembled one frame, but prost has not allocated its tree.
        // DecodeBuf is backed by a contiguous BytesMut. If that invariant ever
        // changes, fail closed instead of checking only a prefix of the frame.
        let raw = source.chunk();
        if raw.len() != source.remaining() {
            return Err(Status::internal("native codec received a split frame"));
        }
        check_resource_shape::<U>(raw)?;
        U::decode(source)
            .map(Some)
            .map_err(|error| Status::internal(error.to_string()))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

fn check_resource_shape<U: 'static>(raw: &[u8]) -> Result<(), Status> {
    let result = if TypeId::of::<U>() == TypeId::of::<proto::ApplyTaskOperationsRequest>() {
        resource_preflight::check_operation_batch(raw)
    } else if TypeId::of::<U>() == TypeId::of::<proto::ApplyTaskControlOperationsRequest>() {
        resource_preflight::check_control_operation_batch(raw)
    } else if TypeId::of::<U>() == TypeId::of::<proto::SubscribeTaskStatusRequest>() {
        resource_preflight::check_status_subscription(raw)
    } else {
        Ok(())
    };
    result.map_err(|error| Status::resource_exhausted(format!("native codec preflight: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoder_dispatches_resource_checks_by_message_type() {
        let raw = proto::ApplyTaskOperationsRequest {
            operations: vec![proto::TaskOperation::default(); 33],
        }
        .encode_to_vec();
        assert_eq!(
            check_resource_shape::<proto::ApplyTaskOperationsRequest>(&raw)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert!(check_resource_shape::<proto::HeartbeatRequest>(&raw).is_ok());

        let control = proto::ApplyTaskControlOperationsRequest {
            operations: vec![proto::TaskControlOperation::default(); 33],
        }
        .encode_to_vec();
        assert_eq!(
            check_resource_shape::<proto::ApplyTaskControlOperationsRequest>(&control)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
    }
}
