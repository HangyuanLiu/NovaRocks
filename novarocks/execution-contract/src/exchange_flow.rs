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

//! Exact exchange facts. Permits grant capacity, never delivery or completion.

use std::fmt;

use novarocks_types::UniqueId;

use crate::{ExchangeSource, FragmentNodeId, TaskIdentity};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeFlowContractError {
    CrossAttemptStream,
    NegativeDestinationNode,
    EndFrameGeometry,
    NegativeSequence,
    SequenceExhausted,
    GeometryOverflow,
    EmptyPermitToken,
}

impl fmt::Display for ExchangeFlowContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CrossAttemptStream => "exchange stream crosses query attempts",
            Self::NegativeDestinationNode => "exchange destination node must be nonnegative",
            Self::EndFrameGeometry => "exchange End frame requires zero payload geometry",
            Self::NegativeSequence => "exchange sequence must be nonnegative",
            Self::SequenceExhausted => "exchange sequence domain is exhausted",
            Self::GeometryOverflow => "exchange frame geometry overflows its byte domain",
            Self::EmptyPermitToken => "exchange permit token must be nonzero",
        })
    }
}
impl std::error::Error for ExchangeFlowContractError {}

/// One source-to-input stream; endpoint reuse never changes its identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExchangeStreamIdentity {
    source: ExchangeSource,
    destination: TaskIdentity,
    destination_fragment_instance_id: UniqueId,
    destination_node: FragmentNodeId,
}

impl ExchangeStreamIdentity {
    pub fn try_new(
        source: ExchangeSource,
        destination: TaskIdentity,
        destination_fragment_instance_id: UniqueId,
        destination_node: FragmentNodeId,
    ) -> Result<Self, ExchangeFlowContractError> {
        if source.task().query_execution_id() != destination.query_execution_id() {
            return Err(ExchangeFlowContractError::CrossAttemptStream);
        }
        if destination_node.get() < 0 {
            return Err(ExchangeFlowContractError::NegativeDestinationNode);
        }
        Ok(Self {
            source,
            destination,
            destination_fragment_instance_id,
            destination_node,
        })
    }
    pub const fn source(self) -> ExchangeSource {
        self.source
    }
    pub const fn destination(self) -> TaskIdentity {
        self.destination
    }
    pub const fn destination_fragment_instance_id(self) -> UniqueId {
        self.destination_fragment_instance_id
    }
    pub const fn destination_node(self) -> FragmentNodeId {
        self.destination_node
    }
}

/// Allocated at the exact stream's serialized admission point, including EOS.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExchangeFrameSequence(i64);

impl ExchangeFrameSequence {
    pub const FIRST: Self = Self(0);
    pub fn try_new(sequence: i64) -> Result<Self, ExchangeFlowContractError> {
        if sequence < 0 {
            return Err(ExchangeFlowContractError::NegativeSequence);
        }
        Ok(Self(sequence))
    }
    pub const fn get(self) -> i64 {
        self.0
    }
    pub fn checked_next(self) -> Result<Self, ExchangeFlowContractError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ExchangeFlowContractError::SequenceExhausted)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeDeliveryRequirement {
    DemandDriven,
    MustDrain,
}

/// Authored by actual input exhaustion, not inferred from Task status or ACK.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeCloseDisposition {
    Consumed {
        final_sequence: ExchangeFrameSequence,
    },
    Withdrawn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeStreamClose {
    pub stream: ExchangeStreamIdentity,
    pub disposition: ExchangeCloseDisposition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeFrameKind {
    Data,
    LogicalSummary,
    WriterResults,
    End,
}

/// Bounds declared before payload allocation and checked against actual IPC.
/// Final decoded backing belongs to Q; raw and scratch belong to transient R.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeFrameGeometry {
    wire_bytes: u64,
    decoded_bytes: u64,
    decode_scratch_bytes: u64,
    staging_bytes: u64,
    peak_bytes: u64,
}

impl ExchangeFrameGeometry {
    pub fn try_new(
        wire_bytes: u64,
        decoded_bytes: u64,
        decode_scratch_bytes: u64,
    ) -> Result<Self, ExchangeFlowContractError> {
        let staging_bytes = wire_bytes
            .checked_add(decode_scratch_bytes)
            .ok_or(ExchangeFlowContractError::GeometryOverflow)?;
        let peak_bytes = staging_bytes
            .checked_add(decoded_bytes)
            .ok_or(ExchangeFlowContractError::GeometryOverflow)?;
        Ok(Self {
            wire_bytes,
            decoded_bytes,
            decode_scratch_bytes,
            staging_bytes,
            peak_bytes,
        })
    }
    pub const fn wire_bytes(self) -> u64 {
        self.wire_bytes
    }
    pub const fn decoded_bytes(self) -> u64 {
        self.decoded_bytes
    }
    pub const fn decode_scratch_bytes(self) -> u64 {
        self.decode_scratch_bytes
    }
    pub const fn staging_bytes(self) -> u64 {
        self.staging_bytes
    }
    pub const fn peak_bytes(self) -> u64 {
        self.peak_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangePermitRequest {
    pub stream: ExchangeStreamIdentity,
    pub sequence: ExchangeFrameSequence,
    pub kind: ExchangeFrameKind,
    pub geometry: ExchangeFrameGeometry,
}

impl ExchangePermitRequest {
    /// Validate kind-specific geometry before issuing or using a permit.
    /// Other kinds may carry a legitimate empty batch; zero bytes are not a
    /// missing-field sentinel in the wire representation.
    pub fn validate(self) -> Result<(), ExchangeFlowContractError> {
        if self.kind == ExchangeFrameKind::End && self.geometry.peak_bytes() != 0 {
            return Err(ExchangeFlowContractError::EndFrameGeometry);
        }
        Ok(())
    }
}

/// Opaque and receiver-scoped. Identity alone never grants capacity.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct ExchangePermitToken([u8; 16]);

impl ExchangePermitToken {
    pub fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, ExchangeFlowContractError> {
        if bytes == [0; 16] {
            return Err(ExchangeFlowContractError::EmptyPermitToken);
        }
        Ok(Self(bytes))
    }
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}
impl fmt::Debug for ExchangePermitToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExchangePermitToken([redacted])")
    }
}

/// A permit names the complete request. Expiry is owned by the receiver clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangePermitGrant {
    pub request: ExchangePermitRequest,
    pub token: ExchangePermitToken,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_exhaustion_and_negative_input_are_not_wrapped() {
        assert_eq!(
            ExchangeFrameSequence::try_new(-1),
            Err(ExchangeFlowContractError::NegativeSequence)
        );
        assert_eq!(
            ExchangeFrameSequence::FIRST.checked_next().unwrap().get(),
            1
        );
        assert_eq!(
            ExchangeFrameSequence::try_new(i64::MAX)
                .unwrap()
                .checked_next(),
            Err(ExchangeFlowContractError::SequenceExhausted)
        );
    }

    #[test]
    fn geometry_keeps_long_lived_queue_and_transient_staging_disjoint() {
        let g = ExchangeFrameGeometry::try_new(7, 23, 11).unwrap();
        assert_eq!(g.staging_bytes(), 18);
        assert_eq!(g.decoded_bytes(), 23);
        assert_eq!(g.peak_bytes(), 41);
        assert_eq!(
            ExchangeFrameGeometry::try_new(u64::MAX, 1, 0),
            Err(ExchangeFlowContractError::GeometryOverflow)
        );
        assert_eq!(
            ExchangeFrameGeometry::try_new(u64::MAX, 0, 1),
            Err(ExchangeFlowContractError::GeometryOverflow)
        );
    }
}
