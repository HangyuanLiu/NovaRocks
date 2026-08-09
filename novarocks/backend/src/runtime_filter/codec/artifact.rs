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

//! Canonical Backend artifact-delivery (`NRFA`) envelope.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use novarocks_execution::runtime_filter::{
    LogicalVersion, RuntimeFilterMembershipSchema, UnavailableReason,
};

use crate::runtime_filter::artifact::{ArtifactBundle, ArtifactKind, ConsumerArtifactProfile};
use crate::runtime_filter::codec::leaf::{self, ArtifactDecodeExpectations};

const MAGIC: &[u8; 4] = b"NRFA";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 56;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedArtifactFrame {
    profile_digest: [u8; 32],
    payload: Vec<u8>,
}
impl EncodedArtifactFrame {
    pub(crate) const fn profile_digest(&self) -> &[u8; 32] {
        &self.profile_digest
    }
    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ArtifactDecodeExpectation<'a> {
    pub(crate) profile: &'a ConsumerArtifactProfile,
    pub(crate) schema: &'a RuntimeFilterMembershipSchema,
    /// Ordered artifacts carry their order-contract digest in the NRFA schema
    /// slot. Membership artifacts continue to use `schema`; callers must
    /// explicitly supply this contract before a Range bundle can be decoded.
    pub(crate) order_contract:
        Option<&'a novarocks_execution::runtime_filter::contribution::RuntimeOrderContract>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactWireCodecError {
    Malformed,
    Truncated,
    UnknownVersion,
    UnknownKind,
    UnknownReason,
    UnknownArtifactKind,
    InvalidFlags,
    KindMismatch,
    KindNotAccepted,
    ProfileMismatch,
    SchemaMismatch,
    LengthOverflow,
    TrailingBytes,
    NonCanonicalPayload,
    EncodedSizeExceeded,
    ResourceLimit,
}
impl fmt::Display for ArtifactWireCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid runtime filter artifact frame: {self:?}")
    }
}
impl Error for ArtifactWireCodecError {}

pub(crate) fn max_encoded_len_for_artifact_budget(
    max_semantic_bytes: usize,
) -> Result<usize, ArtifactWireCodecError> {
    HEADER_LEN
        .checked_add(max_semantic_bytes)
        .ok_or(ArtifactWireCodecError::LengthOverflow)
}

pub(crate) fn encode_artifact_bundle(
    bundle: &ArtifactBundle,
    expectation: ArtifactDecodeExpectation<'_>,
    max_encoded: usize,
) -> Result<EncodedArtifactFrame, ArtifactWireCodecError> {
    if bundle.profile_id() != expectation.profile.id() {
        return Err(ArtifactWireCodecError::ProfileMismatch);
    }
    let [(first_kind, first)] = &bundle.artifacts()[..1] else {
        return Err(ArtifactWireCodecError::Malformed);
    };
    let _ = first_kind;
    let mut body = Vec::new();
    body.extend_from_slice(&bundle.channel_id().to_be_bytes());
    body.extend_from_slice(&first.schema_digest().bytes());
    body.extend_from_slice(
        &(u16::try_from(bundle.artifacts().len())
            .map_err(|_| ArtifactWireCodecError::LengthOverflow)?)
        .to_be_bytes(),
    );
    for (kind, artifact) in bundle.artifacts() {
        body.push(kind.tag());
        body.extend_from_slice(
            &(u64::try_from(artifact.canonical_bytes().len())
                .map_err(|_| ArtifactWireCodecError::LengthOverflow)?)
            .to_be_bytes(),
        );
        body.extend_from_slice(artifact.canonical_bytes());
    }
    let total = HEADER_LEN
        .checked_add(body.len())
        .ok_or(ArtifactWireCodecError::LengthOverflow)?;
    if total > max_encoded {
        return Err(ArtifactWireCodecError::EncodedSizeExceeded);
    }
    let payload = frame(
        FrameKind::Bundle,
        expectation.profile.id().bytes(),
        bundle.version(),
        &body,
    )?;
    Ok(EncodedArtifactFrame {
        profile_digest: expectation.profile.id().bytes(),
        payload,
    })
}

pub(crate) fn decode_artifact_bundle(
    payload: &[u8],
    envelope_digest: &[u8; 32],
    expectation: ArtifactDecodeExpectation<'_>,
    max_encoded: usize,
) -> Result<Arc<ArtifactBundle>, ArtifactWireCodecError> {
    if payload.len() > max_encoded {
        return Err(ArtifactWireCodecError::EncodedSizeExceeded);
    }
    let (kind, header_digest, version, body) = parse_frame(payload)?;
    if kind != FrameKind::Bundle {
        return Err(ArtifactWireCodecError::KindMismatch);
    }
    if header_digest != *envelope_digest || header_digest != expectation.profile.id().bytes() {
        return Err(ArtifactWireCodecError::ProfileMismatch);
    }
    let mut r = Reader::new(body);
    let channel_id = r.u32()?;
    let schema_digest = r.array::<32>()?;
    let count = usize::from(r.u16()?);
    let mut artifacts = Vec::new();
    artifacts
        .try_reserve_exact(count)
        .map_err(|_| ArtifactWireCodecError::ResourceLimit)?;
    for _ in 0..count {
        let artifact_kind =
            ArtifactKind::from_tag(r.u8()?).ok_or(ArtifactWireCodecError::UnknownArtifactKind)?;
        if !expectation.profile.accepts(artifact_kind) {
            return Err(ArtifactWireCodecError::KindNotAccepted);
        }
        let expected_schema_digest = match artifact_kind {
            ArtifactKind::Range => {
                let contract = expectation
                    .order_contract
                    .ok_or(ArtifactWireCodecError::SchemaMismatch)?;
                if expectation.profile.order_contract_digest() != Some(contract.digest()) {
                    return Err(ArtifactWireCodecError::ProfileMismatch);
                }
                contract.digest()
            }
            _ => expectation.schema.digest(),
        };
        if schema_digest != expected_schema_digest {
            return Err(ArtifactWireCodecError::SchemaMismatch);
        }
        let len = usize::try_from(r.u64()?).map_err(|_| ArtifactWireCodecError::LengthOverflow)?;
        let bytes = r.take(len)?;
        let hash = if artifact_kind == ArtifactKind::Bloom {
            expectation.profile.bloom_hash_contract()
        } else {
            None
        };
        let artifact = match artifact_kind {
            ArtifactKind::Range => {
                let contract = expectation
                    .order_contract
                    .ok_or(ArtifactWireCodecError::SchemaMismatch)?;
                crate::runtime_filter::materializer::range::decode_range_leaf(
                    bytes,
                    contract,
                    version,
                    max_encoded,
                )
                .map(|(artifact, _)| artifact)
            }
            _ => leaf::decode_leaf(
                bytes,
                ArtifactDecodeExpectations {
                    expected_kind: artifact_kind,
                    schema: expectation.schema,
                    expected_logical_version: version,
                    expected_hash_contract: hash,
                },
                max_encoded,
            ),
        }
        .map_err(|_| ArtifactWireCodecError::NonCanonicalPayload)?;
        artifacts.push((artifact_kind, artifact));
    }
    if !r.empty() {
        return Err(ArtifactWireCodecError::TrailingBytes);
    }
    let bundle = ArtifactBundle::new(
        channel_id,
        version,
        expectation.profile,
        artifacts,
        max_encoded,
    )
    .map_err(|_| ArtifactWireCodecError::NonCanonicalPayload)?;
    let reencoded = encode_artifact_bundle(&bundle, expectation, max_encoded)?;
    if reencoded.payload != payload {
        return Err(ArtifactWireCodecError::NonCanonicalPayload);
    }
    Ok(Arc::new(bundle))
}

pub(crate) fn encode_unavailable(
    reason: UnavailableReason,
    profile: &ConsumerArtifactProfile,
    max_encoded: usize,
) -> Result<EncodedArtifactFrame, ArtifactWireCodecError> {
    let payload = frame(
        FrameKind::Unavailable,
        profile.id().bytes(),
        LogicalVersion::new(0),
        &[reason_tag(reason)],
    )?;
    if payload.len() > max_encoded {
        return Err(ArtifactWireCodecError::EncodedSizeExceeded);
    }
    Ok(EncodedArtifactFrame {
        profile_digest: profile.id().bytes(),
        payload,
    })
}

pub(crate) fn decode_unavailable(
    payload: &[u8],
    envelope_digest: &[u8; 32],
    profile: &ConsumerArtifactProfile,
    max_encoded: usize,
) -> Result<UnavailableReason, ArtifactWireCodecError> {
    if payload.len() > max_encoded {
        return Err(ArtifactWireCodecError::EncodedSizeExceeded);
    }
    let (kind, digest, version, body) = parse_frame(payload)?;
    if kind != FrameKind::Unavailable {
        return Err(ArtifactWireCodecError::KindMismatch);
    }
    if version.get() != 0 || digest != *envelope_digest || digest != profile.id().bytes() {
        return Err(ArtifactWireCodecError::ProfileMismatch);
    }
    if body.len() != 1 {
        return Err(ArtifactWireCodecError::Malformed);
    }
    let reason = reason_from_tag(body[0]).ok_or(ArtifactWireCodecError::UnknownReason)?;
    if encode_unavailable(reason, profile, max_encoded)?.payload != payload {
        return Err(ArtifactWireCodecError::NonCanonicalPayload);
    }
    Ok(reason)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameKind {
    Bundle,
    Unavailable,
}
impl FrameKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Bundle => 1,
            Self::Unavailable => 2,
        }
    }
    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Bundle),
            2 => Some(Self::Unavailable),
            _ => None,
        }
    }
}
fn frame(
    kind: FrameKind,
    profile: [u8; 32],
    version: LogicalVersion,
    body: &[u8],
) -> Result<Vec<u8>, ArtifactWireCodecError> {
    let mut payload = Vec::with_capacity(
        HEADER_LEN
            .checked_add(body.len())
            .ok_or(ArtifactWireCodecError::LengthOverflow)?,
    );
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&VERSION.to_be_bytes());
    payload.push(kind.tag());
    payload.push(0);
    payload.extend_from_slice(&profile);
    payload.extend_from_slice(&version.get().to_be_bytes());
    payload.extend_from_slice(
        &(u64::try_from(body.len()).map_err(|_| ArtifactWireCodecError::LengthOverflow)?)
            .to_be_bytes(),
    );
    payload.extend_from_slice(body);
    Ok(payload)
}
fn parse_frame(
    payload: &[u8],
) -> Result<(FrameKind, [u8; 32], LogicalVersion, &[u8]), ArtifactWireCodecError> {
    let mut r = Reader::new(payload);
    if r.take(4)? != MAGIC {
        return Err(ArtifactWireCodecError::Malformed);
    }
    if r.u16()? != VERSION {
        return Err(ArtifactWireCodecError::UnknownVersion);
    }
    let kind = FrameKind::from_tag(r.u8()?).ok_or(ArtifactWireCodecError::UnknownKind)?;
    if r.u8()? != 0 {
        return Err(ArtifactWireCodecError::InvalidFlags);
    }
    let digest = r.array()?;
    let version = LogicalVersion::new(r.u64()?);
    let len = usize::try_from(r.u64()?).map_err(|_| ArtifactWireCodecError::LengthOverflow)?;
    let body = r.take(len)?;
    if !r.empty() {
        return Err(ArtifactWireCodecError::TrailingBytes);
    }
    Ok((kind, digest, version, body))
}
fn reason_tag(reason: UnavailableReason) -> u8 {
    match reason {
        UnavailableReason::ResourceLimit => 1,
        UnavailableReason::IncompleteCoverage => 2,
        UnavailableReason::ProducerFailed => 3,
        UnavailableReason::MaterializationFailed => 4,
        UnavailableReason::RouteUnavailable => 5,
    }
}
fn reason_from_tag(tag: u8) -> Option<UnavailableReason> {
    match tag {
        1 => Some(UnavailableReason::ResourceLimit),
        2 => Some(UnavailableReason::IncompleteCoverage),
        3 => Some(UnavailableReason::ProducerFailed),
        4 => Some(UnavailableReason::MaterializationFailed),
        5 => Some(UnavailableReason::RouteUnavailable),
        _ => None,
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], ArtifactWireCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ArtifactWireCodecError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ArtifactWireCodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, ArtifactWireCodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ArtifactWireCodecError> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, ArtifactWireCodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, ArtifactWireCodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ArtifactWireCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ArtifactWireCodecError::Truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_filter::{artifact::*, codec::leaf::encode_membership_leaf};
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::{
        RuntimeFilterNullSemantics,
        contribution::{
            MembershipValues, OrderedScalar, OrderedTuple, RuntimeOrderContract, RuntimeOrderKey,
            RuntimeOrderNullOrder, RuntimeOrderSortDirection, ValueDomainDelta,
        },
    };
    use std::collections::BTreeSet;
    #[test]
    fn nrfa_bundle_round_trips_canonically() {
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap();
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let leaf = encode_membership_leaf(
            &ValueDomainDelta::new(MembershipValues::int64([3, 9]), false),
            &schema,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let physical = leaf::decode_leaf(
            &leaf,
            ArtifactDecodeExpectations {
                expected_kind: ArtifactKind::ValueSet,
                schema: &schema,
                expected_logical_version: LogicalVersion::FIRST,
                expected_hash_contract: None,
            },
            4096,
        )
        .unwrap();
        let bundle = ArtifactBundle::new(
            7,
            LogicalVersion::FIRST,
            &profile,
            vec![(ArtifactKind::ValueSet, physical)],
            4096,
        )
        .unwrap();
        let expectation = ArtifactDecodeExpectation {
            profile: &profile,
            schema: &schema,
            order_contract: None,
        };
        let frame = encode_artifact_bundle(&bundle, expectation, 4096).unwrap();
        assert_eq!(&frame.payload()[..4], b"NRFA");
        assert_eq!(
            decode_artifact_bundle(frame.payload(), frame.profile_digest(), expectation, 4096)
                .unwrap()
                .channel_id(),
            7
        );
    }

    #[test]
    fn nrfa_bytes_match_the_v1_golden() {
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap();
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let leaf = encode_membership_leaf(
            &ValueDomainDelta::new(MembershipValues::int64([3, 9]), false),
            &schema,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let physical = leaf::decode_leaf(
            &leaf,
            ArtifactDecodeExpectations {
                expected_kind: ArtifactKind::ValueSet,
                schema: &schema,
                expected_logical_version: LogicalVersion::FIRST,
                expected_hash_contract: None,
            },
            4096,
        )
        .unwrap();
        let bundle = ArtifactBundle::new(
            7,
            LogicalVersion::FIRST,
            &profile,
            vec![(ArtifactKind::ValueSet, physical)],
            4096,
        )
        .unwrap();
        let backend = encode_artifact_bundle(
            &bundle,
            ArtifactDecodeExpectation {
                profile: &profile,
                schema: &schema,
                order_contract: None,
            },
            4096,
        )
        .unwrap();

        assert_eq!(
            backend.profile_digest(),
            &[
                0xf6, 0x13, 0x05, 0x9c, 0xfb, 0xa2, 0xcf, 0x12, 0x7d, 0xd8, 0x64, 0x4d, 0xf2, 0x40,
                0x7b, 0x04, 0x72, 0x88, 0x2b, 0x5b, 0xe6, 0x67, 0x49, 0x97, 0xc8, 0xe0, 0xfe, 0xa1,
                0x12, 0x99, 0xb2, 0x0f,
            ]
        );
        assert_eq!(
            backend.payload(),
            hex_bytes(
                "4e52464100010100f613059cfba2cf127dd8644df2407b0472882b5be6674997c8e0fea11299b20f000000000000000100000000000000ae000000073e87cf7b4c695573789dcd308efb51da3696ddd9e900e417e9ec460463254f0a000101000000000000007f4e52464c0001013e87cf7b4c695573789dcd308efb51da3696ddd9e900e417e9ec460463254f0a002b6e6f7661726f636b732e72756e74696d652d66696c7465722e61727469666163742d736368656d6101050100000000000000010000000000000000001905000000000000000200000000000000030000000000000009"
            )
        );
    }

    fn hex_bytes(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0, "golden hex must have complete bytes");
        (0..hex.len())
            .step_by(2)
            .map(|offset| u8::from_str_radix(&hex[offset..offset + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn nrfa_range_bundle_requires_and_uses_the_frozen_order_contract() {
        let contract = RuntimeOrderContract::from_frozen(
            [RuntimeOrderKey::with_order(
                DataType::Int64,
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::Last,
            )],
            [3; 32],
            [7; 32],
        );
        let profile = ConsumerArtifactProfile::new_ordered_range(contract.digest()).unwrap();
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let bundle = crate::runtime_filter::materializer::range::materialize_range(
            9,
            &contract,
            &OrderedTuple::new([Some(OrderedScalar::Int64(11))]),
            LogicalVersion::FIRST,
            &profile,
            &crate::runtime_filter::materializer::MaterializationAdmission::new(4096),
        )
        .unwrap();
        let expectation = ArtifactDecodeExpectation {
            profile: &profile,
            schema: &schema,
            order_contract: Some(&contract),
        };
        let frame = encode_artifact_bundle(&bundle, expectation, 4096).unwrap();
        assert!(
            decode_artifact_bundle(frame.payload(), frame.profile_digest(), expectation, 4096)
                .unwrap()
                .artifacts()[0]
                .1
                .range_data()
                .is_some()
        );
        assert!(matches!(
            decode_artifact_bundle(
                frame.payload(),
                frame.profile_digest(),
                ArtifactDecodeExpectation {
                    profile: &profile,
                    schema: &schema,
                    order_contract: None,
                },
                4096,
            ),
            Err(ArtifactWireCodecError::SchemaMismatch)
        ));
    }
}
