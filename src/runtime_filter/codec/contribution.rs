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

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};

use crate::common::largeint::LARGEINT_BYTE_WIDTH;
use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
use crate::runtime_filter::port::final_domain::{
    CompletionFence, FinalDomainError, FinalDomainShard, RuntimeCompletionFenceContract,
};
use crate::runtime_filter::port::identity::{ProducerSequence, ProducerStreamId};
use crate::runtime_filter::port::ordered_bound::{
    OrderedBoundUpdate, OrderedScalar, OrderedTuple, RuntimeOrderContract,
};
use crate::runtime_filter::port::producer::RuntimeContractViolationKind;
use crate::runtime_filter::port::topk_summary::{RuntimeTopKSummaryContract, TopKSummary};
use crate::runtime_filter::port::value_domain::{
    ContributionSizeError, FINGERPRINT_VERSION_TAG, MembershipValues, ValueDomainDelta,
};

const MAGIC: &[u8; 4] = b"NRFC";
const CODEC_VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 1 + 1 + 32 + 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WireContributionKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

impl WireContributionKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Membership => 1,
            Self::OrderedBound => 2,
            Self::TopKSummary => 3,
            Self::FinalDomain => 4,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Membership),
            2 => Some(Self::OrderedBound),
            3 => Some(Self::TopKSummary),
            4 => Some(Self::FinalDomain),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterContribution {
    Membership(ValueDomainDelta),
    OrderedBound(OrderedBoundUpdate),
    TopKSummary(TopKSummary),
    FinalDomain(FinalDomainShard),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ContributionCodecExpectation<'a> {
    Membership(&'a ArtifactMembershipSchema),
    OrderedBound(&'a RuntimeOrderContract),
    TopKSummary(&'a RuntimeTopKSummaryContract),
    FinalDomain {
        contract: &'a RuntimeCompletionFenceContract,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedContribution {
    schema_digest: [u8; 32],
    payload: Vec<u8>,
}

impl EncodedContribution {
    pub(crate) const fn schema_digest(&self) -> &[u8; 32] {
        &self.schema_digest
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub(crate) fn into_parts(self) -> ([u8; 32], Vec<u8>) {
        (self.schema_digest, self.payload)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContributionCodecError {
    Malformed,
    Truncated,
    UnknownVersion,
    UnknownKind,
    InvalidFlags,
    KindMismatch,
    SchemaMismatch,
    LengthOverflow,
    TrailingBytes,
    NonCanonicalPayload,
    EncodedSizeExceeded,
    ResourceLimit,
}

impl fmt::Display for ContributionCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid runtime filter contribution: {self:?}")
    }
}

impl Error for ContributionCodecError {}

impl From<ContributionSizeError> for ContributionCodecError {
    fn from(_error: ContributionSizeError) -> Self {
        Self::LengthOverflow
    }
}

pub(crate) fn encoded_contribution_len(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<usize, ContributionCodecError> {
    let body_len = match (contribution, expectation) {
        (
            RuntimeFilterContribution::Membership(delta),
            ContributionCodecExpectation::Membership(schema),
        ) => {
            if !delta.matches_data_type(schema.data_type()) {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            delta.canonical_encoded_len()?
        }
        (
            RuntimeFilterContribution::OrderedBound(update),
            ContributionCodecExpectation::OrderedBound(contract),
        ) => {
            if update.order_contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            update.canonical_contribution_len()?
        }
        (
            RuntimeFilterContribution::TopKSummary(summary),
            ContributionCodecExpectation::TopKSummary(contract),
        ) => {
            if summary.contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            summary.canonical_body_len()?
        }
        (
            RuntimeFilterContribution::FinalDomain(shard),
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream,
                sequence,
            },
        ) => {
            verify_final_domain_scope(shard, contract, stream, sequence)?;
            size_of::<[u8; 32]>()
                .checked_add(shard.domain().canonical_encoded_len()?)
                .ok_or(ContributionCodecError::LengthOverflow)?
        }
        _ => return Err(ContributionCodecError::KindMismatch),
    };
    encoded_frame_len_from_body_len(body_len)
}

pub(crate) fn encode_contribution(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
) -> Result<EncodedContribution, ContributionCodecError> {
    encode_contribution_with_allocator(
        contribution,
        expectation,
        max_encoded_bytes,
        &SystemContributionFrameAllocator,
    )
}

pub(crate) fn decode_contribution(
    payload: &[u8],
    envelope_schema_digest: &[u8; 32],
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
) -> Result<RuntimeFilterContribution, ContributionCodecError> {
    if payload.len() > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }

    let mut reader = Reader::new(payload);
    if reader.read_exact(MAGIC.len())? != MAGIC {
        return Err(ContributionCodecError::Malformed);
    }
    if reader.read_u16()? != CODEC_VERSION {
        return Err(ContributionCodecError::UnknownVersion);
    }
    let frame_kind = WireContributionKind::from_tag(reader.read_u8()?)
        .ok_or(ContributionCodecError::UnknownKind)?;
    if reader.read_u8()? != 0 {
        return Err(ContributionCodecError::InvalidFlags);
    }
    let frame_digest = reader.read_array::<32>()?;
    let body_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    match body_len.cmp(&reader.remaining_len()) {
        std::cmp::Ordering::Less => return Err(ContributionCodecError::TrailingBytes),
        std::cmp::Ordering::Greater => return Err(ContributionCodecError::Truncated),
        std::cmp::Ordering::Equal => {}
    }
    let body = reader.read_exact(body_len)?;
    debug_assert!(reader.is_empty());

    if frame_kind != expectation_kind(expectation) {
        return Err(ContributionCodecError::KindMismatch);
    }
    let installed_digest = expectation_digest(expectation);
    if frame_digest != *envelope_schema_digest
        || frame_digest != installed_digest
        || *envelope_schema_digest != installed_digest
    {
        return Err(ContributionCodecError::SchemaMismatch);
    }

    let contribution = match (frame_kind, expectation) {
        (WireContributionKind::Membership, ContributionCodecExpectation::Membership(schema)) => {
            RuntimeFilterContribution::Membership(decode_membership_body(body, schema.data_type())?)
        }
        (
            WireContributionKind::OrderedBound,
            ContributionCodecExpectation::OrderedBound(contract),
        ) => RuntimeFilterContribution::OrderedBound(decode_ordered_bound_body(body, contract)?),
        (
            WireContributionKind::TopKSummary,
            ContributionCodecExpectation::TopKSummary(contract),
        ) => RuntimeFilterContribution::TopKSummary(decode_topk_body(body, contract)?),
        (
            WireContributionKind::FinalDomain,
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream,
                sequence,
            },
        ) => RuntimeFilterContribution::FinalDomain(decode_final_domain_body(
            body, contract, stream, sequence,
        )?),
        _ => return Err(ContributionCodecError::KindMismatch),
    };
    let canonical = encode_contribution(&contribution, expectation, payload.len())?;
    if canonical.payload() != payload {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(contribution)
}

trait ContributionFrameAllocator {
    fn allocate(&self, exact_len: usize) -> Result<Vec<u8>, ContributionCodecError>;
}

struct SystemContributionFrameAllocator;

impl ContributionFrameAllocator for SystemContributionFrameAllocator {
    fn allocate(&self, exact_len: usize) -> Result<Vec<u8>, ContributionCodecError> {
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(exact_len)
            .map_err(|_| ContributionCodecError::ResourceLimit)?;
        Ok(payload)
    }
}

fn encode_contribution_with_allocator(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
    allocator: &impl ContributionFrameAllocator,
) -> Result<EncodedContribution, ContributionCodecError> {
    let exact_len = encoded_contribution_len(contribution, expectation)?;
    if exact_len > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }

    let (kind, schema_digest, body_len) = match (contribution, expectation) {
        (
            RuntimeFilterContribution::Membership(delta),
            ContributionCodecExpectation::Membership(schema),
        ) => (
            WireContributionKind::Membership,
            schema.digest().bytes(),
            delta.canonical_encoded_len()?,
        ),
        (
            RuntimeFilterContribution::OrderedBound(update),
            ContributionCodecExpectation::OrderedBound(contract),
        ) => {
            if update.order_contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            (
                WireContributionKind::OrderedBound,
                contract.digest().bytes(),
                update.canonical_contribution_len()?,
            )
        }
        (
            RuntimeFilterContribution::TopKSummary(summary),
            ContributionCodecExpectation::TopKSummary(contract),
        ) => {
            if summary.contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            (
                WireContributionKind::TopKSummary,
                contract.digest().bytes(),
                summary.canonical_body_len()?,
            )
        }
        (
            RuntimeFilterContribution::FinalDomain(shard),
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream,
                sequence,
            },
        ) => {
            verify_final_domain_scope(shard, contract, stream, sequence)?;
            (
                WireContributionKind::FinalDomain,
                contract.digest().bytes(),
                size_of::<[u8; 32]>()
                    .checked_add(shard.domain().canonical_encoded_len()?)
                    .ok_or(ContributionCodecError::LengthOverflow)?,
            )
        }
        _ => return Err(ContributionCodecError::KindMismatch),
    };
    let body_len = u64::try_from(body_len).map_err(|_| ContributionCodecError::LengthOverflow)?;
    let mut payload = allocator.allocate(exact_len)?;
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&CODEC_VERSION.to_be_bytes());
    payload.push(kind.tag());
    payload.push(0);
    payload.extend_from_slice(&schema_digest);
    payload.extend_from_slice(&body_len.to_be_bytes());
    match contribution {
        RuntimeFilterContribution::Membership(delta) => {
            delta.encode_canonical_into(&mut payload)?;
        }
        RuntimeFilterContribution::OrderedBound(update) => {
            update.encode_bound_canonical_into(&mut payload)?;
        }
        RuntimeFilterContribution::TopKSummary(summary) => {
            summary.encode_canonical_body_into(&mut payload)?;
        }
        RuntimeFilterContribution::FinalDomain(shard) => {
            payload.extend_from_slice(&shard.fence_digest());
            shard.domain().encode_canonical_into(&mut payload)?;
        }
    }
    debug_assert_eq!(payload.len(), exact_len);
    Ok(EncodedContribution {
        schema_digest,
        payload,
    })
}

fn encoded_frame_len_from_body_len(body_len: usize) -> Result<usize, ContributionCodecError> {
    let exact_len = HEADER_LEN
        .checked_add(body_len)
        .ok_or(ContributionCodecError::LengthOverflow)?;
    u64::try_from(body_len).map_err(|_| ContributionCodecError::LengthOverflow)?;
    u64::try_from(exact_len).map_err(|_| ContributionCodecError::LengthOverflow)?;
    Ok(exact_len)
}

const fn expectation_kind(expectation: ContributionCodecExpectation<'_>) -> WireContributionKind {
    match expectation {
        ContributionCodecExpectation::Membership(_) => WireContributionKind::Membership,
        ContributionCodecExpectation::OrderedBound(_) => WireContributionKind::OrderedBound,
        ContributionCodecExpectation::TopKSummary(_) => WireContributionKind::TopKSummary,
        ContributionCodecExpectation::FinalDomain { .. } => WireContributionKind::FinalDomain,
    }
}

fn expectation_digest(expectation: ContributionCodecExpectation<'_>) -> [u8; 32] {
    match expectation {
        ContributionCodecExpectation::Membership(schema) => schema.digest().bytes(),
        ContributionCodecExpectation::OrderedBound(contract) => contract.digest().bytes(),
        ContributionCodecExpectation::TopKSummary(contract) => contract.digest().bytes(),
        ContributionCodecExpectation::FinalDomain { contract, .. } => contract.digest().bytes(),
    }
}

fn decode_membership_body(
    body: &[u8],
    expected_data_type: &DataType,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    decode_membership_body_with_policy(
        body,
        expected_data_type,
        ContributionCodecError::NonCanonicalPayload,
    )
}

fn decode_membership_body_with_policy(
    body: &[u8],
    expected_data_type: &DataType,
    schema_mismatch_error: ContributionCodecError,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let version_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if reader.read_exact(version_len)? != FINGERPRINT_VERSION_TAG {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let values = match expected_data_type {
        DataType::Boolean => {
            expect_type_tag(&mut reader, 1, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 1)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(match reader.read_u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(ContributionCodecError::NonCanonicalPayload),
                });
            }
            MembershipValues::boolean(values)
        }
        DataType::Int8 => {
            expect_type_tag(&mut reader, 2, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 1)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i8()?);
            }
            MembershipValues::int8(values)
        }
        DataType::Int16 => {
            expect_type_tag(&mut reader, 3, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 2)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i16()?);
            }
            MembershipValues::int16(values)
        }
        DataType::Int32 => {
            expect_type_tag(&mut reader, 4, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i32()?);
            }
            MembershipValues::int32(values)
        }
        DataType::Int64 => {
            expect_type_tag(&mut reader, 5, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i64()?);
            }
            MembershipValues::int64(values)
        }
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            expect_type_tag(&mut reader, 6, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 16)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i128()?);
            }
            MembershipValues::large_int(values)
        }
        DataType::Float32 => {
            expect_type_tag(&mut reader, 7, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(f32::from_bits(reader.read_u32()?));
            }
            MembershipValues::float32(values)
        }
        DataType::Float64 => {
            expect_type_tag(&mut reader, 8, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(f64::from_bits(reader.read_u64()?));
            }
            MembershipValues::float64(values)
        }
        DataType::Utf8 => {
            expect_type_tag(&mut reader, 9, schema_mismatch_error)?;
            let count = read_count(&mut reader)?;
            ensure_count_bytes(&reader, count, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                let len = usize::try_from(reader.read_u64()?)
                    .map_err(|_| ContributionCodecError::LengthOverflow)?;
                let bytes = reader.read_exact(len)?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
                let mut owned = String::new();
                owned
                    .try_reserve_exact(len)
                    .map_err(|_| ContributionCodecError::ResourceLimit)?;
                owned.push_str(value);
                values.push(owned);
            }
            MembershipValues::utf8(values)
        }
        DataType::Date32 => {
            expect_type_tag(&mut reader, 10, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i32()?);
            }
            MembershipValues::date32(values)
        }
        DataType::Timestamp(unit, timezone) => {
            expect_type_tag(&mut reader, 11, schema_mismatch_error)?;
            let encoded_unit = reader.read_u8()?;
            if encoded_unit != time_unit_tag(unit) {
                return Err(if (1..=4).contains(&encoded_unit) {
                    schema_mismatch_error
                } else {
                    ContributionCodecError::NonCanonicalPayload
                });
            }
            match reader.read_u8()? {
                0 if timezone.is_none() => {}
                0 => return Err(schema_mismatch_error),
                1 => {
                    let len = usize::try_from(reader.read_u64()?)
                        .map_err(|_| ContributionCodecError::LengthOverflow)?;
                    let timezone_bytes = reader.read_exact(len)?;
                    std::str::from_utf8(timezone_bytes)
                        .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
                    let Some(expected_timezone) = timezone else {
                        return Err(schema_mismatch_error);
                    };
                    if timezone_bytes != expected_timezone.as_bytes() {
                        return Err(schema_mismatch_error);
                    }
                }
                _ => return Err(ContributionCodecError::NonCanonicalPayload),
            }
            let count = read_fixed_count(&mut reader, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i64()?);
            }
            MembershipValues::timestamp(unit.clone(), timezone.clone(), values)
        }
        DataType::Decimal128(precision, scale) => {
            expect_type_tag(&mut reader, 12, schema_mismatch_error)?;
            let encoded_precision = reader.read_u8()?;
            let encoded_scale = reader.read_u8()? as i8;
            if encoded_precision != *precision || encoded_scale != *scale {
                return Err(
                    if decimal_metadata_is_valid(encoded_precision, encoded_scale) {
                        schema_mismatch_error
                    } else {
                        ContributionCodecError::NonCanonicalPayload
                    },
                );
            }
            let count = read_fixed_count(&mut reader, 16)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i128()?);
            }
            MembershipValues::decimal128(*precision, *scale, values)
                .map_err(|_| ContributionCodecError::NonCanonicalPayload)?
        }
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    };
    let contains_null = match reader.read_u8()? {
        0 => false,
        1 => true,
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    };
    if !reader.is_empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(ValueDomainDelta::new(values, contains_null))
}

fn decode_ordered_bound_body(
    body: &[u8],
    contract: &RuntimeOrderContract,
) -> Result<OrderedBoundUpdate, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let tuple = decode_ordered_tuple(&mut reader, contract)?;
    if !reader.is_empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    OrderedBoundUpdate::new(contract, tuple)
        .map_err(|_| ContributionCodecError::NonCanonicalPayload)
}

fn decode_topk_body(
    body: &[u8],
    contract: &RuntimeTopKSummaryContract,
) -> Result<TopKSummary, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let candidate_count =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    let installed_k =
        usize::try_from(contract.k().get()).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if candidate_count > installed_k {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let minimum_bytes =
        minimum_topk_tuple_prefix_bytes(candidate_count, contract.order().keys().len())?;
    if minimum_bytes > reader.remaining_len() {
        return Err(ContributionCodecError::Truncated);
    }
    let mut candidates = reserve_values(candidate_count)?;
    for _ in 0..candidate_count {
        candidates.push(decode_ordered_tuple(&mut reader, contract.order())?);
    }
    if !reader.is_empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    TopKSummary::try_new(contract, candidates)
        .map_err(|_| ContributionCodecError::NonCanonicalPayload)
}

fn decode_final_domain_body(
    body: &[u8],
    contract: &RuntimeCompletionFenceContract,
    stream: ProducerStreamId,
    sequence: ProducerSequence,
) -> Result<FinalDomainShard, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let encoded_fence_digest = reader.read_array::<32>()?;
    let fence = CompletionFence::try_from_remote_codec(
        contract.digest(),
        stream,
        sequence,
        encoded_fence_digest,
    )
    .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
    let domain = decode_final_domain_membership_body(
        reader.read_exact(reader.remaining_len())?,
        contract.membership_schema().data_type(),
    )?;
    let shard =
        FinalDomainShard::try_new(contract, fence, domain).map_err(map_final_domain_error)?;
    verify_final_domain_scope(&shard, contract, stream, sequence)?;
    Ok(shard)
}

fn decode_final_domain_membership_body(
    body: &[u8],
    expected_data_type: &DataType,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    match decode_membership_body_with_policy(
        body,
        expected_data_type,
        ContributionCodecError::SchemaMismatch,
    ) {
        Ok(domain) => Ok(domain),
        Err(ContributionCodecError::SchemaMismatch) => {
            let encoded_data_type = infer_membership_data_type(body)?;
            decode_membership_body(body, &encoded_data_type)?;
            Err(ContributionCodecError::SchemaMismatch)
        }
        Err(error) => Err(error),
    }
}

fn infer_membership_data_type(body: &[u8]) -> Result<DataType, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let version_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if reader.read_exact(version_len)? != FINGERPRINT_VERSION_TAG {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(match reader.read_u8()? {
        1 => DataType::Boolean,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
        7 => DataType::Float32,
        8 => DataType::Float64,
        9 => DataType::Utf8,
        10 => DataType::Date32,
        11 => {
            let unit = match reader.read_u8()? {
                1 => TimeUnit::Second,
                2 => TimeUnit::Millisecond,
                3 => TimeUnit::Microsecond,
                4 => TimeUnit::Nanosecond,
                _ => return Err(ContributionCodecError::NonCanonicalPayload),
            };
            let timezone = match reader.read_u8()? {
                0 => None,
                1 => {
                    let len = usize::try_from(reader.read_u64()?)
                        .map_err(|_| ContributionCodecError::LengthOverflow)?;
                    let bytes = reader.read_exact(len)?;
                    let timezone = std::str::from_utf8(bytes)
                        .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
                    Some(Arc::from(timezone))
                }
                _ => return Err(ContributionCodecError::NonCanonicalPayload),
            };
            DataType::Timestamp(unit, timezone)
        }
        12 => {
            let precision = reader.read_u8()?;
            let scale = reader.read_u8()? as i8;
            if !decimal_metadata_is_valid(precision, scale) {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            DataType::Decimal128(precision, scale)
        }
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    })
}

fn verify_final_domain_scope(
    shard: &FinalDomainShard,
    contract: &RuntimeCompletionFenceContract,
    stream: ProducerStreamId,
    sequence: ProducerSequence,
) -> Result<(), ContributionCodecError> {
    shard
        .verify_scope(contract, stream, sequence)
        .map_err(|error| match error.kind() {
            RuntimeContractViolationKind::TypeMismatch => ContributionCodecError::SchemaMismatch,
            _ => ContributionCodecError::NonCanonicalPayload,
        })
}

fn map_final_domain_error(error: FinalDomainError) -> ContributionCodecError {
    match error {
        FinalDomainError::ContractMismatch | FinalDomainError::DomainSchemaMismatch => {
            ContributionCodecError::SchemaMismatch
        }
        _ => ContributionCodecError::NonCanonicalPayload,
    }
}

fn decode_ordered_tuple(
    reader: &mut Reader<'_>,
    contract: &RuntimeOrderContract,
) -> Result<OrderedTuple, ContributionCodecError> {
    let arity =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if arity != contract.keys().len() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    if arity > reader.remaining_len() {
        return Err(ContributionCodecError::Truncated);
    }
    let mut values = reserve_values(arity)?;
    for key in contract.keys() {
        values.push(match reader.read_u8()? {
            0 => None,
            1 => Some(decode_ordered_scalar(reader, key.data_type())?),
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        });
    }
    OrderedTuple::try_from_codec(contract, values)
        .map_err(|_| ContributionCodecError::NonCanonicalPayload)
}

fn decode_ordered_scalar(
    reader: &mut Reader<'_>,
    data_type: &DataType,
) -> Result<OrderedScalar, ContributionCodecError> {
    Ok(match data_type {
        DataType::Boolean => OrderedScalar::Boolean(match reader.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        }),
        DataType::Int8 => OrderedScalar::Int8(reader.read_i8()?),
        DataType::Int16 => OrderedScalar::Int16(reader.read_i16()?),
        DataType::Int32 => OrderedScalar::Int32(reader.read_i32()?),
        DataType::Int64 => OrderedScalar::Int64(reader.read_i64()?),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            OrderedScalar::LargeInt(reader.read_i128()?)
        }
        DataType::Utf8 => {
            let len = usize::try_from(reader.read_u64()?)
                .map_err(|_| ContributionCodecError::LengthOverflow)?;
            let bytes = reader.read_exact(len)?;
            let value = std::str::from_utf8(bytes)
                .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
            let mut owned = String::new();
            owned
                .try_reserve_exact(len)
                .map_err(|_| ContributionCodecError::ResourceLimit)?;
            owned.push_str(value);
            OrderedScalar::Utf8(owned.into())
        }
        DataType::Date32 => OrderedScalar::Date32(reader.read_i32()?),
        DataType::Timestamp(_, _) => OrderedScalar::Timestamp(reader.read_i64()?),
        DataType::Decimal128(_, _) => OrderedScalar::Decimal128(reader.read_i128()?),
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    })
}

fn expect_type_tag(
    reader: &mut Reader<'_>,
    expected: u8,
    mismatch_error: ContributionCodecError,
) -> Result<(), ContributionCodecError> {
    let actual = reader.read_u8()?;
    if actual != expected {
        return Err(if (1..=12).contains(&actual) {
            mismatch_error
        } else {
            ContributionCodecError::NonCanonicalPayload
        });
    }
    Ok(())
}

fn decimal_metadata_is_valid(precision: u8, scale: i8) -> bool {
    (1..=38).contains(&precision) && scale <= 38 && (scale <= 0 || scale as u8 <= precision)
}

fn read_count(reader: &mut Reader<'_>) -> Result<usize, ContributionCodecError> {
    usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)
}

fn read_fixed_count(
    reader: &mut Reader<'_>,
    width: usize,
) -> Result<usize, ContributionCodecError> {
    let count = read_count(reader)?;
    ensure_count_bytes(reader, count, width)?;
    Ok(count)
}

fn ensure_count_bytes(
    reader: &Reader<'_>,
    count: usize,
    width: usize,
) -> Result<(), ContributionCodecError> {
    let required = count
        .checked_mul(width)
        .ok_or(ContributionCodecError::LengthOverflow)?;
    if required > reader.remaining_len() {
        return Err(ContributionCodecError::Truncated);
    }
    Ok(())
}

fn minimum_topk_tuple_prefix_bytes(
    candidate_count: usize,
    key_count: usize,
) -> Result<usize, ContributionCodecError> {
    let minimum_per_tuple = size_of::<u64>()
        .checked_add(key_count)
        .ok_or(ContributionCodecError::LengthOverflow)?;
    candidate_count
        .checked_mul(minimum_per_tuple)
        .ok_or(ContributionCodecError::LengthOverflow)
}

fn reserve_values<T>(count: usize) -> Result<Vec<T>, ContributionCodecError> {
    count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(ContributionCodecError::LengthOverflow)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| ContributionCodecError::ResourceLimit)?;
    Ok(values)
}

fn time_unit_tag(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }
}

struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    const fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ContributionCodecError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(len)
            .ok_or(ContributionCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ContributionCodecError> {
        Ok(self.read_exact(N)?.try_into().expect("exact array length"))
    }

    fn read_u8(&mut self) -> Result<u8, ContributionCodecError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ContributionCodecError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, ContributionCodecError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, ContributionCodecError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_i8(&mut self) -> Result<i8, ContributionCodecError> {
        Ok(i8::from_be_bytes(self.read_array()?))
    }

    fn read_i16(&mut self) -> Result<i16, ContributionCodecError> {
        Ok(i16::from_be_bytes(self.read_array()?))
    }

    fn read_i32(&mut self) -> Result<i32, ContributionCodecError> {
        Ok(i32::from_be_bytes(self.read_array()?))
    }

    fn read_i64(&mut self) -> Result<i64, ContributionCodecError> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    fn read_i128(&mut self) -> Result<i128, ContributionCodecError> {
        Ok(i128::from_be_bytes(self.read_array()?))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, TimeUnit};

    use super::*;
    use crate::common::largeint::LARGEINT_BYTE_WIDTH;
    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{
        BindingId, ChannelId, CompletionFenceKind, NullOrder, NullSemantics, OrderContract,
        OrderKeyContract, SortDirection, TopKSummaryRequirement,
    };
    use crate::runtime_filter::port::final_domain::{
        CollectingFinalDomainTestIssuer, CompletionFenceAuthority, FinalDomainTestIssuerTransition,
    };
    use crate::runtime_filter::port::identity::{DeploymentEpoch, PartitionId};
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedScalar, OrderedTuple, RuntimeOrderContract,
        comparator_digest_for_test,
    };
    use crate::runtime_filter::port::value_domain::MembershipValues;

    struct CountingAllocator {
        calls: Cell<usize>,
        exact_len: Cell<usize>,
    }

    impl CountingAllocator {
        fn new() -> Self {
            Self {
                calls: Cell::new(0),
                exact_len: Cell::new(0),
            }
        }
    }

    impl ContributionFrameAllocator for CountingAllocator {
        fn allocate(&self, exact_len: usize) -> Result<Vec<u8>, ContributionCodecError> {
            self.calls.set(self.calls.get() + 1);
            self.exact_len.set(exact_len);
            Ok(Vec::with_capacity(exact_len))
        }
    }

    fn schema(data_type: &DataType, null_semantics: NullSemantics) -> ArtifactMembershipSchema {
        ArtifactMembershipSchema::new(data_type, null_semantics).unwrap()
    }

    fn membership(
        values: MembershipValues,
        contains_null: bool,
    ) -> (RuntimeFilterContribution, ArtifactMembershipSchema) {
        let schema = schema(&values.data_type(), NullSemantics::NullSafeEqual);
        (
            RuntimeFilterContribution::Membership(ValueDomainDelta::new(values, contains_null)),
            schema,
        )
    }

    fn encode_membership(
        values: MembershipValues,
        contains_null: bool,
    ) -> (
        RuntimeFilterContribution,
        ArtifactMembershipSchema,
        EncodedContribution,
    ) {
        let (contribution, schema) = membership(values, contains_null);
        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::Membership(&schema),
            usize::MAX,
        )
        .unwrap();
        (contribution, schema, encoded)
    }

    fn values_offset(payload: &[u8]) -> usize {
        let version_len =
            u64::from_be_bytes(payload[HEADER_LEN..HEADER_LEN + 8].try_into().unwrap()) as usize;
        HEADER_LEN + 8 + version_len
    }

    fn first_value_offset(payload: &[u8]) -> usize {
        values_offset(payload) + 1 + 8
    }

    fn assert_membership_round_trip(values: MembershipValues, contains_null: bool) {
        let (expected, schema, encoded) = encode_membership(values, contains_null);
        assert_eq!(encoded.schema_digest(), &schema.digest().bytes());
        assert_eq!(
            encoded_contribution_len(&expected, ContributionCodecExpectation::Membership(&schema)),
            Ok(encoded.payload().len())
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&schema),
                encoded.payload().len(),
            ),
            Ok(expected.clone())
        );
        assert_eq!(
            encode_contribution(
                &expected,
                ContributionCodecExpectation::Membership(&schema),
                encoded.payload().len(),
            ),
            Ok(encoded)
        );
    }

    fn order_contract(keys: Vec<OrderKeyContract>) -> RuntimeOrderContract {
        let comparator_digest = comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION);
        RuntimeOrderContract::try_from_plan(&OrderContract {
            keys,
            inclusive: true,
            comparator_digest,
        })
        .unwrap()
    }

    fn order_key(
        data_type: DataType,
        direction: SortDirection,
        null_order: NullOrder,
    ) -> OrderKeyContract {
        OrderKeyContract {
            data_type,
            direction,
            null_order,
        }
    }

    fn ordered_bound(
        contract: &RuntimeOrderContract,
        values: impl IntoIterator<Item = Option<OrderedScalar>>,
    ) -> RuntimeFilterContribution {
        let tuple = OrderedTuple::try_new(contract, values).unwrap();
        RuntimeFilterContribution::OrderedBound(OrderedBoundUpdate::new(contract, tuple).unwrap())
    }

    fn assert_ordered_bound_round_trip(
        contract: &RuntimeOrderContract,
        values: impl IntoIterator<Item = Option<OrderedScalar>>,
    ) -> EncodedContribution {
        let contribution = ordered_bound(contract, values);
        let expectation = ContributionCodecExpectation::OrderedBound(contract);
        let encoded = encode_contribution(&contribution, expectation, usize::MAX);
        let expected_len = encoded_contribution_len(&contribution, expectation);
        assert_eq!(
            encoded.as_ref().map(|encoded| encoded.payload().len()),
            expected_len.as_ref().map(|len| *len)
        );
        let encoded = encoded.unwrap();
        assert_eq!(encoded.schema_digest(), &contract.digest().bytes());
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                encoded.payload().len(),
            ),
            Ok(contribution)
        );
        encoded
    }

    fn topk_contract(keys: Vec<OrderKeyContract>, k: u32) -> RuntimeTopKSummaryContract {
        let order = OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        };
        RuntimeTopKSummaryContract::try_from_plan(
            &order,
            TopKSummaryRequirement::try_new(k).unwrap(),
        )
        .unwrap()
    }

    fn topk_summary(
        contract: &RuntimeTopKSummaryContract,
        candidates: impl IntoIterator<Item = Vec<Option<OrderedScalar>>>,
    ) -> RuntimeFilterContribution {
        let candidates = candidates
            .into_iter()
            .map(|values| OrderedTuple::try_new(contract.order(), values).unwrap())
            .collect();
        RuntimeFilterContribution::TopKSummary(TopKSummary::try_new(contract, candidates).unwrap())
    }

    fn assert_topk_round_trip(
        contract: &RuntimeTopKSummaryContract,
        candidates: impl IntoIterator<Item = Vec<Option<OrderedScalar>>>,
    ) -> EncodedContribution {
        let contribution = topk_summary(contract, candidates);
        let expectation = ContributionCodecExpectation::TopKSummary(contract);
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();
        assert_eq!(
            encoded_contribution_len(&contribution, expectation),
            Ok(encoded.payload().len())
        );
        assert_eq!(encoded.schema_digest(), &contract.digest().bytes());
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                encoded.payload().len(),
            ),
            Ok(contribution)
        );
        encoded
    }

    fn final_domain_contract(data_type: &DataType) -> RuntimeCompletionFenceContract {
        RuntimeCompletionFenceContract::try_from_install(
            UniqueId { hi: 101, lo: 102 },
            DeploymentEpoch::new(103),
            ChannelId::new(104),
            CompletionFenceKind::CommittedDomainFrozen,
            &schema(data_type, NullSemantics::NullSafeEqual),
        )
        .unwrap()
    }

    fn final_domain_stream(binding: u32, instance: UniqueId, partition: u32) -> ProducerStreamId {
        ProducerStreamId::new(
            BindingId::new(binding),
            instance,
            PartitionId::new(partition),
        )
    }

    fn final_domain_shard(
        contract: &RuntimeCompletionFenceContract,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        domain: ValueDomainDelta,
    ) -> FinalDomainShard {
        let authority = CompletionFenceAuthority::try_new(
            Arc::new(contract.clone()),
            stream.binding_id(),
            stream.fragment_instance_id(),
        )
        .unwrap();
        let issuer = match CollectingFinalDomainTestIssuer::new(authority, 1).close_driver() {
            FinalDomainTestIssuerTransition::Frozen(issuer) => issuer,
            FinalDomainTestIssuerTransition::Collecting(_) => {
                panic!("the only open driver must freeze the test issuer")
            }
        };
        issuer.issue_shard(stream, sequence, domain).unwrap()
    }

    fn encode_final_domain(
        contract: &RuntimeCompletionFenceContract,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        domain: ValueDomainDelta,
    ) -> (RuntimeFilterContribution, EncodedContribution) {
        let contribution = RuntimeFilterContribution::FinalDomain(final_domain_shard(
            contract, stream, sequence, domain,
        ));
        let expectation = ContributionCodecExpectation::FinalDomain {
            contract,
            stream,
            sequence,
        };
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();
        (contribution, encoded)
    }

    #[test]
    fn membership_round_trip_is_deterministic_and_contract_driven() {
        let cases = vec![
            (MembershipValues::boolean([false, true]), false),
            (MembershipValues::int8([i8::MIN, 0, i8::MAX]), true),
            (MembershipValues::int16([i16::MIN, 0, i16::MAX]), false),
            (MembershipValues::int32([i32::MIN, 0, i32::MAX]), true),
            (MembershipValues::int64([i64::MIN, 0, i64::MAX]), false),
            (MembershipValues::large_int([i128::MIN, 0, i128::MAX]), true),
            (
                MembershipValues::float32([
                    -0.0,
                    0.0,
                    f32::from_bits(0x7fc0_1234),
                    f32::NEG_INFINITY,
                    f32::INFINITY,
                ]),
                false,
            ),
            (
                MembershipValues::float64([
                    -0.0,
                    0.0,
                    f64::from_bits(0x7ff8_0000_0000_1234),
                    f64::NEG_INFINITY,
                    f64::INFINITY,
                ]),
                true,
            ),
            (MembershipValues::utf8(["", "é", "東京"]), false),
            (MembershipValues::date32([i32::MIN, 0, i32::MAX]), true),
            (
                MembershipValues::timestamp(TimeUnit::Second, None, [i64::MIN, i64::MAX]),
                false,
            ),
            (
                MembershipValues::timestamp(
                    TimeUnit::Millisecond,
                    Some(Arc::from("UTC")),
                    [-1, 0, 1],
                ),
                true,
            ),
            (
                MembershipValues::timestamp(TimeUnit::Microsecond, None, [-1, 0, 1]),
                false,
            ),
            (
                MembershipValues::timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("Asia/Shanghai")),
                    [-1, 0, 1],
                ),
                true,
            ),
            (
                MembershipValues::decimal128(38, 4, [-(10_i128.pow(38) - 1), 10_i128.pow(38) - 1])
                    .unwrap(),
                false,
            ),
            (MembershipValues::int64([]), false),
            (
                MembershipValues::empty_for_data_type(&DataType::FixedSizeBinary(
                    LARGEINT_BYTE_WIDTH,
                ))
                .unwrap(),
                true,
            ),
        ];

        for (values, contains_null) in cases {
            assert_membership_round_trip(values, contains_null);
        }

        let contribution = RuntimeFilterContribution::Membership(ValueDomainDelta::new(
            MembershipValues::int64([1]),
            true,
        ));
        let never_matches = schema(&DataType::Int64, NullSemantics::NeverMatches);
        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::Membership(&never_matches),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&never_matches),
                usize::MAX,
            ),
            Ok(contribution)
        );
    }

    #[test]
    fn membership_requires_frame_envelope_and_install_digest_match() {
        let (_, installed_schema, encoded) = encode_membership(MembershipValues::int64([7]), false);
        let wrong_schema = schema(&DataType::Int64, NullSemantics::NeverMatches);
        let mut wrong_frame = encoded.payload().to_vec();
        wrong_frame[8] ^= 1;
        let wrong_envelope = [0x55; 32];

        assert_eq!(
            decode_contribution(
                &wrong_frame,
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&installed_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                &wrong_envelope,
                ContributionCodecExpectation::Membership(&installed_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&wrong_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn membership_encode_rejects_data_type_mismatch() {
        let contribution = RuntimeFilterContribution::Membership(ValueDomainDelta::new(
            MembershipValues::int64([7]),
            false,
        ));
        let schema = schema(&DataType::Int32, NullSemantics::NeverMatches);

        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::Membership(&schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn membership_rejects_bad_magic_version_kind_flags_and_body_lengths() {
        let (_, schema, encoded) = encode_membership(MembershipValues::int32([1]), false);
        let expectation = ContributionCodecExpectation::Membership(&schema);

        for (offset, value, error) in [
            (0, b'X', ContributionCodecError::Malformed),
            (5, 2, ContributionCodecError::UnknownVersion),
            (6, 99, ContributionCodecError::UnknownKind),
            (7, 1, ContributionCodecError::InvalidFlags),
        ] {
            let mut mutated = encoded.payload().to_vec();
            mutated[offset] = value;
            assert_eq!(
                decode_contribution(&mutated, encoded.schema_digest(), expectation, usize::MAX,),
                Err(error)
            );
        }

        let mut wrong_kind = encoded.payload().to_vec();
        wrong_kind[6] = 2;
        assert_eq!(
            decode_contribution(
                &wrong_kind,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::KindMismatch)
        );

        let body_len = encoded.payload().len() - HEADER_LEN;
        for (declared, error) in [
            (body_len - 1, ContributionCodecError::TrailingBytes),
            (body_len + 1, ContributionCodecError::Truncated),
        ] {
            let mut mutated = encoded.payload().to_vec();
            mutated[40..48].copy_from_slice(&(declared as u64).to_be_bytes());
            assert_eq!(
                decode_contribution(&mutated, encoded.schema_digest(), expectation, usize::MAX,),
                Err(error)
            );
        }
    }

    #[test]
    fn membership_rejects_noncanonical_values_and_trailing_bytes() {
        let (_, schema, encoded) = encode_membership(MembershipValues::int32([1, 2]), false);
        let expectation = ContributionCodecExpectation::Membership(&schema);
        let mut duplicate = encoded.payload().to_vec();
        let first = first_value_offset(&duplicate);
        duplicate[first + 4..first + 8].copy_from_slice(&1_i32.to_be_bytes());
        assert_eq!(
            decode_contribution(&duplicate, encoded.schema_digest(), expectation, usize::MAX,),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let mut invalid_null = encoded.payload().to_vec();
        *invalid_null.last_mut().unwrap() = 2;
        assert_eq!(
            decode_contribution(
                &invalid_null,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let (_, bool_schema, bool_encoded) =
            encode_membership(MembershipValues::boolean([true]), false);
        let mut invalid_bool = bool_encoded.payload().to_vec();
        let bool_value = first_value_offset(&invalid_bool);
        invalid_bool[bool_value] = 2;
        assert_eq!(
            decode_contribution(
                &invalid_bool,
                bool_encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&bool_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let mut trailing = encoded.payload().to_vec();
        trailing.push(0);
        assert_eq!(
            decode_contribution(&trailing, encoded.schema_digest(), expectation, usize::MAX,),
            Err(ContributionCodecError::TrailingBytes)
        );
    }

    #[test]
    fn membership_rejects_invalid_utf8_noncanonical_float_and_decimal_overflow() {
        let (_, utf8_schema, utf8) = encode_membership(MembershipValues::utf8(["a"]), false);
        let mut invalid_utf8 = utf8.payload().to_vec();
        let utf8_value = first_value_offset(&invalid_utf8) + 8;
        invalid_utf8[utf8_value] = 0xff;
        assert_eq!(
            decode_contribution(
                &invalid_utf8,
                utf8.schema_digest(),
                ContributionCodecExpectation::Membership(&utf8_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let (_, float_schema, float) =
            encode_membership(MembershipValues::float32([0.0, f32::NAN]), false);
        let mut negative_zero = float.payload().to_vec();
        let float_value = first_value_offset(&negative_zero);
        negative_zero[float_value..float_value + 4]
            .copy_from_slice(&(-0.0_f32).to_bits().to_be_bytes());
        assert_eq!(
            decode_contribution(
                &negative_zero,
                float.schema_digest(),
                ContributionCodecExpectation::Membership(&float_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut noncanonical_nan = float.payload().to_vec();
        noncanonical_nan[float_value + 4..float_value + 8]
            .copy_from_slice(&0x7fc0_0001_u32.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &noncanonical_nan,
                float.schema_digest(),
                ContributionCodecExpectation::Membership(&float_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let (_, decimal_schema, decimal) =
            encode_membership(MembershipValues::decimal128(3, 0, [999]).unwrap(), false);
        let mut overflow = decimal.payload().to_vec();
        let decimal_value = first_value_offset(&overflow) + 2;
        overflow[decimal_value..decimal_value + 16].copy_from_slice(&1000_i128.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &overflow,
                decimal.schema_digest(),
                ContributionCodecExpectation::Membership(&decimal_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn membership_decode_rejects_oversize_before_body_allocation() {
        let schema = schema(&DataType::Int64, NullSemantics::NeverMatches);
        assert_eq!(
            decode_contribution(
                &[0xff],
                &schema.digest().bytes(),
                ContributionCodecExpectation::Membership(&schema),
                0,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
    }

    #[test]
    fn membership_rejects_impossible_counts_before_value_allocation() {
        let (_, fixed_schema, fixed) = encode_membership(MembershipValues::int64([1]), false);
        let mut fixed_overflow = fixed.payload().to_vec();
        let fixed_count = values_offset(&fixed_overflow) + 1;
        fixed_overflow[fixed_count..fixed_count + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &fixed_overflow,
                fixed.schema_digest(),
                ContributionCodecExpectation::Membership(&fixed_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::LengthOverflow)
        );

        let (_, utf8_schema, utf8) = encode_membership(MembershipValues::utf8(["a"]), false);
        let mut utf8_overflow = utf8.payload().to_vec();
        let utf8_count = values_offset(&utf8_overflow) + 1;
        utf8_overflow[utf8_count..utf8_count + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &utf8_overflow,
                utf8.schema_digest(),
                ContributionCodecExpectation::Membership(&utf8_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::LengthOverflow)
        );
    }

    #[test]
    fn membership_encode_rejects_oversize_before_frame_allocation() {
        let (contribution, schema) = membership(MembershipValues::utf8(["large"]), false);
        let exact = encoded_contribution_len(
            &contribution,
            ContributionCodecExpectation::Membership(&schema),
        )
        .unwrap();
        let allocator = CountingAllocator::new();

        assert_eq!(
            encode_contribution_with_allocator(
                &contribution,
                ContributionCodecExpectation::Membership(&schema),
                exact - 1,
                &allocator,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        assert_eq!(allocator.calls.get(), 0);

        let encoded = encode_contribution_with_allocator(
            &contribution,
            ContributionCodecExpectation::Membership(&schema),
            exact,
            &allocator,
        )
        .unwrap();
        assert_eq!(allocator.calls.get(), 1);
        assert_eq!(allocator.exact_len.get(), exact);
        assert_eq!(encoded.payload().len(), exact);
    }

    #[test]
    fn membership_exact_limit_succeeds_and_limit_minus_one_fails() {
        let (contribution, schema) = membership(MembershipValues::int64([1, 2, 3]), true);
        let expectation = ContributionCodecExpectation::Membership(&schema);
        let exact = encoded_contribution_len(&contribution, expectation).unwrap();

        assert_eq!(
            encode_contribution(&contribution, expectation, exact - 1),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        let encoded = encode_contribution(&contribution, expectation, exact).unwrap();
        assert_eq!(encoded.payload().len(), exact);
    }

    #[test]
    fn membership_length_preflight_returns_error_without_panic_or_allocation() {
        assert_eq!(
            encoded_frame_len_from_body_len(usize::MAX),
            Err(ContributionCodecError::LengthOverflow)
        );
    }

    #[test]
    fn ordered_bound_round_trip_uses_installed_contract() {
        let contract = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let contribution = ordered_bound(&contract, [Some(OrderedScalar::Int64(42))]);
        let expectation = ContributionCodecExpectation::OrderedBound(&contract);

        assert_eq!(
            encode_contribution(&contribution, expectation, usize::MAX)
                .map(|encoded| encoded.payload().len()),
            Ok(HEADER_LEN + 8 + 1 + 8)
        );
        assert_ordered_bound_round_trip(&contract, [Some(OrderedScalar::Int64(42))]);
    }

    #[test]
    fn ordered_bound_covers_asc_desc_nulls_first_last_and_multikey() {
        let ascending = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::First,
        )]);
        let descending = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Descending,
            NullOrder::Last,
        )]);
        let ascending_encoded =
            assert_ordered_bound_round_trip(&ascending, [Some(OrderedScalar::Int64(7))]);
        let descending_encoded =
            assert_ordered_bound_round_trip(&descending, [Some(OrderedScalar::Int64(7))]);
        assert_eq!(
            ascending_encoded.payload()[HEADER_LEN..],
            descending_encoded.payload()[HEADER_LEN..]
        );

        for (direction, null_order, value) in [
            (SortDirection::Ascending, NullOrder::First, None),
            (
                SortDirection::Ascending,
                NullOrder::Last,
                Some(OrderedScalar::Int64(1)),
            ),
            (
                SortDirection::Descending,
                NullOrder::First,
                Some(OrderedScalar::Int64(-1)),
            ),
            (SortDirection::Descending, NullOrder::Last, None),
        ] {
            let contract = order_contract(vec![order_key(DataType::Int64, direction, null_order)]);
            assert_ordered_bound_round_trip(&contract, [value]);
        }

        let contract = order_contract(vec![
            order_key(DataType::Int32, SortDirection::Ascending, NullOrder::Last),
            order_key(DataType::Utf8, SortDirection::Descending, NullOrder::First),
        ]);
        assert_ordered_bound_round_trip(
            &contract,
            [
                Some(OrderedScalar::Int32(7)),
                Some(OrderedScalar::Utf8(Arc::from("多键"))),
            ],
        );
    }

    #[test]
    fn ordered_bound_covers_utf8_decimal_timestamp_and_largeint() {
        let cases = [
            (
                DataType::Utf8,
                Some(OrderedScalar::Utf8(Arc::from("héllo-東京"))),
            ),
            (
                DataType::Decimal128(38, 6),
                Some(OrderedScalar::Decimal128(10_i128.pow(38) - 1)),
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                Some(OrderedScalar::Timestamp(i64::MIN)),
            ),
            (
                DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
                Some(OrderedScalar::LargeInt(i128::MAX)),
            ),
        ];

        for (data_type, value) in cases {
            let contract = order_contract(vec![order_key(
                data_type,
                SortDirection::Ascending,
                NullOrder::Last,
            )]);
            assert_ordered_bound_round_trip(&contract, [value]);
        }
    }

    #[test]
    fn ordered_bound_rejects_wrong_kind_digest_arity_type_and_noncanonical_scalar() {
        let contract = order_contract(vec![order_key(
            DataType::Boolean,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let contribution = ordered_bound(&contract, [Some(OrderedScalar::Boolean(true))]);
        let expectation = ContributionCodecExpectation::OrderedBound(&contract);
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();

        let membership_schema = schema(&DataType::Boolean, NullSemantics::NeverMatches);
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&membership_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::KindMismatch)
        );

        let mut wrong_digest = encoded.payload().to_vec();
        wrong_digest[8] ^= 1;
        assert_eq!(
            decode_contribution(
                &wrong_digest,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let other_contract = order_contract(vec![order_key(
            DataType::Boolean,
            SortDirection::Descending,
            NullOrder::Last,
        )]);
        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::OrderedBound(&other_contract),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let mut wrong_arity = encoded.payload().to_vec();
        wrong_arity[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&2_u64.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &wrong_arity,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut impossible_arity = encoded.payload().to_vec();
        impossible_arity[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &impossible_arity,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let mut invalid_boolean = encoded.payload().to_vec();
        invalid_boolean[HEADER_LEN + 8 + 1] = 2;
        assert_eq!(
            decode_contribution(
                &invalid_boolean,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let utf8_contract = order_contract(vec![order_key(
            DataType::Utf8,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let utf8_encoded = assert_ordered_bound_round_trip(
            &utf8_contract,
            [Some(OrderedScalar::Utf8("a".into()))],
        );
        let mut invalid_utf8 = utf8_encoded.payload().to_vec();
        invalid_utf8[HEADER_LEN + 8 + 1 + 8] = 0xff;
        assert_eq!(
            decode_contribution(
                &invalid_utf8,
                utf8_encoded.schema_digest(),
                ContributionCodecExpectation::OrderedBound(&utf8_contract),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut impossible_utf8 = utf8_encoded.payload().to_vec();
        impossible_utf8[HEADER_LEN + 8 + 1..HEADER_LEN + 8 + 1 + 8]
            .copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &impossible_utf8,
                utf8_encoded.schema_digest(),
                ContributionCodecExpectation::OrderedBound(&utf8_contract),
                usize::MAX,
            ),
            Err(ContributionCodecError::Truncated)
        );

        let decimal_contract = order_contract(vec![order_key(
            DataType::Decimal128(3, 0),
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let decimal_encoded = assert_ordered_bound_round_trip(
            &decimal_contract,
            [Some(OrderedScalar::Decimal128(999))],
        );
        let mut decimal_overflow = decimal_encoded.payload().to_vec();
        decimal_overflow[HEADER_LEN + 8 + 1..HEADER_LEN + 8 + 1 + 16]
            .copy_from_slice(&1000_i128.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &decimal_overflow,
                decimal_encoded.schema_digest(),
                ContributionCodecExpectation::OrderedBound(&decimal_contract),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn ordered_bound_exact_limit_succeeds_and_limit_minus_one_fails() {
        let contract = order_contract(vec![order_key(
            DataType::Utf8,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let contribution =
            ordered_bound(&contract, [Some(OrderedScalar::Utf8(Arc::from("exact")))]);
        let expectation = ContributionCodecExpectation::OrderedBound(&contract);
        let exact = encoded_contribution_len(&contribution, expectation);
        assert_eq!(exact, Ok(HEADER_LEN + 8 + 1 + 8 + 5));
        let exact = exact.unwrap();

        assert_eq!(
            encode_contribution(&contribution, expectation, exact - 1),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        let encoded = encode_contribution(&contribution, expectation, exact).unwrap();
        assert_eq!(encoded.payload().len(), exact);
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact - 1,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact,
            ),
            Ok(contribution)
        );
    }

    #[test]
    fn topk_round_trip_accepts_empty_single_and_exact_k_candidates() {
        let contract = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            3,
        );

        let empty = assert_topk_round_trip(&contract, []);
        assert_eq!(empty.payload().len(), HEADER_LEN + 8);
        let single = assert_topk_round_trip(&contract, [vec![Some(OrderedScalar::Int64(7))]]);
        assert_eq!(single.payload().len(), HEADER_LEN + 8 + 8 + 1 + 8);
        assert_topk_round_trip(
            &contract,
            [
                vec![Some(OrderedScalar::Int64(1))],
                vec![Some(OrderedScalar::Int64(2))],
                vec![Some(OrderedScalar::Int64(3))],
            ],
        );
    }

    #[test]
    fn topk_covers_desc_null_order_multikey_utf8_and_decimal() {
        let descending = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Descending,
                NullOrder::First,
            )],
            3,
        );
        assert_topk_round_trip(
            &descending,
            [
                vec![None],
                vec![Some(OrderedScalar::Int64(9))],
                vec![Some(OrderedScalar::Int64(1))],
            ],
        );

        let multikey = topk_contract(
            vec![
                order_key(DataType::Utf8, SortDirection::Ascending, NullOrder::Last),
                order_key(
                    DataType::Decimal128(6, 2),
                    SortDirection::Descending,
                    NullOrder::First,
                ),
            ],
            3,
        );
        assert_topk_round_trip(
            &multikey,
            [
                vec![
                    Some(OrderedScalar::Utf8(Arc::from("a"))),
                    Some(OrderedScalar::Decimal128(9999)),
                ],
                vec![
                    Some(OrderedScalar::Utf8(Arc::from("a"))),
                    Some(OrderedScalar::Decimal128(-9999)),
                ],
                vec![Some(OrderedScalar::Utf8(Arc::from("多键"))), None],
            ],
        );
    }

    #[test]
    fn topk_rejects_over_k_unsorted_wrong_type_and_length_overflow() {
        let contract = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            2,
        );
        let encoded = assert_topk_round_trip(
            &contract,
            [
                vec![Some(OrderedScalar::Int64(1))],
                vec![Some(OrderedScalar::Int64(2))],
            ],
        );
        let expectation = ContributionCodecExpectation::TopKSummary(&contract);

        let mut over_k = encoded.payload().to_vec();
        over_k[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&3_u64.to_be_bytes());
        assert_eq!(
            decode_contribution(&over_k, encoded.schema_digest(), expectation, usize::MAX,),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let mut unsorted = encoded.payload().to_vec();
        let first_value = HEADER_LEN + 8 + 8 + 1;
        let second_value = first_value + 8 + 1 + 8;
        let first = unsorted[first_value..first_value + 8].to_vec();
        let second = unsorted[second_value..second_value + 8].to_vec();
        unsorted[first_value..first_value + 8].copy_from_slice(&second);
        unsorted[second_value..second_value + 8].copy_from_slice(&first);
        assert_eq!(
            decode_contribution(&unsorted, encoded.schema_digest(), expectation, usize::MAX,),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let boolean = topk_contract(
            vec![order_key(
                DataType::Boolean,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            1,
        );
        let boolean_encoded =
            assert_topk_round_trip(&boolean, [vec![Some(OrderedScalar::Boolean(true))]]);
        let mut wrong_type = boolean_encoded.payload().to_vec();
        wrong_type[HEADER_LEN + 8 + 8 + 1] = 2;
        assert_eq!(
            decode_contribution(
                &wrong_type,
                boolean_encoded.schema_digest(),
                ContributionCodecExpectation::TopKSummary(&boolean),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        assert_eq!(
            minimum_topk_tuple_prefix_bytes(usize::MAX, 1),
            Err(ContributionCodecError::LengthOverflow)
        );

        let mut missing_presence_markers =
            encoded.payload()[..HEADER_LEN + 8 + (2 * (8 + 1)) - 1].to_vec();
        let body_len = missing_presence_markers.len() - HEADER_LEN;
        missing_presence_markers[40..48].copy_from_slice(&(body_len as u64).to_be_bytes());
        assert_eq!(
            decode_contribution(
                &missing_presence_markers,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::Truncated)
        );
    }

    #[test]
    fn topk_uses_install_frozen_k_and_digest() {
        let installed = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            2,
        );
        let different_k = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            3,
        );
        let contribution = topk_summary(&installed, [vec![Some(OrderedScalar::Int64(7))]]);
        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::TopKSummary(&different_k),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::TopKSummary(&installed),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::TopKSummary(&different_k),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn topk_exact_limit_succeeds_and_limit_minus_one_fails() {
        let contract = topk_contract(
            vec![order_key(
                DataType::Utf8,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            1,
        );
        let contribution = topk_summary(
            &contract,
            [vec![Some(OrderedScalar::Utf8(Arc::from("exact")))]],
        );
        let expectation = ContributionCodecExpectation::TopKSummary(&contract);
        let exact = encoded_contribution_len(&contribution, expectation).unwrap();

        assert_eq!(
            encode_contribution(&contribution, expectation, exact - 1),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        let encoded = encode_contribution(&contribution, expectation, exact).unwrap();
        assert_eq!(encoded.payload().len(), exact);
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact - 1,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact,
            ),
            Ok(contribution)
        );
    }

    #[test]
    fn final_domain_round_trip_reconstructs_exact_fence_scope() {
        let contract = final_domain_contract(&DataType::Int64);
        let instance = UniqueId { hi: 201, lo: 202 };
        let stream = final_domain_stream(203, instance, 204);
        let sequence = ProducerSequence::new(205);
        let (contribution, encoded) = encode_final_domain(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1, 2]), true),
        );
        let expectation = ContributionCodecExpectation::FinalDomain {
            contract: &contract,
            stream,
            sequence,
        };

        assert_eq!(encoded.schema_digest(), &contract.digest().bytes());
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                encoded.payload().len(),
            ),
            Ok(contribution)
        );
    }

    #[test]
    fn final_domain_body_contains_digest_but_not_route_identity() {
        let contract = final_domain_contract(&DataType::Int64);
        let stream = final_domain_stream(
            0xa1b2_c3d4,
            UniqueId {
                hi: 0x1122_3344_5566_7788,
                lo: 0x2233_4455_6677_8899,
            },
            0xb1c2_d3e4,
        );
        let sequence = ProducerSequence::new(0x3344_5566_7788_99aa);
        let domain = ValueDomainDelta::new(MembershipValues::int64([7]), false);
        let shard = final_domain_shard(&contract, stream, sequence, domain.clone());
        let contribution = RuntimeFilterContribution::FinalDomain(shard.clone());
        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::FinalDomain {
                contract: &contract,
                stream,
                sequence,
            },
            usize::MAX,
        )
        .unwrap();
        let mut expected_body = shard.fence_digest().to_vec();
        domain.encode_canonical_into(&mut expected_body).unwrap();

        assert_eq!(&encoded.payload()[HEADER_LEN..], expected_body);
    }

    #[test]
    fn final_domain_rejects_fence_digest_binding_finst_partition_sequence_mismatch() {
        let contract = final_domain_contract(&DataType::Int64);
        let instance = UniqueId { hi: 301, lo: 302 };
        let stream = final_domain_stream(303, instance, 304);
        let sequence = ProducerSequence::new(305);
        let (contribution, encoded) = encode_final_domain(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1]), false),
        );
        let mismatched = [
            (final_domain_stream(999, instance, 304), sequence),
            (
                final_domain_stream(303, UniqueId { hi: 999, lo: 302 }, 304),
                sequence,
            ),
            (final_domain_stream(303, instance, 999), sequence),
            (stream, ProducerSequence::new(999)),
        ];
        for (other_stream, other_sequence) in mismatched {
            assert_eq!(
                encode_contribution(
                    &contribution,
                    ContributionCodecExpectation::FinalDomain {
                        contract: &contract,
                        stream: other_stream,
                        sequence: other_sequence,
                    },
                    usize::MAX,
                ),
                Err(ContributionCodecError::NonCanonicalPayload)
            );
            assert_eq!(
                decode_contribution(
                    encoded.payload(),
                    encoded.schema_digest(),
                    ContributionCodecExpectation::FinalDomain {
                        contract: &contract,
                        stream: other_stream,
                        sequence: other_sequence,
                    },
                    usize::MAX,
                ),
                Err(ContributionCodecError::NonCanonicalPayload)
            );
        }

        let mut bad_digest = encoded.payload().to_vec();
        bad_digest[HEADER_LEN] ^= 1;
        assert_eq!(
            decode_contribution(
                &bad_digest,
                encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn final_domain_rejects_membership_schema_and_spliced_body_mismatch() {
        let contract = final_domain_contract(&DataType::Int64);
        let utf8_contract = final_domain_contract(&DataType::Utf8);
        let stream = final_domain_stream(403, UniqueId { hi: 401, lo: 402 }, 404);
        let sequence = ProducerSequence::new(405);
        let (contribution, encoded) = encode_final_domain(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1]), false),
        );
        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::FinalDomain {
                    contract: &utf8_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &utf8_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let (_, utf8_encoded) = encode_final_domain(
            &utf8_contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::utf8(["spliced"]), false),
        );
        let mut spliced = encoded.payload().to_vec();
        spliced.truncate(HEADER_LEN + 32);
        spliced.extend_from_slice(&utf8_encoded.payload()[HEADER_LEN + 32..]);
        let body_len = spliced.len() - HEADER_LEN;
        spliced[40..48].copy_from_slice(&(body_len as u64).to_be_bytes());
        assert_eq!(
            decode_contribution(
                &spliced,
                encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn final_domain_keeps_invalid_schema_metadata_noncanonical() {
        let stream = final_domain_stream(453, UniqueId { hi: 451, lo: 452 }, 454);
        let sequence = ProducerSequence::new(455);

        let int_contract = final_domain_contract(&DataType::Int64);
        let (_, int_encoded) = encode_final_domain(
            &int_contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1]), false),
        );
        let type_tag_offset = HEADER_LEN + 32 + 8 + FINGERPRINT_VERSION_TAG.len();
        let mut invalid_tag = int_encoded.payload().to_vec();
        invalid_tag[type_tag_offset] = 99;
        assert_eq!(
            decode_contribution(
                &invalid_tag,
                int_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &int_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut missing_fence_byte = int_encoded.payload()[..HEADER_LEN + 31].to_vec();
        missing_fence_byte[40..48].copy_from_slice(&31_u64.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &missing_fence_byte,
                int_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &int_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::Truncated)
        );
        let mut malformed_alternate_type = int_encoded.payload().to_vec();
        malformed_alternate_type[type_tag_offset] = 1;
        assert_eq!(
            decode_contribution(
                &malformed_alternate_type,
                int_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &int_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let timestamp_contract =
            final_domain_contract(&DataType::Timestamp(TimeUnit::Second, None));
        let (_, timestamp_encoded) = encode_final_domain(
            &timestamp_contract,
            stream,
            sequence,
            ValueDomainDelta::new(
                MembershipValues::timestamp(TimeUnit::Second, None, [1]),
                false,
            ),
        );
        let mut invalid_unit = timestamp_encoded.payload().to_vec();
        invalid_unit[type_tag_offset + 1] = 99;
        assert_eq!(
            decode_contribution(
                &invalid_unit,
                timestamp_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &timestamp_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut invalid_timezone_marker = timestamp_encoded.payload().to_vec();
        invalid_timezone_marker[type_tag_offset + 2] = 2;
        assert_eq!(
            decode_contribution(
                &invalid_timezone_marker,
                timestamp_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &timestamp_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let decimal_contract = final_domain_contract(&DataType::Decimal128(5, 2));
        let (_, decimal_encoded) = encode_final_domain(
            &decimal_contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::decimal128(5, 2, [1]).unwrap(), false),
        );
        for (precision, scale) in [(0, 2), (5, 39)] {
            let mut invalid_decimal = decimal_encoded.payload().to_vec();
            invalid_decimal[type_tag_offset + 1] = precision;
            invalid_decimal[type_tag_offset + 2] = scale as u8;
            assert_eq!(
                decode_contribution(
                    &invalid_decimal,
                    decimal_encoded.schema_digest(),
                    ContributionCodecExpectation::FinalDomain {
                        contract: &decimal_contract,
                        stream,
                        sequence,
                    },
                    usize::MAX,
                ),
                Err(ContributionCodecError::NonCanonicalPayload)
            );
        }
    }

    #[test]
    fn final_domain_exact_limit_succeeds_and_limit_minus_one_fails() {
        let contract = final_domain_contract(&DataType::Utf8);
        let stream = final_domain_stream(503, UniqueId { hi: 501, lo: 502 }, 504);
        let sequence = ProducerSequence::new(505);
        let contribution = RuntimeFilterContribution::FinalDomain(final_domain_shard(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::utf8(["exact"]), true),
        ));
        let expectation = ContributionCodecExpectation::FinalDomain {
            contract: &contract,
            stream,
            sequence,
        };
        let exact = encoded_contribution_len(&contribution, expectation).unwrap();

        assert_eq!(
            encode_contribution(&contribution, expectation, exact - 1),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        let encoded = encode_contribution(&contribution, expectation, exact).unwrap();
        assert_eq!(encoded.payload().len(), exact);
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact - 1,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact,
            ),
            Ok(contribution)
        );
    }
}
