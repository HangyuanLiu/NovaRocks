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

use arrow::datatypes::{DataType, TimeUnit};

use crate::common::largeint::LARGEINT_BYTE_WIDTH;
use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
use crate::runtime_filter::port::final_domain::{FinalDomainShard, RuntimeCompletionFenceContract};
use crate::runtime_filter::port::identity::{ProducerSequence, ProducerStreamId};
use crate::runtime_filter::port::ordered_bound::{OrderedBoundUpdate, RuntimeOrderContract};
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
            RuntimeFilterContribution::OrderedBound(_),
            ContributionCodecExpectation::OrderedBound(_),
        )
        | (
            RuntimeFilterContribution::TopKSummary(_),
            ContributionCodecExpectation::TopKSummary(_),
        )
        | (
            RuntimeFilterContribution::FinalDomain(_),
            ContributionCodecExpectation::FinalDomain { .. },
        )
        | _ => return Err(ContributionCodecError::KindMismatch),
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
        _ => return Err(ContributionCodecError::KindMismatch),
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
    let mut reader = Reader::new(body);
    let version_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if reader.read_exact(version_len)? != FINGERPRINT_VERSION_TAG {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let values = match expected_data_type {
        DataType::Boolean => {
            expect_type_tag(&mut reader, 1)?;
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
            expect_type_tag(&mut reader, 2)?;
            let count = read_fixed_count(&mut reader, 1)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i8()?);
            }
            MembershipValues::int8(values)
        }
        DataType::Int16 => {
            expect_type_tag(&mut reader, 3)?;
            let count = read_fixed_count(&mut reader, 2)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i16()?);
            }
            MembershipValues::int16(values)
        }
        DataType::Int32 => {
            expect_type_tag(&mut reader, 4)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i32()?);
            }
            MembershipValues::int32(values)
        }
        DataType::Int64 => {
            expect_type_tag(&mut reader, 5)?;
            let count = read_fixed_count(&mut reader, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i64()?);
            }
            MembershipValues::int64(values)
        }
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            expect_type_tag(&mut reader, 6)?;
            let count = read_fixed_count(&mut reader, 16)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i128()?);
            }
            MembershipValues::large_int(values)
        }
        DataType::Float32 => {
            expect_type_tag(&mut reader, 7)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(f32::from_bits(reader.read_u32()?));
            }
            MembershipValues::float32(values)
        }
        DataType::Float64 => {
            expect_type_tag(&mut reader, 8)?;
            let count = read_fixed_count(&mut reader, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(f64::from_bits(reader.read_u64()?));
            }
            MembershipValues::float64(values)
        }
        DataType::Utf8 => {
            expect_type_tag(&mut reader, 9)?;
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
            expect_type_tag(&mut reader, 10)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i32()?);
            }
            MembershipValues::date32(values)
        }
        DataType::Timestamp(unit, timezone) => {
            expect_type_tag(&mut reader, 11)?;
            if reader.read_u8()? != time_unit_tag(unit) {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            match (reader.read_u8()?, timezone) {
                (0, None) => {}
                (1, Some(expected_timezone)) => {
                    let len = usize::try_from(reader.read_u64()?)
                        .map_err(|_| ContributionCodecError::LengthOverflow)?;
                    let timezone_bytes = reader.read_exact(len)?;
                    std::str::from_utf8(timezone_bytes)
                        .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
                    if timezone_bytes != expected_timezone.as_bytes() {
                        return Err(ContributionCodecError::NonCanonicalPayload);
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
            expect_type_tag(&mut reader, 12)?;
            if reader.read_u8()? != *precision || reader.read_u8()? as i8 != *scale {
                return Err(ContributionCodecError::NonCanonicalPayload);
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

fn expect_type_tag(reader: &mut Reader<'_>, expected: u8) -> Result<(), ContributionCodecError> {
    if reader.read_u8()? != expected {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(())
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
    use crate::runtime_filter::model::contract::NullSemantics;
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
}
