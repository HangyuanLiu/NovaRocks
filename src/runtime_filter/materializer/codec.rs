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

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::runtime_filter::model::contract::NullSemantics;
use crate::runtime_filter::port::artifact::{
    ArtifactKind, ArtifactMembershipSchema, ArtifactMembershipSchemaView, ArtifactSchemaDigest,
    HashContractDigest, LEAF_CODEC_VERSION, PhysicalArtifact,
};
use crate::runtime_filter::port::identity::LogicalVersion;
use crate::runtime_filter::port::support::{
    ArtifactRetainedBudget, ArtifactRetention, RetainedReservationError, RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::value_domain::{ContributionSizeError, ReducedMembershipDomain};

use super::bloom::{BLOOM_METADATA_BYTES, BloomHashContract};

const MAGIC: &[u8; 4] = b"NRFL";
const FLAG_CONTAINS_NULL: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactDecodeExpectations {
    pub expected_kind: ArtifactKind,
    pub expected_schema_digest: ArtifactSchemaDigest,
    pub expected_logical_version: LogicalVersion,
    pub expected_hash_contract: Option<HashContractDigest>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactCodecError {
    Truncated,
    UnknownVersion,
    UnknownKind,
    UnsupportedKind,
    InvalidFlags,
    InvalidHashContract,
    KindMismatch,
    SchemaMismatch,
    VersionMismatch,
    HashContractMismatch,
    LengthOverflow,
    TrailingBytes,
    NonCanonicalPayload,
    EncodedSizeExceeded,
    ResourceLimit,
}

impl fmt::Display for ArtifactCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid runtime filter leaf artifact: {self:?}")
    }
}

impl Error for ArtifactCodecError {}

impl From<ContributionSizeError> for ArtifactCodecError {
    fn from(error: ContributionSizeError) -> Self {
        match error {
            ContributionSizeError::LengthExceedsCanonicalRange
            | ContributionSizeError::SizeOverflow => Self::LengthOverflow,
        }
    }
}

impl From<RetainedReservationError> for ArtifactCodecError {
    fn from(_error: RetainedReservationError) -> Self {
        Self::ResourceLimit
    }
}

pub(crate) fn encode_membership_leaf(
    domain: &ReducedMembershipDomain,
    null_semantics: NullSemantics,
    logical_version: LogicalVersion,
) -> Result<Vec<u8>, ArtifactCodecError> {
    let contains_null = domain.contains_null();
    if contains_null && null_semantics != NullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let kind = if domain.values().is_empty() && !contains_null {
        ArtifactKind::EmptyDomain
    } else {
        ArtifactKind::ValueSet
    };
    let schema = ArtifactMembershipSchema::new(&domain.data_type(), null_semantics)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
    let mut payload = Vec::new();
    if kind == ArtifactKind::ValueSet {
        let payload_len = domain.values().canonical_encoded_len()?;
        payload
            .try_reserve_exact(payload_len)
            .map_err(|_| ArtifactCodecError::ResourceLimit)?;
        domain.values().encode_canonical_into(&mut payload)?;
    }
    encode_physical_leaf(
        kind,
        &schema,
        logical_version,
        contains_null,
        None,
        &payload,
    )
}

pub(crate) fn encoded_leaf_len(
    schema: &ArtifactMembershipSchema,
    hash_contract: Option<HashContractDigest>,
    payload_len: usize,
) -> Result<usize, ArtifactCodecError> {
    u16::try_from(schema.canonical_bytes().len())
        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    u64::try_from(payload_len).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    4usize
        .checked_add(2)
        .and_then(|size| {
            size.checked_add(
                1 + 32
                    + 2
                    + schema.canonical_bytes().len()
                    + 8
                    + 1
                    + 1
                    + hash_contract.map_or(0, |_| 32)
                    + 8,
            )
        })
        .and_then(|size| size.checked_add(payload_len))
        .ok_or(ArtifactCodecError::LengthOverflow)
}

pub(crate) fn encode_physical_leaf(
    kind: ArtifactKind,
    schema: &ArtifactMembershipSchema,
    logical_version: LogicalVersion,
    contains_null: bool,
    hash_contract: Option<HashContractDigest>,
    payload: &[u8],
) -> Result<Vec<u8>, ArtifactCodecError> {
    if contains_null && schema.null_semantics() != NullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if matches!(kind, ArtifactKind::Bloom) != hash_contract.is_some() {
        return Err(ArtifactCodecError::InvalidHashContract);
    }
    if kind == ArtifactKind::EmptyDomain && (contains_null || !payload.is_empty()) {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let schema_len = u16::try_from(schema.canonical_bytes().len())
        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let capacity = encoded_leaf_len(schema, hash_contract, payload.len())?;
    let payload_len =
        u64::try_from(payload.len()).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&LEAF_CODEC_VERSION.to_be_bytes());
    encoded.push(kind.tag());
    encoded.extend_from_slice(&schema.digest().bytes());
    encoded.extend_from_slice(&schema_len.to_be_bytes());
    encoded.extend_from_slice(schema.canonical_bytes());
    encoded.extend_from_slice(&logical_version.get().to_be_bytes());
    encoded.push(u8::from(contains_null) * FLAG_CONTAINS_NULL);
    match hash_contract {
        Some(digest) => {
            encoded.push(1);
            encoded.extend_from_slice(&digest.bytes());
        }
        None => encoded.push(0),
    }
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(payload);
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

pub(crate) fn decode_leaf(
    encoded: &[u8],
    expectations: ArtifactDecodeExpectations,
    max_artifact_bytes: usize,
    retained_budget: Arc<ArtifactRetainedBudget>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
    if encoded.len() > max_artifact_bytes {
        return Err(ArtifactCodecError::EncodedSizeExceeded);
    }
    let header = parse_header(encoded)?;
    if header.kind != expectations.expected_kind {
        return Err(ArtifactCodecError::KindMismatch);
    }
    if header.schema_digest != expectations.expected_schema_digest {
        return Err(ArtifactCodecError::SchemaMismatch);
    }
    if header.logical_version != expectations.expected_logical_version {
        return Err(ArtifactCodecError::VersionMismatch);
    }
    if header.hash_contract != expectations.expected_hash_contract {
        return Err(ArtifactCodecError::HashContractMismatch);
    }
    if header.contains_null && header.schema.null_semantics() != NullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    match header.kind {
        ArtifactKind::EmptyDomain => {
            if header.contains_null || !header.payload.is_empty() {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
        }
        ArtifactKind::ValueSet => {
            validate_value_set(header.payload, header.contains_null, &header.schema)?
        }
        ArtifactKind::Bitset => {
            validate_bitset(header.payload, header.contains_null, header.schema)?
        }
        ArtifactKind::Bloom => validate_bloom(
            header.payload,
            header.contains_null,
            header.schema,
            header
                .hash_contract
                .ok_or(ArtifactCodecError::InvalidHashContract)?,
        )?,
        ArtifactKind::Range => return Err(ArtifactCodecError::UnsupportedKind),
    }

    let accounted_resident_bytes = PhysicalArtifact::accounted_resident_bytes(encoded.len())
        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let retention =
        ArtifactRetention::try_new(accounted_resident_bytes, retained_budget, memory_account)?;
    let bytes: Arc<[u8]> = Arc::from(encoded);
    let artifact = PhysicalArtifact::from_retained_bytes(
        header.kind,
        header.schema_digest,
        header.logical_version,
        header.contains_null,
        bytes,
        accounted_resident_bytes,
        retention,
    )
    .map_err(|_| ArtifactCodecError::ResourceLimit)?;
    Ok(Arc::new(artifact))
}

struct ParsedHeader<'a> {
    kind: ArtifactKind,
    schema_digest: ArtifactSchemaDigest,
    schema: ArtifactMembershipSchemaView<'a>,
    logical_version: LogicalVersion,
    contains_null: bool,
    hash_contract: Option<HashContractDigest>,
    payload: &'a [u8],
}

fn parse_header(encoded: &[u8]) -> Result<ParsedHeader<'_>, ArtifactCodecError> {
    let mut reader = Reader::new(encoded);
    if reader.read_exact(4)? != MAGIC {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let version = reader.read_u16()?;
    if version != LEAF_CODEC_VERSION {
        return Err(ArtifactCodecError::UnknownVersion);
    }
    let kind = ArtifactKind::from_tag(reader.read_u8()?).ok_or(ArtifactCodecError::UnknownKind)?;
    let schema_digest = ArtifactSchemaDigest::from_canonical_bytes(reader.read_array::<32>()?);
    let schema_len = usize::from(reader.read_u16()?);
    let schema = ArtifactMembershipSchema::view(reader.read_exact(schema_len)?)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
    if schema.digest() != schema_digest {
        return Err(ArtifactCodecError::SchemaMismatch);
    }
    let logical_version = LogicalVersion::new(reader.read_u64()?);
    let flags = reader.read_u8()?;
    if flags & !FLAG_CONTAINS_NULL != 0 {
        return Err(ArtifactCodecError::InvalidFlags);
    }
    let hash_contract = match reader.read_u8()? {
        0 => None,
        1 => Some(HashContractDigest::new(reader.read_array::<32>()?)),
        _ => return Err(ArtifactCodecError::InvalidHashContract),
    };
    if matches!(kind, ArtifactKind::Bloom) != hash_contract.is_some() {
        return Err(ArtifactCodecError::InvalidHashContract);
    }
    let payload_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let payload = reader.read_exact(payload_len)?;
    if !reader.is_empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    Ok(ParsedHeader {
        kind,
        schema_digest,
        schema,
        logical_version,
        contains_null: flags & FLAG_CONTAINS_NULL != 0,
        hash_contract,
        payload,
    })
}

fn validate_value_set(
    payload: &[u8],
    contains_null: bool,
    schema: &ArtifactMembershipSchemaView<'_>,
) -> Result<(), ArtifactCodecError> {
    let mut reader = Reader::new(payload);
    let tag = reader.read_u8()?;
    if tag != schema.payload_tag() {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    match tag {
        1 => validate_ordered(&mut reader, |reader| match reader.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ArtifactCodecError::NonCanonicalPayload),
        })?,
        2 => validate_ordered(&mut reader, |reader| Ok(reader.read_u8()? as i8))?,
        3 => validate_ordered(&mut reader, |reader| reader.read_i16())?,
        4 | 10 => validate_ordered(&mut reader, |reader| reader.read_i32())?,
        5 => validate_ordered(&mut reader, |reader| reader.read_i64())?,
        6 => validate_ordered(&mut reader, |reader| reader.read_i128())?,
        7 => validate_ordered_by(
            &mut reader,
            |reader| reader.read_u32(),
            |left, right| f32::from_bits(*left).total_cmp(&f32::from_bits(*right)),
            canonical_f32_bits,
        )?,
        8 => validate_ordered_by(
            &mut reader,
            |reader| reader.read_u64(),
            |left, right| f64::from_bits(*left).total_cmp(&f64::from_bits(*right)),
            canonical_f64_bits,
        )?,
        9 => validate_utf8(&mut reader)?,
        11 => validate_timestamp(&mut reader, *schema)?,
        12 => validate_decimal(&mut reader, *schema)?,
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    }
    if !reader.is_empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    let cardinality = cardinality_from_payload(payload)?;
    if cardinality == 0 && !contains_null {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if contains_null && schema.null_semantics() != NullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    Ok(())
}

fn validate_bitset(
    payload: &[u8],
    _contains_null: bool,
    schema: ArtifactMembershipSchemaView<'_>,
) -> Result<(), ArtifactCodecError> {
    let mut reader = Reader::new(payload);
    let type_tag = reader.read_u8()?;
    if type_tag != schema.payload_tag()
        || !matches!(type_tag, 1 | 2 | 3 | 4 | 5 | 10 | 12)
        || (type_tag == 12 && !matches!(schema.decimal_contract(), Some((1..=18, _))))
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let min = reader.read_i64()?;
    let max = reader.read_i64()?;
    let endpoints_representable = match type_tag {
        1 => min >= 0 && max <= 1,
        2 => min >= i64::from(i8::MIN) && max <= i64::from(i8::MAX),
        3 => min >= i64::from(i16::MIN) && max <= i64::from(i16::MAX),
        4 | 10 => min >= i64::from(i32::MIN) && max <= i64::from(i32::MAX),
        5 => true,
        12 => schema.decimal_contract().is_some_and(|(precision, _)| {
            10_i64
                .checked_pow(u32::from(precision))
                .is_some_and(|limit| min > -limit && max < limit)
        }),
        _ => false,
    };
    if min > max || !endpoints_representable {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bit_count = reader.read_u64()?;
    let expected = i128::from(max)
        .checked_sub(i128::from(min))
        .and_then(|span| span.checked_add(1))
        .and_then(|span| u64::try_from(span).ok())
        .ok_or(ArtifactCodecError::LengthOverflow)?;
    if bit_count == 0 || bit_count != expected {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let byte_count = usize::try_from(
        bit_count
            .checked_add(7)
            .ok_or(ArtifactCodecError::LengthOverflow)?
            / 8,
    )
    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if reader.remaining_len() != byte_count {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bits = reader.read_exact(byte_count)?;
    if bits.first().is_none_or(|byte| byte & 1 == 0) {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let last_index = bit_count - 1;
    let last_byte =
        usize::try_from(last_index / 8).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if bits[last_byte] & (1 << (last_index % 8)) == 0 {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let used_in_last = bit_count % 8;
    if used_in_last != 0 {
        let padding_mask = !((1u8 << used_in_last) - 1);
        if bits.last().is_some_and(|byte| byte & padding_mask != 0) {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
    }
    Ok(())
}

fn validate_bloom(
    payload: &[u8],
    _contains_null: bool,
    schema: ArtifactMembershipSchemaView<'_>,
    expected_digest: HashContractDigest,
) -> Result<(), ArtifactCodecError> {
    if payload.len() < BLOOM_METADATA_BYTES {
        return Err(ArtifactCodecError::Truncated);
    }
    let mut reader = Reader::new(payload);
    let algorithm_version = reader.read_u16()?;
    let scalar_framing_version = reader.read_u16()?;
    let seed = reader.read_u64()?;
    let bits_per_key = reader.read_u64()?;
    let hash_count = reader.read_u32()?;
    let cardinality = reader.read_u64()?;
    let bit_count = reader.read_u64()?;
    let contract = BloomHashContract::from_fields(
        schema.digest(),
        algorithm_version,
        scalar_framing_version,
        seed,
        bits_per_key,
        hash_count,
    )
    .map_err(|_| ArtifactCodecError::InvalidHashContract)?;
    if contract.digest() != expected_digest
        || contract
            .bit_count_u64(cardinality)
            .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?
            != bit_count
        || bit_count % 64 != 0
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let byte_count =
        usize::try_from(bit_count / 8).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if reader.remaining_len() != byte_count {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bits = reader.read_exact(byte_count)?;
    if bits.iter().all(|byte| *byte == 0) {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    Ok(())
}

fn cardinality_from_payload(payload: &[u8]) -> Result<u64, ArtifactCodecError> {
    let tag = *payload.first().ok_or(ArtifactCodecError::Truncated)?;
    let offset = match tag {
        11 => {
            let mut reader = Reader::new(&payload[1..]);
            reader.read_u8()?;
            match reader.read_u8()? {
                0 => {}
                1 => {
                    let len = usize::try_from(reader.read_u64()?)
                        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                    reader.read_exact(len)?;
                }
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            }
            payload.len() - reader.remaining_len()
        }
        12 => 3,
        _ => 1,
    };
    let bytes = payload
        .get(offset..offset + 8)
        .ok_or(ArtifactCodecError::Truncated)?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("eight-byte slice"),
    ))
}

fn validate_ordered<T: Ord>(
    reader: &mut Reader<'_>,
    read: impl FnMut(&mut Reader<'_>) -> Result<T, ArtifactCodecError>,
) -> Result<(), ArtifactCodecError> {
    validate_ordered_by(reader, read, Ord::cmp, |_| true)
}

fn validate_ordered_by<T>(
    reader: &mut Reader<'_>,
    mut read: impl FnMut(&mut Reader<'_>) -> Result<T, ArtifactCodecError>,
    compare: impl Fn(&T, &T) -> Ordering,
    canonical: impl Fn(&T) -> bool,
) -> Result<(), ArtifactCodecError> {
    let count =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let mut previous = None;
    for _ in 0..count {
        let value = read(reader)?;
        if !canonical(&value)
            || previous
                .as_ref()
                .is_some_and(|old| compare(old, &value) != Ordering::Less)
        {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
        previous = Some(value);
    }
    Ok(())
}

fn canonical_f32_bits(bits: &u32) -> bool {
    let value = f32::from_bits(*bits);
    (!value.is_nan() || *bits == 0x7fc0_0000) && (value != 0.0 || *bits == 0)
}

fn canonical_f64_bits(bits: &u64) -> bool {
    let value = f64::from_bits(*bits);
    (!value.is_nan() || *bits == 0x7ff8_0000_0000_0000) && (value != 0.0 || *bits == 0)
}

fn validate_utf8(reader: &mut Reader<'_>) -> Result<(), ArtifactCodecError> {
    let count =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let mut previous: Option<&str> = None;
    for _ in 0..count {
        let len =
            usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
        let value = std::str::from_utf8(reader.read_exact(len)?)
            .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
        if previous.is_some_and(|old| old >= value) {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
        previous = Some(value);
    }
    Ok(())
}

fn validate_timestamp(
    reader: &mut Reader<'_>,
    expected: ArtifactMembershipSchemaView<'_>,
) -> Result<(), ArtifactCodecError> {
    let Some((expected_unit, expected_timezone)) = expected.timestamp_contract() else {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    };
    let unit = reader.read_u8()?;
    if unit != expected_unit {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    match reader.read_u8()? {
        0 if expected_timezone.is_none() => {}
        1 => {
            let len = usize::try_from(reader.read_u64()?)
                .map_err(|_| ArtifactCodecError::LengthOverflow)?;
            let timezone = std::str::from_utf8(reader.read_exact(len)?)
                .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
            if expected_timezone != Some(timezone) {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
        }
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    }
    validate_ordered(reader, |reader| reader.read_i64())
}

fn validate_decimal(
    reader: &mut Reader<'_>,
    expected: ArtifactMembershipSchemaView<'_>,
) -> Result<(), ArtifactCodecError> {
    let Some((expected_precision, expected_scale)) = expected.decimal_contract() else {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    };
    let precision = reader.read_u8()?;
    let scale = reader.read_u8()? as i8;
    if precision != expected_precision
        || scale != expected_scale
        || precision == 0
        || precision > arrow::datatypes::DECIMAL128_MAX_PRECISION
        || (scale > 0 && scale as u8 > precision)
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bound = 10_i128
        .checked_pow(u32::from(precision))
        .ok_or(ArtifactCodecError::NonCanonicalPayload)?;
    validate_ordered_by(
        reader,
        |reader| reader.read_i128(),
        Ord::cmp,
        |value| *value > -bound && *value < bound,
    )
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
    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ArtifactCodecError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(len)
            .ok_or(ArtifactCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ArtifactCodecError> {
        Ok(self.read_exact(N)?.try_into().expect("exact array length"))
    }
    fn read_u8(&mut self) -> Result<u8, ArtifactCodecError> {
        Ok(self.read_exact(1)?[0])
    }
    fn read_u16(&mut self) -> Result<u16, ArtifactCodecError> {
        Ok(u16::from_be_bytes(self.read_exact(2)?.try_into().unwrap()))
    }
    fn read_u32(&mut self) -> Result<u32, ArtifactCodecError> {
        Ok(u32::from_be_bytes(self.read_exact(4)?.try_into().unwrap()))
    }
    fn read_u64(&mut self) -> Result<u64, ArtifactCodecError> {
        Ok(u64::from_be_bytes(self.read_exact(8)?.try_into().unwrap()))
    }
    fn read_i16(&mut self) -> Result<i16, ArtifactCodecError> {
        Ok(i16::from_be_bytes(self.read_exact(2)?.try_into().unwrap()))
    }
    fn read_i32(&mut self) -> Result<i32, ArtifactCodecError> {
        Ok(i32::from_be_bytes(self.read_exact(4)?.try_into().unwrap()))
    }
    fn read_i64(&mut self) -> Result<i64, ArtifactCodecError> {
        Ok(i64::from_be_bytes(self.read_exact(8)?.try_into().unwrap()))
    }
    fn read_i128(&mut self) -> Result<i128, ArtifactCodecError> {
        Ok(i128::from_be_bytes(
            self.read_exact(16)?.try_into().unwrap(),
        ))
    }
}

#[cfg(test)]
fn decode_leaf_unretained_for_test(
    encoded: &[u8],
    data_type: &arrow::datatypes::DataType,
    null_semantics: NullSemantics,
    logical_version: LogicalVersion,
) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
    struct Unlimited;
    impl RuntimeFilterMemoryAccount for Unlimited {
        fn try_consume(
            &self,
            _bytes: usize,
        ) -> Result<(), crate::runtime_filter::port::support::MemoryAccountError> {
            Ok(())
        }
        fn release(&self, _bytes: usize) {}
    }
    let header = parse_header(encoded)?;
    decode_leaf(
        encoded,
        ArtifactDecodeExpectations {
            expected_kind: header.kind,
            expected_schema_digest: ArtifactSchemaDigest::for_membership(data_type, null_semantics)
                .map_err(|_| ArtifactCodecError::SchemaMismatch)?,
            expected_logical_version: logical_version,
            expected_hash_contract: None,
        },
        encoded.len(),
        Arc::new(ArtifactRetainedBudget::new(
            PhysicalArtifact::accounted_resident_bytes(encoded.len())
                .map_err(|_| ArtifactCodecError::LengthOverflow)?,
        )),
        Arc::new(Unlimited),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::NullSemantics;
    use crate::runtime_filter::port::artifact::{
        ArtifactKind, ArtifactMembershipSchema, ArtifactSchemaDigest, HashContractDigest,
        PhysicalArtifact,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::install::MaterializationPolicy;
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, MemoryAccountError, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ReducedMembershipDomain};

    use super::super::bloom::{BloomHashContract, build_bits};
    use super::{
        ArtifactCodecError, ArtifactDecodeExpectations, decode_leaf,
        decode_leaf_unretained_for_test, encode_membership_leaf, encode_physical_leaf,
    };

    #[derive(Default)]
    struct CountingAccount {
        retained: AtomicUsize,
        reject: bool,
    }

    impl RuntimeFilterMemoryAccount for CountingAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            if self.reject {
                return Err(MemoryAccountError::CapacityExceeded);
            }
            self.retained.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.retained.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    fn int64_leaf(values: impl IntoIterator<Item = i64>, contains_null: bool) -> Vec<u8> {
        encode_membership_leaf(
            &ReducedMembershipDomain::new(MembershipValues::int64(values), contains_null),
            if contains_null {
                NullSemantics::NullSafeEqual
            } else {
                NullSemantics::NeverMatches
            },
            LogicalVersion::FIRST,
        )
        .unwrap()
    }

    fn int64_expectations(contains_null: bool) -> ArtifactDecodeExpectations {
        ArtifactDecodeExpectations {
            expected_kind: ArtifactKind::ValueSet,
            expected_schema_digest: ArtifactSchemaDigest::for_membership(
                &DataType::Int64,
                if contains_null {
                    NullSemantics::NullSafeEqual
                } else {
                    NullSemantics::NeverMatches
                },
            )
            .unwrap(),
            expected_logical_version: LogicalVersion::FIRST,
            expected_hash_contract: None,
        }
    }

    #[test]
    fn null_only_membership_is_not_empty_domain() {
        let domain = ReducedMembershipDomain::new(MembershipValues::int64([]), true);
        let encoded =
            encode_membership_leaf(&domain, NullSemantics::NullSafeEqual, LogicalVersion::FIRST)
                .unwrap();
        let decoded = decode_leaf_unretained_for_test(
            &encoded,
            &DataType::Int64,
            NullSemantics::NullSafeEqual,
            LogicalVersion::FIRST,
        )
        .unwrap();

        assert_eq!(decoded.kind(), ArtifactKind::ValueSet);
        assert!(decoded.contains_null());
    }

    #[test]
    fn encoder_rejects_nulls_under_never_matches_semantics() {
        let domain = ReducedMembershipDomain::new(MembershipValues::int64([1]), true);

        assert_eq!(
            encode_membership_leaf(&domain, NullSemantics::NeverMatches, LogicalVersion::FIRST,)
                .unwrap_err(),
            ArtifactCodecError::NonCanonicalPayload
        );
    }

    #[test]
    fn value_set_round_trips_every_m1_membership_type() {
        let cases = vec![
            MembershipValues::boolean([false, true]),
            MembershipValues::int8([-1, 2]),
            MembershipValues::int16([-2, 3]),
            MembershipValues::int32([-3, 4]),
            MembershipValues::int64([-4, 5]),
            MembershipValues::large_int([-5, 6]),
            MembershipValues::float32([f32::NEG_INFINITY, -0.0, 0.0, f32::INFINITY, f32::NAN]),
            MembershipValues::float64([f64::NEG_INFINITY, -0.0, 0.0, f64::INFINITY, f64::NAN]),
            MembershipValues::utf8(["a", "z"]),
            MembershipValues::date32([-7, 8]),
            MembershipValues::timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("Asia/Shanghai".into()),
                [-9, 10],
            ),
            MembershipValues::decimal128(18, 3, [-11, 12]).unwrap(),
        ];
        for values in cases {
            let data_type = values.data_type();
            let domain = ReducedMembershipDomain::new(values, false);
            let encoded =
                encode_membership_leaf(&domain, NullSemantics::NeverMatches, LogicalVersion::FIRST)
                    .unwrap();
            let decoded = decode_leaf_unretained_for_test(
                &encoded,
                &data_type,
                NullSemantics::NeverMatches,
                LogicalVersion::FIRST,
            )
            .unwrap();
            assert_eq!(decoded.kind(), ArtifactKind::ValueSet);
            assert_eq!(decoded.canonical_bytes(), encoded);
        }
    }

    #[test]
    fn true_empty_domain_has_distinct_strict_encoding() {
        let encoded = int64_leaf([], false);
        let decoded = decode_leaf_unretained_for_test(
            &encoded,
            &DataType::Int64,
            NullSemantics::NeverMatches,
            LogicalVersion::FIRST,
        )
        .unwrap();
        assert_eq!(decoded.kind(), ArtifactKind::EmptyDomain);
        assert!(!decoded.contains_null());
    }

    #[test]
    fn decode_requires_typed_kind_schema_version_and_hash_expectations() {
        let encoded = int64_leaf([1, 2], false);
        let budget = Arc::new(ArtifactRetainedBudget::new(
            PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap() * 4,
        ));
        let account = Arc::new(CountingAccount::default());
        let baseline = int64_expectations(false);

        let mut wrong = baseline;
        wrong.expected_kind = ArtifactKind::EmptyDomain;
        assert_eq!(
            decode_leaf(
                &encoded,
                wrong,
                encoded.len(),
                budget.clone(),
                account.clone()
            )
            .unwrap_err(),
            ArtifactCodecError::KindMismatch
        );
        let mut wrong = baseline;
        wrong.expected_schema_digest =
            ArtifactSchemaDigest::for_membership(&DataType::Utf8, NullSemantics::NeverMatches)
                .unwrap();
        assert_eq!(
            decode_leaf(
                &encoded,
                wrong,
                encoded.len(),
                budget.clone(),
                account.clone()
            )
            .unwrap_err(),
            ArtifactCodecError::SchemaMismatch
        );
        let mut wrong = baseline;
        wrong.expected_logical_version = LogicalVersion::new(99);
        assert_eq!(
            decode_leaf(
                &encoded,
                wrong,
                encoded.len(),
                budget.clone(),
                account.clone()
            )
            .unwrap_err(),
            ArtifactCodecError::VersionMismatch
        );
        let mut wrong = baseline;
        wrong.expected_hash_contract = Some(HashContractDigest::new([7; 32]));
        assert_eq!(
            decode_leaf(
                &encoded,
                wrong,
                encoded.len(),
                budget.clone(),
                account.clone()
            )
            .unwrap_err(),
            ArtifactCodecError::HashContractMismatch
        );
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(account.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn decode_budget_and_memory_failures_roll_back_every_reservation() {
        let encoded = int64_leaf([1, 2], false);
        let expectations = int64_expectations(false);
        let account = Arc::new(CountingAccount::default());
        let footprint = PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap();
        let budget = Arc::new(ArtifactRetainedBudget::new(footprint));
        assert_eq!(
            decode_leaf(
                &encoded,
                expectations,
                encoded.len() - 1,
                budget.clone(),
                account.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::EncodedSizeExceeded
        );
        let first = decode_leaf(
            &encoded,
            expectations,
            encoded.len(),
            budget.clone(),
            account.clone(),
        )
        .unwrap();
        assert_eq!(budget.retained_bytes(), footprint);
        assert_eq!(account.retained.load(Ordering::SeqCst), footprint);
        assert_eq!(
            decode_leaf(
                &encoded,
                expectations,
                encoded.len(),
                budget.clone(),
                account.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::ResourceLimit
        );
        drop(first);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(account.retained.load(Ordering::SeqCst), 0);

        let rejecting = Arc::new(CountingAccount {
            retained: AtomicUsize::new(0),
            reject: true,
        });
        assert_eq!(
            decode_leaf(
                &encoded,
                expectations,
                encoded.len(),
                budget.clone(),
                rejecting.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::ResourceLimit
        );
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(rejecting.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn decode_rejects_truncated_trailing_unknown_and_noncanonical_values() {
        let encoded = int64_leaf([1, 2], false);
        let expectations = int64_expectations(false);
        let decode = |bytes: &[u8]| {
            decode_leaf(
                bytes,
                expectations,
                bytes.len() + 1,
                Arc::new(ArtifactRetainedBudget::new(bytes.len() + 1)),
                Arc::new(CountingAccount::default()),
            )
        };
        assert_eq!(
            decode(&encoded[..encoded.len() - 1]).unwrap_err(),
            ArtifactCodecError::Truncated
        );
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            decode(&trailing).unwrap_err(),
            ArtifactCodecError::TrailingBytes
        );
        let mut unknown = encoded.clone();
        unknown[4..6].copy_from_slice(&99_u16.to_be_bytes());
        assert_eq!(
            decode(&unknown).unwrap_err(),
            ArtifactCodecError::UnknownVersion
        );

        let schema_len = u16::from_be_bytes(encoded[39..41].try_into().unwrap()) as usize;
        let mut duplicate = encoded;
        let payload_start = 4 + 2 + 1 + 32 + 2 + schema_len + 8 + 1 + 1 + 8;
        let first_value = payload_start + 1 + 8;
        let second_value = first_value + 8;
        let duplicate_bytes: [u8; 8] = duplicate[first_value..first_value + 8].try_into().unwrap();
        duplicate[second_value..second_value + 8].copy_from_slice(&duplicate_bytes);
        assert_eq!(
            decode(&duplicate).unwrap_err(),
            ArtifactCodecError::NonCanonicalPayload
        );
    }

    #[test]
    fn decode_rejects_payload_type_spliced_under_an_expected_schema_digest() {
        let int64 = int64_leaf([1], false);
        let mut utf8 = encode_membership_leaf(
            &ReducedMembershipDomain::new(MembershipValues::utf8(["x"]), false),
            NullSemantics::NeverMatches,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let schema_digest_offset = 4 + 2 + 1;
        utf8[schema_digest_offset..schema_digest_offset + 32]
            .copy_from_slice(&int64[schema_digest_offset..schema_digest_offset + 32]);

        assert!(
            decode_leaf(
                &utf8,
                int64_expectations(false),
                utf8.len(),
                Arc::new(ArtifactRetainedBudget::new(utf8.len())),
                Arc::new(CountingAccount::default()),
            )
            .is_err()
        );
    }

    fn decode_test_leaf(
        encoded: &[u8],
        kind: ArtifactKind,
        schema: &ArtifactMembershipSchema,
        hash_contract: Option<HashContractDigest>,
    ) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
        decode_leaf(
            encoded,
            ArtifactDecodeExpectations {
                expected_kind: kind,
                expected_schema_digest: schema.digest(),
                expected_logical_version: LogicalVersion::FIRST,
                expected_hash_contract: hash_contract,
            },
            encoded.len(),
            Arc::new(ArtifactRetainedBudget::new(
                PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap(),
            )),
            Arc::new(CountingAccount::default()),
        )
    }

    #[test]
    fn bitset_decoder_rejects_span_padding_endpoint_and_schema_violations() {
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let payload = |min: i64, max: i64, bit_count: u64, bits: &[u8]| {
            let mut payload = vec![5];
            payload.extend_from_slice(&min.to_be_bytes());
            payload.extend_from_slice(&max.to_be_bytes());
            payload.extend_from_slice(&bit_count.to_be_bytes());
            payload.extend_from_slice(bits);
            payload
        };
        let encode = |payload: &[u8]| {
            encode_physical_leaf(
                ArtifactKind::Bitset,
                &schema,
                LogicalVersion::FIRST,
                false,
                None,
                payload,
            )
            .unwrap()
        };
        let valid = encode(&payload(5, 7, 3, &[0b0000_0101]));
        assert!(decode_test_leaf(&valid, ArtifactKind::Bitset, &schema, None).is_ok());
        for malformed in [
            payload(7, 5, 3, &[0b0000_0101]),
            payload(5, 7, 2, &[0b0000_0011]),
            payload(5, 7, 3, &[0b1000_0101]),
            payload(5, 7, 3, &[0b0000_0100]),
            payload(5, 7, 3, &[0b0000_0001]),
            payload(i64::MIN, i64::MAX, u64::MAX, &[1]),
        ] {
            let encoded = encode(&malformed);
            assert!(decode_test_leaf(&encoded, ArtifactKind::Bitset, &schema, None).is_err());
        }

        let boolean =
            ArtifactMembershipSchema::new(&DataType::Boolean, NullSemantics::NeverMatches).unwrap();
        let mut boolean_payload = vec![1];
        boolean_payload.extend_from_slice(&0_i64.to_be_bytes());
        boolean_payload.extend_from_slice(&2_i64.to_be_bytes());
        boolean_payload.extend_from_slice(&3_u64.to_be_bytes());
        boolean_payload.push(0b0000_0101);
        let encoded = encode_physical_leaf(
            ArtifactKind::Bitset,
            &boolean,
            LogicalVersion::FIRST,
            false,
            None,
            &boolean_payload,
        )
        .unwrap();
        assert!(decode_test_leaf(&encoded, ArtifactKind::Bitset, &boolean, None).is_err());

        let decimal = ArtifactMembershipSchema::new(
            &DataType::Decimal128(19, 0),
            NullSemantics::NeverMatches,
        )
        .unwrap();
        let mut decimal_payload = vec![12];
        decimal_payload.extend_from_slice(&0_i64.to_be_bytes());
        decimal_payload.extend_from_slice(&0_i64.to_be_bytes());
        decimal_payload.extend_from_slice(&1_u64.to_be_bytes());
        decimal_payload.push(1);
        let encoded = encode_physical_leaf(
            ArtifactKind::Bitset,
            &decimal,
            LogicalVersion::FIRST,
            false,
            None,
            &decimal_payload,
        )
        .unwrap();
        assert!(decode_test_leaf(&encoded, ArtifactKind::Bitset, &decimal, None).is_err());
    }

    #[test]
    fn bitset_decoder_rejects_endpoints_outside_lossless_schema_range() {
        let malformed = [
            (
                DataType::Int8,
                2,
                i64::from(i8::MAX),
                i64::from(i8::MAX) + 1,
            ),
            (
                DataType::Int16,
                3,
                i64::from(i16::MAX),
                i64::from(i16::MAX) + 1,
            ),
            (
                DataType::Int32,
                4,
                i64::from(i32::MAX),
                i64::from(i32::MAX) + 1,
            ),
            (
                DataType::Date32,
                10,
                i64::from(i32::MAX),
                i64::from(i32::MAX) + 1,
            ),
            (DataType::Decimal128(2, 0), 12, 99, 100),
            (DataType::Decimal128(2, 0), 12, -101, -99),
        ];
        for (data_type, type_tag, min, max) in malformed {
            let schema =
                ArtifactMembershipSchema::new(&data_type, NullSemantics::NeverMatches).unwrap();
            let bit_count = u64::try_from(i128::from(max) - i128::from(min) + 1).unwrap();
            let mut payload = vec![type_tag];
            payload.extend_from_slice(&min.to_be_bytes());
            payload.extend_from_slice(&max.to_be_bytes());
            payload.extend_from_slice(&bit_count.to_be_bytes());
            let mut bits = vec![0; usize::try_from((bit_count + 7) / 8).unwrap()];
            bits[0] |= 1;
            let last = bit_count - 1;
            bits[usize::try_from(last / 8).unwrap()] |= 1 << (last % 8);
            payload.extend_from_slice(&bits);
            let encoded = encode_physical_leaf(
                ArtifactKind::Bitset,
                &schema,
                LogicalVersion::FIRST,
                false,
                None,
                &payload,
            )
            .unwrap();

            assert!(
                decode_test_leaf(&encoded, ArtifactKind::Bitset, &schema, None).is_err(),
                "accepted out-of-range endpoints {min}..={max} for {data_type:?}"
            );
        }
    }

    #[test]
    fn bloom_decoder_rebuilds_and_validates_full_contract_metadata() {
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let policy = MaterializationPolicy::new(8, 5, 17, 1, 1 << 20, 1 << 16, 1).unwrap();
        let contract = BloomHashContract::new(&schema, policy).unwrap();
        let values = MembershipValues::int64([1, 7, 42]);
        let (bit_count, bits) = build_bits(&values, &contract, &mut Vec::new()).unwrap();
        let payload = |algorithm: u16, cardinality: u64, bit_count: u64, bits: &[u8]| {
            let mut payload = Vec::new();
            payload.extend_from_slice(&algorithm.to_be_bytes());
            payload.extend_from_slice(&contract.scalar_framing_version().to_be_bytes());
            payload.extend_from_slice(&contract.seed().to_be_bytes());
            payload.extend_from_slice(&contract.bits_per_key().to_be_bytes());
            payload.extend_from_slice(&contract.hash_count().to_be_bytes());
            payload.extend_from_slice(&cardinality.to_be_bytes());
            payload.extend_from_slice(&bit_count.to_be_bytes());
            payload.extend_from_slice(bits);
            payload
        };
        let encode = |payload: &[u8]| {
            encode_physical_leaf(
                ArtifactKind::Bloom,
                &schema,
                LogicalVersion::FIRST,
                false,
                Some(contract.digest()),
                payload,
            )
            .unwrap()
        };
        let valid = encode(&payload(1, 3, bit_count, &bits));
        assert!(
            decode_test_leaf(
                &valid,
                ArtifactKind::Bloom,
                &schema,
                Some(contract.digest())
            )
            .is_ok()
        );
        for malformed in [
            payload(2, 3, bit_count, &bits),
            payload(1, 0, bit_count, &bits),
            payload(1, 3, bit_count + 64, &bits),
            payload(1, 3, bit_count, &vec![0; bits.len()]),
            payload(1, 3, bit_count, &bits[..bits.len() - 1]),
        ] {
            let encoded = encode(&malformed);
            assert!(
                decode_test_leaf(
                    &encoded,
                    ArtifactKind::Bloom,
                    &schema,
                    Some(contract.digest())
                )
                .is_err()
            );
        }
    }
}
