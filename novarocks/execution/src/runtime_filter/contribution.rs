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

//! Execution-owned canonical runtime-filter contributions. This module has no
//! participant, connector, or delivery dependencies.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;
use sha2::{Digest, Sha256};

pub const FINGERPRINT_VERSION_TAG: &[u8] = b"novarocks.runtime-filter.value-domain-delta.v1";
const MAGIC: &[u8; 4] = b"NRFC";
const CODEC_VERSION: u16 = 1;
const HEADER_LEN: usize = 48;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContributionCodecError {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid runtime filter contribution: {self:?}")
    }
}
impl Error for ContributionCodecError {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContributionSizeError {
    LengthExceedsCanonicalRange,
    SizeOverflow,
}
impl fmt::Display for ContributionSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "canonical contribution size error: {self:?}")
    }
}
impl Error for ContributionSizeError {}
impl From<ContributionSizeError> for ContributionCodecError {
    fn from(_: ContributionSizeError) -> Self {
        Self::LengthOverflow
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalF32(u32);
impl CanonicalF32 {
    pub fn new(value: f32) -> Self {
        Self(if value == 0.0 {
            0
        } else if value.is_nan() {
            0x7fc0_0000
        } else {
            value.to_bits()
        })
    }
    pub const fn bits(self) -> u32 {
        self.0
    }
}
impl Ord for CanonicalF32 {
    fn cmp(&self, other: &Self) -> Ordering {
        f32::from_bits(self.0).total_cmp(&f32::from_bits(other.0))
    }
}
impl PartialOrd for CanonicalF32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalF64(u64);
impl CanonicalF64 {
    pub fn new(value: f64) -> Self {
        Self(if value == 0.0 {
            0
        } else if value.is_nan() {
            0x7ff8_0000_0000_0000
        } else {
            value.to_bits()
        })
    }
    pub const fn bits(self) -> u64 {
        self.0
    }
}
impl Ord for CanonicalF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        f64::from_bits(self.0).total_cmp(&f64::from_bits(other.0))
    }
}
impl PartialOrd for CanonicalF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipValues {
    Boolean(BTreeSet<bool>),
    Int8(BTreeSet<i8>),
    Int16(BTreeSet<i16>),
    Int32(BTreeSet<i32>),
    Int64(BTreeSet<i64>),
    LargeInt(BTreeSet<i128>),
    Float32(BTreeSet<CanonicalF32>),
    Float64(BTreeSet<CanonicalF64>),
    Utf8(BTreeSet<String>),
    Date32(BTreeSet<i32>),
    Timestamp {
        unit: TimeUnit,
        timezone: Option<String>,
        values: BTreeSet<i64>,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
        values: BTreeSet<i128>,
    },
}
macro_rules! set_constructor {
    ($name:ident, $variant:ident, $ty:ty) => {
        pub fn $name(values: impl IntoIterator<Item = $ty>) -> Self {
            Self::$variant(values.into_iter().collect())
        }
    };
}
impl MembershipValues {
    set_constructor!(boolean, Boolean, bool);
    set_constructor!(int8, Int8, i8);
    set_constructor!(int16, Int16, i16);
    set_constructor!(int32, Int32, i32);
    set_constructor!(int64, Int64, i64);
    set_constructor!(large_int, LargeInt, i128);
    set_constructor!(date32, Date32, i32);
    pub fn float32(values: impl IntoIterator<Item = f32>) -> Self {
        Self::Float32(values.into_iter().map(CanonicalF32::new).collect())
    }
    pub fn float64(values: impl IntoIterator<Item = f64>) -> Self {
        Self::Float64(values.into_iter().map(CanonicalF64::new).collect())
    }
    pub fn utf8<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Utf8(values.into_iter().map(Into::into).collect())
    }
    pub fn timestamp(
        unit: TimeUnit,
        timezone: Option<impl Into<String>>,
        values: impl IntoIterator<Item = i64>,
    ) -> Self {
        Self::Timestamp {
            unit,
            timezone: timezone.map(Into::into),
            values: values.into_iter().collect(),
        }
    }
    pub fn decimal128(
        precision: u8,
        scale: i8,
        values: impl IntoIterator<Item = i128>,
    ) -> Result<Self, ContributionCodecError> {
        let values = values.into_iter().collect();
        validate_decimal(precision, scale, &values)?;
        Ok(Self::Decimal128 {
            precision,
            scale,
            values,
        })
    }
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Boolean(_) => DataType::Boolean,
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::LargeInt(_) => DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Date32(_) => DataType::Date32,
            Self::Timestamp { unit, timezone, .. } => {
                DataType::Timestamp(unit.clone(), timezone.clone().map(Into::into))
            }
            Self::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128(*precision, *scale),
        }
    }
    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), ContributionSizeError> {
        macro_rules! fixed {
            ($tag:expr, $values:expr, $encode:expr) => {{
                out.push($tag);
                push_count(out, $values.len())?;
                for v in $values {
                    out.extend_from_slice(&$encode(v));
                }
            }};
        }
        match self {
            Self::Boolean(v) => fixed!(1, v, |x: &bool| [u8::from(*x)]),
            Self::Int8(v) => fixed!(2, v, |x: &i8| x.to_be_bytes()),
            Self::Int16(v) => fixed!(3, v, |x: &i16| x.to_be_bytes()),
            Self::Int32(v) => fixed!(4, v, |x: &i32| x.to_be_bytes()),
            Self::Int64(v) => fixed!(5, v, |x: &i64| x.to_be_bytes()),
            Self::LargeInt(v) => fixed!(6, v, |x: &i128| x.to_be_bytes()),
            Self::Float32(v) => fixed!(7, v, |x: &CanonicalF32| x.bits().to_be_bytes()),
            Self::Float64(v) => fixed!(8, v, |x: &CanonicalF64| x.bits().to_be_bytes()),
            Self::Utf8(v) => {
                out.push(9);
                push_count(out, v.len())?;
                for x in v {
                    push_bytes(out, x.as_bytes())?;
                }
            }
            Self::Date32(v) => fixed!(10, v, |x: &i32| x.to_be_bytes()),
            Self::Timestamp {
                unit,
                timezone,
                values,
            } => {
                out.extend_from_slice(&[11, time_unit_tag(unit)]);
                match timezone {
                    None => out.push(0),
                    Some(tz) => {
                        out.push(1);
                        push_bytes(out, tz.as_bytes())?;
                    }
                };
                push_count(out, values.len())?;
                for x in values {
                    out.extend_from_slice(&x.to_be_bytes());
                }
            }
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => {
                out.extend_from_slice(&[12, *precision, *scale as u8]);
                push_count(out, values.len())?;
                for x in values {
                    out.extend_from_slice(&x.to_be_bytes());
                }
            }
        };
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValueDomainDelta {
    values: MembershipValues,
    contains_null: bool,
}
impl ValueDomainDelta {
    pub const fn new(values: MembershipValues, contains_null: bool) -> Self {
        Self {
            values,
            contains_null,
        }
    }
    pub const fn values(&self) -> &MembershipValues {
        &self.values
    }
    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }
    pub fn data_type(&self) -> DataType {
        self.values.data_type()
    }
    pub fn matches_data_type(&self, expected: &DataType) -> bool {
        self.data_type() == *expected
    }
    pub fn canonical_encoded_len(&self) -> Result<usize, ContributionSizeError> {
        let mut out = Vec::new();
        self.encode_canonical_into(&mut out)?;
        Ok(out.len())
    }
    pub fn encode_canonical_into(&self, out: &mut Vec<u8>) -> Result<(), ContributionSizeError> {
        push_bytes(out, FINGERPRINT_VERSION_TAG)?;
        self.values.encode_into(out)?;
        out.push(u8::from(self.contains_null));
        Ok(())
    }
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut canonical = Vec::new();
        self.encode_canonical_into(&mut canonical)
            .expect("canonical contribution lengths fit u64");
        Sha256::digest(canonical).into()
    }
}

/// Encodes one membership domain without a contribution envelope.
///
/// Final-domain completion transports exactly this canonical typed payload;
/// the completion fence and routing authority remain outside this value.
pub fn encode_value_domain(
    domain: &ValueDomainDelta,
    max_encoded_bytes: usize,
) -> Result<Vec<u8>, ContributionCodecError> {
    let mut bytes = Vec::new();
    domain.encode_canonical_into(&mut bytes)?;
    if bytes.len() > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }
    Ok(bytes)
}

/// Strictly decodes a canonical membership-domain payload without a frame.
pub fn decode_value_domain(
    payload: &[u8],
    data_type: &DataType,
    max_encoded_bytes: usize,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    if payload.len() > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }
    let domain = decode_membership(payload, data_type)?;
    if encode_value_domain(&domain, max_encoded_bytes)? != payload {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(domain)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderedScalar {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Utf8(String),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedTuple {
    values: Vec<Option<OrderedScalar>>,
}
impl OrderedTuple {
    pub fn new(values: impl IntoIterator<Item = Option<OrderedScalar>>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }
    pub fn values(&self) -> &[Option<OrderedScalar>] {
        &self.values
    }
    pub fn try_new(
        contract: &RuntimeOrderContract,
        values: impl IntoIterator<Item = Option<OrderedScalar>>,
    ) -> Result<Self, OrderedTupleError> {
        let tuple = Self::new(values);
        contract.validate_tuple(&tuple)?;
        Ok(tuple)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedBoundUpdate {
    contract_digest: [u8; 32],
    bound: OrderedTuple,
    replay_digest: [u8; 32],
}
impl OrderedBoundUpdate {
    pub fn new(contract_digest: [u8; 32], bound: OrderedTuple) -> Self {
        Self {
            contract_digest,
            replay_digest: ordered_replay_digest(contract_digest, &bound),
            bound,
        }
    }
    pub fn try_new(
        contract: &RuntimeOrderContract,
        bound: OrderedTuple,
    ) -> Result<Self, OrderedTupleError> {
        contract.validate_tuple(&bound)?;
        Ok(Self::new(contract.digest(), bound))
    }
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    pub const fn bound(&self) -> &OrderedTuple {
        &self.bound
    }
    pub const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopKSummary {
    contract_digest: [u8; 32],
    candidates: Vec<OrderedTuple>,
    replay_digest: [u8; 32],
}
impl TopKSummary {
    pub fn new(
        contract_digest: [u8; 32],
        candidates: impl IntoIterator<Item = OrderedTuple>,
    ) -> Self {
        let candidates = candidates.into_iter().collect::<Vec<_>>();
        Self {
            contract_digest,
            replay_digest: topk_replay_digest(contract_digest, &candidates),
            candidates,
        }
    }
    pub fn try_new(
        contract: &RuntimeTopKSummaryContract,
        candidates: impl IntoIterator<Item = OrderedTuple>,
    ) -> Result<Self, TopKSummaryError> {
        let candidates = candidates.into_iter().collect::<Vec<_>>();
        if candidates.len() > contract.k() as usize {
            return Err(TopKSummaryError::TooManyCandidates);
        }
        for candidate in &candidates {
            contract
                .order()
                .validate_tuple(candidate)
                .map_err(TopKSummaryError::CandidateContractMismatch)?;
        }
        for pair in candidates.windows(2) {
            if contract
                .order()
                .compare(&pair[0], &pair[1])
                .map_err(TopKSummaryError::CandidateContractMismatch)?
                == Ordering::Greater
            {
                return Err(TopKSummaryError::NonCanonicalCandidates);
            }
        }
        let digest = topk_replay_digest(contract.digest(), &candidates);
        Ok(Self {
            contract_digest: contract.digest(),
            candidates,
            replay_digest: digest,
        })
    }
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    pub fn candidates(&self) -> &[OrderedTuple] {
        &self.candidates
    }
    pub const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalDomainShard {
    fence_digest: [u8; 32],
    domain: ValueDomainDelta,
    replay_digest: [u8; 32],
}
impl FinalDomainShard {
    pub fn new(fence_digest: [u8; 32], domain: ValueDomainDelta) -> Self {
        Self {
            fence_digest,
            replay_digest: final_domain_replay_digest(fence_digest, &domain),
            domain,
        }
    }
    pub const fn fence_digest(&self) -> [u8; 32] {
        self.fence_digest
    }
    pub const fn domain(&self) -> &ValueDomainDelta {
        &self.domain
    }
    pub const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContribution {
    Membership(ValueDomainDelta),
    OrderedBound(OrderedBoundUpdate),
    TopKSummary(TopKSummary),
    FinalDomain(FinalDomainShard),
}
impl RuntimeFilterContribution {
    pub const fn membership(delta: ValueDomainDelta) -> Self {
        Self::Membership(delta)
    }
    pub const fn ordered_bound(update: OrderedBoundUpdate) -> Self {
        Self::OrderedBound(update)
    }
    pub const fn top_k_summary(summary: TopKSummary) -> Self {
        Self::TopKSummary(summary)
    }
    pub const fn final_domain(shard: FinalDomainShard) -> Self {
        Self::FinalDomain(shard)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeOrderKey {
    data_type: DataType,
    direction: RuntimeOrderSortDirection,
    null_order: RuntimeOrderNullOrder,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeOrderSortDirection {
    Ascending,
    Descending,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeOrderNullOrder {
    First,
    Last,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderedTupleError {
    ArityMismatch,
    TypeMismatch,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopKSummaryError {
    TooManyCandidates,
    CandidateContractMismatch(OrderedTupleError),
    NonCanonicalCandidates,
}
impl RuntimeOrderKey {
    pub const fn new(data_type: DataType) -> Self {
        Self::with_order(
            data_type,
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        )
    }
    pub const fn with_order(
        data_type: DataType,
        direction: RuntimeOrderSortDirection,
        null_order: RuntimeOrderNullOrder,
    ) -> Self {
        Self {
            data_type,
            direction,
            null_order,
        }
    }
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }
    pub const fn direction(&self) -> RuntimeOrderSortDirection {
        self.direction
    }
    pub const fn null_order(&self) -> RuntimeOrderNullOrder {
        self.null_order
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeOrderContract {
    keys: Vec<RuntimeOrderKey>,
    comparator_digest: [u8; 32],
    digest: [u8; 32],
}
impl RuntimeOrderContract {
    pub fn new(keys: impl IntoIterator<Item = RuntimeOrderKey>, digest: [u8; 32]) -> Self {
        Self::from_frozen(keys, digest, digest)
    }
    /// Construct from the comparator and order digests frozen by the fragment
    /// contract. The codec never recomputes either digest from mutable plan
    /// state.
    pub fn from_frozen(
        keys: impl IntoIterator<Item = RuntimeOrderKey>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    ) -> Self {
        Self {
            keys: keys.into_iter().collect(),
            comparator_digest,
            digest: order_contract_digest,
        }
    }
    pub fn keys(&self) -> &[RuntimeOrderKey] {
        &self.keys
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub const fn comparator_digest(&self) -> [u8; 32] {
        self.comparator_digest
    }
    pub fn validate_tuple(&self, tuple: &OrderedTuple) -> Result<(), OrderedTupleError> {
        if tuple.values().len() != self.keys.len() {
            return Err(OrderedTupleError::ArityMismatch);
        }
        if self.keys.iter().zip(tuple.values()).any(|(key, value)| {
            value
                .as_ref()
                .is_some_and(|value| !scalar_matches(value, key.data_type()))
        }) {
            return Err(OrderedTupleError::TypeMismatch);
        }
        Ok(())
    }
    pub fn compare(
        &self,
        left: &OrderedTuple,
        right: &OrderedTuple,
    ) -> Result<Ordering, OrderedTupleError> {
        self.validate_tuple(left)?;
        self.validate_tuple(right)?;
        for ((key, left), right) in self.keys.iter().zip(left.values()).zip(right.values()) {
            let ordering = match (left, right) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => match key.null_order() {
                    RuntimeOrderNullOrder::First => Ordering::Less,
                    RuntimeOrderNullOrder::Last => Ordering::Greater,
                },
                (Some(_), None) => match key.null_order() {
                    RuntimeOrderNullOrder::First => Ordering::Greater,
                    RuntimeOrderNullOrder::Last => Ordering::Less,
                },
                (Some(left), Some(right)) => {
                    let ordering = compare_scalar(left, right)?;
                    match key.direction() {
                        RuntimeOrderSortDirection::Ascending => ordering,
                        RuntimeOrderSortDirection::Descending => ordering.reverse(),
                    }
                }
            };
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }
        Ok(Ordering::Equal)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeTopKSummaryContract {
    order: RuntimeOrderContract,
    k: u32,
    digest: [u8; 32],
}
impl RuntimeTopKSummaryContract {
    pub const fn new(order: RuntimeOrderContract, k: u32, digest: [u8; 32]) -> Self {
        Self { order, k, digest }
    }
    pub const fn order(&self) -> &RuntimeOrderContract {
        &self.order
    }
    pub const fn k(&self) -> u32 {
        self.k
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ContributionCodecExpectation<'a> {
    Membership {
        data_type: &'a DataType,
        digest: [u8; 32],
    },
    OrderedBound(&'a RuntimeOrderContract),
    TopKSummary(&'a RuntimeTopKSummaryContract),
    FinalDomain {
        data_type: &'a DataType,
        digest: [u8; 32],
    },
}
impl<'a> ContributionCodecExpectation<'a> {
    pub const fn membership(data_type: &'a DataType, digest: [u8; 32]) -> Self {
        Self::Membership { data_type, digest }
    }
    pub const fn final_domain(data_type: &'a DataType, digest: [u8; 32]) -> Self {
        Self::FinalDomain { data_type, digest }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedContribution {
    schema_digest: [u8; 32],
    payload: Vec<u8>,
}
impl EncodedContribution {
    pub const fn schema_digest(&self) -> &[u8; 32] {
        &self.schema_digest
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn into_parts(self) -> ([u8; 32], Vec<u8>) {
        (self.schema_digest, self.payload)
    }
}

pub fn encode_contribution(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
) -> Result<EncodedContribution, ContributionCodecError> {
    let (kind, digest, body) = encode_body(contribution, expectation)?;
    let body_len = u64::try_from(body.len()).map_err(|_| ContributionCodecError::LengthOverflow)?;
    let total = HEADER_LEN
        .checked_add(body.len())
        .ok_or(ContributionCodecError::LengthOverflow)?;
    if total > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(total)
        .map_err(|_| ContributionCodecError::ResourceLimit)?;
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&CODEC_VERSION.to_be_bytes());
    payload.extend_from_slice(&[kind, 0]);
    payload.extend_from_slice(&digest);
    payload.extend_from_slice(&body_len.to_be_bytes());
    payload.extend_from_slice(&body);
    Ok(EncodedContribution {
        schema_digest: digest,
        payload,
    })
}

pub fn max_encoded_len_for_contribution_budget(
    max_contribution_bytes: usize,
) -> Result<usize, ContributionCodecError> {
    HEADER_LEN
        .checked_add(max_contribution_bytes)
        .ok_or(ContributionCodecError::LengthOverflow)
}

pub fn semantic_contribution_bytes(
    contribution: &RuntimeFilterContribution,
) -> Result<usize, ContributionCodecError> {
    let mut body = Vec::new();
    match contribution {
        RuntimeFilterContribution::Membership(domain) => domain.encode_canonical_into(&mut body)?,
        RuntimeFilterContribution::OrderedBound(update) => {
            visit_tuple(update.bound(), |part| body.extend_from_slice(part));
        }
        RuntimeFilterContribution::TopKSummary(summary) => {
            push_count(&mut body, summary.candidates().len())?;
            for candidate in summary.candidates() {
                visit_tuple(candidate, |part| body.extend_from_slice(part));
            }
        }
        RuntimeFilterContribution::FinalDomain(shard) => {
            body.extend_from_slice(&shard.fence_digest());
            shard.domain().encode_canonical_into(&mut body)?;
        }
    }
    Ok(body.len())
}

pub fn encoded_contribution_len(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<usize, ContributionCodecError> {
    let (_, _, body) = encode_body(contribution, expectation)?;
    max_encoded_len_for_contribution_budget(body.len())
}

pub fn validate_contribution_contract(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<(), ContributionCodecError> {
    encode_body(contribution, expectation).map(|_| ())
}

pub fn decode_contribution(
    payload: &[u8],
    envelope_digest: &[u8; 32],
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
) -> Result<RuntimeFilterContribution, ContributionCodecError> {
    if payload.len() > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }
    let mut r = Reader::new(payload);
    if r.take(4)? != MAGIC {
        return Err(ContributionCodecError::Malformed);
    }
    if r.u16()? != CODEC_VERSION {
        return Err(ContributionCodecError::UnknownVersion);
    }
    let kind = r.u8()?;
    if !(1..=4).contains(&kind) {
        return Err(ContributionCodecError::UnknownKind);
    }
    if r.u8()? != 0 {
        return Err(ContributionCodecError::InvalidFlags);
    }
    let digest = r.array::<32>()?;
    let body_len = usize::try_from(r.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if body_len < r.len() {
        return Err(ContributionCodecError::TrailingBytes);
    }
    if body_len > r.len() {
        return Err(ContributionCodecError::Truncated);
    }
    let body = r.take(body_len)?;
    if kind != expectation_kind(expectation) {
        return Err(ContributionCodecError::KindMismatch);
    }
    if digest != *envelope_digest || digest != expectation_digest(expectation) {
        return Err(ContributionCodecError::SchemaMismatch);
    }
    let decoded = match expectation {
        ContributionCodecExpectation::Membership { data_type, .. } => {
            RuntimeFilterContribution::Membership(decode_membership(body, data_type)?)
        }
        ContributionCodecExpectation::OrderedBound(contract) => {
            RuntimeFilterContribution::OrderedBound(OrderedBoundUpdate::new(
                contract.digest(),
                decode_tuple(body, contract)?,
            ))
        }
        ContributionCodecExpectation::TopKSummary(contract) => {
            let mut body = Reader::new(body);
            let count =
                usize::try_from(body.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
            if count > contract.k() as usize {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            let mut candidates = Vec::new();
            candidates
                .try_reserve_exact(count)
                .map_err(|_| ContributionCodecError::ResourceLimit)?;
            for _ in 0..count {
                candidates.push(decode_tuple_reader(&mut body, contract.order())?);
            }
            if !body.empty() {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            RuntimeFilterContribution::TopKSummary(TopKSummary::new(contract.digest(), candidates))
        }
        ContributionCodecExpectation::FinalDomain { data_type, .. } => {
            let mut body = Reader::new(body);
            let fence = body.array::<32>()?;
            RuntimeFilterContribution::FinalDomain(FinalDomainShard::new(
                fence,
                decode_membership(body.take(body.len())?, data_type)?,
            ))
        }
    };
    let canonical = encode_contribution(&decoded, expectation, payload.len())?;
    if canonical.payload != payload {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(decoded)
}

fn encode_body(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<(u8, [u8; 32], Vec<u8>), ContributionCodecError> {
    let mut body = Vec::new();
    let (kind, digest) = match (contribution, expectation) {
        (
            RuntimeFilterContribution::Membership(domain),
            ContributionCodecExpectation::Membership { data_type, digest },
        ) => {
            if !domain.matches_data_type(data_type) {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            domain.encode_canonical_into(&mut body)?;
            (1, digest)
        }
        (
            RuntimeFilterContribution::OrderedBound(update),
            ContributionCodecExpectation::OrderedBound(contract),
        ) => {
            if update.contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            encode_tuple(&update.bound, contract, &mut body)?;
            (2, contract.digest())
        }
        (
            RuntimeFilterContribution::TopKSummary(summary),
            ContributionCodecExpectation::TopKSummary(contract),
        ) => {
            if summary.contract_digest() != contract.digest()
                || summary.candidates().len() > contract.k() as usize
            {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            push_count(&mut body, summary.candidates().len())?;
            for candidate in summary.candidates() {
                encode_tuple(candidate, contract.order(), &mut body)?;
            }
            (3, contract.digest())
        }
        (
            RuntimeFilterContribution::FinalDomain(shard),
            ContributionCodecExpectation::FinalDomain { data_type, digest },
        ) => {
            if !shard.domain().matches_data_type(data_type) {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            body.extend_from_slice(&shard.fence_digest());
            shard.domain().encode_canonical_into(&mut body)?;
            (4, digest)
        }
        _ => return Err(ContributionCodecError::KindMismatch),
    };
    Ok((kind, digest, body))
}
fn expectation_kind(expectation: ContributionCodecExpectation<'_>) -> u8 {
    match expectation {
        ContributionCodecExpectation::Membership { .. } => 1,
        ContributionCodecExpectation::OrderedBound(_) => 2,
        ContributionCodecExpectation::TopKSummary(_) => 3,
        ContributionCodecExpectation::FinalDomain { .. } => 4,
    }
}
fn expectation_digest(expectation: ContributionCodecExpectation<'_>) -> [u8; 32] {
    match expectation {
        ContributionCodecExpectation::Membership { digest, .. }
        | ContributionCodecExpectation::FinalDomain { digest, .. } => digest,
        ContributionCodecExpectation::OrderedBound(contract) => contract.digest(),
        ContributionCodecExpectation::TopKSummary(contract) => contract.digest(),
    }
}

fn encode_tuple(
    tuple: &OrderedTuple,
    contract: &RuntimeOrderContract,
    out: &mut Vec<u8>,
) -> Result<(), ContributionCodecError> {
    if tuple.values().len() != contract.keys().len() {
        return Err(ContributionCodecError::SchemaMismatch);
    }
    push_count(out, tuple.values().len())?;
    for (value, key) in tuple.values().iter().zip(contract.keys()) {
        match value {
            None => out.push(0),
            Some(value) => {
                if !scalar_matches(value, key.data_type()) {
                    return Err(ContributionCodecError::SchemaMismatch);
                }
                out.push(1);
                encode_scalar(value, out)?;
            }
        }
    }
    Ok(())
}
fn encode_scalar(value: &OrderedScalar, out: &mut Vec<u8>) -> Result<(), ContributionCodecError> {
    match value {
        OrderedScalar::Boolean(x) => out.push(u8::from(*x)),
        OrderedScalar::Int8(x) => out.extend_from_slice(&x.to_be_bytes()),
        OrderedScalar::Int16(x) => out.extend_from_slice(&x.to_be_bytes()),
        OrderedScalar::Int32(x) | OrderedScalar::Date32(x) => {
            out.extend_from_slice(&x.to_be_bytes())
        }
        OrderedScalar::Int64(x) | OrderedScalar::Timestamp(x) => {
            out.extend_from_slice(&x.to_be_bytes())
        }
        OrderedScalar::LargeInt(x) | OrderedScalar::Decimal128(x) => {
            out.extend_from_slice(&x.to_be_bytes())
        }
        OrderedScalar::Utf8(x) => push_bytes(out, x.as_bytes())?,
    };
    Ok(())
}

fn decode_membership(
    bytes: &[u8],
    expected: &DataType,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    let mut r = Reader::new(bytes);
    let version_len =
        usize::try_from(r.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if r.take(version_len)? != FINGERPRINT_VERSION_TAG {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let tag = r.u8()?;
    let values = match expected {
        DataType::Boolean => {
            MembershipValues::Boolean(read_set(&mut r, tag, 1, |r| match r.u8()? {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(ContributionCodecError::NonCanonicalPayload),
            })?)
        }
        DataType::Int8 => MembershipValues::Int8(read_set(&mut r, tag, 2, |r| r.i8())?),
        DataType::Int16 => MembershipValues::Int16(read_set(&mut r, tag, 3, |r| r.i16())?),
        DataType::Int32 => MembershipValues::Int32(read_set(&mut r, tag, 4, |r| r.i32())?),
        DataType::Int64 => MembershipValues::Int64(read_set(&mut r, tag, 5, |r| r.i64())?),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            MembershipValues::LargeInt(read_set(&mut r, tag, 6, |r| r.i128())?)
        }
        DataType::Float32 => MembershipValues::Float32(read_set(&mut r, tag, 7, |r| {
            let bits = r.u32()?;
            let x = f32::from_bits(bits);
            if (x == 0.0 && bits != 0) || (x.is_nan() && bits != 0x7fc0_0000) {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            Ok(CanonicalF32(bits))
        })?),
        DataType::Float64 => MembershipValues::Float64(read_set(&mut r, tag, 8, |r| {
            let bits = r.u64()?;
            let x = f64::from_bits(bits);
            if (x == 0.0 && bits != 0) || (x.is_nan() && bits != 0x7ff8_0000_0000_0000) {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            Ok(CanonicalF64(bits))
        })?),
        DataType::Utf8 => {
            expect_tag(tag, 9)?;
            let count = r.count()?;
            let mut values = BTreeSet::new();
            for _ in 0..count {
                let len = usize::try_from(r.u64()?)
                    .map_err(|_| ContributionCodecError::LengthOverflow)?;
                let value = std::str::from_utf8(r.take(len)?)
                    .map_err(|_| ContributionCodecError::NonCanonicalPayload)?
                    .to_owned();
                if !values.insert(value) {
                    return Err(ContributionCodecError::NonCanonicalPayload);
                }
            }
            MembershipValues::Utf8(values)
        }
        DataType::Date32 => MembershipValues::Date32(read_set(&mut r, tag, 10, |r| r.i32())?),
        DataType::Timestamp(unit, timezone) => {
            expect_tag(tag, 11)?;
            if r.u8()? != time_unit_tag(unit) {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            let actual_timezone = match r.u8()? {
                0 => None,
                1 => {
                    let len = usize::try_from(r.u64()?)
                        .map_err(|_| ContributionCodecError::LengthOverflow)?;
                    Some(
                        std::str::from_utf8(r.take(len)?)
                            .map_err(|_| ContributionCodecError::NonCanonicalPayload)?
                            .to_owned(),
                    )
                }
                _ => return Err(ContributionCodecError::NonCanonicalPayload),
            };
            if actual_timezone.as_deref() != timezone.as_deref() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            MembershipValues::Timestamp {
                unit: unit.clone(),
                timezone: actual_timezone,
                values: read_set_count(&mut r, |r| r.i64())?,
            }
        }
        DataType::Decimal128(precision, scale) => {
            expect_tag(tag, 12)?;
            if r.u8()? != *precision || r.u8()? as i8 != *scale {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            let values = read_set_count(&mut r, |r| r.i128())?;
            validate_decimal(*precision, *scale, &values)?;
            MembershipValues::Decimal128 {
                precision: *precision,
                scale: *scale,
                values,
            }
        }
        _ => return Err(ContributionCodecError::SchemaMismatch),
    };
    let contains_null = match r.u8()? {
        0 => false,
        1 => true,
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    };
    if !r.empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(ValueDomainDelta::new(values, contains_null))
}

fn decode_tuple(
    bytes: &[u8],
    contract: &RuntimeOrderContract,
) -> Result<OrderedTuple, ContributionCodecError> {
    let mut reader = Reader::new(bytes);
    let tuple = decode_tuple_reader(&mut reader, contract)?;
    if !reader.empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(tuple)
}
fn decode_tuple_reader(
    reader: &mut Reader<'_>,
    contract: &RuntimeOrderContract,
) -> Result<OrderedTuple, ContributionCodecError> {
    let arity = reader.count()?;
    if arity != contract.keys().len() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(arity)
        .map_err(|_| ContributionCodecError::ResourceLimit)?;
    for key in contract.keys() {
        values.push(match reader.u8()? {
            0 => None,
            1 => Some(decode_scalar(reader, key.data_type())?),
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        });
    }
    Ok(OrderedTuple::new(values))
}
fn decode_scalar(
    r: &mut Reader<'_>,
    data_type: &DataType,
) -> Result<OrderedScalar, ContributionCodecError> {
    Ok(match data_type {
        DataType::Boolean => OrderedScalar::Boolean(match r.u8()? {
            0 => false,
            1 => true,
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        }),
        DataType::Int8 => OrderedScalar::Int8(r.i8()?),
        DataType::Int16 => OrderedScalar::Int16(r.i16()?),
        DataType::Int32 => OrderedScalar::Int32(r.i32()?),
        DataType::Int64 => OrderedScalar::Int64(r.i64()?),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            OrderedScalar::LargeInt(r.i128()?)
        }
        DataType::Utf8 => {
            let len =
                usize::try_from(r.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
            OrderedScalar::Utf8(
                std::str::from_utf8(r.take(len)?)
                    .map_err(|_| ContributionCodecError::NonCanonicalPayload)?
                    .to_owned(),
            )
        }
        DataType::Date32 => OrderedScalar::Date32(r.i32()?),
        DataType::Timestamp(_, _) => OrderedScalar::Timestamp(r.i64()?),
        DataType::Decimal128(_, _) => OrderedScalar::Decimal128(r.i128()?),
        _ => return Err(ContributionCodecError::SchemaMismatch),
    })
}
fn scalar_matches(value: &OrderedScalar, data_type: &DataType) -> bool {
    match (value, data_type) {
        (OrderedScalar::Boolean(_), DataType::Boolean)
        | (OrderedScalar::Int8(_), DataType::Int8)
        | (OrderedScalar::Int16(_), DataType::Int16)
        | (OrderedScalar::Int32(_), DataType::Int32)
        | (OrderedScalar::Int64(_), DataType::Int64)
        | (OrderedScalar::Utf8(_), DataType::Utf8)
        | (OrderedScalar::Date32(_), DataType::Date32)
        | (OrderedScalar::Timestamp(_), DataType::Timestamp(_, _)) => true,
        (OrderedScalar::Decimal128(value), DataType::Decimal128(precision, _)) => 10_i128
            .checked_pow((*precision).into())
            .is_some_and(|limit| *value > -limit && *value < limit),
        (OrderedScalar::LargeInt(_), DataType::FixedSizeBinary(width)) => {
            *width == LARGEINT_BYTE_WIDTH
        }
        _ => false,
    }
}

fn read_set<T: Ord>(
    r: &mut Reader<'_>,
    tag: u8,
    expected_tag: u8,
    mut read: impl FnMut(&mut Reader<'_>) -> Result<T, ContributionCodecError>,
) -> Result<BTreeSet<T>, ContributionCodecError> {
    expect_tag(tag, expected_tag)?;
    read_set_count(r, |r| read(r))
}
fn read_set_count<T: Ord>(
    r: &mut Reader<'_>,
    mut read: impl FnMut(&mut Reader<'_>) -> Result<T, ContributionCodecError>,
) -> Result<BTreeSet<T>, ContributionCodecError> {
    let count = r.count()?;
    let mut values = BTreeSet::new();
    for _ in 0..count {
        let value = read(r)?;
        if !values.insert(value) {
            return Err(ContributionCodecError::NonCanonicalPayload);
        }
    }
    Ok(values)
}
fn expect_tag(actual: u8, expected: u8) -> Result<(), ContributionCodecError> {
    if actual == expected {
        Ok(())
    } else if (1..=12).contains(&actual) {
        Err(ContributionCodecError::SchemaMismatch)
    } else {
        Err(ContributionCodecError::NonCanonicalPayload)
    }
}
fn validate_decimal(
    precision: u8,
    scale: i8,
    values: &BTreeSet<i128>,
) -> Result<(), ContributionCodecError> {
    if !(1..=38).contains(&precision) || scale > 38 || (scale > 0 && scale as u8 > precision) {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let bound = 10_i128
        .checked_pow(precision.into())
        .ok_or(ContributionCodecError::LengthOverflow)?;
    if values
        .iter()
        .any(|value| *value <= -bound || *value >= bound)
    {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(())
}
fn time_unit_tag(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }
}
fn ordered_replay_digest(contract_digest: [u8; 32], tuple: &OrderedTuple) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.runtime-filter.ordered-bound-replay");
    digest.update(1_u16.to_be_bytes());
    digest.update(contract_digest);
    visit_tuple(tuple, |part| digest.update(part));
    digest.finalize().into()
}
fn topk_replay_digest(contract_digest: [u8; 32], candidates: &[OrderedTuple]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.runtime-filter.top-k-summary-replay");
    digest.update(1_u16.to_be_bytes());
    digest.update(contract_digest);
    digest.update((candidates.len() as u64).to_be_bytes());
    for candidate in candidates {
        visit_tuple(candidate, |part| digest.update(part));
    }
    digest.finalize().into()
}
fn final_domain_replay_digest(fence_digest: [u8; 32], domain: &ValueDomainDelta) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.runtime-filter.final-domain-shard");
    digest.update(1_u16.to_be_bytes());
    digest.update(32_u64.to_be_bytes());
    digest.update(fence_digest);
    digest.update(32_u64.to_be_bytes());
    digest.update(domain.fingerprint());
    digest.finalize().into()
}
fn visit_tuple(tuple: &OrderedTuple, mut visit: impl FnMut(&[u8])) {
    visit(&(tuple.values().len() as u64).to_be_bytes());
    for value in tuple.values() {
        match value {
            None => visit(&[0]),
            Some(value) => {
                visit(&[1]);
                visit_scalar(value, &mut visit);
            }
        }
    }
}
fn visit_scalar(value: &OrderedScalar, visit: &mut impl FnMut(&[u8])) {
    match value {
        OrderedScalar::Boolean(value) => visit(&[u8::from(*value)]),
        OrderedScalar::Int8(value) => visit(&value.to_be_bytes()),
        OrderedScalar::Int16(value) => visit(&value.to_be_bytes()),
        OrderedScalar::Int32(value) | OrderedScalar::Date32(value) => visit(&value.to_be_bytes()),
        OrderedScalar::Int64(value) | OrderedScalar::Timestamp(value) => {
            visit(&value.to_be_bytes())
        }
        OrderedScalar::LargeInt(value) | OrderedScalar::Decimal128(value) => {
            visit(&value.to_be_bytes())
        }
        OrderedScalar::Utf8(value) => {
            visit(&(value.len() as u64).to_be_bytes());
            visit(value.as_bytes());
        }
    }
}
fn compare_scalar(
    left: &OrderedScalar,
    right: &OrderedScalar,
) -> Result<Ordering, OrderedTupleError> {
    Ok(match (left, right) {
        (OrderedScalar::Boolean(a), OrderedScalar::Boolean(b)) => a.cmp(b),
        (OrderedScalar::Int8(a), OrderedScalar::Int8(b)) => a.cmp(b),
        (OrderedScalar::Int16(a), OrderedScalar::Int16(b)) => a.cmp(b),
        (OrderedScalar::Int32(a), OrderedScalar::Int32(b)) => a.cmp(b),
        (OrderedScalar::Int64(a), OrderedScalar::Int64(b)) => a.cmp(b),
        (OrderedScalar::LargeInt(a), OrderedScalar::LargeInt(b)) => a.cmp(b),
        (OrderedScalar::Utf8(a), OrderedScalar::Utf8(b)) => a.as_bytes().cmp(b.as_bytes()),
        (OrderedScalar::Date32(a), OrderedScalar::Date32(b)) => a.cmp(b),
        (OrderedScalar::Timestamp(a), OrderedScalar::Timestamp(b)) => a.cmp(b),
        (OrderedScalar::Decimal128(a), OrderedScalar::Decimal128(b)) => a.cmp(b),
        _ => return Err(OrderedTupleError::TypeMismatch),
    })
}
fn push_count(out: &mut Vec<u8>, count: usize) -> Result<(), ContributionSizeError> {
    out.extend_from_slice(
        &u64::try_from(count)
            .map_err(|_| ContributionSizeError::LengthExceedsCanonicalRange)?
            .to_be_bytes(),
    );
    Ok(())
}
fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ContributionSizeError> {
    push_count(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
}
impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    const fn len(&self) -> usize {
        self.bytes.len()
    }
    const fn empty(&self) -> bool {
        self.bytes.is_empty()
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], ContributionCodecError> {
        let (value, remaining) = self
            .bytes
            .split_at_checked(n)
            .ok_or(ContributionCodecError::Truncated)?;
        self.bytes = remaining;
        Ok(value)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ContributionCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ContributionCodecError::Truncated)
    }
    fn u8(&mut self) -> Result<u8, ContributionCodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ContributionCodecError> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, ContributionCodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, ContributionCodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn i8(&mut self) -> Result<i8, ContributionCodecError> {
        Ok(i8::from_be_bytes(self.array()?))
    }
    fn i16(&mut self) -> Result<i16, ContributionCodecError> {
        Ok(i16::from_be_bytes(self.array()?))
    }
    fn i32(&mut self) -> Result<i32, ContributionCodecError> {
        Ok(i32::from_be_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64, ContributionCodecError> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    fn i128(&mut self) -> Result<i128, ContributionCodecError> {
        Ok(i128::from_be_bytes(self.array()?))
    }
    fn count(&mut self) -> Result<usize, ContributionCodecError> {
        usize::try_from(self.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn membership_frame_uses_the_v1_nrfc_fixture_bytes() {
        let contribution = RuntimeFilterContribution::membership(ValueDomainDelta::new(
            MembershipValues::int32([2, 1, 2]),
            true,
        ));
        let digest = [0xA5; 32];

        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::membership(&DataType::Int32, digest),
            usize::MAX,
        )
        .expect("the canonical membership contribution encodes");

        let expected = [
            b'N', b'R', b'F', b'C', 0, 1, 1, 0, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0, 0, 0, 0, 0, 0, 0, 72, 0,
            0, 0, 0, 0, 0, 0, 46, b'n', b'o', b'v', b'a', b'r', b'o', b'c', b'k', b's', b'.', b'r',
            b'u', b'n', b't', b'i', b'm', b'e', b'-', b'f', b'i', b'l', b't', b'e', b'r', b'.',
            b'v', b'a', b'l', b'u', b'e', b'-', b'd', b'o', b'm', b'a', b'i', b'n', b'-', b'd',
            b'e', b'l', b't', b'a', b'.', b'v', b'1', 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0,
            0, 2, 1,
        ];
        assert_eq!(encoded.payload(), expected);
    }

    #[test]
    fn contribution_round_trips_each_canonical_payload_kind() {
        let membership_type = DataType::Int64;
        let membership = RuntimeFilterContribution::membership(ValueDomainDelta::new(
            MembershipValues::int64([9, 3]),
            false,
        ));
        let membership_expectation =
            ContributionCodecExpectation::membership(&membership_type, [1; 32]);
        assert_round_trip(membership, membership_expectation, [1; 32]);

        let order = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Int32)], [2; 32]);
        let ordered = RuntimeFilterContribution::ordered_bound(OrderedBoundUpdate::new(
            [2; 32],
            OrderedTuple::new([Some(OrderedScalar::Int32(7))]),
        ));
        assert_round_trip(
            ordered,
            ContributionCodecExpectation::OrderedBound(&order),
            [2; 32],
        );

        let topk_contract = RuntimeTopKSummaryContract::new(order.clone(), 2, [3; 32]);
        let topk = RuntimeFilterContribution::top_k_summary(TopKSummary::new(
            [3; 32],
            [OrderedTuple::new([Some(OrderedScalar::Int32(7))])],
        ));
        assert_round_trip(
            topk,
            ContributionCodecExpectation::TopKSummary(&topk_contract),
            [3; 32],
        );

        let final_domain = RuntimeFilterContribution::final_domain(FinalDomainShard::new(
            [4; 32],
            ValueDomainDelta::new(MembershipValues::int64([5]), true),
        ));
        assert_round_trip(
            final_domain,
            ContributionCodecExpectation::final_domain(&membership_type, [4; 32]),
            [4; 32],
        );
    }

    #[test]
    fn decoder_rejects_a_trailing_or_noncanonical_membership_frame() {
        let data_type = DataType::Int32;
        let contribution = RuntimeFilterContribution::membership(ValueDomainDelta::new(
            MembershipValues::int32([1, 2]),
            false,
        ));
        let expectation = ContributionCodecExpectation::membership(&data_type, [7; 32]);
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();

        let mut trailing = encoded.payload().to_vec();
        trailing.push(0);
        assert_eq!(
            decode_contribution(&trailing, &[7; 32], expectation, usize::MAX),
            Err(ContributionCodecError::TrailingBytes),
        );

        let mut duplicate = encoded.payload().to_vec();
        let body_len_offset = 40;
        let body_len = u64::from_be_bytes(duplicate[body_len_offset..48].try_into().unwrap());
        let first_value_offset = 48 + 8 + FINGERPRINT_VERSION_TAG.len() + 1 + 8;
        duplicate.splice(first_value_offset + 4..first_value_offset + 4, [0, 0, 0, 2]);
        duplicate[body_len_offset..48].copy_from_slice(&(body_len + 4).to_be_bytes());
        assert_eq!(
            decode_contribution(&duplicate, &[7; 32], expectation, usize::MAX),
            Err(ContributionCodecError::NonCanonicalPayload),
        );
    }

    #[test]
    fn ordered_topk_and_final_domain_use_the_v1_body_fixtures() {
        let order = RuntimeOrderContract::new(
            [RuntimeOrderKey::with_order(
                DataType::Int32,
                RuntimeOrderSortDirection::Descending,
                RuntimeOrderNullOrder::First,
            )],
            [2; 32],
        );
        let tuple = OrderedTuple::new([Some(OrderedScalar::Int32(7))]);
        let ordered = encode_contribution(
            &RuntimeFilterContribution::ordered_bound(
                OrderedBoundUpdate::try_new(&order, tuple.clone()).unwrap(),
            ),
            ContributionCodecExpectation::OrderedBound(&order),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            &ordered.payload()[..8],
            &[b'N', b'R', b'F', b'C', 0, 1, 2, 0]
        );
        assert_eq!(
            &ordered.payload()[48..],
            &[0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 7]
        );

        let topk_contract = RuntimeTopKSummaryContract::new(order.clone(), 2, [3; 32]);
        let topk = encode_contribution(
            &RuntimeFilterContribution::top_k_summary(
                TopKSummary::try_new(&topk_contract, [tuple]).unwrap(),
            ),
            ContributionCodecExpectation::TopKSummary(&topk_contract),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(&topk.payload()[..8], &[b'N', b'R', b'F', b'C', 0, 1, 3, 0]);
        assert_eq!(
            &topk.payload()[48..],
            &[
                0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 7
            ]
        );

        let membership_type = DataType::Int64;
        let final_domain = encode_contribution(
            &RuntimeFilterContribution::final_domain(FinalDomainShard::new(
                [9; 32],
                ValueDomainDelta::new(MembershipValues::int64([11, 13]), false),
            )),
            ContributionCodecExpectation::final_domain(&membership_type, [4; 32]),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            &final_domain.payload()[..8],
            &[b'N', b'R', b'F', b'C', 0, 1, 4, 0]
        );
        assert_eq!(&final_domain.payload()[48..80], &[9; 32]);
        assert_eq!(
            &final_domain.payload()[80..],
            &[
                0, 0, 0, 0, 0, 0, 0, 46, b'n', b'o', b'v', b'a', b'r', b'o', b'c', b'k', b's',
                b'.', b'r', b'u', b'n', b't', b'i', b'm', b'e', b'-', b'f', b'i', b'l', b't', b'e',
                b'r', b'.', b'v', b'a', b'l', b'u', b'e', b'-', b'd', b'o', b'm', b'a', b'i', b'n',
                b'-', b'd', b'e', b'l', b't', b'a', b'.', b'v', b'1', 5, 0, 0, 0, 0, 0, 0, 0, 2, 0,
                0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 13, 0,
            ][..]
        );
    }

    fn assert_round_trip(
        contribution: RuntimeFilterContribution,
        expectation: ContributionCodecExpectation<'_>,
        digest: [u8; 32],
    ) {
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();
        assert_eq!(
            decode_contribution(encoded.payload(), &digest, expectation, usize::MAX).unwrap(),
            contribution
        );
    }
}
