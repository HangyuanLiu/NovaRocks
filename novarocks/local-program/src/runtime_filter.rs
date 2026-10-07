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

//! Closed, immutable runtime-filter declarations. The session and its mutable
//! artifacts are bound separately when a task instance is installed.
//!
//! This module is also the single owner of the canonical membership artifact
//! schema: its bytes and SHA-256 digest are what a static membership contract
//! carries, and Execution derives its runtime schema from the same encoding.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, TimeUnit};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterSortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterNullOrder {
    First,
    Last,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilterOrderKey {
    pub data_type: DataType,
    pub direction: FilterSortDirection,
    pub null_order: FilterNullOrder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StaticFilterContract {
    Membership {
        data_type: DataType,
        null_semantics: FilterNullSemantics,
        digest: [u8; 32],
    },
    Ordered {
        keys: Arc<[FilterOrderKey]>,
        comparator_digest: [u8; 32],
        contract_digest: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterReduction {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: NonZeroU32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterProducerKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterLateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive {
        late_apply: FilterLateApplyGranularity,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticFilterProducer {
    binding_id: u32,
    channel_id: u32,
    kind: FilterProducerKind,
    contract: StaticFilterContract,
    reduction: FilterReduction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticFilterConsumer {
    binding_id: u32,
    channel_id: u32,
    activation: FilterConsumerActivation,
    contract: StaticFilterContract,
    reduction: FilterReduction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticFilterError {
    EmptyOrderedKeys,
    KindContractMismatch,
    ReductionContractMismatch,
    BlockingOrderedConsumer,
}

impl fmt::Display for StaticFilterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid static runtime filter: {self:?}")
    }
}

impl std::error::Error for StaticFilterError {}

impl StaticFilterContract {
    /// The membership contract over `data_type` keys, digested by the
    /// canonical membership schema. This is the only way a compiler obtains
    /// the digest a runtime consumer and producer verify.
    pub fn membership(
        data_type: &DataType,
        null_semantics: FilterNullSemantics,
    ) -> Result<Self, FilterMembershipSchemaError> {
        let schema = FilterMembershipSchema::try_new(data_type, null_semantics)?;
        Ok(Self::Membership {
            data_type: schema.data_type().clone(),
            null_semantics,
            digest: schema.digest(),
        })
    }

    fn validate(&self) -> Result<(), StaticFilterError> {
        if matches!(self, Self::Ordered { keys, .. } if keys.is_empty()) {
            return Err(StaticFilterError::EmptyOrderedKeys);
        }
        Ok(())
    }
}

/// Domain tag that prefixes every canonical membership schema.
const MEMBERSHIP_SCHEMA_DOMAIN: &[u8] = b"novarocks.runtime-filter.artifact-schema";
/// The only canonical membership schema version.
const MEMBERSHIP_SCHEMA_VERSION: u8 = 1;

/// Why a membership schema could not be encoded or decoded. Each variant
/// carries the exact diagnostic Execution reports for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterMembershipSchemaError {
    UnsupportedType,
    TimezoneLengthOverflow,
    DigestMismatch,
    Truncated,
    UnknownPrefix,
    InvalidNullSemantics,
    TrailingBytes,
    InvalidTimeUnit,
    TimezoneLengthDecodeOverflow,
    TimezoneNotUtf8,
    InvalidTimezoneMetadata,
    InvalidDecimal,
    UnsupportedTypeTag,
}

impl FilterMembershipSchemaError {
    pub const fn detail(self) -> &'static str {
        match self {
            Self::UnsupportedType => "unsupported membership schema type",
            Self::TimezoneLengthOverflow => "membership schema timezone length overflows u32",
            Self::DigestMismatch => "membership schema digest does not match canonical bytes",
            Self::Truncated => "truncated membership schema",
            Self::UnknownPrefix => "unknown canonical membership schema prefix",
            Self::InvalidNullSemantics => "invalid membership null semantics",
            Self::TrailingBytes => "membership schema has trailing bytes",
            Self::InvalidTimeUnit => "invalid timestamp time unit",
            Self::TimezoneLengthDecodeOverflow => "timestamp timezone length overflow",
            Self::TimezoneNotUtf8 => "timestamp timezone is not UTF-8",
            Self::InvalidTimezoneMetadata => "invalid timestamp timezone metadata",
            Self::InvalidDecimal => "invalid decimal schema metadata",
            Self::UnsupportedTypeTag => "unsupported membership schema type tag",
        }
    }
}

impl fmt::Display for FilterMembershipSchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.detail())
    }
}

impl std::error::Error for FilterMembershipSchemaError {}

/// The canonical schema of one membership artifact: its key type and NULL
/// semantics, their canonical bytes, and the SHA-256 digest of those bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilterMembershipSchema {
    data_type: DataType,
    null_semantics: FilterNullSemantics,
    canonical_bytes: Arc<[u8]>,
    digest: [u8; 32],
}

impl FilterMembershipSchema {
    pub fn try_new(
        data_type: &DataType,
        null_semantics: FilterNullSemantics,
    ) -> Result<Self, FilterMembershipSchemaError> {
        let mut canonical = Vec::with_capacity(48);
        canonical.extend_from_slice(MEMBERSHIP_SCHEMA_DOMAIN);
        canonical.push(MEMBERSHIP_SCHEMA_VERSION);
        encode_filter_key_type(data_type, &mut canonical)?;
        canonical.push(match null_semantics {
            FilterNullSemantics::NeverMatches => 1,
            FilterNullSemantics::NullSafeEqual => 2,
        });
        let digest = Sha256::digest(&canonical).into();
        Ok(Self {
            data_type: data_type.clone(),
            null_semantics,
            canonical_bytes: canonical.into(),
            digest,
        })
    }

    /// Strictly decode canonical bytes whose digest must be `expected_digest`.
    pub fn from_canonical(
        canonical_bytes: impl AsRef<[u8]>,
        expected_digest: [u8; 32],
    ) -> Result<Self, FilterMembershipSchemaError> {
        let canonical = canonical_bytes.as_ref();
        let digest: [u8; 32] = Sha256::digest(canonical).into();
        if digest != expected_digest {
            return Err(FilterMembershipSchemaError::DigestMismatch);
        }
        let mut cursor = SchemaCursor {
            remaining: canonical,
        };
        if cursor.take(MEMBERSHIP_SCHEMA_DOMAIN.len())? != MEMBERSHIP_SCHEMA_DOMAIN
            || cursor.u8()? != MEMBERSHIP_SCHEMA_VERSION
        {
            return Err(FilterMembershipSchemaError::UnknownPrefix);
        }
        let data_type = decode_filter_key_type(&mut cursor)?;
        let null_semantics = match cursor.u8()? {
            1 => FilterNullSemantics::NeverMatches,
            2 => FilterNullSemantics::NullSafeEqual,
            _ => return Err(FilterMembershipSchemaError::InvalidNullSemantics),
        };
        if !cursor.remaining.is_empty() {
            return Err(FilterMembershipSchemaError::TrailingBytes);
        }
        Ok(Self {
            data_type,
            null_semantics,
            canonical_bytes: Arc::from(canonical),
            digest,
        })
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn null_semantics(&self) -> FilterNullSemantics {
        self.null_semantics
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

struct SchemaCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> SchemaCursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], FilterMembershipSchemaError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or(FilterMembershipSchemaError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, FilterMembershipSchemaError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, FilterMembershipSchemaError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .expect("schema cursor guarantees four bytes"),
        ))
    }
}

/// Append the canonical encoding of one runtime-filter key type. Membership
/// schemas and ordered contract keys share this one type vocabulary.
pub fn encode_filter_key_type(
    data_type: &DataType,
    output: &mut Vec<u8>,
) -> Result<(), FilterMembershipSchemaError> {
    match data_type {
        DataType::Boolean => output.push(1),
        DataType::Int8 => output.push(2),
        DataType::Int16 => output.push(3),
        DataType::Int32 => output.push(4),
        DataType::Int64 => output.push(5),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => output.push(6),
        DataType::Float32 => output.push(7),
        DataType::Float64 => output.push(8),
        DataType::Utf8 => output.push(9),
        DataType::Date32 => output.push(10),
        DataType::Timestamp(unit, timezone) => {
            output.extend_from_slice(&[
                11,
                match unit {
                    TimeUnit::Second => 1,
                    TimeUnit::Millisecond => 2,
                    TimeUnit::Microsecond => 3,
                    TimeUnit::Nanosecond => 4,
                },
            ]);
            match timezone {
                Some(timezone) => {
                    output.push(1);
                    let length = u32::try_from(timezone.len())
                        .map_err(|_| FilterMembershipSchemaError::TimezoneLengthOverflow)?;
                    output.extend_from_slice(&length.to_be_bytes());
                    output.extend_from_slice(timezone.as_bytes());
                }
                None => output.push(0),
            }
        }
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= DECIMAL128_MAX_PRECISION
                && *scale <= DECIMAL128_MAX_SCALE
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            output.extend_from_slice(&[12, *precision, *scale as u8]);
        }
        _ => return Err(FilterMembershipSchemaError::UnsupportedType),
    }
    Ok(())
}

fn decode_filter_key_type(
    cursor: &mut SchemaCursor<'_>,
) -> Result<DataType, FilterMembershipSchemaError> {
    match cursor.u8()? {
        1 => Ok(DataType::Boolean),
        2 => Ok(DataType::Int8),
        3 => Ok(DataType::Int16),
        4 => Ok(DataType::Int32),
        5 => Ok(DataType::Int64),
        6 => Ok(DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH)),
        7 => Ok(DataType::Float32),
        8 => Ok(DataType::Float64),
        9 => Ok(DataType::Utf8),
        10 => Ok(DataType::Date32),
        11 => {
            let unit = match cursor.u8()? {
                1 => TimeUnit::Second,
                2 => TimeUnit::Millisecond,
                3 => TimeUnit::Microsecond,
                4 => TimeUnit::Nanosecond,
                _ => return Err(FilterMembershipSchemaError::InvalidTimeUnit),
            };
            let timezone = match cursor.u8()? {
                0 => None,
                1 => {
                    let length = usize::try_from(cursor.u32()?)
                        .map_err(|_| FilterMembershipSchemaError::TimezoneLengthDecodeOverflow)?;
                    Some(
                        std::str::from_utf8(cursor.take(length)?)
                            .map_err(|_| FilterMembershipSchemaError::TimezoneNotUtf8)?
                            .into(),
                    )
                }
                _ => return Err(FilterMembershipSchemaError::InvalidTimezoneMetadata),
            };
            Ok(DataType::Timestamp(unit, timezone))
        }
        12 => {
            let precision = cursor.u8()?;
            let scale = cursor.u8()? as i8;
            if precision == 0
                || precision > DECIMAL128_MAX_PRECISION
                || scale > DECIMAL128_MAX_SCALE
                || (scale > 0 && scale as u8 > precision)
            {
                return Err(FilterMembershipSchemaError::InvalidDecimal);
            }
            Ok(DataType::Decimal128(precision, scale))
        }
        _ => Err(FilterMembershipSchemaError::UnsupportedTypeTag),
    }
}

impl StaticFilterProducer {
    pub fn try_new(
        binding_id: u32,
        channel_id: u32,
        kind: FilterProducerKind,
        contract: StaticFilterContract,
        reduction: FilterReduction,
    ) -> Result<Self, StaticFilterError> {
        contract.validate()?;
        let valid = matches!(
            (kind, &contract, reduction),
            (
                FilterProducerKind::Membership | FilterProducerKind::FinalDomain,
                StaticFilterContract::Membership { .. },
                FilterReduction::SetUnion,
            ) | (
                FilterProducerKind::OrderedBound,
                StaticFilterContract::Ordered { .. },
                FilterReduction::TightenOrderedBound,
            ) | (
                FilterProducerKind::TopKSummary,
                StaticFilterContract::Ordered { .. },
                FilterReduction::MergeTopKSummary { .. },
            )
        );
        if !valid {
            return Err(StaticFilterError::KindContractMismatch);
        }
        Ok(Self {
            binding_id,
            channel_id,
            kind,
            contract,
            reduction,
        })
    }

    pub const fn binding_id(&self) -> u32 {
        self.binding_id
    }

    pub const fn channel_id(&self) -> u32 {
        self.channel_id
    }

    pub const fn kind(&self) -> FilterProducerKind {
        self.kind
    }

    pub const fn contract(&self) -> &StaticFilterContract {
        &self.contract
    }

    pub const fn reduction(&self) -> FilterReduction {
        self.reduction
    }
}

impl StaticFilterConsumer {
    pub fn try_new(
        binding_id: u32,
        channel_id: u32,
        activation: FilterConsumerActivation,
        contract: StaticFilterContract,
        reduction: FilterReduction,
    ) -> Result<Self, StaticFilterError> {
        contract.validate()?;
        if matches!(activation, FilterConsumerActivation::BlockingSnapshot)
            && matches!(contract, StaticFilterContract::Ordered { .. })
        {
            return Err(StaticFilterError::BlockingOrderedConsumer);
        }
        let valid = matches!(
            (&contract, reduction),
            (
                StaticFilterContract::Membership { .. },
                FilterReduction::SetUnion,
            ) | (
                StaticFilterContract::Ordered { .. },
                FilterReduction::TightenOrderedBound | FilterReduction::MergeTopKSummary { .. },
            )
        );
        if !valid {
            return Err(StaticFilterError::ReductionContractMismatch);
        }
        Ok(Self {
            binding_id,
            channel_id,
            activation,
            contract,
            reduction,
        })
    }

    pub const fn binding_id(&self) -> u32 {
        self.binding_id
    }

    pub const fn channel_id(&self) -> u32 {
        self.channel_id
    }

    pub const fn activation(&self) -> FilterConsumerActivation {
        self.activation
    }

    pub const fn contract(&self) -> &StaticFilterContract {
        &self.contract
    }

    pub const fn reduction(&self) -> FilterReduction {
        self.reduction
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_role_mismatch_before_binding_a_session() {
        let contract = StaticFilterContract::Membership {
            data_type: DataType::Int64,
            null_semantics: FilterNullSemantics::NeverMatches,
            digest: [3; 32],
        };
        assert!(matches!(
            StaticFilterProducer::try_new(
                1,
                2,
                FilterProducerKind::TopKSummary,
                contract,
                FilterReduction::MergeTopKSummary {
                    k: NonZeroU32::new(5).unwrap(),
                },
            ),
            Err(StaticFilterError::KindContractMismatch)
        ));
    }

    #[test]
    fn membership_schema_pins_the_v1_canonical_bytes_and_digest() {
        let schema =
            FilterMembershipSchema::try_new(&DataType::Int32, FilterNullSemantics::NullSafeEqual)
                .expect("int32 null-safe schema is supported");
        assert_eq!(
            schema.canonical_bytes(),
            b"novarocks.runtime-filter.artifact-schema\x01\x04\x02"
        );
        assert_eq!(
            schema.digest(),
            [
                0x92, 0x5d, 0xde, 0xf2, 0x0b, 0x5b, 0xfa, 0x0e, 0xd3, 0xd6, 0xe3, 0x55, 0xf1, 0x18,
                0x79, 0xba, 0xeb, 0x2a, 0x0c, 0x7d, 0x31, 0x0b, 0x56, 0xe2, 0xcc, 0x03, 0x55, 0x57,
                0x10, 0xbf, 0x96, 0x41,
            ]
        );
        assert_eq!(
            FilterMembershipSchema::from_canonical(schema.canonical_bytes(), schema.digest()),
            Ok(schema.clone())
        );
        assert_eq!(
            StaticFilterContract::membership(&DataType::Int32, FilterNullSemantics::NullSafeEqual),
            Ok(StaticFilterContract::Membership {
                data_type: DataType::Int32,
                null_semantics: FilterNullSemantics::NullSafeEqual,
                digest: schema.digest(),
            })
        );
    }

    #[test]
    fn membership_schema_round_trips_parameterized_types_and_refuses_the_rest() {
        for data_type in [
            DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Shanghai".into())),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Decimal128(38, 10),
            DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
        ] {
            let schema =
                FilterMembershipSchema::try_new(&data_type, FilterNullSemantics::NeverMatches)
                    .expect("supported membership type");
            let decoded =
                FilterMembershipSchema::from_canonical(schema.canonical_bytes(), schema.digest())
                    .expect("exact canonical bytes decode");
            assert_eq!(decoded, schema);
        }
        assert_eq!(
            FilterMembershipSchema::try_new(
                &DataType::LargeUtf8,
                FilterNullSemantics::NeverMatches
            ),
            Err(FilterMembershipSchemaError::UnsupportedType)
        );
        let schema =
            FilterMembershipSchema::try_new(&DataType::Int64, FilterNullSemantics::NeverMatches)
                .unwrap();
        assert_eq!(
            FilterMembershipSchema::from_canonical(schema.canonical_bytes(), [0; 32]),
            Err(FilterMembershipSchemaError::DigestMismatch)
        );
        let mut trailing = schema.canonical_bytes().to_vec();
        trailing.push(0);
        let digest: [u8; 32] = Sha256::digest(&trailing).into();
        assert_eq!(
            FilterMembershipSchema::from_canonical(&trailing, digest),
            Err(FilterMembershipSchemaError::TrailingBytes)
        );
    }
}
