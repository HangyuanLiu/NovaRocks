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

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::sync::Arc;

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, TimeUnit};
use sha2::{Digest, Sha256};

use crate::common::largeint::LARGEINT_BYTE_WIDTH;
use crate::runtime_filter::model::contract::{ChannelId, NullSemantics};

use super::identity::LogicalVersion;
use super::support::ArtifactRetention;

const PROFILE_VERSION: u8 = 1;
const SCHEMA_VERSION: u8 = 1;
pub(crate) const LEAF_CODEC_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum ArtifactKind {
    ValueSet,
    Bloom,
    Bitset,
    Range,
    EmptyDomain,
}

impl ArtifactKind {
    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::ValueSet => 1,
            Self::Bloom => 2,
            Self::Bitset => 3,
            Self::Range => 4,
            Self::EmptyDomain => 5,
        }
    }

    pub(crate) const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::ValueSet),
            2 => Some(Self::Bloom),
            3 => Some(Self::Bitset),
            4 => Some(Self::Range),
            5 => Some(Self::EmptyDomain),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct HashContractDigest([u8; 32]);

impl HashContractDigest {
    pub(crate) const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ConsumerProfileId([u8; 32]);

impl ConsumerProfileId {
    pub(crate) const fn bytes(self) -> [u8; 32] {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn for_test(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ArtifactSchemaDigest([u8; 32]);

impl ArtifactSchemaDigest {
    pub(crate) const fn from_canonical_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn for_membership(
        data_type: &DataType,
        null_semantics: NullSemantics,
    ) -> Result<Self, ArtifactContractError> {
        Ok(ArtifactMembershipSchema::new(data_type, null_semantics)?.digest())
    }

    pub(crate) const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactMembershipSchema {
    data_type: DataType,
    null_semantics: NullSemantics,
    canonical_bytes: Arc<[u8]>,
    digest: ArtifactSchemaDigest,
}

impl ArtifactMembershipSchema {
    pub(crate) fn new(
        data_type: &DataType,
        null_semantics: NullSemantics,
    ) -> Result<Self, ArtifactContractError> {
        let mut canonical = Vec::with_capacity(48);
        canonical.extend_from_slice(b"novarocks.runtime-filter.artifact-schema");
        canonical.push(SCHEMA_VERSION);
        encode_schema(data_type, &mut canonical)?;
        canonical.push(match null_semantics {
            NullSemantics::NeverMatches => 1,
            NullSemantics::NullSafeEqual => 2,
        });
        let digest = ArtifactSchemaDigest(Sha256::digest(&canonical).into());
        Ok(Self {
            data_type: data_type.clone(),
            null_semantics,
            canonical_bytes: canonical.into(),
            digest,
        })
    }

    pub(crate) fn view(
        canonical: &[u8],
    ) -> Result<ArtifactMembershipSchemaView<'_>, ArtifactContractError> {
        const DOMAIN: &[u8] = b"novarocks.runtime-filter.artifact-schema";
        let mut cursor = SchemaCursor::new(canonical);
        if cursor.take(DOMAIN.len())? != DOMAIN || cursor.u8()? != SCHEMA_VERSION {
            return Err(ArtifactContractError::NonCanonicalSchema);
        }
        let payload_tag = cursor.u8()?;
        let type_contract = match payload_tag {
            1..=10 => ArtifactMembershipTypeContract::Primitive,
            11 => {
                let unit = match cursor.u8()? {
                    unit @ 1..=4 => unit,
                    _ => return Err(ArtifactContractError::NonCanonicalSchema),
                };
                let timezone = match cursor.u8()? {
                    0 => None,
                    1 => {
                        let len = cursor.u32()? as usize;
                        let timezone = std::str::from_utf8(cursor.take(len)?)
                            .map_err(|_| ArtifactContractError::NonCanonicalSchema)?;
                        Some(timezone)
                    }
                    _ => return Err(ArtifactContractError::NonCanonicalSchema),
                };
                ArtifactMembershipTypeContract::Timestamp { unit, timezone }
            }
            12 => {
                let precision = cursor.u8()?;
                let scale = cursor.u8()? as i8;
                if precision == 0
                    || precision > DECIMAL128_MAX_PRECISION
                    || scale > DECIMAL128_MAX_SCALE
                    || (scale > 0 && scale as u8 > precision)
                {
                    return Err(ArtifactContractError::NonCanonicalSchema);
                }
                ArtifactMembershipTypeContract::Decimal { precision, scale }
            }
            _ => return Err(ArtifactContractError::NonCanonicalSchema),
        };
        let null_semantics = match cursor.u8()? {
            1 => NullSemantics::NeverMatches,
            2 => NullSemantics::NullSafeEqual,
            _ => return Err(ArtifactContractError::NonCanonicalSchema),
        };
        if !cursor.is_empty() {
            return Err(ArtifactContractError::NonCanonicalSchema);
        }
        Ok(ArtifactMembershipSchemaView {
            payload_tag,
            type_contract,
            null_semantics,
            digest: ArtifactSchemaDigest(Sha256::digest(canonical).into()),
        })
    }

    pub(crate) const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub(crate) const fn null_semantics(&self) -> NullSemantics {
        self.null_semantics
    }

    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub(crate) const fn digest(&self) -> ArtifactSchemaDigest {
        self.digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactMembershipTypeContract<'a> {
    Primitive,
    Timestamp { unit: u8, timezone: Option<&'a str> },
    Decimal { precision: u8, scale: i8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactMembershipSchemaView<'a> {
    payload_tag: u8,
    type_contract: ArtifactMembershipTypeContract<'a>,
    null_semantics: NullSemantics,
    digest: ArtifactSchemaDigest,
}

impl<'a> ArtifactMembershipSchemaView<'a> {
    pub(crate) const fn payload_tag(self) -> u8 {
        self.payload_tag
    }

    pub(crate) const fn timestamp_contract(self) -> Option<(u8, Option<&'a str>)> {
        match self.type_contract {
            ArtifactMembershipTypeContract::Timestamp { unit, timezone } => Some((unit, timezone)),
            _ => None,
        }
    }

    pub(crate) const fn decimal_contract(self) -> Option<(u8, i8)> {
        match self.type_contract {
            ArtifactMembershipTypeContract::Decimal { precision, scale } => {
                Some((precision, scale))
            }
            _ => None,
        }
    }

    pub(crate) const fn null_semantics(self) -> NullSemantics {
        self.null_semantics
    }

    pub(crate) const fn digest(self) -> ArtifactSchemaDigest {
        self.digest
    }
}

struct SchemaCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> SchemaCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ArtifactContractError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(len)
            .ok_or(ArtifactContractError::NonCanonicalSchema)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ArtifactContractError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ArtifactContractError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("four-byte schema field"),
        ))
    }
}

fn encode_schema(data_type: &DataType, output: &mut Vec<u8>) -> Result<(), ArtifactContractError> {
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
                    let len = u32::try_from(timezone.len())
                        .map_err(|_| ArtifactContractError::LengthOverflow)?;
                    output.extend_from_slice(&len.to_be_bytes());
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
        _ => return Err(ArtifactContractError::UnsupportedSchema),
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerArtifactProfile {
    accepted_kinds: BTreeSet<ArtifactKind>,
    bloom_hash_contract: Option<HashContractDigest>,
    canonical_bytes: Arc<[u8]>,
    id: ConsumerProfileId,
}

impl ConsumerArtifactProfile {
    pub(crate) fn new(
        accepted_kinds: BTreeSet<ArtifactKind>,
        bloom_hash_contract: Option<HashContractDigest>,
    ) -> Result<Self, ArtifactContractError> {
        if accepted_kinds.is_empty() {
            return Err(ArtifactContractError::EmptyProfile);
        }
        if accepted_kinds.contains(&ArtifactKind::Bloom) != bloom_hash_contract.is_some() {
            return Err(ArtifactContractError::BloomHashContractMismatch);
        }
        let count = u16::try_from(accepted_kinds.len())
            .map_err(|_| ArtifactContractError::LengthOverflow)?;
        let mut canonical = Vec::with_capacity(4 + accepted_kinds.len() + 32);
        canonical.extend_from_slice(&[PROFILE_VERSION]);
        canonical.extend_from_slice(&count.to_be_bytes());
        canonical.extend(accepted_kinds.iter().map(|kind| kind.tag()));
        match bloom_hash_contract {
            Some(digest) => {
                canonical.push(1);
                canonical.extend_from_slice(&digest.bytes());
            }
            None => canonical.push(0),
        }
        let id = ConsumerProfileId(Sha256::digest(&canonical).into());
        Ok(Self {
            accepted_kinds,
            bloom_hash_contract,
            canonical_bytes: canonical.into(),
            id,
        })
    }

    #[cfg(test)]
    pub(crate) fn m1_test_default() -> Self {
        Self::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .expect("built-in test profile is valid")
    }

    pub(crate) const fn accepted_kinds(&self) -> &BTreeSet<ArtifactKind> {
        &self.accepted_kinds
    }

    pub(crate) fn accepts(&self, kind: ArtifactKind) -> bool {
        self.accepted_kinds.contains(&kind)
    }

    pub(crate) const fn bloom_hash_contract(&self) -> Option<HashContractDigest> {
        self.bloom_hash_contract
    }

    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub(crate) const fn id(&self) -> ConsumerProfileId {
        self.id
    }

    #[cfg(test)]
    pub(crate) fn with_test_identity(mut self, id: ConsumerProfileId) -> Self {
        self.id = id;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactContractError {
    EmptyProfile,
    BloomHashContractMismatch,
    UnsupportedSchema,
    NonCanonicalSchema,
    LengthOverflow,
    EmptyBundle,
    DuplicateKind,
    KindNotAccepted,
    KindMismatch,
    VersionMismatch,
    SchemaMismatch,
    EncodedSizeOverflow,
    EncodedSizeExceeded,
    RetentionSizeMismatch,
    ResidentSizeOverflow,
}

impl fmt::Display for ArtifactContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid runtime filter artifact contract: {self:?}"
        )
    }
}

impl Error for ArtifactContractError {}

pub(crate) struct PhysicalArtifact {
    kind: ArtifactKind,
    codec_version: u16,
    schema_digest: ArtifactSchemaDigest,
    version: LogicalVersion,
    contains_null: bool,
    canonical_bytes: Arc<[u8]>,
    canonical_digest: [u8; 32],
    retained_memory: Option<Arc<ArtifactRetention>>,
}

impl PhysicalArtifact {
    pub(crate) fn accounted_resident_component_bytes(
        encoded_bytes: usize,
    ) -> Result<usize, ArtifactContractError> {
        encoded_bytes
            .checked_add(size_of::<Self>())
            .and_then(|bytes| bytes.checked_add(size_of::<Arc<[u8]>>()))
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub(crate) fn accounted_resident_bytes(
        encoded_bytes: usize,
    ) -> Result<usize, ArtifactContractError> {
        Self::accounted_resident_component_bytes(encoded_bytes)?
            .checked_add(size_of::<ArtifactRetention>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub(crate) fn from_retained_bytes(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
        accounted_resident_bytes: usize,
        retained_memory: ArtifactRetention,
    ) -> Result<Self, ArtifactContractError> {
        if accounted_resident_bytes != Self::accounted_resident_bytes(canonical_bytes.len())?
            || retained_memory.bytes() != accounted_resident_bytes
            || retained_memory.budget_bytes() != accounted_resident_bytes
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Ok(Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            retained_memory: Some(Arc::new(retained_memory)),
        })
    }

    pub(crate) fn from_shared_retained_bytes(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
        accounted_resident_component_bytes: usize,
        total_accounted_resident_bytes: usize,
        retained_memory: Arc<ArtifactRetention>,
    ) -> Result<Self, ArtifactContractError> {
        if accounted_resident_component_bytes
            != Self::accounted_resident_component_bytes(canonical_bytes.len())?
            || accounted_resident_component_bytes > total_accounted_resident_bytes
            || retained_memory.bytes() != total_accounted_resident_bytes
            || retained_memory.budget_bytes() != total_accounted_resident_bytes
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Ok(Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            retained_memory: Some(retained_memory),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_test(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
    ) -> Self {
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            retained_memory: None,
        }
    }

    pub(crate) const fn kind(&self) -> ArtifactKind {
        self.kind
    }
    pub(crate) const fn codec_version(&self) -> u16 {
        self.codec_version
    }
    pub(crate) const fn schema_digest(&self) -> ArtifactSchemaDigest {
        self.schema_digest
    }
    pub(crate) const fn version(&self) -> LogicalVersion {
        self.version
    }
    pub(crate) const fn contains_null(&self) -> bool {
        self.contains_null
    }
    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
    pub(crate) const fn canonical_digest(&self) -> [u8; 32] {
        self.canonical_digest
    }
    pub(crate) fn retained_memory_bytes(&self) -> usize {
        self.retained_memory
            .as_ref()
            .map_or(0, |retention| retention.bytes())
    }

    fn shares_retention(&self, retention: &Arc<ArtifactRetention>) -> bool {
        self.retained_memory
            .as_ref()
            .is_some_and(|owned| Arc::ptr_eq(owned, retention))
    }
}

impl fmt::Debug for PhysicalArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PhysicalArtifact")
            .field("kind", &self.kind)
            .field("codec_version", &self.codec_version)
            .field("schema_digest", &self.schema_digest)
            .field("version", &self.version)
            .field("contains_null", &self.contains_null)
            .field("canonical_bytes", &self.canonical_bytes.len())
            .field("canonical_digest", &self.canonical_digest)
            .field("retained_memory_bytes", &self.retained_memory_bytes())
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct ArtifactBundle {
    channel_id: ChannelId,
    version: LogicalVersion,
    profile_id: ConsumerProfileId,
    artifacts: Box<[(ArtifactKind, Arc<PhysicalArtifact>)]>,
    canonical_digest: [u8; 32],
    encoded_bytes: usize,
    retained_memory: Option<Arc<ArtifactRetention>>,
}

impl ArtifactBundle {
    const CANONICAL_HEADER_BYTES: usize = 4 + 1 + 8 + 8 + 32 + 2;

    pub(crate) fn canonical_encoded_len(
        artifacts: &[(ArtifactKind, Arc<PhysicalArtifact>)],
    ) -> Result<usize, ArtifactContractError> {
        u16::try_from(artifacts.len()).map_err(|_| ArtifactContractError::EncodedSizeOverflow)?;
        artifacts
            .iter()
            .try_fold(Self::CANONICAL_HEADER_BYTES, |encoded, (_, artifact)| {
                u64::try_from(artifact.canonical_bytes().len())
                    .map_err(|_| ArtifactContractError::EncodedSizeOverflow)?;
                encoded
                    .checked_add(1 + 8)
                    .and_then(|encoded| encoded.checked_add(artifact.canonical_bytes().len()))
                    .ok_or(ArtifactContractError::EncodedSizeOverflow)
            })
    }

    pub(crate) fn canonical_encoded_len_for_single_artifact(
        artifact_encoded_bytes: usize,
    ) -> Result<usize, ArtifactContractError> {
        Self::CANONICAL_HEADER_BYTES
            .checked_add(1 + 8)
            .and_then(|bytes| bytes.checked_add(artifact_encoded_bytes))
            .ok_or(ArtifactContractError::EncodedSizeOverflow)
    }

    pub(crate) fn accounted_resident_overhead(
        profile: &ConsumerArtifactProfile,
        artifact_count: usize,
    ) -> Result<usize, ArtifactContractError> {
        let refs = artifact_count
            .checked_mul(size_of::<(ArtifactKind, Arc<PhysicalArtifact>)>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        size_of::<Self>()
            .checked_add(profile.canonical_bytes().len())
            .and_then(|bytes| bytes.checked_add(refs))
            .and_then(|bytes| bytes.checked_add(size_of::<ArtifactRetention>()))
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub(crate) fn new(
        channel_id: ChannelId,
        version: LogicalVersion,
        profile: &ConsumerArtifactProfile,
        artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)>,
        max_artifact_bytes: usize,
    ) -> Result<Self, ArtifactContractError> {
        Self::new_inner(
            channel_id,
            version,
            profile,
            artifacts,
            max_artifact_bytes,
            None,
        )
    }

    pub(crate) fn new_retained(
        channel_id: ChannelId,
        version: LogicalVersion,
        profile: &ConsumerArtifactProfile,
        artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)>,
        max_artifact_bytes: usize,
        retained_memory: Arc<ArtifactRetention>,
    ) -> Result<Self, ArtifactContractError> {
        let expected = artifacts.iter().try_fold(
            Self::accounted_resident_overhead(profile, artifacts.len())?,
            |bytes, (_, artifact)| {
                bytes
                    .checked_add(PhysicalArtifact::accounted_resident_component_bytes(
                        artifact.canonical_bytes().len(),
                    )?)
                    .ok_or(ArtifactContractError::ResidentSizeOverflow)
            },
        )?;
        if retained_memory.bytes() != expected || retained_memory.budget_bytes() != expected {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        if artifacts
            .iter()
            .any(|(_, artifact)| !artifact.shares_retention(&retained_memory))
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        Self::new_inner(
            channel_id,
            version,
            profile,
            artifacts,
            max_artifact_bytes,
            Some(retained_memory),
        )
    }

    fn new_inner(
        channel_id: ChannelId,
        version: LogicalVersion,
        profile: &ConsumerArtifactProfile,
        mut artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)>,
        max_artifact_bytes: usize,
        retained_memory: Option<Arc<ArtifactRetention>>,
    ) -> Result<Self, ArtifactContractError> {
        if artifacts.is_empty() {
            return Err(ArtifactContractError::EmptyBundle);
        }
        artifacts.sort_unstable_by_key(|(kind, _)| *kind);
        let mut schema = None;
        let count = u16::try_from(artifacts.len())
            .map_err(|_| ArtifactContractError::EncodedSizeOverflow)?;
        for (index, (kind, artifact)) in artifacts.iter().enumerate() {
            if index != 0 && artifacts[index - 1].0 == *kind {
                return Err(ArtifactContractError::DuplicateKind);
            }
            if !profile.accepts(*kind) {
                return Err(ArtifactContractError::KindNotAccepted);
            }
            if artifact.kind() != *kind {
                return Err(ArtifactContractError::KindMismatch);
            }
            if artifact.version() != version {
                return Err(ArtifactContractError::VersionMismatch);
            }
            if schema
                .replace(artifact.schema_digest())
                .is_some_and(|old| old != artifact.schema_digest())
            {
                return Err(ArtifactContractError::SchemaMismatch);
            }
            u64::try_from(artifact.canonical_bytes().len())
                .map_err(|_| ArtifactContractError::EncodedSizeOverflow)?;
        }
        let encoded_bytes = Self::canonical_encoded_len(&artifacts)?;
        if encoded_bytes > max_artifact_bytes {
            return Err(ArtifactContractError::EncodedSizeExceeded);
        }
        let mut canonical = Sha256::new();
        canonical.update(b"NRFB");
        canonical.update([1]);
        canonical.update(channel_id.get().to_be_bytes());
        canonical.update(version.get().to_be_bytes());
        canonical.update(profile.id().bytes());
        canonical.update(count.to_be_bytes());
        for (kind, artifact) in &artifacts {
            canonical.update([kind.tag()]);
            canonical.update(
                u64::try_from(artifact.canonical_bytes().len())
                    .expect("artifact length was checked before hashing")
                    .to_be_bytes(),
            );
            canonical.update(artifact.canonical_bytes());
        }
        let canonical_digest = canonical.finalize().into();
        Ok(Self {
            channel_id,
            version,
            profile_id: profile.id(),
            artifacts: artifacts.into_boxed_slice(),
            canonical_digest,
            encoded_bytes,
            retained_memory,
        })
    }

    pub(crate) const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }
    pub(crate) const fn version(&self) -> LogicalVersion {
        self.version
    }
    pub(crate) const fn profile_id(&self) -> ConsumerProfileId {
        self.profile_id
    }
    pub(crate) const fn artifacts(&self) -> &[(ArtifactKind, Arc<PhysicalArtifact>)] {
        &self.artifacts
    }
    pub(crate) const fn canonical_digest(&self) -> [u8; 32] {
        self.canonical_digest
    }
    pub(crate) const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    pub(crate) fn retained_memory_bytes(&self) -> usize {
        self.retained_memory
            .as_ref()
            .map_or(0, |retention| retention.bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::mem::size_of;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{ArtifactCapability, ChannelId, NullSemantics};
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactRetention, MemoryAccountError, RuntimeFilterMemoryAccount,
    };

    use super::{
        ArtifactBundle, ArtifactContractError, ArtifactKind, ArtifactSchemaDigest,
        ConsumerArtifactProfile, PhysicalArtifact,
    };

    struct AcceptingMemoryAccount;

    impl RuntimeFilterMemoryAccount for AcceptingMemoryAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    #[test]
    fn semantic_capability_and_physical_kind_remain_distinct() {
        let semantics = BTreeSet::from([
            ArtifactCapability::Membership,
            ArtifactCapability::EmptyDomain,
        ]);
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();

        assert!(semantics.contains(&ArtifactCapability::Membership));
        assert!(profile.accepts(ArtifactKind::ValueSet));
    }

    #[test]
    fn normalized_profile_is_order_independent_and_digest_stable() {
        let left = ConsumerArtifactProfile::new(
            [ArtifactKind::EmptyDomain, ArtifactKind::ValueSet]
                .into_iter()
                .collect(),
            None,
        )
        .unwrap();
        let right = ConsumerArtifactProfile::new(
            [ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]
                .into_iter()
                .collect(),
            None,
        )
        .unwrap();

        assert_eq!(left.canonical_bytes(), right.canonical_bytes());
        assert_eq!(left.id(), right.id());
    }

    #[test]
    fn bundle_keeps_channel_version_profile_and_only_accepted_kinds() {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let artifact = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            schema,
            LogicalVersion::FIRST,
            false,
            Arc::from([1_u8, 2, 3]),
        ));
        let bundle = ArtifactBundle::new(
            ChannelId::new(7),
            LogicalVersion::FIRST,
            &profile,
            vec![(ArtifactKind::ValueSet, artifact)],
            1024,
        )
        .unwrap();

        assert_eq!(bundle.channel_id(), ChannelId::new(7));
        assert_eq!(bundle.version(), LogicalVersion::FIRST);
        assert_eq!(bundle.profile_id(), profile.id());
        assert_eq!(bundle.artifacts().len(), 1);
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::ValueSet);
        assert_eq!(
            bundle.encoded_bytes(),
            ArtifactBundle::canonical_encoded_len(bundle.artifacts()).unwrap()
        );
    }

    #[test]
    fn bundle_rejects_duplicate_unaccepted_mismatched_and_over_budget_artifacts() {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let value_set = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            schema,
            LogicalVersion::FIRST,
            false,
            Arc::from([1_u8]),
        ));
        assert_eq!(
            ArtifactBundle::new(
                ChannelId::new(7),
                LogicalVersion::FIRST,
                &profile,
                vec![
                    (ArtifactKind::ValueSet, value_set.clone()),
                    (ArtifactKind::ValueSet, value_set.clone()),
                ],
                1024,
            )
            .unwrap_err(),
            ArtifactContractError::DuplicateKind
        );
        assert_eq!(
            ArtifactBundle::new(
                ChannelId::new(7),
                LogicalVersion::FIRST,
                &profile,
                vec![(ArtifactKind::Bloom, value_set.clone())],
                1024,
            )
            .unwrap_err(),
            ArtifactContractError::KindNotAccepted
        );
        assert_eq!(
            ArtifactBundle::new(
                ChannelId::new(7),
                LogicalVersion::new(2),
                &profile,
                vec![(ArtifactKind::ValueSet, value_set.clone())],
                1024,
            )
            .unwrap_err(),
            ArtifactContractError::VersionMismatch
        );
        assert_eq!(
            ArtifactBundle::new(
                ChannelId::new(7),
                LogicalVersion::FIRST,
                &profile,
                vec![(ArtifactKind::ValueSet, value_set)],
                1,
            )
            .unwrap_err(),
            ArtifactContractError::EncodedSizeExceeded
        );
    }

    #[test]
    fn retained_artifact_bytes_cannot_exceed_the_bound_reservation() {
        let budget = Arc::new(ArtifactRetainedBudget::new(8));
        let retention =
            ArtifactRetention::try_new(1, budget.clone(), Arc::new(AcceptingMemoryAccount))
                .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let error = PhysicalArtifact::from_retained_bytes(
            ArtifactKind::ValueSet,
            schema,
            LogicalVersion::FIRST,
            false,
            Arc::from([1_u8, 2]),
            PhysicalArtifact::accounted_resident_bytes(2).unwrap(),
            retention,
        )
        .unwrap_err();

        assert_eq!(error, ArtifactContractError::RetentionSizeMismatch);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn accounted_artifact_footprint_includes_shared_retention_owner_metadata() {
        let encoded_bytes = 17;
        let accounted = PhysicalArtifact::accounted_resident_bytes(encoded_bytes).unwrap();
        assert!(
            accounted
                >= encoded_bytes + size_of::<PhysicalArtifact>() + size_of::<ArtifactRetention>()
        );
    }

    #[test]
    fn two_artifact_bundle_accounts_one_shared_owner_at_the_exact_boundary() {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let first_bytes: Arc<[u8]> = Arc::from([1_u8, 2]);
        let second_bytes: Arc<[u8]> = Arc::from([3_u8]);
        let first_component =
            PhysicalArtifact::accounted_resident_component_bytes(first_bytes.len()).unwrap();
        let second_component =
            PhysicalArtifact::accounted_resident_component_bytes(second_bytes.len()).unwrap();
        let total = ArtifactBundle::accounted_resident_overhead(&profile, 2)
            .unwrap()
            .checked_add(first_component)
            .and_then(|bytes| bytes.checked_add(second_component))
            .unwrap();
        let short_budget = Arc::new(ArtifactRetainedBudget::new(total - 1));
        assert!(
            ArtifactRetention::try_new(
                total,
                short_budget.clone(),
                Arc::new(AcceptingMemoryAccount)
            )
            .is_err()
        );
        assert_eq!(short_budget.retained_bytes(), 0);

        let budget = Arc::new(ArtifactRetainedBudget::new(total));
        let retention = Arc::new(
            ArtifactRetention::try_new(total, budget.clone(), Arc::new(AcceptingMemoryAccount))
                .unwrap(),
        );
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let first = Arc::new(
            PhysicalArtifact::from_shared_retained_bytes(
                ArtifactKind::ValueSet,
                schema,
                LogicalVersion::FIRST,
                false,
                first_bytes,
                first_component,
                total,
                retention.clone(),
            )
            .unwrap(),
        );
        let second = Arc::new(
            PhysicalArtifact::from_shared_retained_bytes(
                ArtifactKind::EmptyDomain,
                schema,
                LogicalVersion::FIRST,
                false,
                second_bytes,
                second_component,
                total,
                retention.clone(),
            )
            .unwrap(),
        );
        let bundle = ArtifactBundle::new_retained(
            ChannelId::new(8),
            LogicalVersion::FIRST,
            &profile,
            vec![
                (ArtifactKind::ValueSet, first.clone()),
                (ArtifactKind::EmptyDomain, second.clone()),
            ],
            usize::MAX,
            retention,
        )
        .unwrap();
        assert_eq!(bundle.retained_memory_bytes(), total);
        assert_eq!(budget.retained_bytes(), total);
        drop(bundle);
        assert_eq!(budget.retained_bytes(), total);
        drop(first);
        assert_eq!(budget.retained_bytes(), total);
        drop(second);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn schema_digest_uses_explicit_timestamp_and_null_semantics() {
        let utc = ArtifactSchemaDigest::for_membership(
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            NullSemantics::NeverMatches,
        )
        .unwrap();
        let nullable = ArtifactSchemaDigest::for_membership(
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            NullSemantics::NullSafeEqual,
        )
        .unwrap();
        let no_tz = ArtifactSchemaDigest::for_membership(
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            NullSemantics::NeverMatches,
        )
        .unwrap();

        assert_ne!(utc, nullable);
        assert_ne!(utc, no_tz);
    }
}
