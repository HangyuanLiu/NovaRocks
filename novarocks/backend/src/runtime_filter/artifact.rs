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

//! Backend-private physical runtime-filter artifacts.
//!
//! This module deliberately stores only sealed Execution contracts plus
//! canonical physical bytes.  It has no query, scan, Arrow-array, or Core
//! evaluator dependency.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_execution::runtime_filter::LogicalVersion;
use sha2::{Digest, Sha256};

pub(crate) const LEAF_CODEC_VERSION: u16 = 1;

/// Backend-private retained-artifact admission. The lease is held by both the
/// bundle and every artifact it protects, so a cloned resident artifact cannot
/// outlive its reservation.
#[derive(Debug)]
pub(crate) struct ArtifactRetainedBudget {
    max_bytes: usize,
    retained_bytes: AtomicUsize,
}

impl ArtifactRetainedBudget {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            retained_bytes: AtomicUsize::new(0),
        }
    }

    pub(crate) fn try_acquire(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<ArtifactRetention, ArtifactContractError> {
        let mut current = self.retained_bytes.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(bytes)
                .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
            if next > self.max_bytes {
                return Err(ArtifactContractError::RetentionCapacityExceeded);
            }
            match self.retained_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ArtifactRetention {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub(crate) struct ArtifactRetention {
    budget: Arc<ArtifactRetainedBudget>,
    bytes: usize,
}

/// Scratch uses the same bounded atomic lease mechanics but is deliberately
/// a separate budget from retained resident artifacts.
pub(crate) type ArtifactScratchBudget = ArtifactRetainedBudget;
pub(crate) type ArtifactScratchReservation = ArtifactRetention;

impl ArtifactRetention {
    pub(crate) const fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for ArtifactRetention {
    fn drop(&mut self) {
        if self.bytes != 0 {
            let previous = self
                .budget
                .retained_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
            debug_assert!(previous >= self.bytes);
        }
    }
}

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

macro_rules! digest {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub(crate) struct $name([u8; 32]);
        impl $name {
#[allow(dead_code, reason = "Retained for staged backend runtime-filter domain and materialization integration.")]
            pub(crate) const fn new(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }
            pub(crate) const fn bytes(self) -> [u8; 32] {
                self.0
            }
        }
    };
}

digest!(ArtifactSchemaDigest);
digest!(HashContractDigest);
digest!(ConsumerProfileId);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerArtifactProfile {
    accepted_kinds: BTreeSet<ArtifactKind>,
    bloom_hash_contract: Option<HashContractDigest>,
    order_contract_digest: Option<[u8; 32]>,
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
        canonical.push(1); // existing membership profile wire version
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
            order_contract_digest: None,
            canonical_bytes: canonical.into(),
            id,
        })
    }

    pub(crate) fn accepts(&self, kind: ArtifactKind) -> bool {
        self.accepted_kinds.contains(&kind)
    }
    pub(crate) const fn bloom_hash_contract(&self) -> Option<HashContractDigest> {
        self.bloom_hash_contract
    }
    pub(crate) fn new_ordered_range(
        order_contract_digest: [u8; 32],
    ) -> Result<Self, ArtifactContractError> {
        let accepted_kinds = BTreeSet::from([ArtifactKind::Range]);
        let mut canonical = vec![2, 0, 1, ArtifactKind::Range.tag(), 0, 1];
        canonical.extend_from_slice(&order_contract_digest);
        Ok(Self {
            accepted_kinds,
            bloom_hash_contract: None,
            order_contract_digest: Some(order_contract_digest),
            id: ConsumerProfileId(Sha256::digest(&canonical).into()),
            canonical_bytes: canonical.into(),
        })
    }
    pub(crate) const fn order_contract_digest(&self) -> Option<[u8; 32]> {
        self.order_contract_digest
    }
    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
    pub(crate) const fn id(&self) -> ConsumerProfileId {
        self.id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResidentMembershipIndex {
    EmptyDomain,
    Fixed {
        tag: u8,
        values: Range<usize>,
        count: usize,
        width: usize,
    },
    Utf8 {
        payload: Range<usize>,
        length_offsets: Box<[usize]>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResidentFilterIndex {
    Bitset {
        min: i64,
        bit_count: u64,
        bits: Range<usize>,
    },
    Bloom {
        bit_count: u64,
        bits: Range<usize>,
        hash_contract: HashContractDigest,
    },
}

impl ResidentFilterIndex {
    /// Exact query for the retained numeric bitset representation. Bloom is
    /// intentionally not exposed here: its scalar framing is typed and is
    /// evaluated only by the later Backend artifact-query adapter.
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn bitset_contains_i64(
        &self,
        encoded: &[u8],
        value: i64,
    ) -> Result<Option<bool>, ArtifactContractError> {
        let Self::Bitset {
            min,
            bit_count,
            bits,
        } = self
        else {
            return Ok(None);
        };
        if *bit_count == 0 || bits.end < bits.start || bits.end > encoded.len() {
            return Err(ArtifactContractError::InvalidResidentFilterIndex);
        }
        let byte_len = usize::try_from(
            (*bit_count)
                .checked_add(7)
                .and_then(|bits| bits.checked_div(8))
                .ok_or(ArtifactContractError::ResidentSizeOverflow)?,
        )
        .map_err(|_| ArtifactContractError::ResidentSizeOverflow)?;
        if byte_len != bits.end - bits.start {
            return Err(ArtifactContractError::InvalidResidentFilterIndex);
        }
        let offset = match value.checked_sub(*min) {
            Some(offset) if offset >= 0 => u64::try_from(offset).unwrap_or(u64::MAX),
            _ => return Ok(Some(false)),
        };
        if offset >= *bit_count {
            return Ok(Some(false));
        }
        let byte = encoded
            .get(
                bits.start
                    + usize::try_from(offset / 8)
                        .map_err(|_| ArtifactContractError::ResidentSizeOverflow)?,
            )
            .ok_or(ArtifactContractError::InvalidResidentFilterIndex)?;
        Ok(Some((byte & (1 << (offset % 8))) != 0))
    }
}

/// Backend-owned retained ordered-bound facts.  Keeping this alongside the
/// canonical NRRG bytes prevents a later consumer from re-decoding plan state.
#[derive(Clone, Debug)]
pub(crate) struct RangeResidentData {
    pub(crate) contract:
        Arc<novarocks_execution::runtime_filter::contribution::RuntimeOrderContract>,
    pub(crate) bound: novarocks_execution::runtime_filter::contribution::OrderedTuple,
}

impl ResidentMembershipIndex {
    pub(crate) fn heap_bytes(&self) -> Result<usize, ArtifactContractError> {
        match self {
            Self::Utf8 { length_offsets, .. } => length_offsets
                .len()
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or(ArtifactContractError::ResidentSizeOverflow),
            Self::EmptyDomain | Self::Fixed { .. } => Ok(0),
        }
    }

    /// Exact primitive query for the fixed-width Int64 resident layout. Other
    /// schema kinds remain typed capability-unsupported rather than falling
    /// back to a logical-domain decode.
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn contains_i64(
        &self,
        encoded: &[u8],
        needle: i64,
    ) -> Result<Option<bool>, ArtifactContractError> {
        let Self::Fixed {
            tag: 5,
            values,
            count,
            width: 8,
        } = self
        else {
            return Ok(None);
        };
        let bytes = encoded
            .get(values.clone())
            .ok_or(ArtifactContractError::InvalidMembershipIndex)?;
        if bytes.len()
            != count
                .checked_mul(8)
                .ok_or(ArtifactContractError::ResidentSizeOverflow)?
        {
            return Err(ArtifactContractError::InvalidMembershipIndex);
        }
        let mut low = 0;
        let mut high = *count;
        while low < high {
            let mid = low + (high - low) / 2;
            let value = fixed_i64_at(bytes, mid)?;
            match value.cmp(&needle) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return Ok(Some(true)),
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        Ok(Some(false))
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn range_may_match_i64(
        &self,
        encoded: &[u8],
        min: i64,
        max: i64,
    ) -> Result<Option<bool>, ArtifactContractError> {
        if min > max {
            return Err(ArtifactContractError::InvalidMembershipIndex);
        }
        let Self::Fixed {
            tag: 5,
            values,
            count,
            width: 8,
        } = self
        else {
            return Ok(None);
        };
        let bytes = encoded
            .get(values.clone())
            .ok_or(ArtifactContractError::InvalidMembershipIndex)?;
        if bytes.len()
            != count
                .checked_mul(8)
                .ok_or(ArtifactContractError::ResidentSizeOverflow)?
        {
            return Err(ArtifactContractError::InvalidMembershipIndex);
        }
        let mut offset = 0;
        let mut high = *count;
        while offset < high {
            let mid = offset + (high - offset) / 2;
            if fixed_i64_at(bytes, mid)? < min {
                offset = mid + 1;
            } else {
                high = mid;
            }
        }
        Ok(Some(offset < *count && fixed_i64_at(bytes, offset)? <= max))
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn fixed_i64_at(bytes: &[u8], offset: usize) -> Result<i64, ArtifactContractError> {
    let start = offset
        .checked_mul(8)
        .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
    let value = bytes
        .get(start..start + 8)
        .ok_or(ArtifactContractError::InvalidMembershipIndex)?;
    Ok(i64::from_be_bytes(value.try_into().map_err(|_| {
        ArtifactContractError::InvalidMembershipIndex
    })?))
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalArtifact {
    kind: ArtifactKind,
    schema_digest: ArtifactSchemaDigest,
    version: LogicalVersion,
    contains_null: bool,
    canonical_bytes: Arc<[u8]>,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    canonical_digest: [u8; 32],
    membership_index: Option<ResidentMembershipIndex>,
    filter_index: Option<ResidentFilterIndex>,
    range_data: Option<Arc<RangeResidentData>>,
    retained_memory: Option<Arc<ArtifactRetention>>,
}

impl PhysicalArtifact {
    pub(crate) fn new(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
        membership_index: Option<ResidentMembershipIndex>,
    ) -> Self {
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Self {
            kind,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            membership_index,
            filter_index: None,
            range_data: None,
            retained_memory: None,
        }
    }
    pub(crate) const fn kind(&self) -> ArtifactKind {
        self.kind
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
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn canonical_digest(&self) -> [u8; 32] {
        self.canonical_digest
    }
    pub(crate) const fn membership_index(&self) -> Option<&ResidentMembershipIndex> {
        self.membership_index.as_ref()
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn filter_index(&self) -> Option<&ResidentFilterIndex> {
        self.filter_index.as_ref()
    }
    pub(crate) fn with_filter_index(mut self, index: ResidentFilterIndex) -> Self {
        self.filter_index = Some(index);
        self
    }
    pub(crate) fn with_range_data(mut self, data: RangeResidentData) -> Self {
        self.range_data = Some(Arc::new(data));
        self
    }
    pub(crate) fn with_retention(mut self, retention: Arc<ArtifactRetention>) -> Self {
        self.retained_memory = Some(retention);
        self
    }
    pub(crate) fn accounted_resident_component_bytes(
        &self,
    ) -> Result<usize, ArtifactContractError> {
        let index_heap = self
            .membership_index
            .as_ref()
            .map(ResidentMembershipIndex::heap_bytes)
            .transpose()?
            .unwrap_or(0);
        self.canonical_bytes
            .len()
            .checked_add(std::mem::size_of::<Self>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Arc<[u8]>>()))
            .and_then(|bytes| bytes.checked_add(index_heap))
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }
    pub(crate) const fn range_data(&self) -> Option<&Arc<RangeResidentData>> {
        self.range_data.as_ref()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ArtifactBundle {
    channel_id: u32,
    version: LogicalVersion,
    profile_id: ConsumerProfileId,
    artifacts: Box<[(ArtifactKind, Arc<PhysicalArtifact>)]>,
    retained_memory: Option<Arc<ArtifactRetention>>,
}

impl ArtifactBundle {
    pub(crate) fn new(
        channel_id: u32,
        version: LogicalVersion,
        profile: &ConsumerArtifactProfile,
        mut artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)>,
        max_artifact_bytes: usize,
    ) -> Result<Self, ArtifactContractError> {
        if artifacts.is_empty() {
            return Err(ArtifactContractError::EmptyBundle);
        }
        artifacts.sort_unstable_by_key(|(kind, _)| *kind);
        let mut schema = None;
        let mut encoded = 4usize + 32 + 2;
        for (index, (kind, artifact)) in artifacts.iter().enumerate() {
            if index > 0 && artifacts[index - 1].0 == *kind {
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
            encoded = encoded
                .checked_add(9)
                .and_then(|size| size.checked_add(artifact.canonical_bytes().len()))
                .ok_or(ArtifactContractError::LengthOverflow)?;
        }
        if encoded > max_artifact_bytes {
            return Err(ArtifactContractError::EncodedSizeExceeded);
        }
        Ok(Self {
            channel_id,
            version,
            profile_id: profile.id(),
            artifacts: artifacts.into_boxed_slice(),
            retained_memory: None,
        })
    }
    pub(crate) fn accounted_resident_bytes(
        profile: &ConsumerArtifactProfile,
        artifacts: &[(ArtifactKind, Arc<PhysicalArtifact>)],
    ) -> Result<usize, ArtifactContractError> {
        let refs = artifacts
            .len()
            .checked_mul(std::mem::size_of::<(ArtifactKind, Arc<PhysicalArtifact>)>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        let overhead = std::mem::size_of::<Self>()
            .checked_add(profile.canonical_bytes().len())
            .and_then(|bytes| bytes.checked_add(refs))
            .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        artifacts.iter().try_fold(overhead, |bytes, (_, artifact)| {
            bytes
                .checked_add(artifact.accounted_resident_component_bytes()?)
                .ok_or(ArtifactContractError::ResidentSizeOverflow)
        })
    }
    pub(crate) fn new_retained(
        channel_id: u32,
        version: LogicalVersion,
        profile: &ConsumerArtifactProfile,
        mut artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)>,
        max_artifact_bytes: usize,
        retained_memory: Arc<ArtifactRetention>,
    ) -> Result<Self, ArtifactContractError> {
        let expected = Self::accounted_resident_bytes(profile, &artifacts)?;
        if retained_memory.bytes() != expected
            || artifacts.iter().any(|(_, artifact)| {
                !artifact
                    .retained_memory
                    .as_ref()
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, &retained_memory))
            })
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        let mut bundle = Self::new(
            channel_id,
            version,
            profile,
            std::mem::take(&mut artifacts),
            max_artifact_bytes,
        )?;
        bundle.retained_memory = Some(retained_memory);
        Ok(bundle)
    }
    pub(crate) const fn channel_id(&self) -> u32 {
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactContractError {
    EmptyProfile,
    BloomHashContractMismatch,
    LengthOverflow,
    EmptyBundle,
    DuplicateKind,
    KindNotAccepted,
    KindMismatch,
    VersionMismatch,
    SchemaMismatch,
    EncodedSizeExceeded,
    ResidentSizeOverflow,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    InvalidMembershipIndex,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    InvalidResidentFilterIndex,
    RetentionCapacityExceeded,
    RetentionSizeMismatch,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resident_membership_int64_queries_use_the_validated_lower_bound() {
        let bytes = [1_i64, 7, 42]
            .into_iter()
            .flat_map(i64::to_be_bytes)
            .collect::<Vec<_>>();
        let index = ResidentMembershipIndex::Fixed {
            tag: 5,
            values: 0..bytes.len(),
            count: 3,
            width: 8,
        };
        assert_eq!(index.contains_i64(&bytes, 7).unwrap(), Some(true));
        assert_eq!(index.contains_i64(&bytes, 8).unwrap(), Some(false));
        assert_eq!(
            index.range_may_match_i64(&bytes, 8, 41).unwrap(),
            Some(false)
        );
        assert_eq!(
            index.range_may_match_i64(&bytes, 8, 42).unwrap(),
            Some(true)
        );
    }

    #[test]
    fn resident_bitset_rejects_invalid_layout_and_queries_valid_bits() {
        let bytes = vec![0b0000_0101];
        let index = ResidentFilterIndex::Bitset {
            min: 10,
            bit_count: 3,
            bits: 0..1,
        };
        assert_eq!(index.bitset_contains_i64(&bytes, 10).unwrap(), Some(true));
        assert_eq!(index.bitset_contains_i64(&bytes, 11).unwrap(), Some(false));
        assert_eq!(index.bitset_contains_i64(&bytes, 12).unwrap(), Some(true));
        assert!(matches!(
            ResidentFilterIndex::Bitset {
                min: 0,
                bit_count: 9,
                bits: 0..1,
            }
            .bitset_contains_i64(&bytes, 0),
            Err(ArtifactContractError::InvalidResidentFilterIndex)
        ));
    }

    #[test]
    fn retained_artifact_lease_is_shared_and_released_once() {
        let budget = Arc::new(ArtifactRetainedBudget::new(16));
        let retention = Arc::new(budget.try_acquire(12).unwrap());
        let copy = retention.clone();
        assert_eq!(budget.retained_bytes(), 12);
        drop(retention);
        assert_eq!(budget.retained_bytes(), 12);
        drop(copy);
        assert_eq!(budget.retained_bytes(), 0);
    }
}
