// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Provider-owned immutable facts produced after read negotiation.

use std::fmt::Debug;
use std::sync::Arc;

use super::{ColumnHandle, ConnectorReadRelationVersion};
use crate::connector::{ConnectorError, ConnectorErrorKind};

pub const MAX_READ_INPUT_VERSION_BYTES: usize = 4096;
pub const MAX_READ_COVERAGE_EVIDENCE_BYTES: usize = 64 * 1024;
pub const MAX_READ_PROPERTY_KEYS: usize = 1024;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorReadInputVersion(Arc<[u8]>);

impl ConnectorReadInputVersion {
    pub fn try_new(bytes: impl Into<Arc<[u8]>>) -> Result<Self, ConnectorError> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > MAX_READ_INPUT_VERSION_BYTES {
            return Err(invalid(
                "connector read input version must be non-empty and bounded",
            ));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadMetadataKind(Arc<str>);

impl ConnectorReadMetadataKind {
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, ConnectorError> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > 128 || !value.is_ascii() {
            return Err(invalid(
                "connector metadata relation kind must be bounded non-empty ASCII",
            ));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadMetadataVersion {
    Current,
    SnapshotId(i64),
    TimestampMillis(i64),
}

impl ConnectorReadMetadataVersion {
    pub fn try_new(value: Self) -> Result<Self, ConnectorError> {
        match value {
            Self::SnapshotId(id) if id < 0 => Err(invalid(
                "connector metadata snapshot identifier must be non-negative",
            )),
            Self::TimestampMillis(timestamp) if timestamp < 0 => {
                Err(invalid("connector metadata timestamp must be non-negative"))
            }
            value => Ok(value),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadMetadataRequest {
    kind: ConnectorReadMetadataKind,
    version: ConnectorReadMetadataVersion,
}

impl ConnectorReadMetadataRequest {
    pub fn try_new(
        kind: ConnectorReadMetadataKind,
        version: ConnectorReadMetadataVersion,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            kind,
            version: ConnectorReadMetadataVersion::try_new(version)?,
        })
    }

    pub const fn kind(&self) -> &ConnectorReadMetadataKind {
        &self.kind
    }

    pub const fn version(&self) -> ConnectorReadMetadataVersion {
        self.version
    }
}

impl TryFrom<ConnectorReadRelationVersion> for ConnectorReadMetadataVersion {
    type Error = ConnectorError;

    fn try_from(value: ConnectorReadRelationVersion) -> Result<Self, Self::Error> {
        match value {
            ConnectorReadRelationVersion::Current => Ok(Self::Current),
            ConnectorReadRelationVersion::SnapshotId(id) => Self::try_new(Self::SnapshotId(id)),
            ConnectorReadRelationVersion::Reference => Err(invalid(
                "connector metadata request cannot preserve a reference without its name",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadSortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadNullOrdering {
    First,
    Last,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadOrderingKey<C> {
    column: C,
    direction: ConnectorReadSortDirection,
    null_ordering: ConnectorReadNullOrdering,
}

impl<C> ConnectorReadOrderingKey<C> {
    pub const fn new(
        column: C,
        direction: ConnectorReadSortDirection,
        null_ordering: ConnectorReadNullOrdering,
    ) -> Self {
        Self {
            column,
            direction,
            null_ordering,
        }
    }

    pub const fn column(&self) -> &C {
        &self.column
    }

    pub const fn direction(&self) -> ConnectorReadSortDirection {
        self.direction
    }

    pub const fn null_ordering(&self) -> ConnectorReadNullOrdering {
        self.null_ordering
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadPartitionHash {
    XxHash64,
    Murmur3X64_128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadBucketLayout {
    DirectModulo,
    JumpConsistent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorReadPartitionCountDomain {
    min: u32,
    max: u32,
    requires_power_of_two: bool,
}

impl ConnectorReadPartitionCountDomain {
    pub fn try_new(
        min: u32,
        max: u32,
        requires_power_of_two: bool,
    ) -> Result<Self, ConnectorError> {
        if min == 0 || max < min {
            return Err(invalid("connector read partition count domain is empty"));
        }
        if requires_power_of_two
            && min
                .checked_next_power_of_two()
                .is_none_or(|first| first > max)
        {
            return Err(invalid(
                "connector read partition count domain contains no power of two",
            ));
        }
        Ok(Self {
            min,
            max,
            requires_power_of_two,
        })
    }

    pub const fn min(&self) -> u32 {
        self.min
    }

    pub const fn max(&self) -> u32 {
        self.max
    }

    pub const fn requires_power_of_two(&self) -> bool {
        self.requires_power_of_two
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorReadDistribution<C> {
    Unconstrained,
    Singleton,
    RoundRobin,
    Hash {
        keys: Arc<[C]>,
        partition_space: [u8; 32],
        admissible: ConnectorReadPartitionCountDomain,
        algorithm: ConnectorReadPartitionHash,
    },
    BucketShuffle {
        keys: Arc<[C]>,
        partition_space: [u8; 32],
        bucket_count: u32,
        hash: ConnectorReadPartitionHash,
        layout: ConnectorReadBucketLayout,
        ordinal_domain_evidence: [u8; 32],
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadProperties<C> {
    distribution: ConnectorReadDistribution<C>,
    ordering: Arc<[ConnectorReadOrderingKey<C>]>,
}

impl<C: ColumnHandle> ConnectorReadProperties<C> {
    pub fn try_new(
        distribution: ConnectorReadDistribution<C>,
        ordering: impl Into<Arc<[ConnectorReadOrderingKey<C>]>>,
    ) -> Result<Self, ConnectorError> {
        let ordering = ordering.into();
        if ordering.len() > MAX_READ_PROPERTY_KEYS {
            return Err(exhausted("connector read ordering exceeds the hard limit"));
        }
        let distribution_keys = match &distribution {
            ConnectorReadDistribution::Unconstrained
            | ConnectorReadDistribution::Singleton
            | ConnectorReadDistribution::RoundRobin => None,
            ConnectorReadDistribution::Hash {
                keys,
                partition_space,
                ..
            } => {
                if *partition_space == [0; 32] {
                    return Err(invalid("connector read hash partition space is zero"));
                }
                Some(keys)
            }
            ConnectorReadDistribution::BucketShuffle {
                keys,
                partition_space,
                bucket_count,
                ordinal_domain_evidence,
                ..
            } => {
                if *partition_space == [0; 32]
                    || *bucket_count == 0
                    || *ordinal_domain_evidence == [0; 32]
                {
                    return Err(invalid(
                        "connector read bucket distribution evidence is incomplete",
                    ));
                }
                Some(keys)
            }
        };
        if distribution_keys.is_some_and(|keys| {
            keys.is_empty() || keys.len() > MAX_READ_PROPERTY_KEYS || has_duplicate_columns(keys)
        }) {
            return Err(invalid(
                "connector read distribution keys must be non-empty, bounded, and unique",
            ));
        }
        let ordering_columns = ordering
            .iter()
            .map(ConnectorReadOrderingKey::column)
            .cloned()
            .collect::<Vec<_>>();
        if has_duplicate_columns(&ordering_columns) {
            return Err(invalid("connector read ordering repeats a column"));
        }
        Ok(Self {
            distribution,
            ordering,
        })
    }

    pub const fn distribution(&self) -> &ConnectorReadDistribution<C> {
        &self.distribution
    }

    pub fn ordering(&self) -> &[ConnectorReadOrderingKey<C>] {
        &self.ordering
    }
}

fn has_duplicate_columns<C: Ord>(columns: &[C]) -> bool {
    let mut columns = columns.iter().collect::<Vec<_>>();
    columns.sort_unstable();
    columns.windows(2).any(|pair| pair[0] == pair[1])
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorReadArtifactCoverage {
    NoArtifactInputs,
    Exact {
        source_selection_digest: [u8; 32],
        content_digest: [u8; 32],
        evidence: Arc<[u8]>,
    },
}

impl ConnectorReadArtifactCoverage {
    pub fn exact(
        source_selection_digest: [u8; 32],
        content_digest: [u8; 32],
        evidence: impl Into<Arc<[u8]>>,
    ) -> Result<Self, ConnectorError> {
        let evidence = evidence.into();
        if source_selection_digest == [0; 32]
            || content_digest == [0; 32]
            || evidence.is_empty()
            || evidence.len() > MAX_READ_COVERAGE_EVIDENCE_BYTES
        {
            return Err(invalid(
                "connector read artifact coverage must carry bounded non-zero evidence",
            ));
        }
        Ok(Self::Exact {
            source_selection_digest,
            content_digest,
            evidence,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadStaticFacts<C> {
    input_version: ConnectorReadInputVersion,
    selection_digest: [u8; 32],
    properties: ConnectorReadProperties<C>,
    artifact_coverage: ConnectorReadArtifactCoverage,
    coverage_evidence: Arc<[u8]>,
}

impl<C: ColumnHandle> ConnectorReadStaticFacts<C> {
    pub fn try_new(
        input_version: ConnectorReadInputVersion,
        selection_digest: [u8; 32],
        properties: ConnectorReadProperties<C>,
        artifact_coverage: ConnectorReadArtifactCoverage,
        coverage_evidence: impl Into<Arc<[u8]>>,
    ) -> Result<Self, ConnectorError> {
        let coverage_evidence = coverage_evidence.into();
        if selection_digest == [0; 32] {
            return Err(invalid("connector read selection digest must be non-zero"));
        }
        if coverage_evidence.len() > MAX_READ_COVERAGE_EVIDENCE_BYTES {
            return Err(exhausted(
                "connector read coverage evidence exceeds the hard limit",
            ));
        }
        Ok(Self {
            input_version,
            selection_digest,
            properties,
            artifact_coverage,
            coverage_evidence,
        })
    }

    pub const fn input_version(&self) -> &ConnectorReadInputVersion {
        &self.input_version
    }

    pub const fn selection_digest(&self) -> [u8; 32] {
        self.selection_digest
    }

    pub const fn properties(&self) -> &ConnectorReadProperties<C> {
        &self.properties
    }

    pub const fn artifact_coverage(&self) -> &ConnectorReadArtifactCoverage {
        &self.artifact_coverage
    }

    pub fn coverage_evidence(&self) -> &[u8] {
        &self.coverage_evidence
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        ConnectorReadArtifactCoverage, ConnectorReadDistribution, ConnectorReadInputVersion,
        ConnectorReadMetadataKind, ConnectorReadMetadataRequest, ConnectorReadMetadataVersion,
        ConnectorReadProperties, ConnectorReadStaticFacts,
    };
    use crate::connector::read_stack::ConnectorReadColumnHandle;

    #[test]
    fn static_facts_require_real_version_and_selection_identity() {
        assert!(ConnectorReadInputVersion::try_new(Vec::<u8>::new()).is_err());
        let properties = ConnectorReadProperties::<ConnectorReadColumnHandle>::try_new(
            ConnectorReadDistribution::Unconstrained,
            Vec::new(),
        )
        .unwrap();
        assert!(
            ConnectorReadStaticFacts::try_new(
                ConnectorReadInputVersion::try_new(Arc::<[u8]>::from([1_u8; 32])).unwrap(),
                [0; 32],
                properties,
                ConnectorReadArtifactCoverage::NoArtifactInputs,
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn artifact_coverage_requires_both_exact_digests_and_evidence() {
        assert!(ConnectorReadArtifactCoverage::exact([0; 32], [2; 32], vec![3]).is_err());
        assert!(ConnectorReadArtifactCoverage::exact([1; 32], [0; 32], vec![3]).is_err());
        assert!(ConnectorReadArtifactCoverage::exact([1; 32], [2; 32], Vec::new()).is_err());
        assert!(ConnectorReadArtifactCoverage::exact([1; 32], [2; 32], vec![3]).is_ok());
    }

    #[test]
    fn metadata_request_preserves_kind_and_version() {
        let request = ConnectorReadMetadataRequest::try_new(
            ConnectorReadMetadataKind::try_new("$files").unwrap(),
            ConnectorReadMetadataVersion::SnapshotId(42),
        )
        .unwrap();
        assert_eq!(request.kind().as_str(), "$files");
        assert_eq!(
            request.version(),
            ConnectorReadMetadataVersion::SnapshotId(42)
        );
        assert!(
            ConnectorReadMetadataRequest::try_new(
                ConnectorReadMetadataKind::try_new("$files").unwrap(),
                ConnectorReadMetadataVersion::TimestampMillis(-1),
            )
            .is_err()
        );
    }
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}
