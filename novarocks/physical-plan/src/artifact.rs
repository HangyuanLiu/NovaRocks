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

use crate::{
    ArtifactRefId, IdentityError, NullOrdering, ProviderReadReference, SortDirection, ValueId,
    ValueType, stable_identity,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactKind(Box<str>);

impl ArtifactKind {
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        stable_identity("artifact kind", value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactFormatId(Box<str>);

impl ArtifactFormatId {
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        stable_identity("artifact format", value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactFormat {
    pub id: ArtifactFormatId,
    pub revision: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageRange {
    /// Inclusive canonical boundary. `None` means negative infinity.
    pub start: Option<Box<[u8]>>,
    /// Exclusive canonical boundary. `None` means positive infinity.
    pub end: Option<Box<[u8]>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageSet {
    pub domain: Box<str>,
    /// Exact frozen selection whose coverage is described by these ranges.
    pub selection_digest: [u8; 32],
    /// Strictly ordered, non-overlapping independent publication ranges.
    pub ranges: Box<[CoverageRange]>,
    /// Proves that the ranges cover the complete frozen input.
    pub complete_input: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactSortKey {
    pub value: ValueId,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArtifactInputField {
    pub value: ValueId,
    pub ty: ValueType,
}

/// Exact sealed artifact contract required by a relation.
#[derive(Clone, Debug, PartialEq)]
pub struct ArtifactInputRequirement {
    pub artifact: ArtifactRefId,
    pub kind: ArtifactKind,
    pub format: ArtifactFormat,
    pub schema: Box<[ValueType]>,
    pub source: ArtifactSourceBinding,
    pub required_coverage: CoverageSet,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ArtifactSourceBinding {
    pub source: ProviderReadReference,
    /// Exact selection/coverage evidence used to derive this artifact.
    pub selection_digest: [u8; 32],
}

/// Static requirements for one managed sealed-artifact sink.
#[derive(Clone, Debug, PartialEq)]
pub struct SealedArtifactSinkSpec {
    pub kind: ArtifactKind,
    pub format: ArtifactFormat,
    pub input: Box<[ArtifactInputField]>,
    pub partition_by: Box<[ValueId]>,
    pub order_by: Box<[ArtifactSortKey]>,
    pub group_boundaries: Box<[ValueId]>,
    pub source: ArtifactSourceBinding,
    pub required_coverage: CoverageSet,
    pub max_reference_bytes: u32,
}

/// Immutable reference delivered only after the represented range is sealed.
#[derive(Clone, Debug, PartialEq)]
pub struct SealedArtifactRef {
    pub id: ArtifactRefId,
    pub kind: ArtifactKind,
    pub format: ArtifactFormat,
    pub schema: Box<[ValueType]>,
    pub source: ArtifactSourceBinding,
    pub coverage: CoverageSet,
    pub location: Box<str>,
    pub content_digest: [u8; 32],
    pub schema_digest: [u8; 32],
    pub object_count: u64,
    pub row_count: u64,
}
