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

use novarocks_connector_contract::{
    ConnectorEncodedPayload, ConnectorReadBinding, ConnectorReadRelationPayload,
    ConnectorReadWorkSource,
};

use crate::{
    ArtifactInputRequirement, ArtifactSourceBinding, ExprId, IdentityError, PhysicalProperties,
    ValueType, stable_identity,
};

pub use novarocks_connector_contract::ConnectorReadInputVersion as ExactInputVersion;

/// Frozen, no-I/O provider relation reference.
///
/// The relation contains distinct provider-private table and read-view values
/// with public exact binding headers. Runtime handles and provider instances
/// are resolved later by the execution application from this immutable
/// reference.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProviderReadReference {
    pub binding: ConnectorReadBinding,
    pub input_version: ExactInputVersion,
    pub relation: ConnectorReadRelationPayload,
}

/// Exact provider column identity associated with the same frozen relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderColumnReference {
    pub column_payload: ConnectorEncodedPayload,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelationField {
    pub column: ProviderColumnReference,
    pub ty: ValueType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PredicateGuaranteeKind {
    /// Every returned row satisfies the predicate.
    Exact,
    /// The predicate only prunes candidate work; execution must evaluate it.
    PruningOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PredicateGuarantee {
    pub predicate: ExprId,
    pub kind: PredicateGuaranteeKind,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataRelation {
    pub read: ProviderReadReference,
    /// Whether execution receives provider splits or opens the whole relation.
    pub work_source: ConnectorReadWorkSource,
    /// Digest of the exact frozen row/split selection represented by this scan.
    pub selection_digest: [u8; 32],
    pub schema: Box<[RelationField]>,
    /// Provider guarantees for exact scan-owned predicate occurrences.
    /// `PruningOnly` never transfers row-level evaluation responsibility.
    pub predicate_guarantees: Box<[PredicateGuarantee]>,
    pub provided_properties: PhysicalProperties,
    pub artifact_inputs: Box<[ArtifactInputRequirement]>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataRelationKind(Box<str>);

impl MetadataRelationKind {
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        stable_identity("metadata relation kind", value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Closed source contract for provider metadata discovery.
///
/// `coverage_evidence` is provider-defined immutable evidence for the exact
/// source coverage represented by this relation. It does not grant I/O or
/// publication authority.
#[derive(Clone, Debug, PartialEq)]
pub struct MetadataRelation {
    pub kind: MetadataRelationKind,
    pub read: ProviderReadReference,
    /// Whether execution receives provider splits or opens the whole relation.
    pub work_source: ConnectorReadWorkSource,
    /// Digest of the exact frozen metadata selection represented by this scan.
    pub selection_digest: [u8; 32],
    pub schema: Box<[RelationField]>,
    /// Provider guarantees for exact scan-owned predicate occurrences.
    /// `PruningOnly` never transfers row-level evaluation responsibility.
    pub predicate_guarantees: Box<[PredicateGuarantee]>,
    pub provided_properties: PhysicalProperties,
    pub coverage_evidence: Box<[u8]>,
    pub artifact_inputs: Box<[ArtifactInputRequirement]>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Relation {
    Data(DataRelation),
    Metadata(MetadataRelation),
}

impl Relation {
    pub const fn read(&self) -> &ProviderReadReference {
        match self {
            Self::Data(relation) => &relation.read,
            Self::Metadata(relation) => &relation.read,
        }
    }

    pub fn schema(&self) -> &[RelationField] {
        match self {
            Self::Data(relation) => &relation.schema,
            Self::Metadata(relation) => &relation.schema,
        }
    }

    pub fn predicate_guarantees(&self) -> &[PredicateGuarantee] {
        match self {
            Self::Data(relation) => &relation.predicate_guarantees,
            Self::Metadata(relation) => &relation.predicate_guarantees,
        }
    }

    /// How execution obtains work for this exact frozen relation.
    ///
    /// `WholeRelation` means one executor opens the provider relation directly;
    /// it is not an implicit one-split fallback.
    pub const fn work_source(&self) -> ConnectorReadWorkSource {
        match self {
            Self::Data(relation) => relation.work_source,
            Self::Metadata(relation) => relation.work_source,
        }
    }

    pub fn provided_properties(&self) -> &PhysicalProperties {
        match self {
            Self::Data(relation) => &relation.provided_properties,
            Self::Metadata(relation) => &relation.provided_properties,
        }
    }

    pub fn artifact_inputs(&self) -> &[ArtifactInputRequirement] {
        match self {
            Self::Data(relation) => &relation.artifact_inputs,
            Self::Metadata(relation) => &relation.artifact_inputs,
        }
    }

    pub fn source_binding(&self) -> ArtifactSourceBinding {
        match self {
            Self::Data(relation) => ArtifactSourceBinding {
                source: relation.read.clone(),
                selection_digest: relation.selection_digest,
            },
            Self::Metadata(relation) => ArtifactSourceBinding {
                source: relation.read.clone(),
                selection_digest: relation.selection_digest,
            },
        }
    }
}
