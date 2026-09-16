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

//! Validated, immutable product facts reconstructed only from exact lake documents.

use novarocks_spi::connector::{ConnectorCommittedVersion, ConnectorTableObjectId};

use super::codec::{
    ConfigurationDocument, DefinitionDocument, InterpretationDocument, PublicationDocument,
    encode_configuration, encode_definition, encode_interpretation, encode_publication,
};
use super::definition::{MvAcceleratorCommittedVersionRevision, MvAcceleratorSourceRevision};
use super::documents::MvObservedCurrentDocuments;
use super::identity::{DocumentRevision, ObjectIdentity};
use super::validation::{validate_definition_interpretation, validate_document_set};
use crate::product::MvTarget;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvDocumentProjection {
    source_revision: MvAcceleratorSourceRevision,
    metadata_version: ConnectorCommittedVersion,
    target: MvTarget,
    definition: DefinitionDocument,
    interpretation: InterpretationDocument,
    configuration: ConfigurationDocument,
    publication: MvPublicationState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvPublicationState {
    NeverPublished,
    Published(MvPublishedFacts),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvPublishedFacts {
    document: PublicationDocument,
    revision: DocumentRevision,
    output_version: ConnectorCommittedVersion,
    storage_rows: Option<u64>,
}

impl MvPublishedFacts {
    pub fn document(&self) -> &PublicationDocument {
        &self.document
    }
    pub fn revision(&self) -> DocumentRevision {
        self.revision
    }
    pub fn output_version(&self) -> &ConnectorCommittedVersion {
        &self.output_version
    }
    pub fn storage_rows(&self) -> Option<u64> {
        self.storage_rows
    }
}

/// Storage rows must be projected from the same exact output as P's attachment.
/// Unknown storage statistics are absent, never inferred from logical rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvOutputStatistics {
    pub object_id: ConnectorTableObjectId,
    pub output_version: ConnectorCommittedVersion,
    pub storage_rows: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredMvProjection {
    pub mv_id: i64,
    pub facts: MvDocumentProjection,
}

impl MvDocumentProjection {
    /// Accepts only a sealed Current observation decoded by the MV document owner.
    pub fn try_from_current(
        observed: MvObservedCurrentDocuments,
        statistics: Option<MvOutputStatistics>,
    ) -> Result<Self, String> {
        let source_revision = observed.source_revision();
        if observed.publication.is_some() != observed.publication_output_version.is_some() {
            return Err("MV observation publication/output presence differ".into());
        }
        if let Some(statistics) = &statistics {
            if statistics.object_id != observed.target_object_id
                || observed.publication_output_version.as_ref() != Some(&statistics.output_version)
            {
                return Err(
                    "MV storage statistics do not belong to the exact publication output".into(),
                );
            }
        }
        Self::try_from_parts(
            source_revision,
            observed.metadata_version,
            observed.definition,
            observed.interpretation,
            observed.configuration,
            observed
                .publication
                .zip(observed.publication_output_version),
            statistics.map(|statistics| statistics.storage_rows),
        )
    }

    /// Cache reconstruction is private to the product and repeats all checks.
    pub(crate) fn try_from_parts(
        source: MvAcceleratorSourceRevision,
        metadata_version: ConnectorCommittedVersion,
        definition: DefinitionDocument,
        interpretation: InterpretationDocument,
        configuration: ConfigurationDocument,
        publication: Option<(PublicationDocument, ConnectorCommittedVersion)>,
        storage_rows: Option<u64>,
    ) -> Result<Self, String> {
        metadata_version
            .validate()
            .map_err(|error| error.to_string())?;
        let encoded_definition =
            encode_definition(&definition).map_err(|error| error.to_string())?;
        let encoded_interpretation =
            encode_interpretation(&interpretation).map_err(|error| error.to_string())?;
        let encoded_configuration =
            encode_configuration(&configuration).map_err(|error| error.to_string())?;
        if source.definition_revision != encoded_definition.revision()
            || source.interpretation_revision != encoded_interpretation.revision()
            || source.configuration_revision != encoded_configuration.revision()
            || source.metadata_version
                != MvAcceleratorCommittedVersionRevision::from_committed(&metadata_version)
        {
            return Err(
                "MV source revision does not match its canonical documents or metadata".into(),
            );
        }
        validate_definition_interpretation(
            &definition,
            source.definition_revision,
            &interpretation,
        )
        .map_err(|error| error.to_string())?;
        if interpretation.target.object_id.as_bytes() != source.target_object_id.as_bytes().as_ref()
        {
            return Err("MV interpretation binds a different exact target object".into());
        }
        let publication = match publication {
            Some((document, output_version)) => {
                output_version
                    .validate()
                    .map_err(|error| error.to_string())?;
                let revision = encode_publication(&document)
                    .map_err(|error| error.to_string())?
                    .revision();
                if source.publication_revision != Some(revision)
                    || source.publication_output_version.as_ref()
                        != Some(&MvAcceleratorCommittedVersionRevision::from_committed(
                            &output_version,
                        ))
                {
                    return Err(
                        "MV publication source revision does not match its exact output".into(),
                    );
                }
                validate_document_set(
                    &definition,
                    source.definition_revision,
                    &interpretation,
                    source.interpretation_revision,
                    &document,
                )
                .map_err(|error| error.to_string())?;
                MvPublicationState::Published(MvPublishedFacts {
                    document,
                    revision,
                    output_version,
                    storage_rows,
                })
            }
            None => {
                // The active document provider is snapshot-oriented. A sealed metadata
                // observation with no current output is the no-publication witness.
                // Other provider models must supply an explicit typed witness before
                // being admitted; a missing P beside a current output is corruption.
                if metadata_version.snapshot_id().is_some()
                    || source.publication_revision.is_some()
                    || source.publication_output_version.is_some()
                    || storage_rows.is_some()
                {
                    return Err(
                        "missing MV publication is not a proven never-published target".into(),
                    );
                }
                MvPublicationState::NeverPublished
            }
        };
        let target = MvTarget::try_new(
            Some(source.target.instance_id.as_str().to_owned()),
            source.target.namespace.to_string(),
            source.target.table.to_string(),
        )
        .map_err(|error| error.to_string())?;
        Ok(Self {
            source_revision: source,
            metadata_version,
            target,
            definition,
            interpretation,
            configuration,
            publication,
        })
    }

    pub fn source_revision(&self) -> &MvAcceleratorSourceRevision {
        &self.source_revision
    }
    pub fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }
    pub fn target(&self) -> &MvTarget {
        &self.target
    }
    pub fn definition(&self) -> &DefinitionDocument {
        &self.definition
    }
    pub fn interpretation(&self) -> &InterpretationDocument {
        &self.interpretation
    }
    pub fn configuration(&self) -> &ConfigurationDocument {
        &self.configuration
    }
    pub fn publication(&self) -> &MvPublicationState {
        &self.publication
    }

    /// Lossless occurrence inventory; name equality never merges source objects.
    pub fn dependencies(&self) -> Vec<MvProjectionDependency> {
        self.definition
            .relation_occurrences
            .iter()
            .map(|occurrence| MvProjectionDependency {
                occurrence_id: occurrence.occurrence_id,
                catalog: occurrence.catalog_at_binding.clone(),
                namespace: occurrence.namespace_at_binding.clone(),
                relation: occurrence.relation_at_binding.clone(),
                object_id: occurrence.object_id.clone(),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvProjectionDependency {
    pub occurrence_id: u32,
    pub catalog: String,
    pub namespace: String,
    pub relation: String,
    pub object_id: ObjectIdentity,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::test_support::{observed_current, sample_projection};
    use novarocks_spi::connector::{CatalogHandle, CatalogVersion, ConnectorInstanceId};

    #[test]
    fn storage_statistics_require_exact_target_object_and_complete_output_payload() {
        let facts = sample_projection(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(11));
        let catalog = CatalogHandle::new(
            ConnectorInstanceId::parse("ice").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        );
        let MvPublicationState::Published(published) = facts.publication() else {
            panic!("published fixture")
        };
        let exact = MvOutputStatistics {
            object_id: facts.source_revision().target_object_id.clone(),
            output_version: published.output_version().clone(),
            storage_rows: 10,
        };
        assert!(
            MvDocumentProjection::try_from_current(
                observed_current(&facts, catalog.clone()),
                Some(exact.clone())
            )
            .is_ok()
        );
        let mut wrong = exact.clone();
        wrong.object_id =
            ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"another-object")).unwrap();
        assert!(
            MvDocumentProjection::try_from_current(
                observed_current(&facts, catalog.clone()),
                Some(wrong)
            )
            .is_err()
        );
        let mut wrong = exact;
        wrong.output_version = ConnectorCommittedVersion::try_new(
            bytes::Bytes::from_static(b"same-snapshot-different-payload"),
            Some(11),
        )
        .unwrap();
        assert!(
            MvDocumentProjection::try_from_current(observed_current(&facts, catalog), Some(wrong))
                .is_err()
        );
    }
}
