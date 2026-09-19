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

//! Freezing one MV publication document from the facts a refresh actually has.
//!
//! P is written in two moments, because its facts become true in two moments.
//! Its input watermark is known before the write runs -- it is exactly the set
//! of source revisions the refresh pinned -- while its result is known only
//! after the writers close. Freezing the watermark first and completing it with
//! the executed result keeps each half sourced from the moment that can state
//! it, and leaves the output data version to the provider, which binds it at
//! the commit itself.

use std::collections::BTreeMap;

use crate::persistence::codec::{
    DefinitionDocument, InterpretationDocument, PublicationDocument, PublicationInput,
    PublicationKind, PublicationOutput, PublicationStatistics,
};
use crate::persistence::definition::MvAcceleratorSourceRevision;
use crate::persistence::exact_revision::persist_exact_connector_revision;
use crate::persistence::identity::PublicationIdentity;
use crate::persistence::validation::validate_publication;
use novarocks_spi::connector::{ConnectorExactSemanticRevision, LakePublicationId};

/// One refresh's complete input watermark, in D's own occurrence order.
///
/// The order and multiplicity are D's, not a table's: a self-join occupies two
/// occurrences of one physical object and records two inputs, because the
/// publication has to be able to say what each occurrence read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvPublicationInputs {
    inputs: Vec<PublicationInput>,
}

impl MvPublicationInputs {
    /// Freeze the watermark from the exact revision each D occurrence was
    /// pinned at.
    ///
    /// Every occurrence must be present and must still name the object D
    /// bound: a refresh that read something else is not a refresh of this
    /// definition, and saying so here is cheaper than discovering it when the
    /// document set is validated.
    pub fn freeze(
        definition: &DefinitionDocument,
        revision_by_occurrence: &BTreeMap<u32, ConnectorExactSemanticRevision>,
    ) -> Result<Self, String> {
        if revision_by_occurrence.len() != definition.relation_occurrences.len() {
            return Err(format!(
                "MV publication needs one pinned revision per definition occurrence, but {} \
                 occurrences were pinned for {} occurrences",
                revision_by_occurrence.len(),
                definition.relation_occurrences.len(),
            ));
        }
        let inputs = definition
            .relation_occurrences
            .iter()
            .map(|occurrence| {
                let revision = revision_by_occurrence
                    .get(&occurrence.occurrence_id)
                    .ok_or_else(|| {
                        format!(
                            "MV publication has no pinned revision for definition occurrence {}",
                            occurrence.occurrence_id,
                        )
                    })?;
                let (object_id, native_data_version) = persist_exact_connector_revision(revision)
                    .map_err(|error| {
                    format!(
                        "persist the exact revision of definition occurrence {}: {error}",
                        occurrence.occurrence_id,
                    )
                })?;
                if object_id != occurrence.object_id {
                    return Err(format!(
                        "MV publication read a different object than definition occurrence {} \
                         binds",
                        occurrence.occurrence_id,
                    ));
                }
                Ok(PublicationInput {
                    relation_occurrence_id: occurrence.occurrence_id,
                    object_id,
                    native_data_version,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self { inputs })
    }

    pub fn inputs(&self) -> &[PublicationInput] {
        &self.inputs
    }
}

/// What one executed refresh write produced, before its commit is attempted.
///
/// These are the writers' own accepted facts, not the provider's: the commit
/// has not happened yet, and the output data version it will mint is the
/// provider's to attach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MvPublicationResult {
    pub kind: PublicationKind,
    pub logical_result_rows: u64,
}

/// Complete P from its frozen watermark and the write's executed result.
pub fn freeze_publication_document(
    source_revision: &MvAcceleratorSourceRevision,
    interpretation: &InterpretationDocument,
    publication_id: LakePublicationId,
    inputs: &MvPublicationInputs,
    result: MvPublicationResult,
    prepared_at_ms: u64,
) -> Result<PublicationDocument, String> {
    let document = PublicationDocument {
        publication_prepared_at_ms: prepared_at_ms,
        publication_id: PublicationIdentity::try_new(publication_id.to_bytes().to_vec())
            .map_err(|error| format!("build the MV publication identity: {error}"))?,
        definition_revision: source_revision.definition_revision,
        interpretation_revision: source_revision.interpretation_revision,
        inputs: inputs.inputs.clone(),
        output: PublicationOutput {
            object_id: interpretation.target.object_id.clone(),
            empty_result: result.logical_result_rows == 0,
        },
        kind: result.kind,
        statistics: PublicationStatistics {
            logical_result_rows: Some(result.logical_result_rows),
            // The rows a refresh consumed are not a fact any writer reports;
            // recording an output count here would be an invention.
            processed_input_rows: None,
        },
    };
    validate_publication(&document)
        .map_err(|error| format!("frozen MV publication document is invalid: {error}"))?;
    Ok(document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::documents::publication_document_set;
    use crate::persistence::test_support::sample_projection;
    use crate::product::MvTarget;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorProviderId, ConnectorTableObjectId, LakePublicationId,
    };

    /// The fixture definition is a self-join: two occurrences of one physical
    /// object, which is precisely what a table-name-keyed watermark loses.
    fn projection() -> crate::persistence::projection::MvDocumentProjection {
        sample_projection(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(1))
    }

    fn revision(object_value: u8, snapshot_id: i64) -> ConnectorExactSemanticRevision {
        let object = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(&[object_value]))
            .expect("source object");
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").expect("provider"),
            &object,
            Some(snapshot_id),
        )
        .expect("source revision")
    }

    /// The fixture's occurrences are bound at snapshot 9, so reading them
    /// there is what "unchanged sources" looks like.
    fn pinned_at_binding() -> BTreeMap<u32, ConnectorExactSemanticRevision> {
        BTreeMap::from([(7, revision(11, 9)), (8, revision(11, 9))])
    }

    #[test]
    fn a_self_join_records_one_input_per_occurrence() {
        let projection = projection();
        let inputs = MvPublicationInputs::freeze(projection.definition(), &pinned_at_binding())
            .expect("every occurrence is pinned");

        assert_eq!(
            inputs
                .inputs()
                .iter()
                .map(|input| input.relation_occurrence_id)
                .collect::<Vec<_>>(),
            vec![7, 8],
            "both occurrences of one object must survive"
        );
        assert_eq!(inputs.inputs()[0].object_id, inputs.inputs()[1].object_id);
    }

    #[test]
    fn an_unpinned_occurrence_fails_closed() {
        let projection = projection();
        let error = MvPublicationInputs::freeze(
            projection.definition(),
            &BTreeMap::from([(7, revision(11, 9))]),
        )
        .expect_err("a partial watermark cannot describe this refresh");

        assert!(
            error.contains("one pinned revision per definition occurrence"),
            "{error}"
        );
    }

    #[test]
    fn an_occurrence_read_at_another_object_fails_closed() {
        let projection = projection();
        let error = MvPublicationInputs::freeze(
            projection.definition(),
            &BTreeMap::from([(7, revision(11, 9)), (8, revision(12, 9))]),
        )
        .expect_err("a refresh that read elsewhere is not a refresh of this definition");

        assert!(
            error.contains("different object than definition occurrence 8"),
            "{error}"
        );
    }

    #[test]
    fn a_frozen_publication_completes_its_own_document_set() {
        let projection = projection();
        let inputs = MvPublicationInputs::freeze(projection.definition(), &pinned_at_binding())
            .expect("every occurrence is pinned");
        let source_revision = projection.source_revision();
        let publication = freeze_publication_document(
            &source_revision,
            projection.interpretation(),
            LakePublicationId::try_from_uuid(uuid::Uuid::now_v7()).expect("publication id"),
            &inputs,
            MvPublicationResult {
                kind: PublicationKind::FullRefresh,
                logical_result_rows: 3,
            },
            1_700_000_020_000,
        )
        .expect("the frozen publication is valid");

        assert_eq!(publication.statistics.logical_result_rows, Some(3));
        assert!(!publication.output.empty_result);
        publication_document_set(
            projection.definition(),
            projection.interpretation(),
            &publication,
        )
        .expect("P references the exact D/L it was frozen against");
    }

    #[test]
    fn a_publication_that_materialized_no_row_says_so() {
        let projection = projection();
        let inputs = MvPublicationInputs::freeze(projection.definition(), &pinned_at_binding())
            .expect("every occurrence is pinned");
        let source_revision = projection.source_revision();
        let publication = freeze_publication_document(
            &source_revision,
            projection.interpretation(),
            LakePublicationId::try_from_uuid(uuid::Uuid::now_v7()).expect("publication id"),
            &inputs,
            MvPublicationResult {
                kind: PublicationKind::IncrementalRefresh,
                logical_result_rows: 0,
            },
            1_700_000_020_000,
        )
        .expect("an empty result is a valid publication");

        assert!(publication.output.empty_result);
        assert_eq!(publication.statistics.logical_result_rows, Some(0));
    }
}
