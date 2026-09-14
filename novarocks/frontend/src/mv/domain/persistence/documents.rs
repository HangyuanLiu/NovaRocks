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

//! Frontend-owned adapter between MV D/L/P/C and opaque Connector documents.
//!
//! The Connector owns atomic storage and exact attachment points. The MV
//! application owns these names, formats, codecs, references, and the rule
//! that a Current document set must be internally complete. There is no
//! descriptor-property or provider-token fallback in this adapter.

use std::collections::BTreeMap;

use bytes::Bytes;
use novarocks_mv_application::persistence::codec::{
    ConfigurationDocument, DefinitionDocument, EncodedDocument, InterpretationDocument,
    PersistenceCodecError, PublicationDocument, decode_configuration, decode_definition,
    decode_interpretation, decode_publication, encode_configuration, encode_definition,
    encode_interpretation, encode_publication,
};
use novarocks_mv_application::persistence::identity::DocumentRevision;
use novarocks_mv_application::persistence::validation::{
    PersistenceDecodeBudget, ValidationError, validate_document_set,
};
use novarocks_spi::connector::document_storage::{
    ConnectorDocument, ConnectorDocumentAttachment, ConnectorDocumentFormat, ConnectorDocumentId,
    ConnectorDocumentName, ConnectorDocumentOwner, ConnectorDocumentReference,
    ConnectorDocumentRevision, ConnectorDocumentSet, ConnectorStoredDocument,
    ConnectorStoredDocumentAttachment,
};
use novarocks_spi::connector::{ConnectorPreparedCreateDocumentTarget, ConnectorTableObjectId};

const OWNER: &str = "novarocks.mv";
const DEFINITION: &str = "definition";
const INTERPRETATION: &str = "interpretation";
const PUBLICATION: &str = "publication";
const CONFIGURATION: &str = "configuration";
const REFERENCES_DEFINITION: &str = "definition";
const REFERENCES_INTERPRETATION: &str = "interpretation";
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvDecodedDocuments {
    pub definition: DefinitionDocument,
    pub definition_revision: DocumentRevision,
    pub interpretation: InterpretationDocument,
    pub interpretation_revision: DocumentRevision,
    pub publication: Option<PublicationDocument>,
    pub configuration: ConfigurationDocument,
}

#[derive(Debug)]
pub(crate) enum MvDocumentError {
    Codec(PersistenceCodecError),
    Contract(String),
    Connector(novarocks_spi::connector::ConnectorError),
    Validation(ValidationError),
}

impl std::fmt::Display for MvDocumentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Codec(error) => write!(formatter, "MV document codec failed: {error}"),
            Self::Contract(message) => write!(formatter, "invalid MV document contract: {message}"),
            Self::Connector(error) => {
                write!(formatter, "MV document storage contract failed: {error}")
            }
            Self::Validation(error) => write!(formatter, "MV document validation failed: {error}"),
        }
    }
}

impl std::error::Error for MvDocumentError {}

impl From<PersistenceCodecError> for MvDocumentError {
    fn from(value: PersistenceCodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<novarocks_spi::connector::ConnectorError> for MvDocumentError {
    fn from(value: novarocks_spi::connector::ConnectorError) -> Self {
        Self::Connector(value)
    }
}

impl From<ValidationError> for MvDocumentError {
    fn from(value: ValidationError) -> Self {
        Self::Validation(value)
    }
}

/// Creates the immutable D/L and independently mutable C document set used by
/// invisible target creation. All three documents attach to table metadata.
pub(crate) fn create_document_set(
    definition: &DefinitionDocument,
    interpretation: &InterpretationDocument,
    configuration: &ConfigurationDocument,
    target: &ConnectorPreparedCreateDocumentTarget,
) -> Result<ConnectorDocumentSet, MvDocumentError> {
    validate_create_target(interpretation, target)?;
    let definition = encode_definition(definition)?;
    if interpretation.definition_revision != definition.revision()
        || interpretation.computation_identity
            != definition_document_identity(definition.as_bytes())?
    {
        return Err(MvDocumentError::Contract(
            "interpretation does not bind the exact encoded definition".to_string(),
        ));
    }
    let interpretation = encode_interpretation(interpretation)?;
    let configuration = encode_configuration(configuration)?;

    let definition = connector_document(
        DEFINITION,
        definition,
        Vec::new(),
        ConnectorDocumentAttachment::TableMetadata,
    )?;
    let interpretation_reference =
        ConnectorDocumentReference::try_new(REFERENCES_DEFINITION, definition.id().clone())?;
    let interpretation = connector_document(
        INTERPRETATION,
        interpretation,
        vec![interpretation_reference],
        ConnectorDocumentAttachment::TableMetadata,
    )?;
    let configuration = connector_document(
        CONFIGURATION,
        configuration,
        Vec::new(),
        ConnectorDocumentAttachment::TableMetadata,
    )?;
    ConnectorDocumentSet::try_new(vec![definition, interpretation, configuration])
        .map_err(Into::into)
}

/// Creates the P-only replacement used at the atomic data commit. P retains
/// exact historical D/L references and asks the provider to bind its output
/// data version at the same commit point.
pub(crate) fn publication_document_set(
    definition: &DefinitionDocument,
    interpretation: &InterpretationDocument,
    publication: &PublicationDocument,
) -> Result<ConnectorDocumentSet, MvDocumentError> {
    let definition = encode_definition(definition)?;
    let interpretation = encode_interpretation(interpretation)?;
    let publication_encoded = encode_publication(publication)?;
    validate_document_set(
        definition_document(definition.as_bytes())?.as_ref(),
        definition.revision(),
        interpretation_document(interpretation.as_bytes())?.as_ref(),
        interpretation.revision(),
        publication,
    )?;

    let definition_id = document_id(DEFINITION, definition.revision())?;
    let interpretation_id = document_id(INTERPRETATION, interpretation.revision())?;
    let references = vec![
        ConnectorDocumentReference::try_new(REFERENCES_DEFINITION, definition_id)?,
        ConnectorDocumentReference::try_new(REFERENCES_INTERPRETATION, interpretation_id)?,
    ];
    let publication = connector_document(
        PUBLICATION,
        publication_encoded,
        references,
        ConnectorDocumentAttachment::CommitOutput,
    )?;
    ConnectorDocumentSet::try_new(vec![publication]).map_err(Into::into)
}

/// Decodes one exact Current set. Deferred bodies must be loaded by the
/// Connector lease before entering this application boundary.
pub(crate) fn decode_current_documents(
    documents: &[ConnectorStoredDocument],
    budget: PersistenceDecodeBudget,
) -> Result<MvDecodedDocuments, MvDocumentError> {
    let mut by_name = BTreeMap::new();
    for document in documents {
        validate_envelope(document)?;
        if by_name
            .insert(document.id().name().as_str(), document)
            .is_some()
        {
            return Err(MvDocumentError::Contract(
                "Current contains duplicate MV document names".to_string(),
            ));
        }
    }
    let expected = if by_name.contains_key(PUBLICATION) {
        4
    } else {
        3
    };
    if by_name.len() != expected {
        return Err(MvDocumentError::Contract(
            "Current must contain exactly D/L/C and optional P".to_string(),
        ));
    }

    let definition_stored = required_document(&by_name, DEFINITION)?;
    let interpretation_stored = required_document(&by_name, INTERPRETATION)?;
    let configuration_stored = required_document(&by_name, CONFIGURATION)?;
    require_attachment(definition_stored, false)?;
    require_attachment(interpretation_stored, false)?;
    require_attachment(configuration_stored, false)?;

    let definition = decode_definition(available_content(definition_stored)?, budget)?;
    let interpretation = decode_interpretation(available_content(interpretation_stored)?, budget)?;
    let configuration = decode_configuration(available_content(configuration_stored)?, budget)?;
    let definition_revision = revision(definition_stored);
    let interpretation_revision = revision(interpretation_stored);
    require_exact_reference(
        interpretation_stored,
        REFERENCES_DEFINITION,
        definition_stored.id(),
    )?;

    let publication = match by_name.get(PUBLICATION) {
        Some(stored) => {
            require_attachment(stored, true)?;
            require_exact_reference(stored, REFERENCES_DEFINITION, definition_stored.id())?;
            require_exact_reference(
                stored,
                REFERENCES_INTERPRETATION,
                interpretation_stored.id(),
            )?;
            let publication = decode_publication(available_content(stored)?, budget)?;
            validate_document_set(
                &definition,
                definition_revision,
                &interpretation,
                interpretation_revision,
                &publication,
            )?;
            Some(publication)
        }
        None => {
            if interpretation.definition_revision != definition_revision
                || interpretation.computation_identity != definition.computation_identity
            {
                return Err(MvDocumentError::Contract(
                    "Current L does not bind exact Current D".to_string(),
                ));
            }
            None
        }
    };

    Ok(MvDecodedDocuments {
        definition,
        definition_revision,
        interpretation,
        interpretation_revision,
        publication,
        configuration,
    })
}

fn validate_create_target(
    interpretation: &InterpretationDocument,
    target: &ConnectorPreparedCreateDocumentTarget,
) -> Result<(), MvDocumentError> {
    if interpretation.target.object_id.as_bytes() != target.object_id().as_bytes().as_ref()
        || interpretation.target.schema_version.as_bytes() != target.schema_version().as_ref()
        || interpretation.target.partition_spec_version.as_bytes()
            != target.partition_spec_version().as_ref()
    {
        return Err(MvDocumentError::Contract(
            "interpretation target does not match the provider-prepared target".to_string(),
        ));
    }
    let target_fields = interpretation
        .target
        .fields
        .iter()
        .map(|field| field.target_field_id.as_bytes())
        .collect::<std::collections::BTreeSet<_>>();
    let prepared_fields = target
        .fields()
        .iter()
        .map(|field| field.provider_field_id().as_ref())
        .collect::<std::collections::BTreeSet<_>>();
    if target_fields != prepared_fields {
        return Err(MvDocumentError::Contract(
            "interpretation target fields do not match provider-prepared field bindings"
                .to_string(),
        ));
    }
    Ok(())
}

fn connector_document(
    name: &'static str,
    encoded: EncodedDocument,
    references: Vec<ConnectorDocumentReference>,
    attachment: ConnectorDocumentAttachment,
) -> Result<ConnectorDocument, MvDocumentError> {
    ConnectorDocument::try_new(
        owner()?,
        document_name(name)?,
        document_format(name)?,
        Bytes::from(encoded.into_bytes()),
        references,
        attachment,
    )
    .map_err(Into::into)
}

fn document_id(
    name: &'static str,
    revision: DocumentRevision,
) -> Result<ConnectorDocumentId, MvDocumentError> {
    Ok(ConnectorDocumentId::new(
        owner()?,
        document_name(name)?,
        ConnectorDocumentRevision::from_bytes(*revision.as_bytes()),
    ))
}

fn owner() -> Result<ConnectorDocumentOwner, MvDocumentError> {
    ConnectorDocumentOwner::parse(OWNER).map_err(Into::into)
}

fn document_name(name: &'static str) -> Result<ConnectorDocumentName, MvDocumentError> {
    ConnectorDocumentName::parse(name).map_err(Into::into)
}

fn document_format(name: &'static str) -> Result<ConnectorDocumentFormat, MvDocumentError> {
    ConnectorDocumentFormat::try_new(OWNER, name, FORMAT_VERSION).map_err(Into::into)
}

fn definition_document(bytes: &[u8]) -> Result<Box<DefinitionDocument>, MvDocumentError> {
    Ok(Box::new(decode_definition(
        bytes,
        PersistenceDecodeBudget::default(),
    )?))
}

fn interpretation_document(bytes: &[u8]) -> Result<Box<InterpretationDocument>, MvDocumentError> {
    Ok(Box::new(decode_interpretation(
        bytes,
        PersistenceDecodeBudget::default(),
    )?))
}

fn definition_document_identity(
    bytes: &[u8],
) -> Result<novarocks_mv_application::persistence::identity::ComputationIdentity, MvDocumentError> {
    Ok(decode_definition(bytes, PersistenceDecodeBudget::default())?.computation_identity)
}

fn validate_envelope(document: &ConnectorStoredDocument) -> Result<(), MvDocumentError> {
    let name = document.id().name().as_str();
    if document.id().owner().as_str() != OWNER
        || document.format().owner() != OWNER
        || document.format().name() != name
        || document.format().version() != FORMAT_VERSION
        || !matches!(
            name,
            DEFINITION | INTERPRETATION | PUBLICATION | CONFIGURATION
        )
    {
        return Err(MvDocumentError::Contract(
            "Current contains an unknown MV owner, name, or format".to_string(),
        ));
    }
    Ok(())
}

fn required_document<'a>(
    documents: &BTreeMap<&str, &'a ConnectorStoredDocument>,
    name: &'static str,
) -> Result<&'a ConnectorStoredDocument, MvDocumentError> {
    documents.get(name).copied().ok_or_else(|| {
        MvDocumentError::Contract(format!("Current is missing required {name} document"))
    })
}

fn available_content(document: &ConnectorStoredDocument) -> Result<&[u8], MvDocumentError> {
    match document.carrier() {
        novarocks_spi::connector::document_storage::ConnectorDocumentCarrier::AvailableContent(
            content,
        ) => Ok(content),
        novarocks_spi::connector::document_storage::ConnectorDocumentCarrier::DeferredContent(
            _,
        ) => Err(MvDocumentError::Contract(
            "Current document content was not loaded before decode".to_string(),
        )),
    }
}

fn revision(document: &ConnectorStoredDocument) -> DocumentRevision {
    DocumentRevision::try_from_bytes(&document.id().revision().to_bytes())
        .expect("Connector and MV document revisions are both SHA-256")
}

fn require_attachment(
    document: &ConnectorStoredDocument,
    output: bool,
) -> Result<(), MvDocumentError> {
    let valid = matches!(
        (output, document.attachment()),
        (false, ConnectorStoredDocumentAttachment::TableMetadata)
            | (true, ConnectorStoredDocumentAttachment::ExactOutput(_))
    );
    if !valid {
        return Err(MvDocumentError::Contract(
            "MV document is attached to the wrong provider version domain".to_string(),
        ));
    }
    Ok(())
}

fn require_exact_reference(
    document: &ConnectorStoredDocument,
    relationship: &'static str,
    target: &ConnectorDocumentId,
) -> Result<(), MvDocumentError> {
    if document
        .references()
        .iter()
        .filter(|reference| {
            reference.relationship() == relationship && reference.target() == target
        })
        .count()
        != 1
    {
        return Err(MvDocumentError::Contract(format!(
            "{} does not reference exact Current {relationship}",
            document.id().name().as_str()
        )));
    }
    Ok(())
}

pub(crate) fn target_object_identity(
    object_id: &ConnectorTableObjectId,
) -> Result<novarocks_mv_application::persistence::identity::ObjectIdentity, MvDocumentError> {
    novarocks_mv_application::persistence::identity::ObjectIdentity::try_new(
        object_id.as_bytes().to_vec(),
    )
    .map_err(|error| MvDocumentError::Contract(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_mv_application::persistence::codec::{
        ApplyKey, ApplyKeyComponent, ApplyKeyKind, ExpressionKind, ExpressionShape, OutputBinding,
        OutputDefinition, PhysicalFieldBinding, PhysicalFieldLogicalIdentity, PublicationInput,
        PublicationKind, PublicationOutput, PublicationStatistics, QueryDialect, QuerySource,
        RefreshPolicy, RelationOccurrence, ResolutionContext, SourceFieldBinding,
        SourceFieldReference, TargetBinding, build_definition,
    };
    use novarocks_mv_application::persistence::identity::{
        ApplyKeyIdentity, FieldIdentity, NativeDataVersion, ObjectIdentity, OutputIdentity,
        PartitionSpecVersion, PublicationIdentity, SchemaVersion,
    };
    use novarocks_spi::connector::document_storage::{
        ConnectorDocumentCarrier, ConnectorStoredDocument,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCommittedVersion, ConnectorInstanceId,
        ConnectorMutationOperationId, ConnectorPreparedCreateFieldBinding,
        ConnectorProviderBindingKey, ConnectorTableIdentity, ProviderBindingEpoch,
    };

    use super::*;

    fn opaque<T, E: std::fmt::Debug>(value: u8, make: impl FnOnce(Vec<u8>) -> Result<T, E>) -> T {
        make(vec![value]).expect("fixture identity")
    }

    fn fixture() -> (
        DefinitionDocument,
        InterpretationDocument,
        ConfigurationDocument,
        ConnectorPreparedCreateDocumentTarget,
    ) {
        let source_object = opaque(1, ObjectIdentity::try_new);
        let source_field = opaque(2, FieldIdentity::try_new);
        let output = opaque(3, OutputIdentity::try_new);
        let target_object = opaque(4, ObjectIdentity::try_new);
        let target_output = opaque(5, FieldIdentity::try_new);
        let apply_logical = opaque(6, ApplyKeyIdentity::try_new);
        let target_apply = opaque(7, FieldIdentity::try_new);
        let target_schema = opaque(8, SchemaVersion::try_new);
        let target_spec = opaque(9, PartitionSpecVersion::try_new);
        let definition = build_definition(
            QuerySource {
                effective_sql: "SELECT o.order_id FROM ice.sales.orders o".to_string(),
                dialect: QueryDialect::StarRocks,
                resolution: ResolutionContext {
                    default_catalog: "ice".to_string(),
                    default_namespace: "sales".to_string(),
                },
            },
            vec![RelationOccurrence {
                occurrence_id: 0,
                catalog_at_binding: "ice".to_string(),
                namespace_at_binding: "sales".to_string(),
                relation_at_binding: "orders".to_string(),
                qualifier_at_binding: "o".to_string(),
                object_id: source_object.clone(),
                schema_version: opaque(10, SchemaVersion::try_new),
                fields: vec![SourceFieldBinding {
                    field_id: source_field.clone(),
                    name_at_binding: "order_id".to_string(),
                    type_signature: "bigint".to_string(),
                    nullable: false,
                }],
            }],
            vec![OutputDefinition {
                output_id: output.clone(),
                name: "order_id".to_string(),
                type_signature: "bigint".to_string(),
                nullable: false,
                expression: ExpressionShape {
                    kind: ExpressionKind::Field,
                    function_identity: None,
                    source_fields: vec![SourceFieldReference {
                        occurrence_id: 0,
                        field_id: source_field,
                    }],
                },
            }],
        )
        .expect("definition");
        let definition_revision = encode_definition(&definition).unwrap().revision();
        let interpretation = InterpretationDocument {
            definition_revision,
            computation_identity: definition.computation_identity,
            outputs: vec![OutputBinding {
                output_id: output.clone(),
                target_field_id: target_output.clone(),
                type_signature: "bigint".to_string(),
                nullable: false,
            }],
            state_slots: Vec::new(),
            apply_key: ApplyKey {
                kind: ApplyKeyKind::BaseRowId,
                components: vec![ApplyKeyComponent {
                    logical_id: apply_logical.clone(),
                    target_field_id: target_apply.clone(),
                }],
            },
            aggregates: Vec::new(),
            branches: Vec::new(),
            target: TargetBinding {
                object_id: target_object.clone(),
                schema_version: target_schema.clone(),
                partition_spec_version: target_spec.clone(),
                fields: vec![
                    PhysicalFieldBinding {
                        logical_identity: PhysicalFieldLogicalIdentity::Output(output),
                        target_field_id: target_output.clone(),
                        type_signature: "bigint".to_string(),
                        nullable: false,
                    },
                    PhysicalFieldBinding {
                        logical_identity: PhysicalFieldLogicalIdentity::ApplyKey(apply_logical),
                        target_field_id: target_apply.clone(),
                        type_signature: "binary".to_string(),
                        nullable: false,
                    },
                ],
            },
        };
        let configuration = ConfigurationDocument {
            refresh_policy: RefreshPolicy::Manual,
            paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
        };
        let instance_id = ConnectorInstanceId::parse("ice").unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::new(),
        };
        let target = ConnectorPreparedCreateDocumentTarget::try_new(
            owner,
            CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([1; 32])),
            ConnectorMutationOperationId::new(),
            ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("sales"),
                table: Arc::from("mv_orders"),
            },
            ConnectorTableObjectId::try_new(Bytes::copy_from_slice(target_object.as_bytes()))
                .unwrap(),
            Bytes::copy_from_slice(target_schema.as_bytes()),
            Bytes::copy_from_slice(target_spec.as_bytes()),
            vec![
                ConnectorPreparedCreateFieldBinding::try_new(
                    0,
                    Bytes::copy_from_slice(target_output.as_bytes()),
                )
                .unwrap(),
                ConnectorPreparedCreateFieldBinding::try_new(
                    1,
                    Bytes::copy_from_slice(target_apply.as_bytes()),
                )
                .unwrap(),
            ],
            Bytes::from_static(b"provider-token"),
        )
        .unwrap();
        (definition, interpretation, configuration, target)
    }

    fn stored(document: &ConnectorDocument, output: bool) -> ConnectorStoredDocument {
        ConnectorStoredDocument::try_new(
            document.id().clone(),
            document.format().clone(),
            document.content().len(),
            document.references().to_vec(),
            if output {
                ConnectorStoredDocumentAttachment::ExactOutput(
                    ConnectorCommittedVersion::try_new(
                        Bytes::from_static(b"snapshot-11"),
                        Some(11),
                    )
                    .unwrap(),
                )
            } else {
                ConnectorStoredDocumentAttachment::TableMetadata
            },
            ConnectorDocumentCarrier::AvailableContent(document.content().clone()),
        )
        .unwrap()
    }

    #[test]
    fn create_and_publication_sets_round_trip_as_one_exact_current() {
        let (definition, interpretation, configuration, target) = fixture();
        let create = create_document_set(&definition, &interpretation, &configuration, &target)
            .expect("create documents");
        assert_eq!(
            create
                .documents()
                .iter()
                .map(|document| document.id().name().as_str())
                .collect::<Vec<_>>(),
            [DEFINITION, INTERPRETATION, CONFIGURATION]
        );

        let definition_revision = encode_definition(&definition).unwrap().revision();
        let interpretation_revision = encode_interpretation(&interpretation).unwrap().revision();
        let publication = PublicationDocument {
            publication_id: PublicationIdentity::try_new(vec![1]).unwrap(),
            definition_revision,
            interpretation_revision,
            inputs: vec![PublicationInput {
                relation_occurrence_id: 0,
                object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
                native_data_version: NativeDataVersion::try_new(vec![12]).unwrap(),
            }],
            output: PublicationOutput {
                object_id: ObjectIdentity::try_new(vec![4]).unwrap(),
                native_data_version: NativeDataVersion::try_new(vec![13]).unwrap(),
                empty_result: false,
            },
            kind: PublicationKind::FullRefresh,
            statistics: PublicationStatistics::default(),
        };
        let publication_set = publication_document_set(&definition, &interpretation, &publication)
            .expect("publication documents");
        let mut current = create
            .documents()
            .iter()
            .map(|document| stored(document, false))
            .collect::<Vec<_>>();
        current.push(stored(&publication_set.documents()[0], true));

        let decoded = decode_current_documents(&current, PersistenceDecodeBudget::default())
            .expect("exact Current");
        assert_eq!(decoded.definition, definition);
        assert_eq!(decoded.interpretation, interpretation);
        assert_eq!(decoded.configuration, configuration);
        assert_eq!(decoded.publication, Some(publication));
    }

    #[test]
    fn create_rejects_provider_target_version_drift() {
        let (definition, interpretation, configuration, target) = fixture();
        let mut drifted = interpretation;
        drifted.target.schema_version = SchemaVersion::try_new(vec![99]).unwrap();
        assert!(create_document_set(&definition, &drifted, &configuration, &target).is_err());
    }

    #[test]
    fn current_rejects_a_publication_referencing_non_current_definition() {
        let (definition, interpretation, configuration, target) = fixture();
        let create = create_document_set(&definition, &interpretation, &configuration, &target)
            .expect("create documents");
        let mut current = create
            .documents()
            .iter()
            .map(|document| stored(document, false))
            .collect::<Vec<_>>();
        let publication = PublicationDocument {
            publication_id: PublicationIdentity::try_new(vec![1]).unwrap(),
            definition_revision: encode_definition(&definition).unwrap().revision(),
            interpretation_revision: encode_interpretation(&interpretation).unwrap().revision(),
            inputs: vec![PublicationInput {
                relation_occurrence_id: 0,
                object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
                native_data_version: NativeDataVersion::try_new(vec![12]).unwrap(),
            }],
            output: PublicationOutput {
                object_id: ObjectIdentity::try_new(vec![4]).unwrap(),
                native_data_version: NativeDataVersion::try_new(vec![13]).unwrap(),
                empty_result: false,
            },
            kind: PublicationKind::FullRefresh,
            statistics: PublicationStatistics::default(),
        };
        let publication_set =
            publication_document_set(&definition, &interpretation, &publication).unwrap();
        let original = &publication_set.documents()[0];
        let wrong = ConnectorDocumentReference::try_new(
            REFERENCES_DEFINITION,
            ConnectorDocumentId::new(
                owner().unwrap(),
                document_name(DEFINITION).unwrap(),
                ConnectorDocumentRevision::for_content(b"different"),
            ),
        )
        .unwrap();
        let tampered = ConnectorDocument::try_new(
            owner().unwrap(),
            document_name(PUBLICATION).unwrap(),
            document_format(PUBLICATION).unwrap(),
            original.content().clone(),
            vec![wrong, original.references()[1].clone()],
            ConnectorDocumentAttachment::CommitOutput,
        )
        .unwrap();
        current.push(stored(&tampered, true));
        assert!(decode_current_documents(&current, PersistenceDecodeBudget::default()).is_err());
    }
}
