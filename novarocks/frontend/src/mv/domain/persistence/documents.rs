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
    encode_interpretation, encode_publication, preflight_current_document_set,
};
use novarocks_mv_application::persistence::identity::DocumentRevision;
use novarocks_mv_application::persistence::validation::{
    PersistenceDecodeBudget, ValidationError, validate_document_set,
};
use novarocks_spi::connector::document_storage::{
    ConnectorDocument, ConnectorDocumentAttachment, ConnectorDocumentFormat, ConnectorDocumentId,
    ConnectorDocumentManagementObservation, ConnectorDocumentName, ConnectorDocumentOwner,
    ConnectorDocumentReference, ConnectorDocumentRevision, ConnectorDocumentSet,
    ConnectorStoredDocument, ConnectorStoredDocumentAttachment,
};
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorPreparedCreateDocumentTarget, ConnectorTableIdentity,
    ConnectorTableObjectId,
};

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
    pub target: ConnectorTableIdentity,
    pub target_object_id: ConnectorTableObjectId,
    pub metadata_version: ConnectorCommittedVersion,
    pub definition: DefinitionDocument,
    pub definition_revision: DocumentRevision,
    pub interpretation: InterpretationDocument,
    pub interpretation_revision: DocumentRevision,
    pub publication: Option<PublicationDocument>,
    pub publication_revision: Option<DocumentRevision>,
    pub publication_output_version: Option<ConnectorCommittedVersion>,
    pub configuration: ConfigurationDocument,
    pub configuration_revision: DocumentRevision,
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

/// Decodes one exact, lease-sealed management observation. Deferred bodies
/// must be loaded through the original observation request before entering
/// this application boundary.
pub(crate) fn decode_current_management_documents(
    observation: &ConnectorDocumentManagementObservation,
    loaded_documents: &[ConnectorDocument],
    budget: PersistenceDecodeBudget,
) -> Result<MvDecodedDocuments, MvDocumentError> {
    observation.validate_sealed()?;
    let decoded = decode_document_slice(observation.documents(), loaded_documents, budget)?;
    if decoded.interpretation.target.object_id.as_bytes()
        != observation.object_id().as_bytes().as_ref()
    {
        return Err(MvDocumentError::Contract(
            "Current L target object does not match the exact observed table".to_string(),
        ));
    }
    Ok(MvDecodedDocuments {
        target: observation.target().clone(),
        target_object_id: observation.object_id().clone(),
        metadata_version: observation.metadata_version().clone(),
        definition: decoded.definition,
        definition_revision: decoded.definition_revision,
        interpretation: decoded.interpretation,
        interpretation_revision: decoded.interpretation_revision,
        publication: decoded.publication,
        publication_revision: decoded.publication_revision,
        publication_output_version: decoded.publication_output_version,
        configuration: decoded.configuration,
        configuration_revision: decoded.configuration_revision,
    })
}

#[derive(Debug)]
struct DecodedDocumentSlice {
    definition: DefinitionDocument,
    definition_revision: DocumentRevision,
    interpretation: InterpretationDocument,
    interpretation_revision: DocumentRevision,
    publication: Option<PublicationDocument>,
    publication_revision: Option<DocumentRevision>,
    publication_output_version: Option<ConnectorCommittedVersion>,
    configuration: ConfigurationDocument,
    configuration_revision: DocumentRevision,
}

fn decode_document_slice(
    documents: &[ConnectorStoredDocument],
    loaded_documents: &[ConnectorDocument],
    budget: PersistenceDecodeBudget,
) -> Result<DecodedDocumentSlice, MvDocumentError> {
    let loaded_by_id = validate_loaded_documents(documents, loaded_documents)?;
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
    require_exact_references(definition_stored, &[])?;
    require_exact_references(configuration_stored, &[])?;

    let definition_content = resolved_content(definition_stored, &loaded_by_id)?;
    let interpretation_content = resolved_content(interpretation_stored, &loaded_by_id)?;
    let configuration_content = resolved_content(configuration_stored, &loaded_by_id)?;
    let publication_content = by_name
        .get(PUBLICATION)
        .map(|stored| resolved_content(stored, &loaded_by_id))
        .transpose()?;
    preflight_current_document_set(
        definition_content,
        interpretation_content,
        publication_content,
        configuration_content,
        budget,
    )?;

    let definition = decode_definition(definition_content, budget)?;
    let interpretation = decode_interpretation(interpretation_content, budget)?;
    let configuration = decode_configuration(configuration_content, budget)?;
    let definition_revision = revision(definition_stored);
    let interpretation_revision = revision(interpretation_stored);
    let configuration_revision = revision(configuration_stored);
    require_exact_references(
        interpretation_stored,
        &[(REFERENCES_DEFINITION, definition_stored.id())],
    )?;

    let (publication, publication_revision, publication_output_version) =
        match by_name.get(PUBLICATION) {
            Some(stored) => {
                require_attachment(stored, true)?;
                require_exact_references(
                    stored,
                    &[
                        (REFERENCES_DEFINITION, definition_stored.id()),
                        (REFERENCES_INTERPRETATION, interpretation_stored.id()),
                    ],
                )?;
                let publication = decode_publication(
                    publication_content.expect("publication content was resolved above"),
                    budget,
                )?;
                validate_document_set(
                    &definition,
                    definition_revision,
                    &interpretation,
                    interpretation_revision,
                    &publication,
                )?;
                let ConnectorStoredDocumentAttachment::ExactOutput(output_version) =
                    stored.attachment()
                else {
                    unreachable!("publication attachment was validated above")
                };
                (
                    Some(publication),
                    Some(revision(stored)),
                    Some(output_version.clone()),
                )
            }
            None => {
                if interpretation.definition_revision != definition_revision
                    || interpretation.computation_identity != definition.computation_identity
                {
                    return Err(MvDocumentError::Contract(
                        "Current L does not bind exact Current D".to_string(),
                    ));
                }
                (None, None, None)
            }
        };

    Ok(DecodedDocumentSlice {
        definition,
        definition_revision,
        interpretation,
        interpretation_revision,
        publication,
        publication_revision,
        publication_output_version,
        configuration,
        configuration_revision,
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
    // A document-managed CREATE emits its physical columns in the same
    // canonical logical-identity order used by L. Preserve the provider's
    // request-ordinal mapping here instead of degrading it to set equality.
    let mut target_fields = interpretation.target.fields.iter().collect::<Vec<_>>();
    target_fields.sort_by(|left, right| left.logical_identity.cmp(&right.logical_identity));
    if target_fields.len() != target.fields().len()
        || target_fields.iter().zip(target.fields()).enumerate().any(
            |(ordinal, (field, prepared))| {
                prepared.request_ordinal() as usize != ordinal
                    || field.target_field_id.as_bytes() != prepared.provider_field_id().as_ref()
            },
        )
    {
        return Err(MvDocumentError::Contract(
            "interpretation target fields do not match the provider-prepared ordinal bindings"
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

fn validate_loaded_documents<'a>(
    stored: &[ConnectorStoredDocument],
    loaded: &'a [ConnectorDocument],
) -> Result<BTreeMap<ConnectorDocumentId, &'a ConnectorDocument>, MvDocumentError> {
    let stored_by_id = stored
        .iter()
        .map(|document| (document.id(), document))
        .collect::<BTreeMap<_, _>>();
    let mut loaded_by_id = BTreeMap::new();
    for document in loaded {
        let Some(envelope) = stored_by_id.get(document.id()).copied() else {
            return Err(MvDocumentError::Contract(
                "loaded MV document was not requested by the exact observation".to_string(),
            ));
        };
        if loaded_by_id.insert(document.id().clone(), document).is_some()
            || !matches!(
                envelope.carrier(),
                novarocks_spi::connector::document_storage::ConnectorDocumentCarrier::DeferredContent(_)
            )
            || envelope.format() != document.format()
            || envelope.references() != document.references()
            || envelope.encoded_len() != document.content().len()
            || !loaded_attachment_matches(envelope.attachment(), document.attachment())
        {
            return Err(MvDocumentError::Contract(
                "loaded MV document does not match its exact observed envelope".to_string(),
            ));
        }
    }
    Ok(loaded_by_id)
}

fn loaded_attachment_matches(
    stored: &ConnectorStoredDocumentAttachment,
    loaded: &ConnectorDocumentAttachment,
) -> bool {
    matches!(
        (stored, loaded),
        (
            ConnectorStoredDocumentAttachment::TableMetadata,
            ConnectorDocumentAttachment::TableMetadata
        )
    ) || matches!(
        (stored, loaded),
        (
            ConnectorStoredDocumentAttachment::ExactOutput(expected),
            ConnectorDocumentAttachment::ExactOutput(actual)
        ) if expected == actual
    )
}

fn resolved_content<'a>(
    document: &'a ConnectorStoredDocument,
    loaded: &BTreeMap<ConnectorDocumentId, &'a ConnectorDocument>,
) -> Result<&'a [u8], MvDocumentError> {
    match document.carrier() {
        novarocks_spi::connector::document_storage::ConnectorDocumentCarrier::AvailableContent(
            content,
        ) => Ok(content),
        novarocks_spi::connector::document_storage::ConnectorDocumentCarrier::DeferredContent(
            _,
        ) => loaded
            .get(document.id())
            .map(|loaded| loaded.content().as_ref())
            .ok_or_else(|| {
                MvDocumentError::Contract(
                    "Current document content was not loaded before decode".to_string(),
                )
            }),
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

fn require_exact_references(
    document: &ConnectorStoredDocument,
    expected: &[(&'static str, &ConnectorDocumentId)],
) -> Result<(), MvDocumentError> {
    if document.references().len() != expected.len()
        || expected.iter().any(|(relationship, target)| {
            document
                .references()
                .iter()
                .filter(|reference| {
                    reference.relationship() == *relationship && reference.target() == *target
                })
                .count()
                != 1
        })
    {
        return Err(MvDocumentError::Contract(format!(
            "{} does not contain exactly its required Current references",
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
    use std::time::{Duration, Instant};

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
        ConnectorDeferredDocumentHandle, ConnectorDocumentCarrier,
        ConnectorDocumentManagementObservation, ConnectorDocumentObservationRequest,
        ConnectorDocumentStorageBudget, ConnectorDocumentStorageLimits,
        ConnectorManagedObjectMarker, ConnectorStoredDocument,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCancellation, ConnectorCommittedVersion,
        ConnectorInstanceId, ConnectorMutationOperationId, ConnectorPreparedCreateFieldBinding,
        ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableIdentity,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        ProviderBindingEpoch,
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

    fn stored_deferred(document: &ConnectorDocument) -> ConnectorStoredDocument {
        ConnectorStoredDocument::try_new(
            document.id().clone(),
            document.format().clone(),
            document.content().len(),
            document.references().to_vec(),
            ConnectorStoredDocumentAttachment::TableMetadata,
            ConnectorDocumentCarrier::DeferredContent(
                ConnectorDeferredDocumentHandle::try_new(Bytes::from_static(b"deferred")).unwrap(),
            ),
        )
        .unwrap()
    }

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
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

        let decoded = decode_document_slice(&current, &[], PersistenceDecodeBudget::default())
            .expect("exact Current");
        assert_eq!(decoded.definition, definition);
        assert_eq!(decoded.interpretation, interpretation);
        assert_eq!(decoded.configuration, configuration);
        assert_eq!(decoded.publication, Some(publication));
        assert_eq!(
            decoded
                .publication_output_version
                .as_ref()
                .and_then(ConnectorCommittedVersion::snapshot_id),
            Some(11)
        );
    }

    #[test]
    fn create_rejects_provider_target_version_drift() {
        let (definition, interpretation, configuration, target) = fixture();
        let mut schema_drifted = interpretation.clone();
        schema_drifted.target.schema_version = SchemaVersion::try_new(vec![99]).unwrap();
        assert!(
            create_document_set(&definition, &schema_drifted, &configuration, &target).is_err()
        );
        let mut spec_drifted = interpretation;
        spec_drifted.target.partition_spec_version =
            PartitionSpecVersion::try_new(vec![99]).unwrap();
        assert!(create_document_set(&definition, &spec_drifted, &configuration, &target).is_err());
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
        assert!(decode_document_slice(&current, &[], PersistenceDecodeBudget::default()).is_err());
    }

    #[test]
    fn create_rejects_swapped_provider_field_ordinals() {
        let (definition, mut interpretation, configuration, target) = fixture();
        let first = interpretation.target.fields[0].target_field_id.clone();
        interpretation.target.fields[0].target_field_id =
            interpretation.target.fields[1].target_field_id.clone();
        interpretation.target.fields[1].target_field_id = first;

        assert!(
            create_document_set(&definition, &interpretation, &configuration, &target).is_err()
        );
    }

    #[test]
    fn current_rejects_extra_document_references() {
        let (definition, interpretation, configuration, target) = fixture();
        let create = create_document_set(&definition, &interpretation, &configuration, &target)
            .expect("create documents");
        let original = &create.documents()[0];
        let extra = ConnectorDocumentReference::try_new(
            "stale",
            ConnectorDocumentId::new(
                owner().unwrap(),
                document_name(DEFINITION).unwrap(),
                ConnectorDocumentRevision::for_content(b"stale"),
            ),
        )
        .unwrap();
        let tampered = ConnectorDocument::try_new(
            owner().unwrap(),
            document_name(DEFINITION).unwrap(),
            document_format(DEFINITION).unwrap(),
            original.content().clone(),
            vec![extra],
            ConnectorDocumentAttachment::TableMetadata,
        )
        .unwrap();
        let mut current = vec![stored(&tampered, false)];
        current.extend(
            create.documents()[1..]
                .iter()
                .map(|document| stored(document, false)),
        );

        assert!(decode_document_slice(&current, &[], PersistenceDecodeBudget::default()).is_err());
    }

    #[test]
    fn current_requires_and_accepts_the_exact_loaded_deferred_document() {
        let (definition, interpretation, configuration, target) = fixture();
        let create = create_document_set(&definition, &interpretation, &configuration, &target)
            .expect("create documents");
        let definition_document = create.documents()[0].clone();
        let mut current = create
            .documents()
            .iter()
            .map(|document| stored(document, false))
            .collect::<Vec<_>>();
        current[0] = stored_deferred(&definition_document);

        assert!(decode_document_slice(&current, &[], PersistenceDecodeBudget::default()).is_err());
        assert!(
            decode_document_slice(
                &current,
                &[definition_document],
                PersistenceDecodeBudget::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn current_management_decode_rejects_an_unsealed_observation() {
        let (definition, interpretation, configuration, target) = fixture();
        let create = create_document_set(&definition, &interpretation, &configuration, &target)
            .expect("create documents");
        let request = ConnectorDocumentObservationRequest::try_new(
            target.owner().clone(),
            target.catalog_handle().clone(),
            target.target().clone(),
            target.object_id().clone(),
            ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
            request_context(),
        )
        .unwrap();
        let observation = ConnectorDocumentManagementObservation::try_new(
            &request,
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata-11"), Some(11))
                .unwrap(),
            ConnectorManagedObjectMarker::try_new("materialized-view", "deployment", "incarnation")
                .unwrap(),
            create
                .documents()
                .iter()
                .map(|document| stored(document, false))
                .collect(),
        )
        .unwrap();

        assert!(
            decode_current_management_documents(
                &observation,
                &[],
                PersistenceDecodeBudget::default()
            )
            .is_err()
        );
    }
}
