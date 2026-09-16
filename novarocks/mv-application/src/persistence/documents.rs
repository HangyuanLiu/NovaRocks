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

//! MV-owned adapter between MV D/L/P/C and opaque Connector documents.
//!
//! The Connector owns atomic storage and exact attachment points. The MV
//! application owns these names, formats, codecs, references, and the rule
//! that a Current document set must be internally complete. There is no
//! descriptor-property or provider-token fallback in this adapter.

use std::collections::{BTreeMap, BTreeSet};

use crate::management::{DeploymentOwner, ManagedMvTarget, ProcessIncarnation};
use crate::persistence::codec::{
    ConfigurationDocument, DefinitionDocument, EncodedDocument, InterpretationDocument,
    PersistenceCodecError, PublicationDocument, decode_configuration, decode_definition,
    decode_interpretation, decode_publication, encode_configuration, encode_definition,
    encode_interpretation, encode_publication, preflight_current_document_set,
};
use crate::persistence::identity::DocumentRevision;
use crate::persistence::validation::{
    PersistenceDecodeBudget, ValidationError, validate_document_set,
};
use bytes::Bytes;
use novarocks_spi::connector::document_storage::{
    ConnectorDocument, ConnectorDocumentAttachment, ConnectorDocumentCarrier,
    ConnectorDocumentFormat, ConnectorDocumentId, ConnectorDocumentManagementObservation,
    ConnectorDocumentName, ConnectorDocumentObservationRequest, ConnectorDocumentOwner,
    ConnectorDocumentReference, ConnectorDocumentRevision, ConnectorDocumentSet,
    ConnectorDocumentStorageLease, ConnectorStoredDocument, ConnectorStoredDocumentAttachment,
    FrozenConnectorDocumentObservation,
};
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorPreparedCreateDocumentTarget, ConnectorTableIdentity,
    ConnectorTableObjectId,
};

use super::definition::{MvAcceleratorCommittedVersionRevision, MvAcceleratorSourceRevision};

const OWNER: &str = "novarocks.mv";
const DEFINITION: &str = "definition";
const INTERPRETATION: &str = "interpretation";
const PUBLICATION: &str = "publication";
const CONFIGURATION: &str = "configuration";
const REFERENCES_DEFINITION: &str = "definition";
const REFERENCES_INTERPRETATION: &str = "interpretation";
const FORMAT_VERSION: u32 = 1;
const MANAGED_MV_KIND: &str = "materialized-view";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvObservedCurrentDocuments {
    pub(crate) management_target: ManagedMvTarget,
    pub(crate) deployment_owner: DeploymentOwner,
    pub(crate) process_incarnation: ProcessIncarnation,
    pub(crate) target: ConnectorTableIdentity,
    pub(crate) target_object_id: ConnectorTableObjectId,
    pub(crate) metadata_version: ConnectorCommittedVersion,
    pub(crate) definition: DefinitionDocument,
    pub(crate) definition_revision: DocumentRevision,
    pub(crate) interpretation: InterpretationDocument,
    pub(crate) interpretation_revision: DocumentRevision,
    pub(crate) publication: Option<PublicationDocument>,
    pub(crate) publication_revision: Option<DocumentRevision>,
    pub(crate) publication_output_version: Option<ConnectorCommittedVersion>,
    pub(crate) configuration: ConfigurationDocument,
    pub(crate) configuration_revision: DocumentRevision,
}

impl MvObservedCurrentDocuments {
    /// The exact target object this observation was read from.
    pub fn target_object_id(&self) -> &ConnectorTableObjectId {
        &self.target_object_id
    }

    /// The committed output version P attached to, absent when this target has
    /// never published.
    pub fn publication_output_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.publication_output_version.as_ref()
    }

    /// The exact document dependencies this observation proves.
    ///
    /// The management entrance installs against these and refuses a later
    /// effect whose frozen set no longer matches, so the revisions stay
    /// private and are only ever compared as a whole.
    pub fn management_dependencies(
        &self,
        control_runtime_id: novarocks_spi::connector::ConnectorControlRuntimeId,
    ) -> crate::management::ManagementDependencySet {
        self.source_revision()
            .management_dependencies(control_runtime_id)
    }

    pub(crate) fn source_revision(&self) -> MvAcceleratorSourceRevision {
        MvAcceleratorSourceRevision {
            target: self.target.clone(),
            target_object_id: self.target_object_id.clone(),
            metadata_version: MvAcceleratorCommittedVersionRevision::from_committed(
                &self.metadata_version,
            ),
            definition_revision: self.definition_revision,
            interpretation_revision: self.interpretation_revision,
            publication_revision: self.publication_revision,
            publication_output_version: self
                .publication_output_version
                .as_ref()
                .map(MvAcceleratorCommittedVersionRevision::from_committed),
            configuration_revision: self.configuration_revision,
            deployment_owner: self.deployment_owner.clone(),
            process_incarnation: self.process_incarnation.clone(),
        }
    }
}

#[derive(Debug)]
pub enum MvDocumentError {
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
pub fn create_document_set(
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
pub fn publication_document_set(
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
fn decode_current_management_documents(
    observation: &ConnectorDocumentManagementObservation,
    loaded_documents: &[ConnectorDocument],
    budget: PersistenceDecodeBudget,
) -> Result<MvObservedCurrentDocuments, MvDocumentError> {
    observation.validate_sealed()?;
    if observation.marker().kind() != MANAGED_MV_KIND {
        return Err(MvDocumentError::Contract(
            "Current management marker is not a materialized view".to_string(),
        ));
    }
    let management_target = ManagedMvTarget::from_observation(observation)
        .map_err(|error| MvDocumentError::Contract(error.to_string()))?;
    let deployment_owner = DeploymentOwner::parse(observation.marker().owner())
        .map_err(|error| MvDocumentError::Contract(error.to_string()))?;
    let process_incarnation = ProcessIncarnation::parse(observation.marker().incarnation())
        .map_err(|error| MvDocumentError::Contract(error.to_string()))?;
    let decoded = decode_document_slice(observation.documents(), loaded_documents, budget)?;
    if decoded.interpretation.target.object_id.as_bytes()
        != observation.object_id().as_bytes().as_ref()
    {
        return Err(MvDocumentError::Contract(
            "Current L target object does not match the exact observed table".to_string(),
        ));
    }
    Ok(MvObservedCurrentDocuments {
        management_target,
        deployment_owner,
        process_incarnation,
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

/// Observe one exact Current package, explicitly resolve only its deferred
/// bodies through the retained request, then decode the sealed D/L/P/C set.
///
/// Cloning the request retains the same shared operation budget. Every body
/// load is therefore charged together with the initial observation rather than
/// receiving a fresh per-document allowance.
pub fn observe_current_management_documents(
    lease: &ConnectorDocumentStorageLease,
    request: ConnectorDocumentObservationRequest,
    decode_budget: PersistenceDecodeBudget,
) -> Result<MvObservedCurrentDocuments, MvDocumentError> {
    Ok(observe_current_management_document_set(lease, request, decode_budget)?.documents)
}

/// One sealed provider Current observation together with the D/L/P/C documents
/// decoded from that exact value. The raw observation is retained so the
/// management owner can complete readmission and mint the one-shot readiness
/// admission without reopening Current or accepting a historical read.
pub struct MvObservedCurrentManagementDocumentSet {
    observation: ConnectorDocumentManagementObservation,
    documents: MvObservedCurrentDocuments,
}

impl MvObservedCurrentManagementDocumentSet {
    pub fn observation(&self) -> &ConnectorDocumentManagementObservation {
        &self.observation
    }

    pub fn into_parts(
        self,
    ) -> (
        ConnectorDocumentManagementObservation,
        MvObservedCurrentDocuments,
    ) {
        (self.observation, self.documents)
    }
}

pub fn observe_current_management_document_set(
    lease: &ConnectorDocumentStorageLease,
    request: ConnectorDocumentObservationRequest,
    decode_budget: PersistenceDecodeBudget,
) -> Result<MvObservedCurrentManagementDocumentSet, MvDocumentError> {
    let retained_request = request.clone();
    let observation = lease.observe_current_management(request)?;
    let mut loaded_documents = Vec::new();
    for stored in observation.documents() {
        if matches!(
            stored.carrier(),
            ConnectorDocumentCarrier::DeferredContent(_)
        ) {
            let load = retained_request
                .try_load_request(stored.clone(), retained_request.context().clone())?;
            loaded_documents.push(lease.load_document(load)?);
        }
    }
    let documents =
        decode_current_management_documents(&observation, &loaded_documents, decode_budget)?;
    Ok(MvObservedCurrentManagementDocumentSet {
        observation,
        documents,
    })
}

/// A validated query read view of a frozen P and its exact D/L references.
/// It carries no Current-management authority and cannot install readiness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvFrozenPublicationReadView {
    catalog_handle: novarocks_spi::connector::CatalogHandle,
    provider_owner: novarocks_spi::connector::ConnectorProviderBindingKey,
    target: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    metadata_version: ConnectorCommittedVersion,
    definition: DefinitionDocument,
    definition_revision: DocumentRevision,
    interpretation: InterpretationDocument,
    interpretation_revision: DocumentRevision,
    publication: PublicationDocument,
    publication_revision: DocumentRevision,
    output_version: ConnectorCommittedVersion,
}

impl MvFrozenPublicationReadView {
    pub fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
        &self.catalog_handle
    }
    pub fn provider_owner(&self) -> &novarocks_spi::connector::ConnectorProviderBindingKey {
        &self.provider_owner
    }
    pub fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }
    pub fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }
    pub fn definition(&self) -> &DefinitionDocument {
        &self.definition
    }
    pub fn definition_revision(&self) -> DocumentRevision {
        self.definition_revision
    }
    pub fn interpretation(&self) -> &InterpretationDocument {
        &self.interpretation
    }
    pub fn interpretation_revision(&self) -> DocumentRevision {
        self.interpretation_revision
    }
    pub fn publication(&self) -> &PublicationDocument {
        &self.publication
    }
    pub fn publication_revision(&self) -> DocumentRevision {
        self.publication_revision
    }
    pub fn output_version(&self) -> &ConnectorCommittedVersion {
        &self.output_version
    }
}

pub fn observe_frozen_publication_documents(
    lease: &ConnectorDocumentStorageLease,
    request: ConnectorDocumentObservationRequest,
    budget: PersistenceDecodeBudget,
) -> Result<MvFrozenPublicationReadView, MvDocumentError> {
    let retained_request = request.clone();
    let observation = lease.observe_documents(request)?;
    let mut loaded = Vec::new();
    for stored in observation.documents() {
        if matches!(
            stored.carrier(),
            ConnectorDocumentCarrier::DeferredContent(_)
        ) {
            loaded.push(
                lease.load_document(
                    retained_request
                        .try_load_request(stored.clone(), retained_request.context().clone())?,
                )?,
            );
        }
    }
    decode_frozen_publication_documents(&observation, &loaded, budget)
}

pub fn decode_frozen_publication_documents(
    observation: &FrozenConnectorDocumentObservation,
    loaded_documents: &[ConnectorDocument],
    budget: PersistenceDecodeBudget,
) -> Result<MvFrozenPublicationReadView, MvDocumentError> {
    observation.validate_sealed()?;
    let loaded = validate_loaded_documents(observation.documents(), loaded_documents)?;
    let mut documents = BTreeMap::new();
    for stored in observation.documents() {
        validate_envelope(stored)?;
        if !matches!(
            stored.id().name().as_str(),
            DEFINITION | INTERPRETATION | PUBLICATION | CONFIGURATION
        ) || documents
            .insert(stored.id().name().as_str(), stored)
            .is_some()
        {
            return Err(MvDocumentError::Contract(
                "frozen publication contains unexpected or duplicate documents".into(),
            ));
        }
    }
    let d = required_document(&documents, DEFINITION)?;
    let l = required_document(&documents, INTERPRETATION)?;
    let p = required_document(&documents, PUBLICATION)?;
    require_attachment(d, false)?;
    require_attachment(l, false)?;
    require_attachment(p, true)?;
    require_exact_references(d, &[])?;
    require_exact_references(l, &[(REFERENCES_DEFINITION, d.id())])?;
    require_exact_references(
        p,
        &[
            (REFERENCES_DEFINITION, d.id()),
            (REFERENCES_INTERPRETATION, l.id()),
        ],
    )?;
    let d_bytes = resolved_content(d, &loaded)?;
    let l_bytes = resolved_content(l, &loaded)?;
    let p_bytes = resolved_content(p, &loaded)?;
    // Empty C contributes zero wire resources; C is neither required nor used
    // as query proof. Provider observation/load budgets still cover every body.
    preflight_current_document_set(d_bytes, l_bytes, Some(p_bytes), &[], budget)?;
    let definition = decode_definition(d_bytes, budget)?;
    let interpretation = decode_interpretation(l_bytes, budget)?;
    let publication = decode_publication(p_bytes, budget)?;
    validate_document_set(
        &definition,
        revision(d),
        &interpretation,
        revision(l),
        &publication,
    )?;
    if interpretation.target.object_id.as_bytes() != observation.object_id().as_bytes().as_ref() {
        return Err(MvDocumentError::Contract(
            "frozen interpretation belongs to another target object".into(),
        ));
    }
    let ConnectorStoredDocumentAttachment::ExactOutput(output_version) = p.attachment() else {
        unreachable!("publication attachment was validated above")
    };
    Ok(MvFrozenPublicationReadView {
        catalog_handle: observation.catalog_handle().clone(),
        provider_owner: observation.owner().clone(),
        target: observation.target().clone(),
        object_id: observation.object_id().clone(),
        metadata_version: observation.metadata_version().clone(),
        definition,
        definition_revision: revision(d),
        interpretation,
        interpretation_revision: revision(l),
        publication,
        publication_revision: revision(p),
        output_version: output_version.clone(),
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
    // A document-managed CREATE emits each distinct physical column at the
    // ordinal of its first binding in L's canonical logical-identity order.
    // Multiple typed logical identities may intentionally share that column.
    // Preserve both the ordinal and provider field identity instead of
    // degrading this comparison to unordered set equality.
    let mut target_fields = interpretation.target.fields.iter().collect::<Vec<_>>();
    target_fields.sort_by(|left, right| left.logical_identity.cmp(&right.logical_identity));
    let mut physical_fields = Vec::with_capacity(target_fields.len());
    let mut seen_physical_fields = BTreeSet::new();
    for field in target_fields {
        if seen_physical_fields.insert(field.target_field_id.as_bytes()) {
            physical_fields.push(field.target_field_id.as_bytes());
        }
    }
    if physical_fields.len() != target.fields().len()
        || physical_fields.iter().zip(target.fields()).enumerate().any(
            |(ordinal, (field_id, prepared))| {
                prepared.request_ordinal() as usize != ordinal
                    || *field_id != prepared.provider_field_id().as_ref()
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
) -> Result<crate::persistence::identity::ComputationIdentity, MvDocumentError> {
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
) -> Result<crate::persistence::identity::ObjectIdentity, MvDocumentError> {
    crate::persistence::identity::ObjectIdentity::try_new(object_id.as_bytes().to_vec())
        .map_err(|error| MvDocumentError::Contract(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::management::ManagementDependencySet;
    use crate::persistence::codec::{
        ApplyKey, ApplyKeyComponent, ApplyKeyKind, BranchInterpretation, ExpressionKind,
        ExpressionShape, OutputBinding, OutputDefinition, PhysicalFieldBinding,
        PhysicalFieldLogicalIdentity, PublicationInput, PublicationKind, PublicationOutput,
        PublicationStatistics, QueryDialect, QuerySource, RefreshPolicy, RelationOccurrence,
        ResolutionContext, SourceFieldBinding, SourceFieldReference, TargetBinding,
        build_definition,
    };
    use crate::persistence::identity::{
        ApplyKeyIdentity, BranchIdentity, FieldIdentity, NativeDataVersion, ObjectIdentity,
        OutputIdentity, PartitionSpecVersion, PublicationIdentity, SchemaVersion,
    };
    use novarocks_spi::connector::document_storage::{
        ConnectorDeferredDocumentHandle, ConnectorDocumentCarrier, ConnectorDocumentDiscoveryPage,
        ConnectorDocumentDiscoveryRequest, ConnectorDocumentLoadRequest,
        ConnectorDocumentManagementObservation, ConnectorDocumentObservationRequest,
        ConnectorDocumentStorageBinding, ConnectorDocumentStorageBudget,
        ConnectorDocumentStorageLimits, ConnectorDocumentStorageObservation,
        ConnectorManagedObjectMarker, ConnectorStoredDocument, FrozenConnectorDocumentObservation,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCancellation, ConnectorCommittedVersion,
        ConnectorControlRuntimeId, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorMutationOperationId, ConnectorPreparedCreateFieldBinding,
        ConnectorProviderBindingKey, ConnectorProviderId, ConnectorRequestContext,
        ConnectorTableIdentity, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, ProviderBindingEpoch,
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
            1_700_000_000_000,
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
            incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
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
                    "output".to_string(),
                    "binary".to_string(),
                    false,
                )
                .unwrap(),
                ConnectorPreparedCreateFieldBinding::try_new(
                    1,
                    Bytes::copy_from_slice(target_apply.as_bytes()),
                    "apply".to_string(),
                    "binary".to_string(),
                    false,
                )
                .unwrap(),
            ],
            Bytes::from_static(b"provider-token"),
        )
        .unwrap();
        (definition, interpretation, configuration, target)
    }

    fn prepared_target_with_fields(
        target: &ConnectorPreparedCreateDocumentTarget,
        fields: Vec<(u32, Bytes)>,
    ) -> Result<ConnectorPreparedCreateDocumentTarget, ConnectorError> {
        ConnectorPreparedCreateDocumentTarget::try_new(
            target.owner().clone(),
            target.catalog_handle().clone(),
            target.operation_id(),
            target.target().clone(),
            target.object_id().clone(),
            target.schema_version().clone(),
            target.partition_spec_version().clone(),
            fields
                .into_iter()
                .map(|(ordinal, field_id)| {
                    ConnectorPreparedCreateFieldBinding::try_new(
                        ordinal,
                        field_id,
                        format!("field_{ordinal}"),
                        "binary".to_string(),
                        false,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            target.provider_token().clone(),
        )
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

    struct DocumentObservation {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        stored: Vec<ConnectorStoredDocument>,
        loaded: Vec<ConnectorDocument>,
        metadata_version: ConnectorCommittedVersion,
    }

    impl ConnectorDocumentStorageObservation for DocumentObservation {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn observe_documents(
            &self,
            request: ConnectorDocumentObservationRequest,
        ) -> Result<FrozenConnectorDocumentObservation, ConnectorError> {
            FrozenConnectorDocumentObservation::try_new(
                &request,
                self.metadata_version.clone(),
                self.stored.clone(),
            )
        }

        fn load_document(
            &self,
            request: ConnectorDocumentLoadRequest,
        ) -> Result<ConnectorDocument, ConnectorError> {
            self.loaded
                .iter()
                .find(|document| document.id() == request.document().id())
                .cloned()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "deferred MV document body is not present",
                    )
                })
        }

        fn observe_current_management(
            &self,
            request: ConnectorDocumentObservationRequest,
        ) -> Result<ConnectorDocumentManagementObservation, ConnectorError> {
            ConnectorDocumentManagementObservation::try_new(
                &request,
                self.metadata_version.clone(),
                ConnectorManagedObjectMarker::try_new(
                    MANAGED_MV_KIND,
                    "deployment-a",
                    "process-a",
                )?,
                self.stored.clone(),
            )
        }

        fn discover_documents(
            &self,
            _request: ConnectorDocumentDiscoveryRequest,
        ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "not used by the Current MV document test",
            ))
        }
    }

    fn document_lease(
        target: &ConnectorPreparedCreateDocumentTarget,
        stored: Vec<ConnectorStoredDocument>,
        loaded: Vec<ConnectorDocument>,
        metadata_version: ConnectorCommittedVersion,
    ) -> ConnectorDocumentStorageLease {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: target.target().instance_id.clone(),
        };
        let observation = Arc::new(DocumentObservation {
            descriptor: descriptor.clone(),
            incarnation: target.owner().incarnation,
            stored,
            loaded,
            metadata_version,
        });
        let documents = ConnectorDocumentStorageBinding::try_new(
            descriptor,
            target.owner().incarnation,
            Some(observation),
            None,
        )
        .unwrap();
        let binding = novarocks_catalog_application::test_support::test_control_binding_for(
            target.target().instance_id.clone(),
            1,
        )
        .with_catalog_properties(
            novarocks_spi::connector::CatalogProperties::new(
                target.catalog_handle().clone(),
                ConnectorProviderId::parse("iceberg").unwrap(),
                1,
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
        )
        .and_then(|binding| binding.try_with_document_storage(Some(documents)))
        .unwrap();
        novarocks_spi::connector::ConnectorControlPlanningLease::new(Arc::new(binding), || {})
            .derive_document_storage_lease()
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
            publication_prepared_at_ms: 1_700_000_001_000,
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
        assert_eq!(decoded.publication, Some(publication.clone()));
        assert_eq!(
            decoded
                .publication_output_version
                .as_ref()
                .and_then(ConnectorCommittedVersion::snapshot_id),
            Some(11)
        );

        // Query proof reads a sealed historical package without consulting
        // management markers or requiring C/readiness.
        current.retain(|stored| stored.id().name().as_str() != CONFIGURATION);
        let metadata =
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"frozen-metadata"), Some(99))
                .unwrap();
        let lease = document_lease(&target, current, Vec::new(), metadata.clone());
        let request = ConnectorDocumentObservationRequest::try_new(
            target.owner().clone(),
            target.catalog_handle().clone(),
            target.target().clone(),
            target.object_id().clone(),
            ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
            request_context(),
        )
        .unwrap();
        let historical = observe_frozen_publication_documents(
            &lease,
            request,
            PersistenceDecodeBudget::default(),
        )
        .unwrap();
        assert_eq!(historical.publication(), &publication);
        assert_eq!(historical.metadata_version(), &metadata);
        assert_eq!(historical.output_version().snapshot_id(), Some(11));
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
            publication_prepared_at_ms: 1_700_000_001_000,
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
    fn create_accepts_multiple_branch_identities_sharing_one_prepared_physical_field() {
        let (definition, mut interpretation, configuration, target) = fixture();
        let output_id = interpretation.outputs[0].output_id.clone();
        let shared_field_id = interpretation.outputs[0].target_field_id.clone();
        let first_branch = opaque(20, BranchIdentity::try_new);
        let second_branch = opaque(21, BranchIdentity::try_new);
        interpretation.branches = vec![
            BranchInterpretation {
                branch_id: first_branch.clone(),
                relation_occurrence_ids: vec![0],
                output_ids: vec![output_id.clone()],
            },
            BranchInterpretation {
                branch_id: second_branch.clone(),
                relation_occurrence_ids: vec![0],
                output_ids: vec![output_id],
            },
        ];
        interpretation.target.fields.extend([
            PhysicalFieldBinding {
                logical_identity: PhysicalFieldLogicalIdentity::Branch(first_branch),
                target_field_id: shared_field_id.clone(),
                type_signature: "bigint".to_string(),
                nullable: false,
            },
            PhysicalFieldBinding {
                logical_identity: PhysicalFieldLogicalIdentity::Branch(second_branch),
                target_field_id: shared_field_id,
                type_signature: "bigint".to_string(),
                nullable: false,
            },
        ]);

        create_document_set(&definition, &interpretation, &configuration, &target)
            .expect("shared physical target field");
    }

    #[test]
    fn create_rejects_missing_or_extra_prepared_physical_fields() {
        let (definition, interpretation, configuration, target) = fixture();
        let missing = prepared_target_with_fields(
            &target,
            vec![(0, target.fields()[0].provider_field_id().clone())],
        )
        .unwrap();
        assert!(
            create_document_set(&definition, &interpretation, &configuration, &missing).is_err()
        );

        let extra = prepared_target_with_fields(
            &target,
            vec![
                (0, target.fields()[0].provider_field_id().clone()),
                (1, target.fields()[1].provider_field_id().clone()),
                (2, Bytes::from_static(b"extra-provider-field")),
            ],
        )
        .unwrap();
        assert!(create_document_set(&definition, &interpretation, &configuration, &extra).is_err());
    }

    #[test]
    fn create_rejects_mismatched_prepared_ordinal_or_field_identity() {
        let (definition, interpretation, configuration, target) = fixture();
        let swapped_ordinals = prepared_target_with_fields(
            &target,
            vec![
                (0, target.fields()[1].provider_field_id().clone()),
                (1, target.fields()[0].provider_field_id().clone()),
            ],
        )
        .unwrap();
        assert!(
            create_document_set(
                &definition,
                &interpretation,
                &configuration,
                &swapped_ordinals,
            )
            .is_err()
        );

        let mismatched_field = prepared_target_with_fields(
            &target,
            vec![
                (0, target.fields()[0].provider_field_id().clone()),
                (1, Bytes::from_static(b"unknown-provider-field")),
            ],
        )
        .unwrap();
        assert!(
            create_document_set(
                &definition,
                &interpretation,
                &configuration,
                &mismatched_field,
            )
            .is_err()
        );
    }

    #[test]
    fn prepared_target_rejects_duplicate_or_non_dense_ordinals() {
        let (_, _, _, target) = fixture();
        assert!(
            prepared_target_with_fields(
                &target,
                vec![
                    (0, target.fields()[0].provider_field_id().clone()),
                    (0, target.fields()[1].provider_field_id().clone()),
                ],
            )
            .is_err()
        );
        assert!(
            prepared_target_with_fields(
                &target,
                vec![
                    (0, target.fields()[0].provider_field_id().clone()),
                    (2, target.fields()[1].provider_field_id().clone()),
                ],
            )
            .is_err()
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
    fn production_observation_loads_deferred_bodies_and_keeps_the_complete_source_revision() {
        let (definition, interpretation, configuration, target) = fixture();
        let create = create_document_set(&definition, &interpretation, &configuration, &target)
            .expect("create documents");
        let mut current = create
            .documents()
            .iter()
            .map(|document| stored(document, false))
            .collect::<Vec<_>>();
        current[0] = stored_deferred(&create.documents()[0]);
        let metadata_version =
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata-23"), Some(23))
                .unwrap();
        let lease = document_lease(
            &target,
            current,
            vec![create.documents()[0].clone()],
            metadata_version.clone(),
        );
        let request = ConnectorDocumentObservationRequest::try_new(
            target.owner().clone(),
            target.catalog_handle().clone(),
            target.target().clone(),
            target.object_id().clone(),
            ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
            request_context(),
        )
        .unwrap();

        let decoded = observe_current_management_documents(
            &lease,
            request,
            PersistenceDecodeBudget::default(),
        )
        .expect("sealed Current documents");
        let revision = decoded.source_revision();

        assert_eq!(decoded.definition, definition);
        assert_eq!(decoded.interpretation, interpretation);
        assert_eq!(decoded.configuration, configuration);
        assert_eq!(revision.target, *target.target());
        assert_eq!(revision.target_object_id, *target.object_id());
        assert_eq!(
            revision.metadata_version,
            MvAcceleratorCommittedVersionRevision::from_committed(&metadata_version)
        );
        assert_eq!(revision.definition_revision, decoded.definition_revision);
        assert_eq!(
            revision.interpretation_revision,
            decoded.interpretation_revision
        );
        assert_eq!(revision.publication_revision, None);
        assert_eq!(revision.publication_output_version, None);
        assert_eq!(
            revision.configuration_revision,
            decoded.configuration_revision
        );
        assert_eq!(revision.deployment_owner.as_str(), "deployment-a");
        assert_eq!(revision.process_incarnation.as_str(), "process-a");
        assert_eq!(
            revision.management_dependencies(ConnectorControlRuntimeId::from_bytes([17; 16])),
            ManagementDependencySet::new(
                *decoded.definition_revision.as_bytes(),
                *decoded.interpretation_revision.as_bytes(),
                None,
                ConnectorControlRuntimeId::from_bytes([17; 16]),
            )
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

        let frozen = FrozenConnectorDocumentObservation::try_new(
            &request,
            observation.metadata_version().clone(),
            observation.documents().to_vec(),
        )
        .unwrap();
        assert!(
            decode_frozen_publication_documents(&frozen, &[], PersistenceDecodeBudget::default())
                .is_err()
        );

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
