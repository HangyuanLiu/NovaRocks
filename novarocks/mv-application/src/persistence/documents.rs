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
use crate::persistence::codec::preflight_current_document_set_with_eligibility;
use crate::persistence::codec::{
    ConfigurationDocument, DefinitionDocument, EncodedDocument, InterpretationDocument,
    PersistenceCodecError, PublicationDocument, decode_configuration, decode_definition,
    decode_interpretation, decode_publication, encode_configuration, encode_definition,
    encode_interpretation, encode_publication, preflight_current_document_set,
};
use crate::persistence::eligibility::{
    EligibilityDocument, EligibilityState, decode_eligibility, encode_eligibility,
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
const ELIGIBILITY: &str = "eligibility";
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
    pub(crate) eligibility: Option<EligibilityDocument>,
    pub(crate) eligibility_revision: Option<DocumentRevision>,
}

impl MvObservedCurrentDocuments {
    /// Every source object identity frozen in D, including repeated SQL
    /// occurrences. Management readmission must check these against Current.
    pub fn relation_occurrences(&self) -> &[crate::persistence::codec::RelationOccurrence] {
        &self.definition.relation_occurrences
    }

    /// The revisions the lake currently holds for this target's documents.
    ///
    /// They are the exact identity of what was read, which is what a caller
    /// needs to say "this MV exists in the lake, and this is the version of it
    /// I saw". P is absent on a view that has never published.
    pub const fn definition_revision(&self) -> DocumentRevision {
        self.definition_revision
    }

    pub const fn interpretation_revision(&self) -> DocumentRevision {
        self.interpretation_revision
    }

    pub const fn publication_revision(&self) -> Option<DocumentRevision> {
        self.publication_revision
    }

    pub fn eligibility(&self) -> Option<&EligibilityDocument> {
        self.eligibility.as_ref()
    }

    pub const fn eligibility_revision(&self) -> Option<DocumentRevision> {
        self.eligibility_revision
    }

    /// The independently mutable configuration this target currently holds.
    pub fn configuration(&self) -> &ConfigurationDocument {
        &self.configuration
    }

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
            eligibility_revision: self.eligibility_revision,
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

impl From<super::identity::IdentityError> for MvDocumentError {
    fn from(error: super::identity::IdentityError) -> Self {
        PersistenceCodecError::from(error).into()
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

/// Publish a new immutable L together with P in one target commit. A managed
/// repartition changes the interpretation of the target's physical layout;
/// P must reference that exact L rather than the previously current one.
pub fn repartition_document_set(
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
    let interpretation = connector_document(
        INTERPRETATION,
        interpretation,
        vec![ConnectorDocumentReference::try_new(
            REFERENCES_DEFINITION,
            definition_id.clone(),
        )?],
        ConnectorDocumentAttachment::TableMetadata,
    )?;
    let publication = connector_document(
        PUBLICATION,
        publication_encoded,
        vec![
            ConnectorDocumentReference::try_new(REFERENCES_DEFINITION, definition_id)?,
            ConnectorDocumentReference::try_new(REFERENCES_INTERPRETATION, interpretation_id)?,
        ],
        ConnectorDocumentAttachment::CommitOutput,
    )?;
    ConnectorDocumentSet::try_new(vec![interpretation, publication]).map_err(Into::into)
}

/// Creates the C-only set used by an update that changes no MV semantics.
///
/// C is the one document a target's owner may rewrite on its own: D and L are
/// the computation and are immutable for the life of a generation, and P
/// belongs to a publication. An update that carries only C therefore says
/// exactly "nothing about what this view computes has changed", which is what
/// an ownership registration and a configuration change both need to say.
pub fn configuration_document_set(
    configuration: &ConfigurationDocument,
) -> Result<ConnectorDocumentSet, MvDocumentError> {
    let configuration = encode_configuration(configuration)?;
    let configuration = connector_document(
        CONFIGURATION,
        configuration,
        Vec::new(),
        ConnectorDocumentAttachment::TableMetadata,
    )?;
    ConnectorDocumentSet::try_new(vec![configuration]).map_err(Into::into)
}

/// Writes an independently mutable, exact maintenance eligibility control fact.
pub fn eligibility_document_set(
    eligibility: &EligibilityDocument,
) -> Result<ConnectorDocumentSet, MvDocumentError> {
    let document = connector_document(
        ELIGIBILITY,
        encode_eligibility(eligibility)?,
        Vec::new(),
        ConnectorDocumentAttachment::TableMetadata,
    )?;
    ConnectorDocumentSet::try_new(vec![document]).map_err(Into::into)
}

/// Publishes data, P and Eligible as one atomic document set. A metadata-only
/// management update must use eligibility_document_set and preserve its cause.
pub fn publication_with_eligibility_document_set(
    definition: &DefinitionDocument,
    interpretation: &InterpretationDocument,
    publication: &PublicationDocument,
    eligibility: &EligibilityDocument,
) -> Result<ConnectorDocumentSet, MvDocumentError> {
    let p = encode_publication(publication)?;
    if eligibility.state != EligibilityState::Eligible
        || eligibility.binding.object_id != publication.output.object_id
        || eligibility.binding.publication_id != publication.publication_id
        || eligibility.binding.publication_revision != p.revision()
        || eligibility.binding.computation_identity != definition.computation_identity
    {
        return Err(MvDocumentError::Contract(
            "publication eligibility must bind exact new P and computation".into(),
        ));
    }
    let mut documents = publication_document_set(definition, interpretation, publication)?
        .documents()
        .to_vec();
    documents.extend(
        eligibility_document_set(eligibility)?
            .documents()
            .iter()
            .cloned(),
    );
    ConnectorDocumentSet::try_new(documents).map_err(Into::into)
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
        eligibility: decoded.eligibility,
        eligibility_revision: decoded.eligibility_revision,
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

/// Exact Current facts that authorize only retirement of a managed object.
/// No interpretation or query-ready projection can be obtained from this value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvCurrentDropDescriptor {
    source_revision: MvAcceleratorSourceRevision,
    configuration: ConfigurationDocument,
    legacy_nonaggregate: bool,
}

impl MvCurrentDropDescriptor {
    pub fn source_revision(&self) -> &MvAcceleratorSourceRevision {
        &self.source_revision
    }
    pub fn configuration(&self) -> &ConfigurationDocument {
        &self.configuration
    }
    pub fn requires_drop_recreate(&self) -> bool {
        self.legacy_nonaggregate
    }
    pub fn is_legacy_nonaggregate(&self) -> bool {
        self.requires_drop_recreate()
    }

    pub fn management_dependencies(
        &self,
        runtime: novarocks_spi::connector::ConnectorControlRuntimeId,
    ) -> crate::management::ManagementDependencySet {
        self.source_revision.management_dependencies(runtime)
    }
}

/// Exact dependency facts for DROP. This value deliberately exposes neither a
/// query definition nor outputs or a row interpretation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvDropDefinitionFacts {
    pub created_at_ms: u64,
    pub computation_identity: super::identity::ComputationIdentity,
    pub relation_occurrences: Vec<MvDropRelationFacts>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvDropRelationFacts {
    pub occurrence_id: u32,
    pub catalog_at_binding: String,
    pub namespace_at_binding: String,
    pub relation_at_binding: String,
    pub object_id: super::identity::ObjectIdentity,
}

/// Inspect the exact raw Current headers for DROP. Only the closed retired
/// complex grammar and retired nonaggregate interpretation are admitted here.
/// This never creates substitute child types or a query-ready projection.
pub(crate) fn decode_drop_document_bodies(
    definition_bytes: &[u8],
    interpretation_bytes: &[u8],
    publication_bytes: Option<&[u8]>,
    configuration_bytes: &[u8],
    budget: PersistenceDecodeBudget,
) -> Result<
    (
        MvDropDefinitionFacts,
        ConfigurationDocument,
        bool,
        super::identity::ObjectIdentity,
    ),
    MvDocumentError,
> {
    use super::codec::{logical_type::TypeCodecError, require_version, required};
    use prost::Message;
    preflight_current_document_set(
        definition_bytes,
        interpretation_bytes,
        publication_bytes,
        configuration_bytes,
        budget,
    )?;
    let raw_d = super::generated::DefinitionDocument::decode(definition_bytes)
        .map_err(PersistenceCodecError::ProtobufDecode)?;
    let raw_l = super::generated::InterpretationDocument::decode(interpretation_bytes)
        .map_err(PersistenceCodecError::ProtobufDecode)?;
    require_version("definition", raw_d.format_version)?;
    require_version("interpretation", raw_l.format_version)?;
    if raw_d.encode_to_vec() != definition_bytes || raw_l.encode_to_vec() != interpretation_bytes {
        return Err(MvDocumentError::Contract(
            "DROP D/L do not use canonical bytes".into(),
        ));
    }
    let d_revision = DocumentRevision::from_canonical_bytes(definition_bytes);
    let l_revision = DocumentRevision::from_canonical_bytes(interpretation_bytes);
    let computation_identity = super::identity::ComputationIdentity::try_from_bytes(&required(
        raw_d.computation_identity.clone(),
        "definition.computation_identity",
    )?)?;
    let mut identity_d = raw_d.clone();
    identity_d.format_version = None;
    identity_d.computation_identity = None;
    identity_d.created_at_ms = None;
    if super::identity::ComputationIdentity::from_canonical_bytes(&identity_d.encode_to_vec())
        != computation_identity
    {
        return Err(MvDocumentError::Contract(
            "DROP D computation identity differs from its exact raw semantics".into(),
        ));
    }
    if raw_l.definition_revision.as_deref() != Some(d_revision.as_bytes().as_slice())
        || raw_l.computation_identity.as_deref() != Some(computation_identity.as_bytes().as_slice())
    {
        return Err(MvDocumentError::Contract(
            "DROP L does not bind exact D/computation".into(),
        ));
    }
    let mut retired_complex = false;
    let mut inspect_type = |value: Option<&str>| -> Result<(), MvDocumentError> {
        let value = value.ok_or(PersistenceCodecError::MissingField("type_signature"))?;
        match super::codec::MvLogicalType::decode_signature(value) {
            Ok(_) => Ok(()),
            Err(TypeCodecError::RetiredComplex) => {
                retired_complex = true;
                Ok(())
            }
            Err(error) => Err(PersistenceCodecError::LogicalType(error).into()),
        }
    };
    for occurrence in &raw_d.relation_occurrences {
        for field in &occurrence.fields {
            inspect_type(field.type_signature.as_deref())?;
        }
    }
    for output in &raw_d.outputs {
        inspect_type(output.type_signature.as_deref())?;
    }
    for output in &raw_l.outputs {
        inspect_type(output.type_signature.as_deref())?;
    }
    for state in &raw_l.state_slots {
        inspect_type(state.type_signature.as_deref())?;
    }
    let target = raw_l
        .target
        .as_ref()
        .ok_or(PersistenceCodecError::MissingField("interpretation.target"))?;
    for field in &target.fields {
        inspect_type(field.type_signature.as_deref())?;
    }
    let object = super::identity::ObjectIdentity::try_new(required(
        target.object_id.clone(),
        "interpretation.target.object_id",
    )?)?;
    super::identity::SchemaVersion::try_new(required(
        target.schema_version.clone(),
        "interpretation.target.schema_version",
    )?)?;
    super::identity::PartitionSpecVersion::try_new(required(
        target.partition_spec_version.clone(),
        "interpretation.target.partition_spec_version",
    )?)?;
    if raw_l.outputs.is_empty()
        || target.fields.is_empty()
        || raw_l.state_slots.iter().any(|state| {
            !matches!(state.role, Some(1..=4))
                || state.encoding != Some(1)
                || state.nullable.is_none()
        })
        || target
            .fields
            .iter()
            .any(|field| !matches!(field.kind, Some(1..=4)) || field.nullable.is_none())
        || raw_l.outputs.iter().any(|output| output.nullable.is_none())
        || raw_l.apply_key.as_ref().is_some_and(|key| {
            !matches!(key.kind, Some(1..=3)) || (key.kind == Some(3) && key.components.is_empty())
        })
    {
        return Err(MvDocumentError::Contract(
            "DROP L has unknown or incomplete interpretation facts".into(),
        ));
    }
    // Physical schema payloads carry opaque identity, whereas logical outputs
    // and states do not. Compare their semantic type; retirement spellings have
    // no reconstructed tree and must instead match exactly.
    let same_logical_type = |left: Option<&str>, right: Option<&str>, opaque_state: bool| -> bool {
        let (Some(left), Some(right)) = (left, right) else {
            return false;
        };
        match (
            super::codec::MvLogicalType::decode_signature(left),
            super::codec::MvLogicalType::decode_signature(right),
        ) {
            (Ok(left), Ok(right)) => super::validation::logical_type_matches_iceberg_read(
                right.logical_type(),
                left.logical_type(),
                opaque_state,
            ),
            (Err(TypeCodecError::RetiredComplex), Err(TypeCodecError::RetiredComplex)) => {
                left == right
            }
            _ => false,
        }
    };
    let mut target_logical_ids = std::collections::BTreeSet::new();
    for field in &target.fields {
        super::identity::FieldIdentity::try_new(required(
            field.target_field_id.clone(),
            "interpretation.target.field.target_field_id",
        )?)?;
        let id = required(
            field.logical_id.clone(),
            "interpretation.target.field.logical_id",
        )?;
        if id.is_empty() || !target_logical_ids.insert((field.kind, id)) {
            return Err(MvDocumentError::Contract(
                "DROP L has empty or duplicate logical bindings".into(),
            ));
        }
    }
    for field in &raw_l.outputs {
        super::identity::OutputIdentity::try_new(required(
            field.output_id.clone(),
            "interpretation.output.output_id",
        )?)?;
        super::identity::FieldIdentity::try_new(required(
            field.target_field_id.clone(),
            "interpretation.output.target_field_id",
        )?)?;
        if !target.fields.iter().any(|bound| {
            bound.kind == Some(1)
                && bound.logical_id == field.output_id
                && bound.target_field_id == field.target_field_id
                && bound.nullable == field.nullable
                && same_logical_type(
                    bound.type_signature.as_deref(),
                    field.type_signature.as_deref(),
                    false,
                )
        }) {
            return Err(MvDocumentError::Contract(
                "DROP L output differs from its exact physical binding".into(),
            ));
        }
    }
    for state in &raw_l.state_slots {
        super::identity::StateSlotIdentity::try_new(required(
            state.slot_id.clone(),
            "interpretation.state_slot.slot_id",
        )?)?;
        super::identity::FieldIdentity::try_new(required(
            state.target_field_id.clone(),
            "interpretation.state_slot.target_field_id",
        )?)?;
        if !target.fields.iter().any(|bound| {
            bound.kind == Some(2)
                && bound.logical_id == state.slot_id
                && bound.target_field_id == state.target_field_id
                && bound.nullable == state.nullable
                && same_logical_type(
                    bound.type_signature.as_deref(),
                    state.type_signature.as_deref(),
                    matches!(state.role, Some(1..=3))
                        && state.encoding == Some(1)
                        && state.nullable == Some(false),
                )
        }) {
            return Err(MvDocumentError::Contract(
                "DROP L state differs from its exact physical binding".into(),
            ));
        }
    }
    if target
        .partition_fields
        .iter()
        .any(|field| !matches!(field.transform, Some(1..=8)))
    {
        return Err(MvDocumentError::Contract(
            "DROP L contains an unknown partition transform".into(),
        ));
    }
    // Complete types still pass every ordinary semantic validator. A retired
    // type can authorize DROP only, after exact raw header/dependency checks.
    let legacy_nonaggregate = match decode_interpretation(interpretation_bytes, budget) {
        Ok(_) => false,
        Err(PersistenceCodecError::LegacyNonAggregateInterpretation) => true,
        Err(PersistenceCodecError::LogicalType(TypeCodecError::RetiredComplex))
            if retired_complex =>
        {
            false
        }
        Err(error) => return Err(error.into()),
    };
    if !retired_complex {
        let definition = decode_definition(definition_bytes, budget)?;
        if !legacy_nonaggregate {
            let interpretation = decode_interpretation(interpretation_bytes, budget)?;
            super::validation::validate_definition_interpretation(
                &definition,
                d_revision,
                &interpretation,
            )?;
        } else if !raw_l.aggregates.is_empty() {
            return Err(MvDocumentError::Contract(
                "retired nonaggregate DROP L contains aggregates".into(),
            ));
        }
    }
    let query = raw_d
        .query
        .as_ref()
        .ok_or(PersistenceCodecError::MissingField("definition.query"))?;
    let resolution = query
        .resolution
        .as_ref()
        .ok_or(PersistenceCodecError::MissingField(
            "definition.query.resolution",
        ))?;
    if query.dialect != Some(1)
        || query.effective_sql.as_deref().is_none_or(str::is_empty)
        || resolution
            .default_catalog
            .as_deref()
            .is_none_or(str::is_empty)
        || resolution
            .default_namespace
            .as_deref()
            .is_none_or(str::is_empty)
        || raw_d.outputs.is_empty()
        || raw_d.relation_occurrences.is_empty()
    {
        return Err(MvDocumentError::Contract(
            "DROP D lacks its required exact definition headers".into(),
        ));
    }
    let mut occurrences = Vec::with_capacity(raw_d.relation_occurrences.len());
    let mut occurrence_ids = BTreeMap::new();
    for occurrence in &raw_d.relation_occurrences {
        let occurrence_id = required(
            occurrence.occurrence_id,
            "definition.relation.occurrence_id",
        )?;
        let object_id = super::identity::ObjectIdentity::try_new(required(
            occurrence.object_id.clone(),
            "definition.relation.object_id",
        )?)?;
        super::identity::SchemaVersion::try_new(required(
            occurrence.schema_version.clone(),
            "definition.relation.schema_version",
        )?)?;
        if occurrence_ids
            .insert(occurrence_id, object_id.clone())
            .is_some()
            || occurrence.fields.is_empty()
        {
            return Err(MvDocumentError::Contract(
                "DROP D has missing or duplicate occurrence facts".into(),
            ));
        }
        let mut fields = std::collections::BTreeSet::new();
        for field in &occurrence.fields {
            let id = super::identity::FieldIdentity::try_new(required(
                field.field_id.clone(),
                "definition.relation.field_id",
            )?)?;
            if !fields.insert(id)
                || field.name_at_binding.as_deref().is_none_or(str::is_empty)
                || field.nullable.is_none()
            {
                return Err(MvDocumentError::Contract(
                    "DROP D has invalid exact source field facts".into(),
                ));
            }
        }
        if occurrence
            .qualifier_at_binding
            .as_deref()
            .is_none_or(|value| value.is_empty() || value.contains('\0'))
        {
            return Err(MvDocumentError::Contract(
                "DROP D has an invalid frozen qualifier".into(),
            ));
        }
        let text = |value: &Option<String>| -> Result<String, MvDocumentError> {
            value
                .as_ref()
                .filter(|value| !value.is_empty() && !value.contains('\0'))
                .cloned()
                .ok_or_else(|| {
                    MvDocumentError::Contract("DROP D has an invalid dependency locator".into())
                })
        };
        occurrences.push(MvDropRelationFacts {
            occurrence_id,
            object_id,
            catalog_at_binding: text(&occurrence.catalog_at_binding)?,
            namespace_at_binding: text(&occurrence.namespace_at_binding)?,
            relation_at_binding: text(&occurrence.relation_at_binding)?,
        });
    }
    let mut output_ids = std::collections::BTreeSet::new();
    for output in &raw_d.outputs {
        let id = super::identity::OutputIdentity::try_new(required(
            output.output_id.clone(),
            "definition.output.output_id",
        )?)?;
        let expression = output
            .expression
            .as_ref()
            .ok_or(PersistenceCodecError::MissingField(
                "definition.output.expression",
            ))?;
        if !output_ids.insert(id)
            || output.name.as_deref().is_none_or(str::is_empty)
            || output.nullable.is_none()
            || !matches!(expression.kind, Some(1..=5))
        {
            return Err(MvDocumentError::Contract(
                "DROP D has invalid exact output facts".into(),
            ));
        }
        if (matches!(expression.kind, Some(4 | 5))
            && expression
                .function_identity
                .as_deref()
                .is_none_or(str::is_empty))
            || (expression.kind == Some(2) && !expression.source_fields.is_empty())
        {
            return Err(MvDocumentError::Contract(
                "DROP D has an incomplete expression declaration".into(),
            ));
        }
        for reference in &expression.source_fields {
            let occurrence = raw_d
                .relation_occurrences
                .iter()
                .find(|occurrence| occurrence.occurrence_id == reference.occurrence_id);
            if occurrence.is_none_or(|occurrence| {
                !occurrence
                    .fields
                    .iter()
                    .any(|field| field.field_id == reference.field_id)
            }) {
                return Err(MvDocumentError::Contract(
                    "DROP D output references a missing exact source field".into(),
                ));
            }
        }
    }
    let definition = MvDropDefinitionFacts {
        created_at_ms: required(raw_d.created_at_ms, "definition.created_at_ms")?,
        computation_identity,
        relation_occurrences: occurrences,
    };
    if let Some(bytes) = publication_bytes {
        let publication = decode_publication(bytes, budget)?;
        if publication.definition_revision != d_revision
            || publication.interpretation_revision != l_revision
            || publication.output.object_id != object
            || publication.inputs.len() != definition.relation_occurrences.len()
            || publication
                .inputs
                .iter()
                .zip(&definition.relation_occurrences)
                .any(|(input, occurrence)| {
                    input.relation_occurrence_id != occurrence.occurrence_id
                        || input.object_id != occurrence.object_id
                })
        {
            return Err(MvDocumentError::Contract(
                "DROP P does not bind exact D/L/source/target objects".into(),
            ));
        }
    }
    Ok((
        definition,
        decode_configuration(configuration_bytes, budget)?,
        legacy_nonaggregate || retired_complex,
        object,
    ))
}

/// Observe a sealed Current package for explicit DROP. This accepts the typed
/// retired nonaggregate format only after validating all exact envelope links.
pub fn observe_current_drop_descriptor(
    lease: &ConnectorDocumentStorageLease,
    request: ConnectorDocumentObservationRequest,
    budget: PersistenceDecodeBudget,
) -> Result<
    (
        ConnectorDocumentManagementObservation,
        MvCurrentDropDescriptor,
    ),
    MvDocumentError,
> {
    let retained = request.clone();
    let observation = lease.observe_current_management(request)?;
    let mut loaded = Vec::new();
    for stored in observation.documents() {
        if matches!(
            stored.carrier(),
            ConnectorDocumentCarrier::DeferredContent(_)
        ) {
            loaded.push(lease.load_document(
                retained.try_load_request(stored.clone(), retained.context().clone())?,
            )?);
        }
    }
    let descriptor = decode_current_drop_descriptor(&observation, &loaded, budget)?;
    Ok((observation, descriptor))
}

fn decode_current_drop_descriptor(
    observation: &ConnectorDocumentManagementObservation,
    loaded: &[ConnectorDocument],
    budget: PersistenceDecodeBudget,
) -> Result<MvCurrentDropDescriptor, MvDocumentError> {
    observation.validate_sealed()?;
    if observation.marker().kind() != MANAGED_MV_KIND {
        return Err(MvDocumentError::Contract(
            "DROP Current marker is not a materialized view".into(),
        ));
    }
    let loaded = validate_loaded_documents(observation.documents(), loaded)?;
    let mut by_name = BTreeMap::new();
    for stored in observation.documents() {
        validate_envelope(stored)?;
        if by_name
            .insert(stored.id().name().as_str(), stored)
            .is_some()
        {
            return Err(MvDocumentError::Contract(
                "DROP Current has duplicate MV documents".into(),
            ));
        }
        let body = resolved_content(stored, &loaded)?;
        if DocumentRevision::from_canonical_bytes(body) != revision(stored) {
            return Err(MvDocumentError::Contract(
                "DROP document body differs from its exact revision".into(),
            ));
        }
    }
    if by_name.len()
        != 3 + usize::from(by_name.contains_key(PUBLICATION))
            + usize::from(by_name.contains_key(ELIGIBILITY))
    {
        return Err(MvDocumentError::Contract(
            "DROP Current must contain exactly D/L/C and optional P/eligibility".into(),
        ));
    }
    let d = required_document(&by_name, DEFINITION)?;
    let l = required_document(&by_name, INTERPRETATION)?;
    let c = required_document(&by_name, CONFIGURATION)?;
    for stored in [d, l, c] {
        require_attachment(stored, false)?;
    }
    require_exact_references(d, &[])?;
    require_exact_references(c, &[])?;
    require_exact_references(l, &[(REFERENCES_DEFINITION, d.id())])?;
    let p = by_name.get(PUBLICATION).copied();
    if let Some(p) = p {
        require_attachment(p, true)?;
        require_exact_references(
            p,
            &[
                (REFERENCES_DEFINITION, d.id()),
                (REFERENCES_INTERPRETATION, l.id()),
            ],
        )?;
    }
    let d_bytes = resolved_content(d, &loaded)?;
    let l_bytes = resolved_content(l, &loaded)?;
    let c_bytes = resolved_content(c, &loaded)?;
    let p_bytes = p.map(|p| resolved_content(p, &loaded)).transpose()?;
    let e = by_name.get(ELIGIBILITY).copied();
    let e_bytes = e.map(|e| resolved_content(e, &loaded)).transpose()?;
    preflight_current_document_set_with_eligibility(
        d_bytes, l_bytes, p_bytes, c_bytes, e_bytes, budget,
    )?;
    let (definition, configuration, legacy_nonaggregate, object) =
        decode_drop_document_bodies(d_bytes, l_bytes, p_bytes, c_bytes, budget)?;
    if object.as_bytes() != observation.object_id().as_bytes().as_ref() {
        return Err(MvDocumentError::Contract(
            "DROP L names a different target object".into(),
        ));
    }
    if let Some(e) = e {
        require_attachment(e, false)?;
        require_exact_references(e, &[])?;
        let eligibility = decode_eligibility(e_bytes.expect("resolved E"), budget)?;
        if eligibility.binding.object_id != object
            || eligibility.binding.computation_identity != definition.computation_identity
        {
            return Err(MvDocumentError::Contract(
                "DROP eligibility belongs to another target/computation".into(),
            ));
        }
        if matches!(eligibility.state, EligibilityState::Eligible) {
            let publication = p_bytes
                .map(|bytes| decode_publication(bytes, budget))
                .transpose()?;
            if publication.is_none_or(|p| eligibility.binding.publication_id != p.publication_id)
                || Some(eligibility.binding.publication_revision) != p.map(revision)
            {
                return Err(MvDocumentError::Contract(
                    "DROP Eligible does not bind exact Current P".into(),
                ));
            }
        }
    }
    Ok(MvCurrentDropDescriptor {
        source_revision: MvAcceleratorSourceRevision {
            target: observation.target().clone(),
            target_object_id: observation.object_id().clone(),
            metadata_version: MvAcceleratorCommittedVersionRevision::from_committed(
                observation.metadata_version(),
            ),
            definition_revision: revision(d),
            interpretation_revision: revision(l),
            configuration_revision: revision(c),
            publication_revision: p.map(revision),
            publication_output_version: p.map(|p| match p.attachment() {
                ConnectorStoredDocumentAttachment::ExactOutput(output) => {
                    MvAcceleratorCommittedVersionRevision::from_committed(output)
                }
                _ => unreachable!("validated exact output"),
            }),
            eligibility_revision: e.map(revision),
            deployment_owner: DeploymentOwner::parse(observation.marker().owner())
                .map_err(|e| MvDocumentError::Contract(e.to_string()))?,
            process_incarnation: ProcessIncarnation::parse(observation.marker().incarnation())
                .map_err(|e| MvDocumentError::Contract(e.to_string()))?,
        },
        configuration,
        legacy_nonaggregate,
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
            DEFINITION | INTERPRETATION | PUBLICATION | CONFIGURATION | ELIGIBILITY
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
    eligibility: Option<EligibilityDocument>,
    eligibility_revision: Option<DocumentRevision>,
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
    let expected = 3
        + usize::from(by_name.contains_key(PUBLICATION))
        + usize::from(by_name.contains_key(ELIGIBILITY));
    if by_name.len() != expected {
        return Err(MvDocumentError::Contract(
            "Current must contain exactly D/L/C and optional P/eligibility".to_string(),
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
    let eligibility_content = by_name
        .get(ELIGIBILITY)
        .map(|stored| resolved_content(stored, &loaded_by_id))
        .transpose()?;
    preflight_current_document_set_with_eligibility(
        definition_content,
        interpretation_content,
        publication_content,
        configuration_content,
        eligibility_content,
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

    let (eligibility, eligibility_revision) = match by_name.get(ELIGIBILITY) {
        Some(stored) => {
            require_attachment(stored, false)?;
            require_exact_references(stored, &[])?;
            let eligibility =
                decode_eligibility(eligibility_content.expect("resolved eligibility"), budget)?;
            if eligibility.binding.object_id != interpretation.target.object_id
                || eligibility.binding.computation_identity != definition.computation_identity
            {
                return Err(MvDocumentError::Contract(
                    "eligibility belongs to another target object or computation".into(),
                ));
            }
            if matches!(eligibility.state, EligibilityState::Eligible)
                && publication.as_ref().is_none_or(|p| {
                    eligibility.binding.publication_id != p.publication_id
                        || Some(eligibility.binding.publication_revision) != publication_revision
                })
            {
                return Err(MvDocumentError::Contract(
                    "Eligible does not bind exact Current publication".into(),
                ));
            }
            (Some(eligibility), Some(revision(stored)))
        }
        None => (None, None),
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
        eligibility,
        eligibility_revision,
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
    let prepared_partition_fields = target
        .partition_fields()
        .iter()
        .map(crate::persistence::codec::TargetPartitionFieldBinding::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(MvDocumentError::Contract)?;
    if interpretation.target.partition_fields != prepared_partition_fields {
        return Err(MvDocumentError::Contract(
            "interpretation partition binding does not match the provider-prepared target"
                .to_string(),
        ));
    }
    // Every prepared column must be bound, and every binding must name a
    // prepared column.
    //
    // This compares what is bound, not where it sits. The target's column
    // order is the one the statement asked for, while L orders its bindings by
    // logical identity and lets several typed identities share one physical
    // column; the two orders have no reason to coincide, and requiring it made
    // any view of more than a couple of columns unconstructible. The prepared
    // list's own shape -- dense ordinals, distinct field identities -- is
    // guaranteed by the type that carries it.
    let prepared_fields = target
        .fields()
        .iter()
        .map(|prepared| prepared.provider_field_id().as_ref())
        .collect::<BTreeSet<_>>();
    let bound_fields = interpretation
        .target
        .fields
        .iter()
        .map(|field| field.target_field_id.as_bytes())
        .collect::<BTreeSet<_>>();
    if prepared_fields != bound_fields {
        return Err(MvDocumentError::Contract(
            "interpretation target bindings and the provider-prepared columns are not the same set"
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
            DEFINITION | INTERPRETATION | PUBLICATION | CONFIGURATION | ELIGIBILITY
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
        BranchInterpretation, ExpressionKind, ExpressionShape, OutputBinding, OutputDefinition,
        PhysicalFieldBinding, PhysicalFieldLogicalIdentity, PublicationInput, PublicationKind,
        PublicationOutput, PublicationStatistics, QueryDialect, QuerySource, RefreshPolicy,
        RelationOccurrence, ResolutionContext, SourceFieldBinding, SourceFieldReference,
        TargetBinding, build_definition,
    };
    use crate::persistence::identity::{
        BranchIdentity, FieldIdentity, NativeDataVersion, ObjectIdentity, OutputIdentity,
        PartitionSpecVersion, PublicationIdentity, SchemaVersion,
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
        CatalogHandle, CatalogVersion, ConnectorCommittedVersion, ConnectorControlRuntimeId,
        ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorMutationOperationId, ConnectorPreparedCreateFieldBinding,
        ConnectorProviderBindingKey, ConnectorProviderId, ConnectorRequestContext,
        ConnectorStopOwner, ConnectorTableIdentity, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
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
        let apply_logical = opaque(6, OutputIdentity::try_new);
        let target_apply = opaque(7, FieldIdentity::try_new);
        let target_schema = opaque(8, SchemaVersion::try_new);
        let target_spec = opaque(9, PartitionSpecVersion::try_new);
        let definition = build_definition(
            1_700_000_000_000,
            QuerySource {
                effective_sql: "SELECT o.order_id, 1 AS tag FROM ice.sales.orders o".to_string(),
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
                    data_type: crate::persistence::codec::MvLogicalType::decode_signature("bigint")
                        .expect("valid fixture type"),
                    nullable: false,
                }],
            }],
            vec![
                OutputDefinition {
                    output_id: output.clone(),
                    name: "order_id".to_string(),
                    data_type: crate::persistence::codec::MvLogicalType::decode_signature("bigint")
                        .expect("valid fixture type"),
                    nullable: false,
                    expression: ExpressionShape {
                        kind: ExpressionKind::Field,
                        function_identity: None,
                        source_fields: vec![SourceFieldReference {
                            occurrence_id: 0,
                            field_id: source_field,
                        }],
                    },
                },
                OutputDefinition {
                    output_id: apply_logical.clone(),
                    name: "tag".into(),
                    data_type: crate::persistence::codec::MvLogicalType::decode_signature("bigint")
                        .expect("valid fixture type"),
                    nullable: false,
                    expression: ExpressionShape {
                        kind: ExpressionKind::Literal,
                        function_identity: None,
                        source_fields: Vec::new(),
                    },
                },
            ],
        )
        .expect("definition");
        let definition_revision = encode_definition(&definition).unwrap().revision();
        let interpretation = InterpretationDocument {
            definition_revision,
            computation_identity: definition.computation_identity,
            outputs: vec![
                OutputBinding {
                    output_id: output.clone(),
                    target_field_id: target_output.clone(),
                    data_type: crate::persistence::codec::MvLogicalType::decode_signature("bigint")
                        .expect("valid fixture type"),
                    nullable: false,
                },
                OutputBinding {
                    output_id: apply_logical.clone(),
                    target_field_id: target_apply.clone(),
                    data_type: crate::persistence::codec::MvLogicalType::decode_signature("bigint")
                        .expect("valid fixture type"),
                    nullable: false,
                },
            ],
            state_slots: Vec::new(),
            apply_key: None,
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
                        data_type: crate::persistence::codec::MvLogicalType::decode_signature(
                            "bigint",
                        )
                        .expect("valid fixture type"),
                        nullable: false,
                    },
                    PhysicalFieldBinding {
                        logical_identity: PhysicalFieldLogicalIdentity::Output(apply_logical),
                        target_field_id: target_apply.clone(),
                        data_type: crate::persistence::codec::MvLogicalType::decode_signature(
                            "bigint",
                        )
                        .expect("valid fixture type"),
                        nullable: false,
                    },
                ],
                partition_fields: Vec::new(),
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
                    novarocks_type_contract::LogicalType::Binary,
                    Some(novarocks_type_contract::LogicalType::Binary),
                    Bytes::from_static(b"opaque-provider-field"),
                    false,
                )
                .unwrap(),
                ConnectorPreparedCreateFieldBinding::try_new(
                    1,
                    Bytes::copy_from_slice(target_apply.as_bytes()),
                    "apply".to_string(),
                    novarocks_type_contract::LogicalType::Binary,
                    Some(novarocks_type_contract::LogicalType::Binary),
                    Bytes::from_static(b"opaque-provider-field"),
                    false,
                )
                .unwrap(),
            ],
            Vec::new(),
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
                        novarocks_type_contract::LogicalType::Binary,
                        Some(novarocks_type_contract::LogicalType::Binary),
                        Bytes::from_static(b"opaque-provider-field"),
                        false,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            target.partition_fields().to_vec(),
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

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
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
        let eligibility = EligibilityDocument {
            binding: crate::persistence::eligibility::EligibilityBinding {
                object_id: publication.output.object_id.clone(),
                publication_id: publication.publication_id.clone(),
                publication_revision: encode_publication(&publication).unwrap().revision(),
                computation_identity: definition.computation_identity,
                content_contract_identity:
                    crate::persistence::eligibility::native_result_content_v1_identity(),
                generation: 1,
            },
            state: EligibilityState::Eligible,
        };
        let publication_set = publication_with_eligibility_document_set(
            &definition,
            &interpretation,
            &publication,
            &eligibility,
        )
        .expect("atomic publication eligibility");
        let mut current = create
            .documents()
            .iter()
            .map(|document| stored(document, false))
            .collect::<Vec<_>>();
        current.push(stored(&publication_set.documents()[0], true));
        current.push(stored(&publication_set.documents()[1], false));

        let decoded = decode_document_slice(&current, &[], PersistenceDecodeBudget::default())
            .expect("exact Current");
        assert_eq!(decoded.eligibility, Some(eligibility.clone()));
        assert_eq!(
            decoded.eligibility_revision,
            Some(encode_eligibility(&eligibility).unwrap().revision())
        );
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
    fn repartition_publishes_new_interpretation_and_its_exact_output_together() {
        let (definition, mut interpretation, configuration, target) = fixture();
        let create = create_document_set(&definition, &interpretation, &configuration, &target)
            .expect("create documents");
        interpretation.target.partition_spec_version =
            PartitionSpecVersion::try_new(vec![17]).unwrap();
        let publication = PublicationDocument {
            publication_prepared_at_ms: 1_700_000_001_000,
            publication_id: PublicationIdentity::try_new(vec![2]).unwrap(),
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
            kind: PublicationKind::Repartition,
            statistics: PublicationStatistics::default(),
        };
        let repartition = repartition_document_set(&definition, &interpretation, &publication)
            .expect("repartition documents");
        assert_eq!(
            repartition
                .documents()
                .iter()
                .map(|document| document.id().name().as_str())
                .collect::<Vec<_>>(),
            [INTERPRETATION, PUBLICATION]
        );
        let mut current = create
            .documents()
            .iter()
            .filter(|document| document.id().name().as_str() != INTERPRETATION)
            .map(|document| stored(document, false))
            .collect::<Vec<_>>();
        current.push(stored(&repartition.documents()[0], false));
        current.push(stored(&repartition.documents()[1], true));
        let decoded = decode_document_slice(&current, &[], PersistenceDecodeBudget::default())
            .expect("repartition Current");
        assert_eq!(decoded.interpretation, interpretation);
        assert_eq!(decoded.publication, Some(publication));
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
    fn create_rejects_nonaggregate_branch_target_bindings() {
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
                data_type: crate::persistence::codec::MvLogicalType::decode_signature("bigint")
                    .expect("valid fixture type"),
                nullable: false,
            },
            PhysicalFieldBinding {
                logical_identity: PhysicalFieldLogicalIdentity::Branch(second_branch),
                target_field_id: shared_field_id,
                data_type: crate::persistence::codec::MvLogicalType::decode_signature("bigint")
                    .expect("valid fixture type"),
                nullable: false,
            },
        ]);

        assert!(
            create_document_set(&definition, &interpretation, &configuration, &target).is_err()
        );
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
    fn create_accepts_the_providers_own_column_order() {
        let (definition, interpretation, configuration, target) = fixture();
        // Which physical column sits at which ordinal is the provider's fact
        // about the table it staged, and L binds by field identity rather than
        // position. Demanding that the two orders coincide is what used to
        // make any view of more than a couple of columns unconstructible.
        let swapped_ordinals = prepared_target_with_fields(
            &target,
            vec![
                (0, target.fields()[1].provider_field_id().clone()),
                (1, target.fields()[0].provider_field_id().clone()),
            ],
        )
        .unwrap();

        create_document_set(
            &definition,
            &interpretation,
            &configuration,
            &swapped_ordinals,
        )
        .expect("the same columns in the provider's own order still describe this target");
    }

    #[test]
    fn create_rejects_a_binding_the_provider_never_prepared() {
        let (definition, interpretation, configuration, target) = fixture();

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
    fn legacy_drop_documents() -> (
        ConnectorDocumentStorageLease,
        ConnectorDocumentObservationRequest,
    ) {
        use prost::Message;
        let (definition, interpretation, configuration, target) = fixture();
        let create =
            create_document_set(&definition, &interpretation, &configuration, &target).unwrap();
        let mut raw = super::super::generated::InterpretationDocument::decode(
            encode_interpretation(&interpretation).unwrap().as_bytes(),
        )
        .unwrap();
        raw.apply_key = Some(super::super::generated::ApplyKey {
            kind: Some(1),
            components: vec![],
        });
        let legacy = ConnectorDocument::try_new(
            owner().unwrap(),
            document_name(INTERPRETATION).unwrap(),
            document_format(INTERPRETATION).unwrap(),
            Bytes::from(raw.encode_to_vec()),
            vec![
                ConnectorDocumentReference::try_new(
                    REFERENCES_DEFINITION,
                    create.documents()[0].id().clone(),
                )
                .unwrap(),
            ],
            ConnectorDocumentAttachment::TableMetadata,
        )
        .unwrap();
        let stored_documents = vec![
            stored(&create.documents()[0], false),
            stored(&legacy, false),
            stored(&create.documents()[2], false),
        ];
        let lease = document_lease(
            &target,
            stored_documents,
            vec![],
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"drop-metadata"), None).unwrap(),
        );
        let request = ConnectorDocumentObservationRequest::try_new(
            lease.owner().clone(),
            lease.catalog_handle().clone(),
            target.target().clone(),
            target.object_id().clone(),
            ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
            request_context(),
        )
        .unwrap();
        (lease, request)
    }

    #[test]
    fn legacy_current_is_retirable_but_cannot_become_ordinary_documents() {
        let (lease, request) = legacy_drop_documents();
        assert!(matches!(
            observe_current_management_documents(
                &lease,
                request.clone(),
                PersistenceDecodeBudget::default()
            ),
            Err(MvDocumentError::Codec(
                PersistenceCodecError::LegacyNonAggregateInterpretation
            ))
        ));
        let (observation, descriptor) =
            observe_current_drop_descriptor(&lease, request, PersistenceDecodeBudget::default())
                .unwrap();
        assert!(descriptor.is_legacy_nonaggregate());
        assert_eq!(
            &descriptor.source_revision().target_object_id,
            observation.object_id()
        );
        let entrance = crate::management::ManagementEntrance::new(
            DeploymentOwner::parse("deployment-a").unwrap(),
            ProcessIncarnation::parse("process-a").unwrap(),
        );
        let runtime = ConnectorControlRuntimeId::from_bytes([7; 16]);
        entrance
            .install_fresh_drop_target(&observation, &descriptor, runtime)
            .unwrap();
        for operation in [novarocks_spi::connector::document_storage::ConnectorDocumentManagementOperation::Publication,novarocks_spi::connector::document_storage::ConnectorDocumentManagementOperation::SingleTargetUpdate] {
            let request = crate::management::ManagementRequest::try_new(observation.catalog_handle().clone(),observation.target().clone(),Some(observation.object_id().clone()),operation,
                Some(descriptor.management_dependencies(runtime)),crate::management::EffectScope::CATALOG_COMMIT).unwrap();
            assert!(entrance.acquire(request,|| false).is_err());
        }
        let request = crate::management::ManagementRequest::try_new(
            observation.catalog_handle().clone(),
            observation.target().clone(),
            Some(observation.object_id().clone()),
            novarocks_spi::connector::document_storage::ConnectorDocumentManagementOperation::Drop,
            Some(descriptor.management_dependencies(runtime)),
            crate::management::EffectScope::CATALOG_AND_OBJECT_DELETION,
        )
        .unwrap();
        let mut turn = entrance.acquire(request, || false).unwrap();
        turn.mark_dispatched(crate::management::EffectResponsibility::new(
            crate::management::EffectIdentity::from_bytes([8; 16]),
            ManagedMvTarget::from_observation(&observation).unwrap(),
            entrance.incarnation().clone(),
            crate::management::EffectScope::CATALOG_AND_OBJECT_DELETION,
            crate::management::ManagementTimestamp::from_unix_millis(1),
        ))
        .unwrap();
        drop(turn);
        assert!(
            entrance
                .install_fresh_drop_target(&observation, &descriptor, runtime)
                .is_err(),
            "fresh DROP must not erase Unknown"
        );
    }

    #[test]
    fn retired_drop_headers_are_exact_and_bounded() {
        use prost::Message;
        let (definition, interpretation, configuration, _) = fixture();
        let d = encode_definition(&definition).unwrap();
        let c = encode_configuration(&configuration).unwrap();
        let mut raw = super::super::generated::InterpretationDocument::decode(
            encode_interpretation(&interpretation).unwrap().as_bytes(),
        )
        .unwrap();
        raw.apply_key = Some(super::super::generated::ApplyKey {
            kind: Some(1),
            components: vec![],
        });
        assert!(
            decode_drop_document_bodies(
                d.as_bytes(),
                &raw.encode_to_vec(),
                None,
                c.as_bytes(),
                PersistenceDecodeBudget::default()
            )
            .unwrap()
            .2
        );
        let tiny_budget = PersistenceDecodeBudget {
            max_document_bytes: 1,
            ..PersistenceDecodeBudget::default()
        };
        assert!(
            decode_drop_document_bodies(
                d.as_bytes(),
                &raw.encode_to_vec(),
                None,
                c.as_bytes(),
                tiny_budget
            )
            .is_err()
        );
        raw.aggregates
            .push(super::super::generated::AggregateInterpretation::default());
        assert!(
            decode_drop_document_bodies(
                d.as_bytes(),
                &raw.encode_to_vec(),
                None,
                c.as_bytes(),
                PersistenceDecodeBudget::default()
            )
            .is_err(),
            "legacy tags cannot admit an unsupported aggregate L"
        );
        raw.aggregates.clear();
        let wrong_publication = PublicationDocument {
            publication_prepared_at_ms: 1,
            publication_id: PublicationIdentity::try_new(vec![1]).unwrap(),
            definition_revision: d.revision(),
            interpretation_revision: DocumentRevision::from_canonical_bytes(b"another-L"),
            inputs: definition
                .relation_occurrences
                .iter()
                .map(|o| PublicationInput {
                    relation_occurrence_id: o.occurrence_id,
                    object_id: o.object_id.clone(),
                    native_data_version: NativeDataVersion::try_new(vec![1]).unwrap(),
                })
                .collect(),
            output: PublicationOutput {
                object_id: interpretation.target.object_id.clone(),
                empty_result: false,
            },
            kind: PublicationKind::FullRefresh,
            statistics: PublicationStatistics::default(),
        };
        let p = encode_publication(&wrong_publication).unwrap();
        assert!(
            decode_drop_document_bodies(
                d.as_bytes(),
                &raw.encode_to_vec(),
                Some(p.as_bytes()),
                c.as_bytes(),
                PersistenceDecodeBudget::default()
            )
            .is_err()
        );
        raw.definition_revision = Some(vec![0; 32]);
        assert!(
            decode_drop_document_bodies(
                d.as_bytes(),
                &raw.encode_to_vec(),
                None,
                c.as_bytes(),
                PersistenceDecodeBudget::default()
            )
            .is_err()
        );
        raw.definition_revision = Some(d.revision().as_bytes().to_vec());
        raw.computation_identity = None;
        assert!(
            decode_drop_document_bodies(
                d.as_bytes(),
                &raw.encode_to_vec(),
                None,
                c.as_bytes(),
                PersistenceDecodeBudget::default()
            )
            .is_err()
        );
        let truncated = vec![0xff];
        assert!(
            decode_drop_document_bodies(
                d.as_bytes(),
                &truncated,
                None,
                c.as_bytes(),
                PersistenceDecodeBudget::default()
            )
            .is_err()
        );
    }
    #[test]
    fn known_retired_complex_drop_preserves_exact_p_links_and_rejects_unknown_or_stale_headers() {
        use prost::Message;
        let budget = PersistenceDecodeBudget::default();
        let mut d = super::super::generated::DefinitionDocument::decode(
            include_bytes!("codec/fixtures/legacy-v1/definition.pb").as_slice(),
        )
        .unwrap();
        let mut l = super::super::generated::InterpretationDocument::decode(
            include_bytes!("codec/fixtures/legacy-v1/interpretation.pb").as_slice(),
        )
        .unwrap();
        let mut p = decode_publication(
            include_bytes!("codec/fixtures/legacy-v1/publication.pb"),
            budget,
        )
        .unwrap();
        let c = include_bytes!("codec/fixtures/legacy-v1/configuration.pb");
        let output = d.outputs[0].output_id.clone();
        d.outputs[0].type_signature = Some("list".into());
        for binding in &mut l.outputs {
            if binding.output_id == output {
                binding.type_signature = Some("list".into());
            }
        }
        for binding in &mut l.target.as_mut().unwrap().fields {
            if binding.kind == Some(1) && binding.logical_id == output {
                binding.type_signature = Some("list".into());
            }
        }
        let mut semantics = d.clone();
        semantics.format_version = None;
        semantics.created_at_ms = None;
        semantics.computation_identity = None;
        let computation = super::super::identity::ComputationIdentity::from_canonical_bytes(
            &semantics.encode_to_vec(),
        );
        d.computation_identity = Some(computation.as_bytes().to_vec());
        let d_bytes = d.encode_to_vec();
        let d_revision = DocumentRevision::from_canonical_bytes(&d_bytes);
        l.definition_revision = Some(d_revision.as_bytes().to_vec());
        l.computation_identity = Some(computation.as_bytes().to_vec());
        let l_bytes = l.encode_to_vec();
        p.definition_revision = d_revision;
        p.interpretation_revision = DocumentRevision::from_canonical_bytes(&l_bytes);
        let p_bytes = encode_publication(&p).unwrap();
        assert!(matches!(
            decode_definition(&d_bytes, budget),
            Err(PersistenceCodecError::LogicalType(
                super::super::codec::TypeCodecError::RetiredComplex
            ))
        ));
        assert!(matches!(
            decode_interpretation(&l_bytes, budget),
            Err(PersistenceCodecError::LogicalType(
                super::super::codec::TypeCodecError::RetiredComplex
            ))
        ));
        let (drop, _, retired, object) =
            decode_drop_document_bodies(&d_bytes, &l_bytes, Some(p_bytes.as_bytes()), c, budget)
                .unwrap();
        assert!(retired);
        assert_eq!(drop.computation_identity, computation);
        assert_eq!(object, p.output.object_id);
        let mut unknown = d.clone();
        unknown.outputs[0].type_signature = Some("list<unknown>".into());
        assert!(
            decode_drop_document_bodies(
                &unknown.encode_to_vec(),
                &l_bytes,
                Some(p_bytes.as_bytes()),
                c,
                budget
            )
            .is_err()
        );
        let mut stale = l.clone();
        stale.definition_revision = Some([0; 32].to_vec());
        assert!(
            decode_drop_document_bodies(
                &d_bytes,
                &stale.encode_to_vec(),
                Some(p_bytes.as_bytes()),
                c,
                budget
            )
            .is_err()
        );
        let mut unknown_role = l;
        unknown_role.target.as_mut().unwrap().fields[0].kind = Some(999);
        assert!(
            decode_drop_document_bodies(&d_bytes, &unknown_role.encode_to_vec(), None, c, budget)
                .is_err()
        );
        assert!(
            decode_drop_document_bodies(
                &d_bytes,
                &l_bytes,
                Some(p_bytes.as_bytes()),
                c,
                PersistenceDecodeBudget {
                    max_items: 1,
                    ..budget
                }
            )
            .is_err()
        );
    }

    #[test]
    fn drop_complete_bound_schema_validates_semantics_without_equating_binding_bytes() {
        use prost::Message;
        let budget = PersistenceDecodeBudget::default();
        let d_bytes = include_bytes!("codec/fixtures/legacy-v1/definition.pb");
        let mut interpretation = decode_interpretation(
            include_bytes!("codec/fixtures/legacy-v1/interpretation.pb"),
            budget,
        )
        .unwrap();
        for field in &mut interpretation.target.fields {
            field.data_type = super::super::codec::MvLogicalType::from_schema_type(
                field.data_type.logical_type().clone(),
                Bytes::from_static(b"exact-physical-field-ids"),
            )
            .unwrap();
        }
        let l = encode_interpretation(&interpretation).unwrap();
        let mut p = super::super::generated::PublicationDocument::decode(
            include_bytes!("codec/fixtures/legacy-v1/publication.pb").as_slice(),
        )
        .unwrap();
        p.interpretation_revision = Some(l.revision().as_bytes().to_vec());
        let c = include_bytes!("codec/fixtures/legacy-v1/configuration.pb");
        let (_, _, retired, _) =
            decode_drop_document_bodies(d_bytes, l.as_bytes(), Some(&p.encode_to_vec()), c, budget)
                .unwrap();
        assert!(!retired);
    }

    #[tokio::test]
    async fn legacy_drop_reservation_keeps_queries_closed_and_rejects_stale_completion() {
        let (lease, request) = legacy_drop_documents();
        let (_, descriptor) =
            observe_current_drop_descriptor(&lease, request, PersistenceDecodeBudget::default())
                .unwrap();
        let repository = Arc::new(crate::test_repository::InMemoryMvRepository::default());
        let runtime = Arc::new(crate::process_runtime::ProcessRuntime::default());
        let service = crate::readiness::MvReadinessService::new(repository, runtime.clone());
        let source = descriptor.source_revision();
        let target = crate::product::MvTarget::from_parts(
            Some(source.target.instance_id.as_str()),
            &source.target.namespace,
            &source.target.table,
        );
        let crate::readiness::MvDropReadiness::ReadyToDrop(guard) =
            service.prepare_current_drop(&descriptor).await.unwrap()
        else {
            panic!("Current object remains retirable without Accelerator");
        };
        assert_eq!(
            guard.expected_target_object_id(),
            Some(&source.target_object_id)
        );
        assert!(!guard.has_projection());
        assert!(matches!(
            runtime.readiness(&target),
            crate::process_runtime::TargetReadiness::Unavailable(_)
        ));
        service
            .invalidate_current(target, "a newer exact observation superseded DROP".into())
            .await
            .unwrap();
        assert!(matches!(
            service
                .delete_after_provider_drop(uuid::Uuid::now_v7(), guard)
                .await
                .unwrap(),
            crate::readiness::MvProjectionInstallOutcome::Superseded
        ));
    }
}
