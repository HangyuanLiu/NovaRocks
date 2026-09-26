// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file under the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::document::{exhausted, invalid};
use super::observation::{
    ConnectorDocumentDiscoveryPage, ConnectorDocumentDiscoveryRequest,
    ConnectorDocumentLoadRequest, ConnectorDocumentManagementObservation,
    ConnectorDocumentObservationRequest, ConnectorDocumentStorageObservation,
    FrozenConnectorDocumentObservation, validate_observation_owner, validate_owner_key,
    validate_table,
};
use super::{
    ConnectorDocumentId, ConnectorDocumentSet, MAX_CONNECTOR_DOCUMENT_BYTES,
    MAX_CONNECTOR_DOCUMENT_SET_BYTES, MAX_CONNECTOR_DOCUMENTS,
};
use crate::connector::{
    CatalogHandle, ConnectorControlRuntimeId, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorMutationOperationId, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorTableObjectId, ConnectorWriteBaseVersion, LakePublicationId,
    ProviderBindingEpoch,
};

pub const CONNECTOR_DOCUMENT_STORAGE_CONTRACT_VERSION: u16 = 1;
pub const DEFAULT_CONNECTOR_DOCUMENT_LOAD_TOTAL_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_CONNECTOR_DOCUMENT_DECODE_WORKING_SET_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_BYTES: usize = 8 * 1024;
pub const DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_TOTAL_BYTES: usize = 32 * 1024;
pub const MAX_CONNECTOR_DOCUMENT_STRUCTURE_DEPTH: usize = 64;
const MAX_DOCUMENT_PROVIDER_TOKEN_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentStorageLimits {
    max_document_bytes: usize,
    max_load_total_bytes: usize,
    max_decode_working_set_bytes: usize,
    max_documents: usize,
    max_references: usize,
    max_structure_depth: usize,
}

impl ConnectorDocumentStorageLimits {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        max_document_bytes: usize,
        max_load_total_bytes: usize,
        max_decode_working_set_bytes: usize,
        max_documents: usize,
        max_references: usize,
        max_structure_depth: usize,
    ) -> Result<Self, ConnectorError> {
        if max_document_bytes == 0
            || max_document_bytes > MAX_CONNECTOR_DOCUMENT_BYTES
            || max_load_total_bytes < max_document_bytes
            || max_load_total_bytes > MAX_CONNECTOR_DOCUMENT_SET_BYTES
            || max_decode_working_set_bytes < max_load_total_bytes
            || max_decode_working_set_bytes > DEFAULT_CONNECTOR_DOCUMENT_DECODE_WORKING_SET_BYTES
            || max_documents == 0
            || max_documents > MAX_CONNECTOR_DOCUMENTS
            || max_references == 0
            || max_references > super::MAX_CONNECTOR_DOCUMENT_REFERENCES
            || max_structure_depth == 0
            || max_structure_depth > MAX_CONNECTOR_DOCUMENT_STRUCTURE_DEPTH
        {
            return Err(invalid(
                "document storage limits are inconsistent or exceed the hard bounds",
            ));
        }
        Ok(Self {
            max_document_bytes,
            max_load_total_bytes,
            max_decode_working_set_bytes,
            max_documents,
            max_references,
            max_structure_depth,
        })
    }

    pub const fn spec_default() -> Self {
        Self {
            max_document_bytes: MAX_CONNECTOR_DOCUMENT_BYTES,
            max_load_total_bytes: DEFAULT_CONNECTOR_DOCUMENT_LOAD_TOTAL_BYTES,
            max_decode_working_set_bytes: DEFAULT_CONNECTOR_DOCUMENT_DECODE_WORKING_SET_BYTES,
            max_documents: MAX_CONNECTOR_DOCUMENTS,
            max_references: super::MAX_CONNECTOR_DOCUMENT_REFERENCES,
            max_structure_depth: MAX_CONNECTOR_DOCUMENT_STRUCTURE_DEPTH,
        }
    }

    pub const fn max_document_bytes(self) -> usize {
        self.max_document_bytes
    }
    pub const fn max_load_total_bytes(self) -> usize {
        self.max_load_total_bytes
    }
    pub const fn max_decode_working_set_bytes(self) -> usize {
        self.max_decode_working_set_bytes
    }
    pub const fn max_documents(self) -> usize {
        self.max_documents
    }
    pub const fn max_references(self) -> usize {
        self.max_references
    }
    pub const fn max_structure_depth(self) -> usize {
        self.max_structure_depth
    }
}

impl Default for ConnectorDocumentStorageLimits {
    fn default() -> Self {
        Self::spec_default()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorDocumentManagementOperation {
    Create,
    SingleTargetUpdate,
    Publication,
    Drop,
}

#[derive(Clone)]
pub struct ConnectorDocumentManagementAdmissionRequest {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    operation_id: ConnectorMutationOperationId,
    target: ConnectorTableIdentity,
    expected_object_id: Option<ConnectorTableObjectId>,
    operation: ConnectorDocumentManagementOperation,
    context: ConnectorRequestContext,
}

impl ConnectorDocumentManagementAdmissionRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        catalog_handle: CatalogHandle,
        operation_id: ConnectorMutationOperationId,
        target: ConnectorTableIdentity,
        expected_object_id: Option<ConnectorTableObjectId>,
        operation: ConnectorDocumentManagementOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_table(&target)?;
        if target.instance_id != owner.instance_id
            || catalog_handle.catalog_name() != &owner.instance_id
            || (operation == ConnectorDocumentManagementOperation::Create)
                == expected_object_id.is_some()
        {
            return Err(invalid(
                "document admission has an inconsistent owner, target, or object precondition",
            ));
        }
        Ok(Self {
            owner,
            catalog_handle,
            operation_id,
            target,
            expected_object_id,
            operation,
            context,
        })
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        validate_table(&self.target)?;
        if self.target.instance_id != self.owner.instance_id
            || self.catalog_handle.catalog_name() != &self.owner.instance_id
            || (self.operation == ConnectorDocumentManagementOperation::Create)
                == self.expected_object_id.is_some()
        {
            return Err(invalid(
                "document admission has an inconsistent owner, target, or object precondition",
            ));
        }
        Ok(())
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub const fn expected_object_id(&self) -> Option<&ConnectorTableObjectId> {
        self.expected_object_id.as_ref()
    }
    pub const fn operation(&self) -> ConnectorDocumentManagementOperation {
        self.operation
    }
    pub const fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDocumentManagementAdmission {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    operation_id: ConnectorMutationOperationId,
    target: ConnectorTableIdentity,
    expected_object_id: Option<ConnectorTableObjectId>,
    operation: ConnectorDocumentManagementOperation,
    provider_token: Bytes,
    digest: [u8; 32],
}

impl ConnectorDocumentManagementAdmission {
    fn try_new(
        request: &ConnectorDocumentManagementAdmissionRequest,
        provider_token: Bytes,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_provider_token(&provider_token)?;
        let digest = admission_digest(request, &provider_token);
        Ok(Self {
            owner: request.owner.clone(),
            catalog_handle: request.catalog_handle.clone(),
            operation_id: request.operation_id,
            target: request.target.clone(),
            expected_object_id: request.expected_object_id.clone(),
            operation: request.operation,
            provider_token,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.digest
            != admission_digest_fields(
                &self.owner,
                &self.catalog_handle,
                self.operation_id,
                &self.target,
                self.expected_object_id.as_ref(),
                self.operation,
                &self.provider_token,
            )
        {
            return Err(invalid("document admission digest is invalid"));
        }
        Ok(())
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub const fn expected_object_id(&self) -> Option<&ConnectorTableObjectId> {
        self.expected_object_id.as_ref()
    }
    pub const fn operation(&self) -> ConnectorDocumentManagementOperation {
        self.operation
    }
    pub const fn provider_token(&self) -> &Bytes {
        &self.provider_token
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl std::fmt::Debug for ConnectorDocumentManagementAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorDocumentManagementAdmission")
            .field("owner", &self.owner)
            .field("catalog_handle", &self.catalog_handle)
            .field("operation_id", &self.operation_id)
            .field("target", &self.target)
            .field("expected_object_id", &self.expected_object_id)
            .field("operation", &self.operation)
            .field("provider_token_bytes", &self.provider_token.len())
            .field("digest", &self.digest)
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorPrepareDocumentsRequest {
    admission: ConnectorDocumentManagementAdmission,
    documents: ConnectorDocumentSet,
    context: ConnectorRequestContext,
}

impl ConnectorPrepareDocumentsRequest {
    pub fn try_new(
        admission: ConnectorDocumentManagementAdmission,
        documents: ConnectorDocumentSet,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        admission.validate()?;
        Ok(Self {
            admission,
            documents,
            context,
        })
    }

    pub const fn admission(&self) -> &ConnectorDocumentManagementAdmission {
        &self.admission
    }
    pub const fn documents(&self) -> &ConnectorDocumentSet {
        &self.documents
    }
    pub const fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorPreparedDocumentSet {
    admission: ConnectorDocumentManagementAdmission,
    documents: Vec<ConnectorPreparedDocument>,
    provider_token: Bytes,
    digest: [u8; 32],
}

impl ConnectorPreparedDocumentSet {
    fn try_new(
        request: &ConnectorPrepareDocumentsRequest,
        provider_token: Bytes,
    ) -> Result<Self, ConnectorError> {
        request.admission().validate()?;
        validate_provider_token(&provider_token)?;
        let documents = request
            .documents()
            .documents()
            .iter()
            .map(|document| ConnectorPreparedDocument {
                id: document.id().clone(),
                attachment: document.attachment().clone(),
            })
            .collect::<Vec<_>>();
        let digest = prepared_digest(request.admission().digest(), &documents, &provider_token);
        Ok(Self {
            admission: request.admission().clone(),
            documents,
            provider_token,
            digest,
        })
    }

    pub fn validate_for(
        &self,
        admission: &ConnectorDocumentManagementAdmission,
    ) -> Result<(), ConnectorError> {
        admission.validate()?;
        let expected = prepared_digest(
            self.admission.digest(),
            &self.documents,
            &self.provider_token,
        );
        if &self.admission != admission || self.digest != expected {
            return Err(invalid(
                "prepared document set does not match its exact admitted operation",
            ));
        }
        Ok(())
    }

    pub const fn admission(&self) -> &ConnectorDocumentManagementAdmission {
        &self.admission
    }
    pub fn documents(&self) -> &[ConnectorPreparedDocument] {
        &self.documents
    }
    pub const fn provider_token(&self) -> &Bytes {
        &self.provider_token
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorPreparedDocument {
    id: ConnectorDocumentId,
    attachment: super::ConnectorDocumentAttachment,
}

impl ConnectorPreparedDocument {
    pub const fn id(&self) -> &ConnectorDocumentId {
        &self.id
    }

    pub const fn attachment(&self) -> &super::ConnectorDocumentAttachment {
        &self.attachment
    }
}

impl std::fmt::Debug for ConnectorPreparedDocumentSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorPreparedDocumentSet")
            .field("admission", &self.admission)
            .field("documents", &self.documents)
            .field("provider_token_bytes", &self.provider_token.len())
            .field("digest", &self.digest)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDocumentCreatePublicationIntent {
    prepared_target: crate::connector::ConnectorPreparedCreateDocumentTarget,
    prepared_documents: ConnectorPreparedDocumentSet,
    marker: super::ConnectorManagedObjectMarker,
}

impl std::fmt::Debug for ConnectorDocumentCreatePublicationIntent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorDocumentCreatePublicationIntent")
            .field("prepared_target", &self.prepared_target)
            .field("prepared_documents", &self.prepared_documents)
            .field("marker", &self.marker)
            .finish()
    }
}

impl ConnectorDocumentCreatePublicationIntent {
    pub fn try_new(
        handle: &crate::connector::ConnectorStagedTableHandle,
        prepared_documents: ConnectorPreparedDocumentSet,
        marker: super::ConnectorManagedObjectMarker,
    ) -> Result<Self, ConnectorError> {
        let prepared_target = handle.document_target().ok_or_else(|| {
            invalid("ordinary staged target cannot publish application documents")
        })?;
        let admission = prepared_documents.admission();
        prepared_documents.validate_for(admission)?;
        if admission.operation() != ConnectorDocumentManagementOperation::Create
            || admission.owner() != prepared_target.owner()
            || admission.catalog_handle() != prepared_target.catalog_handle()
            || admission.operation_id() != prepared_target.operation_id()
            || admission.target() != prepared_target.target()
            || admission.expected_object_id().is_some()
            || prepared_documents.documents().iter().any(|document| {
                !matches!(
                    document.attachment(),
                    super::ConnectorDocumentAttachment::TableMetadata
                )
            })
        {
            return Err(invalid("create publication received non-create documents"));
        }
        Ok(Self {
            prepared_target: prepared_target.clone(),
            prepared_documents,
            marker,
        })
    }

    pub const fn prepared_target(
        &self,
    ) -> &crate::connector::ConnectorPreparedCreateDocumentTarget {
        &self.prepared_target
    }
    pub const fn prepared_documents(&self) -> &ConnectorPreparedDocumentSet {
        &self.prepared_documents
    }

    pub const fn marker(&self) -> &super::ConnectorManagedObjectMarker {
        &self.marker
    }

    pub(crate) fn validate_for_handle(
        &self,
        handle: &crate::connector::ConnectorStagedTableHandle,
    ) -> Result<(), ConnectorError> {
        if handle.document_target() != Some(&self.prepared_target) {
            return Err(invalid(
                "document create publication does not match its staged target",
            ));
        }
        self.prepared_documents
            .validate_for(self.prepared_documents.admission())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentUpdateIntent {
    prepared_documents: ConnectorPreparedDocumentSet,
    observation: ConnectorDocumentManagementObservation,
    marker_change: ConnectorManagedObjectMarkerChange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorManagedObjectMarkerChange {
    Preserve,
    Replace {
        expected: super::ConnectorManagedObjectMarker,
        replacement: super::ConnectorManagedObjectMarker,
    },
}

impl ConnectorDocumentUpdateIntent {
    pub fn try_new(
        prepared_documents: ConnectorPreparedDocumentSet,
        observation: ConnectorDocumentManagementObservation,
        marker_change: ConnectorManagedObjectMarkerChange,
    ) -> Result<Self, ConnectorError> {
        let admission = prepared_documents.admission();
        prepared_documents.validate_for(admission)?;
        observation.validate_sealed()?;
        if admission.operation() != ConnectorDocumentManagementOperation::SingleTargetUpdate
            || admission.owner() != observation.owner()
            || admission.catalog_handle() != observation.catalog_handle()
            || admission.expected_object_id() != Some(observation.object_id())
            || admission.target() != observation.target()
        {
            return Err(invalid(
                "document update does not match its exact frozen observation",
            ));
        }
        if let ConnectorManagedObjectMarkerChange::Replace {
            expected,
            replacement,
        } = &marker_change
            && (expected != observation.marker() || expected == replacement)
        {
            return Err(invalid(
                "managed object marker replacement does not match the current marker",
            ));
        }
        if prepared_documents.documents().iter().any(|document| {
            !matches!(
                document.attachment(),
                super::ConnectorDocumentAttachment::TableMetadata
            )
        }) {
            return Err(invalid(
                "document update may only replace table-metadata documents",
            ));
        }
        Ok(Self {
            prepared_documents,
            observation,
            marker_change,
        })
    }

    pub const fn prepared_documents(&self) -> &ConnectorPreparedDocumentSet {
        &self.prepared_documents
    }
    pub const fn observation(&self) -> &ConnectorDocumentManagementObservation {
        &self.observation
    }

    pub const fn marker_change(&self) -> &ConnectorManagedObjectMarkerChange {
        &self.marker_change
    }

    pub(crate) fn validate_for(
        &self,
        owner: &ConnectorProviderBindingKey,
        operation_id: ConnectorMutationOperationId,
    ) -> Result<(), ConnectorError> {
        let admission = self.prepared_documents.admission();
        self.prepared_documents.validate_for(admission)?;
        if admission.owner() != owner || admission.operation_id() != operation_id {
            return Err(invalid(
                "document update does not match its catalog mutation authority",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentPublicationDeclaration {
    publication_id: LakePublicationId,
    admission: ConnectorDocumentManagementAdmission,
    target_object_id: ConnectorTableObjectId,
    expected_base: ConnectorWriteBaseVersion,
    technique: crate::connector::ConnectorManagedPublicationTechnique,
    empty_input: crate::connector::ConnectorManagedPublicationEmptyInputDisposition,
    partition_spec_replacement: Option<crate::connector::ConnectorManagedPartitionSpecReplacement>,
    expected_committed_partitioning: Option<crate::connector::ConnectorCommittedPartitioning>,
}

impl ConnectorDocumentPublicationDeclaration {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        publication_id: LakePublicationId,
        admission: ConnectorDocumentManagementAdmission,
        target_object_id: ConnectorTableObjectId,
        expected_base: ConnectorWriteBaseVersion,
        technique: crate::connector::ConnectorManagedPublicationTechnique,
        empty_input: crate::connector::ConnectorManagedPublicationEmptyInputDisposition,
        partition_spec_replacement: Option<
            crate::connector::ConnectorManagedPartitionSpecReplacement,
        >,
        expected_committed_partitioning: Option<crate::connector::ConnectorCommittedPartitioning>,
    ) -> Result<Self, ConnectorError> {
        admission.validate()?;
        expected_base.validate()?;
        if admission.operation() != ConnectorDocumentManagementOperation::Publication
            || admission.operation_id().to_bytes() != publication_id.to_bytes()
            || admission.expected_object_id() != Some(&target_object_id)
        {
            return Err(invalid(
                "document publication declaration does not match its admitted operation",
            ));
        }
        if partition_spec_replacement.is_some() != expected_committed_partitioning.is_some() {
            return Err(invalid(
                "document publication partition replacement is incomplete",
            ));
        }
        if let Some(partitioning) = &expected_committed_partitioning {
            partitioning.validate()?;
        }
        Ok(Self {
            publication_id,
            admission,
            target_object_id,
            expected_base,
            technique,
            empty_input,
            partition_spec_replacement,
            expected_committed_partitioning,
        })
    }

    pub const fn publication_id(&self) -> LakePublicationId {
        self.publication_id
    }
    pub const fn admission(&self) -> &ConnectorDocumentManagementAdmission {
        &self.admission
    }
    pub const fn target_object_id(&self) -> &ConnectorTableObjectId {
        &self.target_object_id
    }
    pub const fn expected_base(&self) -> &ConnectorWriteBaseVersion {
        &self.expected_base
    }
    pub const fn technique(&self) -> crate::connector::ConnectorManagedPublicationTechnique {
        self.technique
    }
    pub const fn empty_input(
        &self,
    ) -> crate::connector::ConnectorManagedPublicationEmptyInputDisposition {
        self.empty_input
    }
    pub const fn partition_spec_replacement(
        &self,
    ) -> Option<&crate::connector::ConnectorManagedPartitionSpecReplacement> {
        self.partition_spec_replacement.as_ref()
    }
    pub const fn expected_committed_partitioning(
        &self,
    ) -> Option<&crate::connector::ConnectorCommittedPartitioning> {
        self.expected_committed_partitioning.as_ref()
    }

    pub(crate) fn validate_for_write(
        &self,
        base: Option<&ConnectorWriteBaseVersion>,
    ) -> Result<(), ConnectorError> {
        if base != Some(&self.expected_base) {
            return Err(invalid(
                "document publication declaration does not match its exact write base",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentPublicationIntent {
    publication_id: LakePublicationId,
    prepared_documents: ConnectorPreparedDocumentSet,
}

impl ConnectorDocumentPublicationIntent {
    pub fn try_new(
        declaration: &ConnectorDocumentPublicationDeclaration,
        prepared_documents: ConnectorPreparedDocumentSet,
    ) -> Result<Self, ConnectorError> {
        let admission = prepared_documents.admission();
        prepared_documents.validate_for(admission)?;
        if admission != declaration.admission()
            || !prepared_documents.documents().iter().any(|document| {
                matches!(
                    document.attachment(),
                    super::ConnectorDocumentAttachment::CommitOutput
                )
            })
        {
            return Err(invalid(
                "document publication payload does not match its declaration or commit output",
            ));
        }
        Ok(Self {
            publication_id: declaration.publication_id(),
            prepared_documents,
        })
    }

    pub const fn publication_id(&self) -> LakePublicationId {
        self.publication_id
    }

    pub const fn prepared_documents(&self) -> &ConnectorPreparedDocumentSet {
        &self.prepared_documents
    }

    pub fn validate_for(
        &self,
        declaration: &ConnectorDocumentPublicationDeclaration,
    ) -> Result<(), ConnectorError> {
        if self.publication_id != declaration.publication_id()
            || self.prepared_documents.admission() != declaration.admission()
        {
            return Err(invalid(
                "document publication payload does not match its begin declaration",
            ));
        }
        self.prepared_documents
            .validate_for(declaration.admission())
    }
}

/// Provider-owned preflight and immutable-carrier preparation only. Final
/// catalog effects remain on staged-create, catalog-mutation, and write-session.
pub trait ConnectorDocumentStorageManagement: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;

    fn admit_management(
        &self,
        request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<Bytes, ConnectorError>;

    fn prepare_documents(
        &self,
        request: ConnectorPrepareDocumentsRequest,
    ) -> Result<Bytes, ConnectorError>;
}

/// Independently optional FE document facets for one exact generation.
#[derive(Clone)]
pub struct ConnectorDocumentStorageBinding {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    observation: Option<Arc<dyn ConnectorDocumentStorageObservation>>,
    management: Option<Arc<dyn ConnectorDocumentStorageManagement>>,
}

impl ConnectorDocumentStorageBinding {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        observation: Option<Arc<dyn ConnectorDocumentStorageObservation>>,
        management: Option<Arc<dyn ConnectorDocumentStorageManagement>>,
    ) -> Result<Self, ConnectorError> {
        if observation.is_none() && management.is_none() {
            return Err(invalid(
                "document storage binding must contain at least one real capability",
            ));
        }
        if let Some(capability) = &observation {
            validate_observation_owner(&descriptor, incarnation, capability.as_ref())?;
        }
        if let Some(capability) = &management
            && (capability.descriptor() != &descriptor || capability.incarnation() != incarnation)
        {
            return Err(invalid(
                "document management capability does not match its control generation",
            ));
        }
        Ok(Self {
            descriptor,
            incarnation,
            observation,
            management,
        })
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    pub(crate) fn observation(&self) -> Option<Arc<dyn ConnectorDocumentStorageObservation>> {
        self.observation.as_ref().map(Arc::clone)
    }
    pub(crate) fn management(&self) -> Option<Arc<dyn ConnectorDocumentStorageManagement>> {
        self.management.as_ref().map(Arc::clone)
    }

    pub(crate) const fn supports_management(&self) -> bool {
        self.management.is_some()
    }

    pub(crate) const fn supports_observation(&self) -> bool {
        self.observation.is_some()
    }
}

#[derive(Clone)]
pub struct ConnectorDocumentStorageLease {
    control_runtime_id: ConnectorControlRuntimeId,
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    binding: ConnectorDocumentStorageBinding,
    _release: Arc<DocumentStorageLeaseRelease>,
}

struct DocumentStorageLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorDocumentStorageLease {
    pub(crate) fn new(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        incarnation: ProviderBindingEpoch,
        catalog_handle: CatalogHandle,
        binding: ConnectorDocumentStorageBinding,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if binding.descriptor() != &descriptor
            || binding.incarnation() != incarnation
            || catalog_handle.catalog_name() != &descriptor.instance_id
        {
            return Err(invalid(
                "document storage binding does not match its retained control generation",
            ));
        }
        Ok(Self {
            control_runtime_id,
            owner: ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            catalog_handle,
            binding,
            _release: Arc::new(DocumentStorageLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }
    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    fn observation(&self) -> Result<Arc<dyn ConnectorDocumentStorageObservation>, ConnectorError> {
        self.binding.observation().ok_or_else(|| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "connector control generation has no document observation capability",
            )
        })
    }

    pub fn observe_documents(
        &self,
        request: ConnectorDocumentObservationRequest,
    ) -> Result<FrozenConnectorDocumentObservation, ConnectorError> {
        validate_owner_key(&self.owner, request.owner())?;
        validate_catalog_handle(&self.catalog_handle, request.catalog_handle())?;
        let retained_request = request.clone();
        let mut observation = self.observation()?.observe_documents(request)?;
        observation.validate_for(&retained_request)?;
        retained_request.reserve_observation(observation.documents())?;
        observation.seal_for(&retained_request);
        Ok(observation)
    }

    pub fn load_document(
        &self,
        request: ConnectorDocumentLoadRequest,
    ) -> Result<super::ConnectorDocument, ConnectorError> {
        validate_owner_key(&self.owner, request.owner())?;
        validate_catalog_handle(&self.catalog_handle, request.catalog_handle())?;
        request.reserve_load()?;
        let expected_document = request.document().clone();
        let document = self.observation()?.load_document(request)?;
        if document.id() != expected_document.id()
            || document.format() != expected_document.format()
            || document.references() != expected_document.references()
            || document.content().len() != expected_document.encoded_len()
            || super::ConnectorDocumentRevision::for_content(document.content())
                != expected_document.id().revision()
            || !resolved_attachment_matches(expected_document.attachment(), document.attachment())
        {
            return Err(invalid(
                "document load returned content that does not match its exact stored envelope",
            ));
        }
        Ok(document)
    }

    pub fn observe_current_management(
        &self,
        request: ConnectorDocumentObservationRequest,
    ) -> Result<ConnectorDocumentManagementObservation, ConnectorError> {
        validate_owner_key(&self.owner, request.owner())?;
        validate_catalog_handle(&self.catalog_handle, request.catalog_handle())?;
        let retained_request = request.clone();
        let mut observation = self.observation()?.observe_current_management(request)?;
        observation.validate_for(&retained_request)?;
        retained_request.reserve_observation(observation.documents())?;
        observation.seal_for(&retained_request);
        Ok(observation)
    }

    pub fn discover_documents(
        &self,
        request: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        validate_owner_key(&self.owner, request.owner())?;
        validate_catalog_handle(&self.catalog_handle, request.catalog_handle())?;
        let retained_request = request.clone();
        let mut page = self.observation()?.discover_documents(request)?;
        page.validate_for(&retained_request)?;
        retained_request.reserve_page(page.items().len())?;
        page.seal_for(&retained_request);
        Ok(page)
    }

    fn management(&self) -> Result<Arc<dyn ConnectorDocumentStorageManagement>, ConnectorError> {
        self.binding.management().ok_or_else(|| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "connector control generation has no document management capability",
            )
        })
    }

    pub fn admit_management(
        &self,
        request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<ConnectorDocumentManagementAdmission, ConnectorError> {
        request.validate()?;
        validate_owner_key(&self.owner, request.owner())?;
        validate_catalog_handle(&self.catalog_handle, request.catalog_handle())?;
        let retained_request = request.clone();
        let provider_token = self.management()?.admit_management(request)?;
        ConnectorDocumentManagementAdmission::try_new(&retained_request, provider_token)
    }

    pub fn prepare_documents(
        &self,
        request: ConnectorPrepareDocumentsRequest,
    ) -> Result<ConnectorPreparedDocumentSet, ConnectorError> {
        validate_owner_key(&self.owner, request.admission().owner())?;
        validate_catalog_handle(&self.catalog_handle, request.admission().catalog_handle())?;
        request.admission().validate()?;
        let retained_request = request.clone();
        let provider_token = self.management()?.prepare_documents(request)?;
        ConnectorPreparedDocumentSet::try_new(&retained_request, provider_token)
    }
}

impl Drop for DocumentStorageLeaseRelease {
    fn drop(&mut self) {
        if let Some(release) = self
            .release
            .lock()
            .expect("document storage lease release lock")
            .take()
        {
            release();
        }
    }
}

fn validate_provider_token(token: &Bytes) -> Result<(), ConnectorError> {
    if token.is_empty() || token.len() > MAX_DOCUMENT_PROVIDER_TOKEN_BYTES {
        return Err(exhausted(
            "document provider token is empty or exceeds its byte limit",
        ));
    }
    Ok(())
}

fn admission_digest(
    request: &ConnectorDocumentManagementAdmissionRequest,
    token: &Bytes,
) -> [u8; 32] {
    admission_digest_fields(
        &request.owner,
        &request.catalog_handle,
        request.operation_id,
        &request.target,
        request.expected_object_id.as_ref(),
        request.operation,
        token,
    )
}

fn admission_digest_fields(
    owner: &ConnectorProviderBindingKey,
    catalog_handle: &CatalogHandle,
    operation_id: ConnectorMutationOperationId,
    target: &ConnectorTableIdentity,
    expected_object_id: Option<&ConnectorTableObjectId>,
    operation: ConnectorDocumentManagementOperation,
    token: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-document-admission.v1\0");
    digest_bytes(&mut hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
    hasher.update(catalog_handle.version().as_bytes());
    hasher.update(operation_id.to_bytes());
    digest_bytes(&mut hasher, target.namespace.as_bytes());
    digest_bytes(&mut hasher, target.table.as_bytes());
    hasher.update([operation as u8]);
    match expected_object_id {
        Some(object_id) => {
            hasher.update([1]);
            digest_bytes(&mut hasher, object_id.as_bytes());
        }
        None => hasher.update([0]),
    }
    digest_bytes(&mut hasher, token);
    hasher.finalize().into()
}

fn prepared_digest(
    admission_digest: [u8; 32],
    documents: &[ConnectorPreparedDocument],
    token: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-prepared-documents.v1\0");
    hasher.update(admission_digest);
    for document in documents {
        digest_bytes(&mut hasher, document.id().owner().as_str().as_bytes());
        digest_bytes(&mut hasher, document.id().name().as_str().as_bytes());
        hasher.update(document.id().revision().to_bytes());
        match document.attachment() {
            super::ConnectorDocumentAttachment::TableMetadata => hasher.update([0]),
            super::ConnectorDocumentAttachment::ExactOutput(version) => {
                hasher.update([1]);
                hasher.update(version.digest());
            }
            super::ConnectorDocumentAttachment::CommitOutput => hasher.update([2]),
        }
    }
    digest_bytes(&mut hasher, token);
    hasher.finalize().into()
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn validate_catalog_handle(
    expected: &CatalogHandle,
    actual: &CatalogHandle,
) -> Result<(), ConnectorError> {
    if expected != actual {
        return Err(invalid(
            "document storage request does not match the retained catalog handle",
        ));
    }
    Ok(())
}

fn resolved_attachment_matches(
    stored: &super::ConnectorStoredDocumentAttachment,
    loaded: &super::ConnectorDocumentAttachment,
) -> bool {
    match (stored, loaded) {
        (
            super::ConnectorStoredDocumentAttachment::TableMetadata,
            super::ConnectorDocumentAttachment::TableMetadata,
        ) => true,
        (
            super::ConnectorStoredDocumentAttachment::ExactOutput(expected),
            super::ConnectorDocumentAttachment::ExactOutput(actual),
        ) => expected == actual,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::connector::{
        CatalogVersion, ConnectorCommittedVersion, ConnectorDeferredDocumentHandle,
        ConnectorDocumentAttachment, ConnectorDocumentCarrier,
        ConnectorDocumentDiscoveryCompleteness, ConnectorDocumentDiscoveryIncompleteReason,
        ConnectorDocumentDiscoveryItem, ConnectorDocumentFormat, ConnectorDocumentName,
        ConnectorDocumentOwner, ConnectorDocumentRevision, ConnectorDocumentStorageBudget,
        ConnectorErrorKind, ConnectorInstanceId, ConnectorManagedObjectMarker, ConnectorProviderId,
        ConnectorStoredDocument, ConnectorStoredDocumentAttachment,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            crate::connector::ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    fn descriptor(instance: &str) -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse(instance).unwrap(),
        }
    }

    fn catalog_handle(instance: &str) -> CatalogHandle {
        catalog_handle_with_version(instance, 7)
    }

    fn catalog_handle_with_version(instance: &str, version: u8) -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse(instance).unwrap(),
            CatalogVersion::from_bytes([version; 32]),
        )
    }

    fn table(instance: &str, namespace: &str) -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse(instance).unwrap(),
            namespace: Arc::from(namespace),
            table: Arc::from("table"),
        }
    }

    fn object_id() -> ConnectorTableObjectId {
        ConnectorTableObjectId::try_new(Bytes::from_static(b"table-object")).unwrap()
    }

    fn metadata_version() -> ConnectorCommittedVersion {
        ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata-version"), Some(7))
            .unwrap()
    }

    fn document_set() -> ConnectorDocumentSet {
        ConnectorDocumentSet::try_new(vec![
            crate::connector::ConnectorDocument::try_new(
                ConnectorDocumentOwner::parse("novarocks.mv").unwrap(),
                ConnectorDocumentName::parse("definition").unwrap(),
                ConnectorDocumentFormat::try_new("novarocks.mv", "definition", 1).unwrap(),
                Bytes::from_static(b"opaque"),
                vec![],
                ConnectorDocumentAttachment::TableMetadata,
            )
            .unwrap(),
        ])
        .unwrap()
    }

    struct Management {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        calls: Arc<AtomicUsize>,
    }

    impl ConnectorDocumentStorageManagement for Management {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn admit_management(
            &self,
            _request: ConnectorDocumentManagementAdmissionRequest,
        ) -> Result<Bytes, ConnectorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::from_static(b"admitted"))
        }

        fn prepare_documents(
            &self,
            _request: ConnectorPrepareDocumentsRequest,
        ) -> Result<Bytes, ConnectorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::from_static(b"prepared"))
        }
    }

    struct Observation {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
    }

    struct EchoObservation {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        observe_calls: Arc<AtomicUsize>,
        load_calls: Arc<AtomicUsize>,
    }

    impl ConnectorDocumentStorageObservation for EchoObservation {
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
            self.observe_calls.fetch_add(1, Ordering::SeqCst);
            let content = Bytes::from_static(b"data");
            let format = ConnectorDocumentFormat::try_new("novarocks.mv", "definition", 1)?;
            let owner = ConnectorDocumentOwner::parse("novarocks.mv")?;
            let documents = ["definition", "interpretation"]
                .into_iter()
                .map(|name| {
                    ConnectorStoredDocument::try_new(
                        ConnectorDocumentId::new(
                            owner.clone(),
                            ConnectorDocumentName::parse(name)?,
                            ConnectorDocumentRevision::for_content(&content),
                        ),
                        format.clone(),
                        content.len(),
                        Vec::new(),
                        ConnectorStoredDocumentAttachment::TableMetadata,
                        ConnectorDocumentCarrier::DeferredContent(
                            ConnectorDeferredDocumentHandle::try_new(Bytes::copy_from_slice(
                                name.as_bytes(),
                            ))?,
                        ),
                    )
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?;
            FrozenConnectorDocumentObservation::try_new(&request, metadata_version(), documents)
        }

        fn load_document(
            &self,
            request: ConnectorDocumentLoadRequest,
        ) -> Result<crate::connector::ConnectorDocument, ConnectorError> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            let document = request.document();
            crate::connector::ConnectorDocument::try_new(
                document.id().owner().clone(),
                document.id().name().clone(),
                document.format().clone(),
                Bytes::from_static(b"data"),
                document.references().to_vec(),
                ConnectorDocumentAttachment::TableMetadata,
            )
        }

        fn observe_current_management(
            &self,
            request: ConnectorDocumentObservationRequest,
        ) -> Result<ConnectorDocumentManagementObservation, ConnectorError> {
            ConnectorDocumentManagementObservation::try_new(
                &request,
                metadata_version(),
                ConnectorManagedObjectMarker::try_new("mv", "owner", "incarnation")?,
                Vec::new(),
            )
        }

        fn discover_documents(
            &self,
            request: ConnectorDocumentDiscoveryRequest,
        ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
            ConnectorDocumentDiscoveryPage::try_new(
                vec![ConnectorDocumentDiscoveryItem::try_new(
                    table(self.descriptor.instance_id.as_str(), "escaped"),
                    object_id(),
                    metadata_version(),
                    ConnectorManagedObjectMarker::try_new("mv", "owner", "incarnation")?,
                )?],
                None,
                ConnectorDocumentDiscoveryCompleteness::Complete,
                &request,
            )
        }
    }

    impl ConnectorDocumentStorageObservation for Observation {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn observe_documents(
            &self,
            _request: ConnectorDocumentObservationRequest,
        ) -> Result<FrozenConnectorDocumentObservation, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "not used by management tests",
            ))
        }

        fn load_document(
            &self,
            _request: ConnectorDocumentLoadRequest,
        ) -> Result<crate::connector::ConnectorDocument, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "not used by management tests",
            ))
        }

        fn observe_current_management(
            &self,
            _request: ConnectorDocumentObservationRequest,
        ) -> Result<ConnectorDocumentManagementObservation, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "not used by management tests",
            ))
        }

        fn discover_documents(
            &self,
            _request: ConnectorDocumentDiscoveryRequest,
        ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "not used by management tests",
            ))
        }
    }

    fn lease(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        management: Option<Arc<dyn ConnectorDocumentStorageManagement>>,
    ) -> ConnectorDocumentStorageLease {
        let binding = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            incarnation,
            Some(Arc::new(Observation {
                descriptor: descriptor.clone(),
                incarnation,
            })),
            management,
        )
        .unwrap();
        let catalog_handle = catalog_handle(descriptor.instance_id.as_str());
        ConnectorDocumentStorageLease::new(
            descriptor,
            ConnectorControlRuntimeId::new(),
            incarnation,
            catalog_handle,
            binding,
            || {},
        )
        .unwrap()
    }

    #[test]
    fn binding_rejects_management_from_another_generation() {
        let descriptor = descriptor("catalog");
        let expected_incarnation = ProviderBindingEpoch::from_bytes([1; 16]);
        let capability = Arc::new(Management {
            descriptor: descriptor.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([2; 16]),
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let error = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            expected_incarnation,
            Some(Arc::new(Observation {
                descriptor,
                incarnation: expected_incarnation,
            })),
            Some(capability),
        )
        .err()
        .expect("generation mismatch");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn binding_allows_management_without_observation_and_observation_stays_unsupported() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([2; 16]);
        let calls = Arc::new(AtomicUsize::new(0));
        let capability = Arc::new(Management {
            descriptor: descriptor.clone(),
            incarnation,
            calls: Arc::clone(&calls),
        });
        let binding = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            incarnation,
            None,
            Some(capability),
        )
        .unwrap();
        let lease = ConnectorDocumentStorageLease::new(
            descriptor.clone(),
            ConnectorControlRuntimeId::new(),
            incarnation,
            catalog_handle("catalog"),
            binding,
            || {},
        )
        .unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let observation = ConnectorDocumentObservationRequest::try_new(
            owner.clone(),
            catalog_handle("catalog"),
            table("catalog", "ns"),
            object_id(),
            ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
            context(),
        )
        .unwrap();
        assert_eq!(
            lease.observe_documents(observation).unwrap_err().kind(),
            ConnectorErrorKind::Unsupported
        );
        let admission = ConnectorDocumentManagementAdmissionRequest::try_new(
            owner,
            catalog_handle("catalog"),
            ConnectorMutationOperationId::new(),
            table("catalog", "ns"),
            None,
            ConnectorDocumentManagementOperation::Create,
            context(),
        )
        .unwrap();
        lease.admit_management(admission).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn wrong_owner_is_rejected_before_management_dispatch() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([3; 16]);
        let calls = Arc::new(AtomicUsize::new(0));
        let capability = Arc::new(Management {
            descriptor: descriptor.clone(),
            incarnation,
            calls: Arc::clone(&calls),
        });
        let lease = lease(descriptor.clone(), incarnation, Some(capability));
        let request = ConnectorDocumentManagementAdmissionRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation: ProviderBindingEpoch::from_bytes([4; 16]),
            },
            catalog_handle("catalog"),
            ConnectorMutationOperationId::new(),
            ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse("catalog").unwrap(),
                namespace: Arc::from("ns"),
                table: Arc::from("table"),
            },
            None,
            ConnectorDocumentManagementOperation::Create,
            context(),
        )
        .unwrap();
        let error = lease.admit_management(request).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepared_documents_are_bound_to_the_exact_admission() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([5; 16]);
        let calls = Arc::new(AtomicUsize::new(0));
        let capability = Arc::new(Management {
            descriptor: descriptor.clone(),
            incarnation,
            calls,
        });
        let lease = lease(descriptor.clone(), incarnation, Some(capability));
        let request = ConnectorDocumentManagementAdmissionRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            catalog_handle("catalog"),
            ConnectorMutationOperationId::new(),
            ConnectorTableIdentity {
                instance_id: descriptor.instance_id,
                namespace: Arc::from("ns"),
                table: Arc::from("table"),
            },
            None,
            ConnectorDocumentManagementOperation::Create,
            context(),
        )
        .unwrap();
        let admission = lease.admit_management(request).unwrap();
        let prepared = lease
            .prepare_documents(
                ConnectorPrepareDocumentsRequest::try_new(
                    admission.clone(),
                    document_set(),
                    context(),
                )
                .unwrap(),
            )
            .unwrap();
        prepared.validate_for(&admission).unwrap();
        let other = ConnectorDocumentManagementAdmission {
            operation_id: ConnectorMutationOperationId::new(),
            ..admission
        };
        assert_eq!(
            prepared.validate_for(&other).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn catalog_generation_mismatch_is_rejected_before_management_dispatch() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([6; 16]);
        let calls = Arc::new(AtomicUsize::new(0));
        let capability = Arc::new(Management {
            descriptor: descriptor.clone(),
            incarnation,
            calls: Arc::clone(&calls),
        });
        let lease = lease(descriptor.clone(), incarnation, Some(capability));
        let request = ConnectorDocumentManagementAdmissionRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            catalog_handle_with_version("catalog", 8),
            ConnectorMutationOperationId::new(),
            table(descriptor.instance_id.as_str(), "ns"),
            None,
            ConnectorDocumentManagementOperation::Create,
            context(),
        )
        .unwrap();
        let error = lease.admit_management(request).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn update_rejects_observation_from_another_catalog_version() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([11; 16]);
        let make_lease = |catalog_version: u8| {
            let management = Arc::new(Management {
                descriptor: descriptor.clone(),
                incarnation,
                calls: Arc::new(AtomicUsize::new(0)),
            });
            let binding = ConnectorDocumentStorageBinding::try_new(
                descriptor.clone(),
                incarnation,
                Some(Arc::new(EchoObservation {
                    descriptor: descriptor.clone(),
                    incarnation,
                    observe_calls: Arc::new(AtomicUsize::new(0)),
                    load_calls: Arc::new(AtomicUsize::new(0)),
                })),
                Some(management),
            )
            .unwrap();
            ConnectorDocumentStorageLease::new(
                descriptor.clone(),
                ConnectorControlRuntimeId::new(),
                incarnation,
                catalog_handle_with_version("catalog", catalog_version),
                binding,
                || {},
            )
            .unwrap()
        };
        let target = table("catalog", "ns");
        let object_id = object_id();
        let old_lease = make_lease(7);
        let old_observation = old_lease
            .observe_current_management(
                ConnectorDocumentObservationRequest::try_new(
                    ConnectorProviderBindingKey {
                        instance_id: descriptor.instance_id.clone(),
                        incarnation,
                    },
                    catalog_handle_with_version("catalog", 7),
                    target.clone(),
                    object_id.clone(),
                    ConnectorDocumentStorageBudget::new(
                        ConnectorDocumentStorageLimits::spec_default(),
                    ),
                    context(),
                )
                .unwrap(),
            )
            .unwrap();
        let new_lease = make_lease(8);
        let admission = new_lease
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    ConnectorProviderBindingKey {
                        instance_id: descriptor.instance_id,
                        incarnation,
                    },
                    catalog_handle_with_version("catalog", 8),
                    ConnectorMutationOperationId::new(),
                    target,
                    Some(object_id),
                    ConnectorDocumentManagementOperation::SingleTargetUpdate,
                    context(),
                )
                .unwrap(),
            )
            .unwrap();
        let prepared = new_lease
            .prepare_documents(
                ConnectorPrepareDocumentsRequest::try_new(admission, document_set(), context())
                    .unwrap(),
            )
            .unwrap();
        let error = ConnectorDocumentUpdateIntent::try_new(
            prepared,
            old_observation,
            ConnectorManagedObjectMarkerChange::Preserve,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn deferred_load_budget_is_cumulative_and_pre_dispatch() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([7; 16]);
        let load_calls = Arc::new(AtomicUsize::new(0));
        let binding = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            incarnation,
            Some(Arc::new(EchoObservation {
                descriptor: descriptor.clone(),
                incarnation,
                observe_calls: Arc::new(AtomicUsize::new(0)),
                load_calls: Arc::clone(&load_calls),
            })),
            None,
        )
        .unwrap();
        let lease = ConnectorDocumentStorageLease::new(
            descriptor.clone(),
            ConnectorControlRuntimeId::new(),
            incarnation,
            catalog_handle("catalog"),
            binding,
            || {},
        )
        .unwrap();
        let limits = ConnectorDocumentStorageLimits::try_new(4, 7, 7, 2, 1, 1).unwrap();
        let request = ConnectorDocumentObservationRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            catalog_handle("catalog"),
            table("catalog", "ns"),
            object_id(),
            ConnectorDocumentStorageBudget::new(limits),
            context(),
        )
        .unwrap();
        let observation = lease.observe_documents(request.clone()).unwrap();
        let first = request
            .try_load_request(observation.documents()[0].clone(), context())
            .unwrap();
        let second = request
            .try_load_request(observation.documents()[1].clone(), context())
            .unwrap();
        lease.load_document(first).unwrap();
        let error = lease.load_document(second).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(load_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn discovery_reply_cannot_escape_the_requested_namespace() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([8; 16]);
        let binding = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            incarnation,
            Some(Arc::new(EchoObservation {
                descriptor: descriptor.clone(),
                incarnation,
                observe_calls: Arc::new(AtomicUsize::new(0)),
                load_calls: Arc::new(AtomicUsize::new(0)),
            })),
            None,
        )
        .unwrap();
        let lease = ConnectorDocumentStorageLease::new(
            descriptor.clone(),
            ConnectorControlRuntimeId::new(),
            incarnation,
            catalog_handle("catalog"),
            binding,
            || {},
        )
        .unwrap();
        let request = ConnectorDocumentDiscoveryRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            catalog_handle("catalog"),
            Some(Arc::from("requested")),
            1,
            ConnectorDocumentStorageBudget::new(
                ConnectorDocumentStorageLimits::try_new(4, 7, 7, 1, 1, 1).unwrap(),
            ),
            context(),
        )
        .unwrap();
        let error = lease.discover_documents(request).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn available_content_is_rejected_before_it_exceeds_its_budget() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([9; 16]);
        let request = ConnectorDocumentObservationRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            catalog_handle("catalog"),
            table("catalog", "ns"),
            object_id(),
            ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
            context(),
        )
        .unwrap();
        let content = Bytes::from(vec![
            7;
            DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_BYTES + 1
        ]);
        let document = ConnectorStoredDocument::try_new(
            ConnectorDocumentId::new(
                ConnectorDocumentOwner::parse("novarocks.mv").unwrap(),
                ConnectorDocumentName::parse("definition").unwrap(),
                ConnectorDocumentRevision::for_content(&content),
            ),
            ConnectorDocumentFormat::try_new("novarocks.mv", "definition", 1).unwrap(),
            content.len(),
            Vec::new(),
            ConnectorStoredDocumentAttachment::TableMetadata,
            ConnectorDocumentCarrier::AvailableContent(content),
        )
        .unwrap();
        let error = request.reserve_observation(&[document]).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn observation_budget_is_shared_across_candidate_requests() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([10; 16]);
        let observe_calls = Arc::new(AtomicUsize::new(0));
        let binding = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            incarnation,
            Some(Arc::new(EchoObservation {
                descriptor: descriptor.clone(),
                incarnation,
                observe_calls: Arc::clone(&observe_calls),
                load_calls: Arc::new(AtomicUsize::new(0)),
            })),
            None,
        )
        .unwrap();
        let lease = ConnectorDocumentStorageLease::new(
            descriptor.clone(),
            ConnectorControlRuntimeId::new(),
            incarnation,
            catalog_handle("catalog"),
            binding,
            || {},
        )
        .unwrap();
        let budget = ConnectorDocumentStorageBudget::new(
            ConnectorDocumentStorageLimits::try_new(4, 8, 8, 3, 1, 1).unwrap(),
        );
        let request = |table_name: &str| {
            ConnectorDocumentObservationRequest::try_new(
                ConnectorProviderBindingKey {
                    instance_id: descriptor.instance_id.clone(),
                    incarnation,
                },
                catalog_handle("catalog"),
                ConnectorTableIdentity {
                    instance_id: descriptor.instance_id.clone(),
                    namespace: Arc::from("ns"),
                    table: Arc::from(table_name),
                },
                object_id(),
                budget.clone(),
                context(),
            )
            .unwrap()
        };
        lease.observe_documents(request("first")).unwrap();
        let error = lease.observe_documents(request("second")).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(observe_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn discovery_continuation_is_derived_from_a_sealed_page_and_reduced_budget() {
        let descriptor = descriptor("catalog");
        let incarnation = ProviderBindingEpoch::from_bytes([11; 16]);
        let budget = ConnectorDocumentStorageBudget::new(
            ConnectorDocumentStorageLimits::try_new(4, 8, 8, 3, 3, 1).unwrap(),
        );
        let request = ConnectorDocumentDiscoveryRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            catalog_handle("catalog"),
            Some(Arc::from("ns")),
            2,
            budget,
            context(),
        )
        .unwrap();
        let mut page = ConnectorDocumentDiscoveryPage::try_new(
            vec![
                ConnectorDocumentDiscoveryItem::try_new(
                    table("catalog", "ns"),
                    object_id(),
                    metadata_version(),
                    ConnectorManagedObjectMarker::try_new("mv", "owner", "incarnation").unwrap(),
                )
                .unwrap(),
            ],
            Some(Bytes::from_static(b"next")),
            ConnectorDocumentDiscoveryCompleteness::Incomplete(
                ConnectorDocumentDiscoveryIncompleteReason::PageBoundary,
            ),
            &request,
        )
        .unwrap();

        let error = match page.try_next_request(&request, context()) {
            Ok(_) => panic!("unsealed page must not produce a continuation request"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        request.reserve_page(page.items().len()).unwrap();
        page.seal_for(&request);
        let next = page.try_next_request(&request, context()).unwrap().unwrap();
        assert_eq!(next.cursor().map(Bytes::as_ref), Some(b"next".as_slice()));
        assert_eq!(next.remaining_items(), 2);
        assert_eq!(next.page_size(), 2);
        next.reserve_page(2).unwrap();
        assert_eq!(
            next.reserve_page(1).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn limits_only_allow_callers_to_tighten_hard_bounds() {
        assert_eq!(
            ConnectorDocumentStorageLimits::try_new(
                MAX_CONNECTOR_DOCUMENT_BYTES + 1,
                MAX_CONNECTOR_DOCUMENT_SET_BYTES,
                DEFAULT_CONNECTOR_DOCUMENT_DECODE_WORKING_SET_BYTES,
                MAX_CONNECTOR_DOCUMENTS,
                super::super::MAX_CONNECTOR_DOCUMENT_REFERENCES,
                MAX_CONNECTOR_DOCUMENT_STRUCTURE_DEPTH,
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let id = ConnectorDocumentId::new(
            ConnectorDocumentOwner::parse("owner").unwrap(),
            ConnectorDocumentName::parse("name").unwrap(),
            ConnectorDocumentRevision::for_content(b"value"),
        );
        assert_eq!(id.owner().as_str(), "owner");
    }
}
