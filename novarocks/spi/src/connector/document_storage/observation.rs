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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::document::{exhausted, invalid, validate_token};
use super::{
    ConnectorDocument, ConnectorDocumentFormat, ConnectorDocumentId, ConnectorDocumentReference,
    ConnectorDocumentRevision, ConnectorDocumentStorageLimits, MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
    MAX_CONNECTOR_DOCUMENT_REFERENCES, MAX_CONNECTOR_DOCUMENTS,
};
use crate::connector::{
    CatalogHandle, ConnectorCommittedVersion, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableObjectId, ProviderBindingEpoch,
};

pub const MAX_CONNECTOR_DEFERRED_DOCUMENT_HANDLE_BYTES: usize = 16 * 1024;
pub const MAX_CONNECTOR_DOCUMENT_DISCOVERY_PAGE_SIZE: usize = 128;

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDeferredDocumentHandle(Bytes);

impl ConnectorDeferredDocumentHandle {
    pub fn try_new(value: Bytes) -> Result<Self, ConnectorError> {
        if value.is_empty() || value.len() > MAX_CONNECTOR_DEFERRED_DOCUMENT_HANDLE_BYTES {
            return Err(invalid(
                "deferred document handle must be non-empty and within its opaque byte limit",
            ));
        }
        Ok(Self(value))
    }

    pub const fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl std::fmt::Debug for ConnectorDeferredDocumentHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorDeferredDocumentHandle")
            .field("bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum ConnectorDocumentCarrier {
    AvailableContent(Bytes),
    DeferredContent(ConnectorDeferredDocumentHandle),
}

impl std::fmt::Debug for ConnectorDocumentCarrier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AvailableContent(content) => formatter
                .debug_struct("AvailableContent")
                .field("bytes", &content.len())
                .finish(),
            Self::DeferredContent(handle) => formatter
                .debug_tuple("DeferredContent")
                .field(handle)
                .finish(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStoredDocumentAttachment {
    TableMetadata,
    ExactOutput(ConnectorCommittedVersion),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStoredDocument {
    id: ConnectorDocumentId,
    format: ConnectorDocumentFormat,
    encoded_len: usize,
    references: Vec<ConnectorDocumentReference>,
    attachment: ConnectorStoredDocumentAttachment,
    carrier: ConnectorDocumentCarrier,
    observation_witness: Option<ConnectorDocumentObservationWitness>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ConnectorDocumentObservationWitness {
    request_digest: [u8; 32],
    metadata_version: ConnectorCommittedVersion,
}

impl ConnectorStoredDocument {
    pub fn try_new(
        id: ConnectorDocumentId,
        format: ConnectorDocumentFormat,
        encoded_len: usize,
        references: Vec<ConnectorDocumentReference>,
        attachment: ConnectorStoredDocumentAttachment,
        carrier: ConnectorDocumentCarrier,
    ) -> Result<Self, ConnectorError> {
        if encoded_len == 0 || encoded_len > super::MAX_CONNECTOR_DOCUMENT_BYTES {
            return Err(exhausted(
                "stored document length is outside the document budget",
            ));
        }
        if references.len() > MAX_CONNECTOR_DOCUMENT_REFERENCES {
            return Err(exhausted(
                "stored document references exceed the item limit",
            ));
        }
        if let ConnectorStoredDocumentAttachment::ExactOutput(version) = &attachment {
            version.validate()?;
        }
        let mut seen = HashSet::with_capacity(references.len());
        for reference in &references {
            if reference.target() == &id
                || !seen.insert((reference.relationship(), reference.target()))
            {
                return Err(invalid(
                    "stored document references are duplicate or self-referential",
                ));
            }
        }
        if let ConnectorDocumentCarrier::AvailableContent(content) = &carrier
            && (content.len() != encoded_len
                || ConnectorDocumentRevision::for_content(content) != id.revision())
        {
            return Err(crate::connector::ConnectorError::new(
                crate::connector::ConnectorErrorKind::CorruptData,
                "available document content does not match its declared length and revision",
            ));
        }
        Ok(Self {
            id,
            format,
            encoded_len,
            references,
            attachment,
            carrier,
            observation_witness: None,
        })
    }

    pub const fn id(&self) -> &ConnectorDocumentId {
        &self.id
    }
    pub const fn format(&self) -> &ConnectorDocumentFormat {
        &self.format
    }
    pub const fn encoded_len(&self) -> usize {
        self.encoded_len
    }
    pub fn references(&self) -> &[ConnectorDocumentReference] {
        &self.references
    }
    pub const fn attachment(&self) -> &ConnectorStoredDocumentAttachment {
        &self.attachment
    }
    pub const fn carrier(&self) -> &ConnectorDocumentCarrier {
        &self.carrier
    }

    fn seal_observation(
        &mut self,
        request_digest: [u8; 32],
        metadata_version: &ConnectorCommittedVersion,
    ) {
        self.observation_witness = Some(ConnectorDocumentObservationWitness {
            request_digest,
            metadata_version: metadata_version.clone(),
        });
    }

    fn belongs_to_observation(&self, request_digest: [u8; 32]) -> bool {
        self.observation_witness
            .as_ref()
            .is_some_and(|witness| witness.request_digest == request_digest)
    }

    fn observed_metadata_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.observation_witness
            .as_ref()
            .map(|witness| &witness.metadata_version)
    }
}

#[derive(Clone)]
pub struct ConnectorDocumentObservationRequest {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    target: ConnectorTableIdentity,
    expected_object_id: ConnectorTableObjectId,
    budget: ConnectorDocumentStorageBudget,
    context: ConnectorRequestContext,
}

impl ConnectorDocumentObservationRequest {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        catalog_handle: CatalogHandle,
        target: ConnectorTableIdentity,
        expected_object_id: ConnectorTableObjectId,
        budget: ConnectorDocumentStorageBudget,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_table(&target)?;
        if owner.instance_id != target.instance_id
            || catalog_handle.catalog_name() != &owner.instance_id
        {
            return Err(invalid(
                "document observation target does not match its provider generation",
            ));
        }
        Ok(Self {
            owner,
            catalog_handle,
            target,
            expected_object_id,
            budget,
            context,
        })
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub const fn expected_object_id(&self) -> &ConnectorTableObjectId {
        &self.expected_object_id
    }
    pub const fn limits(&self) -> ConnectorDocumentStorageLimits {
        self.budget.limits()
    }
    pub const fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub(crate) fn request_digest(&self) -> [u8; 32] {
        observation_request_digest(self)
    }

    pub fn try_load_request(
        &self,
        document: ConnectorStoredDocument,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorDocumentLoadRequest, ConnectorError> {
        ConnectorDocumentLoadRequest::try_new(self, document, context)
    }

    pub(crate) fn reserve_observation(
        &self,
        documents: &[ConnectorStoredDocument],
    ) -> Result<(), ConnectorError> {
        self.budget.reserve_observation(documents)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenConnectorDocumentObservation {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    target: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    metadata_version: ConnectorCommittedVersion,
    documents: Vec<ConnectorStoredDocument>,
    request_digest: [u8; 32],
    lease_witness: Option<[u8; 32]>,
}

impl FrozenConnectorDocumentObservation {
    pub fn try_new(
        request: &ConnectorDocumentObservationRequest,
        metadata_version: ConnectorCommittedVersion,
        documents: Vec<ConnectorStoredDocument>,
    ) -> Result<Self, ConnectorError> {
        validate_table(request.target())?;
        metadata_version.validate()?;
        if documents.len() > MAX_CONNECTOR_DOCUMENTS {
            return Err(exhausted(
                "frozen document observation exceeds the document limit",
            ));
        }
        let mut ids = HashSet::with_capacity(documents.len());
        if documents.iter().any(|document| !ids.insert(document.id())) {
            return Err(invalid(
                "frozen document observation contains duplicate identities",
            ));
        }
        Ok(Self {
            owner: request.owner().clone(),
            catalog_handle: request.catalog_handle().clone(),
            target: request.target().clone(),
            object_id: request.expected_object_id().clone(),
            metadata_version,
            documents,
            request_digest: request.request_digest(),
            lease_witness: None,
        })
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }
    pub const fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }
    pub fn documents(&self) -> &[ConnectorStoredDocument] {
        &self.documents
    }

    pub(crate) fn validate_for(
        &self,
        request: &ConnectorDocumentObservationRequest,
    ) -> Result<(), ConnectorError> {
        if self.request_digest != request.request_digest()
            || self.owner() != request.owner()
            || self.catalog_handle() != request.catalog_handle()
            || self.target() != request.target()
            || self.object_id() != request.expected_object_id()
        {
            return Err(invalid(
                "document observation reply does not match the original request",
            ));
        }
        Ok(())
    }

    pub(crate) fn seal_for(&mut self, request: &ConnectorDocumentObservationRequest) {
        self.lease_witness = Some(request.request_digest());
        for document in &mut self.documents {
            document.seal_observation(request.request_digest(), &self.metadata_version);
        }
    }

    pub fn validate_sealed(&self) -> Result<(), ConnectorError> {
        if self.lease_witness != Some(self.request_digest) {
            return Err(invalid(
                "document observation was not validated by its exact storage lease",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConnectorDocumentLoadRequest {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    target: ConnectorTableIdentity,
    expected_object_id: ConnectorTableObjectId,
    metadata_version: ConnectorCommittedVersion,
    document: ConnectorStoredDocument,
    budget: ConnectorDocumentStorageBudget,
    context: ConnectorRequestContext,
}

impl ConnectorDocumentLoadRequest {
    fn try_new(
        observation: &ConnectorDocumentObservationRequest,
        document: ConnectorStoredDocument,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        let metadata_version = document
            .observed_metadata_version()
            .cloned()
            .ok_or_else(|| {
                invalid(
                    "document load requires an envelope returned by the exact observation request",
                )
            })?;
        metadata_version.validate()?;
        if document.encoded_len() > observation.limits().max_document_bytes() {
            return Err(exhausted(
                "document load exceeds the per-document byte budget",
            ));
        }
        if !document.belongs_to_observation(observation.request_digest()) {
            return Err(invalid(
                "document load requires an envelope returned by the exact observation request",
            ));
        }
        if !matches!(
            document.carrier(),
            ConnectorDocumentCarrier::DeferredContent(_)
        ) {
            return Err(invalid("only deferred document content may be loaded"));
        }
        Ok(Self {
            owner: observation.owner.clone(),
            catalog_handle: observation.catalog_handle.clone(),
            target: observation.target.clone(),
            expected_object_id: observation.expected_object_id.clone(),
            metadata_version,
            document,
            budget: observation.budget.clone(),
            context,
        })
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub const fn expected_object_id(&self) -> &ConnectorTableObjectId {
        &self.expected_object_id
    }
    pub const fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }
    pub const fn document(&self) -> &ConnectorStoredDocument {
        &self.document
    }
    pub const fn limits(&self) -> ConnectorDocumentStorageLimits {
        self.budget.limits()
    }
    pub const fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub(crate) fn reserve_load(&self) -> Result<(), ConnectorError> {
        self.budget.reserve_load(self.document.encoded_len())
    }
}

#[derive(Clone)]
pub struct ConnectorDocumentStorageBudget {
    limits: ConnectorDocumentStorageLimits,
    state: Arc<Mutex<ConnectorDocumentStorageBudgetState>>,
}

#[derive(Debug, Default)]
struct ConnectorDocumentStorageBudgetState {
    loaded_bytes: usize,
    available_bytes: usize,
    consumed_items: usize,
    consumed_references: usize,
}

impl ConnectorDocumentStorageBudget {
    pub fn new(limits: ConnectorDocumentStorageLimits) -> Self {
        Self {
            limits,
            state: Arc::new(Mutex::new(ConnectorDocumentStorageBudgetState::default())),
        }
    }

    pub const fn limits(&self) -> ConnectorDocumentStorageLimits {
        self.limits
    }

    fn reserve_observation(
        &self,
        documents: &[ConnectorStoredDocument],
    ) -> Result<(), ConnectorError> {
        let document_count = documents.len();
        let reference_count = documents
            .iter()
            .try_fold(0usize, |count, document| {
                count.checked_add(document.references().len())
            })
            .ok_or_else(|| exhausted("document observation reference accounting overflowed"))?;
        let available_bytes = documents
            .iter()
            .filter_map(|document| match document.carrier() {
                ConnectorDocumentCarrier::AvailableContent(content) => Some(content.len()),
                ConnectorDocumentCarrier::DeferredContent(_) => None,
            })
            .try_fold(0usize, |count, bytes| count.checked_add(bytes))
            .ok_or_else(|| exhausted("document observation byte accounting overflowed"))?;
        if documents.iter().any(|document| {
            document.encoded_len() > self.limits.max_document_bytes()
                || matches!(
                    document.carrier(),
                    ConnectorDocumentCarrier::AvailableContent(content)
                        if content.len()
                            > super::DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_BYTES
                )
        }) {
            return Err(exhausted(
                "document envelope exceeds the caller's per-document byte budget",
            ));
        }
        let mut state = self.state.lock().expect("document load budget lock");
        let next_documents = state
            .consumed_items
            .checked_add(document_count)
            .ok_or_else(|| exhausted("document observation item accounting overflowed"))?;
        let next_references = state
            .consumed_references
            .checked_add(reference_count)
            .ok_or_else(|| exhausted("document observation reference accounting overflowed"))?;
        let next_available_bytes = state
            .available_bytes
            .checked_add(available_bytes)
            .ok_or_else(|| exhausted("available document byte accounting overflowed"))?;
        if next_documents > self.limits.max_documents()
            || next_references > self.limits.max_references()
            || next_available_bytes
                > super::DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_TOTAL_BYTES
        {
            return Err(exhausted(
                "document observation exceeds its cumulative item, reference, or byte budget",
            ));
        }
        state.consumed_items = next_documents;
        state.consumed_references = next_references;
        state.available_bytes = next_available_bytes;
        Ok(())
    }

    fn reserve_load(&self, bytes: usize) -> Result<(), ConnectorError> {
        let mut state = self.state.lock().expect("document load budget lock");
        let next = state
            .loaded_bytes
            .checked_add(bytes)
            .ok_or_else(|| exhausted("document load byte accounting overflowed"))?;
        if bytes > self.limits.max_document_bytes() || next > self.limits.max_load_total_bytes() {
            return Err(exhausted(
                "document load exceeds its cumulative byte budget",
            ));
        }
        state.loaded_bytes = next;
        Ok(())
    }

    fn remaining_items(&self) -> Result<usize, ConnectorError> {
        let state = self.state.lock().expect("document storage budget lock");
        self.limits
            .max_documents()
            .checked_sub(state.consumed_items)
            .ok_or_else(|| exhausted("document storage item budget underflowed"))
    }

    fn reserve_discovery(&self, items: usize) -> Result<(), ConnectorError> {
        let mut state = self.state.lock().expect("document storage budget lock");
        let next = state
            .consumed_items
            .checked_add(items)
            .ok_or_else(|| exhausted("document discovery item accounting overflowed"))?;
        if next > self.limits.max_documents() {
            return Err(exhausted(
                "document discovery exceeds its cumulative item budget",
            ));
        }
        state.consumed_items = next;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorManagedObjectMarker {
    kind: Arc<str>,
    owner: Arc<str>,
    incarnation: Arc<str>,
}

impl ConnectorManagedObjectMarker {
    pub fn try_new(
        kind: impl AsRef<str>,
        owner: impl AsRef<str>,
        incarnation: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        validate_token(
            kind.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "managed object kind",
        )?;
        validate_token(
            owner.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "managed object owner",
        )?;
        validate_token(
            incarnation.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "managed object incarnation",
        )?;
        Ok(Self {
            kind: Arc::from(kind.as_ref()),
            owner: Arc::from(owner.as_ref()),
            incarnation: Arc::from(incarnation.as_ref()),
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn owner(&self) -> &str {
        &self.owner
    }
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentManagementObservation {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    target: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    metadata_version: ConnectorCommittedVersion,
    marker: ConnectorManagedObjectMarker,
    documents: Vec<ConnectorStoredDocument>,
    request_digest: [u8; 32],
    lease_witness: Option<[u8; 32]>,
}

impl ConnectorDocumentManagementObservation {
    pub fn try_new(
        request: &ConnectorDocumentObservationRequest,
        metadata_version: ConnectorCommittedVersion,
        marker: ConnectorManagedObjectMarker,
        documents: Vec<ConnectorStoredDocument>,
    ) -> Result<Self, ConnectorError> {
        let frozen =
            FrozenConnectorDocumentObservation::try_new(request, metadata_version, documents)?;
        Ok(Self {
            owner: frozen.owner,
            catalog_handle: frozen.catalog_handle,
            target: frozen.target,
            object_id: frozen.object_id,
            metadata_version: frozen.metadata_version,
            marker,
            documents: frozen.documents,
            request_digest: frozen.request_digest,
            lease_witness: None,
        })
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }
    pub const fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }
    pub const fn marker(&self) -> &ConnectorManagedObjectMarker {
        &self.marker
    }
    pub fn documents(&self) -> &[ConnectorStoredDocument] {
        &self.documents
    }

    pub(crate) fn validate_for(
        &self,
        request: &ConnectorDocumentObservationRequest,
    ) -> Result<(), ConnectorError> {
        if self.request_digest != request.request_digest()
            || self.owner() != request.owner()
            || self.catalog_handle() != request.catalog_handle()
            || self.target() != request.target()
            || self.object_id() != request.expected_object_id()
        {
            return Err(invalid(
                "management observation reply does not match the original request",
            ));
        }
        Ok(())
    }

    pub(crate) fn seal_for(&mut self, request: &ConnectorDocumentObservationRequest) {
        self.lease_witness = Some(request.request_digest());
        for document in &mut self.documents {
            document.seal_observation(request.request_digest(), &self.metadata_version);
        }
    }

    pub(crate) fn validate_sealed(&self) -> Result<(), ConnectorError> {
        if self.lease_witness != Some(self.request_digest) {
            return Err(invalid(
                "management observation was not validated by its exact storage lease",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConnectorDocumentDiscoveryRequest {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    namespace: Option<Arc<str>>,
    cursor: Option<Bytes>,
    page_size: usize,
    remaining_items: usize,
    budget: ConnectorDocumentStorageBudget,
    context: ConnectorRequestContext,
}

impl ConnectorDocumentDiscoveryRequest {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        catalog_handle: CatalogHandle,
        namespace: Option<Arc<str>>,
        page_size: usize,
        budget: ConnectorDocumentStorageBudget,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        let max_items = budget.remaining_items()?;
        if max_items == 0 {
            return Err(exhausted(
                "document discovery has no remaining operation item budget",
            ));
        }
        if catalog_handle.catalog_name() != &owner.instance_id
            || namespace.as_ref().is_some_and(|value| value.is_empty())
            || page_size == 0
            || page_size > MAX_CONNECTOR_DOCUMENT_DISCOVERY_PAGE_SIZE
        {
            return Err(invalid(
                "document discovery scope, cursor, or item budget is invalid",
            ));
        }
        Ok(Self {
            owner,
            catalog_handle,
            namespace,
            cursor: None,
            page_size: page_size.min(max_items),
            remaining_items: max_items,
            budget,
            context,
        })
    }

    fn next_from(
        previous: &Self,
        cursor: Bytes,
        remaining_items: usize,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        if cursor.is_empty()
            || cursor.len() > MAX_CONNECTOR_DEFERRED_DOCUMENT_HANDLE_BYTES
            || remaining_items == 0
            || remaining_items >= previous.remaining_items
        {
            return Err(invalid(
                "document discovery continuation does not reduce the original budget",
            ));
        }
        Ok(Self {
            owner: previous.owner.clone(),
            catalog_handle: previous.catalog_handle.clone(),
            namespace: previous.namespace.clone(),
            cursor: Some(cursor),
            page_size: previous.page_size.min(remaining_items),
            remaining_items,
            budget: previous.budget.clone(),
            context,
        })
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
    pub const fn namespace(&self) -> Option<&Arc<str>> {
        self.namespace.as_ref()
    }
    pub const fn cursor(&self) -> Option<&Bytes> {
        self.cursor.as_ref()
    }
    pub const fn page_size(&self) -> usize {
        self.page_size
    }
    pub const fn remaining_items(&self) -> usize {
        self.remaining_items
    }
    pub const fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub(crate) fn request_digest(&self) -> [u8; 32] {
        discovery_request_digest(self)
    }

    pub(crate) fn reserve_page(&self, items: usize) -> Result<(), ConnectorError> {
        self.budget.reserve_discovery(items)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentDiscoveryItem {
    target: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    metadata_version: ConnectorCommittedVersion,
    marker: ConnectorManagedObjectMarker,
}

impl ConnectorDocumentDiscoveryItem {
    pub fn try_new(
        target: ConnectorTableIdentity,
        object_id: ConnectorTableObjectId,
        metadata_version: ConnectorCommittedVersion,
        marker: ConnectorManagedObjectMarker,
    ) -> Result<Self, ConnectorError> {
        validate_table(&target)?;
        metadata_version.validate()?;
        Ok(Self {
            target,
            object_id,
            metadata_version,
            marker,
        })
    }

    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }

    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }

    pub const fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }

    pub const fn marker(&self) -> &ConnectorManagedObjectMarker {
        &self.marker
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDocumentDiscoveryIncompleteReason {
    PageBoundary,
    ItemBudget,
    ProviderLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDocumentDiscoveryCompleteness {
    Complete,
    Incomplete(ConnectorDocumentDiscoveryIncompleteReason),
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDocumentDiscoveryPage {
    items: Vec<ConnectorDocumentDiscoveryItem>,
    next_cursor: Option<Bytes>,
    completeness: ConnectorDocumentDiscoveryCompleteness,
    request_digest: [u8; 32],
    lease_witness: Option<[u8; 32]>,
}

impl std::fmt::Debug for ConnectorDocumentDiscoveryPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorDocumentDiscoveryPage")
            .field("items", &self.items)
            .field(
                "next_cursor_bytes",
                &self.next_cursor.as_ref().map(Bytes::len),
            )
            .field("completeness", &self.completeness)
            .finish()
    }
}

impl ConnectorDocumentDiscoveryPage {
    pub fn try_new(
        items: Vec<ConnectorDocumentDiscoveryItem>,
        next_cursor: Option<Bytes>,
        completeness: ConnectorDocumentDiscoveryCompleteness,
        request: &ConnectorDocumentDiscoveryRequest,
    ) -> Result<Self, ConnectorError> {
        if items.len() > request.page_size() || items.len() > request.remaining_items() {
            return Err(exhausted(
                "document discovery page exceeds the caller budget",
            ));
        }
        if next_cursor.as_ref().is_some_and(|value| {
            value.is_empty() || value.len() > MAX_CONNECTOR_DEFERRED_DOCUMENT_HANDLE_BYTES
        }) {
            return Err(invalid("document discovery continuation cursor is invalid"));
        }
        if next_cursor.is_some() && next_cursor.as_ref() == request.cursor() {
            return Err(invalid(
                "document discovery continuation cursor made no progress",
            ));
        }
        if matches!(
            completeness,
            ConnectorDocumentDiscoveryCompleteness::Complete
        ) != next_cursor.is_none()
        {
            return Err(invalid(
                "document discovery completeness and continuation disagree",
            ));
        }
        if items.is_empty()
            && matches!(
                completeness,
                ConnectorDocumentDiscoveryCompleteness::Incomplete(_)
            )
        {
            return Err(invalid(
                "incomplete document discovery page must make item progress",
            ));
        }
        let mut identities = HashSet::with_capacity(items.len());
        if items
            .iter()
            .any(|item| !identities.insert((item.target(), item.object_id())))
        {
            return Err(invalid(
                "document discovery page contains duplicate object identities",
            ));
        }
        Ok(Self {
            items,
            next_cursor,
            completeness,
            request_digest: request.request_digest(),
            lease_witness: None,
        })
    }

    pub(crate) fn validate_for(
        &self,
        request: &ConnectorDocumentDiscoveryRequest,
    ) -> Result<(), ConnectorError> {
        if self.request_digest != request.request_digest()
            || self.items.len() > request.page_size()
            || self.items.len() > request.remaining_items()
            || matches!(
                self.completeness,
                ConnectorDocumentDiscoveryCompleteness::Complete
            ) != self.next_cursor.is_none()
        {
            return Err(invalid(
                "document discovery reply does not match the original request budget",
            ));
        }
        for item in &self.items {
            if item.target().instance_id != request.owner().instance_id
                || request
                    .namespace()
                    .is_some_and(|namespace| item.target().namespace.as_ref() != namespace.as_ref())
            {
                return Err(invalid(
                    "document discovery reply escaped the original catalog or namespace scope",
                ));
            }
            validate_table(item.target())?;
            item.metadata_version().validate()?;
        }
        Ok(())
    }

    pub(crate) fn seal_for(&mut self, request: &ConnectorDocumentDiscoveryRequest) {
        self.lease_witness = Some(request.request_digest());
    }

    pub fn try_next_request(
        &self,
        previous: &ConnectorDocumentDiscoveryRequest,
        context: ConnectorRequestContext,
    ) -> Result<Option<ConnectorDocumentDiscoveryRequest>, ConnectorError> {
        if self.lease_witness != Some(previous.request_digest()) {
            return Err(invalid(
                "document discovery page was not validated by its exact storage lease",
            ));
        }
        if matches!(
            self.completeness,
            ConnectorDocumentDiscoveryCompleteness::Complete
        ) {
            return Ok(None);
        }
        let remaining_from_page = previous
            .remaining_items()
            .checked_sub(self.items.len())
            .ok_or_else(|| exhausted("document discovery item budget underflowed"))?;
        let remaining = remaining_from_page.min(previous.budget.remaining_items()?);
        if remaining == 0 {
            if matches!(
                self.completeness,
                ConnectorDocumentDiscoveryCompleteness::Incomplete(
                    ConnectorDocumentDiscoveryIncompleteReason::ItemBudget
                )
            ) {
                return Ok(None);
            }
            return Err(invalid(
                "document discovery exhausted its item budget without an item-budget result",
            ));
        }
        let cursor = self.next_cursor.clone().ok_or_else(|| {
            invalid("incomplete document discovery page omitted its continuation cursor")
        })?;
        ConnectorDocumentDiscoveryRequest::next_from(previous, cursor, remaining, context).map(Some)
    }

    pub fn items(&self) -> &[ConnectorDocumentDiscoveryItem] {
        &self.items
    }
    pub const fn next_cursor(&self) -> Option<&Bytes> {
        self.next_cursor.as_ref()
    }
    pub const fn completeness(&self) -> ConnectorDocumentDiscoveryCompleteness {
        self.completeness
    }
}

pub trait ConnectorDocumentStorageObservation: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;

    /// Project only the already-frozen provider metadata source. Content that
    /// is not already available is returned as an opaque deferred handle and
    /// must not be loaded by this call.
    fn observe_documents(
        &self,
        request: ConnectorDocumentObservationRequest,
    ) -> Result<FrozenConnectorDocumentObservation, ConnectorError>;

    fn load_document(
        &self,
        request: ConnectorDocumentLoadRequest,
    ) -> Result<ConnectorDocument, ConnectorError>;

    /// Observe the current table entry used for ownership and incarnation
    /// checks. A historical frozen observation is not accepted as a substitute.
    fn observe_current_management(
        &self,
        request: ConnectorDocumentObservationRequest,
    ) -> Result<ConnectorDocumentManagementObservation, ConnectorError>;

    fn discover_documents(
        &self,
        request: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError>;
}

pub(crate) fn validate_observation_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    capability: &dyn ConnectorDocumentStorageObservation,
) -> Result<(), ConnectorError> {
    if capability.descriptor() != descriptor || capability.incarnation() != incarnation {
        return Err(invalid(
            "document observation capability does not match its control generation",
        ));
    }
    Ok(())
}

pub(crate) fn validate_table(table: &ConnectorTableIdentity) -> Result<(), ConnectorError> {
    if table.namespace.is_empty() || table.table.is_empty() {
        return Err(invalid(
            "document storage target must name a namespace and table",
        ));
    }
    Ok(())
}

pub(crate) fn validate_owner_key(
    expected: &ConnectorProviderBindingKey,
    actual: &ConnectorProviderBindingKey,
) -> Result<(), ConnectorError> {
    if expected != actual {
        return Err(invalid(
            "document storage request does not match the retained control generation",
        ));
    }
    Ok(())
}

fn observation_request_digest(request: &ConnectorDocumentObservationRequest) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-document-observation.v1\0");
    digest_bytes(&mut hasher, request.owner().instance_id.as_str().as_bytes());
    hasher.update(request.owner().incarnation.to_bytes());
    hasher.update(request.catalog_handle().version().as_bytes());
    digest_bytes(&mut hasher, request.target().namespace.as_bytes());
    digest_bytes(&mut hasher, request.target().table.as_bytes());
    digest_bytes(&mut hasher, request.expected_object_id().as_bytes());
    digest_limits(&mut hasher, request.limits());
    hasher.finalize().into()
}

fn discovery_request_digest(request: &ConnectorDocumentDiscoveryRequest) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-document-discovery.v1\0");
    digest_bytes(&mut hasher, request.owner().instance_id.as_str().as_bytes());
    hasher.update(request.owner().incarnation.to_bytes());
    hasher.update(request.catalog_handle().version().as_bytes());
    digest_optional_bytes(
        &mut hasher,
        request.namespace().map(|value| value.as_bytes()),
    );
    digest_optional_bytes(&mut hasher, request.cursor().map(Bytes::as_ref));
    hasher.update((request.page_size() as u64).to_be_bytes());
    hasher.update((request.remaining_items() as u64).to_be_bytes());
    hasher.finalize().into()
}

fn digest_limits(hasher: &mut Sha256, limits: ConnectorDocumentStorageLimits) {
    hasher.update((limits.max_document_bytes() as u64).to_be_bytes());
    hasher.update((limits.max_load_total_bytes() as u64).to_be_bytes());
    hasher.update((limits.max_decode_working_set_bytes() as u64).to_be_bytes());
    hasher.update((limits.max_documents() as u64).to_be_bytes());
    hasher.update((limits.max_references() as u64).to_be_bytes());
    hasher.update((limits.max_structure_depth() as u64).to_be_bytes());
}

fn digest_optional_bytes(hasher: &mut Sha256, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            digest_bytes(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}
