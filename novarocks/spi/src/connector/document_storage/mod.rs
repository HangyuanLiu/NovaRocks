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

//! Provider-neutral storage for application-owned durable documents.
//!
//! Applications own document schemas and codecs. Connectors own the physical
//! envelope, attachment point, atomic mutation, discovery, and retention. The
//! SPI treats every document body as opaque bytes and deliberately exposes no
//! arbitrary table-property patch API.

mod document;
mod observation;
mod operation;
mod retention;

pub use document::{
    ConnectorDocument, ConnectorDocumentAttachment, ConnectorDocumentFormat, ConnectorDocumentId,
    ConnectorDocumentName, ConnectorDocumentOwner, ConnectorDocumentReference,
    ConnectorDocumentRevision, ConnectorDocumentSet, MAX_CONNECTOR_DOCUMENT_BYTES,
    MAX_CONNECTOR_DOCUMENT_FORMAT_BYTES, MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
    MAX_CONNECTOR_DOCUMENT_REFERENCES, MAX_CONNECTOR_DOCUMENT_SET_BYTES, MAX_CONNECTOR_DOCUMENTS,
};
pub use observation::{
    ConnectorDeferredDocumentHandle, ConnectorDocumentCarrier,
    ConnectorDocumentDiscoveryCompleteness, ConnectorDocumentDiscoveryIncompleteReason,
    ConnectorDocumentDiscoveryItem, ConnectorDocumentDiscoveryPage,
    ConnectorDocumentDiscoveryRequest, ConnectorDocumentLoadRequest,
    ConnectorDocumentManagementObservation, ConnectorDocumentObservationRequest,
    ConnectorDocumentStorageBudget, ConnectorDocumentStorageObservation,
    ConnectorManagedObjectMarker, ConnectorStoredDocument, ConnectorStoredDocumentAttachment,
    FrozenConnectorDocumentObservation, MAX_CONNECTOR_DEFERRED_DOCUMENT_HANDLE_BYTES,
    MAX_CONNECTOR_DOCUMENT_DISCOVERY_PAGE_SIZE,
};
pub use operation::{
    CONNECTOR_DOCUMENT_STORAGE_CONTRACT_VERSION, ConnectorDocumentCreatePublicationIntent,
    ConnectorDocumentManagementAdmission, ConnectorDocumentManagementAdmissionRequest,
    ConnectorDocumentManagementOperation, ConnectorDocumentPublicationDeclaration,
    ConnectorDocumentPublicationIntent, ConnectorDocumentStorageBinding,
    ConnectorDocumentStorageLease, ConnectorDocumentStorageLimits,
    ConnectorDocumentStorageManagement, ConnectorDocumentUpdateIntent,
    ConnectorManagedObjectMarkerChange, ConnectorPrepareDocumentsRequest,
    ConnectorPreparedDocument, ConnectorPreparedDocumentSet,
    DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_BYTES,
    DEFAULT_CONNECTOR_DOCUMENT_AVAILABLE_CONTENT_TOTAL_BYTES,
    DEFAULT_CONNECTOR_DOCUMENT_DECODE_WORKING_SET_BYTES,
    DEFAULT_CONNECTOR_DOCUMENT_LOAD_TOTAL_BYTES, MAX_CONNECTOR_DOCUMENT_STRUCTURE_DEPTH,
};
pub use retention::{
    ConnectorDocumentRetentionConstraint, ConnectorDocumentRetentionGraph,
    ConnectorDocumentRetentionRoot,
};
