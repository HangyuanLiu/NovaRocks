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
use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::super::{ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind};

pub const MAX_CONNECTOR_DOCUMENT_NAME_BYTES: usize = 128;
pub const MAX_CONNECTOR_DOCUMENT_FORMAT_BYTES: usize = 128;
pub const MAX_CONNECTOR_DOCUMENT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONNECTOR_DOCUMENT_SET_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_CONNECTOR_DOCUMENTS: usize = 4096;
pub const MAX_CONNECTOR_DOCUMENT_REFERENCES: usize = 4096;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorDocumentOwner(Arc<str>);

impl ConnectorDocumentOwner {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ConnectorError> {
        validate_token(
            value.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "document owner",
        )?;
        Ok(Self(Arc::from(value.as_ref())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorDocumentName(Arc<str>);

impl ConnectorDocumentName {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ConnectorError> {
        validate_token(
            value.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "document name",
        )?;
        Ok(Self(Arc::from(value.as_ref())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorDocumentFormat {
    owner: Arc<str>,
    name: Arc<str>,
    version: u32,
}

impl ConnectorDocumentFormat {
    pub fn try_new(
        owner: impl AsRef<str>,
        name: impl AsRef<str>,
        version: u32,
    ) -> Result<Self, ConnectorError> {
        validate_token(
            owner.as_ref(),
            MAX_CONNECTOR_DOCUMENT_FORMAT_BYTES,
            "document format owner",
        )?;
        validate_token(
            name.as_ref(),
            MAX_CONNECTOR_DOCUMENT_FORMAT_BYTES,
            "document format name",
        )?;
        if version == 0 {
            return Err(invalid("document format version must be non-zero"));
        }
        Ok(Self {
            owner: Arc::from(owner.as_ref()),
            name: Arc::from(name.as_ref()),
            version,
        })
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn version(&self) -> u32 {
        self.version
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorDocumentRevision([u8; 32]);

impl ConnectorDocumentRevision {
    pub fn for_content(content: &[u8]) -> Self {
        Self(Sha256::digest(content).into())
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorDocumentId {
    owner: ConnectorDocumentOwner,
    name: ConnectorDocumentName,
    revision: ConnectorDocumentRevision,
}

impl ConnectorDocumentId {
    pub const fn new(
        owner: ConnectorDocumentOwner,
        name: ConnectorDocumentName,
        revision: ConnectorDocumentRevision,
    ) -> Self {
        Self {
            owner,
            name,
            revision,
        }
    }

    pub const fn owner(&self) -> &ConnectorDocumentOwner {
        &self.owner
    }

    pub const fn name(&self) -> &ConnectorDocumentName {
        &self.name
    }

    pub const fn revision(&self) -> ConnectorDocumentRevision {
        self.revision
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentReference {
    relationship: Arc<str>,
    target: ConnectorDocumentId,
}

impl ConnectorDocumentReference {
    pub fn try_new(
        relationship: impl AsRef<str>,
        target: ConnectorDocumentId,
    ) -> Result<Self, ConnectorError> {
        validate_token(
            relationship.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "document relationship",
        )?;
        Ok(Self {
            relationship: Arc::from(relationship.as_ref()),
            target,
        })
    }

    pub fn relationship(&self) -> &str {
        &self.relationship
    }

    pub const fn target(&self) -> &ConnectorDocumentId {
        &self.target
    }
}

/// The public attachment intent. `CommitOutput` lets the provider bind the
/// version allocated at the atomic commit point; callers never predict it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorDocumentAttachment {
    TableMetadata,
    ExactOutput(ConnectorCommittedVersion),
    CommitOutput,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDocument {
    id: ConnectorDocumentId,
    format: ConnectorDocumentFormat,
    content: Bytes,
    references: Vec<ConnectorDocumentReference>,
    attachment: ConnectorDocumentAttachment,
}

impl ConnectorDocument {
    pub fn try_new(
        owner: ConnectorDocumentOwner,
        name: ConnectorDocumentName,
        format: ConnectorDocumentFormat,
        content: Bytes,
        references: Vec<ConnectorDocumentReference>,
        attachment: ConnectorDocumentAttachment,
    ) -> Result<Self, ConnectorError> {
        if content.is_empty() {
            return Err(invalid("document content must not be empty"));
        }
        if content.len() > MAX_CONNECTOR_DOCUMENT_BYTES {
            return Err(exhausted("document content exceeds the per-document limit"));
        }
        if references.len() > MAX_CONNECTOR_DOCUMENT_REFERENCES {
            return Err(exhausted("document references exceed the item limit"));
        }
        if let ConnectorDocumentAttachment::ExactOutput(version) = &attachment {
            version.validate()?;
        }
        let id = ConnectorDocumentId::new(
            owner,
            name,
            ConnectorDocumentRevision::for_content(&content),
        );
        let mut seen = HashSet::with_capacity(references.len());
        for reference in &references {
            let key = (reference.relationship(), reference.target());
            if reference.target() == &id || !seen.insert(key) {
                return Err(invalid(
                    "document references must be unique and must not self-reference",
                ));
            }
        }
        Ok(Self {
            id,
            format,
            content,
            references,
            attachment,
        })
    }

    pub const fn id(&self) -> &ConnectorDocumentId {
        &self.id
    }

    pub const fn format(&self) -> &ConnectorDocumentFormat {
        &self.format
    }

    pub const fn content(&self) -> &Bytes {
        &self.content
    }

    pub fn references(&self) -> &[ConnectorDocumentReference] {
        &self.references
    }

    pub const fn attachment(&self) -> &ConnectorDocumentAttachment {
        &self.attachment
    }
}

impl std::fmt::Debug for ConnectorDocument {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorDocument")
            .field("id", &self.id)
            .field("format", &self.format)
            .field("content_bytes", &self.content.len())
            .field("references", &self.references)
            .field("attachment", &self.attachment)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentSet {
    documents: Vec<ConnectorDocument>,
    encoded_bytes: usize,
}

impl ConnectorDocumentSet {
    pub fn try_new(documents: Vec<ConnectorDocument>) -> Result<Self, ConnectorError> {
        if documents.is_empty() || documents.len() > MAX_CONNECTOR_DOCUMENTS {
            return Err(invalid(
                "document set must contain a bounded non-empty document list",
            ));
        }
        let mut encoded_bytes = 0usize;
        let mut reference_count = 0usize;
        let mut names = HashSet::with_capacity(documents.len());
        let mut ids = HashSet::with_capacity(documents.len());
        for document in &documents {
            if !names.insert((document.id().owner(), document.id().name()))
                || !ids.insert(document.id())
            {
                return Err(invalid(
                    "document set contains a duplicate document name or identity",
                ));
            }
            encoded_bytes = encoded_bytes
                .checked_add(document.content().len())
                .ok_or_else(|| exhausted("document set byte accounting overflowed"))?;
            reference_count = reference_count
                .checked_add(document.references().len())
                .ok_or_else(|| exhausted("document set reference accounting overflowed"))?;
            if encoded_bytes > MAX_CONNECTOR_DOCUMENT_SET_BYTES {
                return Err(exhausted(
                    "document set exceeds the total encoded byte limit",
                ));
            }
            if reference_count > MAX_CONNECTOR_DOCUMENT_REFERENCES {
                return Err(exhausted("document set exceeds the total reference limit"));
            }
        }
        Ok(Self {
            documents,
            encoded_bytes,
        })
    }

    pub fn documents(&self) -> &[ConnectorDocument] {
        &self.documents
    }

    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
}

pub(crate) fn validate_token(
    value: &str,
    max_bytes: usize,
    field: &str,
) -> Result<(), ConnectorError> {
    if value.is_empty()
        || value.len() > max_bytes
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte == 0)
    {
        return Err(invalid(format!(
            "{field} must be bounded non-whitespace ASCII"
        )));
    }
    Ok(())
}

pub(crate) fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

pub(crate) fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn format() -> ConnectorDocumentFormat {
        ConnectorDocumentFormat::try_new("novarocks.mv", "definition", 1).unwrap()
    }

    fn owner() -> ConnectorDocumentOwner {
        ConnectorDocumentOwner::parse("novarocks.mv").unwrap()
    }

    #[test]
    fn revision_is_content_addressed_and_provider_neutral() {
        let left = ConnectorDocument::try_new(
            owner(),
            ConnectorDocumentName::parse("definition").unwrap(),
            format(),
            Bytes::from_static(b"opaque-domain-bytes"),
            vec![],
            ConnectorDocumentAttachment::TableMetadata,
        )
        .unwrap();
        let right = ConnectorDocument::try_new(
            owner(),
            ConnectorDocumentName::parse("definition").unwrap(),
            format(),
            Bytes::from_static(b"opaque-domain-bytes"),
            vec![],
            ConnectorDocumentAttachment::CommitOutput,
        )
        .unwrap();
        assert_eq!(left.id().revision(), right.id().revision());
    }

    #[test]
    fn document_set_rejects_duplicate_names_even_for_distinct_revisions() {
        let make = |content| {
            ConnectorDocument::try_new(
                owner(),
                ConnectorDocumentName::parse("configuration").unwrap(),
                ConnectorDocumentFormat::try_new("novarocks.mv", "configuration", 1).unwrap(),
                content,
                vec![],
                ConnectorDocumentAttachment::TableMetadata,
            )
            .unwrap()
        };
        let error = ConnectorDocumentSet::try_new(vec![
            make(Bytes::from_static(b"one")),
            make(Bytes::from_static(b"two")),
        ])
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn documents_are_bounded_before_they_enter_a_set() {
        let error = ConnectorDocument::try_new(
            owner(),
            ConnectorDocumentName::parse("definition").unwrap(),
            format(),
            Bytes::from(vec![0; MAX_CONNECTOR_DOCUMENT_BYTES + 1]),
            vec![],
            ConnectorDocumentAttachment::TableMetadata,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn document_set_enforces_the_reference_budget_across_documents() {
        let make = |name: &str, start: usize| {
            let references = (start..start + (MAX_CONNECTOR_DOCUMENT_REFERENCES / 2 + 1))
                .map(|index| {
                    ConnectorDocumentReference::try_new(
                        "depends-on",
                        ConnectorDocumentId::new(
                            owner(),
                            ConnectorDocumentName::parse(format!("r{index}")).unwrap(),
                            ConnectorDocumentRevision::from_bytes([index as u8; 32]),
                        ),
                    )
                    .unwrap()
                })
                .collect();
            ConnectorDocument::try_new(
                owner(),
                ConnectorDocumentName::parse(name).unwrap(),
                format(),
                Bytes::copy_from_slice(name.as_bytes()),
                references,
                ConnectorDocumentAttachment::TableMetadata,
            )
            .unwrap()
        };
        let error = ConnectorDocumentSet::try_new(vec![
            make("definition", 0),
            make("interpretation", MAX_CONNECTOR_DOCUMENT_REFERENCES),
        ])
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }
}
