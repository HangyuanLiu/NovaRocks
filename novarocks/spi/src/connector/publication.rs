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

//! Provider-neutral identity and diagnostic vocabulary for one lake publication.
// Design: ADR-0110 (docs/adr/ADR-0110-lake-publication-crash-only-contract.md)

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ConnectorError, ConnectorErrorKind};

/// The canonical 128-bit identity frozen before a mutating statement can
/// dispatch an external request.
///
/// A value decoded from a legacy durable payload is intentionally preserved as
/// bytes. New statement admission must use [`LakePublicationId::new_v7`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LakePublicationId(Uuid);

impl LakePublicationId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn try_from_uuid(value: Uuid) -> Result<Self, ConnectorError> {
        if value.get_version_num() != 7 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "lake publication ID must be UUIDv7",
            ));
        }
        Ok(Self(value))
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, ConnectorError> {
        Self::try_from_uuid(Uuid::from_bytes(bytes))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for LakePublicationId {
    fn default() -> Self {
        Self::new_v7()
    }
}

impl From<Uuid> for LakePublicationId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl fmt::Display for LakePublicationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for LakePublicationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Statement family recorded in a common marker and a user-visible terminal
/// diagnostic. It is deliberately descriptive, not a retry discriminator.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LakePublicationFamily {
    Write,
    DataMutation,
    CatalogMutation,
    Ctas,
    MaterializedViewRefresh,
    MetadataMaintenance,
    Statistics,
}

impl LakePublicationFamily {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::DataMutation => "data_mutation",
            Self::CatalogMutation => "catalog_mutation",
            Self::Ctas => "ctas",
            Self::MaterializedViewRefresh => "materialized_view_refresh",
            Self::MetadataMaintenance => "metadata_maintenance",
            Self::Statistics => "statistics",
        }
    }
}

impl fmt::Display for LakePublicationFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The only terminal knowledge states a caller may expose for a dispatched
/// lake publication. `CommitUnknown` never grants a follow-up mutation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LakePublicationDisposition {
    KnownUncommitted,
    CommitUnknown,
    KnownCommitted,
}

impl LakePublicationDisposition {
    pub const fn do_not_retry(self) -> bool {
        matches!(self, Self::CommitUnknown | Self::KnownCommitted)
    }
}

/// The sole caller action following a terminal publication disposition. It is
/// diagnostic only: it cannot authorize another provider mutation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LakePublicationNextAction {
    RetryStatement,
    InspectPublishedState,
}

/// The provider-neutral header every NovaRocks-owned lake marker must carry.
/// Family-specific payloads follow this header and remain provider-owned.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LakePublicationMarkerHeader {
    version: u8,
    publication_id: LakePublicationId,
    family: LakePublicationFamily,
}

impl LakePublicationMarkerHeader {
    pub const VERSION: u8 = 1;

    pub const fn new(publication_id: LakePublicationId, family: LakePublicationFamily) -> Self {
        Self {
            version: Self::VERSION,
            publication_id,
            family,
        }
    }

    pub const fn publication_id(self) -> LakePublicationId {
        self.publication_id
    }

    pub const fn family(self) -> LakePublicationFamily {
        self.family
    }

    pub fn validate(self) -> Result<(), ConnectorError> {
        if self.version != Self::VERSION {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "unsupported lake publication marker header version {}",
                    self.version
                ),
            ));
        }
        Ok(())
    }
}

/// Human-readable publication target retained in terminal diagnostics. This
/// projection is never used for authorization, OCC, identity comparison, or
/// retry de-duplication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LakePublicationTarget {
    catalog: String,
    namespace: String,
    table: Option<String>,
    reference: Option<String>,
}

impl LakePublicationTarget {
    pub fn try_new(
        catalog: String,
        namespace: String,
        table: Option<String>,
        reference: Option<String>,
    ) -> Result<Self, ConnectorError> {
        if catalog.trim().is_empty() || namespace.trim().is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "lake publication target requires a catalog and namespace",
            ));
        }
        if table
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
            || reference
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "lake publication target components must not be empty",
            ));
        }
        Ok(Self {
            catalog,
            namespace,
            table,
            reference,
        })
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }
    pub fn reference(&self) -> Option<&str> {
        self.reference.as_deref()
    }
}

/// Optional caller-supplied diagnostic tag. It is deliberately not an
/// identity component and has no effect on authorization, OCC or retries.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct LakePublicationStatementTag(String);

impl LakePublicationStatementTag {
    pub const MAX_BYTES: usize = 1_024;

    pub fn try_new(value: String) -> Result<Self, ConnectorError> {
        if value.trim().is_empty() || value.len() > Self::MAX_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "lake publication statement tag must be non-empty and at most 1024 bytes",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The public, provider-neutral terminal projection for a lake publication.
/// Provider receipts and reconciliation evidence retain their native codecs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LakePublicationTerminal {
    header: LakePublicationMarkerHeader,
    target: LakePublicationTarget,
    disposition: LakePublicationDisposition,
    next_action: LakePublicationNextAction,
    client_statement_tag: Option<LakePublicationStatementTag>,
}

impl LakePublicationTerminal {
    pub const fn new(
        header: LakePublicationMarkerHeader,
        target: LakePublicationTarget,
        disposition: LakePublicationDisposition,
        next_action: LakePublicationNextAction,
        client_statement_tag: Option<LakePublicationStatementTag>,
    ) -> Self {
        Self {
            header,
            target,
            disposition,
            next_action,
            client_statement_tag,
        }
    }

    pub const fn header(&self) -> LakePublicationMarkerHeader {
        self.header
    }
    pub fn target(&self) -> &LakePublicationTarget {
        &self.target
    }
    pub const fn disposition(&self) -> LakePublicationDisposition {
        self.disposition
    }
    pub const fn do_not_retry(&self) -> bool {
        self.disposition.do_not_retry()
    }
    pub const fn next_action(&self) -> LakePublicationNextAction {
        self.next_action
    }
    pub fn client_statement_tag(&self) -> Option<&LakePublicationStatementTag> {
        self.client_statement_tag.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{
        LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
        LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationStatementTag,
        LakePublicationTarget, LakePublicationTerminal,
    };

    #[test]
    fn publication_id_has_one_canonical_round_trip() {
        let id = LakePublicationId::new_v7();
        assert_eq!(LakePublicationId::from_str(&id.to_string()).unwrap(), id);
        assert_eq!(
            LakePublicationId::try_from_bytes(id.to_bytes()).unwrap(),
            id
        );
    }

    #[test]
    fn new_publication_id_rejects_non_v7_input() {
        assert!(LakePublicationId::try_from_bytes([7; 16]).is_err());
    }

    #[test]
    fn marker_header_keeps_the_statement_identity() {
        let id = LakePublicationId::new_v7();
        let header = LakePublicationMarkerHeader::new(id, LakePublicationFamily::Ctas);
        assert_eq!(header.publication_id(), id);
        assert_eq!(header.family(), LakePublicationFamily::Ctas);
        header.validate().unwrap();
    }

    #[test]
    fn unknown_and_committed_are_never_retryable() {
        assert!(!LakePublicationDisposition::KnownUncommitted.do_not_retry());
        assert!(LakePublicationDisposition::CommitUnknown.do_not_retry());
        assert!(LakePublicationDisposition::KnownCommitted.do_not_retry());
    }

    #[test]
    fn terminal_projects_one_diagnostic_identity_without_affecting_retry_policy() {
        let id = LakePublicationId::new_v7();
        let terminal = LakePublicationTerminal::new(
            LakePublicationMarkerHeader::new(id, LakePublicationFamily::CatalogMutation),
            LakePublicationTarget::try_new(
                "ice".to_string(),
                "db".to_string(),
                Some("t".to_string()),
                Some("main".to_string()),
            )
            .unwrap(),
            LakePublicationDisposition::CommitUnknown,
            LakePublicationNextAction::InspectPublishedState,
            Some(LakePublicationStatementTag::try_new("billing-refresh".to_string()).unwrap()),
        );
        assert_eq!(terminal.header().publication_id(), id);
        assert!(terminal.do_not_retry());
        assert_eq!(
            terminal.next_action(),
            LakePublicationNextAction::InspectPublishedState
        );
        assert_eq!(terminal.target().table(), Some("t"));
        assert_eq!(
            terminal
                .client_statement_tag()
                .map(LakePublicationStatementTag::as_str),
            Some("billing-refresh")
        );
    }
}
