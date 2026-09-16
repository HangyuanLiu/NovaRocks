// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{fmt, sync::Arc};

use bytes::Bytes;

use super::{ConnectorError, ConnectorErrorKind, ConnectorProviderId, ConnectorTableObjectId};

pub const MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES: usize = 128;
pub const MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES: usize = 256;

const TABLE_OBJECT_ID_FORMAT: &str = "connector-table-object-id";
const SNAPSHOT_ID_FORMAT: &str = "connector-snapshot-id";
const SEMANTIC_FACT_VERSION_V1: u16 = 1;

/// A provider-issued, process-independent semantic fact.
///
/// Consumers may compare the complete value but cannot construct or interpret
/// one without going through a Connector provider or another SPI-owned
/// observation adapter.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorSemanticFact {
    provider: ConnectorProviderId,
    format: Arc<str>,
    version: u16,
    value: Bytes,
}

impl ConnectorSemanticFact {
    fn try_new(
        provider: ConnectorProviderId,
        format: impl Into<Arc<str>>,
        version: u16,
        value: Bytes,
    ) -> Result<Self, ConnectorError> {
        let format = format.into();
        if format.is_empty()
            || format.len() > MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES
            || !format.is_ascii()
            || version == 0
            || value.is_empty()
            || value.len() > MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector semantic revision fact is invalid",
            ));
        }
        Ok(Self {
            provider,
            format,
            version,
            value,
        })
    }

    pub const fn provider(&self) -> &ConnectorProviderId {
        &self.provider
    }

    pub fn format(&self) -> &str {
        &self.format
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn value(&self) -> &Bytes {
        &self.value
    }
}

impl fmt::Debug for ConnectorSemanticFact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorSemanticFact")
            .field("provider", &self.provider)
            .field("format", &self.format)
            .field("version", &self.version)
            .field("value_len", &self.value.len())
            .finish()
    }
}

/// Stable identity and data-version facts for one exact table observation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorExactSemanticRevision {
    object_identity: ConnectorSemanticFact,
    data_version: ConnectorSemanticFact,
}

impl ConnectorExactSemanticRevision {
    /// Build the common table-object/snapshot revision used by providers whose
    /// catalog publishes a stable object ID and an exact numeric snapshot.
    pub fn try_from_table_object_and_snapshot(
        provider: ConnectorProviderId,
        object_identity: &ConnectorTableObjectId,
        snapshot_id: Option<i64>,
    ) -> Result<Self, ConnectorError> {
        let object_identity = ConnectorSemanticFact::try_new(
            provider.clone(),
            TABLE_OBJECT_ID_FORMAT,
            SEMANTIC_FACT_VERSION_V1,
            object_identity.as_bytes().clone(),
        )?;
        let mut encoded_snapshot = Vec::with_capacity(9);
        match snapshot_id {
            Some(snapshot_id) => {
                encoded_snapshot.push(1);
                encoded_snapshot.extend_from_slice(&snapshot_id.to_be_bytes());
            }
            None => encoded_snapshot.push(0),
        }
        let data_version = ConnectorSemanticFact::try_new(
            provider,
            SNAPSHOT_ID_FORMAT,
            SEMANTIC_FACT_VERSION_V1,
            Bytes::from(encoded_snapshot),
        )?;
        Ok(Self {
            object_identity,
            data_version,
        })
    }

    /// Restore the complete provider-issued facts carried by an
    /// application-owned durable publication.
    ///
    /// This does not interpret either value. It reapplies the SPI bounds and
    /// requires one provider identity for both halves so a persisted bare byte
    /// string can never be promoted into an exact revision by itself.
    #[allow(clippy::too_many_arguments)]
    pub fn try_from_persisted_facts(
        provider: ConnectorProviderId,
        object_format: impl Into<Arc<str>>,
        object_version: u16,
        object_value: Bytes,
        data_format: impl Into<Arc<str>>,
        data_version: u16,
        data_value: Bytes,
    ) -> Result<Self, ConnectorError> {
        let object_identity = ConnectorSemanticFact::try_new(
            provider.clone(),
            object_format,
            object_version,
            object_value,
        )?;
        let data_version =
            ConnectorSemanticFact::try_new(provider, data_format, data_version, data_value)?;
        Ok(Self {
            object_identity,
            data_version,
        })
    }

    pub const fn object_identity(&self) -> &ConnectorSemanticFact {
        &self.object_identity
    }

    pub const fn data_version(&self) -> &ConnectorSemanticFact {
        &self.data_version
    }

    /// The read point this revision names, when its data version is in this
    /// contract's own canonical snapshot form.
    ///
    /// This is not an interpretation of a provider-private fact: the form is
    /// the one [`Self::try_from_table_object_and_snapshot`] writes, and this
    /// only reads back what that wrote. A provider whose data version means
    /// something else -- a sequence number, a log offset, a change token --
    /// carries its own format string and is absent here, so a consumer that
    /// needs a snapshot fails closed rather than misreading one as a number.
    ///
    /// The answer says only what the revision names. Whether that point is
    /// still readable is the provider's to admit against the live table.
    pub fn canonical_read_point(&self) -> Option<ConnectorCanonicalReadPoint> {
        if self.data_version.format() != SNAPSHOT_ID_FORMAT
            || self.data_version.version() != SEMANTIC_FACT_VERSION_V1
        {
            return None;
        }
        match self.data_version.value().as_ref() {
            [0] => Some(ConnectorCanonicalReadPoint::Snapshot(None)),
            [1, snapshot @ ..] => <[u8; 8]>::try_from(snapshot).ok().map(|snapshot| {
                ConnectorCanonicalReadPoint::Snapshot(Some(i64::from_be_bytes(snapshot)))
            }),
            _ => None,
        }
    }
}

/// The read point a canonical exact revision names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorCanonicalReadPoint {
    /// One numeric snapshot, or the absence of one for an object that had
    /// published nothing when the revision was taken.
    Snapshot(Option<i64>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_revision_distinguishes_object_and_snapshot() {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let object = ConnectorTableObjectId::try_new(Bytes::from_static(b"table-a")).unwrap();
        let other = ConnectorTableObjectId::try_new(Bytes::from_static(b"table-b")).unwrap();
        let first = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            provider.clone(),
            &object,
            Some(101),
        )
        .unwrap();
        assert_ne!(
            first,
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider.clone(),
                &object,
                Some(102),
            )
            .unwrap()
        );
        assert_ne!(
            first,
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider,
                &other,
                Some(101),
            )
            .unwrap()
        );
    }

    #[test]
    fn exact_revision_restores_only_complete_persisted_provider_facts() {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let object = ConnectorTableObjectId::try_new(Bytes::from_static(b"table-a")).unwrap();
        let original = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            provider.clone(),
            &object,
            Some(101),
        )
        .unwrap();

        let restored = ConnectorExactSemanticRevision::try_from_persisted_facts(
            provider,
            original.object_identity().format(),
            original.object_identity().version(),
            original.object_identity().value().clone(),
            original.data_version().format(),
            original.data_version().version(),
            original.data_version().value().clone(),
        )
        .unwrap();

        assert_eq!(restored, original);
        assert!(
            ConnectorExactSemanticRevision::try_from_persisted_facts(
                ConnectorProviderId::parse("iceberg").unwrap(),
                "",
                1,
                Bytes::from_static(b"object"),
                "snapshot",
                1,
                Bytes::from_static(b"data"),
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod canonical_read_point_tests {
    use super::*;

    fn object() -> ConnectorTableObjectId {
        ConnectorTableObjectId::try_new(Bytes::from_static(b"object")).expect("object id")
    }

    fn provider() -> ConnectorProviderId {
        ConnectorProviderId::parse("iceberg").expect("provider")
    }

    #[test]
    fn a_canonical_revision_reads_back_the_snapshot_it_was_written_with() {
        let revision = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            provider(),
            &object(),
            Some(i64::MAX),
        )
        .expect("revision");

        assert_eq!(
            revision.canonical_read_point(),
            Some(ConnectorCanonicalReadPoint::Snapshot(Some(i64::MAX)))
        );
    }

    #[test]
    fn an_object_that_had_published_nothing_reads_back_as_no_snapshot() {
        let revision = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            provider(),
            &object(),
            None,
        )
        .expect("revision");

        assert_eq!(
            revision.canonical_read_point(),
            Some(ConnectorCanonicalReadPoint::Snapshot(None))
        );
    }

    #[test]
    fn a_data_version_in_another_form_is_absent_rather_than_misread() {
        let revision = ConnectorExactSemanticRevision::try_from_persisted_facts(
            provider(),
            TABLE_OBJECT_ID_FORMAT,
            SEMANTIC_FACT_VERSION_V1,
            Bytes::from_static(b"object"),
            "provider-sequence-number",
            SEMANTIC_FACT_VERSION_V1,
            Bytes::from_static(&[1, 0, 0, 0, 0, 0, 0, 0, 7]),
        )
        .expect("revision");

        assert_eq!(
            revision.canonical_read_point(),
            None,
            "a sequence number must not be read as a snapshot id"
        );
    }

    #[test]
    fn a_truncated_canonical_value_is_absent_rather_than_guessed() {
        let revision = ConnectorExactSemanticRevision::try_from_persisted_facts(
            provider(),
            TABLE_OBJECT_ID_FORMAT,
            SEMANTIC_FACT_VERSION_V1,
            Bytes::from_static(b"object"),
            SNAPSHOT_ID_FORMAT,
            SEMANTIC_FACT_VERSION_V1,
            Bytes::from_static(&[1, 0, 0]),
        )
        .expect("revision");

        assert_eq!(revision.canonical_read_point(), None);
    }
}
