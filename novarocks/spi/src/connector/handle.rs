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

use std::sync::Arc;

use bytes::Bytes;

use super::{ConnectorError, ConnectorErrorKind, ConnectorInstanceId};

pub const MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableHandle {
    owner: ConnectorInstanceId,
    payload: Bytes,
}

impl ConnectorTableHandle {
    pub fn try_new(owner: ConnectorInstanceId, payload: Bytes) -> Result<Self, ConnectorError> {
        validate_payload(&payload)?;
        Ok(Self { owner, payload })
    }

    pub fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorScanHandle {
    owner: ConnectorInstanceId,
    payload: Bytes,
}

impl ConnectorScanHandle {
    pub fn try_new(owner: ConnectorInstanceId, payload: Bytes) -> Result<Self, ConnectorError> {
        validate_payload(&payload)?;
        Ok(Self { owner, payload })
    }

    pub fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorSplit {
    owner: ConnectorInstanceId,
    split_id: Arc<str>,
    payload: Bytes,
    estimated_bytes: Option<u64>,
}

impl ConnectorSplit {
    pub fn try_new(
        owner: ConnectorInstanceId,
        split_id: impl Into<Arc<str>>,
        payload: Bytes,
        estimated_bytes: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        let split_id = split_id.into();
        if split_id.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector split ID must not be empty",
            ));
        }
        validate_payload(&payload)?;
        Ok(Self {
            owner,
            split_id,
            payload,
            estimated_bytes,
        })
    }

    pub fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }

    pub fn split_id(&self) -> &str {
        &self.split_id
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn estimated_bytes(&self) -> Option<u64> {
        self.estimated_bytes
    }
}

fn validate_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector handle payload exceeds the hard limit",
        ));
    }
    Ok(())
}

/// The hard bound on how many files one pinned read may name.
pub const MAX_CONNECTOR_PINNED_FILES: usize = 4096;

/// Exactly the files one provider-frozen cohort reads, at the relation version
/// they were frozen at.
///
/// This is a set the connector mints while preparing its own mutation or
/// rewrite; the engine only carries it back to that connector. It is named
/// rather than described because the cohort's commit replaces precisely these
/// files: a read narrowed by a predicate, a size threshold, or a re-derived
/// selection would let the read and the commit disagree, which corrupts the
/// relation instead of returning a wrong answer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorPinnedFileSet {
    namespace: Arc<str>,
    table: Arc<str>,
    version_ordinal: i64,
    files: Vec<Arc<str>>,
}

impl ConnectorPinnedFileSet {
    /// The relation is named here, and not recovered from whatever synthetic
    /// name the engine planned the cohort under: only the connector knows
    /// which relation its own frozen files belong to.
    ///
    /// `version_ordinal` is the provider's own version identity, exactly as
    /// `ConnectorWritePreparation::base_version_ordinal` reports it.
    ///
    /// An empty set is legal and reads no rows. The files are sorted and
    /// deduplicated here so one pinned read has one spelling everywhere.
    pub fn try_new<F: AsRef<str>>(
        namespace: impl AsRef<str>,
        table: impl AsRef<str>,
        version_ordinal: i64,
        files: impl IntoIterator<Item = F>,
    ) -> Result<Self, ConnectorError> {
        let namespace = namespace.as_ref();
        let table = table.as_ref();
        if namespace.is_empty() || table.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector pinned file set requires a complete relation name",
            ));
        }
        let mut sorted = Vec::new();
        for file in files {
            let file = file.as_ref();
            if file.is_empty() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector pinned file identity must not be empty",
                ));
            }
            sorted.push(Arc::<str>::from(file));
        }
        sorted.sort();
        let named = sorted.len();
        sorted.dedup();
        if sorted.len() != named {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector pinned file set names the same file more than once",
            ));
        }
        if sorted.len() > MAX_CONNECTOR_PINNED_FILES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector pinned file count exceeds the hard limit",
            ));
        }
        Ok(Self {
            namespace: Arc::from(namespace),
            table: Arc::from(table),
            version_ordinal,
            files: sorted,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub const fn version_ordinal(&self) -> i64 {
        self.version_ordinal
    }

    pub fn files(&self) -> &[Arc<str>] {
        &self.files
    }
}
