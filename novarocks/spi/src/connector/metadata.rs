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

use arrow::datatypes::SchemaRef;
use bytes::Bytes;

use super::{
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableHandle,
    StatisticsDataVersion,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorNamespaceIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorTableIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
    pub table: Arc<str>,
}

#[derive(Clone)]
pub struct ConnectorTableMetadata {
    pub identity: ConnectorTableIdentity,
    pub schema: SchemaRef,
    /// Provider-owned schema identity. This remains deliberately distinct
    /// from the data-version pin used by statistics and scan planning.
    pub version: Option<Bytes>,
    /// Opaque data-version resolved together with this table metadata. Core
    /// must pass this exact pin to both scan and statistics consumers rather
    /// than resolving `latest` a second time.
    pub statistics_data_version: Option<StatisticsDataVersion>,
    pub table: ConnectorTableHandle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorTableResolution {
    StrictBaseTable,
    ProviderReadAlias,
}

#[derive(Clone)]
pub struct ConnectorNamespaceRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorTableRequest {
    pub table: ConnectorTableIdentity,
    pub resolution: ConnectorTableResolution,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorListTablesRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorListNamespacesRequest {
    pub instance_id: ConnectorInstanceId,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorReadReferenceFactsRequest {
    pub table: ConnectorTableIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadReferenceKind {
    Branch,
    Tag,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadNamedReference {
    pub name: Arc<str>,
    pub kind: ConnectorReadReferenceKind,
    pub snapshot_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadSnapshotLogEntry {
    pub snapshot_id: i64,
    pub timestamp_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadReferenceFacts {
    snapshot_ids: Vec<i64>,
    snapshot_log: Vec<ConnectorReadSnapshotLogEntry>,
    named_references: Vec<ConnectorReadNamedReference>,
    current_snapshot_id: Option<i64>,
}

impl ConnectorReadReferenceFacts {
    pub fn try_new(
        mut snapshot_ids: Vec<i64>,
        mut snapshot_log: Vec<ConnectorReadSnapshotLogEntry>,
        mut named_references: Vec<ConnectorReadNamedReference>,
        current_snapshot_id: Option<i64>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        snapshot_ids.sort_unstable();
        if snapshot_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts contain duplicate snapshot IDs",
            ));
        }

        let contains_snapshot = |snapshot_id| snapshot_ids.binary_search(&snapshot_id).is_ok();
        if current_snapshot_id.is_some_and(|snapshot_id| !contains_snapshot(snapshot_id)) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts current snapshot is not listed",
            ));
        }

        snapshot_log.sort_by_key(|entry| (entry.timestamp_millis, entry.snapshot_id));
        if snapshot_log
            .iter()
            .any(|entry| !contains_snapshot(entry.snapshot_id))
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts snapshot log references an unknown snapshot",
            ));
        }
        if snapshot_log.windows(2).any(|pair| {
            pair[0].timestamp_millis == pair[1].timestamp_millis
                && pair[0].snapshot_id == pair[1].snapshot_id
        }) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts contain duplicate snapshot-log entries",
            ));
        }

        named_references.sort_by(|left, right| left.name.cmp(&right.name));
        let mut previous_name: Option<&str> = None;
        for reference in &named_references {
            if reference.name.is_empty() || !contains_snapshot(reference.snapshot_id) {
                return Err(ConnectorError::new(
                    super::ConnectorErrorKind::CorruptData,
                    "connector read reference facts contain an invalid named reference",
                ));
            }
            if previous_name == Some(reference.name.as_ref()) {
                return Err(ConnectorError::new(
                    super::ConnectorErrorKind::CorruptData,
                    "connector read reference facts contain duplicate named references",
                ));
            }
            previous_name = Some(reference.name.as_ref());
        }

        let bytes = snapshot_ids
            .len()
            .saturating_mul(std::mem::size_of::<i64>())
            + snapshot_log
                .len()
                .saturating_mul(2 * std::mem::size_of::<i64>())
            + named_references.iter().fold(0usize, |total, reference| {
                total
                    .saturating_add(reference.name.len())
                    .saturating_add(std::mem::size_of::<i64>())
                    .saturating_add(1)
            })
            + usize::from(current_snapshot_id.is_some()) * std::mem::size_of::<i64>();
        if bytes > context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::ResourceExhausted,
                "connector read reference facts exceed request total payload budget",
            ));
        }

        Ok(Self {
            snapshot_ids,
            snapshot_log,
            named_references,
            current_snapshot_id,
        })
    }

    pub fn snapshot_ids(&self) -> &[i64] {
        &self.snapshot_ids
    }

    pub fn snapshot_log(&self) -> &[ConnectorReadSnapshotLogEntry] {
        &self.snapshot_log
    }

    pub fn named_references(&self) -> &[ConnectorReadNamedReference] {
        &self.named_references
    }

    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }
}

pub trait ConnectorMetadata: Send + Sync {
    fn instance_id(&self) -> &ConnectorInstanceId;

    fn list_namespaces(
        &self,
        _request: ConnectorListNamespacesRequest,
    ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "connector metadata does not support namespace enumeration",
        ))
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError>;

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError>;

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError>;

    fn read_reference_facts(
        &self,
        _request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "connector metadata does not support read reference facts",
        ))
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError>;
}
