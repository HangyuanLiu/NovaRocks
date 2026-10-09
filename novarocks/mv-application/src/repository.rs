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

//! Provider-neutral port for the lake-sourced MV Accelerator.
//!
//! The repository owns one closed rebuildable family. Runtime attempts,
//! scheduler state, partition freshness, recovery and provider transactions do
//! not cross this boundary.

use std::fmt;

use novarocks_state_store_api::VersionToken;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::persistence::definition::{MvAcceleratorSourceRevision, MvDesiredRefreshPolicy};
pub use crate::persistence::dependency::CreateMvDependencyRequest;
use crate::persistence::dependency::StoredMvDependency;
use crate::persistence::projection::{MvDocumentProjection, StoredMvProjection};
use crate::persistence::validation::PersistenceDecodeBudget;
use crate::product::MvTarget;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvRepositoryErrorKind {
    InvalidRequest,
    NotFound,
    Conflict,
    Corruption,
    Unavailable,
    CommitUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRepositoryError {
    kind: MvRepositoryErrorKind,
    message: String,
}

impl MvRepositoryError {
    pub fn new(kind: MvRepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> MvRepositoryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvRepositoryError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetLookup {
    pub mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialMvRefreshConfiguration {
    pub policy: MvDesiredRefreshPolicy,
    pub paused: bool,
    pub interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
}

impl Default for InitialMvRefreshConfiguration {
    fn default() -> Self {
        Self {
            policy: MvDesiredRefreshPolicy::Manual,
            paused: false,
            interval_ms: None,
            max_staleness_ms: None,
        }
    }
}

/// A validated complete document root, replaced with its derived indexes in one CAS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvProjectionRequest {
    pub facts: MvDocumentProjection,
}

impl From<MvDocumentProjection> for MvProjectionRequest {
    fn from(facts: MvDocumentProjection) -> Self {
        Self { facts }
    }
}

/// Opaque StateStore version returned only by a successful repository read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProjectionVersion(VersionToken);

impl MvProjectionVersion {
    pub(crate) fn from_store(version: VersionToken) -> Self {
        Self(version)
    }

    pub(crate) fn store_version(&self) -> &VersionToken {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedMvProjection {
    pub projection: StoredMvProjection,
    pub version: MvProjectionVersion,
}

/// One explicit inventory promise. It is supplied by the local consumer;
/// planning and maintenance reads retain their separate repository contract.
#[derive(Clone, Copy, Debug)]
pub struct MvProjectionInventoryBound {
    pub entries: usize,
    pub snapshot_bytes: usize,
    pub raw_page_bytes: usize,
    pub single_name_bytes: usize,
    pub continuation_token_bytes: usize,
    pub decode: PersistenceDecodeBudget,
}

/// Local dependency display reads share one raw/decode page promise with a
/// complete thin classification inventory and a separate finite collector.
#[derive(Clone, Copy, Debug)]
pub struct MvDependencyReadBound {
    pub inventory: MvProjectionInventoryBound,
    pub entries: usize,
    pub collection_bytes: usize,
}

/// Thin identities from one complete StateStore snapshot. These are inventory
/// locators, never authority to consume an old projection or manage its target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProjectionInventoryEntry {
    pub mv_id: i64,
    pub target: MvTarget,
    pub object_id: novarocks_spi::connector::ConnectorTableObjectId,
}

pub(crate) struct MvProjectionInventoryBuilder {
    entries: Vec<MvProjectionInventoryEntry>,
    payload_bytes: usize,
    bound: MvProjectionInventoryBound,
}

impl MvProjectionInventoryBuilder {
    pub(crate) fn new(bound: MvProjectionInventoryBound) -> Result<Self, MvRepositoryError> {
        if bound.entries == 0
            || bound.snapshot_bytes == 0
            || bound.raw_page_bytes == 0
            || bound.single_name_bytes == 0
            || bound.continuation_token_bytes == 0
            || bound.decode.max_working_set_bytes == 0
            || bound.decode.max_document_bytes == 0
        {
            return Err(Self::refusal(
                "MV inventory requires nonzero explicit bounds",
            ));
        }
        Ok(Self {
            entries: Vec::new(),
            payload_bytes: 0,
            bound,
        })
    }

    fn refusal(message: &'static str) -> MvRepositoryError {
        MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, message)
    }

    pub(crate) fn push(
        &mut self,
        projection: &StoredMvProjection,
    ) -> Result<(), MvRepositoryError> {
        if self.entries.len() >= self.bound.entries {
            return Err(Self::refusal("MV inventory exceeds its entry bound"));
        }
        let target = projection.facts.target();
        let object = &projection.facts.source_revision().target_object_id;
        if [
            target.catalog().unwrap_or_default(),
            target.namespace(),
            target.name(),
        ]
        .into_iter()
        .any(|name| name.len() > self.bound.single_name_bytes)
        {
            return Err(Self::refusal(
                "MV inventory exceeds its single name byte bound",
            ));
        }
        let payload = [
            target.catalog().map_or(0, str::len),
            target.namespace().len(),
            target.name().len(),
            object.as_bytes().len(),
            4 * 64,
        ]
        .into_iter()
        .try_fold(self.payload_bytes, usize::checked_add)
        .ok_or_else(|| Self::refusal("MV inventory snapshot size overflows"))?;
        let capacity = if self.entries.len() == self.entries.capacity() {
            self.entries
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(self.bound.entries)
        } else {
            self.entries.capacity()
        };
        // During growth both old and new vector allocations may coexist.
        let vector_slots = if capacity != self.entries.capacity() {
            capacity.checked_add(self.entries.capacity())
        } else {
            Some(capacity)
        };
        let peak = vector_slots
            .and_then(|n| n.checked_mul(std::mem::size_of::<MvProjectionInventoryEntry>()))
            .and_then(|n| n.checked_add(payload))
            .ok_or_else(|| Self::refusal("MV inventory snapshot size overflows"))?;
        if peak > self.bound.snapshot_bytes {
            return Err(Self::refusal(
                "MV inventory exceeds its snapshot byte bound",
            ));
        }
        if capacity != self.entries.capacity() {
            self.entries
                .try_reserve_exact(capacity - self.entries.len())
                .map_err(|_| Self::refusal("MV inventory allocation failed"))?;
        }
        // Copy only the opaque identity bytes. A small locator cannot retain
        // a larger decoder backing through a Bytes alias.
        let object_id = novarocks_spi::connector::ConnectorTableObjectId::try_new(
            bytes::Bytes::copy_from_slice(object.as_bytes()),
        )
        .map_err(|error| {
            MvRepositoryError::new(MvRepositoryErrorKind::Corruption, error.to_string())
        })?;
        self.entries.push(MvProjectionInventoryEntry {
            mv_id: projection.mv_id,
            target: target.clone(),
            object_id,
        });
        self.payload_bytes = payload;
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<MvProjectionInventoryEntry> {
        self.entries
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceMvProjectionRequest {
    pub mv_id: i64,
    pub expected_version: MvProjectionVersion,
    pub projection: MvProjectionRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteMvProjectionRequest {
    pub mv_id: i64,
    pub expected_version: MvProjectionVersion,
    pub expected_source_revision: MvAcceleratorSourceRevision,
}

/// Asynchronous application port. Durable state is reached through an async
/// StateStore, and this boundary says so rather than hiding a blocking bridge
/// behind a synchronous signature. No raw key or transaction crosses it.
///
/// The port is async because the store is. The previous synchronous contract
/// forced every implementation to block a worker thread, and made the whole
/// port unusable on a current-thread runtime — which is what test
/// compositions actually run on.
#[async_trait::async_trait]
pub trait MvRepository: Send + Sync {
    async fn create_projection(
        &self,
        operation_id: Uuid,
        projection: MvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError>;

    async fn replace_projection(
        &self,
        operation_id: Uuid,
        request: ReplaceMvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError>;

    async fn load_by_id(&self, mv_id: i64)
    -> Result<Option<LoadedMvProjection>, MvRepositoryError>;

    async fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError>;

    async fn list_projections(&self) -> Result<Vec<LoadedMvProjection>, MvRepositoryError>;

    /// Complete bounded inventory; implementations must page directly rather
    /// than calling the full-model list and checking its length afterwards.
    async fn list_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError>;

    /// Fresh target lookup with an explicit decode promise, checked before
    /// owned projection materialization. No inventory version is a CAS token.
    async fn find_by_target_bounded(
        &self,
        target: &MvTarget,
        bound: MvProjectionInventoryBound,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError>;

    /// The projection of one provider target object, whichever catalog
    /// attachment it was discovered through.
    ///
    /// A materialized view is the object it publishes into. The by-target
    /// index is keyed by the attachment's own name, so it cannot answer this:
    /// one object reachable through two attachments has two names and one
    /// projection. Scanning is deliberate -- this is asked once per
    /// installation, and a second durable index would have to be kept exact
    /// against the one that already exists.
    async fn find_by_target_object(
        &self,
        object_id: &novarocks_spi::connector::ConnectorTableObjectId,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        Ok(self.list_projections().await?.into_iter().find(|loaded| {
            &loaded.projection.facts.source_revision().target_object_id == object_id
        }))
    }

    async fn delete_projection(
        &self,
        operation_id: Uuid,
        request: DeleteMvProjectionRequest,
    ) -> Result<bool, MvRepositoryError>;

    /// Test/harness-only destructive wipe of one rebuildable projection.
    /// It deliberately has no source-equivalence semantics.
    async fn wipe_projection_by_target(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError>;

    /// Test/harness-only wipe of the complete current Accelerator family,
    /// including the internal sequence. Old physical families remain untouched.
    async fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvRepositoryError>;

    async fn list_dependencies_by_downstream(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError>;

    /// Exact downstream version and canonical occurrences, classified only
    /// against one complete bounded inventory in the same read snapshot.
    async fn list_dependencies_by_downstream_bounded(
        &self,
        mv_id: i64,
        expected_version: &MvProjectionVersion,
        bound: MvDependencyReadBound,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError>;

    async fn list_downstream_dependencies(
        &self,
        upstream: &crate::dependency::MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError>;

    async fn ensure_no_downstream_dependencies(
        &self,
        upstream: &crate::dependency::MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError>;
}
