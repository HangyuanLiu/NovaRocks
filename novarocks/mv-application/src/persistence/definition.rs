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

use crate::management::{DeploymentOwner, ManagementDependencySet, ProcessIncarnation};
use crate::persistence::identity::DocumentRevision;
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorControlRuntimeId, ConnectorTableIdentity,
    ConnectorTableObjectId,
};
use serde::{Deserialize, Serialize};

use novarocks_query_application::persisted_query_definition::PersistedQueryDefinition;

pub(crate) const MV_ACCELERATOR_PROJECTION_SUBJECT: &str = "mv.accelerator_projection";

/// Exact provider version identity retained by the Accelerator.
///
/// The provider payload itself is not application state. Its validated digest
/// plus the provider's structured snapshot fact is sufficient to compare the
/// exact version across a StateStore round trip without teaching the
/// Accelerator how to parse the payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvAcceleratorCommittedVersionRevision {
    digest: [u8; 32],
    snapshot_id: Option<i64>,
}

impl MvAcceleratorCommittedVersionRevision {
    pub(crate) fn from_committed(version: &ConnectorCommittedVersion) -> Self {
        Self {
            digest: version.digest(),
            snapshot_id: version.snapshot_id(),
        }
    }

    pub(crate) fn try_from_parts(
        digest: [u8; 32],
        snapshot_id: Option<i64>,
    ) -> Result<Self, String> {
        if snapshot_id.is_some_and(|value| value <= 0) {
            return Err("MV Accelerator committed snapshot ID must be positive".to_string());
        }
        Ok(Self {
            digest,
            snapshot_id,
        })
    }

    pub(crate) const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub(crate) const fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }
}

/// Complete lake source revision from which one Accelerator projection was
/// derived.
///
/// Logical and physical target identity, provider metadata/output versions,
/// every D/L/P/C document revision, and management ownership remain distinct.
/// Comparing only Current, one snapshot, or one document digest would allow a
/// stale projector to suppress a required replacement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvAcceleratorSourceRevision {
    pub target: ConnectorTableIdentity,
    pub target_object_id: ConnectorTableObjectId,
    pub metadata_version: MvAcceleratorCommittedVersionRevision,
    pub definition_revision: DocumentRevision,
    pub interpretation_revision: DocumentRevision,
    pub publication_revision: Option<DocumentRevision>,
    pub publication_output_version: Option<MvAcceleratorCommittedVersionRevision>,
    pub configuration_revision: DocumentRevision,
    pub deployment_owner: DeploymentOwner,
    pub process_incarnation: ProcessIncarnation,
}

impl MvAcceleratorSourceRevision {
    /// The exact immutable dependencies guarded by the single management
    /// entrance. C remains an independent target mutation, but is still part
    /// of this complete projection source revision.
    pub(crate) fn management_dependencies(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> ManagementDependencySet {
        ManagementDependencySet::new(
            *self.definition_revision.as_bytes(),
            *self.interpretation_revision.as_bytes(),
            self.publication_revision
                .as_ref()
                .map(|revision| *revision.as_bytes()),
            control_runtime_id,
        )
    }
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn test_source_revision(
    target_catalog: &str,
    target_namespace: &str,
    target_table: &str,
    target_object_id: ConnectorTableObjectId,
    published_snapshot_id: Option<i64>,
) -> MvAcceleratorSourceRevision {
    use sha2::{Digest, Sha256};
    use std::sync::Arc;

    fn version(seed: &[u8], snapshot_id: Option<i64>) -> MvAcceleratorCommittedVersionRevision {
        MvAcceleratorCommittedVersionRevision::try_from_parts(
            Sha256::digest(seed).into(),
            snapshot_id,
        )
        .expect("test committed version revision")
    }

    MvAcceleratorSourceRevision {
        target: ConnectorTableIdentity {
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(target_catalog)
                .expect("test target catalog"),
            namespace: Arc::from(target_namespace),
            table: Arc::from(target_table),
        },
        target_object_id,
        metadata_version: version(b"metadata", published_snapshot_id),
        definition_revision: DocumentRevision::from_canonical_bytes(b"definition"),
        interpretation_revision: DocumentRevision::from_canonical_bytes(b"interpretation"),
        publication_revision: published_snapshot_id
            .map(|_| DocumentRevision::from_canonical_bytes(b"publication")),
        publication_output_version: published_snapshot_id
            .map(|snapshot_id| version(b"publication-output", Some(snapshot_id))),
        configuration_revision: DocumentRevision::from_canonical_bytes(b"configuration"),
        deployment_owner: DeploymentOwner::parse("test-deployment").expect("test owner"),
        process_incarnation: ProcessIncarnation::parse("test-process").expect("test incarnation"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvDefinitionRequest {
    pub query_definition: PersistedQueryDefinition,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvDesiredRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval,
}

impl MvDesiredRefreshPolicy {
    pub fn as_sql_str(&self) -> &'static str {
        match self {
            Self::Manual => "DEFERRED_MANUAL",
            Self::AsyncOnChange => "ASYNC_ON_CHANGE",
            Self::AsyncInterval => "ASYNC_INTERVAL",
        }
    }

    pub(crate) fn accepts_interval(&self) -> bool {
        matches!(self, Self::AsyncInterval)
    }
}
