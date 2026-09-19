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

use std::fmt;
use std::sync::Arc;

use novarocks_spi::connector::{
    CatalogHandle, ConnectorDocumentManagementObservation, ConnectorTableIdentity,
    ConnectorTableObjectId,
};

use super::EffectIdentity;

const MAX_IDENTITY_BYTES: usize = 128;
const MANAGED_MV_KIND: &str = "materialized-view";

/// Stable management owner derived from `[native_trust].deployment_id`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeploymentOwner(Arc<str>);

impl DeploymentOwner {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ManagementOwnershipError> {
        validate_identity(value.as_ref(), "deployment owner")?;
        Ok(Self(Arc::from(value.as_ref())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Diagnostic process identity. It is deliberately not a lease or fence.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProcessIncarnation(Arc<str>);

impl ProcessIncarnation {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ManagementOwnershipError> {
        validate_identity(value.as_ref(), "process incarnation")?;
        Ok(Self(Arc::from(value.as_ref())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact logical and physical target used by management policy.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ManagedMvTarget {
    catalog: CatalogHandle,
    table: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
}

impl ManagedMvTarget {
    pub fn try_new(
        catalog: CatalogHandle,
        table: ConnectorTableIdentity,
        object_id: ConnectorTableObjectId,
    ) -> Result<Self, ManagementOwnershipError> {
        if catalog.catalog_name() != &table.instance_id {
            return Err(ManagementOwnershipError::TargetCatalogMismatch);
        }
        Ok(Self {
            catalog,
            table,
            object_id,
        })
    }

    pub fn from_observation(
        observation: &ConnectorDocumentManagementObservation,
    ) -> Result<Self, ManagementOwnershipError> {
        Self::try_new(
            observation.catalog_handle().clone(),
            observation.target().clone(),
            observation.object_id().clone(),
        )
    }

    pub const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }

    pub(crate) fn validate_observation(
        &self,
        observation: &ConnectorDocumentManagementObservation,
    ) -> Result<(), ManagementOwnershipError> {
        if observation.catalog_handle() != &self.catalog
            || observation.target() != &self.table
            || observation.object_id() != &self.object_id
        {
            return Err(ManagementOwnershipError::TargetReplaced);
        }
        if observation.marker().kind() != MANAGED_MV_KIND {
            return Err(ManagementOwnershipError::WrongObjectKind);
        }
        Ok(())
    }
}

/// The responsibility identity minted before a provider can return the
/// physical object identity for a staged MV CREATE.  Absence is intentional:
/// this is an exact catalog and logical-table assertion, not a synthetic UUID
/// for an object the provider has not created yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateIntent {
    catalog: CatalogHandle,
    table: ConnectorTableIdentity,
    operation_id: EffectIdentity,
}

impl CreateIntent {
    pub fn try_new(
        catalog: CatalogHandle,
        table: ConnectorTableIdentity,
        operation_id: EffectIdentity,
    ) -> Result<Self, ManagementOwnershipError> {
        if catalog.catalog_name() != &table.instance_id {
            return Err(ManagementOwnershipError::TargetCatalogMismatch);
        }
        Ok(Self {
            catalog,
            table,
            operation_id,
        })
    }

    pub const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    /// A CREATE intent always asserts that the logical target was absent when
    /// it entered the management FIFO.
    pub const fn expects_absent(&self) -> bool {
        true
    }

    pub const fn operation_id(&self) -> EffectIdentity {
        self.operation_id
    }

    pub fn bind_target(
        &self,
        target: ManagedMvTarget,
    ) -> Result<ManagedMvTarget, ManagementOwnershipError> {
        if target.catalog() != &self.catalog || target.table() != &self.table {
            return Err(ManagementOwnershipError::TargetReplaced);
        }
        Ok(target)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementOwnershipError {
    EmptyIdentity,
    InvalidIdentity,
    TargetCatalogMismatch,
    TargetReplaced,
    WrongObjectKind,
    ForeignOwner,
    IncarnationMismatch,
}

impl fmt::Display for ManagementOwnershipError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyIdentity => "management identity must not be empty",
            Self::InvalidIdentity => "management identity is invalid or exceeds its limit",
            Self::TargetCatalogMismatch => "MV target does not belong to the exact catalog handle",
            Self::TargetReplaced => "MV management observation denotes another target generation",
            Self::WrongObjectKind => "management observation is not a materialized view",
            Self::ForeignOwner => "materialized view belongs to another deployment",
            Self::IncarnationMismatch => "a fresh management observation found another incarnation",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ManagementOwnershipError {}

fn validate_identity(value: &str, _subject: &str) -> Result<(), ManagementOwnershipError> {
    if value.is_empty() {
        return Err(ManagementOwnershipError::EmptyIdentity);
    }
    if value.len() > MAX_IDENTITY_BYTES
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(ManagementOwnershipError::InvalidIdentity);
    }
    Ok(())
}
