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

//! Consumer-owned catalog control and admission contracts.
//!
//! Core consumes these facts but never owns the durable attachment record,
//! provider factory, or a provider-concrete catalog handle. Frontend owns
//! those control-plane concerns and projects Ready observations into this
//! boundary.

use std::fmt;

use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogCreateCommand {
    pub instance_id: ConnectorInstanceId,
    pub display_name: String,
    pub properties: Vec<(String, String)>,
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogDropCommand {
    pub instance_id: ConnectorInstanceId,
    pub if_exists: bool,
}

/// The exact identity that Core may admit into a query/runtime path.
///
/// `attachment_id` distinguishes a catalog recreated under the same SQL
/// name; `generation` distinguishes locally retired and republished runtime
/// projections of that durable attachment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRuntimeObservation {
    pub attachment_id: Uuid,
    pub instance_id: ConnectorInstanceId,
    pub provider_id: ConnectorProviderId,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogAdmission {
    Absent,
    Unavailable { reason: String },
    Ready(CatalogRuntimeObservation),
}

impl CatalogAdmission {
    pub fn require_ready(self) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        match self {
            Self::Ready(observation) => Ok(observation),
            Self::Absent => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::NotFound,
                "catalog attachment was not found",
            )),
            Self::Unavailable { reason } => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                reason,
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogApplicationErrorKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogApplicationError {
    kind: CatalogApplicationErrorKind,
    message: String,
}

impl CatalogApplicationError {
    pub fn new(kind: CatalogApplicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> CatalogApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for CatalogApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CatalogApplicationError {}

/// Core's catalog command and admission dependency.
///
/// Implemented by Frontend. Core must not downcast this port to access an
/// attachment repository, control host, registry, or provider handle.
pub trait CatalogApplicationPort: Send + Sync {
    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError>;

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError>;

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission;
}

/// The provider-neutral sink Frontend uses to project a Ready catalog runtime
/// into Core. It deliberately exposes only exact observations and retirement,
/// never a concrete registry or provider handle.
pub trait CatalogRuntimePublisherSink: Send + Sync {
    fn publish_catalog_runtime(
        &self,
        observation: CatalogRuntimeObservation,
    ) -> Result<(), CatalogApplicationError>;

    fn unpublish_catalog_runtime(
        &self,
        instance_id: &ConnectorInstanceId,
        generation: u64,
    ) -> Result<(), CatalogApplicationError>;
}

/// Process-local health facts for the Frontend-owned catalog projection.
///
/// The durable attachment remains in StateStore; these fields only describe
/// the local controller that projects it into a runtime generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogProjectionMetricsSnapshot {
    pub projected_catalogs: usize,
    pub successful_polls: u64,
    pub failed_polls: u64,
    pub resyncs: u64,
    pub freshness_expiries: u64,
}

/// Publishes Frontend-owned projection health to the process metrics endpoint.
pub fn publish_catalog_projection_metrics(snapshot: CatalogProjectionMetricsSnapshot) {
    crate::service::metrics_http::publish_catalog_projection_metrics(snapshot);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation() -> CatalogRuntimeObservation {
        CatalogRuntimeObservation {
            attachment_id: Uuid::now_v7(),
            instance_id: ConnectorInstanceId::parse("warehouse").expect("instance"),
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            generation: 7,
        }
    }

    #[test]
    fn admission_preserves_not_found_and_unavailable_as_distinct_outcomes() {
        assert_eq!(
            CatalogAdmission::Absent
                .require_ready()
                .expect_err("absent catalog")
                .kind(),
            CatalogApplicationErrorKind::NotFound
        );
        assert_eq!(
            CatalogAdmission::Unavailable {
                reason: "projection is stale".to_string(),
            }
            .require_ready()
            .expect_err("unavailable catalog")
            .kind(),
            CatalogApplicationErrorKind::Unavailable
        );
        assert_eq!(
            CatalogAdmission::Ready(observation())
                .require_ready()
                .expect("ready catalog")
                .generation,
            7
        );
    }
}
