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

//! Catalog command, admission, and error contracts.

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

/// The exact identity that query/runtime consumers may admit.
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
    pub fn require_ready(
        self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        match self {
            Self::Ready(observation) => Ok(observation),
            Self::Absent => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::NotFound,
                format!("unknown catalog `{}`", instance_id.as_str()),
            )),
            Self::Unavailable { reason } => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                format!(
                    "catalog `{}` is unavailable on this frontend: {reason}",
                    instance_id.as_str()
                ),
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
    UnsupportedSourceMode,
    DesiredStateEnumerationIncomplete,
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

/// The Catalog application's command and admission dependency.
pub trait CatalogApplicationPort: Send + Sync {
    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError>;

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError>;

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission;
}

/// The local runtime publication sink supplied by the query-owning application.
///
/// Catalog owns which runtime observation is valid; a consumer owns how that
/// observation becomes query-local resolution state.
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
