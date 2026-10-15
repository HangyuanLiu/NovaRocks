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

//! Exact artifactless requests. Freezing is pure; session and authentication
//! preparation run only after the publication journal has claimed the entry.

use super::admission::{CatalogAdmissionTarget, CatalogOperation};
use super::{CatalogCreateIntent, CatalogNamespaceName, CatalogTableName};
use crate::commit::publication_descriptor::{AttributionPlan, PublicationDescriptor};
use crate::iceberg::{Error, ErrorKind, Result, TableCreation};
use crate::rest_protocol::evidence::AttributionExpectation;
use crate::rest_protocol::mutation::prepared::MutationRoute;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) enum CatalogPublicationRequest {
    CreateNamespace {
        namespace: CatalogNamespaceName,
        properties: HashMap<String, String>,
    },
    DropNamespace(CatalogNamespaceName),
    CreateTable {
        namespace: CatalogNamespaceName,
        creation: TableCreation,
    },
    StageCreate {
        namespace: CatalogNamespaceName,
        creation: TableCreation,
    },
    DropTable(CatalogTableName),
    CreateView {
        namespace: CatalogNamespaceName,
        creation: super::view_request::ViewCreate,
    },
    ReplaceView(super::view_request::ValidatedViewReplace),
    DropView(CatalogTableName),
}

/// Response data only. The journal remains the sole owner of the conclusion.
#[derive(Clone, Default)]
pub(crate) struct CatalogResponseDocument(Arc<Mutex<Option<Vec<u8>>>>);
impl CatalogResponseDocument {
    pub(crate) fn retain(&self, document: Option<Vec<u8>>) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = document;
    }
    pub(crate) fn take(&self) -> Option<Vec<u8>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

pub(crate) struct FrozenCatalogPublication {
    operation_id: [u8; 16],
    pub(super) operation: CatalogOperation,
    pub(super) target: CatalogAdmissionTarget,
    pub(super) route: MutationRoute,
    pub(super) expectation: AttributionExpectation,
    pub(super) stage_create: bool,
    request: Vec<u8>,
    attribution: AttributionPlan,
}
impl FrozenCatalogPublication {
    pub(crate) fn targets(&self) -> Vec<String> {
        vec![match &self.target {
            CatalogAdmissionTarget::Namespace(namespace) => namespace.to_string(),
            CatalogAdmissionTarget::Table(table) => table.to_string(),
        }]
    }
    pub(crate) fn request_bytes(&self) -> &[u8] {
        &self.request
    }
    pub(crate) fn attribution(&self) -> &AttributionPlan {
        &self.attribution
    }
    pub(crate) fn validate_descriptor(&self, descriptor: &PublicationDescriptor) -> Result<()> {
        descriptor.validate_catalog(
            self.operation_id,
            self.targets(),
            self.request.clone(),
            self.attribution.clone(),
        )
    }
}

impl CatalogPublicationRequest {
    pub(crate) fn freeze(self, operation_id: [u8; 16]) -> Result<FrozenCatalogPublication> {
        use crate::rest_protocol::mutation::{CommitViewRequest, CreateViewRequest};
        use crate::rest_protocol::wire::{CreateNamespaceRequest, CreateTableRequest};
        let mut attribution = AttributionPlan::ObserveOnly;
        let mut stage_create = false;
        let (operation, target, route, expectation, request) = match self {
            Self::CreateNamespace {
                namespace,
                properties,
            } => {
                let ident = super::delegate::namespace_ident(&namespace).map_err(invalid_target)?;
                let body = CreateNamespaceRequest {
                    namespace: ident.clone(),
                    properties,
                };
                (
                    CatalogOperation::CreateNamespace,
                    namespace.into(),
                    MutationRoute::CreateNamespace,
                    AttributionExpectation::create_namespace(ident.as_ref().clone()),
                    encode(&body)?,
                )
            }
            Self::DropNamespace(namespace) => {
                let ident = super::delegate::namespace_ident(&namespace).map_err(invalid_target)?;
                (
                    CatalogOperation::DropNamespace,
                    namespace.into(),
                    MutationRoute::DropNamespace(ident),
                    AttributionExpectation::drop_namespace(),
                    Vec::new(),
                )
            }
            request @ (Self::CreateTable { .. } | Self::StageCreate { .. }) => {
                let (namespace, creation) = match request {
                    Self::CreateTable {
                        namespace,
                        creation,
                    } => (namespace, creation),
                    Self::StageCreate {
                        namespace,
                        creation,
                    } => {
                        stage_create = true;
                        (namespace, creation)
                    }
                    _ => unreachable!(),
                };
                let ident = super::delegate::namespace_ident(&namespace).map_err(invalid_target)?;
                let mut creation = creation;
                creation
                    .properties
                    .entry("format-version".into())
                    .or_insert_with(|| (creation.format_version as u8).to_string());
                let target = CatalogTableName::new(namespace.namespace, creation.name.clone());
                let body = CreateTableRequest {
                    name: creation.name,
                    location: creation.location,
                    schema: creation.schema,
                    partition_spec: creation.partition_spec,
                    write_order: creation.sort_order,
                    stage_create: stage_create.then_some(true),
                    properties: creation.properties,
                };
                (
                    CatalogOperation::CreateTable(if stage_create {
                        CatalogCreateIntent::CreateTableAsSelect
                    } else {
                        CatalogCreateIntent::EmptyTable
                    }),
                    target.into(),
                    MutationRoute::CreateTable(ident),
                    if stage_create {
                        AttributionExpectation::stage_create()
                    } else {
                        AttributionExpectation::create_table()
                    },
                    encode(&body)?,
                )
            }
            Self::DropTable(table) => {
                let ident = super::delegate::table_ident(&table).map_err(invalid_target)?;
                (
                    CatalogOperation::DropTable,
                    table.into(),
                    MutationRoute::DropTable(ident),
                    AttributionExpectation::drop_table(),
                    Vec::new(),
                )
            }
            Self::CreateView {
                namespace,
                creation,
            } => {
                let ident = super::delegate::namespace_ident(&namespace).map_err(invalid_target)?;
                let target = CatalogTableName::new(namespace.namespace, creation.name());
                (
                    CatalogOperation::CreateView,
                    target.into(),
                    MutationRoute::CreateView(ident),
                    AttributionExpectation::create_view(),
                    encode(&CreateViewRequest::encode(creation))?,
                )
            }
            Self::ReplaceView(replace) => {
                let target = CatalogTableName::from_identifier(replace.ident());
                let marker = replace
                    .version()
                    .summary
                    .get(super::view_request::OPERATION_ID_SUMMARY_KEY)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "View replacement has no frozen operation marker",
                        )
                    })?;
                attribution = AttributionPlan::ViewOperation {
                    view_uuid: Some(replace.uuid()),
                    operation_id: marker.clone(),
                };
                (
                    CatalogOperation::ReplaceView,
                    target.into(),
                    MutationRoute::ReplaceView(replace.ident().clone()),
                    AttributionExpectation::replace_view(replace.uuid()),
                    encode(&CommitViewRequest::encode(&replace))?,
                )
            }
            Self::DropView(view) => {
                let ident = super::delegate::table_ident(&view).map_err(invalid_target)?;
                (
                    CatalogOperation::DropView,
                    view.into(),
                    MutationRoute::DropView(ident),
                    AttributionExpectation::drop_view(),
                    Vec::new(),
                )
            }
        };
        operation
            .validate_target(&target)
            .map_err(|error| Error::new(ErrorKind::DataInvalid, error.message()))?;
        Ok(FrozenCatalogPublication {
            operation_id,
            operation,
            target,
            route,
            expectation,
            stage_create,
            request,
            attribution,
        })
    }
}

fn encode<T: serde::Serialize>(body: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(body).map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            "Cannot freeze catalog publication request",
        )
        .with_source(error)
    })
}

fn invalid_target(error: novarocks_spi::connector::ConnectorError) -> Error {
    Error::new(ErrorKind::DataInvalid, "Invalid catalog publication target").with_source(error)
}
