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
        no_op_if_exists: bool,
    },
    DropNamespace {
        namespace: CatalogNamespaceName,
        no_op_if_missing: bool,
    },
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
    namespace_no_op: Option<bool>,
    pub(super) namespace_has_properties: bool,
    request: Vec<u8>,
    attribution: AttributionPlan,
}
impl FrozenCatalogPublication {
    pub(crate) fn targets(&self) -> Vec<String> {
        // Bind the actual operation, structured name and local policy even
        // when two endpoints use identical empty HTTP bodies. Display names
        // are ambiguous (namespace "db.t" versus table {db, t}).
        let target = match &self.target {
            CatalogAdmissionTarget::Namespace(namespace) => serde_json::json!({
                "kind": "namespace", "namespace": namespace.namespace.as_ref(),
            }),
            CatalogAdmissionTarget::Table(table) => serde_json::json!({
                "kind": "table", "namespace": table.namespace.as_ref(), "name": table.name.as_ref(),
            }),
        };
        vec![
            serde_json::json!({
                "catalog-request-scope-version": 1,
                "operation": self.operation.name(), "target": target,
                "namespace-no-op": self.namespace_no_op,
            })
            .to_string(),
        ]
    }
    pub(crate) fn request_bytes(&self) -> &[u8] {
        &self.request
    }
    pub(crate) fn attribution(&self) -> &AttributionPlan {
        &self.attribution
    }
    pub(super) fn namespace_no_op(
        &self,
        exists: bool,
    ) -> super::publication_dispatch::PreparationResult<bool> {
        use novarocks_spi::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
        let satisfied = match self.operation {
            CatalogOperation::CreateNamespace => exists,
            CatalogOperation::DropNamespace => !exists,
            _ => {
                return Err(super::publication_dispatch::PreparationFailure(
                    ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::InvalidRequest,
                        "Namespace precondition requires an actual namespace operation",
                    ),
                ));
            }
        };
        if !satisfied {
            return Ok(false);
        }
        if self.namespace_no_op == Some(true) {
            return Ok(true);
        }
        Err(super::publication_dispatch::PreparationFailure(
            ConnectorMutationFailure::new(
                if exists {
                    ConnectorMutationFailureKind::AlreadyExists
                } else {
                    ConnectorMutationFailureKind::NotFound
                },
                if exists {
                    "Iceberg namespace already exists"
                } else {
                    "Iceberg namespace does not exist"
                },
            ),
        ))
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
        let mut namespace_no_op = None;
        let mut namespace_has_properties = false;
        let (operation, target, route, expectation, request) = match self {
            Self::CreateNamespace {
                namespace,
                properties,
                no_op_if_exists,
            } => {
                namespace_no_op = Some(no_op_if_exists);
                namespace_has_properties = !properties.is_empty();
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
            Self::DropNamespace {
                namespace,
                no_op_if_missing,
            } => {
                namespace_no_op = Some(no_op_if_missing);
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
            namespace_no_op,
            namespace_has_properties,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_binds_actual_endpoint_structured_target_and_local_policy() {
        let id = [9; 16];
        let table = CatalogTableName::new("db", "t");
        let source = CatalogPublicationRequest::DropTable(table.clone())
            .freeze(id)
            .unwrap();
        let descriptor = PublicationDescriptor::catalog(
            id,
            source.targets(),
            source.request_bytes().to_vec(),
            source.attribution().clone(),
        )
        .unwrap();
        source.validate_descriptor(&descriptor).unwrap();
        for other in [
            CatalogPublicationRequest::DropView(table),
            CatalogPublicationRequest::DropNamespace {
                namespace: CatalogNamespaceName::new("db.t"),
                no_op_if_missing: false,
            },
            CatalogPublicationRequest::DropTable(CatalogTableName::new("db.t", "")),
        ] {
            let other = other.freeze(id);
            if let Ok(other) = other {
                assert!(other.validate_descriptor(&descriptor).is_err());
            }
        }
        let strict = CatalogPublicationRequest::DropNamespace {
            namespace: CatalogNamespaceName::new("db"),
            no_op_if_missing: false,
        }
        .freeze(id)
        .unwrap();
        let descriptor = PublicationDescriptor::catalog(
            id,
            strict.targets(),
            strict.request_bytes().to_vec(),
            strict.attribution().clone(),
        )
        .unwrap();
        let conditional = CatalogPublicationRequest::DropNamespace {
            namespace: CatalogNamespaceName::new("db"),
            no_op_if_missing: true,
        }
        .freeze(id)
        .unwrap();
        assert!(conditional.validate_descriptor(&descriptor).is_err());
    }
}
