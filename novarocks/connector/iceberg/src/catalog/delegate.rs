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

//! Shared delegation onto one concrete Iceberg catalog client.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)
//!
//! Every concrete implementation owns an `Arc<dyn iceberg::Catalog>`, and for
//! reads and single-frontier mutations they behave identically, so that
//! behavior lives here once rather than three times.
//!
//! # Why views need no per-catalog code
//!
//! The vendored `Catalog` trait gives its view methods default bodies that
//! return `ErrorKind::FeatureUnsupported`. The REST client overrides all of
//! them; the Hive and Hadoop clients override none. Plain delegation therefore
//! already produces a real answer on REST and a typed `Unsupported` elsewhere.
//!
//! The behavior this replaces did the opposite on purpose: it checked for a
//! REST client and answered `false` / empty for the others, converting "this
//! catalog cannot answer" into "this catalog says no". Removing that
//! short-circuit *is* the fix — there is nothing to reimplement.

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailureKind, ExternalMutationEffect,
};
use novarocks_types::naming::normalize_identifier;

use crate::iceberg::{Catalog, NamespaceIdent, TableIdent};

use super::error::{CatalogCommitEvidence, CatalogOutcome, map_read_error, proves_uncommitted};
use super::{
    CatalogDropTableReceipt, CatalogNamespaceName, CatalogTableName,
    error::uncommitted_failure_kind,
};

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

pub(super) fn namespace_ident(
    name: &CatalogNamespaceName,
) -> Result<NamespaceIdent, ConnectorError> {
    let normalized = normalize_identifier(&name.namespace).map_err(invalid)?;
    Ok(NamespaceIdent::new(normalized))
}

pub(super) fn table_ident(name: &CatalogTableName) -> Result<TableIdent, ConnectorError> {
    let namespace = normalize_identifier(&name.namespace).map_err(invalid)?;
    let table = normalize_identifier(&name.name).map_err(invalid)?;
    TableIdent::from_strs([namespace.as_str(), table.as_str()])
        .map_err(|error| invalid(format!("build Iceberg identity for {name}: {error}")))
}

/// Delegation onto one concrete catalog client.
#[derive(Debug)]
pub(super) struct CatalogDelegate {
    client: Arc<dyn Catalog>,
}

impl CatalogDelegate {
    pub(super) fn new(client: Arc<dyn Catalog>) -> Self {
        Self { client }
    }

    pub(super) fn client(&self) -> &Arc<dyn Catalog> {
        &self.client
    }

    // ---- Reads ----------------------------------------------------------

    pub(super) async fn list_namespaces(&self) -> Result<Vec<String>, ConnectorError> {
        let namespaces = self
            .client
            .list_namespaces(None)
            .await
            .map_err(|error| map_read_error(&error))?;
        let mut names = namespaces
            .into_iter()
            .flat_map(|ident| ident.inner())
            // A leading dot marks catalog-internal bookkeeping namespaces,
            // including the CTAS staging root, which SQL must never see.
            .filter(|name| !name.starts_with('.'))
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub(super) async fn namespace_exists(
        &self,
        namespace: &CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        let ident = namespace_ident(namespace)?;
        self.client
            .namespace_exists(&ident)
            .await
            .map_err(|error| map_read_error(&error))
    }

    pub(super) async fn list_tables(
        &self,
        namespace: &CatalogNamespaceName,
    ) -> Result<Vec<String>, ConnectorError> {
        let ident = namespace_ident(namespace)?;
        let tables = self
            .client
            .list_tables(&ident)
            .await
            .map_err(|error| map_read_error(&error))?;
        let mut names = tables
            .into_iter()
            .map(|ident| ident.name)
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub(super) async fn table_exists(
        &self,
        table: &CatalogTableName,
    ) -> Result<bool, ConnectorError> {
        let ident = table_ident(table)?;
        self.client
            .table_exists(&ident)
            .await
            .map_err(|error| map_read_error(&error))
    }

    pub(super) async fn load_table(
        &self,
        table: &CatalogTableName,
    ) -> Result<crate::iceberg::table::Table, ConnectorError> {
        let ident = table_ident(table)?;
        self.client
            .load_table(&ident)
            .await
            .map_err(|error| map_read_error(&error))
    }

    pub(super) async fn view_exists(
        &self,
        view: &CatalogTableName,
    ) -> Result<bool, ConnectorError> {
        let ident = table_ident(view)?;
        self.client
            .view_exists(&ident)
            .await
            .map_err(|error| map_read_error(&error))
    }

    pub(super) async fn list_views(
        &self,
        namespace: &CatalogNamespaceName,
    ) -> Result<Vec<String>, ConnectorError> {
        let ident = namespace_ident(namespace)?;
        let views = self
            .client
            .list_views(&ident)
            .await
            .map_err(|error| map_read_error(&error))?;
        let mut names = views
            .into_iter()
            .map(|ident| ident.name)
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub(super) async fn load_view(
        &self,
        view: &CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        let ident = table_ident(view)?;
        self.client
            .load_view(&ident)
            .await
            .map_err(|error| map_read_error(&error))
    }

    // ---- Direct mutations ----------------------------------------------

    pub(super) async fn create_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        let ident = match namespace_ident(&namespace) {
            Ok(ident) => ident,
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                );
            }
        };
        match self
            .client
            .create_namespace(&ident, std::collections::HashMap::new())
            .await
        {
            Ok(_) => CatalogOutcome::committed(namespace, ExternalMutationEffect::Applied),
            Err(error) => self.classify(&error, || {
                CatalogCommitEvidence::for_target(namespace.namespace.clone())
            }),
        }
    }

    pub(super) async fn drop_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        let ident = match namespace_ident(&namespace) {
            Ok(ident) => ident,
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                );
            }
        };
        match self.client.drop_namespace(&ident).await {
            Ok(()) => CatalogOutcome::committed(namespace, ExternalMutationEffect::Applied),
            Err(error) => self.classify(&error, || {
                CatalogCommitEvidence::for_target(namespace.namespace.clone())
            }),
        }
    }

    /// Drop a table, capturing its exact object identity first.
    ///
    /// The identity has to be read before the drop because it is unreadable
    /// after, and post-commit cleanup may only ever act on exact identity —
    /// never on a path prefix. A table that cannot be loaded still gets
    /// dropped; the receipt simply carries no cleanup facts, which correctly
    /// leaves nothing eligible for deletion.
    pub(super) async fn drop_table(
        &self,
        table: CatalogTableName,
    ) -> CatalogOutcome<CatalogDropTableReceipt> {
        let ident = match table_ident(&table) {
            Ok(ident) => ident,
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                );
            }
        };
        let receipt = match self.client.load_table(&ident).await {
            Ok(loaded) => CatalogDropTableReceipt {
                table_uuid: Some(Arc::from(loaded.metadata().uuid().to_string())),
                table_location: Some(Arc::from(loaded.metadata().location())),
                metadata_location: loaded.metadata_location().map(Arc::from),
                last_updated_ms: loaded.metadata().last_updated_ms(),
            },
            Err(_) => CatalogDropTableReceipt {
                table_uuid: None,
                table_location: None,
                metadata_location: None,
                last_updated_ms: 0,
            },
        };
        match self.client.drop_table(&ident).await {
            Ok(()) => CatalogOutcome::committed(receipt, ExternalMutationEffect::Applied),
            Err(error) => self.classify(&error, || {
                CatalogCommitEvidence::for_target(table.canonical())
            }),
        }
    }

    pub(super) async fn register_table(
        &self,
        table: CatalogTableName,
        metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        let ident = match table_ident(&table) {
            Ok(ident) => ident,
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                );
            }
        };
        match self
            .client
            .register_table(&ident, metadata_location.to_string())
            .await
        {
            Ok(_) => CatalogOutcome::committed(table, ExternalMutationEffect::Applied),
            Err(error) => self.classify(&error, || {
                CatalogCommitEvidence::for_target(table.canonical())
                    .with_metadata_location(metadata_location.clone())
            }),
        }
    }

    fn classify<T>(
        &self,
        error: &crate::iceberg::Error,
        evidence: impl FnOnce() -> CatalogCommitEvidence,
    ) -> CatalogOutcome<T> {
        if proves_uncommitted(error) {
            return CatalogOutcome::uncommitted(uncommitted_failure_kind(error), error.to_string());
        }
        CatalogOutcome::unknown(error.to_string(), evidence())
    }
}
