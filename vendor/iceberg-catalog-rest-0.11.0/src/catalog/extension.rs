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

//! NovaRocks protocol surfaces implemented by the session catalog.

use super::*;

/// A REST staged-create result and the authoritative initialization updates
/// required by its first `assert-create` commit.
#[derive(Debug)]
pub struct StagedTableCreate {
    table: Table,
    initialization_updates: Vec<TableUpdate>,
}

impl StagedTableCreate {
    /// Return the staged table. Its metadata location is absent because the
    /// table has not been published yet.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Return initialization updates reconstructed from the staged response.
    pub fn initialization_updates(&self) -> &[TableUpdate] {
        &self.initialization_updates
    }

    /// Consume this result into the staged table and initialization updates.
    pub fn into_parts(self) -> (Table, Vec<TableUpdate>) {
        (self.table, self.initialization_updates)
    }
}

/// A redacted view of one prefix-scoped REST storage credential.
///
/// Credential values remain private. Consumers can inspect only the keys they
/// understand and retrieve a value by its exact key while translating this
/// response into their own closed credential type.
pub struct StorageCredentialDelegation<'a> {
    credential: &'a crate::types::StorageCredential,
}

impl StorageCredentialDelegation<'_> {
    /// Prefix to which this credential applies.
    pub fn prefix(&self) -> &str {
        &self.credential.prefix
    }

    /// Return the value for one known provider configuration key.
    pub fn config_value(&self, key: &str) -> Option<&str> {
        self.credential.config.get(key).map(String::as_str)
    }

    /// Return the available provider configuration key names, never values.
    pub fn config_keys(&self) -> impl Iterator<Item = &str> {
        self.credential.config.keys().map(String::as_str)
    }
}

impl Debug for StorageCredentialDelegation<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageCredentialDelegation")
            .field("prefix", &self.credential.prefix)
            .field(
                "config_keys",
                &self.credential.config.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Storage delegation facts received from one REST table response.
///
/// This is intentionally not serializable and has no accessor that returns a
/// raw configuration map. It is a short-lived handoff from the vendored REST
/// client to the provider-owned parser.
pub struct RestAccessDelegation {
    storage_credentials: Option<Vec<crate::types::StorageCredential>>,
}

impl RestAccessDelegation {
    fn new(storage_credentials: Option<Vec<crate::types::StorageCredential>>) -> Self {
        Self {
            storage_credentials,
        }
    }

    /// Whether the REST response carried the `storage-credentials` member.
    pub fn is_present(&self) -> bool {
        self.storage_credentials.is_some()
    }

    /// Number of prefix-scoped credentials in the response.
    pub fn len(&self) -> usize {
        self.storage_credentials.as_ref().map_or(0, Vec::len)
    }

    /// Whether no prefix-scoped credentials were returned.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over redacted prefix-scoped credential views.
    pub fn credentials(&self) -> impl Iterator<Item = StorageCredentialDelegation<'_>> {
        self.storage_credentials
            .iter()
            .flatten()
            .map(|credential| StorageCredentialDelegation { credential })
    }
}

impl Debug for RestAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestAccessDelegation")
            .field("present", &self.is_present())
            .field("credential_count", &self.len())
            .finish()
    }
}

use crate::types::{
    CommitViewRequest, CreateViewRequest, ListViewsResponse, LoadCredentialsResult, LoadViewResult,
};
use iceberg::TableUpdate;
use iceberg::spec::{ViewMetadata, ViewVersion};
use iceberg::{ViewCommit, ViewCreation};

/// A table response whose immutable metadata has not yet been associated with
/// a [`FileIO`].
///
/// This is for providers that must consume response-local credentials before
/// they choose the request-scoped FileIO.  It deliberately exposes neither
/// the response's configuration map nor its metadata: callers can only
/// materialize the already-frozen table with a FileIO they own.
pub struct DeferredRestTableMaterialization {
    table_ident: TableIdent,
    metadata_location: Option<String>,
    metadata: iceberg::spec::TableMetadata,
    // Keep ordinary REST table config private so the catalog can retain its
    // legacy materialization path without exposing a second raw config API.
    config: HashMap<String, String>,
    runtime: Runtime,
    kms_client: Option<Arc<dyn KeyManagementClient>>,
}

impl DeferredRestTableMaterialization {
    /// Materialize this frozen response with an explicitly supplied FileIO.
    ///
    /// A request-scoped provider calls this only after it has converted the
    /// companion access delegation into its own sealed credential capability.
    pub fn materialize_with_file_io(self, file_io: FileIO) -> Result<Table> {
        let mut table_builder = Table::builder()
            .identifier(self.table_ident)
            .file_io(file_io)
            .metadata(self.metadata)
            .runtime(self.runtime);
        if let Some(kms_client) = self.kms_client {
            table_builder = table_builder.kms_client(kms_client);
        }
        match self.metadata_location {
            Some(metadata_location) => table_builder.metadata_location(metadata_location).build(),
            None => table_builder.build(),
        }
    }
}

impl Debug for DeferredRestTableMaterialization {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredRestTableMaterialization")
            .field("table", &self.table_ident)
            .field("has_metadata_location", &self.metadata_location.is_some())
            .field("config_keys", &self.config.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A REST table response with response-local access delegation that has not
/// yet selected a FileIO.
pub struct DeferredRestTableWithAccessDelegation {
    materialization: DeferredRestTableMaterialization,
    access_delegation: RestAccessDelegation,
}

impl DeferredRestTableWithAccessDelegation {
    /// Consume this response into its opaque table materialization and
    /// response-local delegation facts.
    pub fn into_parts(self) -> (DeferredRestTableMaterialization, RestAccessDelegation) {
        (self.materialization, self.access_delegation)
    }
}

impl Debug for DeferredRestTableWithAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredRestTableWithAccessDelegation")
            .field("materialization", &self.materialization)
            .field("access_delegation", &self.access_delegation)
            .finish()
    }
}

/// A staged table response whose FileIO is intentionally deferred until a
/// provider has consumed response-local access delegation.
pub struct DeferredStagedTableCreateWithAccessDelegation {
    materialization: DeferredRestTableMaterialization,
    initialization_updates: Vec<TableUpdate>,
    access_delegation: RestAccessDelegation,
}

impl DeferredStagedTableCreateWithAccessDelegation {
    /// Consume this staged response into opaque table materialization, the
    /// server-derived initialization updates, and response-local delegation.
    pub fn into_parts(
        self,
    ) -> (
        DeferredRestTableMaterialization,
        Vec<TableUpdate>,
        RestAccessDelegation,
    ) {
        (
            self.materialization,
            self.initialization_updates,
            self.access_delegation,
        )
    }
}

impl Debug for DeferredStagedTableCreateWithAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredStagedTableCreateWithAccessDelegation")
            .field("materialization", &self.materialization)
            .field(
                "initialization_update_count",
                &self.initialization_updates.len(),
            )
            .field("access_delegation", &self.access_delegation)
            .finish()
    }
}

impl RestSessionCatalog {
    fn defer_table_response(
        &self,
        table_ident: TableIdent,
        response: LoadTableResult,
    ) -> DeferredRestTableWithAccessDelegation {
        let LoadTableResult {
            metadata_location,
            metadata,
            config,
            storage_credentials,
        } = response;
        DeferredRestTableWithAccessDelegation {
            materialization: DeferredRestTableMaterialization {
                table_ident,
                metadata_location,
                metadata,
                config,
                runtime: self.runtime.clone(),
                kms_client: self.kms_client.clone(),
            },
            access_delegation: RestAccessDelegation::new(storage_credentials),
        }
    }
}

impl RestCatalog {
    /// Load a response with storage delegation before selecting a FileIO.
    pub async fn load_table_deferred_with_access_delegation(
        &self,
        table_ident: &TableIdent,
    ) -> Result<DeferredRestTableWithAccessDelegation> {
        self.inner
            .load_table_deferred_with_access_delegation(&self.session_context, table_ident)
            .await
    }
}

impl RestSessionCatalog {
    /// Load a frozen response and its response-local storage delegation.
    pub async fn load_table_deferred_with_access_delegation(
        &self,
        _context: &SessionContext,
        table_ident: &TableIdent,
    ) -> Result<DeferredRestTableWithAccessDelegation> {
        let client = self.client().await?;
        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::GET, client.config.table_endpoint(table_ident))
                .header("x-iceberg-access-delegation", "vended-credentials"),
        )?;
        let response = client.query_catalog(request).await?;
        let response = match response.status() {
            StatusCode::OK | StatusCode::NOT_MODIFIED => {
                deserialize_catalog_response::<LoadTableResult>(response)?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    "Tried to load a table that does not exist",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    response,
                    client.http_client.disable_header_redaction(),
                ));
            }
        };
        Ok(self.defer_table_response(table_ident.clone(), response))
    }
}

impl RestCatalog {
    /// Renew storage delegation without re-observing table metadata.
    pub async fn load_credentials_with_access_delegation(
        &self,
        credentials_endpoint: &str,
    ) -> Result<RestAccessDelegation> {
        self.inner
            .load_credentials_with_access_delegation(&self.session_context, credentials_endpoint)
            .await
    }
}

impl RestSessionCatalog {
    /// Renew credentials using this session owner's authenticated HTTP client.
    pub async fn load_credentials_with_access_delegation(
        &self,
        _context: &SessionContext,
        credentials_endpoint: &str,
    ) -> Result<RestAccessDelegation> {
        let client = self.client().await?;
        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::GET, credentials_endpoint),
        )?;
        let response = client.query_catalog(request).await?;
        match response.status() {
            StatusCode::OK => {
                let response = deserialize_catalog_response::<LoadCredentialsResult>(response)?;
                Ok(RestAccessDelegation::new(response.storage_credentials))
            }
            _ => Err(deserialize_unexpected_catalog_error(
                response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }
}

/// Dispatch certainty for the REST staged-create request. Downstream saga
/// owners must never infer this boundary from an error string or a generic
/// `Unexpected` kind.
#[derive(Debug)]
pub enum StagedCreateError {
    /// The server rejected the staged create because the target exists.
    Conflict(Error),
    /// The request was not sent to the staged-create endpoint.
    KnownNotDispatched(Error),
    /// The request may have reached the server and needs reconciliation.
    PossiblyDispatched(Error),
}

/// Dispatch certainty for the assert-create publication request.
#[derive(Debug)]
pub enum StagedCommitError {
    /// The assert-create requirement rejected a concurrent visible table.
    Conflict(Error),
    /// The publication request was not sent.
    KnownNotDispatched(Error),
    /// The publication request may have reached the server.
    PossiblyDispatched(Error),
    /// The server confirmed success but the response could not be finalized.
    CommittedResponseInvalid(Error),
}

impl RestCatalog {
    /// Stage authoritative initial metadata with dispatch certainty.
    pub async fn stage_create_table_typed(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> std::result::Result<StagedTableCreate, StagedCreateError> {
        self.inner
            .stage_create_table_typed(&self.session_context, namespace, creation)
            .await
    }

    /// Stage metadata and retain delegation before choosing a FileIO.
    pub async fn stage_create_table_typed_deferred_with_access_delegation(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> std::result::Result<DeferredStagedTableCreateWithAccessDelegation, StagedCreateError> {
        self.inner
            .stage_create_table_typed_deferred_with_access_delegation(
                &self.session_context,
                namespace,
                creation,
            )
            .await
    }

    /// Publish a staged table once with dispatch certainty.
    pub async fn commit_staged_table_typed(
        &self,
        commit: TableCommit,
    ) -> std::result::Result<Table, StagedCommitError> {
        self.inner
            .commit_staged_table_typed(&self.session_context, commit)
            .await
    }

    /// Publish once and finalize with the caller's already-admitted FileIO.
    pub async fn commit_staged_table_typed_with_file_io(
        &self,
        commit: TableCommit,
        file_io: FileIO,
    ) -> std::result::Result<Table, StagedCommitError> {
        self.inner
            .commit_staged_table_typed_with_file_io(&self.session_context, commit, file_io)
            .await
    }
}

impl RestSessionCatalog {
    /// Stage initial metadata; even the static form requests delegation.
    pub async fn stage_create_table_typed(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> std::result::Result<StagedTableCreate, StagedCreateError> {
        let staged = self
            .stage_create_table_typed_deferred_with_access_delegation(context, namespace, creation)
            .await?;
        let (materialization, initialization_updates, _) = staged.into_parts();
        let file_io_location = materialization
            .metadata_location
            .as_deref()
            .unwrap_or_else(|| materialization.metadata.location());
        let file_io = self
            .load_file_io(Some(file_io_location), Some(materialization.config.clone()))
            .await
            .map_err(StagedCreateError::PossiblyDispatched)?;
        let table = materialization
            .materialize_with_file_io(file_io)
            .map_err(StagedCreateError::PossiblyDispatched)?;
        Ok(StagedTableCreate {
            table,
            initialization_updates,
        })
    }

    /// Stage initial metadata without selecting a catalog-global FileIO.
    pub async fn stage_create_table_typed_deferred_with_access_delegation(
        &self,
        _context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> std::result::Result<DeferredStagedTableCreateWithAccessDelegation, StagedCreateError> {
        let client = self
            .client()
            .await
            .map_err(StagedCreateError::KnownNotDispatched)?;
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::POST, client.config.tables_endpoint(namespace))
                .header("x-iceberg-access-delegation", "vended-credentials")
                .json(&CreateTableRequest {
                    name: creation.name,
                    location: creation.location,
                    schema: creation.schema,
                    partition_spec: creation.partition_spec,
                    write_order: creation.sort_order,
                    stage_create: Some(true),
                    properties: creation.properties,
                }),
        )
        .map_err(StagedCreateError::KnownNotDispatched)?;
        let response = client
            .query_catalog(request)
            .await
            .map_err(StagedCreateError::PossiblyDispatched)?;
        let response = match response.status() {
            StatusCode::OK => deserialize_catalog_response::<LoadTableResult>(response)
                .map_err(StagedCreateError::PossiblyDispatched)?,
            StatusCode::CONFLICT => {
                return Err(StagedCreateError::Conflict(Error::new(
                    ErrorKind::TableAlreadyExists,
                    "The table already exists",
                )));
            }
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => {
                return Err(StagedCreateError::PossiblyDispatched(
                    deserialize_unexpected_catalog_error(
                        response,
                        client.http_client.disable_header_redaction(),
                    ),
                ));
            }
            _ => {
                return Err(StagedCreateError::KnownNotDispatched(
                    deserialize_unexpected_catalog_error(
                        response,
                        client.http_client.disable_header_redaction(),
                    ),
                ));
            }
        };
        let initialization_updates = response
            .metadata
            .staged_create_initialization_updates()
            .map_err(StagedCreateError::PossiblyDispatched)?;
        let (materialization, access_delegation) = self
            .defer_table_response(table_ident, response)
            .into_parts();
        Ok(DeferredStagedTableCreateWithAccessDelegation {
            materialization,
            initialization_updates,
            access_delegation,
        })
    }

    /// Publish once and materialize with the catalog's storage factory.
    pub async fn commit_staged_table_typed(
        &self,
        context: &SessionContext,
        commit: TableCommit,
    ) -> std::result::Result<Table, StagedCommitError> {
        let materialization = self
            .commit_staged_table_typed_deferred(context, commit)
            .await?;
        let file_io = self
            .load_file_io(materialization.metadata_location.as_deref(), None)
            .await
            .map_err(StagedCommitError::CommittedResponseInvalid)?;
        materialization
            .materialize_with_file_io(file_io)
            .map_err(StagedCommitError::CommittedResponseInvalid)
    }

    /// Publish once and finalize with caller-owned FileIO, without fallback.
    pub async fn commit_staged_table_typed_with_file_io(
        &self,
        context: &SessionContext,
        commit: TableCommit,
        file_io: FileIO,
    ) -> std::result::Result<Table, StagedCommitError> {
        self.commit_staged_table_typed_deferred(context, commit)
            .await?
            .materialize_with_file_io(file_io)
            .map_err(StagedCommitError::CommittedResponseInvalid)
    }

    async fn commit_staged_table_typed_deferred(
        &self,
        _context: &SessionContext,
        mut commit: TableCommit,
    ) -> std::result::Result<DeferredRestTableMaterialization, StagedCommitError> {
        let client = self
            .client()
            .await
            .map_err(StagedCommitError::KnownNotDispatched)?;
        let request = HttpRequest::build(
            client
                .http_client
                .request(
                    Method::POST,
                    client.config.table_endpoint(commit.identifier()),
                )
                .json(&CommitTableRequest {
                    identifier: Some(commit.identifier().clone()),
                    requirements: commit.take_requirements(),
                    updates: commit.take_updates(),
                }),
        )
        .map_err(StagedCommitError::KnownNotDispatched)?;
        let response = client
            .query_catalog(request)
            .await
            .map_err(StagedCommitError::PossiblyDispatched)?;
        let response: CommitTableResponse = match response.status() {
            StatusCode::OK => deserialize_catalog_response(response)
                .map_err(StagedCommitError::CommittedResponseInvalid)?,
            StatusCode::CONFLICT => {
                return Err(StagedCommitError::Conflict(
                    Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "CatalogCommitConflicts, one or more requirements failed.",
                    )
                    .with_retryable(true),
                ));
            }
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => {
                return Err(StagedCommitError::PossiblyDispatched(
                    deserialize_unexpected_catalog_error(
                        response,
                        client.http_client.disable_header_redaction(),
                    ),
                ));
            }
            _ => {
                return Err(StagedCommitError::KnownNotDispatched(
                    deserialize_unexpected_catalog_error(
                        response,
                        client.http_client.disable_header_redaction(),
                    ),
                ));
            }
        };
        Ok(DeferredRestTableMaterialization {
            table_ident: commit.identifier().clone(),
            metadata_location: Some(response.metadata_location),
            metadata: response.metadata,
            config: HashMap::new(),
            runtime: self.runtime.clone(),
            kms_client: self.kms_client.clone(),
        })
    }
}

impl RestCatalogConfig {
    fn views_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "views"])
    }

    fn view_endpoint(&self, view: &TableIdent) -> String {
        self.url_prefixed(&[
            "namespaces",
            &view.namespace.to_url_string(),
            "views",
            &view.name,
        ])
    }
}

impl RestSessionCatalog {
    /// Create a new view inside the namespace.
    pub async fn create_view(
        &self,
        _context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> Result<ViewMetadata> {
        let client = self.client().await?;

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("system clock before epoch: {e}"),
                )
            })?
            .as_millis() as i64;
        let view_version = ViewVersion::builder()
            .with_version_id(1)
            .with_schema_id(creation.schema.schema_id())
            .with_timestamp_ms(timestamp_ms)
            .with_summary(creation.summary)
            .with_representations(creation.representations)
            .with_default_catalog(creation.default_catalog)
            .with_default_namespace(creation.default_namespace)
            .build();

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::POST, client.config.views_endpoint(namespace))
                .json(&CreateViewRequest {
                    name: creation.name,
                    location: creation.location,
                    schema: creation.schema,
                    view_version,
                    properties: creation.properties,
                }),
        )?;

        let http_response = client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response = deserialize_catalog_response::<LoadViewResult>(http_response)?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::NamespaceNotFound,
                "Tried to create a view under a namespace that does not exist",
            )),
            StatusCode::CONFLICT => {
                Err(Error::new(ErrorKind::Unexpected, "The view already exists"))
            }
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    /// Load a view's metadata from the catalog.
    pub async fn load_view(
        &self,
        _context: &SessionContext,
        view: &TableIdent,
    ) -> Result<ViewMetadata> {
        let client = self.client().await?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::GET, client.config.view_endpoint(view)),
        )?;

        let http_response = client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response = deserialize_catalog_response::<LoadViewResult>(http_response)?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to load a view that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    /// Commit updates to an existing view.
    pub async fn update_view(
        &self,
        _context: &SessionContext,
        mut commit: ViewCommit,
    ) -> Result<ViewMetadata> {
        let client = self.client().await?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(
                    Method::POST,
                    client.config.view_endpoint(commit.identifier()),
                )
                .json(&CommitViewRequest {
                    identifier: Some(commit.identifier().clone()),
                    requirements: commit.take_requirements(),
                    updates: commit.take_updates(),
                }),
        )?;

        let http_response = client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response = deserialize_catalog_response::<LoadViewResult>(http_response)?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to update a view that does not exist",
            )),
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "View commit failed due to a conflicting update",
            )
            .with_retryable(true)),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    /// Drop a view from the catalog.
    pub async fn drop_view(&self, _context: &SessionContext, view: &TableIdent) -> Result<()> {
        let client = self.client().await?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::DELETE, client.config.view_endpoint(view)),
        )?;

        let http_response = client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to drop a view that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    /// Check if a view exists in the catalog.
    pub async fn view_exists(&self, _context: &SessionContext, view: &TableIdent) -> Result<bool> {
        let client = self.client().await?;

        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::HEAD, client.config.view_endpoint(view)),
        )?;

        let http_response = client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    /// List views in the namespace.
    pub async fn list_views(
        &self,
        _context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>> {
        let client = self.client().await?;
        let endpoint = client.config.views_endpoint(namespace);
        let mut identifiers = Vec::new();
        let mut next_token = None;

        loop {
            let mut request = client.http_client.request(Method::GET, endpoint.clone());

            if let Some(token) = next_token {
                request = request.query(&[("pageToken", token)]);
            }

            let http_response = client.query_catalog(HttpRequest::build(request)?).await?;

            match http_response.status() {
                StatusCode::OK => {
                    let response =
                        deserialize_catalog_response::<ListViewsResponse>(http_response)?;

                    identifiers.extend(response.identifiers);

                    match response.next_page_token {
                        Some(token) => next_token = Some(token),
                        None => break,
                    }
                }
                StatusCode::NOT_FOUND => {
                    return Err(Error::new(
                        ErrorKind::NamespaceNotFound,
                        "Tried to list views under a namespace that does not exist",
                    ));
                }
                _ => {
                    return Err(deserialize_unexpected_catalog_error(
                        http_response,
                        client.http_client.disable_header_redaction(),
                    ));
                }
            }
        }

        Ok(identifiers)
    }
}

#[cfg(test)]
mod tests {
    //! Credential-handoff correctness coverage: exact-key reads preserve the
    //! response while Debug never reveals values. This isolated in-memory test
    //! follows the closed handoff contract and runs with the REST crate suite;
    //! the NovaRocks connector maintainer owns it until IRU-13a replaces R6.
    //! Scripted HTTP cases each own a mockito listener and join its lifetime
    //! on drop; they preserve the prior credential response and opt-in oracle.
    use super::*;
    use iceberg::TableRequirement;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, SortOrder, Summary, TableMetadata, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    use mockito::{Matcher, Mock, Server, ServerGuard};
    use serde_json::json;
    use uuid::uuid;

    async fn create_config_mock(server: &mut ServerGuard) -> Mock {
        server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(r#"{"overrides":{"warehouse":"s3://iceberg-catalog"},"defaults":{}}"#)
            .create_async()
            .await
    }

    fn catalog(
        config: RestCatalogConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
    ) -> RestCatalog {
        RestCatalog::new(
            SessionContext::empty(),
            config,
            None,
            storage_factory,
            Runtime::try_current().unwrap(),
            None,
        )
    }

    #[test]
    fn storage_delegation_exposes_exact_keys_without_debug_secrets() {
        let delegation = RestAccessDelegation::new(Some(vec![crate::types::StorageCredential {
            prefix: "s3://warehouse/data/".into(),
            config: HashMap::from([
                ("s3.access-key-id".into(), "access-canary".into()),
                ("s3.secret-access-key".into(), "secret-canary".into()),
            ]),
        }]));
        assert!(delegation.is_present());
        assert_eq!(delegation.len(), 1);
        let credential = delegation.credentials().next().unwrap();
        assert_eq!(credential.prefix(), "s3://warehouse/data/");
        assert_eq!(
            credential.config_value("s3.access-key-id"),
            Some("access-canary")
        );
        assert_eq!(credential.config_value("unknown"), None);
        assert!(!format!("{delegation:?}").contains("secret-canary"));
        assert!(!format!("{credential:?}").contains("secret-canary"));
        assert!(format!("{credential:?}").contains("s3.secret-access-key"));
        assert!(!RestAccessDelegation::new(None).is_present());
        assert!(RestAccessDelegation::new(Some(vec![])).is_empty());
    }

    #[test]
    fn deferred_materialization_retains_loading_runtime_and_kms_without_ambient_runtime() {
        let executor = tokio::runtime::Runtime::new().unwrap();
        let runtime = {
            let _entered = executor.enter();
            Runtime::try_current().unwrap()
        };
        for has_kms in [false, true] {
            let mut response: serde_json::Value =
                serde_json::from_str(include_str!("../../testdata/create_table_response.json"))
                    .unwrap();
            response["metadata"]["format-version"] = serde_json::json!(3);
            response["metadata"]["next-row-id"] = serde_json::json!(0);
            response["metadata"]["properties"]["encryption.key-id"] =
                serde_json::json!("table-key");
            let response: LoadTableResult = serde_json::from_value(response).unwrap();
            let kms_client = has_kms.then(|| {
                Arc::new(iceberg::encryption::kms::MemoryKeyManagementClient::new())
                    as Arc<dyn KeyManagementClient>
            });
            let catalog = RestSessionCatalog::new(
                RestCatalogConfig::builder()
                    .uri("http://localhost".into())
                    .build(),
                None,
                None,
                runtime.clone(),
                kms_client,
            );
            let (materialization, _) = catalog
                .defer_table_response(TableIdent::from_strs(["ns", "t"]).unwrap(), response)
                .into_parts();
            let materialized = materialization.materialize_with_file_io(FileIO::new_with_memory());
            if has_kms {
                assert_eq!(materialized.unwrap().identifier().name, "t");
            } else {
                assert_eq!(
                    materialized.unwrap_err().kind(),
                    ErrorKind::PreconditionFailed
                );
            }
        }
    }
    #[tokio::test]
    async fn access_delegation_is_opt_in_and_preserves_redacted_facts() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;
        let response = std::fs::read_to_string(format!(
            "{}/testdata/load_table_response.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
        .replacen(
            "\n}",
            ",\n  \"storage-credentials\": [{\n    \"prefix\": \"s3://warehouse/database/\",\n    \"config\": {\"s3.access-key-id\": \"access-canary\", \"s3.secret-access-key\": \"secret-canary\"}\n  }]\n}",
            1,
        );
        let ordinary = server
            .mock("GET", "/v1/namespaces/ns1/tables/ordinary")
            .match_header("x-iceberg-access-delegation", Matcher::Missing)
            .with_status(200)
            .with_body(response.clone())
            .create_async()
            .await;
        let delegated = server
            .mock("GET", "/v1/namespaces/ns1/tables/delegated")
            .match_header("x-iceberg-access-delegation", "vended-credentials")
            .with_status(200)
            .with_body(response)
            .create_async()
            .await;
        let catalog = catalog(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let ordinary_table = catalog
            .load_table(&TableIdent::from_strs(["ns1", "ordinary"]).unwrap())
            .await
            .unwrap();
        assert_eq!(ordinary_table.identifier().name, "ordinary");
        let delegated_response = catalog
            .load_table_deferred_with_access_delegation(
                &TableIdent::from_strs(["ns1", "delegated"]).unwrap(),
            )
            .await
            .unwrap();
        let (materialization, delegation) = delegated_response.into_parts();
        assert!(delegation.is_present());
        assert_eq!(delegation.len(), 1);
        let credential = delegation.credentials().next().unwrap();
        assert_eq!(credential.prefix(), "s3://warehouse/database/");
        assert_eq!(
            credential.config_value("s3.access-key-id"),
            Some("access-canary")
        );
        assert!(format!("{credential:?}").contains("s3.access-key-id"));
        assert!(!format!("{credential:?}").contains("secret-canary"));
        assert!(!format!("{materialization:?}").contains("secret-canary"));
        let request_scoped_table = materialization
            .materialize_with_file_io(FileIO::new_with_memory())
            .expect("materialize deferred response with caller-owned FileIO");
        assert_eq!(request_scoped_table.identifier().name, "delegated");

        config_mock.assert_async().await;
        ordinary.assert_async().await;
        delegated.assert_async().await;
    }
    #[tokio::test]
    async fn credential_refresh_uses_catalog_authentication_and_redacts_values() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;
        let credentials = server
            .mock("GET", "/credentials")
            .match_header("authorization", "Bearer refresh-token")
            .with_status(200)
            .with_body(
                r#"{
                    "storage-credentials": [{
                        "prefix": "s3://warehouse/data/",
                        "config": {
                            "s3.access-key-id": "access-canary",
                            "s3.secret-access-key": "secret-canary"
                        }
                    }]
                }"#,
            )
            .create_async()
            .await;
        let catalog = catalog(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(HashMap::from([(
                    "token".to_string(),
                    "refresh-token".to_string(),
                )]))
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let delegation = catalog
            .load_credentials_with_access_delegation(&format!("{}/credentials", server.url()))
            .await
            .expect("refresh response");
        assert!(delegation.is_present());
        assert_eq!(delegation.len(), 1);
        let credential = delegation.credentials().next().expect("credential");
        assert_eq!(credential.prefix(), "s3://warehouse/data/");
        assert_eq!(
            credential.config_value("s3.access-key-id"),
            Some("access-canary")
        );
        assert!(!format!("{credential:?}").contains("secret-canary"));
        assert!(!format!("{delegation:?}").contains("secret-canary"));

        config_mock.assert_async().await;
        credentials.assert_async().await;
    }
    #[tokio::test]
    async fn test_stage_create_and_commit_authoritative_metadata_with_snapshot_updates() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;

        let mut staged_response: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/testdata/create_table_response.json"
        )))
        .unwrap();
        staged_response["metadata-location"] = serde_json::Value::Null;

        let stage_create_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .match_header("x-iceberg-access-delegation", "vended-credentials")
            .match_body(mockito::Matcher::PartialJson(json!({
                "name": "test1",
                "stage-create": true,
                "properties": {"owner": "request-owner"}
            })))
            .with_status(200)
            .with_body(staged_response.to_string())
            .expect(1)
            .create_async()
            .await;

        let catalog = catalog(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );
        let request_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "request_field", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .with_schema_id(77)
            .build()
            .unwrap();
        let staged = catalog
            .stage_create_table_typed(
                &NamespaceIdent::from_strs(["ns1"]).unwrap(),
                TableCreation::builder()
                    .name("test1".to_string())
                    .schema(request_schema)
                    .properties(HashMap::from([(
                        "owner".to_string(),
                        "request-owner".to_string(),
                    )]))
                    .build(),
            )
            .await
            .unwrap();

        assert_eq!(None, staged.table().metadata_location());
        assert_eq!(
            uuid!("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            staged.table().metadata().uuid()
        );
        assert_eq!(
            "s3://warehouse/database/table",
            staged.table().metadata().location()
        );
        assert_eq!(0, staged.table().metadata().current_schema_id());
        assert_eq!(0, staged.table().metadata().default_partition_spec_id());
        assert_eq!(0, staged.table().metadata().default_sort_order_id());
        assert_eq!(
            Some(&"zstd".to_string()),
            staged
                .table()
                .metadata()
                .properties()
                .get("write.parquet.compression-codec")
        );
        assert!(!staged.table().metadata().properties().contains_key("owner"));

        let initialization_json = serde_json::to_value(staged.initialization_updates()).unwrap();
        let initialization_json = initialization_json.as_array().unwrap();
        assert_eq!(9, initialization_json.len());
        assert_eq!(
            json!({
                "action": "assign-uuid",
                "uuid": "bf289591-dcc0-4234-ad4f-5c3eed811a29"
            }),
            initialization_json[0]
        );
        assert_eq!(
            json!({
                "action": "set-location",
                "location": "s3://warehouse/database/table"
            }),
            initialization_json[1]
        );
        assert_eq!(0, initialization_json[2]["schema"]["schema-id"]);
        assert_eq!(3, initialization_json[2]["last-column-id"]);
        assert_eq!(0, initialization_json[3]["schema-id"]);
        assert_eq!(0, initialization_json[4]["spec"]["spec-id"]);
        assert_eq!(0, initialization_json[5]["spec-id"]);
        assert_eq!(0, initialization_json[6]["sort-order"]["order-id"]);
        assert_eq!(0, initialization_json[7]["sort-order-id"]);
        assert_eq!(
            "zstd",
            initialization_json[8]["updates"]["write.parquet.compression-codec"]
        );

        let ident = staged.table().identifier().clone();
        let (_, mut updates) = staged.into_parts();
        let snapshot_id = 3055729675574597000;
        updates.push(TableUpdate::AddSnapshot {
            snapshot: Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_timestamp_ms(1657810968051)
                .with_sequence_number(0)
                .with_manifest_list("s3://warehouse/database/table/metadata/snap-proof.avro")
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build(),
        });
        updates.push(TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        });

        let expected_request = CommitTableRequest {
            identifier: Some(ident.clone()),
            requirements: vec![TableRequirement::NotExist],
            updates: updates.clone(),
        };
        let commit_response = json!({
            "metadata-location": "s3://warehouse/database/table/metadata/v1.metadata.json",
            "metadata": staged_response["metadata"].clone()
        });
        let commit_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .match_body(mockito::Matcher::Json(
                serde_json::to_value(&expected_request).unwrap(),
            ))
            .with_status(200)
            .with_body(commit_response.to_string())
            .expect(1)
            .create_async()
            .await;

        let committed = catalog
            .update_table(
                TableCommit::builder()
                    .ident(ident)
                    .requirements(vec![TableRequirement::NotExist])
                    .updates(updates)
                    .build(),
            )
            .await
            .unwrap();
        assert_eq!(
            Some("s3://warehouse/database/table/metadata/v1.metadata.json"),
            committed.metadata_location()
        );

        config_mock.assert_async().await;
        stage_create_mock.assert_async().await;
        commit_mock.assert_async().await;
    }

    fn typed_stage_creation() -> TableCreation {
        TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    ])
                    .build()
                    .unwrap(),
            )
            .build()
    }

    fn typed_staged_commit() -> TableCommit {
        TableCommit::builder()
            .ident(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .requirements(vec![TableRequirement::NotExist])
            .updates(Vec::new())
            .build()
    }

    fn catalog_error_body(status: u16) -> String {
        json!({
            "error": {
                "message": "typed staged operation failure",
                "type": "TestException",
                "code": status
            }
        })
        .to_string()
    }

    #[tokio::test]
    async fn typed_deferred_stage_create_keeps_vended_credentials_out_of_file_io() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;
        let response = std::fs::read_to_string(format!(
            "{}/testdata/create_table_response.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
        .replacen(
            "\n}",
            ",\n  \"storage-credentials\": [{\n    \"prefix\": \"s3://warehouse/database/\",\n    \"config\": {\"s3.access-key-id\": \"access-canary\", \"s3.secret-access-key\": \"secret-canary\"}\n  }]\n}",
            1,
        );
        let stage_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .match_header("x-iceberg-access-delegation", "vended-credentials")
            .with_status(200)
            .with_body(response)
            .create_async()
            .await;
        // No StorageFactory is installed. A successful deferred response
        // proves the method did not select a catalog-global FileIO first.
        let catalog = catalog(RestCatalogConfig::builder().uri(server.url()).build(), None);

        let staged = catalog
            .stage_create_table_typed_deferred_with_access_delegation(
                &NamespaceIdent::from_strs(["ns1"]).unwrap(),
                typed_stage_creation(),
            )
            .await
            .expect("deferred stage response");
        let (materialization, initialization_updates, delegation) = staged.into_parts();
        assert!(!initialization_updates.is_empty());
        assert!(delegation.is_present());
        assert!(!format!("{materialization:?}").contains("secret-canary"));
        assert!(!format!("{delegation:?}").contains("secret-canary"));
        let table = materialization
            .materialize_with_file_io(FileIO::new_with_memory())
            .expect("caller-owned request FileIO materializes staged table");
        assert_eq!(table.identifier().name, "test1");

        config_mock.assert_async().await;
        stage_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_typed_stage_create_preserves_conflict_and_dispatch_uncertainty() {
        for (status, expected_conflict) in [(409, true), (503, false)] {
            let mut server = Server::new_async().await;
            let config_mock = create_config_mock(&mut server).await;
            let stage_mock = server
                .mock("POST", "/v1/namespaces/ns1/tables")
                .with_status(status)
                .with_body(catalog_error_body(status as u16))
                .expect(1)
                .create_async()
                .await;
            let catalog = catalog(
                RestCatalogConfig::builder().uri(server.url()).build(),
                Some(Arc::new(LocalFsStorageFactory)),
            );
            let error = catalog
                .stage_create_table_typed(
                    &NamespaceIdent::from_strs(["ns1"]).unwrap(),
                    typed_stage_creation(),
                )
                .await
                .unwrap_err();
            assert_eq!(
                expected_conflict,
                matches!(error, StagedCreateError::Conflict(_))
            );
            assert_eq!(
                !expected_conflict,
                matches!(error, StagedCreateError::PossiblyDispatched(_))
            );
            config_mock.assert_async().await;
            stage_mock.assert_async().await;
        }
    }

    #[tokio::test]
    async fn test_typed_stage_create_request_build_failure_is_known_not_dispatched() {
        let catalog = catalog(
            RestCatalogConfig::builder()
                .uri("://invalid".to_string())
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );
        let error = catalog
            .stage_create_table_typed(
                &NamespaceIdent::from_strs(["ns1"]).unwrap(),
                typed_stage_creation(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StagedCreateError::KnownNotDispatched(_)));
    }

    #[tokio::test]
    async fn test_typed_staged_commit_preserves_conflict_and_response_uncertainty() {
        for (status, expected) in [(409, "conflict"), (503, "dispatch"), (200, "response")] {
            let mut server = Server::new_async().await;
            let config_mock = create_config_mock(&mut server).await;
            let body = if status == 200 {
                "not-json".to_string()
            } else {
                catalog_error_body(status)
            };
            let commit_mock = server
                .mock("POST", "/v1/namespaces/ns1/tables/test1")
                .with_status(status as usize)
                .with_body(body)
                .expect(1)
                .create_async()
                .await;
            let catalog = catalog(
                RestCatalogConfig::builder().uri(server.url()).build(),
                Some(Arc::new(LocalFsStorageFactory)),
            );
            let error = catalog
                .commit_staged_table_typed(typed_staged_commit())
                .await
                .unwrap_err();
            match expected {
                "conflict" => assert!(matches!(error, StagedCommitError::Conflict(_))),
                "dispatch" => assert!(matches!(error, StagedCommitError::PossiblyDispatched(_))),
                "response" => {
                    assert!(matches!(
                        error,
                        StagedCommitError::CommittedResponseInvalid(_)
                    ))
                }
                _ => unreachable!(),
            }
            config_mock.assert_async().await;
            commit_mock.assert_async().await;
        }
    }

    #[tokio::test]
    async fn typed_staged_commit_with_caller_file_io_needs_no_catalog_storage_factory() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;
        let response = json!({
            "metadata-location": "s3://warehouse/database/table/metadata/v1.metadata.json",
            "metadata": staged_metadata_json(),
        });
        let commit_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body(response.to_string())
            .expect(1)
            .create_async()
            .await;
        // The caller-owned FileIO is the only storage capability. A vended
        // catalog must not need a catalog-global StorageFactory to finalize a
        // successful staged commit response.
        let catalog = catalog(RestCatalogConfig::builder().uri(server.url()).build(), None);

        let table = catalog
            .commit_staged_table_typed_with_file_io(
                typed_staged_commit(),
                FileIO::new_with_memory(),
            )
            .await
            .expect("caller-owned FileIO materializes committed staged response");
        assert_eq!(table.identifier().name, "test1");

        config_mock.assert_async().await;
        commit_mock.assert_async().await;
    }

    fn staged_metadata_json() -> serde_json::Value {
        let response: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/testdata/create_table_response.json"
        )))
        .unwrap();
        response["metadata"].clone()
    }

    #[test]
    fn test_staged_create_initialization_updates_preserve_v2_and_v3_format() {
        for expected in [FormatVersion::V2, FormatVersion::V3] {
            let metadata = TableMetadataBuilder::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    ])
                    .build()
                    .unwrap(),
                UnboundPartitionSpec::default(),
                SortOrder::unsorted_order(),
                "s3://warehouse/database/versioned".to_string(),
                expected,
                HashMap::new(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let updates = metadata.staged_create_initialization_updates().unwrap();
            assert!(updates.contains(&TableUpdate::UpgradeFormatVersion {
                format_version: expected,
            }));
        }
    }

    #[test]
    fn test_staged_create_initialization_updates_reject_snapshot_state() {
        let mut metadata_json = staged_metadata_json();
        let snapshot_id = 3055729675574597000i64;
        metadata_json["current-snapshot-id"] = json!(snapshot_id);
        metadata_json["snapshots"] = json!([{
            "snapshot-id": snapshot_id,
            "timestamp-ms": 1657810968051i64,
            "summary": {"operation": "append"},
            "manifest-list": "s3://warehouse/database/table/metadata/snap-proof.avro"
        }]);
        metadata_json["refs"] = json!({
            "main": {"snapshot-id": snapshot_id, "type": "branch"}
        });
        let metadata: TableMetadata = serde_json::from_value(metadata_json).unwrap();
        let error = metadata.staged_create_initialization_updates().unwrap_err();
        assert_eq!(ErrorKind::DataInvalid, error.kind());
        assert!(error.message().contains("cannot be represented"));
    }

    #[test]
    fn test_staged_create_initialization_updates_reject_unrepresented_partition_watermark() {
        let mut metadata_json = staged_metadata_json();
        metadata_json["last-partition-id"] = json!(1000);
        let metadata: TableMetadata = serde_json::from_value(metadata_json).unwrap();
        let error = metadata.staged_create_initialization_updates().unwrap_err();
        assert_eq!(ErrorKind::DataInvalid, error.kind());
        assert!(error.message().contains("partition high-watermark"));
    }
}
