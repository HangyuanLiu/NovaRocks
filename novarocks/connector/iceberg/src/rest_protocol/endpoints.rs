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

//! The NovaRocks-issued REST read endpoints of one catalog generation.
//!
//! Every request uses the one session binding captured while upstream
//! established its session (L27): single-page listing, the delegated table
//! load and baseline re-read, table existence in both modes, the credentials
//! endpoint, view reads, and the load-table credential acquisition. Reads keep
//! today's error classes (L17): a load's 404 is `NotFound`, an existence
//! check's 404 is `false`, everything else is `Unavailable`. Non-success
//! bodies are never read.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use reqwest::Method;
use reqwest::header::HeaderValue;
use serde::de::DeserializeOwned;

use crate::iceberg::spec::ViewMetadata;
use crate::iceberg::{NamespaceIdent, Runtime, TableIdent};
use crate::iceberg_catalog_rest::{
    ListNamespaceResponse, ListTablesResponse, LoadTableResult, RestCatalog,
};
use crate::rest_protocol::attempt::{SessionGate, ensure_session};
use crate::rest_protocol::auth::RestAuthOwner;
use crate::rest_protocol::binding::SessionBinding;
use crate::rest_protocol::deferred::{FrozenLoadTable, RestDelegation};
use crate::rest_protocol::evidence::DispatchStage;
use crate::rest_protocol::http::{RequestContext, ResponseBody, RestRequest};
use crate::rest_protocol::json::{JsonScanner, ViewSink};
use crate::rest_protocol::minimal_view::{BackendAcquisitionSink, CredentialsSink};

/// The value the client sends to ask for vended storage credentials.
pub(crate) const VENDED_CREDENTIALS: HeaderValue = HeaderValue::from_static("vended-credentials");

/// `GET .../views/{view}` response. NovaRocks owns this wire shape: the
/// upstream REST crate has no view API.
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadViewResult {
    #[allow(dead_code)]
    metadata_location: String,
    metadata: ViewMetadata,
}

/// One catalog generation's REST session authority and read endpoints.
pub(crate) struct RestEndpoints {
    upstream: Arc<RestCatalog>,
    owner: Arc<RestAuthOwner>,
    gate: SessionGate,
    runtime: Runtime,
    declared_routing_headers: BTreeSet<String>,
}

impl fmt::Debug for RestEndpoints {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the upstream client (L29).
        formatter
            .debug_struct("RestEndpoints")
            .field("owner", &self.owner)
            .field("established", &self.gate.ready().is_some())
            .field("declared_routing_headers", &self.declared_routing_headers)
            .finish_non_exhaustive()
    }
}

/// Where a NovaRocks read ended.
enum Answer {
    /// A success status with its unread body.
    Success { code: u16, body: ResponseBody },
    /// Any other status; the body was not read.
    Status(u16),
}

impl RestEndpoints {
    pub(crate) fn new(
        upstream: Arc<RestCatalog>,
        owner: Arc<RestAuthOwner>,
        runtime: Runtime,
        declared_routing_headers: BTreeSet<String>,
    ) -> Self {
        Self {
            upstream,
            owner,
            gate: SessionGate::default(),
            runtime,
            declared_routing_headers,
        }
    }

    /// The generation's session binding, establishing the session when needed
    /// (L2). Concurrent callers share one establishment attempt.
    pub(crate) async fn binding(&self) -> Result<Arc<SessionBinding>, ConnectorError> {
        ensure_session(&self.owner, &self.gate, &self.upstream)
            .await
            .map_err(|failure| {
                ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    format!(
                        "Iceberg REST session could not be established: {:?}",
                        failure.step
                    ),
                )
            })
    }

    /// The upstream client, only after the session has been established
    /// through this generation's gate, so upstream never initializes itself.
    pub(crate) async fn upstream(&self) -> Result<&RestCatalog, ConnectorError> {
        self.binding().await?;
        Ok(&self.upstream)
    }

    pub(crate) fn owner(&self) -> &Arc<RestAuthOwner> {
        &self.owner
    }

    pub(crate) fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub(crate) fn declared_routing_headers(&self) -> &BTreeSet<String> {
        &self.declared_routing_headers
    }

    /// One page of namespaces. `parent` is never sent (L27).
    pub(crate) async fn list_namespaces_page(
        &self,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListNamespaceResponse, ConnectorError> {
        const OPERATION: &str = "list namespaces";
        let binding = self.binding().await?;
        let url = page_url(binding.namespaces_url(), page_token, page_size, OPERATION)?;
        match self.get(OPERATION, &binding, url, None, &[200]).await? {
            Answer::Success { body, .. } => decode_document(OPERATION, body).await,
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// One page of tables in `namespace`.
    pub(crate) async fn list_tables_page(
        &self,
        namespace: &NamespaceIdent,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListTablesResponse, ConnectorError> {
        const OPERATION: &str = "list tables";
        let binding = self.binding().await?;
        let url = page_url(
            binding.tables_url(namespace),
            page_token,
            page_size,
            OPERATION,
        )?;
        match self.get(OPERATION, &binding, url, None, &[200]).await? {
            Answer::Success { body, .. } => decode_document(OPERATION, body).await,
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// One page of views in `namespace`; a missing namespace is `NotFound`.
    pub(crate) async fn list_views_page(
        &self,
        namespace: &NamespaceIdent,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListTablesResponse, ConnectorError> {
        const OPERATION: &str = "list views";
        let binding = self.binding().await?;
        let url = page_url(
            binding.views_url(namespace),
            page_token,
            page_size,
            OPERATION,
        )?;
        match self.get(OPERATION, &binding, url, None, &[200]).await? {
            Answer::Success { body, .. } => decode_document(OPERATION, body).await,
            Answer::Status(404) => Err(not_found(OPERATION)),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Load and freeze one table response. `delegated` sends the access
    /// delegation header (L23): the vended load and every baseline re-read.
    pub(crate) async fn load_table(
        &self,
        table: &TableIdent,
        delegated: bool,
    ) -> Result<(FrozenLoadTable, RestDelegation), ConnectorError> {
        const OPERATION: &str = "load table";
        let binding = self.binding().await?;
        let delegation = delegated.then_some(VENDED_CREDENTIALS);
        match self
            .get(
                OPERATION,
                &binding,
                binding.table_url(table),
                delegation,
                &[200, 304],
            )
            .await?
        {
            Answer::Success { body, .. } => {
                let response: LoadTableResult = decode_document(OPERATION, body).await?;
                Ok(FrozenLoadTable::from_response(
                    table.clone(),
                    response,
                    self.runtime.clone(),
                ))
            }
            Answer::Status(404) => Err(not_found(OPERATION)),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Whether `table` exists, in both access-delegation modes (L27): `HEAD`
    /// when the server advertised it, otherwise a `GET` without the
    /// delegation header. Only the status is consulted; no body is read and
    /// no FileIO or table object is built.
    pub(crate) async fn table_exists(&self, table: &TableIdent) -> Result<bool, ConnectorError> {
        const OPERATION: &str = "check table existence";
        let binding = self.binding().await?;
        let (method, success): (Method, &[u16]) = if binding.table_head_advertised() {
            (Method::HEAD, &[200, 204])
        } else {
            (Method::GET, &[200])
        };
        match self
            .send(
                OPERATION,
                &binding,
                method,
                binding.table_url(table),
                None,
                success,
            )
            .await?
        {
            // Dropped unread: existence is the status alone.
            Answer::Success { .. } => Ok(true),
            Answer::Status(404) => Ok(false),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Load one view's metadata; a missing view is `NotFound`.
    pub(crate) async fn load_view(
        &self,
        view: &TableIdent,
    ) -> Result<ViewMetadata, ConnectorError> {
        const OPERATION: &str = "load view";
        let binding = self.binding().await?;
        match self
            .get(OPERATION, &binding, binding.view_url(view), None, &[200])
            .await?
        {
            Answer::Success { body, .. } => {
                let response: LoadViewResult = decode_document(OPERATION, body).await?;
                Ok(response.metadata)
            }
            Answer::Status(404) => Err(not_found(OPERATION)),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Whether `view` exists (`HEAD`).
    pub(crate) async fn view_exists(&self, view: &TableIdent) -> Result<bool, ConnectorError> {
        const OPERATION: &str = "check view existence";
        let binding = self.binding().await?;
        match self
            .send(
                OPERATION,
                &binding,
                Method::HEAD,
                binding.view_url(view),
                None,
                &[200, 204],
            )
            .await?
        {
            Answer::Success { .. } => Ok(true),
            Answer::Status(404) => Ok(false),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Renew through the server-provided credentials endpoint: authenticated,
    /// without the delegation header, decoding `storage-credentials` only.
    /// Every request-layer failure is retryable before progress (L17).
    pub(crate) async fn load_credentials(
        &self,
        endpoint: &str,
    ) -> Result<RestDelegation, ConnectorError> {
        const OPERATION: &str = "load credentials";
        let binding = self.binding().await.map_err(retryable)?;
        match self
            .get(OPERATION, &binding, endpoint.to_string(), None, &[200])
            .await
            .map_err(retryable)?
        {
            Answer::Success { body, .. } => {
                let view = scan_body(OPERATION, body, CredentialsSink::new())
                    .await
                    .map_err(retryable)?;
                Ok(RestDelegation::new(view.into_credentials()))
            }
            Answer::Status(code) => Err(retryable(status_error(OPERATION, code))),
        }
    }

    /// Acquire credentials through a delegated table load, decoding only the
    /// table UUID and `storage-credentials`. Every request-layer failure is
    /// retryable before progress (L17).
    pub(crate) async fn load_table_credentials(
        &self,
        table: &TableIdent,
    ) -> Result<(Option<String>, RestDelegation), ConnectorError> {
        const OPERATION: &str = "acquire table credentials";
        let binding = self.binding().await.map_err(retryable)?;
        match self
            .get(
                OPERATION,
                &binding,
                binding.table_url(table),
                Some(VENDED_CREDENTIALS),
                &[200],
            )
            .await
            .map_err(retryable)?
        {
            Answer::Success { body, .. } => {
                let view = scan_body(OPERATION, body, BackendAcquisitionSink::new())
                    .await
                    .map_err(retryable)?;
                let (uuid, credentials) = view.into_parts();
                Ok((uuid, RestDelegation::new(credentials)))
            }
            Answer::Status(code) => Err(retryable(status_error(OPERATION, code))),
        }
    }

    async fn get(
        &self,
        operation: &'static str,
        binding: &SessionBinding,
        url: String,
        delegation: Option<HeaderValue>,
        success: &[u16],
    ) -> Result<Answer, ConnectorError> {
        self.send(operation, binding, Method::GET, url, delegation, success)
            .await
    }

    /// Send one NovaRocks read with the session's headers and this role's
    /// `Authorization`, exactly once.
    async fn send(
        &self,
        operation: &'static str,
        binding: &SessionBinding,
        method: Method,
        url: String,
        delegation: Option<HeaderValue>,
        success: &[u16],
    ) -> Result<Answer, ConnectorError> {
        let mut headers = binding.request_headers(delegation);
        self.owner
            .authorize(&mut headers)
            .await
            .map_err(|failure| {
                unavailable(format!(
                    "Iceberg REST {operation} failed: no catalog token ({:?})",
                    failure.step
                ))
            })?;
        let (stage, body) = self
            .owner
            .http()
            .send_once(
                Ok(RestRequest::new(method, url, headers)),
                success,
                &RequestContext::default(),
            )
            .await;
        match (stage, body) {
            (DispatchStage::Status { code, .. }, Some(body)) => Ok(Answer::Success { code, body }),
            (DispatchStage::Status { code, .. }, None) => Ok(Answer::Status(code)),
            (DispatchStage::NotWritten(cause), _) => Err(unavailable(format!(
                "Iceberg REST {operation} failed before the request was written ({cause:?})"
            ))),
            (DispatchStage::WrittenNoStatus(cause), _) => Err(unavailable(format!(
                "Iceberg REST {operation} failed without a response ({cause:?})"
            ))),
        }
    }
}

/// Append the single-page parameters exactly as upstream encodes query pairs.
fn page_url(
    base: String,
    page_token: Option<&str>,
    page_size: usize,
    operation: &'static str,
) -> Result<String, ConnectorError> {
    if page_size == 0 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("Iceberg REST {operation} page size must be non-zero"),
        ));
    }
    let mut url = url::Url::parse(&base).map_err(|_| {
        unavailable(format!(
            "Iceberg REST {operation} failed: invalid catalog URL"
        ))
    })?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("pageSize", &page_size.to_string());
        if let Some(token) = page_token {
            pairs.append_pair("pageToken", token);
        }
    }
    Ok(url.into())
}

/// Read a complete document and decode it once (L8). The decoder's error is
/// withheld: it can quote response values.
async fn decode_document<T: DeserializeOwned>(
    operation: &'static str,
    body: ResponseBody,
) -> Result<T, ConnectorError> {
    let bytes = body.read_to_end().await.map_err(|error| {
        unavailable(format!(
            "Iceberg REST {operation} failed while reading the response ({error:?})"
        ))
    })?;
    serde_json::from_slice(&bytes).map_err(|_| {
        unavailable(format!(
            "Iceberg REST {operation} returned a response that could not be decoded"
        ))
    })
}

/// Scan a body chunk by chunk into a minimal view, never holding the raw
/// document (L8).
async fn scan_body<S: ViewSink>(
    operation: &'static str,
    mut body: ResponseBody,
    sink: S,
) -> Result<S::View, ConnectorError> {
    let mut scanner = JsonScanner::new(sink);
    while let Some(chunk) = body.next_chunk().await.map_err(|error| {
        unavailable(format!(
            "Iceberg REST {operation} failed while reading the response ({error:?})"
        ))
    })? {
        scanner.feed(&chunk).map_err(|error| {
            unavailable(format!(
                "Iceberg REST {operation} returned an invalid response ({error:?})"
            ))
        })?;
    }
    scanner.finish().map_err(|error| {
        unavailable(format!(
            "Iceberg REST {operation} returned an invalid response ({error:?})"
        ))
    })
}

fn status_error(operation: &'static str, code: u16) -> ConnectorError {
    unavailable(format!("Iceberg REST {operation} failed with HTTP {code}"))
}

fn not_found(operation: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::NotFound,
        format!("Iceberg REST {operation} failed: the object does not exist"),
    )
}

fn unavailable(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

fn retryable(error: ConnectorError) -> ConnectorError {
    error.with_retryable_before_progress()
}

#[cfg(test)]
mod tests {
    //! NovaRocks read endpoints against a scripted server, through the real
    //! upstream session: request split (L27), one session per generation (L2,
    //! L28), the delegation header per call site (L23) and read error classes
    //! (L17). Each test owns its loopback server; Drop joins it.
    use std::collections::HashMap;
    use std::sync::Arc;

    use novarocks_spi::connector::ConnectorErrorKind;

    use super::RestEndpoints;
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use crate::iceberg_catalog_rest::RestCatalog;
    use crate::rest_protocol::test_server::{
        RecordedRequest, Reply, Script, TestServer, oauth2_identity,
    };

    const DELEGATION: &str = "x-iceberg-access-delegation";
    const TABLE_HEAD: &str = "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}";

    async fn generation(
        server: &TestServer,
        extra: &[(&str, &str)],
        identity: Option<crate::access_binding::IcebergRestAuthMaterial>,
    ) -> (Arc<RestCatalog>, Arc<RestEndpoints>) {
        let mut properties: HashMap<String, String> = extra
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        properties.insert("uri".to_string(), server.url());
        crate::catalog_runtime::build_vended_rest_generation_for_test(
            properties,
            identity,
            crate::iceberg::Runtime::try_current().expect("test runtime"),
        )
        .await
        .expect("REST generation")
    }

    fn table(name: &str) -> TableIdent {
        TableIdent::new(NamespaceIdent::new("db".to_string()), name.to_string())
    }

    fn table_metadata_json(properties: &[(&str, &str)]) -> serde_json::Value {
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
            Type, UnboundPartitionSpec,
        };
        let metadata = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "s3://warehouse/db/t".into(),
            FormatVersion::V2,
            properties
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        serde_json::to_value(metadata).unwrap()
    }

    fn load_response(credentials: bool) -> serde_json::Value {
        let mut response = serde_json::json!({
            "metadata-location": "s3://warehouse/db/t/metadata/00001.metadata.json",
            "metadata": table_metadata_json(&[]),
        });
        if credentials {
            response["storage-credentials"] = serde_json::json!([{
                "prefix": "s3://warehouse/db/t",
                "config": {"s3.access-key-id": "AKIA-canary"},
            }]);
        }
        response
    }

    fn non_session(requests: Vec<RecordedRequest>) -> Vec<RecordedRequest> {
        requests
            .into_iter()
            .filter(|request| {
                request.path() != "/v1/config" && request.path() != "/v1/oauth/tokens"
            })
            .collect()
    }

    #[tokio::test]
    async fn the_first_novarocks_request_establishes_the_session_without_a_probe_request() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("POST", "/v1/oauth/tokens", Reply::token("fe-token"))
                .on("GET", "/v1/namespaces/db/tables/t", Reply::status(404)),
        );
        let (_upstream, endpoints) =
            generation(&server, &[], Some(oauth2_identity("fe-control", "secret"))).await;

        assert!(
            !endpoints
                .table_exists(&table("t"))
                .await
                .expect("existence")
        );

        let requests = server.requests();
        let lines: Vec<_> = requests
            .iter()
            .map(|request| format!("{} {}", request.method, request.path()))
            .collect();
        assert_eq!(
            lines,
            [
                "POST /v1/oauth/tokens",
                "GET /v1/config",
                "GET /v1/namespaces/db/tables/t"
            ],
            "the probe is vetoed and only the session handshake precedes the read"
        );
        assert!(
            requests
                .iter()
                .all(|request| !request.target.contains("__novarocks_probe_"))
        );
        let existence = &requests[2];
        assert_eq!(existence.header("authorization"), Some("Bearer fe-token"));
        assert_eq!(existence.header(DELEGATION), None);
    }

    #[tokio::test]
    async fn table_existence_uses_head_only_when_advertised_and_never_delegates() {
        for advertised in [false, true] {
            let endpoints_list: &[&str] = if advertised { &[TABLE_HEAD] } else { &[] };
            let server = TestServer::start(
                Script::new()
                    .on(
                        "GET",
                        "/v1/config",
                        Reply::config(&[], &[], advertised.then_some(endpoints_list)),
                    )
                    .on("HEAD", "/v1/namespaces/db/tables/t", Reply::status(204))
                    // A body no client could decode: existence must not read it.
                    .on(
                        "GET",
                        "/v1/namespaces/db/tables/t",
                        Reply::bytes(200, b"<html>not metadata</html>".to_vec()),
                    ),
            );
            let (_upstream, endpoints) = generation(&server, &[], None).await;

            assert!(
                endpoints
                    .table_exists(&table("t"))
                    .await
                    .expect("existence")
            );

            let reads = non_session(server.requests());
            assert_eq!(reads.len(), 1, "{advertised}");
            assert_eq!(reads[0].method, if advertised { "HEAD" } else { "GET" });
            assert_eq!(reads[0].header(DELEGATION), None);
            assert_eq!(
                reads[0].header("authorization"),
                None,
                "no identity, no token"
            );
        }
    }

    #[tokio::test]
    async fn existence_of_a_table_whose_metadata_needs_kms_is_a_status_fact() {
        // A v3 table with `encryption.key-id` cannot become a table object
        // without a KMS; existence must not need one (L27).
        let mut response = load_response(false);
        response["metadata"] = table_metadata_json(&[("encryption.key-id", "kms-key")]);
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    Reply::json(200, &response),
                ),
        );
        let (_upstream, endpoints) = generation(&server, &[], None).await;

        assert!(
            endpoints
                .table_exists(&table("t"))
                .await
                .expect("existence")
        );
    }

    #[tokio::test]
    async fn delegated_loads_send_the_delegation_header_and_other_reads_do_not() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    Reply::json(200, &load_response(true)),
                )
                .on(
                    "GET",
                    "/credentials",
                    Reply::json(
                        200,
                        &serde_json::json!({"storage-credentials": [{
                            "prefix": "s3://warehouse/db/t",
                            "config": {"s3.access-key-id": "AKIA-canary"},
                        }]}),
                    ),
                ),
        );
        let (_upstream, endpoints) = generation(&server, &[], None).await;

        let (frozen, delegation) = endpoints.load_table(&table("t"), true).await.expect("load");
        assert_eq!(delegation.len(), 1);
        assert!(!format!("{frozen:?}{delegation:?}").contains("AKIA-canary"));
        endpoints
            .load_table(&table("t"), false)
            .await
            .expect("plain load");
        let credentials = endpoints
            .load_credentials(&format!("{}/credentials", server.url()))
            .await
            .expect("credentials");
        assert_eq!(credentials.len(), 1);
        let (uuid, acquired) = endpoints
            .load_table_credentials(&table("t"))
            .await
            .expect("acquisition");
        assert!(uuid.is_some());
        assert_eq!(acquired.len(), 1);

        let reads = non_session(server.requests());
        let delegated: Vec<_> = reads
            .iter()
            .map(|request| {
                (
                    request.path().to_string(),
                    request.header(DELEGATION).map(str::to_string),
                )
            })
            .collect();
        assert_eq!(
            delegated,
            [
                (
                    "/v1/namespaces/db/tables/t".to_string(),
                    Some("vended-credentials".to_string())
                ),
                ("/v1/namespaces/db/tables/t".to_string(), None),
                ("/credentials".to_string(), None),
                (
                    "/v1/namespaces/db/tables/t".to_string(),
                    Some("vended-credentials".to_string())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn listing_sends_one_page_with_page_size_and_never_a_parent() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces",
                    Reply::json(200, &serde_json::json!({"namespaces": [["db"]]})),
                )
                .on(
                    "GET",
                    "/v1/namespaces/db/tables",
                    Reply::json(200, &serde_json::json!({"identifiers": []})),
                )
                .on(
                    "GET",
                    "/v1/namespaces/db/views",
                    Reply::json(200, &serde_json::json!({"identifiers": []})),
                ),
        );
        let (_upstream, endpoints) = generation(&server, &[], None).await;
        let db = NamespaceIdent::new("db".to_string());

        endpoints
            .list_namespaces_page(Some("a b&c"), 10)
            .await
            .expect("namespaces");
        endpoints
            .list_tables_page(&db, None, 7)
            .await
            .expect("tables");
        endpoints
            .list_views_page(&db, Some("t1"), 3)
            .await
            .expect("views");

        let queries: Vec<_> = non_session(server.requests())
            .iter()
            .map(|request| request.query().map(str::to_string))
            .collect();
        assert_eq!(
            queries,
            [
                Some("pageSize=10&pageToken=a+b%26c".to_string()),
                Some("pageSize=7".to_string()),
                Some("pageSize=3&pageToken=t1".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn read_error_classes_stay_as_today_and_view_404_is_not_found() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("GET", "/v1/namespaces/db/views/v", Reply::status(404))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/missing",
                    Reply::status(404),
                )
                .on("GET", "/v1/namespaces/db/tables/t", Reply::status(503))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/moved",
                    Reply::redirect(307, "/elsewhere"),
                ),
        );
        let (_upstream, endpoints) = generation(&server, &[], None).await;

        let view = endpoints
            .load_view(&table("v"))
            .await
            .expect_err("missing view");
        assert_eq!(view.kind(), ConnectorErrorKind::NotFound);
        let missing = endpoints
            .load_table(&table("missing"), true)
            .await
            .expect_err("missing");
        assert_eq!(missing.kind(), ConnectorErrorKind::NotFound);
        let unavailable = endpoints
            .load_table(&table("t"), true)
            .await
            .expect_err("503");
        assert_eq!(unavailable.kind(), ConnectorErrorKind::Unavailable);
        assert!(!unavailable.retryable_before_progress());
        let moved = endpoints
            .load_table(&table("moved"), true)
            .await
            .expect_err("307");
        assert_eq!(moved.kind(), ConnectorErrorKind::Unavailable);
        assert_eq!(
            server.count("GET", "/elsewhere"),
            0,
            "redirects are never followed"
        );
    }

    #[tokio::test]
    async fn renewal_request_failures_are_retryable_before_progress() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("GET", "/credentials", Reply::status(401))
                .on("GET", "/v1/namespaces/db/tables/t", Reply::status(401)),
        );
        let (_upstream, endpoints) = generation(&server, &[], None).await;

        let credentials = endpoints
            .load_credentials(&format!("{}/credentials", server.url()))
            .await
            .expect_err("401");
        assert_eq!(credentials.kind(), ConnectorErrorKind::Unavailable);
        assert!(credentials.retryable_before_progress());
        let acquisition = endpoints
            .load_table_credentials(&table("t"))
            .await
            .expect_err("401");
        assert!(acquisition.retryable_before_progress());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_session_serves_upstream_and_novarocks_requests_after_config_changes() {
        let server = TestServer::start(
            Script::new()
                .on_sequence(
                    "GET",
                    "/v1/config",
                    vec![
                        Reply::config(&[("prefix", "p1")], &[], None),
                        Reply::config(&[("prefix", "p2")], &[], None),
                    ],
                )
                .on(
                    "GET",
                    "/v1/p1/namespaces/db",
                    Reply::json(
                        200,
                        &serde_json::json!({"namespace": ["db"], "properties": {}}),
                    ),
                )
                .on("GET", "/v1/p1/namespaces/db/tables/t", Reply::status(404))
                .on(
                    "GET",
                    "/v1/p1/namespaces/db/tables",
                    Reply::json(200, &serde_json::json!({"identifiers": []})),
                ),
        );
        let (_upstream, endpoints) = generation(&server, &[], None).await;
        let db = NamespaceIdent::new("db".to_string());

        // Cold session: an upstream read and a NovaRocks read race.
        let upstream_read = async {
            let upstream = endpoints.upstream().await.expect("session");
            crate::iceberg::Catalog::namespace_exists(upstream, &db)
                .await
                .expect("namespace")
        };
        let probe_table = table("t");
        let (namespace, exists) = tokio::join!(upstream_read, endpoints.table_exists(&probe_table));
        assert!(namespace);
        assert!(!exists.expect("existence"));
        // Later reads keep the first binding.
        endpoints
            .list_tables_page(&db, None, 5)
            .await
            .expect("tables");
        let upstream = endpoints.upstream().await.expect("session");
        assert!(
            crate::iceberg::Catalog::namespace_exists(upstream, &db)
                .await
                .expect("namespace")
        );

        assert_eq!(server.count("GET", "/v1/config"), 1);
        let paths: Vec<_> = non_session(server.requests())
            .iter()
            .map(|request| request.path().to_string())
            .collect();
        assert!(!paths.is_empty());
        assert!(
            paths.iter().all(|path| path.starts_with("/v1/p1/")),
            "{paths:?}"
        );
    }

    #[tokio::test]
    async fn novarocks_and_upstream_loads_are_the_same_request_but_for_delegation() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("POST", "/v1/oauth/tokens", Reply::token("fe-token"))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    Reply::json(200, &load_response(false)),
                ),
        );
        let (_upstream, endpoints) = generation(
            &server,
            &[("header.x-tenant", "t1")],
            Some(oauth2_identity("fe-control", "secret")),
        )
        .await;

        endpoints
            .load_table(&table("t"), true)
            .await
            .expect("NovaRocks load");
        let upstream = endpoints.upstream().await.expect("session");
        // Upstream materializes with a storage factory this generation does
        // not have; only its request matters here.
        let _ = crate::iceberg::Catalog::load_table(upstream, &table("t")).await;

        let loads = server.requests_to("GET", "/v1/namespaces/db/tables/t");
        assert_eq!(loads.len(), 2);
        let mut novarocks = loads[0].header_map();
        let upstream = loads[1].header_map();
        assert_eq!(
            novarocks.remove(DELEGATION).as_deref(),
            Some("vended-credentials")
        );
        for name in [
            "authorization",
            "x-tenant",
            "content-type",
            "x-client-version",
            "user-agent",
        ] {
            assert_eq!(novarocks.get(name), upstream.get(name), "{name}");
        }
        assert_eq!(loads[0].target, loads[1].target);
        assert_eq!(server.count("POST", "/v1/oauth/tokens"), 1);
    }
}
