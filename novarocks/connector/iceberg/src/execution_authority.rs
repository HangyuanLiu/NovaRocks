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

//! The execution node's own acquisition path for vended data credentials.
//!
//! CAD-1 D1 puts the catalog identity on the node that consumes the material:
//! an execution node authenticates to the REST control plane as itself and
//! exchanges that identity for storage credentials, instead of waiting for a
//! coordinator to push material it cannot renew.
//!
//! Everything here is one adapter over the existing REST catalog client. The
//! acquisition budget, retry shape and failure classification are deliberately
//! *not* duplicated: they belong to [`crate::authority_source`], which wraps
//! this refresher.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorVendedS3CredentialLeaseRefresher,
    VendedS3CredentialLeaseRefresh, VendedS3CredentialRefreshCallPolicy,
};

use crate::access_binding::IcebergRestAuthMaterial;
use crate::catalog_config::{IcebergCatalogKind, parse_catalog_configuration};
use crate::catalog_runtime::RestAccessDelegationMode;
use crate::loaded_table::{IcebergAccessDelegation, parse_vended_access_delegation};
use crate::resources::IcebergCatalogRuntime;

/// REST property carrying an OAuth2 client credential pair.
const REST_PROP_CREDENTIAL: &str = "credential";
/// REST property carrying a bearer token.
const REST_PROP_TOKEN: &str = "token";

/// Which of the two advertised shapes this node acquires along.
///
/// A consumer selects one and never probes the other after a failure: the
/// catalog stated which path it serves, and trying the other would assert a
/// capability the deployment may not have (CAD-1 D2).
#[derive(Clone, Debug)]
pub(crate) enum ExecutionNodeAcquisitionPath {
    /// The scope's credentials are served from this address.
    CredentialsEndpoint(Arc<str>),
    /// The catalog vends only inside a load-table response for this table.
    LoadTableDelegation {
        table: crate::iceberg::TableIdent,
        expected_table_uuid: uuid::Uuid,
    },
}

impl ExecutionNodeAcquisitionPath {
    /// Project one announced path into the provider's own vocabulary.
    ///
    /// Fallible on purpose, and resolved before the authority is formed: an
    /// announcement this node cannot parse is a disagreement about the table,
    /// and an acquisition that proceeded with a degraded identity would have
    /// nothing meaningful left to compare a response against.
    pub(crate) fn project(
        announced: &novarocks_spi::connector::CredentialRenewalPath,
    ) -> Result<Self, ConnectorError> {
        match announced {
            novarocks_spi::connector::CredentialRenewalPath::CredentialsEndpoint(endpoint) => {
                Ok(Self::CredentialsEndpoint(Arc::clone(endpoint)))
            }
            novarocks_spi::connector::CredentialRenewalPath::LoadTableDelegation(delegation) => {
                let namespace = crate::iceberg::NamespaceIdent::from_vec(
                    delegation
                        .namespace()
                        .iter()
                        .map(|level| level.as_ref().to_string())
                        .collect(),
                )
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("announced load-table namespace is not usable: {error}"),
                    )
                })?;
                let expected_table_uuid =
                    uuid::Uuid::parse_str(delegation.table_uuid()).map_err(|error| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            format!("announced load-table uuid is not a uuid: {error}"),
                        )
                    })?;
                Ok(Self::LoadTableDelegation {
                    table: crate::iceberg::TableIdent::new(
                        namespace,
                        delegation.table().to_string(),
                    ),
                    expected_table_uuid,
                })
            }
        }
    }
}

/// Acquires data credentials along one advertised path using this process's
/// own catalog principal.
///
/// The client is built once and retained: a process-lived authority that
/// rebuilt its client on every renewal would also rebuild its OAuth2 token
/// exchange, turning one storage refresh into two remote round trips.
pub(crate) struct ExecutionNodeCredentialsEndpointRefresher {
    catalog_name: String,
    non_secret_properties: Vec<(String, String)>,
    principal: IcebergRestAuthMaterial,
    runtime: IcebergCatalogRuntime,
    path: ExecutionNodeAcquisitionPath,
    client: Mutex<Option<Arc<crate::iceberg_catalog_rest::RestCatalog>>>,
}

impl std::fmt::Debug for ExecutionNodeCredentialsEndpointRefresher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut rendered = formatter.debug_struct("ExecutionNodeCredentialsEndpointRefresher");
        rendered.field("catalog_name", &self.catalog_name);
        match &self.path {
            ExecutionNodeAcquisitionPath::CredentialsEndpoint(endpoint) => {
                rendered.field("endpoint", &endpoint.as_ref());
            }
            ExecutionNodeAcquisitionPath::LoadTableDelegation { table, .. } => {
                rendered.field("load_table", &table.name());
            }
        }
        rendered.finish_non_exhaustive()
    }
}

impl ExecutionNodeCredentialsEndpointRefresher {
    pub(crate) fn new(
        catalog_name: String,
        non_secret_properties: Vec<(String, String)>,
        principal: IcebergRestAuthMaterial,
        runtime: IcebergCatalogRuntime,
        path: ExecutionNodeAcquisitionPath,
    ) -> Self {
        Self {
            catalog_name,
            non_secret_properties,
            principal,
            runtime,
            path,
            client: Mutex::new(None),
        }
    }

    /// The frozen catalog definition with this node's own identity added.
    ///
    /// The definition that crossed the wire is non-secret by construction, so
    /// the principal is never read from it; it comes from the role-local
    /// registry and is joined here, in the one place that builds a client.
    fn client_properties(&self) -> Result<HashMap<String, String>, ConnectorError> {
        let configuration = parse_catalog_configuration(
            &self.catalog_name,
            &self.non_secret_properties,
        )
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("parse Iceberg catalog definition for credential acquisition: {error}"),
            )
        })?;
        if configuration.kind != IcebergCatalogKind::Rest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "credential acquisition requires a REST Iceberg catalog definition",
            ));
        }
        let uri = configuration.rest_uri.clone().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "REST Iceberg catalog definition has no uri for credential acquisition",
            )
        })?;
        let mut properties = configuration
            .properties
            .iter()
            .filter(|(key, _)| key != "type")
            .cloned()
            .collect::<HashMap<_, _>>();
        properties.insert(
            crate::iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_string(),
            uri,
        );
        if !configuration.warehouse_uri.is_empty() {
            properties.insert(
                crate::iceberg_catalog_rest::REST_CATALOG_PROP_WAREHOUSE.to_string(),
                configuration.warehouse_uri.clone(),
            );
        }
        match &self.principal {
            IcebergRestAuthMaterial::Oauth2 {
                client_id,
                client_secret,
            } => {
                properties.remove(REST_PROP_TOKEN);
                properties.insert(
                    REST_PROP_CREDENTIAL.to_string(),
                    format!("{client_id}:{}", client_secret.expose_secret()),
                );
            }
            IcebergRestAuthMaterial::Bearer { token } => {
                properties.remove(REST_PROP_CREDENTIAL);
                properties.insert(
                    REST_PROP_TOKEN.to_string(),
                    token.expose_secret().to_string(),
                );
            }
        }
        Ok(properties)
    }

    fn client(&self) -> Result<Arc<crate::iceberg_catalog_rest::RestCatalog>, ConnectorError> {
        let mut slot = self
            .client
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(client) = slot.as_ref() {
            return Ok(Arc::clone(client));
        }
        let properties = self.client_properties()?;
        // `Vended` deliberately installs no warehouse storage factory: this
        // client speaks to the control plane only, and the material it returns
        // is installed into the authority rather than into a FileIO.
        let built = self
            .runtime
            .block_on(crate::catalog_runtime::build_rest_catalog_from_properties(
                properties,
                RestAccessDelegationMode::Vended,
            ))
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("run Iceberg credential-acquisition client build: {error}"),
                )
            })?
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error)
                    .with_retryable_before_progress()
            })?;
        let client = Arc::new(built);
        *slot = Some(Arc::clone(&client));
        Ok(client)
    }
}

impl ConnectorVendedS3CredentialLeaseRefresher for ExecutionNodeCredentialsEndpointRefresher {
    fn refresh_vended_s3_credentials(
        &self,
        policy: VendedS3CredentialRefreshCallPolicy,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        let client = self.client()?;
        let delegation = match &self.path {
            ExecutionNodeAcquisitionPath::CredentialsEndpoint(endpoint) => {
                let endpoint = Arc::clone(endpoint);
                crate::loaded_table::run_vended_refresh_with_policy(
                    &self.runtime,
                    policy,
                    "Iceberg execution-node credential acquisition",
                    move || {
                        let client = Arc::clone(&client);
                        let endpoint = Arc::clone(&endpoint);
                        async move {
                            client
                                .load_credentials_with_access_delegation(endpoint.as_ref())
                                .await
                        }
                    },
                )?
            }
            ExecutionNodeAcquisitionPath::LoadTableDelegation {
                table,
                expected_table_uuid,
            } => {
                let table_ident = table.clone();
                let response = crate::loaded_table::run_vended_refresh_with_policy(
                    &self.runtime,
                    policy,
                    "Iceberg execution-node load-table credential acquisition",
                    move || {
                        let client = Arc::clone(&client);
                        let table = table_ident.clone();
                        async move {
                            client
                                .load_table_deferred_with_access_delegation(&table)
                                .await
                        }
                    },
                )?;
                // D11b: the response's metadata is destructured here and never
                // leaves this expression. Only the identity check reads it, and
                // it reads exactly one field; the materialization is dropped at
                // the end of this block, so no plan, cache or table state on
                // this node can observe the coordinator's frozen snapshot being
                // overtaken.
                let (materialization, delegation) = response.into_parts();
                let observed = crate::loaded_table::table_uuid_for_identity_check(materialization)?;
                if observed != *expected_table_uuid {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::PermissionDenied,
                        "load-table credential acquisition answered for a different table",
                    ));
                }
                delegation
            }
        };
        // No prefix-scope equality check here, unlike the coordinator's
        // refresher. That check exists to keep one admitted attempt's authority
        // from being widened mid-flight; this node holds one authority per
        // scope and `IcebergAuthorityMaterialSource` already refuses a response
        // that does not cover the exact scope it owns (CAD-1 D7).
        let seed = match parse_vended_access_delegation(&delegation)? {
            IcebergAccessDelegation::Vended(seed) => seed,
            IcebergAccessDelegation::Static => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::PermissionDenied,
                    "credential acquisition returned static access delegation",
                ));
            }
        };
        let (entries, _) = seed
            .into_vended_s3_credential_lease_contribution()?
            .into_parts();
        VendedS3CredentialLeaseRefresh::try_new(entries)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use novarocks_spi::connector::{
        CredentialLoadTableDelegation, CredentialRenewalPath, VendedS3CredentialRefreshDispatch,
        VendedS3CredentialRefreshDispatchGuard,
    };

    use super::*;

    const TABLE_UUID: &str = "8f1d0c6e-0000-4000-8000-000000000001";
    const OTHER_TABLE_UUID: &str = "8f1d0c6e-0000-4000-8000-0000000000ff";

    struct PermittedDispatch;

    impl VendedS3CredentialRefreshDispatchGuard for PermittedDispatch {
        fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
            VendedS3CredentialRefreshDispatch::Permitted
        }
    }

    /// One Iceberg format-2 table, the smallest a REST response may carry.
    fn table_metadata(uuid: &str) -> String {
        format!(
            r#"{{
              "format-version": 2,
              "table-uuid": "{uuid}",
              "location": "s3://warehouse/sales/orders",
              "last-sequence-number": 1,
              "last-updated-ms": 1700000000000,
              "last-column-id": 1,
              "current-schema-id": 0,
              "schemas": [{{"type":"struct","schema-id":0,"fields":[
                {{"id":1,"name":"id","required":false,"type":"long"}}
              ]}}],
              "default-spec-id": 0,
              "partition-specs": [{{"spec-id":0,"fields":[]}}],
              "last-partition-id": 999,
              "default-sort-order-id": 0,
              "sort-orders": [{{"order-id":0,"fields":[]}}],
              "properties": {{}},
              "current-snapshot-id": -1,
              "snapshots": [],
              "snapshot-log": [],
              "metadata-log": []
            }}"#
        )
    }

    fn storage_credentials(prefix: &str, not_after_unix_ms: u64) -> String {
        format!(
            r#""storage-credentials": [{{
              "prefix": "{prefix}",
              "config": {{
                "s3.access-key-id": "vended-access",
                "s3.secret-access-key": "vended-secret",
                "s3.session-token": "vended-token",
                "s3.session-token-expires-at-ms": "{not_after_unix_ms}"
              }}
            }}]"#
        )
    }

    /// A REST catalog that answers exactly the two calls an acquisition makes.
    ///
    /// It records the Authorization header of every request, which is how the
    /// test observes *whose* identity the acquisition used — the point of D1,
    /// and the thing an implementation could silently get wrong while still
    /// returning material.
    struct ScriptedRestCatalog {
        address: SocketAddr,
        shutdown: Arc<AtomicBool>,
        authorizations: Arc<Mutex<Vec<String>>>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl ScriptedRestCatalog {
        fn start(table_uuid: String, not_after_unix_ms: u64) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind scripted REST catalog");
            listener
                .set_nonblocking(true)
                .expect("make scripted REST catalog nonblocking");
            let address = listener.local_addr().expect("read scripted REST address");
            let shutdown = Arc::new(AtomicBool::new(false));
            let authorizations = Arc::new(Mutex::new(Vec::new()));
            let thread_shutdown = Arc::clone(&shutdown);
            let thread_authorizations = Arc::clone(&authorizations);
            let thread = std::thread::spawn(move || {
                while !thread_shutdown.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let _ = stream.set_nonblocking(false);
                            let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                            let mut buffer = [0_u8; 4096];
                            let read = stream.read(&mut buffer).unwrap_or(0);
                            if read == 0 {
                                continue;
                            }
                            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                            for line in request.lines() {
                                if let Some(value) = line
                                    .strip_prefix("authorization: ")
                                    .or_else(|| line.strip_prefix("Authorization: "))
                                {
                                    thread_authorizations
                                        .lock()
                                        .unwrap_or_else(|error| error.into_inner())
                                        .push(value.trim().to_string());
                                }
                            }
                            let body = if request.contains("/v1/config") {
                                r#"{"defaults":{},"overrides":{}}"#.to_string()
                            } else if request.contains("/credentials") {
                                format!(
                                    "{{{}}}",
                                    storage_credentials(
                                        "s3://warehouse/sales/orders",
                                        not_after_unix_ms
                                    )
                                )
                            } else {
                                format!(
                                    r#"{{"metadata-location":"s3://warehouse/sales/orders/metadata/v1.json","metadata":{},{}}}"#,
                                    table_metadata(&table_uuid),
                                    storage_credentials(
                                        "s3://warehouse/sales/orders",
                                        not_after_unix_ms
                                    )
                                )
                            };
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = stream.write_all(response.as_bytes());
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("accept scripted REST connection: {error}"),
                    }
                }
            });
            Self {
                address,
                shutdown,
                authorizations,
                thread: Some(thread),
            }
        }

        fn uri(&self) -> String {
            format!("http://{}", self.address)
        }

        fn authorizations(&self) -> Vec<String> {
            self.authorizations
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
        }
    }

    impl Drop for ScriptedRestCatalog {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                // Never raise from a destructor that is already unwinding: Rust
                // turns that into an abort, which destroys every other test's
                // result and buries the failure being reported.
                assert!(
                    thread.join().is_ok() || std::thread::panicking(),
                    "join scripted REST catalog"
                );
            }
        }
    }

    fn not_after() -> u64 {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("wall clock")
                .as_millis(),
        )
        .expect("wall clock fits")
            + 3_600_000
    }

    fn refresher(
        server: &ScriptedRestCatalog,
        runtime: &tokio::runtime::Runtime,
        path: ExecutionNodeAcquisitionPath,
    ) -> ExecutionNodeCredentialsEndpointRefresher {
        ExecutionNodeCredentialsEndpointRefresher::new(
            "warehouse".to_string(),
            vec![
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), server.uri()),
            ],
            IcebergRestAuthMaterial::Bearer {
                token: novarocks_fs::SecretValue::new("execution-node-token"),
            },
            IcebergCatalogRuntime::new(runtime.handle().clone()),
            path,
        )
    }

    fn policy() -> VendedS3CredentialRefreshCallPolicy {
        VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_secs(10),
            std::num::NonZeroU8::new(1).expect("attempts"),
            Duration::from_millis(10),
            Arc::new(PermittedDispatch),
        )
        .expect("policy")
    }

    fn load_table_path(uuid: &str) -> ExecutionNodeAcquisitionPath {
        ExecutionNodeAcquisitionPath::project(&CredentialRenewalPath::LoadTableDelegation(
            CredentialLoadTableDelegation::try_new(
                vec![Arc::from("sales")],
                Arc::from("orders"),
                Arc::from(uuid),
            )
            .expect("delegation"),
        ))
        .expect("projected path")
    }

    #[test]
    fn an_execution_node_acquires_along_the_endpoint_path_under_its_own_identity() {
        // CAD-1 D1 and C11: the acquisition authenticates as this node, not as
        // whoever seeded the material. Returning credentials is not evidence of
        // that — the Authorization header is.
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let server = ScriptedRestCatalog::start(TABLE_UUID.to_string(), not_after());
        let endpoint = format!("{}/v1/credentials", server.uri());
        let refresher = refresher(
            &server,
            &runtime,
            ExecutionNodeAcquisitionPath::CredentialsEndpoint(Arc::from(endpoint.as_str())),
        );

        let refreshed = refresher
            .refresh_vended_s3_credentials(policy())
            .expect("acquisition");
        let entries = refreshed.into_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].prefix().as_str(), "s3://warehouse/sales/orders");

        let authorizations = server.authorizations();
        assert!(!authorizations.is_empty());
        assert!(
            authorizations
                .iter()
                .all(|value| value == "Bearer execution-node-token"),
            "every catalog call must carry this node's own identity, saw {authorizations:?}"
        );
    }

    #[test]
    fn an_execution_node_acquires_along_the_load_table_path_and_keeps_only_the_material() {
        // CAD-1 acceptance 9 and 17. A catalog with no credentials endpoint
        // still renews, and the response's metadata reaches nothing: the only
        // value that survives the call is the credential entry set.
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let server = ScriptedRestCatalog::start(TABLE_UUID.to_string(), not_after());
        let refresher = refresher(&server, &runtime, load_table_path(TABLE_UUID));

        let refreshed = refresher
            .refresh_vended_s3_credentials(policy())
            .expect("acquisition");
        let entries = refreshed.into_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].prefix().as_str(), "s3://warehouse/sales/orders");
    }

    #[test]
    fn a_load_table_acquisition_refuses_a_response_for_another_table() {
        // The identity check is the whole protection against installing
        // material under an authority it was not issued for.
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let server = ScriptedRestCatalog::start(OTHER_TABLE_UUID.to_string(), not_after());
        let refresher = refresher(&server, &runtime, load_table_path(TABLE_UUID));

        let error = match refresher.refresh_vended_s3_credentials(policy()) {
            Err(error) => error,
            Ok(_) => panic!("a response for another table is not this authority's material"),
        };
        assert_eq!(error.kind(), ConnectorErrorKind::PermissionDenied);
        assert!(error.message().contains("different table"));
    }
}
