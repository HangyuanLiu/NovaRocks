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

/// Acquires data credentials from a catalog-advertised credentials endpoint
/// using this process's own catalog principal.
///
/// The client is built once and retained: a process-lived authority that
/// rebuilt its client on every renewal would also rebuild its OAuth2 token
/// exchange, turning one storage refresh into two remote round trips.
pub(crate) struct ExecutionNodeCredentialsEndpointRefresher {
    catalog_name: String,
    non_secret_properties: Vec<(String, String)>,
    principal: IcebergRestAuthMaterial,
    runtime: IcebergCatalogRuntime,
    endpoint: Arc<str>,
    client: Mutex<Option<Arc<crate::iceberg_catalog_rest::RestCatalog>>>,
}

impl std::fmt::Debug for ExecutionNodeCredentialsEndpointRefresher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionNodeCredentialsEndpointRefresher")
            .field("catalog_name", &self.catalog_name)
            .field("endpoint", &self.endpoint.as_ref())
            .finish_non_exhaustive()
    }
}

impl ExecutionNodeCredentialsEndpointRefresher {
    pub(crate) fn new(
        catalog_name: String,
        non_secret_properties: Vec<(String, String)>,
        principal: IcebergRestAuthMaterial,
        runtime: IcebergCatalogRuntime,
        endpoint: Arc<str>,
    ) -> Self {
        Self {
            catalog_name,
            non_secret_properties,
            principal,
            runtime,
            endpoint,
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
        let endpoint = Arc::clone(&self.endpoint);
        let delegation = crate::loaded_table::run_vended_refresh_with_policy(
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
        )?;
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
