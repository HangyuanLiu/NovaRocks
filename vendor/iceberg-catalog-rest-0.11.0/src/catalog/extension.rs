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

use iceberg::TableUpdate;

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

#[cfg(test)]
mod tests {
    //! Credential-handoff correctness coverage: exact-key reads preserve the
    //! response while Debug never reveals values. This isolated in-memory test
    //! follows the closed handoff contract and runs with the REST crate suite;
    //! the NovaRocks connector maintainer owns it until IRU-13a replaces R6.
    use super::*;

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
}
