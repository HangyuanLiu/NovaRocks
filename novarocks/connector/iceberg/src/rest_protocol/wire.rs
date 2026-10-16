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

//! Provider-owned REST wire vocabulary. Format facts remain in `iceberg`.

use crate::iceberg::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
use crate::iceberg::{Namespace, NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(crate) const REST_CATALOG_PROP_URI: &str = "uri";
pub(crate) const REST_CATALOG_PROP_WAREHOUSE: &str = "warehouse";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Namespace response
pub struct NamespaceResponse {
    /// Namespace identifier
    pub namespace: NamespaceIdent,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    /// Properties stored on the namespace, if supported by the server.
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Create namespace request
pub struct CreateNamespaceRequest {
    /// Name of the namespace to create
    pub namespace: NamespaceIdent,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    /// Properties to set on the namespace
    pub properties: HashMap<String, String>,
}

impl From<&Namespace> for NamespaceResponse {
    fn from(value: &Namespace) -> Self {
        Self {
            namespace: value.name().clone(),
            properties: value.properties().clone(),
        }
    }
}

impl From<NamespaceResponse> for Namespace {
    fn from(value: NamespaceResponse) -> Self {
        Namespace::with_properties(value.namespace, value.properties)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Response containing a list of namespace identifiers, with optional pagination support.
pub struct ListNamespaceResponse {
    /// List of namespace identifiers returned by the server
    pub namespaces: Vec<NamespaceIdent>,
    /// Opaque token for pagination. If present, indicates there are more results available.
    /// Use this value in subsequent requests to retrieve the next page.
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Response containing a list of table identifiers, with optional pagination support.
pub struct ListTablesResponse {
    /// List of table identifiers under the requested namespace
    pub identifiers: Vec<TableIdent>,
    /// Opaque token for pagination. If present, indicates there are more results available.
    /// Use this value in subsequent requests to retrieve the next page.
    #[serde(default)]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Result returned when a table is successfully loaded or created.
///
/// The table metadata JSON is returned in the `metadata` field. The corresponding file location
/// of table metadata should be returned in the `metadata_location` field, unless the metadata
/// is not yet committed. For example, a create transaction may return metadata that is staged
/// but not committed.
///
/// The `config` map returns table-specific configuration for the table's resources, including
/// its HTTP client and FileIO. For example, config may contain a specific FileIO implementation
/// class for the table depending on its underlying storage.
pub struct LoadTableResult {
    /// May be null if the table is staged as part of a transaction
    pub metadata_location: Option<String>,
    /// The table's full metadata
    pub metadata: TableMetadata,
    /// Table-specific configuration overriding catalog configuration
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub config: HashMap<String, String>,
    /// Storage credentials for accessing table data. Clients should check this field
    /// before falling back to credentials in the `config` field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Storage credential for a specific location prefix.
///
/// Indicates a storage location prefix where the credential is relevant. Clients should
/// choose the most specific prefix (by selecting the longest prefix) if several credentials
/// of the same type are available.
pub struct StorageCredential {
    /// Storage location prefix where this credential is relevant
    pub prefix: String,
    /// Configuration map containing credential information
    pub config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Request to create a new table in a namespace.
///
/// If `stage_create` is false, the table is created immediately.
/// If `stage_create` is true, the table is not created, but table metadata is initialized
/// and returned. The service should prepare as needed for a commit to the table commit
/// endpoint to complete the create transaction.
pub struct CreateTableRequest {
    /// Name of the table to create
    pub name: String,
    /// Optional table location. If not provided, the server will choose a location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Table schema
    pub schema: Schema,
    /// Optional partition specification. If not provided, the table will be unpartitioned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<UnboundPartitionSpec>,
    /// Optional sort order for the table
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_order: Option<SortOrder>,
    /// Whether to stage the create for a transaction (true) or create immediately (false)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_create: Option<bool>,
    /// Optional properties to set on the table
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Request to commit updates to a table.
///
/// Commits have two parts: requirements and updates. Requirements are assertions that will
/// be validated before attempting to make and commit changes. Updates are changes to make
/// to table metadata.
///
/// Create table transactions that are started by createTable with `stage-create` set to true
/// are committed using this request. Transactions should include all changes to the table,
/// including table initialization, like AddSchemaUpdate and SetCurrentSchemaUpdate.
pub struct CommitTableRequest {
    /// Table identifier to update; must be present for CommitTransactionRequest
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    /// List of requirements that must be satisfied before committing changes
    pub requirements: Vec<TableRequirement>,
    /// List of updates to apply to the table metadata
    pub updates: Vec<TableUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Response returned when a table is successfully updated.
///
/// The table metadata JSON is returned in the metadata field. The corresponding file location
/// of table metadata must be returned in the metadata-location field. Clients can check whether
/// metadata has changed by comparing metadata locations.
pub struct CommitTableResponse {
    /// Location of the updated table metadata file
    pub metadata_location: String,
    /// The table's updated metadata
    pub metadata: TableMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_and_listing_wire_examples_are_not_rust_field_names() {
        let namespace: NamespaceResponse =
            serde_json::from_str(r#"{"namespace":["nested","db"],"properties":{"owner":"team"}}"#)
                .unwrap();
        assert_eq!(namespace.namespace.to_url_string(), "nested\u{1f}db");
        assert_eq!(namespace.properties["owner"], "team");
        let page: ListTablesResponse = serde_json::from_str(
            r#"{"identifiers":[{"namespace":["db"],"name":"t"}],"next-page-token":"opaque/&"}"#,
        )
        .unwrap();
        assert_eq!(page.identifiers[0].name, "t");
        assert_eq!(page.next_page_token.as_deref(), Some("opaque/&"));
        let legacy: ListNamespaceResponse =
            serde_json::from_str(r#"{"namespaces":[["db"]]}"#).unwrap();
        assert!(legacy.next_page_token.is_none());
    }

    #[test]
    fn request_examples_match_the_independent_pinned_upstream_wire_models() {
        let input = serde_json::json!({"namespace":["db"],"properties":{"comment":"catalog"}});
        let ours: CreateNamespaceRequest = serde_json::from_value(input.clone()).unwrap();
        let upstream: crate::iceberg_catalog_rest::CreateNamespaceRequest =
            serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(ours).unwrap(), input);
        assert_eq!(serde_json::to_value(upstream).unwrap(), input);
        let create = serde_json::json!({
            "name":"orders", "stage-create":true,
            "schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]},
            "partition-spec":{"spec-id":0,"fields":[]},
            "write-order":{"order-id":0,"fields":[]},
            "properties":{"format-version":"2"}
        });
        let ours: CreateTableRequest = serde_json::from_value(create.clone()).unwrap();
        let upstream: crate::iceberg_catalog_rest::CreateTableRequest =
            serde_json::from_value(create.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(ours).unwrap(),
            serde_json::to_value(upstream).unwrap()
        );
        let commit = serde_json::json!({"identifier":{"namespace":["db"],"name":"orders"},"requirements":[{"type":"assert-create"}],"updates":[{"action":"set-properties","updates":{"comment":"a"}}]});
        let ours: CommitTableRequest = serde_json::from_value(commit.clone()).unwrap();
        let upstream: crate::iceberg_catalog_rest::CommitTableRequest =
            serde_json::from_value(commit.clone()).unwrap();
        assert_eq!(serde_json::to_value(ours).unwrap(), commit);
        assert_eq!(serde_json::to_value(upstream).unwrap(), commit);
    }
}
