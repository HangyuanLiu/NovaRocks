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

//! Provider-owned normalization of Iceberg catalog configuration.
//!
//! This is deliberately limited to provider configuration. Catalog clients,
//! caches, and control generations are added by the control runtime; SQL and
//! application table projections never enter this module.

use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};

use novarocks_fs::{
    ObjectStoreConfig, ObjectStoreEndpointConfig, is_object_store_location_parse_only,
    normalize_aws_s3_catalog_properties, object_store_config_from_aws_s3_catalog_properties,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergCatalogKind {
    Hadoop,
    Rest,
    Hive,
}

#[derive(Clone)]
pub struct IcebergCatalogConfiguration {
    pub kind: IcebergCatalogKind,
    pub warehouse_uri: String,
    pub rest_uri: Option<String>,
    pub hms_uris: Option<String>,
    pub properties: Vec<(String, String)>,
    pub object_store_config: Option<ObjectStoreConfig>,
    pub warehouse_path: PathBuf,
}

impl IcebergCatalogConfiguration {
    /// Discard the short-lived parser bridge used only to validate legacy
    /// property syntax. Production catalog generations retain no object-store
    /// secret material; their bound access capability stays role-local.
    pub fn without_object_store_config(mut self) -> Self {
        self.object_store_config = None;
        self
    }
}

impl Debug for IcebergCatalogConfiguration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergCatalogConfiguration")
            .field("kind", &self.kind)
            .field("warehouse_uri", &self.warehouse_uri)
            .field("rest_uri", &self.rest_uri)
            .field("hms_uris", &self.hms_uris)
            .field("property_count", &self.properties.len())
            .field("object_store_config", &self.object_store_config)
            .field("warehouse_path", &self.warehouse_path)
            .finish()
    }
}

pub fn parse_catalog_configuration(
    catalog_name: &str,
    properties: &[(String, String)],
) -> Result<IcebergCatalogConfiguration, String> {
    parse_catalog_configuration_with_object_store_binding(catalog_name, properties, None)
}

/// Parse catalog properties with credentials injected by the process
/// composition root.  Catalog properties may provide the same credentials for
/// a CREATE request, but they must match the role-local binding: a provider
/// generation can never depend on a credential that its BE installers do not
/// also possess.
pub fn parse_catalog_configuration_with_object_store_binding(
    catalog_name: &str,
    properties: &[(String, String)],
    injected_object_store_config: Option<&ObjectStoreConfig>,
) -> Result<IcebergCatalogConfiguration, String> {
    let properties = normalized_properties(properties);
    if let Some(kind) = properties.get("type")
        && !kind.eq_ignore_ascii_case("iceberg")
    {
        return Err(format!(
            "standalone iceberg catalog only supports type=iceberg, got {kind}"
        ));
    }
    let kind = match properties.get("iceberg.catalog.type") {
        None => IcebergCatalogKind::Hadoop,
        Some(value) if value.eq_ignore_ascii_case("hadoop") => IcebergCatalogKind::Hadoop,
        Some(value) if value.eq_ignore_ascii_case("rest") => IcebergCatalogKind::Rest,
        Some(value) if value.eq_ignore_ascii_case("hive") => IcebergCatalogKind::Hive,
        Some(value) => {
            return Err(format!(
                "standalone iceberg catalog supports iceberg.catalog.type=hadoop|rest|hive, got {value}"
            ));
        }
    };

    match kind {
        IcebergCatalogKind::Hadoop => {
            parse_hadoop_configuration(catalog_name, properties, injected_object_store_config)
        }
        IcebergCatalogKind::Rest => {
            parse_rest_configuration(properties, injected_object_store_config)
        }
        IcebergCatalogKind::Hive => {
            parse_hive_configuration(properties, injected_object_store_config)
        }
    }
}

fn parse_hadoop_configuration(
    catalog_name: &str,
    mut properties: HashMap<String, String>,
    injected_object_store_config: Option<&ObjectStoreConfig>,
) -> Result<IcebergCatalogConfiguration, String> {
    let raw_warehouse = properties
        .get("iceberg.catalog.warehouse")
        .or_else(|| properties.get("warehouse"))
        .cloned()
        .ok_or_else(|| {
            "standalone iceberg catalog requires `iceberg.catalog.warehouse`".to_string()
        })?;
    let is_object_store = match is_object_store_location_parse_only(&raw_warehouse) {
        Ok(value) => value,
        Err(error) if error.to_string().contains("unsupported fs location scheme") => false,
        Err(error) => {
            return Err(format!(
                "parse iceberg catalog warehouse URI {raw_warehouse}: {error}"
            ));
        }
    };
    let (warehouse_uri, warehouse_path, object_store_config) = if is_object_store {
        let object_store_config = resolve_object_store_config(
            &properties,
            injected_object_store_config,
        )?
        .ok_or_else(|| {
            "object-store iceberg catalog requires aws.s3.endpoint, aws.s3.access_key, aws.s3.secret_key"
                .to_string()
        })?;
        // Authorization belongs to the role-local Iceberg access capability.
        // Configuration parsing must not construct an unscoped filesystem
        // operator from catalog properties.
        let cache_dir = std::env::temp_dir()
            .join("novarocks_iceberg_cache")
            .join(catalog_name);
        std::fs::create_dir_all(&cache_dir)
            .map_err(|error| format!("create iceberg cache dir failed: {error}"))?;
        (raw_warehouse, cache_dir, Some(object_store_config))
    } else {
        let (warehouse_uri, warehouse_path) = normalize_warehouse_location(&raw_warehouse)?;
        std::fs::create_dir_all(&warehouse_path).map_err(|error| {
            format!(
                "create iceberg warehouse directory {} failed: {error}",
                warehouse_path.display()
            )
        })?;
        (warehouse_uri, warehouse_path, None)
    };
    properties.insert("type".to_string(), "iceberg".to_string());
    properties.insert(
        "iceberg.catalog.warehouse".to_string(),
        warehouse_uri.clone(),
    );
    Ok(IcebergCatalogConfiguration {
        kind: IcebergCatalogKind::Hadoop,
        warehouse_uri,
        rest_uri: None,
        hms_uris: None,
        properties: sorted_properties(&properties),
        object_store_config,
        warehouse_path,
    })
}

fn parse_rest_configuration(
    mut properties: HashMap<String, String>,
    injected_object_store_config: Option<&ObjectStoreConfig>,
) -> Result<IcebergCatalogConfiguration, String> {
    let uri = properties
        .get("uri")
        .or_else(|| properties.get("iceberg.catalog.uri"))
        .cloned()
        .ok_or_else(|| {
            "REST iceberg catalog requires `uri` property pointing at the REST endpoint".to_string()
        })?;
    let warehouse_uri = properties
        .get("warehouse")
        .or_else(|| properties.get("iceberg.catalog.warehouse"))
        .cloned()
        .unwrap_or_default();
    let object_store_config =
        resolve_object_store_config(&properties, injected_object_store_config)?;
    properties.insert("type".to_string(), "iceberg".to_string());
    properties.insert("iceberg.catalog.type".to_string(), "rest".to_string());
    properties.insert("uri".to_string(), uri.clone());
    if !warehouse_uri.is_empty() {
        properties.insert(
            "iceberg.catalog.warehouse".to_string(),
            warehouse_uri.clone(),
        );
    }
    Ok(IcebergCatalogConfiguration {
        kind: IcebergCatalogKind::Rest,
        warehouse_uri,
        rest_uri: Some(uri),
        hms_uris: None,
        properties: sorted_properties(&properties),
        object_store_config,
        warehouse_path: PathBuf::from("/__novarocks_rest_catalog_no_local_warehouse__"),
    })
}

fn parse_hive_configuration(
    mut properties: HashMap<String, String>,
    injected_object_store_config: Option<&ObjectStoreConfig>,
) -> Result<IcebergCatalogConfiguration, String> {
    for key in properties.keys() {
        let normalized = key.to_ascii_lowercase();
        if normalized.contains("kerberos")
            || normalized.contains("sasl")
            || normalized.contains("keytab")
            || normalized.contains("principal")
        {
            return Err(format!(
                "hive iceberg catalog v1 supports plaintext thrift only; unsupported auth property `{key}`"
            ));
        }
    }
    let raw_uris = properties
        .get("hive.metastore.uris")
        .or_else(|| properties.get("iceberg.catalog.hive.metastore.uris"))
        .cloned()
        .ok_or_else(|| {
            "hive iceberg catalog requires `hive.metastore.uris` (e.g. thrift://host:9083)"
                .to_string()
        })?;
    let first_uri = raw_uris
        .split(',')
        .map(str::trim)
        .find(|value| !value.is_empty())
        .ok_or_else(|| "hive.metastore.uris is empty".to_string())?
        .to_string();
    let hms_uris = first_uri
        .strip_prefix("thrift://")
        .unwrap_or(&first_uri)
        .to_string();
    let warehouse_uri = properties
        .get("iceberg.catalog.warehouse")
        .or_else(|| properties.get("warehouse"))
        .or_else(|| properties.get("hive.metastore.warehouse.dir"))
        .cloned()
        .unwrap_or_default();
    let object_store_config =
        resolve_object_store_config(&properties, injected_object_store_config)?;
    properties.insert("type".to_string(), "iceberg".to_string());
    properties.insert("iceberg.catalog.type".to_string(), "hive".to_string());
    properties.insert("hive.metastore.uris".to_string(), first_uri);
    if !warehouse_uri.is_empty() {
        properties.insert(
            "iceberg.catalog.warehouse".to_string(),
            warehouse_uri.clone(),
        );
    }
    Ok(IcebergCatalogConfiguration {
        kind: IcebergCatalogKind::Hive,
        warehouse_uri,
        rest_uri: None,
        hms_uris: Some(hms_uris),
        properties: sorted_properties(&properties),
        object_store_config,
        warehouse_path: PathBuf::from("/__novarocks_hms_catalog_no_local_warehouse__"),
    })
}

fn normalized_properties(properties: &[(String, String)]) -> HashMap<String, String> {
    let raw = properties.iter().cloned().collect::<BTreeMap<_, _>>();
    normalize_aws_s3_catalog_properties(&raw)
        .into_iter()
        .collect()
}

fn object_store_config(
    properties: &HashMap<String, String>,
) -> Result<Option<ObjectStoreConfig>, String> {
    let properties = properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    object_store_config_from_aws_s3_catalog_properties(&properties)
}

/// Reconcile the credentials a catalog statement supplies with the
/// server-composed binding.
///
/// The binding is authoritative for every value it states, so a statement may
/// not contradict it. Silence is not contradiction: catalog statements state
/// only what they need, and the composition root cannot express every optional
/// knob, so a value only one side states is kept rather than dropped.
fn merge_object_store_config(
    supplied: &ObjectStoreConfig,
    injected: &ObjectStoreConfig,
) -> Result<ObjectStoreConfig, String> {
    fn conflict() -> String {
        "Iceberg catalog object-store credentials do not match the server-composed binding"
            .to_string()
    }
    fn merge<T: Clone + PartialEq>(
        supplied: &Option<T>,
        injected: &Option<T>,
    ) -> Result<Option<T>, String> {
        match (supplied, injected) {
            (Some(supplied), Some(injected)) if supplied != injected => Err(conflict()),
            (_, Some(injected)) => Ok(Some(injected.clone())),
            (supplied, None) => Ok(supplied.clone()),
        }
    }

    if supplied.endpoint != injected.endpoint
        || supplied.access_key_id != injected.access_key_id
        || supplied.access_key_secret != injected.access_key_secret
    {
        return Err(conflict());
    }
    Ok(ObjectStoreConfig {
        endpoint: injected.endpoint.clone(),
        access_key_id: injected.access_key_id.clone(),
        access_key_secret: injected.access_key_secret.clone(),
        session_token: merge(&supplied.session_token, &injected.session_token)?,
        enable_path_style_access: merge(
            &supplied.enable_path_style_access,
            &injected.enable_path_style_access,
        )?,
        region: merge(&supplied.region, &injected.region)?,
        retry_max_times: merge(&supplied.retry_max_times, &injected.retry_max_times)?,
        retry_min_delay_ms: merge(&supplied.retry_min_delay_ms, &injected.retry_min_delay_ms)?,
        retry_max_delay_ms: merge(&supplied.retry_max_delay_ms, &injected.retry_max_delay_ms)?,
        timeout_ms: merge(&supplied.timeout_ms, &injected.timeout_ms)?,
        io_timeout_ms: merge(&supplied.io_timeout_ms, &injected.io_timeout_ms)?,
    })
}

fn resolve_object_store_config(
    properties: &HashMap<String, String>,
    injected: Option<&ObjectStoreConfig>,
) -> Result<Option<ObjectStoreConfig>, String> {
    let supplied = object_store_config(properties)?;
    match (supplied, injected) {
        (Some(supplied), Some(injected)) => {
            Ok(Some(merge_object_store_config(&supplied, injected)?))
        }
        // The legacy parser remains useful to provider unit tests and tools
        // that only construct a catalog configuration. Production factory
        // construction performs the stronger role-resource check after
        // parsing, where it can distinguish an omitted binding from this
        // standalone configuration path.
        (Some(supplied), None) => Ok(Some(supplied)),
        (None, Some(injected)) => Ok(Some(injected.clone())),
        (None, None) => Ok(None),
    }
}

/// Decode only the non-secret object-store endpoint settings carried by an
/// immutable catalog definition. Static credential material is deliberately
/// resolved by the role-local credential registry, never from these
/// properties.
pub fn object_store_endpoint_config_from_catalog_properties(
    properties: &[(String, String)],
) -> Result<Option<ObjectStoreEndpointConfig>, String> {
    let properties = normalized_properties(properties);
    let endpoint = ["aws.s3.endpoint", "aws.s3.endpoint_url"]
        .into_iter()
        .find_map(|key| properties.get(key).map(String::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let Some(endpoint) = endpoint else {
        return Ok(None);
    };

    let optional = |key: &str| {
        properties
            .get(key)
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let parse_bool = |key: &str| -> Result<Option<bool>, String> {
        optional(key)
            .map(|value| match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(true),
                "false" | "0" | "no" => Ok(false),
                _ => Err(format!("invalid {key} boolean value")),
            })
            .transpose()
    };
    let parse_number = |key: &str| -> Result<Option<u64>, String> {
        optional(key)
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid {key} value"))
            })
            .transpose()
    };

    let retry_max_times = optional("aws.s3.max_retries")
        .or_else(|| optional("aws.s3.retry_max_times"))
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| "invalid aws.s3.max_retries value".to_string())
        })
        .transpose()?;
    let timeout_ms = optional("aws.s3.request_timeout_ms")
        .or_else(|| optional("aws.s3.timeout_ms"))
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| "invalid aws.s3.request_timeout_ms value".to_string())
        })
        .transpose()?;

    Ok(Some(ObjectStoreEndpointConfig {
        endpoint: endpoint.to_string(),
        enable_path_style_access: parse_bool("aws.s3.enable_path_style_access")?,
        region: optional("aws.s3.region").map(str::to_string),
        retry_max_times,
        retry_min_delay_ms: parse_number("aws.s3.retry_min_delay_ms")?,
        retry_max_delay_ms: parse_number("aws.s3.retry_max_delay_ms")?,
        timeout_ms,
        io_timeout_ms: parse_number("aws.s3.io_timeout_ms")?,
    }))
}

fn sorted_properties(properties: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut entries = properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
}

fn normalize_warehouse_location(raw: &str) -> Result<(String, PathBuf), String> {
    if let Some(stripped) = raw.strip_prefix("file://") {
        let path = canonicalize_or_join(Path::new(stripped))?;
        return Ok((format!("file://{}", path.display()), path));
    }
    if raw.contains("://") {
        return Err(format!(
            "standalone iceberg hadoop catalog warehouse only supports local paths or file:// URIs, got {raw}"
        ));
    }
    let path = canonicalize_or_join(Path::new(raw))?;
    Ok((format!("file://{}", path.display()), path))
}

fn canonicalize_or_join(path: &Path) -> Result<PathBuf, String> {
    if path.exists() {
        std::fs::canonicalize(path).map_err(|error| format!("canonicalize path failed: {error}"))
    } else if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .map_err(|error| format!("read current directory failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_rest_catalog_and_keeps_credentials_process_local() {
        let configuration = parse_catalog_configuration(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), "http://127.0.0.1:8181".to_string()),
                (
                    "AWS.S3.ENDPOINT".to_string(),
                    "http://127.0.0.1:9000".to_string(),
                ),
                (
                    "aws.s3.access_key".to_string(),
                    "super-sensitive-access".to_string(),
                ),
                ("aws.s3.secret_key".to_string(), "top-secret".to_string()),
            ],
        )
        .expect("parse REST catalog");

        assert_eq!(configuration.kind, IcebergCatalogKind::Rest);
        assert_eq!(
            configuration.rest_uri.as_deref(),
            Some("http://127.0.0.1:8181")
        );
        assert_eq!(
            configuration
                .object_store_config
                .as_ref()
                .map(|config| config.endpoint.as_str()),
            Some("http://127.0.0.1:9000")
        );
        let debug = format!("{configuration:?}");
        assert!(!debug.contains("super-sensitive-access"));
        assert!(!debug.contains("top-secret"));
    }

    fn object_store_binding() -> ObjectStoreConfig {
        ObjectStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            access_key_id: novarocks_fs::SecretValue::new("admin"),
            access_key_secret: novarocks_fs::SecretValue::new("admin123"),
            session_token: None,
            enable_path_style_access: Some(true),
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    fn catalog_properties(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut properties = HashMap::from([
            (
                "aws.s3.endpoint".to_string(),
                "http://127.0.0.1:9000".to_string(),
            ),
            ("aws.s3.access_key".to_string(), "admin".to_string()),
            ("aws.s3.secret_key".to_string(), "admin123".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "true".to_string(),
            ),
        ]);
        properties.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        );
        properties
    }

    #[test]
    fn object_store_binding_keeps_a_value_only_one_side_states() {
        let binding = object_store_binding();
        let statement_only = resolve_object_store_config(
            &catalog_properties(&[("aws.s3.region", "us-east-1")]),
            Some(&binding),
        )
        .expect("statement region is additive")
        .expect("resolved binding");
        assert_eq!(statement_only.region.as_deref(), Some("us-east-1"));

        let binding_only = resolve_object_store_config(&catalog_properties(&[]), Some(&binding))
            .expect("an omitted region is not a conflict")
            .expect("resolved binding");
        assert_eq!(binding_only.region, None);

        let mut regional = object_store_binding();
        regional.region = Some("us-east-1".to_string());
        let agreed = resolve_object_store_config(&catalog_properties(&[]), Some(&regional))
            .expect("silence never contradicts the binding")
            .expect("resolved binding");
        assert_eq!(agreed.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn object_store_binding_rejects_a_contradicted_value() {
        let mut regional = object_store_binding();
        regional.region = Some("us-east-1".to_string());
        let error = resolve_object_store_config(
            &catalog_properties(&[("aws.s3.region", "eu-west-1")]),
            Some(&regional),
        )
        .expect_err("a stated region cannot be overridden");
        assert!(error.contains("do not match the server-composed binding"));

        let error = resolve_object_store_config(
            &catalog_properties(&[("aws.s3.access_key", "other")]),
            Some(&object_store_binding()),
        )
        .expect_err("a stated credential cannot be overridden");
        assert!(error.contains("do not match the server-composed binding"));
    }

    #[test]
    fn rejects_unsupported_hive_authentication() {
        let error = parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".to_string(), "hive".to_string()),
                (
                    "hive.metastore.uris".to_string(),
                    "thrift://hms:9083".to_string(),
                ),
                (
                    "hive.metastore.kerberos.principal".to_string(),
                    "hive/_HOST".to_string(),
                ),
            ],
        )
        .expect_err("kerberos must fail");

        assert!(error.contains("plaintext thrift"));
    }
}
