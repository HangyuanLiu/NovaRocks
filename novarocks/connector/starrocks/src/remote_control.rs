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

//! The remote StarRocks control HTTP client.
//!
//! Only the metadata half of that API is spoken here: capabilities, databases,
//! tables and one table definition. The scan half — `prepare_scan`,
//! `start_scan` and `cleanup_sessions`, and the session lease that drove them
//! — went away with the untyped read stack, and a typed StarRocks read has to
//! define its own scan lifecycle before those calls come back.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorRequestContext};
use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::control::StarRocksMetadataSource;
use crate::domain::{StarRocksCapabilitySnapshot, StarRocksResolvedTable, invalid, unsupported};

#[derive(Clone)]
pub struct StarRocksRemoteControlConfig {
    endpoints: Vec<Url>,
    username: Arc<str>,
    password: Arc<str>,
    request_timeout: Duration,
    retry_count: u32,
}

impl StarRocksRemoteControlConfig {
    pub fn try_new(
        endpoints: &[String],
        username: impl Into<Arc<str>>,
        password: impl Into<Arc<str>>,
        request_timeout: Duration,
        retry_count: u32,
    ) -> Result<Self, ConnectorError> {
        if endpoints.is_empty() || request_timeout.is_zero() {
            return Err(invalid("invalid StarRocks remote control configuration"));
        }
        let endpoints = endpoints
            .iter()
            .map(|raw| {
                let url = Url::parse(raw)
                    .map_err(|_| invalid("invalid StarRocks remote control endpoint"))?;
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || url.port_or_known_default().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                {
                    return Err(invalid("invalid StarRocks remote control endpoint"));
                }
                Ok(url)
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        Ok(Self {
            endpoints,
            username: username.into(),
            password: password.into(),
            request_timeout,
            retry_count,
        })
    }
}
impl fmt::Debug for StarRocksRemoteControlConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StarRocksRemoteControlConfig")
            .field("endpoints", &self.endpoints)
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .field("request_timeout", &self.request_timeout)
            .field("retry_count", &self.retry_count)
            .finish()
    }
}

pub struct StarRocksHttpRequest<'a> {
    pub method: &'a str,
    pub url: &'a Url,
    pub body: Option<&'a [u8]>,
    pub username: &'a str,
    pub password: &'a str,
    pub timeout: Duration,
    pub context: &'a ConnectorRequestContext,
}

pub trait StarRocksHttpTransport: Send + Sync {
    fn request(&self, request: StarRocksHttpRequest<'_>) -> Result<Bytes, ConnectorError>;
}

pub struct StarRocksReqwestHttpTransport {
    client: reqwest::blocking::Client,
}
impl StarRocksReqwestHttpTransport {
    pub fn try_new() -> Result<Self, ConnectorError> {
        Ok(Self {
            client: reqwest::blocking::Client::builder().build().map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "construct StarRocks HTTP client",
                )
            })?,
        })
    }
}
impl StarRocksHttpTransport for StarRocksReqwestHttpTransport {
    fn request(&self, request: StarRocksHttpRequest<'_>) -> Result<Bytes, ConnectorError> {
        active(request.context)?;
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|_| invalid("invalid HTTP method"))?;
        let mut builder = self
            .client
            .request(method, request.url.clone())
            .timeout(request.timeout)
            .basic_auth(
                request
                    .username
                    .split('@')
                    .next()
                    .unwrap_or(request.username),
                Some(request.password),
            )
            .header(reqwest::header::ACCEPT, "application/json");
        if let Some(body) = request.body {
            builder = builder
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_vec());
        }
        let response = builder.send().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "StarRocks remote control request failed",
            )
        })?;
        let bytes = response.bytes().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "read StarRocks remote control response",
            )
        })?;
        if bytes.len() > request.context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "StarRocks remote control response exceeds payload budget",
            ));
        }
        Ok(Bytes::from(bytes.to_vec()))
    }
}

pub struct StarRocksRemoteControlClient {
    config: StarRocksRemoteControlConfig,
    transport: Arc<dyn StarRocksHttpTransport>,
}
impl StarRocksRemoteControlClient {
    pub fn try_new(config: StarRocksRemoteControlConfig) -> Result<Self, ConnectorError> {
        Ok(Self {
            config,
            transport: Arc::new(StarRocksReqwestHttpTransport::try_new()?),
        })
    }
    pub fn with_transport(
        config: StarRocksRemoteControlConfig,
        transport: Arc<dyn StarRocksHttpTransport>,
    ) -> Self {
        Self { config, transport }
    }
    fn call<T: for<'de> Deserialize<'de>, B: Serialize>(
        &self,
        method: &str,
        path: &str,
        body: Option<&B>,
        context: &ConnectorRequestContext,
    ) -> Result<T, ConnectorError> {
        active(context)?;
        let body = body
            .map(|body| {
                serde_json::to_vec(body).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "encode StarRocks remote control request",
                    )
                })
            })
            .transpose()?;
        let attempts = if self.config.endpoints.len() == 1 {
            self.config.retry_count + 1
        } else {
            1
        };
        let mut last = None;
        for _ in 0..attempts {
            for endpoint in &self.config.endpoints {
                active(context)?;
                let url = endpoint
                    .join(path)
                    .map_err(|_| invalid("invalid StarRocks remote control path"))?;
                let timeout = self
                    .config
                    .request_timeout
                    .min(context.deadline().saturating_duration_since(Instant::now()));
                match self
                    .transport
                    .request(StarRocksHttpRequest {
                        method,
                        url: &url,
                        body: body.as_deref(),
                        username: &self.config.username,
                        password: &self.config.password,
                        timeout,
                        context,
                    })
                    .and_then(|bytes| {
                        serde_json::from_slice(&bytes).map_err(|_| {
                            ConnectorError::new(
                                ConnectorErrorKind::CorruptData,
                                "decode StarRocks remote control response",
                            )
                        })
                    }) {
                    Ok(value) => return Ok(value),
                    Err(error) => last = Some(error),
                }
            }
        }
        Err(last.unwrap_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "StarRocks remote control is unavailable",
            )
        }))
    }
    fn get<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        context: &ConnectorRequestContext,
    ) -> Result<T, ConnectorError> {
        self.call::<T, Empty>("GET", path, None, context)
    }
    fn capabilities(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<Capabilities, ConnectorError> {
        self.get("/api/_starrocks_remote/capabilities", context)
    }
    fn list_databases(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ListResponse, ConnectorError> {
        self.get("/api/_starrocks_remote/databases", context)
    }
    fn list_tables(
        &self,
        namespace: &str,
        context: &ConnectorRequestContext,
    ) -> Result<ListResponse, ConnectorError> {
        self.get(
            &format!("/api/_starrocks_remote/tables?db={namespace}"),
            context,
        )
    }
    fn table(
        &self,
        namespace: &str,
        table: &str,
        context: &ConnectorRequestContext,
    ) -> Result<TableResponse, ConnectorError> {
        self.get(
            &format!("/api/_starrocks_remote/table?db={namespace}&table={table}"),
            context,
        )
    }
}

pub struct StarRocksRemoteMetadataSource {
    client: Arc<StarRocksRemoteControlClient>,
}
impl StarRocksRemoteMetadataSource {
    pub fn new(client: Arc<StarRocksRemoteControlClient>) -> Self {
        Self { client }
    }
}
impl StarRocksMetadataSource for StarRocksRemoteMetadataSource {
    fn namespace_exists(
        &self,
        namespace: &str,
        context: &ConnectorRequestContext,
    ) -> Result<bool, ConnectorError> {
        Ok(self
            .client
            .list_databases(context)?
            .values
            .contains(&namespace.to_string()))
    }
    fn table_exists(
        &self,
        namespace: &str,
        table: &str,
        context: &ConnectorRequestContext,
    ) -> Result<bool, ConnectorError> {
        Ok(self.client.table(namespace, table, context)?.status.status == 200)
    }
    fn list_tables(
        &self,
        namespace: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, ConnectorError> {
        let response = self.client.list_tables(namespace, context)?;
        response.ok()?;
        Ok(response.values)
    }
    fn load_table(
        &self,
        namespace: &str,
        table: &str,
        context: &ConnectorRequestContext,
    ) -> Result<StarRocksResolvedTable, ConnectorError> {
        let caps = self.client.capabilities(context)?;
        caps.ok()?;
        let response = self.client.table(namespace, table, context)?;
        response.ok()?;
        let table = response.table.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks remote table response is missing table",
            )
        })?;
        let fields = table
            .columns
            .into_iter()
            .map(|column| {
                Ok(Field::new(
                    column.name,
                    parse_type(&column.data_type)?,
                    column.nullable,
                ))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        StarRocksResolvedTable::try_new(
            namespace,
            table.table,
            Arc::new(Schema::new(fields)),
            Bytes::from(table.schema_version.to_string()),
            Bytes::from(format!(
                "remote-current:v1:{}:{}",
                caps.cluster_id, table.schema_version
            )),
            StarRocksCapabilitySnapshot {
                api_contract_version: crate::STARROCKS_CONTRACT_VERSION,
            },
        )
    }
}

fn active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ))
    } else if Instant::now() >= context.deadline() {
        Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ))
    } else {
        Ok(())
    }
}

fn parse_type(value: &str) -> Result<DataType, ConnectorError> {
    match value.to_ascii_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "TINYINT" => Ok(DataType::Int8),
        "SMALLINT" => Ok(DataType::Int16),
        "INT" | "INTEGER" => Ok(DataType::Int32),
        "BIGINT" => Ok(DataType::Int64),
        "FLOAT" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "CHAR" | "VARCHAR" | "STRING" | "UTF8" => Ok(DataType::Utf8),
        "VARBINARY" | "BINARY" => Ok(DataType::Binary),
        _ => Err(unsupported("unsupported StarRocks remote column type")),
    }
}

#[derive(Serialize)]
struct Empty;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Status {
    status: u16,
}
impl Status {
    fn ok(&self) -> Result<(), ConnectorError> {
        if self.status == 200 {
            Ok(())
        } else {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "StarRocks remote control returned a non-success status",
            ))
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Capabilities {
    #[serde(flatten)]
    status: Status,
    cluster_id: i64,
    /// Parsed but unread: the response shape is validated strictly, and a real
    /// cluster still announces its read transports. Nothing consumes them
    /// while the connector has no typed read.
    #[serde(rename = "supported_transports")]
    _supported_transports: Vec<String>,
}
impl Capabilities {
    fn ok(&self) -> Result<(), ConnectorError> {
        self.status.ok()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListResponse {
    #[serde(flatten)]
    status: Status,
    #[serde(alias = "databases", alias = "tables")]
    values: Vec<String>,
}
impl ListResponse {
    fn ok(&self) -> Result<(), ConnectorError> {
        self.status.ok()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableResponse {
    #[serde(flatten)]
    status: Status,
    table: Option<RemoteTable>,
}
impl TableResponse {
    fn ok(&self) -> Result<(), ConnectorError> {
        self.status.ok()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteTable {
    table: String,
    schema_version: i64,
    columns: Vec<RemoteColumn>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteColumn {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    nullable: bool,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use novarocks_spi::connector::ConnectorCancellation;

    use super::*;

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[derive(Default)]
    struct Transport {
        paths: Mutex<Vec<String>>,
    }

    impl StarRocksHttpTransport for Transport {
        fn request(&self, request: StarRocksHttpRequest<'_>) -> Result<Bytes, ConnectorError> {
            self.paths
                .lock()
                .expect("paths")
                .push(request.url.to_string());
            let response = match request.url.path() {
                path if path.ends_with("/tables") => r#"{"status":200,"tables":["t"]}"#,
                path if path.ends_with("/capabilities") => {
                    r#"{"status":200,"cluster_id":42,"supported_transports":["brpc_chunk"]}"#
                }
                path if path.ends_with("/table") => {
                    r#"{"status":200,"table":{"table":"t","schema_version":11,"columns":[{"name":"id","type":"BIGINT","nullable":false}]}}"#
                }
                _ => r#"{"status":200}"#,
            };
            Ok(Bytes::from_static(response.as_bytes()))
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            4096,
            4096,
        )
        .expect("context")
    }

    fn client(transport: Arc<Transport>) -> Arc<StarRocksRemoteControlClient> {
        Arc::new(StarRocksRemoteControlClient::with_transport(
            StarRocksRemoteControlConfig::try_new(
                &["https://fe.example:8030".to_string()],
                "user@cluster",
                "p@ssw0rd",
                Duration::from_secs(1),
                0,
            )
            .expect("config"),
            transport,
        ))
    }

    #[test]
    fn starrocks_remote_control_config_redacts_credentials_and_rejects_bearer_urls() {
        let rejected = StarRocksRemoteControlConfig::try_new(
            &["https://user:password@fe.example:8030".to_string()],
            "user",
            "p@ssw0rd",
            Duration::from_secs(1),
            0,
        )
        .expect_err("URL credentials are not a control-plane credential channel");
        assert_eq!(rejected.kind(), ConnectorErrorKind::InvalidRequest);

        let config = StarRocksRemoteControlConfig::try_new(
            &["https://fe.example:8030".to_string()],
            "user@cluster",
            "p@ssw0rd",
            Duration::from_secs(1),
            0,
        )
        .expect("config");
        let debug = format!("{config:?}");
        assert!(!debug.contains("p@ssw0rd"));
        assert!(!debug.contains("user@cluster"));
    }

    #[test]
    fn starrocks_remote_control_preserves_the_metadata_query() {
        let transport = Arc::new(Transport::default());
        let client = client(Arc::clone(&transport));

        assert_eq!(
            client.list_tables("db", &context()).expect("tables").values,
            ["t"]
        );

        let paths = transport.paths.lock().expect("paths");
        assert!(paths.iter().any(|path| path.ends_with("tables?db=db")));
    }

    #[test]
    fn loading_a_table_resolves_its_schema_and_both_versions() {
        let transport = Arc::new(Transport::default());
        let source = StarRocksRemoteMetadataSource::new(client(Arc::clone(&transport)));

        let table = source
            .load_table("db", "t", &context())
            .expect("resolved table");

        assert_eq!(table.namespace.as_ref(), "db");
        assert_eq!(table.table.as_ref(), "t");
        assert_eq!(
            table.schema.as_ref(),
            &Schema::new(vec![Field::new("id", DataType::Int64, false)])
        );
        assert_eq!(table.schema_version, Bytes::from_static(b"11"));
        assert_eq!(
            table.data_version,
            Bytes::from_static(b"remote-current:v1:42:11")
        );
        assert_eq!(
            table.capability.api_contract_version,
            crate::STARROCKS_CONTRACT_VERSION
        );
    }

    #[test]
    fn an_unknown_remote_column_type_is_not_guessed() {
        assert_eq!(
            parse_type("HLL")
                .expect_err("an unmapped StarRocks type has no Arrow equivalent")
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }
}
