use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorReadSession,
    ConnectorReadSessionFinalizationContext, ConnectorReadSessionLease,
    ConnectorReadSessionOutcome, ConnectorRequestContext, ConnectorSplitPlanningRequest,
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::control::{
    StarRocksMetadataSource, StarRocksRpcSplitPlan, StarRocksRpcSplitPlanner, rpc_outer_facts,
};
use crate::domain::{
    StarRocksCapabilitySnapshot, StarRocksResolvedTable, StarRocksRpcTransport,
    StarRocksSelectedStrategy, StarRocksSplitPlanningInput, StarRocksStrategySplit,
    StarRocksStrategySplitPayload, StarRocksTopology, invalid, unsupported,
};
use crate::rpc::{
    StarRocksRemoteEndpoint, StarRocksRpcOutputBinding, StarRocksRpcSplit, encode_rpc_split,
};

#[derive(Clone)]
pub struct StarRocksRemoteControlConfig {
    endpoints: Vec<Url>,
    username: Arc<str>,
    password: Arc<str>,
    request_timeout: Duration,
    cleanup_timeout: Duration,
    retry_count: u32,
}

impl StarRocksRemoteControlConfig {
    pub fn try_new(
        endpoints: &[String],
        username: impl Into<Arc<str>>,
        password: impl Into<Arc<str>>,
        request_timeout: Duration,
        cleanup_timeout: Duration,
        retry_count: u32,
    ) -> Result<Self, ConnectorError> {
        if endpoints.is_empty() || request_timeout.is_zero() || cleanup_timeout.is_zero() {
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
            cleanup_timeout,
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
            .field("cleanup_timeout", &self.cleanup_timeout)
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
    fn prepare(
        &self,
        body: &PrepareRequest,
        context: &ConnectorRequestContext,
    ) -> Result<PrepareResponse, ConnectorError> {
        self.call(
            "POST",
            "/api/_starrocks_remote/prepare_scan",
            Some(body),
            context,
        )
    }
    fn start(
        &self,
        session_id: &str,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        let response: Status = self.call(
            "POST",
            "/api/_starrocks_remote/start_scan?forward_request=true",
            Some(&SessionRequest { session_id }),
            context,
        )?;
        response.ok()
    }
    fn cleanup(
        &self,
        session_id: &str,
        aborted: bool,
        context: ConnectorReadSessionFinalizationContext,
    ) -> Result<(), ConnectorError> {
        let synthetic = ConnectorRequestContext::try_new(
            context.deadline(),
            Arc::new(NeverCancelled),
            1024 * 1024,
            1024 * 1024,
        )?;
        let response: Status = self.call(
            "POST",
            "/api/_starrocks_remote/cleanup_sessions?forward_request=true",
            Some(&CleanupRequest {
                items: vec![Cleanup {
                    session_id,
                    cancel: aborted,
                }],
            }),
            &synthetic,
        )?;
        response.ok()
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
        let transports = caps
            .supported_transports
            .into_iter()
            .filter_map(|value| parse_transport(&value))
            .collect::<BTreeSet<_>>();
        StarRocksResolvedTable::try_new(
            namespace,
            table.table,
            Arc::new(Schema::new(fields)),
            StarRocksTopology::Unknown,
            Bytes::from(table.schema_version.to_string()),
            Bytes::from(format!(
                "remote-current:v1:{}:{}",
                caps.cluster_id, table.schema_version
            )),
            StarRocksCapabilitySnapshot {
                api_contract_version: crate::STARROCKS_CONTRACT_VERSION,
                rpc_transports: transports,
                rpc_ready: true,
                direct_contract_version: None,
                direct_ready: false,
            },
        )
    }
}

pub struct StarRocksRemoteScanPlanner {
    client: Arc<StarRocksRemoteControlClient>,
}
impl StarRocksRemoteScanPlanner {
    pub fn new(client: Arc<StarRocksRemoteControlClient>) -> Self {
        Self { client }
    }
}
impl StarRocksRpcSplitPlanner for StarRocksRemoteScanPlanner {
    fn plan_rpc_splits(
        &self,
        _: &StarRocksSplitPlanningInput,
        _: &ConnectorSplitPlanningRequest,
    ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError> {
        Err(unsupported("use StarRocks remote scan planning lifecycle"))
    }
    fn plan_rpc_read(
        &self,
        input: &StarRocksSplitPlanningInput,
        request: &ConnectorSplitPlanningRequest,
    ) -> Result<StarRocksRpcSplitPlan, ConnectorError> {
        let facts = rpc_outer_facts(input)?;
        let StarRocksSelectedStrategy::Rpc { transport } = input.strategy else {
            return Err(invalid("remote scan planner requires RPC strategy"));
        };
        let session_id = Uuid::now_v7().to_string();
        let required_outputs = if input.output_schema.fields().is_empty() {
            vec![RequiredOutput {
                output_index: None,
                name: "ROW_MARKER".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
                row_marker: true,
            }]
        } else {
            input
                .output_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| RequiredOutput {
                    output_index: Some(index),
                    name: field.name().clone(),
                    data_type: format!("{:?}", field.data_type()),
                    nullable: field.is_nullable(),
                    row_marker: false,
                })
                .collect()
        };
        let response = self.client.prepare(
            &PrepareRequest {
                db: &input.namespace,
                table: &input.table,
                schema_version: std::str::from_utf8(&input.schema_version)
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0),
                required_columns: input
                    .output_schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
                required_outputs: required_outputs.clone(),
                soft_limit: (!input.has_residual_predicates)
                    .then_some(input.limit)
                    .flatten()
                    .and_then(|value| i64::try_from(value).ok())
                    .unwrap_or(-1),
                session_id: &session_id,
                transport: transport_name(transport),
            },
            &request.context,
        )?;
        response.ok()?;
        if response.session_id != session_id {
            return Err(invalid(
                "StarRocks remote prepare response session does not match request",
            ));
        }
        let outputs = response
            .outputs
            .into_iter()
            .map(|output| {
                let data_type = parse_type(output.data_type())?;
                let row_marker = output.row_marker();
                Ok(StarRocksRpcOutputBinding {
                    output_index: output.output_index,
                    remote_slot_id: output.remote_slot_id,
                    name: Arc::from(output.name),
                    data_type,
                    nullable: output.nullable,
                    is_const: output.is_const,
                    row_marker,
                })
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        if outputs.len() != required_outputs.len()
            || outputs.iter().any(|output| {
                required_outputs
                    .iter()
                    .find(|expected| expected.output_index == output.output_index)
                    .is_none_or(|expected| {
                        expected.row_marker != output.row_marker
                            || expected.name != output.name.as_ref()
                            || expected.nullable != output.nullable
                            || if expected.row_marker {
                                output.data_type != DataType::Int64
                            } else {
                                output
                                    .output_index
                                    .and_then(|index| input.output_schema.fields().get(index))
                                    .is_none_or(|field| field.data_type() != &output.data_type)
                            }
                    })
            })
        {
            return Err(invalid(
                "StarRocks remote prepare output mapping does not match the frozen scan",
            ));
        }
        let splits = response
            .streams
            .into_iter()
            .enumerate()
            .map(|(index, stream)| {
                if parse_transport(&stream.transport) != Some(transport) {
                    return Err(invalid("StarRocks remote stream transport mismatch"));
                }
                let split = StarRocksRpcSplit::try_new(
                    transport,
                    StarRocksRemoteEndpoint::try_new(stream.remote_be.host, stream.remote_be.port)?,
                    Bytes::from(stream.scan_token),
                    outputs.clone(),
                )?;
                Ok(StarRocksStrategySplit {
                    split_id: Arc::from(format!("remote-{index}")),
                    payload: StarRocksStrategySplitPayload::Rpc(encode_rpc_split(
                        &facts,
                        &split,
                        request.context.max_handle_payload_bytes(),
                    )?),
                    estimated_bytes: None,
                })
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        let session = ConnectorReadSessionLease::try_new(
            Arc::new(RemoteSession {
                client: Arc::clone(&self.client),
                session_id,
            }),
            request.context.clone(),
            self.client.config.cleanup_timeout,
        )?;
        Ok(StarRocksRpcSplitPlan {
            splits,
            session: Some(session),
        })
    }
}

struct RemoteSession {
    client: Arc<StarRocksRemoteControlClient>,
    session_id: String,
}
impl ConnectorReadSession for RemoteSession {
    fn start(&self, context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
        self.client.start(&self.session_id, context)
    }
    fn finish(
        &self,
        outcome: ConnectorReadSessionOutcome,
        context: ConnectorReadSessionFinalizationContext,
    ) -> Result<(), ConnectorError> {
        self.client.cleanup(
            &self.session_id,
            outcome == ConnectorReadSessionOutcome::Aborted,
            context,
        )
    }
}
struct NeverCancelled;
impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorReadSessionLease, ConnectorReadSessionOutcome,
    };

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
            let response = if request.url.path().ends_with("/tables") {
                r#"{"status":200,"values":["t"]}"#
            } else {
                r#"{"status":200}"#
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
                Duration::from_secs(1),
                0,
            )
            .expect("config"),
            transport,
        ))
    }

    struct PlanningTransport;
    impl StarRocksHttpTransport for PlanningTransport {
        fn request(&self, request: StarRocksHttpRequest<'_>) -> Result<Bytes, ConnectorError> {
            let response = if request.url.path().ends_with("/prepare_scan") {
                let body: serde_json::Value =
                    serde_json::from_slice(request.body.expect("prepare body"))
                        .expect("valid JSON");
                let session_id = body["session_id"].as_str().expect("session ID");
                format!(
                    r#"{{"status":200,"session_id":"{session_id}","streams":[{{"scan_token":"query-token","remote_be":{{"host":"be.example","port":8040}},"transport":"brpc_chunk"}}],"outputs":[{{"output_index":0,"remote_slot_id":1,"name":"value","actual_wire_type":"BIGINT","nullable":false,"is_const":false,"wire_shape":"DATA"}}]}}"#
                )
            } else {
                r#"{"status":200}"#.to_string()
            };
            Ok(Bytes::from(response))
        }
    }

    #[test]
    fn starrocks_remote_control_config_redacts_credentials_and_rejects_bearer_urls() {
        let rejected = StarRocksRemoteControlConfig::try_new(
            &["https://user:password@fe.example:8030".to_string()],
            "user",
            "p@ssw0rd",
            Duration::from_secs(1),
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
            Duration::from_secs(1),
            0,
        )
        .expect("config");
        let debug = format!("{config:?}");
        assert!(!debug.contains("p@ssw0rd"));
        assert!(!debug.contains("user@cluster"));
    }

    #[test]
    fn starrocks_remote_control_preserves_query_and_finalizes_the_same_session() {
        let transport = Arc::new(Transport::default());
        let client = client(Arc::clone(&transport));
        assert_eq!(
            client.list_tables("db", &context()).expect("tables").values,
            ["t"]
        );

        let lease = ConnectorReadSessionLease::try_new(
            Arc::new(RemoteSession {
                client,
                session_id: "01808080-8080-7080-8080-808080808080".to_string(),
            }),
            context(),
            Duration::from_secs(1),
        )
        .expect("lease");
        lease.start().expect("start");
        lease
            .finish(ConnectorReadSessionOutcome::Completed)
            .expect("cleanup");

        let paths = transport.paths.lock().expect("paths");
        assert!(paths.iter().any(|path| path.ends_with("tables?db=db")));
        assert!(
            paths
                .iter()
                .any(|path| path.contains("/start_scan?forward_request=true"))
        );
        assert!(
            paths
                .iter()
                .any(|path| path.contains("/cleanup_sessions?forward_request=true"))
        );
    }

    #[test]
    fn starrocks_remote_planner_freezes_a_typed_rpc_split_and_session() {
        use std::num::NonZeroUsize;

        use novarocks_spi::connector::{ConnectorInstanceId, ConnectorSplitPlanningRequest};

        let client = Arc::new(StarRocksRemoteControlClient::with_transport(
            StarRocksRemoteControlConfig::try_new(
                &["https://fe.example:8030".to_string()],
                "user",
                "p@ssw0rd",
                Duration::from_secs(1),
                Duration::from_secs(1),
                0,
            )
            .expect("config"),
            Arc::new(PlanningTransport),
        ));
        let input = StarRocksSplitPlanningInput {
            owner: ConnectorInstanceId::parse("catalog.starrocks").expect("owner"),
            incarnation: novarocks_spi::connector::ConnectorInstanceIncarnation::new(),
            attempt: crate::StarRocksReadAttemptId::new(),
            freeze: crate::StarRocksFreezeDigest([7; 32]),
            strategy: StarRocksSelectedStrategy::Rpc {
                transport: StarRocksRpcTransport::BrpcChunk,
            },
            topology: crate::StarRocksTopology::Unknown,
            namespace: Arc::from("db"),
            table: Arc::from("t"),
            schema_version: Bytes::from_static(b"11"),
            data_version: Bytes::from_static(b"remote-current:v1"),
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            projection: vec![0],
            limit: Some(10),
            has_residual_predicates: false,
        };
        let plan = StarRocksRemoteScanPlanner::new(client)
            .plan_rpc_read(
                &input,
                &ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                    max_split_bytes: None,
                    context: context(),
                },
            )
            .expect("plan");

        assert!(plan.session.is_some());
        let StarRocksStrategySplitPayload::Rpc(payload) = &plan.splits[0].payload else {
            panic!("expected RPC split");
        };
        let split = crate::rpc::decode_rpc_split(
            payload.as_bytes(),
            &crate::control::rpc_outer_facts(&input).expect("facts"),
        )
        .expect("typed split");
        assert_eq!(split.endpoint().host(), "be.example");
        assert_eq!(split.outputs()[0].remote_slot_id, 1);
        assert!(!format!("{split:?}").contains("query-token"));
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
fn parse_transport(value: &str) -> Option<StarRocksRpcTransport> {
    match value {
        "brpc_chunk" => Some(StarRocksRpcTransport::BrpcChunk),
        "arrow_flight" => Some(StarRocksRpcTransport::ArrowFlight),
        _ => None,
    }
}
fn transport_name(value: StarRocksRpcTransport) -> &'static str {
    match value {
        StarRocksRpcTransport::BrpcChunk => "brpc_chunk",
        StarRocksRpcTransport::ArrowFlight => "arrow_flight",
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
    supported_transports: Vec<String>,
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
#[derive(Serialize)]
struct PrepareRequest<'a> {
    db: &'a str,
    table: &'a str,
    schema_version: i64,
    required_columns: Vec<String>,
    required_outputs: Vec<RequiredOutput>,
    soft_limit: i64,
    session_id: &'a str,
    transport: &'a str,
}
#[derive(Clone, Serialize)]
struct RequiredOutput {
    output_index: Option<usize>,
    name: String,
    data_type: String,
    nullable: bool,
    row_marker: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareResponse {
    #[serde(flatten)]
    status: Status,
    session_id: String,
    streams: Vec<RemoteStream>,
    outputs: Vec<RemoteOutput>,
}
impl PrepareResponse {
    fn ok(&self) -> Result<(), ConnectorError> {
        self.status.ok()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteStream {
    scan_token: String,
    remote_be: RemoteEndpoint,
    transport: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteEndpoint {
    host: String,
    port: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteOutput {
    output_index: Option<usize>,
    remote_slot_id: i32,
    name: String,
    actual_wire_type: String,
    nullable: bool,
    is_const: bool,
    wire_shape: String,
}
impl RemoteOutput {
    fn row_marker(&self) -> bool {
        self.wire_shape == "ROW_MARKER"
    }
    fn data_type(&self) -> &str {
        &self.actual_wire_type
    }
}
#[derive(Serialize)]
struct SessionRequest<'a> {
    session_id: &'a str,
}
#[derive(Serialize)]
struct CleanupRequest<'a> {
    items: Vec<Cleanup<'a>>,
}
#[derive(Serialize)]
struct Cleanup<'a> {
    session_id: &'a str,
    cancel: bool,
}
