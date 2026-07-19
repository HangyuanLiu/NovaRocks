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

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use moka::sync::Cache;

use crate::common::config;
use crate::common::types::UniqueId;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::service::disk_report;
use crate::service::frontend_rpc::{FrontendRpcError, FrontendRpcKind, FrontendRpcManager};
use crate::thrift::agent_service::TTabletSchema;
use crate::thrift::frontend_service;
use crate::thrift::status::TStatus;
use crate::thrift::status_code;
use crate::thrift::types;

#[derive(Clone, Debug)]
pub(crate) struct TableSchemaFetchRequest {
    pub(crate) fe_addr: types::TNetworkAddress,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) source: frontend_service::TTableSchemaRequestSource,
    pub(crate) tablet_id: Option<i64>,
    pub(crate) query_id: Option<types::TUniqueId>,
    pub(crate) txn_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct TableSchemaCacheKey {
    db_id: i64,
    table_id: i64,
    schema_id: i64,
}

#[derive(Clone, Debug)]
struct TableSchemaError {
    kind: TableSchemaErrorKind,
    message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TableSchemaErrorKind {
    Transport,
    TableNotFound,
    Other,
}

impl TableSchemaError {
    fn other(message: impl Into<String>) -> Self {
        Self {
            kind: TableSchemaErrorKind::Other,
            message: message.into(),
        }
    }

    fn table_not_found(message: impl Into<String>) -> Self {
        Self {
            kind: TableSchemaErrorKind::TableNotFound,
            message: message.into(),
        }
    }

    fn is_transport(&self) -> bool {
        self.kind == TableSchemaErrorKind::Transport
    }

    fn is_table_not_found(&self) -> bool {
        self.kind == TableSchemaErrorKind::TableNotFound
    }
}

impl From<FrontendRpcError> for TableSchemaError {
    fn from(value: FrontendRpcError) -> Self {
        if value.is_transport() {
            Self {
                kind: TableSchemaErrorKind::Transport,
                message: value.to_string(),
            }
        } else {
            Self::other(value.to_string())
        }
    }
}

pub(crate) struct TableSchemaService {
    cache: Cache<TableSchemaCacheKey, TTabletSchema>,
    flights: SingleFlightGroup,
    max_retries: usize,
}

impl TableSchemaService {
    fn new() -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(config::table_schema_service_cache_capacity())
                .build(),
            flights: SingleFlightGroup::default(),
            max_retries: config::table_schema_service_max_retries(),
        }
    }

    pub(crate) fn shared() -> &'static Self {
        static INSTANCE: OnceLock<TableSchemaService> = OnceLock::new();
        INSTANCE.get_or_init(TableSchemaService::new)
    }

    pub(crate) fn fetch(
        &self,
        request: TableSchemaFetchRequest,
        local_schema: Option<&TTabletSchema>,
    ) -> Result<TTabletSchema, String> {
        self.fetch_with_loader(request, local_schema, |request| self.fetch_remote(request))
    }

    fn fetch_with_loader<F>(
        &self,
        request: TableSchemaFetchRequest,
        local_schema: Option<&TTabletSchema>,
        mut load_remote: F,
    ) -> Result<TTabletSchema, String>
    where
        F: FnMut(&TableSchemaFetchRequest) -> Result<TTabletSchema, TableSchemaError>,
    {
        validate_request(&request)?;
        let cache_key = TableSchemaCacheKey {
            db_id: request.db_id,
            table_id: request.table_id,
            schema_id: request.schema_id,
        };

        if let Some(schema) = local_schema.filter(|schema| schema.id == Some(request.schema_id)) {
            tracing::debug!(
                target: "novarocks::schema",
                event = "local_hit",
                fe_addr = %format_addr(&request.fe_addr),
                db_id = request.db_id,
                table_id = request.table_id,
                schema_id = request.schema_id,
                source = %source_label(request.source),
                "Resolved table schema from local metadata"
            );
            self.cache.insert(cache_key, schema.clone());
            return Ok(schema.clone());
        }

        if let Some(schema) = self.cache.get(&cache_key) {
            tracing::debug!(
                target: "novarocks::schema",
                event = "cache_hit",
                fe_addr = %format_addr(&request.fe_addr),
                db_id = request.db_id,
                table_id = request.table_id,
                schema_id = request.schema_id,
                source = %source_label(request.source),
                "Resolved table schema from local cache"
            );
            return Ok(schema);
        }

        let mut last_error = None;
        for attempt in 1..=self.max_retries {
            let isolated_retry = attempt == self.max_retries && self.max_retries > 1;
            let flight_key = build_flight_key(&request, isolated_retry);
            if isolated_retry {
                tracing::debug!(
                    target: "novarocks::schema",
                    event = "isolated_retry",
                    fe_addr = %format_addr(&request.fe_addr),
                    db_id = request.db_id,
                    table_id = request.table_id,
                    schema_id = request.schema_id,
                    source = %source_label(request.source),
                    attempt,
                    "Running isolated table schema retry"
                );
            }
            let (result, shared) = self.flights.execute(flight_key, || load_remote(&request));
            if shared {
                tracing::debug!(
                    target: "novarocks::schema",
                    event = "singleflight_shared",
                    fe_addr = %format_addr(&request.fe_addr),
                    db_id = request.db_id,
                    table_id = request.table_id,
                    schema_id = request.schema_id,
                    source = %source_label(request.source),
                    attempt,
                    "Shared in-flight FE getTableSchema request"
                );
            }

            match result {
                Ok(schema) => {
                    self.cache.insert(cache_key, schema.clone());
                    return Ok(schema);
                }
                Err(err) if err.is_transport() || err.is_table_not_found() => {
                    return Err(err.message);
                }
                Err(err) => {
                    last_error = Some(err.message);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            "FE getTableSchema failed without a terminal error message".to_string()
        }))
    }

    fn fetch_remote(
        &self,
        request: &TableSchemaFetchRequest,
    ) -> Result<TTabletSchema, TableSchemaError> {
        let rpc_request = frontend_service::TBatchGetTableSchemaRequest {
            requests: Some(vec![frontend_service::TGetTableSchemaRequest {
                schema_key: Some(crate::thrift::descriptors::TTableSchemaKey {
                    db_id: Some(request.db_id),
                    table_id: Some(request.table_id),
                    schema_id: Some(request.schema_id),
                }),
                source: Some(request.source),
                tablet_id: request.tablet_id,
                query_id: request.query_id.clone(),
                txn_id: request.txn_id,
            }]),
        };
        let response = FrontendRpcManager::shared()
            .call(FrontendRpcKind::SchemaMeta, &request.fe_addr, |client| {
                client
                    .get_table_schema(rpc_request.clone())
                    .map_err(FrontendRpcError::from_thrift)
            })
            .map_err(TableSchemaError::from)?;

        let top_status = response.status.as_ref().ok_or_else(|| {
            TableSchemaError::other("FE getTableSchema returned empty top-level status")
        })?;
        ensure_ok_status(
            top_status,
            "FE getTableSchema returned non-OK top-level status",
        )?;

        let mut responses = response.responses.unwrap_or_default();
        if responses.len() != 1 {
            return Err(TableSchemaError::other(format!(
                "FE getTableSchema returned unexpected response count: expected=1 actual={}",
                responses.len()
            )));
        }
        let item = responses
            .pop()
            .ok_or_else(|| TableSchemaError::other("FE getTableSchema returned empty responses"))?;
        let item_status = item.status.as_ref().ok_or_else(|| {
            TableSchemaError::other("FE getTableSchema response item missing status")
        })?;
        ensure_ok_status(
            item_status,
            "FE getTableSchema response item returned non-OK",
        )?;
        item.schema.ok_or_else(|| {
            TableSchemaError::other("FE getTableSchema response item missing schema")
        })
    }
}

fn scan_frontend_transport_address(
    fe_addr: Option<&RuntimeEndpoint>,
) -> Option<types::TNetworkAddress> {
    fe_addr
        .map(|endpoint| types::TNetworkAddress::new(endpoint.host().to_string(), endpoint.port()))
        .or_else(disk_report::latest_fe_addr)
}

pub(crate) fn fetch_table_schema_for_lake_scan(
    fe_addr: Option<&RuntimeEndpoint>,
    db_id: i64,
    table_id: i64,
    schema_id: i64,
    tablet_id: Option<i64>,
    query_id: Option<UniqueId>,
    local_schema: Option<&TTabletSchema>,
) -> Result<TTabletSchema, String> {
    let query_id = query_id.ok_or_else(|| {
        format!(
            "missing query_id for FE getTableSchema scan request: db_id={} table_id={} schema_id={}",
            db_id, table_id, schema_id
        )
    })?;
    let fe_addr = scan_frontend_transport_address(fe_addr).ok_or_else(|| {
        "missing FE address for getTableSchema (coord is absent and heartbeat cache is empty)"
            .to_string()
    })?;
    TableSchemaService::shared().fetch(
        TableSchemaFetchRequest {
            fe_addr,
            db_id,
            table_id,
            schema_id,
            source: frontend_service::TTableSchemaRequestSource::SCAN,
            tablet_id,
            query_id: Some(types::TUniqueId::new(query_id.hi, query_id.lo)),
            txn_id: None,
        },
        local_schema,
    )
}

fn validate_request(request: &TableSchemaFetchRequest) -> Result<(), String> {
    if request.schema_id <= 0 {
        return Err(format!(
            "invalid schema_id for FE getTableSchema: db_id={} table_id={} schema_id={}",
            request.db_id, request.table_id, request.schema_id
        ));
    }
    match request.source {
        frontend_service::TTableSchemaRequestSource::SCAN => {
            if request.query_id.is_none() {
                return Err(format!(
                    "missing query_id for FE getTableSchema scan request: db_id={} table_id={} schema_id={}",
                    request.db_id, request.table_id, request.schema_id
                ));
            }
        }
        frontend_service::TTableSchemaRequestSource::LOAD => {
            if request.txn_id.is_none() {
                return Err(format!(
                    "missing txn_id for FE getTableSchema load request: db_id={} table_id={} schema_id={}",
                    request.db_id, request.table_id, request.schema_id
                ));
            }
        }
        _ => {
            return Err(format!(
                "unsupported table schema request source: {}",
                request.source.0
            ));
        }
    }
    Ok(())
}

fn ensure_ok_status(status: &TStatus, context: &str) -> Result<(), TableSchemaError> {
    if status.status_code == status_code::TStatusCode::OK {
        return Ok(());
    }
    let detail = status
        .error_msgs
        .as_ref()
        .map(|msgs| msgs.join("; "))
        .unwrap_or_default();
    let message = if detail.is_empty() {
        format!("{context}: {:?}", status)
    } else {
        format!("{context}: status={:?}, error={detail}", status.status_code)
    };
    if is_table_not_found_status(status) {
        Err(TableSchemaError::table_not_found(message))
    } else {
        Err(TableSchemaError::other(message))
    }
}

fn is_table_not_found_status(status: &TStatus) -> bool {
    if status.status_code == status_code::TStatusCode::NOT_FOUND {
        return true;
    }
    status
        .error_msgs
        .as_ref()
        .map(|msgs| {
            msgs.iter().any(|msg| {
                let lower = msg.to_ascii_lowercase();
                lower.contains("table") && lower.contains("not found")
            })
        })
        .unwrap_or(false)
}

fn build_flight_key(request: &TableSchemaFetchRequest, isolated_retry: bool) -> String {
    if !isolated_retry {
        return format!(
            "shared:{}:{}:{}:{}:{}",
            format_addr(&request.fe_addr),
            request.db_id,
            request.table_id,
            request.schema_id,
            source_label(request.source)
        );
    }
    match request.source {
        frontend_service::TTableSchemaRequestSource::SCAN => {
            let query_id = request
                .query_id
                .as_ref()
                .map(|id| format!("{}:{}", id.hi, id.lo))
                .unwrap_or_else(|| "missing".to_string());
            format!(
                "isolated-scan:{}:{}:{}:{}:{}",
                format_addr(&request.fe_addr),
                request.db_id,
                request.table_id,
                request.schema_id,
                query_id
            )
        }
        frontend_service::TTableSchemaRequestSource::LOAD => format!(
            "isolated-load:{}:{}:{}:{}:{}",
            format_addr(&request.fe_addr),
            request.db_id,
            request.table_id,
            request.schema_id,
            request.txn_id.unwrap_or_default()
        ),
        _ => format!(
            "isolated-unknown:{}:{}:{}:{}:{}",
            format_addr(&request.fe_addr),
            request.db_id,
            request.table_id,
            request.schema_id,
            request.source.0
        ),
    }
}

fn format_addr(fe_addr: &types::TNetworkAddress) -> String {
    format!("{}:{}", fe_addr.hostname, fe_addr.port)
}

fn source_label(source: frontend_service::TTableSchemaRequestSource) -> &'static str {
    match source {
        frontend_service::TTableSchemaRequestSource::SCAN => "SCAN",
        frontend_service::TTableSchemaRequestSource::LOAD => "LOAD",
        _ => "UNKNOWN",
    }
}

#[derive(Default)]
struct SingleFlightGroup {
    entries: Mutex<HashMap<String, Arc<SingleFlightEntry>>>,
}

struct SingleFlightEntry {
    state: Mutex<SingleFlightState>,
    cv: Condvar,
}

enum SingleFlightState {
    Running,
    Ready(Result<TTabletSchema, TableSchemaError>),
}

impl Default for SingleFlightEntry {
    fn default() -> Self {
        Self {
            state: Mutex::new(SingleFlightState::Running),
            cv: Condvar::new(),
        }
    }
}

impl SingleFlightGroup {
    fn execute<F>(&self, key: String, op: F) -> (Result<TTabletSchema, TableSchemaError>, bool)
    where
        F: FnOnce() -> Result<TTabletSchema, TableSchemaError>,
    {
        let (entry, shared) = {
            let mut guard = self.entries.lock().expect("table schema flights lock");
            if let Some(entry) = guard.get(&key) {
                (Arc::clone(entry), true)
            } else {
                let entry = Arc::new(SingleFlightEntry::default());
                guard.insert(key.clone(), Arc::clone(&entry));
                (entry, false)
            }
        };

        if shared {
            let mut state = entry.state.lock().expect("table schema flight state");
            loop {
                match &*state {
                    SingleFlightState::Ready(result) => return (result.clone(), true),
                    SingleFlightState::Running => {
                        state = entry.cv.wait(state).expect("table schema flight wait");
                    }
                }
            }
        }

        let result = op();
        {
            let mut state = entry.state.lock().expect("table schema flight state");
            *state = SingleFlightState::Ready(result.clone());
            entry.cv.notify_all();
        }
        let mut guard = self.entries.lock().expect("table schema flights lock");
        if guard
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            guard.remove(&key);
        }
        (result, false)
    }
}
