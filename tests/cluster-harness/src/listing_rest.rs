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

//! Controlled standard REST Catalog facts for SDK listing acceptance.
//!
//! This is a protocol fixture, not a replacement for real-provider acceptance.
//! It owns no row data. Target allocator samples exclude this process's memory.

use anyhow::{Context, Result, ensure};
use axum::Router;
use axum::extract::State;
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;

pub const NAMESPACES: usize = 32;
pub const MEMBERS: usize = 512;
pub const PAGE_SIZE: usize = 256;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ListingMode {
    #[default]
    Empty,
    Normal,
    ExactEntries,
    PagedOverflow,
    TerminalOverflow,
    NameOverflow,
    TokenOverflow,
    TokenCycle,
    PageOverflow,
    Delayed,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ListingSnapshot {
    pub active_listing_requests: u64,
    pub peak_listing_requests: u64,
    pub namespace_pages: u64,
    pub table_pages: u64,
    pub view_pages: u64,
    pub emitted_entries: u64,
    pub emitted_name_bytes: u64,
    pub table_loads: u64,
    pub destructive_mutations: u64,
}

#[derive(Default)]
struct Facts {
    mode: ListingMode,
    audit: ListingSnapshot,
    removed_tables: BTreeSet<String>,
    removed_views: BTreeSet<String>,
    removed_namespaces: BTreeSet<String>,
}

struct FixtureState {
    facts: Mutex<Facts>,
}

/// The scenario owns the listener, its runtime and every active response.
pub struct ListingRestFixture {
    endpoint: String,
    state: Arc<FixtureState>,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl ListingRestFixture {
    pub fn start() -> Result<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(FixtureState {
            facts: Mutex::new(Facts::default()),
        });
        let server_state = state.clone();
        let (stop, stopped) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("listing-rest-fixture".into())
            .spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()?
                    .block_on(async {
                        let listener = tokio::net::TcpListener::from_std(listener)?;
                        axum::serve(
                            listener,
                            Router::new().fallback(serve).with_state(server_state),
                        )
                        .with_graceful_shutdown(async {
                            let _ = stopped.await;
                        })
                        .await?;
                        Ok(())
                    })
            })?;
        Ok(Self {
            endpoint,
            state,
            stop: Some(stop),
            thread: Some(thread),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Reset only fixture facts between completed phases, never target owners.
    pub fn set_mode(&self, mode: ListingMode) -> Result<()> {
        let mut facts = self
            .state
            .facts
            .lock()
            .map_err(|_| anyhow::anyhow!("listing fixture lock poisoned"))?;
        ensure!(
            facts.audit.active_listing_requests == 0,
            "listing fixture phase still has active responses"
        );
        *facts = Facts {
            mode,
            ..Default::default()
        };
        Ok(())
    }

    pub fn snapshot(&self) -> Result<ListingSnapshot> {
        Ok(self
            .state
            .facts
            .lock()
            .map_err(|_| anyhow::anyhow!("listing fixture lock poisoned"))?
            .audit
            .clone())
    }

    pub fn shutdown(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| anyhow::anyhow!("listing fixture thread panicked"))??;
        }
        Ok(())
    }
}

impl Drop for ListingRestFixture {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

struct ActiveListing(Arc<FixtureState>);
impl Drop for ActiveListing {
    fn drop(&mut self) {
        if let Ok(mut facts) = self.0.facts.lock() {
            facts.audit.active_listing_requests -= 1;
        }
    }
}

async fn serve(State(state): State<Arc<FixtureState>>, method: Method, uri: Uri) -> Response {
    match reply(state, method, uri).await {
        Ok(response) => response,
        Err(_) => (StatusCode::BAD_REQUEST, axum::Json(json!({"error":{"code":400,"type":"BadRequestException","message":"invalid controlled listing request"}}))).into_response(),
    }
}

async fn reply(state: Arc<FixtureState>, method: Method, uri: Uri) -> Result<Response> {
    let segments = uri.path().trim_matches('/').split('/').collect::<Vec<_>>();
    if segments.as_slice() == ["v1", "config"] {
        return Ok(axum::Json(json!({"defaults":{},"overrides":{}})).into_response());
    }
    ensure!(
        segments.first() == Some(&"v1") && segments.get(1) == Some(&"namespaces"),
        "unknown fixture route"
    );
    let namespace = segments.get(2).copied().unwrap_or("");
    if method == Method::HEAD {
        let facts = state
            .facts
            .lock()
            .map_err(|_| anyhow::anyhow!("listing fixture lock poisoned"))?;
        let status = if facts.removed_namespaces.contains(namespace) {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::OK
        };
        return Ok(status.into_response());
    }
    if method == Method::DELETE {
        let mut facts = state
            .facts
            .lock()
            .map_err(|_| anyhow::anyhow!("listing fixture lock poisoned"))?;
        if segments.len() == 3 {
            facts.removed_namespaces.insert(namespace.to_string());
        } else {
            ensure!(segments.len() == 5, "invalid destructive route");
            let member = format!("{namespace}/{}", segments[4]);
            match segments[3] {
                "tables" => {
                    facts.removed_tables.insert(member);
                }
                "views" => {
                    facts.removed_views.insert(member);
                }
                _ => anyhow::bail!("invalid destructive object"),
            }
        }
        facts.audit.destructive_mutations += 1;
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    ensure!(method == Method::GET, "unsupported fixture method");
    if segments.len() == 3 {
        return Ok(axum::Json(json!({"namespace":[namespace],"properties":{}})).into_response());
    }
    if segments.len() == 5 && segments[3] == "tables" {
        let name = segments[4];
        let namespace_index = namespace
            .strip_prefix("cl_ns_")
            .context("invalid namespace")?
            .parse::<usize>()?;
        let member_index = name
            .strip_prefix("cl_table_")
            .context("invalid table")?
            .parse::<usize>()?;
        ensure!(
            namespace_index < NAMESPACES && member_index < MEMBERS,
            "table outside normal fixture"
        );
        state
            .facts
            .lock()
            .map_err(|_| anyhow::anyhow!("listing fixture lock poisoned"))?
            .audit
            .table_loads += 1;
        return Ok(axum::Json(json!({"metadata-location":format!("s3://cl-fixture/warehouse/{namespace}/{name}/metadata/00000.json"), "metadata": {
            "format-version":2,"table-uuid":format!("00000000-0000-7000-8000-{:012x}", namespace_index * MEMBERS + member_index),
            "location":format!("s3://cl-fixture/warehouse/{namespace}/{name}"),"last-sequence-number":0,"last-updated-ms":1700000000000_u64,
            "last-column-id":1,"current-schema-id":0,"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}],
            "default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],"last-partition-id":999,
            "default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"properties":{},"snapshots":[],"snapshot-log":[],"metadata-log":[]
        }})).into_response());
    }
    let kind = if segments.len() == 2 {
        "namespaces"
    } else {
        ensure!(segments.len() == 4, "invalid listing route");
        segments[3]
    };
    ensure!(
        ["namespaces", "tables", "views"].contains(&kind),
        "unknown listing kind"
    );
    let token = uri
        .query()
        .unwrap_or("")
        .split('&')
        .find_map(|q| q.strip_prefix("pageToken="))
        .unwrap_or("");
    let mode = {
        let mut facts = state
            .facts
            .lock()
            .map_err(|_| anyhow::anyhow!("listing fixture lock poisoned"))?;
        facts.audit.active_listing_requests += 1;
        facts.audit.peak_listing_requests = facts
            .audit
            .peak_listing_requests
            .max(facts.audit.active_listing_requests);
        match kind {
            "namespaces" => facts.audit.namespace_pages += 1,
            "tables" => facts.audit.table_pages += 1,
            _ => facts.audit.view_pages += 1,
        }
        facts.mode
    };
    let _active = ActiveListing(state.clone());
    let delay = if mode == ListingMode::Delayed {
        Duration::from_secs(2)
    } else {
        Duration::from_millis(15)
    };
    tokio::time::sleep(delay).await;
    let requested = uri
        .query()
        .unwrap_or("")
        .split('&')
        .find_map(|q| q.strip_prefix("pageSize="));
    let page_size = requested
        .map(str::parse::<usize>)
        .transpose()?
        .unwrap_or(PAGE_SIZE);
    ensure!(
        (1..=PAGE_SIZE).contains(&page_size),
        "page request outside fixture support"
    );
    let (names, continuation) = listing_page(mode, kind, token, page_size)?;
    let mut facts = state
        .facts
        .lock()
        .map_err(|_| anyhow::anyhow!("listing fixture lock poisoned"))?;
    let names = names
        .into_iter()
        .filter(|name| match kind {
            "namespaces" => !facts.removed_namespaces.contains(name),
            "tables" => !facts
                .removed_tables
                .contains(&format!("{namespace}/{name}")),
            _ => !facts.removed_views.contains(&format!("{namespace}/{name}")),
        })
        .collect::<Vec<_>>();
    facts.audit.emitted_entries += names.len() as u64;
    facts.audit.emitted_name_bytes += names.iter().map(|s| s.len() as u64).sum::<u64>();
    drop(facts);
    let mut body = if kind == "namespaces" {
        json!({"namespaces":names.iter().map(|n| vec![n]).collect::<Vec<_>>()})
    } else {
        json!({"identifiers":names.iter().map(|n| json!({"namespace":[namespace],"name":n})).collect::<Vec<_>>()})
    };
    if let Some(token) = continuation {
        body["next-page-token"] = Value::String(token);
    }
    Ok(axum::Json(body).into_response())
}

fn listing_page(
    mode: ListingMode,
    kind: &str,
    token: &str,
    page_size: usize,
) -> Result<(Vec<String>, Option<String>)> {
    if mode == ListingMode::Empty {
        return Ok((Vec::new(), None));
    }
    let prefix = match kind {
        "namespaces" => "cl_ns_",
        "tables" => "cl_table_",
        _ => "cl_view_",
    };
    if kind != "namespaces" {
        match mode {
            ListingMode::TokenOverflow => {
                return Ok((vec![format!("{prefix}000000")], Some("x".repeat(4097))));
            }
            ListingMode::TokenCycle => {
                return Ok((
                    vec![format!(
                        "{prefix}{}",
                        if token.is_empty() {
                            "000000"
                        } else if token == "a" {
                            "000001"
                        } else {
                            "000002"
                        }
                    )],
                    Some(if token == "a" { "b" } else { "a" }.into()),
                ));
            }
            ListingMode::PageOverflow => {
                return Ok((
                    Vec::new(),
                    Some((token.parse::<usize>().unwrap_or(0) + 1).to_string()),
                ));
            }
            ListingMode::NameOverflow => {
                if !token.is_empty() {
                    return Ok((vec!["x".into()], None));
                }
                let names = (0..256)
                    .map(|n| {
                        let prefix = format!("cl_{n:06}_");
                        format!("{prefix}{}", "n".repeat(65536 - prefix.len()))
                    })
                    .collect();
                return Ok((names, Some("last".into())));
            }
            _ => {}
        }
    }
    let total = if kind == "namespaces" {
        NAMESPACES
    } else {
        match mode {
            ListingMode::ExactEntries => 65536,
            ListingMode::PagedOverflow | ListingMode::TerminalOverflow => 65537,
            _ => MEMBERS,
        }
    };
    let start = if token.is_empty() {
        0
    } else {
        token.parse::<usize>()?
    };
    ensure!(start <= total, "page token outside fixture");
    let end = if mode == ListingMode::TerminalOverflow && kind != "namespaces" {
        total
    } else {
        (start + page_size).min(total)
    };
    let names = (start..end)
        .map(|n| {
            if kind == "namespaces" {
                format!("{prefix}{n:04}")
            } else {
                format!("{prefix}{n:06}")
            }
        })
        .collect();
    Ok((names, (end < total).then(|| end.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn controlled_rest_listener_serves_catalog_and_load_protocols() {
        let fixture = ListingRestFixture::start().unwrap();
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let config: Value = client
            .get(format!("{}/v1/config", fixture.endpoint()))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(config, json!({"defaults":{},"overrides":{}}));
        fixture.set_mode(ListingMode::Normal).unwrap();
        let table: Value = client
            .get(format!(
                "{}/v1/namespaces/cl_ns_0000/tables/cl_table_000000",
                fixture.endpoint()
            ))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(table["metadata"]["format-version"], 2);
        assert_eq!(fixture.snapshot().unwrap().table_loads, 1);
        let page: Value = client
            .get(format!(
                "{}/v1/namespaces/cl_ns_0000/tables?pageSize=128",
                fixture.endpoint()
            ))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(page["identifiers"].as_array().unwrap().len(), 128);
        assert_eq!(page["next-page-token"], "128");
    }
    #[test]
    fn controlled_listing_pages_preserve_frozen_boundaries() {
        let (first, token) = listing_page(ListingMode::Normal, "tables", "", PAGE_SIZE).unwrap();
        assert_eq!(first.len(), PAGE_SIZE);
        let (last, end) =
            listing_page(ListingMode::Normal, "tables", &token.unwrap(), PAGE_SIZE).unwrap();
        assert_eq!(last.len(), PAGE_SIZE);
        assert!(end.is_none());
        let (terminal, end) =
            listing_page(ListingMode::TerminalOverflow, "tables", "", PAGE_SIZE).unwrap();
        assert_eq!(terminal.len(), 65537);
        assert!(end.is_none());
        let (names, token) =
            listing_page(ListingMode::NameOverflow, "tables", "", PAGE_SIZE).unwrap();
        assert!(names.iter().all(|name| name.len() == 65536));
        let (last, _) = listing_page(
            ListingMode::NameOverflow,
            "tables",
            &token.unwrap(),
            PAGE_SIZE,
        )
        .unwrap();
        assert_eq!(
            names.iter().map(String::len).sum::<usize>() + last[0].len(),
            16777217
        );
    }
}
