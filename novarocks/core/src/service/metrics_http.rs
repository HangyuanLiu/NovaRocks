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
use std::net::{SocketAddr, TcpListener};
use std::thread::JoinHandle;

use axum::extract::Query;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use once_cell::sync::Lazy;
use prometheus::{Encoder, IntGaugeVec, TextEncoder, register_int_gauge_vec};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;

/// Dedicated HTTP listener for process metrics.  Native execution continues to
/// expose `/metrics` on its gRPC/HTTP endpoint for compatibility, while
/// backend deployments use this listener at the configured HTTP port.
pub struct MetricsHttpServer {
    shutdown_tx: Option<watch::Sender<bool>>,
    join_handle: Option<JoinHandle<()>>,
}

impl MetricsHttpServer {
    /// Use this when the caller intentionally shares `/metrics` with a
    /// gRPC/HTTP listener on the same configured port.
    pub const fn shared_with_grpc() -> Self {
        Self {
            shutdown_tx: None,
            join_handle: None,
        }
    }

    pub fn start(host: &str, port: u16) -> Result<Self, String> {
        let bind_addr = parse_metrics_bind_addr(host, port)
            .map_err(|error| format!("parse metrics HTTP bind address failed: {error}"))?;
        let listener = TcpListener::bind(bind_addr).map_err(|error| {
            format!("bind metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("configure metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let join_handle = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("failed to build metrics HTTP runtime: {error}");
                    return;
                }
            };
            runtime.block_on(async move {
                let listener = match TokioTcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        eprintln!("failed to create metrics HTTP listener: {error}");
                        return;
                    }
                };
                let app = Router::new().route("/metrics", get(handle_metrics));
                if let Err(error) = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*shutdown_rx.borrow() {
                            if shutdown_rx.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await
                {
                    eprintln!("metrics HTTP server exited with error: {error}");
                }
            });
        });
        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        })
    }

    pub fn stop(mut self) -> Result<(), String> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "metrics HTTP server thread panicked".to_string())?;
        }
        Ok(())
    }
}

static BACKEND_QUERY_LIFECYCLE_ENTRIES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_lifecycle_entries",
        "Number of backend query lifecycle entries by state.",
        &["state"]
    )
    .expect("register novarocks_backend_query_lifecycle_entries")
});

static BACKEND_QUERY_LIFECYCLE_REJECTIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_lifecycle_rejections",
        "Cumulative backend query lifecycle rejections by reason.",
        &["reason"]
    )
    .expect("register novarocks_backend_query_lifecycle_rejections")
});

static BACKEND_QUERY_LIFECYCLE_TERMINATIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_lifecycle_terminations",
        "Cumulative backend query lifecycle terminations by reason.",
        &["reason"]
    )
    .expect("register novarocks_backend_query_lifecycle_terminations")
});

static BACKEND_QUERY_LIFECYCLE_TERMINAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_lifecycle_terminal_total",
        "Cumulative backend query terminal lifecycle outcomes.",
        &["outcome"]
    )
    .expect("register novarocks_backend_query_lifecycle_terminal_total")
});

static BACKEND_QUERY_EXECUTION_RESOURCES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_execution_resources",
        "Backend execution resources as reported by their owning component.",
        &["resource"]
    )
    .expect("register novarocks_backend_query_execution_resources")
});

fn parse_metrics_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|error| format!("parse metrics bind addr '{formatted}' failed: {error}"))
}

pub fn publish_backend_query_lifecycle_metrics(
    snapshot: crate::service::query_lifecycle_metrics::BackendQueryLifecycleMetricsSnapshot,
    termination_reasons: [u64; 6],
) {
    for (state_name, count) in [
        ("initializing", snapshot.initializing),
        ("initialized", snapshot.initialized),
        ("control_attached", snapshot.control_attached),
        ("terminating", snapshot.terminating),
        ("tombstone", snapshot.tombstones),
    ] {
        BACKEND_QUERY_LIFECYCLE_ENTRIES
            .with_label_values(&[state_name])
            .set(count as i64);
    }
    for (reason, count) in [
        ("admission", snapshot.admission_rejected),
        ("init_conflict", snapshot.init_conflicts),
        ("heartbeat_timeout", snapshot.heartbeat_timeouts),
        (
            "terminal_fallback_rejected",
            snapshot.terminal_fallback_rejected,
        ),
    ] {
        BACKEND_QUERY_LIFECYCLE_REJECTIONS
            .with_label_values(&[reason])
            .set(count as i64);
    }
    for (reason, count) in [
        ("coordinator_abort", termination_reasons[0]),
        ("coordinator_finalize", termination_reasons[1]),
        ("coordinator_stream_lost", termination_reasons[2]),
        ("coordinator_heartbeat_timeout", termination_reasons[3]),
        ("local_failure", termination_reasons[4]),
        ("pre_start_timeout", termination_reasons[5]),
    ] {
        BACKEND_QUERY_LIFECYCLE_TERMINATIONS
            .with_label_values(&[reason])
            .set(count as i64);
    }
    for (outcome, count) in [
        ("terminal_fact", snapshot.terminal_facts),
        ("terminal_local_drained", snapshot.terminal_locally_drained),
        ("terminal_record_frozen", snapshot.terminal_records_frozen),
        ("terminal_acknowledged", snapshot.terminal_acknowledged),
        (
            "terminal_retention_expired",
            snapshot.terminal_retention_expired,
        ),
        (
            "terminal_fallback_accepted",
            snapshot.terminal_fallback_accepted,
        ),
        ("terminal_retained", snapshot.terminal_retained as u64),
        (
            "terminal_retained_bytes",
            snapshot.terminal_retained_bytes as u64,
        ),
    ] {
        BACKEND_QUERY_LIFECYCLE_TERMINAL
            .with_label_values(&[outcome])
            .set(count as i64);
    }
}

/// Publish a scalar snapshot after its owner has released its own lock. The
/// metrics layer deliberately holds no execution resource references.
pub fn publish_backend_query_execution_resource(resource: &'static str, value: usize) {
    BACKEND_QUERY_EXECUTION_RESOURCES
        .with_label_values(&[resource])
        .set(value as i64);
}

pub fn publish_backend_query_lifecycle_terminal_limits(capacity: usize, max_bytes: usize) {
    BACKEND_QUERY_LIFECYCLE_TERMINAL
        .with_label_values(&["terminal_retained_capacity"])
        .set(capacity as i64);
    BACKEND_QUERY_LIFECYCLE_TERMINAL
        .with_label_values(&["terminal_max_retained_bytes"])
        .set(max_bytes as i64);
}

/// Shared metrics HTTP handler for role-owned native listeners.
pub async fn handle_metrics(Query(params): Query<HashMap<String, String>>) -> Response {
    if params
        .get("type")
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return match render_metrics_json() {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
        };
    }

    match render_metrics() {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

/// Renders the process metrics in Prometheus text format for any listener
/// host. The caller owns HTTP route composition.
pub fn render_metrics() -> Result<String, String> {
    refresh_backend_gauges();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&prometheus::gather(), &mut buf)
        .map_err(|e| format!("encode prometheus metrics failed: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("prometheus metrics were not utf-8: {e}"))
}

/// Renders the process metrics in the existing JSON form for listener hosts.
pub fn render_metrics_json() -> Result<String, String> {
    refresh_backend_gauges();
    let mut rows = Vec::new();
    for family in prometheus::gather() {
        for metric in family.get_metric() {
            let mut tags = serde_json::Map::new();
            tags.insert(
                "metric".to_string(),
                serde_json::Value::String(family.get_name().to_string()),
            );
            for label in metric.get_label() {
                tags.insert(
                    label.get_name().to_string(),
                    serde_json::Value::String(label.get_value().to_string()),
                );
            }

            if metric.has_counter() {
                rows.push(serde_json::json!({
                    "tags": tags,
                    "value": metric.get_counter().get_value(),
                }));
            } else if metric.has_gauge() {
                rows.push(serde_json::json!({
                    "tags": tags,
                    "value": metric.get_gauge().get_value(),
                }));
            } else if metric.has_untyped() {
                rows.push(serde_json::json!({
                    "tags": tags,
                    "value": metric.get_untyped().get_value(),
                }));
            } else if metric.has_histogram() {
                let histogram = metric.get_histogram();
                let mut count_tags = tags.clone();
                count_tags.insert(
                    "metric".to_string(),
                    serde_json::Value::String(format!("{}_count", family.get_name())),
                );
                rows.push(serde_json::json!({
                    "tags": count_tags,
                    "value": histogram.get_sample_count(),
                }));
                let mut sum_tags = tags;
                sum_tags.insert(
                    "metric".to_string(),
                    serde_json::Value::String(format!("{}_sum", family.get_name())),
                );
                rows.push(serde_json::json!({
                    "tags": sum_tags,
                    "value": histogram.get_sample_sum(),
                }));
            }
        }
    }
    serde_json::to_string(&rows).map_err(|e| format!("encode metrics json failed: {e}"))
}

fn refresh_backend_gauges() {
    novarocks_execution::runtime::fragment::io::exchange_metrics::ensure_exchange_metrics_registered();
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_ENTRIES);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_REJECTIONS);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_TERMINATIONS);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_TERMINAL);
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_bind_addr_accepts_ipv4_and_ipv6_literals() {
        assert_eq!(
            parse_metrics_bind_addr("127.0.0.1", 9070).expect("parse IPv4"),
            "127.0.0.1:9070"
                .parse::<SocketAddr>()
                .expect("IPv4 address")
        );
        assert_eq!(
            parse_metrics_bind_addr("::1", 9070).expect("parse bare IPv6"),
            "[::1]:9070".parse::<SocketAddr>().expect("IPv6 address")
        );
        assert_eq!(
            parse_metrics_bind_addr("[::]", 9070).expect("parse bracketed IPv6"),
            "[::]:9070".parse::<SocketAddr>().expect("IPv6 wildcard")
        );
    }
}
