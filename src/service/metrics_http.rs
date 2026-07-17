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
use std::time::Duration;

use axum::extract::Query;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, TextEncoder,
    register_histogram, register_int_counter, register_int_gauge, register_int_gauge_vec,
};

use crate::coordinator::cluster::{BackendState, backend_registry};

static FRAGMENT_SCHEDULED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "novarocks_fragment_scheduled_total",
        "Total number of plan fragment instances scheduled to backends."
    )
    .expect("register novarocks_fragment_scheduled_total")
});

static EXCHANGE_SHUFFLE_BYTES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "novarocks_exchange_shuffle_bytes_total",
        "Total number of exchange shuffle payload bytes sent."
    )
    .expect("register novarocks_exchange_shuffle_bytes_total")
});

static HEARTBEAT_RTT_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(HistogramOpts::new(
        "novarocks_heartbeat_rtt_seconds",
        "Backend heartbeat round-trip time in seconds."
    ))
    .expect("register novarocks_heartbeat_rtt_seconds")
});

static LIVE_BACKENDS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "novarocks_live_backends",
        "Number of live backends in the FE registry."
    )
    .expect("register novarocks_live_backends")
});

static BACKENDS_BY_STATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backends",
        "Number of backends by registry state.",
        &["state"]
    )
    .expect("register novarocks_backends")
});

pub(crate) fn observe_fragment_scheduled() {
    Lazy::force(&FRAGMENT_SCHEDULED_TOTAL).inc();
}

pub(crate) fn observe_exchange_shuffle_bytes(bytes: usize) {
    Lazy::force(&EXCHANGE_SHUFFLE_BYTES_TOTAL).inc_by(bytes as u64);
}

pub(crate) fn observe_heartbeat_rtt(duration: Duration) {
    Lazy::force(&HEARTBEAT_RTT_SECONDS).observe(duration.as_secs_f64());
}

pub(crate) async fn handle_metrics(Query(params): Query<HashMap<String, String>>) -> Response {
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

pub(crate) fn render_metrics() -> Result<String, String> {
    refresh_backend_gauges();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&prometheus::gather(), &mut buf)
        .map_err(|e| format!("encode prometheus metrics failed: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("prometheus metrics were not utf-8: {e}"))
}

pub(crate) fn render_metrics_json() -> Result<String, String> {
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
    Lazy::force(&FRAGMENT_SCHEDULED_TOTAL);
    Lazy::force(&EXCHANGE_SHUFFLE_BYTES_TOTAL);
    Lazy::force(&HEARTBEAT_RTT_SECONDS);
    Lazy::force(&LIVE_BACKENDS);
    Lazy::force(&BACKENDS_BY_STATE);

    let entries = backend_registry()
        .map(|registry| registry.snapshot())
        .unwrap_or_default();
    LIVE_BACKENDS.set(
        entries
            .iter()
            .filter(|entry| entry.state == BackendState::Live)
            .count() as i64,
    );

    for state in [
        BackendState::Registering,
        BackendState::Live,
        BackendState::Lost,
        BackendState::Decommissioning,
    ] {
        let state_name = backend_state_label(state);
        let count = entries.iter().filter(|entry| entry.state == state).count() as i64;
        BACKENDS_BY_STATE
            .with_label_values(&[state_name])
            .set(count);
    }
}

fn backend_state_label(state: BackendState) -> &'static str {
    match state {
        BackendState::Registering => "registering",
        BackendState::Live => "live",
        BackendState::Lost => "lost",
        BackendState::Decommissioning => "decommissioning",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_metrics_include_cluster_core_names() {
        observe_fragment_scheduled();
        observe_exchange_shuffle_bytes(7);
        observe_heartbeat_rtt(Duration::from_millis(5));

        let body = render_metrics().expect("render metrics");
        assert!(body.contains("novarocks_fragment_scheduled_total"));
        assert!(body.contains("novarocks_exchange_shuffle_bytes_total"));
        assert!(body.contains("novarocks_heartbeat_rtt_seconds"));
        assert!(body.contains("novarocks_live_backends"));
        assert!(body.contains("novarocks_backends"));
    }

    #[test]
    fn rendered_json_metrics_are_fe_metrics_compatible_array() {
        observe_fragment_scheduled();
        let body = render_metrics_json().expect("render json metrics");
        let value: serde_json::Value = serde_json::from_str(&body).expect("parse json metrics");
        let rows = value.as_array().expect("json metrics array");
        assert!(rows.iter().any(|row| {
            row.get("tags")
                .and_then(|tags| tags.get("metric"))
                .and_then(serde_json::Value::as_str)
                == Some("novarocks_fragment_scheduled_total")
        }));
    }
}
