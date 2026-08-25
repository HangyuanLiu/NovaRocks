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
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

mod http;
mod management;
pub mod query_lifecycle;
pub(crate) use http::MetricsHttpServer;
pub use query_lifecycle::FrontendQueryLifecycleMetricsSnapshot;

static FRAGMENT_SCHEDULED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_fragment_scheduled_total",
        "Total number of plan fragment instances scheduled to backends.",
    ))
    .expect("register novarocks_fragment_scheduled_total")
});

static HEARTBEAT_RTT_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "novarocks_heartbeat_rtt_seconds",
        "Backend heartbeat round-trip time in seconds.",
    ))
    .expect("register novarocks_heartbeat_rtt_seconds")
});

static LIVE_BACKENDS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_live_backends",
        "Number of live backends in the FE registry.",
    ))
    .expect("register novarocks_live_backends")
});

static BACKENDS_BY_STATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backends",
            "Number of backends by registry state.",
        ),
        &["state"],
    )
    .expect("register novarocks_backends")
});
static FRONTEND_QUERY_LIFECYCLE_ATTEMPTS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_query_lifecycle_active_attempts",
        "Number of frontend-owned query lifecycle attempts.",
    ))
    .expect("register novarocks_frontend_query_lifecycle_active_attempts")
});

static FRONTEND_QUERY_LIFECYCLE_INIT: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_query_lifecycle_init_total",
            "Cumulative frontend query initialization outcomes.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_frontend_query_lifecycle_init_total")
});

static FRONTEND_QUERY_LIFECYCLE_CONTROL: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_query_lifecycle_control_total",
            "Cumulative frontend query control outcomes.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_frontend_query_lifecycle_control_total")
});

static FRONTEND_QUERY_LIFECYCLE_LATENCY: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_query_lifecycle_latency_micros",
            "Cumulative frontend query lifecycle latency and sample counts.",
        ),
        &["phase", "measure"],
    )
    .expect("register novarocks_frontend_query_lifecycle_latency_micros")
});

/// Explicit collector registry owned by one Frontend management host.
///
/// This keeps FE's metrics surface role-local even when a process supervises
/// both Frontend and Backend application hosts.
pub(crate) struct FrontendMetricsRegistry {
    registry: Registry,
}

impl FrontendMetricsRegistry {
    pub(crate) fn new() -> Result<Arc<Self>, String> {
        refresh_frontend_gauges();
        let registry = Registry::new();
        for collector in [
            Box::new(FRAGMENT_SCHEDULED_TOTAL.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(HEARTBEAT_RTT_SECONDS.clone()),
            Box::new(LIVE_BACKENDS.clone()),
            Box::new(BACKENDS_BY_STATE.clone()),
            Box::new(FRONTEND_QUERY_LIFECYCLE_ATTEMPTS.clone()),
            Box::new(FRONTEND_QUERY_LIFECYCLE_INIT.clone()),
            Box::new(FRONTEND_QUERY_LIFECYCLE_CONTROL.clone()),
            Box::new(FRONTEND_QUERY_LIFECYCLE_LATENCY.clone()),
        ] {
            registry
                .register(collector)
                .map_err(|error| format!("register frontend metric collector failed: {error}"))?;
        }
        crate::catalog_projection_metrics::register_collectors(&registry)?;
        Ok(Arc::new(Self { registry }))
    }

    fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }
}

pub(crate) fn observe_fragments_scheduled(count: usize) {
    Lazy::force(&FRAGMENT_SCHEDULED_TOTAL).inc_by(count as u64);
}

/// Record an FE-owned backend heartbeat observation without coupling the
/// heartbeat transport to Core's former gRPC client implementation.
pub fn observe_backend_heartbeat_rtt(duration: Duration) {
    Lazy::force(&HEARTBEAT_RTT_SECONDS).observe(duration.as_secs_f64());
}
/// Publishes already-counted backend registry states as neutral scalars.
///
/// Backend membership authority is owned outside this module (`ADR-0013`), so
/// the metrics surface deliberately names no membership type: it accepts the
/// counts and owns only the `novarocks_backends` label set they map onto. That
/// keeps the membership owner and this listener independently relocatable.
pub(crate) fn publish_backend_topology_metrics(
    registering: usize,
    live: usize,
    incompatible: usize,
    lost: usize,
    decommissioning: usize,
) {
    Lazy::force(&LIVE_BACKENDS).set(live as i64);
    for (state_name, count) in [
        ("registering", registering),
        ("live", live),
        ("incompatible", incompatible),
        ("lost", lost),
        ("decommissioning", decommissioning),
    ] {
        BACKENDS_BY_STATE
            .with_label_values(&[state_name])
            .set(count as i64);
    }
}
pub fn publish_frontend_query_lifecycle_metrics(snapshot: FrontendQueryLifecycleMetricsSnapshot) {
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_ATTEMPTS).set(snapshot.active_attempts as i64);
    for (outcome, count) in [
        ("applied", snapshot.init_applied),
        ("already_applied", snapshot.init_idempotent),
        ("failed", snapshot.init_failed),
        ("uncertain_cleanup", snapshot.init_uncertain_cleanup),
        ("manifest_conflict", snapshot.manifest_conflicts),
    ] {
        FRONTEND_QUERY_LIFECYCLE_INIT
            .with_label_values(&[outcome])
            .set(count as i64);
    }
    for (outcome, count) in [
        ("control_ready", snapshot.control_ready),
        ("attach_failed", snapshot.attach_failed),
        ("heartbeat_timeout", snapshot.heartbeat_timeouts),
        ("coordinator_lost", snapshot.coordinator_lost),
        ("local_failure", snapshot.local_failures),
        ("backend_epoch_mismatch", snapshot.backend_epoch_mismatches),
        ("cleanup_failure", snapshot.cleanup_failures),
        (
            "terminal_locally_drained",
            snapshot.terminal_locally_drained,
        ),
        (
            "terminal_snapshot_accepted",
            snapshot.terminal_snapshots_accepted,
        ),
        (
            "terminal_snapshot_idempotent",
            snapshot.terminal_snapshots_idempotent,
        ),
        (
            "terminal_snapshot_conflict",
            snapshot.terminal_snapshot_conflicts,
        ),
        (
            "terminal_finalize_failure",
            snapshot.terminal_finalize_failures,
        ),
    ] {
        FRONTEND_QUERY_LIFECYCLE_CONTROL
            .with_label_values(&[outcome])
            .set(count as i64);
    }
    for (phase, total, samples) in [
        (
            "init",
            snapshot.init_latency_micros_total,
            snapshot.init_latency_samples,
        ),
        (
            "attach",
            snapshot.attach_latency_micros_total,
            snapshot.attach_latency_samples,
        ),
    ] {
        FRONTEND_QUERY_LIFECYCLE_LATENCY
            .with_label_values(&[phase, "total"])
            .set(total as i64);
        FRONTEND_QUERY_LIFECYCLE_LATENCY
            .with_label_values(&[phase, "samples"])
            .set(samples as i64);
    }
}

/// Management HTTP handler for the role-owned Frontend registry.
pub(crate) async fn handle_metrics(
    State(registry): State<Arc<FrontendMetricsRegistry>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if params
        .get("type")
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return match render_metrics_json(registry.as_ref()) {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
        };
    }

    match render_metrics(registry.as_ref()) {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

/// Renders only the collectors explicitly registered by the Frontend host.
pub(crate) fn render_metrics(registry: &FrontendMetricsRegistry) -> Result<String, String> {
    refresh_frontend_gauges();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&registry.gather(), &mut buf)
        .map_err(|e| format!("encode prometheus metrics failed: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("prometheus metrics were not utf-8: {e}"))
}

/// Renders the Frontend registry in the existing JSON form.
pub(crate) fn render_metrics_json(registry: &FrontendMetricsRegistry) -> Result<String, String> {
    refresh_frontend_gauges();
    let mut rows = Vec::new();
    for family in registry.gather() {
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
fn refresh_frontend_gauges() {
    Lazy::force(&FRAGMENT_SCHEDULED_TOTAL);
    Lazy::force(&HEARTBEAT_RTT_SECONDS);
    Lazy::force(&LIVE_BACKENDS);
    Lazy::force(&BACKENDS_BY_STATE);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_ATTEMPTS);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_INIT);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_CONTROL);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_LATENCY);
    ensure_frontend_metric_label_families();
}

/// Make the documented FE metric families observable before their first event
/// without resetting values already published by their application owner.
fn ensure_frontend_metric_label_families() {
    for state in ["registering", "live", "lost", "decommissioning"] {
        let _ = BACKENDS_BY_STATE.get_metric_with_label_values(&[state]);
    }
    for outcome in [
        "applied",
        "already_applied",
        "failed",
        "uncertain_cleanup",
        "manifest_conflict",
    ] {
        let _ = FRONTEND_QUERY_LIFECYCLE_INIT.get_metric_with_label_values(&[outcome]);
    }
    for outcome in [
        "control_ready",
        "attach_failed",
        "heartbeat_timeout",
        "coordinator_lost",
        "local_failure",
        "backend_epoch_mismatch",
        "cleanup_failure",
        "terminal_locally_drained",
        "terminal_snapshot_accepted",
        "terminal_snapshot_idempotent",
        "terminal_snapshot_conflict",
        "terminal_finalize_failure",
    ] {
        let _ = FRONTEND_QUERY_LIFECYCLE_CONTROL.get_metric_with_label_values(&[outcome]);
    }
    for (phase, measure) in [
        ("init", "total"),
        ("init", "samples"),
        ("attach", "total"),
        ("attach", "samples"),
    ] {
        let _ = FRONTEND_QUERY_LIFECYCLE_LATENCY.get_metric_with_label_values(&[phase, measure]);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn frontend_registry() -> Arc<FrontendMetricsRegistry> {
        FrontendMetricsRegistry::new().expect("create frontend metrics registry")
    }

    #[test]
    fn rendered_metrics_include_cluster_core_names() {
        observe_fragments_scheduled(1);
        observe_backend_heartbeat_rtt(Duration::from_millis(5));

        let registry = frontend_registry();
        let body = render_metrics(registry.as_ref()).expect("render metrics");
        assert!(body.contains("novarocks_fragment_scheduled_total"));
        assert!(body.contains("novarocks_heartbeat_rtt_seconds"));
        assert!(body.contains("novarocks_live_backends"));
        assert!(body.contains("novarocks_backends"));
    }

    #[test]
    fn backend_topology_gauges_preserve_the_last_nonzero_frontend_snapshot() {
        publish_backend_topology_metrics(1, 2, 3, 4, 5);

        let registry = frontend_registry();
        let first = render_metrics(registry.as_ref()).expect("render first metrics snapshot");
        let second = render_metrics(registry.as_ref()).expect("render second metrics snapshot");

        for body in [&first, &second] {
            assert!(body.contains("novarocks_live_backends 2"), "{body}");
            assert!(
                body.contains("novarocks_backends{state=\"registering\"} 1"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backends{state=\"live\"} 2"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backends{state=\"incompatible\"} 3"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backends{state=\"lost\"} 4"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backends{state=\"decommissioning\"} 5"),
                "{body}"
            );
        }
    }

    #[test]
    fn rendered_json_metrics_are_fe_metrics_compatible_array() {
        observe_fragments_scheduled(1);
        let registry = frontend_registry();
        let body = render_metrics_json(registry.as_ref()).expect("render json metrics");
        let value: serde_json::Value = serde_json::from_str(&body).expect("parse json metrics");
        let rows = value.as_array().expect("json metrics array");
        assert!(rows.iter().any(|row| {
            row.get("tags")
                .and_then(|tags| tags.get("metric"))
                .and_then(serde_json::Value::as_str)
                == Some("novarocks_fragment_scheduled_total")
        }));
    }

    #[test]
    fn frontend_query_lifecycle_metrics_publish_structured_snapshot() {
        publish_frontend_query_lifecycle_metrics(FrontendQueryLifecycleMetricsSnapshot {
            active_attempts: 2,
            init_applied: 3,
            init_idempotent: 4,
            init_failed: 5,
            init_uncertain_cleanup: 6,
            manifest_conflicts: 7,
            init_latency_micros_total: 8,
            init_latency_samples: 9,
            control_ready: 10,
            attach_failed: 11,
            attach_latency_micros_total: 12,
            attach_latency_samples: 13,
            heartbeat_timeouts: 14,
            coordinator_lost: 15,
            local_failures: 16,
            backend_epoch_mismatches: 17,
            cleanup_failures: 18,
            terminal_locally_drained: 19,
            terminal_snapshots_accepted: 20,
            terminal_snapshots_idempotent: 21,
            terminal_snapshot_conflicts: 22,
            terminal_finalize_failures: 23,
        });

        let registry = frontend_registry();
        let body =
            render_metrics(registry.as_ref()).expect("render frontend query lifecycle metrics");
        assert!(
            body.contains("novarocks_frontend_query_lifecycle_active_attempts 2"),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"terminal_snapshot_accepted\"} 20"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_init_total{outcome=\"already_applied\"} 4"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"heartbeat_timeout\"} 14"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_latency_micros{measure=\"samples\",phase=\"attach\"} 13"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"local_failure\"} 16"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"cleanup_failure\"} 18"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_init_total{outcome=\"uncertain_cleanup\"} 6"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"backend_epoch_mismatch\"} 17"
            ),
            "{body}"
        );
    }

    #[test]
    fn frontend_registry_excludes_foreign_process_collectors() {
        let foreign = prometheus::IntGauge::new(
            "novarocks_backend_foreign_registry_test_gauge",
            "Foreign collector used to prove Frontend registry isolation.",
        )
        .expect("create foreign collector");
        prometheus::default_registry()
            .register(Box::new(foreign))
            .expect("register foreign process collector");

        let registry = frontend_registry();
        let body = render_metrics(registry.as_ref()).expect("render frontend metrics");
        assert!(
            !body.contains("novarocks_backend_foreign_registry_test_gauge"),
            "Frontend endpoint must not gather process-global collector families: {body}"
        );
    }
}
