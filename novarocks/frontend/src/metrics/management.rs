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

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get};

use crate::coordinator::QueryLifecycleConvergenceReader;

use super::{FrontendMetricsRegistry, handle_metrics};

/// Builds the complete Frontend management HTTP surface. Native report gRPC
/// must not compose any management routes.
pub(crate) fn frontend_management_router(
    registry: Arc<FrontendMetricsRegistry>,
    convergence_reader: Arc<dyn QueryLifecycleConvergenceReader>,
) -> Router {
    frontend_management_router_with_debug(
        registry,
        convergence_reader,
        crate::native::report_server::lifecycle_convergence_debug_enabled(),
    )
}

fn frontend_management_router_with_debug(
    registry: Arc<FrontendMetricsRegistry>,
    convergence_reader: Arc<dyn QueryLifecycleConvergenceReader>,
    debug_enabled: bool,
) -> Router {
    let router = Router::new().route("/metrics", get(handle_metrics));
    let router = if debug_enabled {
        router.route(
            crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH,
            get(move || latest_lifecycle_convergence_snapshot(Arc::clone(&convergence_reader))),
        )
    } else {
        router
    };
    router.with_state(registry)
}

async fn latest_lifecycle_convergence_snapshot(
    reader: Arc<dyn QueryLifecycleConvergenceReader>,
) -> axum::response::Response {
    let Some(snapshot) = reader.latest_convergence_snapshot() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    Json(crate::native::report_server::lifecycle_convergence_debug_json(snapshot)).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    use super::{FrontendMetricsRegistry, frontend_management_router_with_debug};
    use crate::coordinator::{QueryLifecycleConvergenceReader, QueryLifecycleConvergenceSnapshot};

    struct EmptyConvergenceReader;

    impl QueryLifecycleConvergenceReader for EmptyConvergenceReader {
        fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
            None
        }
    }

    fn router(debug_enabled: bool) -> axum::Router {
        frontend_management_router_with_debug(
            FrontendMetricsRegistry::new().expect("create frontend metrics registry"),
            Arc::new(EmptyConvergenceReader),
            debug_enabled,
        )
    }

    #[tokio::test]
    async fn management_router_serves_metrics_and_gates_lifecycle_debug_route() {
        let metrics = router(false)
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(metrics.status(), StatusCode::OK);

        let debug_off = router(false)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH)
                    .body(Body::empty())
                    .expect("debug-off request"),
            )
            .await
            .expect("debug-off response");
        assert_eq!(debug_off.status(), StatusCode::NOT_FOUND);

        let debug_on = router(true)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH)
                    .body(Body::empty())
                    .expect("debug-on request"),
            )
            .await
            .expect("debug-on response");
        assert_eq!(debug_on.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
