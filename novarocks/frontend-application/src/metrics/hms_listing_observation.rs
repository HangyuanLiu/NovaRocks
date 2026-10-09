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

//! Feature-only route hosted by the original management listener owner.

use crate::server::HmsListingObservationHandler;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};

const REQUEST_LIMIT: usize = 1024;
const RESPONSE_LIMIT: usize = 1024 * 1024;

pub(super) fn router(handler: Option<HmsListingObservationHandler>) -> Router {
    match handler {
        None => Router::new(),
        Some(handler) => Router::new()
            .route("/debug/hms-listing-observation", post(observe))
            .layer(DefaultBodyLimit::max(REQUEST_LIMIT))
            .with_state(handler),
    }
}
async fn observe(State(handler): State<HmsListingObservationHandler>, body: Bytes) -> Response {
    match handler(&body) {
        Ok(response) if response.len() <= RESPONSE_LIMIT => {
            ([(header::CONTENT_TYPE, "application/json")], response).into_response()
        }
        Ok(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "HMS observation response exceeds its limit",
        )
            .into_response(),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn absent_handler_has_no_route_and_oversized_body_never_reaches_owner() {
        let request = || {
            Request::post("/debug/hms-listing-observation")
                .body(Body::from("{}"))
                .unwrap()
        };
        assert_eq!(
            router(None).oneshot(request()).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let captured = calls.clone();
        let handler: HmsListingObservationHandler = Arc::new(move |_| {
            captured.fetch_add(1, Ordering::SeqCst);
            Ok(b"{}".to_vec())
        });
        let response = router(Some(handler))
            .oneshot(
                Request::post("/debug/hms-listing-observation")
                    .body(Body::from(vec![0; REQUEST_LIMIT + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn route_passes_original_request_and_refuses_export_failure() {
        let handler: HmsListingObservationHandler = Arc::new(|body| {
            if body == br#"{"operation":"snapshot"}"# {
                Ok(br#"{"domain":"original"}"#.to_vec())
            } else {
                Err("original observation refused")
            }
        });
        let app = router(Some(handler));
        let response = app
            .clone()
            .oneshot(
                Request::post("/debug/hms-listing-observation")
                    .body(Body::from(br#"{"operation":"snapshot"}"#.as_slice()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(
            to_bytes(response.into_body(), RESPONSE_LIMIT)
                .await
                .unwrap(),
            br#"{"domain":"original"}"#.as_slice()
        );
        let response = app
            .oneshot(
                Request::post("/debug/hms-listing-observation")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
