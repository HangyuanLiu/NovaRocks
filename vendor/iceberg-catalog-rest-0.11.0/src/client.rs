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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, RequestBuilder};
use serde::de::DeserializeOwned;

use crate::RestCatalogConfig;
use crate::auth::{AuthSession, NoopSession};
use crate::request::HttpRequest;
use crate::response::HttpResponse;

/// The catalog's HTTP client, handed to an [`AuthManager`] so its own
/// requests share the catalog's connection pool and configuration.
///
/// [`AuthManager`]: crate::auth::AuthManager
#[derive(Clone)]
pub struct HttpClient {
    client: Client,

    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Whether to disable header redaction in error logs (defaults to false for security).
    disable_header_redaction: bool,
    /// Authenticates everything this client sends. A client handed to an
    /// [`AuthManager`] carries no authentication, since that is what the
    /// manager is about to create.
    ///
    /// [`AuthManager`]: crate::auth::AuthManager
    auth_session: Arc<dyn AuthSession>,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // The inner client is omitted: an injected one may carry secrets in
        // its default headers.
        f.debug_struct("HttpClient")
            .field(
                // `header.*` values may hold secrets (e.g. `authorization`).
                "extra_headers",
                &format_headers_redacted(&self.extra_headers, self.disable_header_redaction),
            )
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// The same client authenticating with `auth_session` instead: a derived
    /// auth session reuses the connection pool and headers of the client it
    /// came from, carrying its own authentication.
    pub fn with_auth_session(&self, auth_session: Arc<dyn AuthSession>) -> Self {
        Self {
            auth_session,
            ..self.clone()
        }
    }

    /// The same client with no authentication, for the requests that must not
    /// carry it — a session refreshing its own token over the client it
    /// authenticates would otherwise recurse.
    pub fn without_auth_session(&self) -> Self {
        self.with_auth_session(Arc::new(NoopSession))
    }

    /// Sends a form-encoded POST and returns the response status and body,
    /// which is what an [`AuthManager`] needs to exchange a credential for a
    /// token. Only `headers` are sent; the catalog's own extra headers are
    /// not merged in.
    ///
    /// Like every request, it carries this client's session; call
    /// [`Self::without_auth_session`] first to send it unauthenticated.
    ///
    /// [`AuthManager`]: crate::auth::AuthManager
    pub async fn post_form(
        &self,
        url: &str,
        headers: &HeaderMap,
        form: &HashMap<&str, &str>,
    ) -> Result<HttpResponse> {
        let mut request = HttpRequest::build(
            self.client
                .request(Method::POST, url)
                .headers(headers.clone())
                .form(form),
        )?;
        // `headers` may carry a `content-type: application/json` that `form`
        // leaves in place.
        request.headers_mut().insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        self.auth_session.authenticate(&mut request).await?;
        let response = self.client.execute(request.into_inner()).await?;
        HttpResponse::read(response).await
    }

    /// Create a new http client.
    pub(crate) fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        Ok(HttpClient {
            client: cfg.client(),
            extra_headers: cfg.extra_headers()?,
            disable_header_redaction: cfg.disable_header_redaction(),
            auth_session: Arc::new(NoopSession),
        })
    }

    /// Update the http client with new configuration.
    ///
    /// If cfg carries new value, we will use cfg instead.
    /// Otherwise, we will keep the old value.
    pub(crate) fn update_with(self, cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = (!cfg.extra_headers()?.is_empty())
            .then(|| cfg.extra_headers())
            .transpose()?
            .unwrap_or(self.extra_headers);
        Ok(HttpClient {
            // `cfg.client()` returns the same shared client.
            client: cfg.client(),
            extra_headers,
            disable_header_redaction: cfg.disable_header_redaction(),
            auth_session: self.auth_session,
        })
    }

    /// Testing only: the session authenticating this client's requests.
    #[cfg(test)]
    pub(crate) fn auth_session(&self) -> &Arc<dyn AuthSession> {
        &self.auth_session
    }

    /// Testing only: the bearer token `session` would attach.
    ///
    /// Authenticates a throwaway request (never sent) and reads the header
    /// back, so it works for any [`AuthSession`].
    #[cfg(test)]
    pub(crate) async fn token(&self) -> Option<String> {
        let mut request = HttpRequest::build(
            self.client
                .request(Method::GET, "http://localhost/token-probe"),
        )
        .ok()?;
        self.auth_session.authenticate(&mut request).await.ok()?;
        let request = request.into_inner();
        request
            .headers()
            .get(reqwest::header::AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")
            .map(str::to_string)
    }

    #[inline]
    pub(crate) fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.extra_headers.clone())
    }

    /// Sends `request` to the Iceberg REST catalog, authenticated by this
    /// client's session.
    pub(crate) async fn query_catalog(&self, mut request: HttpRequest) -> Result<HttpResponse> {
        // Authenticate first, then apply extra headers, so a configured
        // `header.authorization` keeps overriding a token (unchanged behavior).
        self.auth_session.authenticate(&mut request).await?;
        let mut request = request.into_inner();
        request.headers_mut().extend(self.extra_headers.clone());
        HttpResponse::read(self.client.execute(request).await?).await
    }

    /// Returns whether header redaction is disabled for this client.
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.disable_header_redaction
    }
}

/// Deserializes a catalog response into the given [`DeserializeOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) fn deserialize_catalog_response<R: DeserializeOwned>(
    response: HttpResponse,
) -> Result<R> {
    let bytes = response.body();

    // JSON decoder errors can quote response values, so their opaque source is withheld.
    serde_json::from_slice::<R>(bytes).map_err(|_| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("response_body", "[REDACTED]")
    })
}

/// Returns response header names only. Error paths must not make arbitrary
/// REST response values observable because servers may return credentials in
/// a non-success response body or extension header.
pub(crate) fn format_headers_redacted(headers: &HeaderMap, _disable_redaction: bool) -> String {
    format!(
        "{:?}",
        headers.keys().map(|name| name.as_str()).collect::<Vec<_>>()
    )
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) fn deserialize_unexpected_catalog_error(
    response: HttpResponse,
    disable_header_redaction: bool,
) -> Error {
    let err = Error::new(
        ErrorKind::Unexpected,
        "Received response with unexpected status code",
    )
    .with_context("status", response.status().to_string())
    .with_context(
        "headers",
        format_headers_redacted(response.headers(), disable_header_redaction),
    );

    err.with_context("response_body", "[REDACTED]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct StaticSession;

    #[async_trait::async_trait]
    impl AuthSession for StaticSession {
        async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
            request
                .headers_mut()
                .insert("authorization", "Bearer tok".parse().unwrap());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_a_truncated_body_error_names_the_url() {
        // `bytes()` builds its error without a URL, so `read` attaches the one
        // the response came from; otherwise a failure can't be attributed to a
        // catalog. Needs a raw socket: the body has to be cut short.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            // Promise more than is sent, then hang up.
            let _ = std::io::Write::write_all(
                &mut stream,
                b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort",
            );
        });

        let url = format!("http://{addr}/token");
        let err = HttpClient::new(&RestCatalogConfig::builder().uri(url.clone()).build())
            .unwrap()
            .post_form(&url, &HeaderMap::new(), &HashMap::new())
            .await
            .unwrap_err();

        assert!(format!("{err:?}").contains(&url), "{err:?}");
    }

    #[tokio::test]
    async fn test_reading_a_response_keeps_status_headers_and_body() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/token")
            .with_status(418)
            .with_header("x-request-id", "abc123")
            .with_body("brewing")
            .create_async()
            .await;

        let response = HttpClient::new(&RestCatalogConfig::builder().uri(server.url()).build())
            .unwrap()
            .post_form(
                &format!("{}/token", server.url()),
                &HeaderMap::new(),
                &HashMap::new(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), 418);
        assert_eq!(response.headers().get("x-request-id").unwrap(), "abc123");
        assert_eq!(response.body(), b"brewing");
        mock.assert_async().await;
    }

    #[test]
    fn test_unexpected_error_carries_status_and_header_names_only() {
        // Retain status and header names without disclosing response values.
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer leaked".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());
        let response = HttpResponse::new(
            http::StatusCode::IM_A_TEAPOT,
            headers,
            br#"{"error": "nope"}"#.to_vec(),
        );

        let err = format!(
            "{:?}",
            deserialize_unexpected_catalog_error(response, false)
        );

        assert!(err.contains("418"), "{err}");
        assert!(err.contains("x-request-id"), "{err}");
        assert!(!err.contains("abc123"), "{err}");
        assert!(!err.contains("nope"), "{err}");
        assert!(err.contains("[REDACTED]"), "{err}");
        assert!(!err.contains("leaked"), "{err}");
    }

    #[tokio::test]
    async fn test_post_form_carries_the_session_until_it_is_removed() {
        // Every request a client sends carries its session; a caller that
        // needs an unauthenticated one removes the session first.
        let mut server = mockito::Server::new_async().await;
        let signed = server
            .mock("POST", "/token")
            .match_header("authorization", "Bearer tok")
            .with_status(200)
            .create_async()
            .await;
        let unsigned = server
            .mock("POST", "/token")
            .match_header("authorization", mockito::Matcher::Missing)
            .with_status(200)
            .create_async()
            .await;

        let client = HttpClient::new(&RestCatalogConfig::builder().uri(server.url()).build())
            .unwrap()
            .with_auth_session(Arc::new(StaticSession));
        let url = format!("{}/token", server.url());

        client
            .post_form(&url, &HeaderMap::new(), &HashMap::new())
            .await
            .unwrap();
        signed.assert_async().await;

        client
            .without_auth_session()
            .post_form(&url, &HeaderMap::new(), &HashMap::new())
            .await
            .unwrap();
        unsigned.assert_async().await;
    }

    #[test]
    fn test_format_headers_redacted_empty() {
        let headers = HeaderMap::new();
        let result = format_headers_redacted(&headers, false);
        assert_eq!(result, "[]");
    }

    #[test]
    fn test_format_headers_redacted_keeps_names_not_values() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        assert!(result.contains("content-type"));
        assert!(result.contains("x-request-id"));
        assert!(!result.contains("application/json"));
        assert!(!result.contains("abc123"));
    }

    #[test]
    fn test_format_headers_redacted_filters_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header names remain, without their values.
        assert!(result.contains("authorization"));
        assert!(!result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("secret-token"));
        // Header names remain observable for diagnostics, never values.
        assert!(result.contains("content-type"));
        assert!(!result.contains("application/json"));
    }

    #[test]
    fn test_format_headers_redacted_filters_set_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "set-cookie",
            "CF_Authorization=sensitive-session-token; Path=/; Secure;"
                .parse()
                .unwrap(),
        );
        headers.insert("server", "cloudflare".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header names remain, without their values.
        assert!(result.contains("set-cookie"));
        assert!(!result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("sensitive-session-token"));
        // Non-sensitive header names are present but values are not.
        assert!(result.contains("server"));
        assert!(!result.contains("cloudflare"));
    }

    #[test]
    fn test_format_headers_redacted_filters_all_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().unwrap());
        headers.insert("proxy-authorization", "Basic creds".parse().unwrap());
        headers.insert("set-cookie", "session=abc".parse().unwrap());
        headers.insert("cookie", "session=abc".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("x-auth-token", "auth-token-456".parse().unwrap());
        headers.insert("x-request-id", "req-123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // All sensitive header names remain, without their values.
        assert!(result.contains("authorization"));
        assert!(result.contains("proxy-authorization"));
        assert!(result.contains("set-cookie"));
        assert!(result.contains("cookie"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("x-auth-token"));
        assert!(!result.contains("[REDACTED]"));

        // Ensure no sensitive values leaked
        assert!(!result.contains("Bearer token"));
        assert!(!result.contains("Basic creds"));
        assert!(!result.contains("session=abc"));
        assert!(!result.contains("api-key-123"));
        assert!(!result.contains("auth-token-456"));

        // Non-sensitive header names remain, but their values do not.
        assert!(result.contains("x-request-id"));
        assert!(!result.contains("req-123"));
    }

    #[test]
    fn test_format_headers_never_discloses_values_when_redaction_disabled() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, true);

        // Response values are never logged, even for the legacy debugging knob.
        assert!(result.contains("authorization"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("content-type"));
        assert!(!result.contains("Bearer secret-token"));
        assert!(!result.contains("api-key-123"));
        assert!(!result.contains("application/json"));
        assert!(!result.contains("[REDACTED]"));
    }

    fn assert_error_redacted(error: &Error, canary: &str) {
        assert!(!error.to_string().contains(canary));
        assert!(!format!("{error:?}").contains(canary));
        let mut source = std::error::Error::source(error);
        while let Some(error) = source {
            assert!(!error.to_string().contains(canary));
            assert!(!format!("{error:?}").contains(canary));
            source = error.source();
        }
    }

    #[test]
    fn response_body_canary_never_enters_decode_error() {
        let canary = "storage-secret-canary";
        let response = HttpResponse::new(
            http::StatusCode::OK,
            HeaderMap::new(),
            canary.as_bytes().to_vec(),
        );
        let error = deserialize_catalog_response::<crate::types::LoadTableResult>(response)
            .expect_err("invalid response must fail");
        assert_error_redacted(&error, canary);
    }

    #[test]
    fn response_enum_canary_never_enters_decode_error_source() {
        #[derive(Debug, serde::Deserialize)]
        enum KnownValue {
            Expected,
        }

        let canary = "storage-secret-known-value-canary";
        let body = serde_json::to_vec(canary).unwrap();
        // This shape makes serde's diagnostic quote the rejected server value.
        let unredacted = serde_json::from_slice::<KnownValue>(&body).unwrap_err();
        assert!(unredacted.to_string().contains(canary));
        let response = HttpResponse::new(http::StatusCode::OK, HeaderMap::new(), body);
        let error = deserialize_catalog_response::<KnownValue>(response).unwrap_err();
        assert_error_redacted(&error, canary);
    }

    #[tokio::test]
    async fn test_http_client_debug_redacts_headers() {
        let config = RestCatalogConfig::builder()
            .uri("http://localhost".to_string())
            .props(HashMap::from([
                ("header.authorization".to_string(), "Basic xyz".to_string()),
                (
                    "header.x-client-secret".to_string(),
                    "shh-secret".to_string(),
                ),
                (
                    "header.x-client-credential".to_string(),
                    "cred-value".to_string(),
                ),
            ]))
            .build();
        let client = HttpClient::new(&config).unwrap();

        let out = format!("{client:?}");
        assert!(!out.contains("Basic xyz"));
        assert!(!out.contains("shh-secret"));
        assert!(!out.contains("cred-value"));
        assert!(!out.contains("[REDACTED]"));
        assert!(out.contains("authorization"));
        assert!(out.contains("x-client-secret"));
        assert!(out.contains("x-client-credential"));
    }
}
