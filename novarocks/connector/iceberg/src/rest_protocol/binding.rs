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

//! The session binding captured while upstream establishes its session, and
//! the address and header composition every NovaRocks request derives from it.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT};

use super::auth::PersistentRestConfig;
use super::evidence::BindingFailure;
use crate::iceberg::{NamespaceIdent, TableIdent};

/// The REST specification version upstream announces in `x-client-version`
/// (`ICEBERG_REST_SPEC_VERSION`, private upstream).
const CLIENT_VERSION: &str = "0.14.1";
/// Upstream's `user-agent`: `iceberg-rs/{its crate version}`, private upstream.
const CLIENT_USER_AGENT: &str = "iceberg-rs/0.11.0";
const X_CLIENT_VERSION: HeaderName = HeaderName::from_static("x-client-version");
/// The per-call-site access delegation header (L23).
pub(crate) const X_ICEBERG_ACCESS_DELEGATION: HeaderName =
    HeaderName::from_static("x-iceberg-access-delegation");

const PROP_URI: &str = "uri";
const PROP_PREFIX: &str = "prefix";
const HEADER_PROPERTY_PREFIX: &str = "header.";
/// Server-sent authentication material the owner rejects (L5).
const SERVER_AUTHENTICATION_PROPERTIES: [&str; 2] = ["token", "credential"];
/// Token-request properties only the persistent definition decides (L16);
/// differing merged values are reported, never used.
const TOKEN_REQUEST_PROPERTIES: [&str; 4] = ["oauth2-server-uri", "scope", "audience", "resource"];

/// The three protocol headers upstream sends on every request, with the
/// values upstream sends (L30).
pub(crate) fn protocol_headers() -> HeaderMap {
    HeaderMap::from_iter([
        (CONTENT_TYPE, HeaderValue::from_static("application/json")),
        (X_CLIENT_VERSION, HeaderValue::from_static(CLIENT_VERSION)),
        (USER_AGENT, HeaderValue::from_static(CLIENT_USER_AGENT)),
    ])
}

/// Where a bound header's effective value came from (L15).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeaderProvenance {
    /// Name and value both equal the persistent definition's.
    Persistent,
    /// Sent by the server through `/v1/config`, or changed by it.
    Server,
}

/// One `header.*` of the effective configuration.
#[derive(Clone)]
pub(crate) struct BoundHeader {
    name: HeaderName,
    value: HeaderValue,
    provenance: HeaderProvenance,
}

impl BoundHeader {
    pub(crate) fn name(&self) -> &HeaderName {
        &self.name
    }

    pub(crate) fn value(&self) -> &HeaderValue {
        &self.value
    }

    pub(crate) fn provenance(&self) -> HeaderProvenance {
        self.provenance
    }
}

impl fmt::Debug for BoundHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundHeader")
            .field("name", &self.name)
            .field("provenance", &self.provenance)
            .finish_non_exhaustive()
    }
}

/// The facts captured while upstream established its session (5.3): the
/// effective `uri`, `prefix` and headers every NovaRocks request derives
/// from. Immutable for the generation once established.
#[derive(Clone)]
pub(crate) struct SessionBinding {
    uri: String,
    prefix: Option<String>,
    warehouse: Option<String>,
    /// Sorted by name.
    headers: Vec<BoundHeader>,
    /// Server-sent protocol headers left out of NovaRocks requests (L26);
    /// diagnostics only.
    excluded_protocol_headers: Vec<HeaderName>,
    /// Server-sent token-request properties that differ from the persistent
    /// definition and are ignored (L16); diagnostics only.
    ignored_token_properties: Vec<&'static str>,
    /// Whether the server advertised the table `HEAD` endpoint, learned from
    /// the session probe; `false` when no probe method was observed.
    table_head_advertised: bool,
    namespace_head_advertised: bool,
    endpoints: Option<Vec<String>>,
    idempotency: super::configuration::IdempotencyCapability,
}

impl fmt::Debug for SessionBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionBinding")
            .field("has_prefix", &self.prefix.is_some())
            .field("headers", &self.headers)
            .field("excluded_protocol_headers", &self.excluded_protocol_headers)
            .field("ignored_token_properties", &self.ignored_token_properties)
            .field("table_head_advertised", &self.table_head_advertised)
            .field("idempotency", &self.idempotency)
            .finish_non_exhaustive()
    }
}

impl SessionBinding {
    /// Validates the merged configuration upstream hands `catalog_session`
    /// and captures the binding from it.
    ///
    /// Server-sent `token`, `credential` or authentication-class headers fail
    /// establishment (L5); server-sent protocol headers are left out (L26).
    pub(crate) fn from_merged_properties(
        merged: &HashMap<String, String>,
        persistent: &PersistentRestConfig,
    ) -> Result<Self, BindingFailure> {
        if SERVER_AUTHENTICATION_PROPERTIES
            .iter()
            .any(|key| merged.contains_key(*key))
        {
            return Err(BindingFailure::InvalidServerConfig);
        }
        let mut headers: Vec<BoundHeader> = Vec::new();
        let mut excluded_protocol_headers = Vec::new();
        for (key, value) in merged {
            let Some(name) = key.strip_prefix(HEADER_PROPERTY_PREFIX) else {
                continue;
            };
            if is_authentication_header(name) {
                return Err(BindingFailure::InvalidServerConfig);
            }
            // Upstream validated every `header.*` before this callback.
            let name =
                HeaderName::from_str(name).map_err(|_| BindingFailure::InvalidServerConfig)?;
            let value =
                HeaderValue::from_str(value).map_err(|_| BindingFailure::InvalidServerConfig)?;
            if is_protocol_header(name.as_str()) {
                excluded_protocol_headers.push(name);
                continue;
            }
            if let Some(existing) = headers.iter().find(|header| header.name == name) {
                // Two spellings of one name: upstream applies either value,
                // so only identical values leave the binding decided.
                if existing.value == value {
                    continue;
                }
                return Err(BindingFailure::InvalidServerConfig);
            }
            let provenance =
                if persistent
                    .headers()
                    .iter()
                    .any(|(persistent_name, persistent_value)| {
                        *persistent_name == name && *persistent_value == value
                    })
                {
                    HeaderProvenance::Persistent
                } else {
                    HeaderProvenance::Server
                };
            headers.push(BoundHeader {
                name,
                value,
                provenance,
            });
        }
        headers.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
        excluded_protocol_headers.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let ignored_token_properties = TOKEN_REQUEST_PROPERTIES
            .into_iter()
            .filter(|key| {
                merged
                    .get(*key)
                    .is_some_and(|value| persistent.token_property(key) != Some(value.as_str()))
            })
            .collect();
        let uri = merged
            .get(PROP_URI)
            .cloned()
            .ok_or(BindingFailure::InvalidServerConfig)?;
        let parsed = reqwest::Url::parse(&uri).map_err(|_| BindingFailure::InvalidServerConfig)?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(BindingFailure::InvalidServerConfig);
        }
        Ok(Self {
            uri,
            prefix: merged.get(PROP_PREFIX).cloned(),
            warehouse: merged.get("warehouse").cloned(),
            headers,
            excluded_protocol_headers,
            ignored_token_properties,
            table_head_advertised: false,
            namespace_head_advertised: false,
            endpoints: None,
            idempotency: Default::default(),
        })
    }

    pub(crate) fn set_capabilities(
        &mut self,
        endpoints: Option<Vec<String>>,
        idempotency: super::configuration::IdempotencyCapability,
    ) -> Result<(), BindingFailure> {
        if let Some(endpoints) = &endpoints {
            for endpoint in endpoints {
                let (method, path) = endpoint
                    .split_once(' ')
                    .ok_or(BindingFailure::InvalidServerConfig)?;
                if reqwest::Method::from_bytes(method.as_bytes()).is_err()
                    || !path.starts_with('/')
                    || path.contains(' ')
                {
                    return Err(BindingFailure::InvalidServerConfig);
                }
            }
        }
        self.table_head_advertised = endpoints.as_ref().is_some_and(|endpoints| {
            endpoints.iter().any(|endpoint| {
                endpoint == "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"
            })
        });
        self.namespace_head_advertised = endpoints.as_ref().is_some_and(|endpoints| {
            endpoints
                .iter()
                .any(|endpoint| endpoint == "HEAD /v1/{prefix}/namespaces/{namespace}")
        });
        self.endpoints = endpoints;
        self.idempotency = idempotency;
        Ok(())
    }

    pub(crate) fn idempotency_capability(&self) -> &super::configuration::IdempotencyCapability {
        &self.idempotency
    }
    pub(crate) fn namespace_head_advertised(&self) -> bool {
        self.namespace_head_advertised
    }

    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    pub(crate) fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    pub(crate) fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    pub(crate) fn advertised_endpoints(&self) -> Option<&[String]> {
        self.endpoints.as_deref()
    }

    pub(crate) fn headers(&self) -> &[BoundHeader] {
        &self.headers
    }

    pub(crate) fn excluded_protocol_headers(&self) -> &[HeaderName] {
        &self.excluded_protocol_headers
    }

    pub(crate) fn ignored_token_properties(&self) -> &[&'static str] {
        &self.ignored_token_properties
    }

    pub(crate) fn table_head_advertised(&self) -> bool {
        self.table_head_advertised
    }

    /// Upstream's address algorithm (L30): a raw `/` join of `uri`, `v1`,
    /// the optional prefix and the path segments, without normalization.
    fn url(&self, parts: &[&str]) -> String {
        let mut url = [self.uri.as_str(), "v1"].join("/");
        for part in self
            .prefix
            .as_deref()
            .into_iter()
            .chain(parts.iter().copied())
        {
            url.push('/');
            url.push_str(part);
        }
        url
    }

    pub(crate) fn namespaces_url(&self) -> String {
        self.url(&["namespaces"])
    }

    /// A multi-level namespace is joined with U+001F, as upstream does.
    pub(crate) fn namespace_url(&self, namespace: &NamespaceIdent) -> String {
        self.url(&["namespaces", &namespace.to_url_string()])
    }

    pub(crate) fn tables_url(&self, namespace: &NamespaceIdent) -> String {
        self.url(&["namespaces", &namespace.to_url_string(), "tables"])
    }

    pub(crate) fn table_url(&self, table: &TableIdent) -> String {
        self.url(&[
            "namespaces",
            &table.namespace.to_url_string(),
            "tables",
            &table.name,
        ])
    }

    pub(crate) fn views_url(&self, namespace: &NamespaceIdent) -> String {
        self.url(&["namespaces", &namespace.to_url_string(), "views"])
    }

    pub(crate) fn view_url(&self, view: &TableIdent) -> String {
        self.url(&[
            "namespaces",
            &view.namespace.to_url_string(),
            "views",
            &view.name,
        ])
    }

    /// Headers of a NovaRocks request before the auth owner adds
    /// `Authorization`: the protocol headers, the bound `header.*`, and the
    /// access delegation header when this call site sends it (L23).
    pub(crate) fn request_headers(&self, access_delegation: Option<HeaderValue>) -> HeaderMap {
        let mut headers = protocol_headers();
        for header in &self.headers {
            headers.insert(header.name.clone(), header.value.clone());
        }
        if let Some(value) = access_delegation {
            headers.insert(X_ICEBERG_ACCESS_DELEGATION, value);
        }
        headers
    }
}

/// Header names only the per-role auth owner may set (L5).
///
/// This is a fail-fast guard against overriding the role's authentication,
/// not the rule that decides whether a header is public: publicity is decided
/// by provenance and explicit declaration (L15).
pub(crate) fn is_authentication_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
    ) || ["token", "secret", "password", "credential", "access_key"]
        .iter()
        .any(|marker| name.contains(marker))
}

/// Header names the client sets per request (L26); `header.*` never overrides
/// them on a NovaRocks request.
pub(crate) fn is_protocol_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "content-type"
            | "accept"
            | "user-agent"
            | "x-client-version"
            | "x-iceberg-access-delegation"
            | "if-none-match"
            | "idempotency-key"
    )
}

#[cfg(test)]
mod header_class_tests {
    use super::*;

    #[test]
    fn authentication_headers_are_recognized_case_insensitively() {
        for name in [
            "Authorization",
            "proxy-authorization",
            "Cookie",
            "SET-COOKIE",
            "x-api-token",
            "X-Client-Secret",
            "x-password",
            "x-credential",
            "x-access_key",
        ] {
            assert!(is_authentication_header(name), "{name}");
        }
        for name in [
            "x-tenant",
            "polaris-realm",
            "x-project-id",
            "x-goog-user-project",
        ] {
            assert!(!is_authentication_header(name), "{name}");
        }
    }

    #[test]
    fn protocol_headers_are_recognized_case_insensitively() {
        for name in [
            "Content-Type",
            "accept",
            "User-Agent",
            "x-client-version",
            "X-Iceberg-Access-Delegation",
        ] {
            assert!(is_protocol_header(name), "{name}");
        }
        assert!(!is_protocol_header("x-tenant"));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::Url;

    use super::*;
    use crate::iceberg::Catalog;
    use crate::rest_protocol::http::RestHttpClient;
    use crate::rest_protocol::test_server::{
        FrontendFixture, Reply, Script, TestServer, bearer_identity,
    };

    const TABLE_HEAD: &str = "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}";

    fn client() -> RestHttpClient {
        RestHttpClient::with_timeouts(Duration::from_secs(2), Duration::from_secs(5)).unwrap()
    }

    fn header<'a>(binding: &'a SessionBinding, name: &str) -> Option<&'a BoundHeader> {
        binding
            .headers()
            .iter()
            .find(|header| header.name().as_str() == name)
    }

    #[tokio::test]
    async fn the_binding_takes_the_effective_uri_prefix_and_header_provenance() {
        // The override points back at this server under another base path.
        let server = TestServer::start(
            Script::new()
                .on_fn("GET", "/v1/config", |request, _| {
                    let effective_uri = format!("http://{}/alt", request.header("host").unwrap());
                    Reply::config(
                        &[
                            ("header.x-tenant", "server-tenant"),
                            ("header.x-keep", "kept"),
                            ("header.Content-Type", "text/plain"),
                            ("scope", "server-scope"),
                        ],
                        &[
                            ("uri", &effective_uri),
                            ("prefix", "pfx"),
                            ("header.x-project", "server-project"),
                        ],
                        Some(&[TABLE_HEAD]),
                    )
                })
                .on("GET", "/alt/v1/pfx/namespaces/a%1Fb", Reply::status(404))
                .on(
                    "HEAD",
                    "/alt/v1/pfx/namespaces/a%1Fb/tables/orders",
                    Reply::status(404),
                ),
        );
        let effective_uri = format!("{}/alt", server.url());
        let fixture = FrontendFixture::new(
            &[
                ("uri", &server.url()),
                ("header.x-project", "persistent-project"),
                ("header.X-Keep", "kept"),
            ],
            Some(bearer_identity("role-token")),
            client(),
        )
        .await;
        let binding = fixture.ensure_session().await.expect("established");
        assert_eq!(binding.uri(), effective_uri);
        assert_eq!(binding.prefix(), Some("pfx"));
        assert!(binding.table_head_advertised());
        assert_eq!(
            binding
                .headers()
                .iter()
                .map(|header| header.name().as_str())
                .collect::<Vec<_>>(),
            vec!["x-keep", "x-project", "x-tenant"]
        );
        let keep = header(&binding, "x-keep").unwrap();
        assert_eq!(keep.provenance(), HeaderProvenance::Persistent);
        let project = header(&binding, "x-project").unwrap();
        assert_eq!(project.provenance(), HeaderProvenance::Server);
        assert_eq!(project.value(), "server-project");
        let tenant = header(&binding, "x-tenant").unwrap();
        assert_eq!(tenant.provenance(), HeaderProvenance::Server);
        assert_eq!(binding.excluded_protocol_headers(), &[CONTENT_TYPE]);
        assert_eq!(binding.ignored_token_properties(), &["scope"]);

        // Upstream addresses the same resources NovaRocks composes (L30).
        let namespace = NamespaceIdent::from_strs(["a", "b"]).unwrap();
        assert!(
            !fixture
                .endpoints
                .namespace_exists(&namespace)
                .await
                .unwrap()
        );
        let table = TableIdent::new(namespace.clone(), "orders".to_string());
        assert!(!fixture.endpoints.table_exists(&table).await.unwrap());
        let namespace_path = Url::parse(&binding.namespace_url(&namespace))
            .unwrap()
            .path()
            .to_string();
        let table_path = Url::parse(&binding.table_url(&table))
            .unwrap()
            .path()
            .to_string();
        assert_eq!(server.count("GET", &namespace_path), 1);
        assert_eq!(server.count("HEAD", &table_path), 1);

        // A NovaRocks request carries the protocol headers, the bound
        // headers and, per call site, access delegation; the server-sent
        // content type never replaces the protocol one.
        let headers = binding.request_headers(Some(HeaderValue::from_static("vended-credentials")));
        assert_eq!(headers[CONTENT_TYPE], "application/json");
        assert_eq!(headers[X_CLIENT_VERSION], "0.14.1");
        assert_eq!(headers[USER_AGENT], "iceberg-rs/0.11.0");
        assert_eq!(headers["x-project"], "server-project");
        assert_eq!(headers["x-keep"], "kept");
        assert_eq!(headers["x-tenant"], "server-tenant");
        assert_eq!(headers[X_ICEBERG_ACCESS_DELEGATION], "vended-credentials");
        assert!(!headers.contains_key("authorization"));
        assert!(
            !binding
                .request_headers(None)
                .contains_key(X_ICEBERG_ACCESS_DELEGATION)
        );
    }

    #[tokio::test]
    async fn server_sent_authentication_material_fails_establishment() {
        type Properties<'a> = &'a [(&'a str, &'a str)];
        let cases: [(Properties, Properties); 5] = [
            (&[], &[("token", "server-token")]),
            (&[("credential", "id:secret")], &[]),
            (&[("header.Authorization", "Bearer server-token")], &[]),
            (&[], &[("header.x-api-token", "server-token")]),
            (&[("header.cookie", "session=1")], &[]),
        ];
        for (defaults, overrides) in cases {
            let server = TestServer::start(Script::new().on(
                "GET",
                "/v1/config",
                Reply::config(defaults, overrides, None),
            ));
            let fixture = FrontendFixture::new(&[("uri", &server.url())], None, client()).await;
            let failure = fixture.ensure_session().await.unwrap_err();
            assert_eq!(
                failure.step,
                BindingFailure::InvalidServerConfig,
                "{defaults:?} {overrides:?}"
            );
        }
    }

    #[test]
    fn urls_join_raw_segments_like_upstream() {
        let binding = SessionBinding {
            uri: "http://catalog:8181/api/".to_string(),
            prefix: Some("ws/main".to_string()),
            warehouse: None,
            headers: Vec::new(),
            excluded_protocol_headers: Vec::new(),
            ignored_token_properties: Vec::new(),
            table_head_advertised: false,
            namespace_head_advertised: false,
            endpoints: None,
            idempotency: Default::default(),
        };
        let namespace = NamespaceIdent::from_strs(["a", "b"]).unwrap();
        let view = TableIdent::new(namespace.clone(), "v".to_string());
        assert_eq!(
            binding.namespaces_url(),
            "http://catalog:8181/api//v1/ws/main/namespaces"
        );
        assert_eq!(
            binding.tables_url(&namespace),
            "http://catalog:8181/api//v1/ws/main/namespaces/a\u{1f}b/tables"
        );
        assert_eq!(
            binding.views_url(&namespace),
            "http://catalog:8181/api//v1/ws/main/namespaces/a\u{1f}b/views"
        );
        assert_eq!(
            binding.view_url(&view),
            "http://catalog:8181/api//v1/ws/main/namespaces/a\u{1f}b/views/v"
        );
        let unprefixed = SessionBinding {
            prefix: None,
            ..binding
        };
        assert_eq!(
            unprefixed.namespace_url(&namespace),
            "http://catalog:8181/api//v1/namespaces/a\u{1f}b"
        );
    }

    #[test]
    fn binding_debug_prints_no_header_values() {
        let persistent = PersistentRestConfig::from_properties(
            &[("uri".to_string(), "http://catalog".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();
        let merged = [
            ("uri", "http://catalog"),
            ("header.x-tenant", "tenant-value-canary"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        let binding = SessionBinding::from_merged_properties(&merged, &persistent).unwrap();
        let debug = format!("{binding:?}");
        assert!(debug.contains("x-tenant"), "{debug}");
        assert!(!debug.contains("tenant-value-canary"), "{debug}");
    }
}
