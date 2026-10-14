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

//! The public routing context the coordinator freezes for an execution node
//! (L15), and the admission that refuses a vended read an execution node could
//! not acquire for (L25).
//!
//! The context is a projection of the generation's one session binding: the
//! complete request address and the public routing headers. Public means a
//! header of the persistent catalog definition, or a server-supplied one the
//! definition explicitly declares. Authentication material never enters it;
//! every node authenticates with its own identity.

use std::collections::BTreeSet;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, CredentialRoutingContext, MAX_CREDENTIALS_ENDPOINT_BYTES,
};

use crate::catalog_config::REST_SERVER_ROUTING_HEADERS_PROPERTY;
use crate::iceberg::TableIdent;
use crate::rest_protocol::binding::{HeaderProvenance, SessionBinding};

/// Where an execution node acquires.
pub(crate) enum RouteTarget<'a> {
    /// The credentials endpoint the server advertised in a load response.
    CredentialsEndpoint(&'a str),
    /// The delegated load of this table on the bound session.
    LoadTable(&'a TableIdent),
}

/// What admission needs to know besides the binding.
pub(crate) struct RouteAdmission<'a> {
    /// Whether the catalog definition declares a `DataCredentialVending`
    /// binding: without it no execution node can acquire.
    pub(crate) vending_declared: bool,
    /// The server-supplied header names the definition declares public.
    pub(crate) declared_headers: &'a BTreeSet<String>,
}

/// One public routing header as it is forwarded: lowercase name and value.
type RouteHeader = (Arc<str>, Arc<str>);

/// Check everything about a route that does not depend on the response:
/// the execution node's acquisition identity and every bound header. CTAS
/// runs this before it stages, so a configuration error is refused before any
/// side effect.
pub(crate) fn preflight_route(
    binding: &SessionBinding,
    admission: &RouteAdmission<'_>,
) -> Result<Vec<RouteHeader>, ConnectorError> {
    if !admission.vending_declared {
        return Err(refusal(
            "a vended Iceberg REST read needs execution nodes that acquire storage credentials \
             themselves, but the catalog definition declares no DataCredentialVending credential \
             binding; add one for the execution nodes' catalog identity"
                .to_string(),
        ));
    }
    let mut headers = Vec::with_capacity(binding.headers().len());
    for header in binding.headers() {
        let name = header.name().as_str();
        let public = match header.provenance() {
            HeaderProvenance::Persistent => true,
            HeaderProvenance::Server => admission.declared_headers.contains(name),
        };
        if !public {
            return Err(refusal(format!(
                "the REST catalog server sent request header `{name}` through /v1/config, and \
                 execution nodes cannot send it unless the catalog definition declares it public; \
                 add `{name}` to the `{REST_SERVER_ROUTING_HEADERS_PROPERTY}` catalog property"
            )));
        }
        let value = header.value().to_str().map_err(|_| {
            refusal(format!(
                "REST catalog request header `{name}` cannot be forwarded to execution nodes"
            ))
        })?;
        headers.push((Arc::from(name), Arc::from(value)));
    }
    Ok(headers)
}

/// Form the frozen routing context for one acquisition path, or refuse the
/// vended read before anything is dispatched (L25).
// Design: ADR-0175 (docs/adr/ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md)
pub(crate) fn route_for_lease(
    binding: &SessionBinding,
    admission: &RouteAdmission<'_>,
    target: RouteTarget<'_>,
) -> Result<CredentialRoutingContext, ConnectorError> {
    let headers = preflight_route(binding, admission)?;
    let url = match target {
        RouteTarget::CredentialsEndpoint(endpoint) => endpoint.to_string(),
        RouteTarget::LoadTable(table) => binding.table_url(table),
    };
    let url = canonical_route_url(&url)?;
    CredentialRoutingContext::try_new(url, headers).map_err(|error| {
        refusal(format!(
            "the REST routing context for execution nodes exceeds its bounds: {}",
            error.message()
        ))
    })
}

/// The address an execution node sends to, as the HTTP client will send it:
/// http or https with a host, and no user information, query or fragment.
/// Parsing also percent-encodes what the client would encode (the U+001F
/// namespace separator, for one), so both roles reach the same target.
pub(crate) fn canonical_route_url(raw: &str) -> Result<String, ConnectorError> {
    let parsed = url::Url::parse(raw)
        .map_err(|_| refusal("the REST routing address is not a valid URL".to_string()))?;
    if !matches!(parsed.scheme(), "https" | "http")
        || parsed.host_str().is_none_or(str::is_empty)
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(refusal(
            "the REST routing address must be http or https with a host and no user information, \
             query or fragment"
                .to_string(),
        ));
    }
    let canonical: String = parsed.into();
    if canonical.len() > MAX_CREDENTIALS_ENDPOINT_BYTES {
        return Err(refusal(format!(
            "the REST routing address exceeds {MAX_CREDENTIALS_ENDPOINT_BYTES} bytes"
        )));
    }
    Ok(canonical)
}

fn refusal(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    //! Admission of a vended read (L25) against bindings captured through a
    //! real session on a scripted server.
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use super::{RouteAdmission, RouteTarget, preflight_route, route_for_lease};
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use crate::rest_protocol::binding::SessionBinding;
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};

    async fn binding(defaults: &[(&str, &str)]) -> (Arc<SessionBinding>, TestServer) {
        let server = TestServer::start(Script::new().on(
            "GET",
            "/v1/config",
            Reply::config(defaults, &[], None),
        ));
        let endpoints = crate::catalog_runtime::build_vended_rest_generation_for_test(
            HashMap::from([
                ("uri".to_string(), server.url()),
                ("header.x-tenant".to_string(), "t1".to_string()),
            ]),
            None,
            crate::iceberg::Runtime::try_current().expect("test runtime"),
        )
        .await
        .expect("REST generation")
        .1;
        (endpoints.binding().await.expect("session"), server)
    }

    fn table() -> TableIdent {
        TableIdent::new(
            NamespaceIdent::from_vec(vec!["sales".into(), "eu".into()]).unwrap(),
            "orders".into(),
        )
    }

    #[tokio::test]
    async fn a_route_carries_public_headers_and_the_canonical_table_address() {
        let (binding, server) = binding(&[("prefix", "p1"), ("header.x-region", "r1")]).await;
        let declared = BTreeSet::from(["x-region".to_string()]);
        let admission = RouteAdmission {
            vending_declared: true,
            declared_headers: &declared,
        };
        let route =
            route_for_lease(&binding, &admission, RouteTarget::LoadTable(&table())).unwrap();
        // The U+001F separator is percent-encoded exactly as the client sends it.
        assert_eq!(
            route.url(),
            format!("{}/v1/p1/namespaces/sales%1Feu/tables/orders", server.url())
        );
        let headers: Vec<_> = route
            .headers()
            .iter()
            .map(|header| (header.name().to_string(), header.value().to_string()))
            .collect();
        assert_eq!(
            headers,
            [
                ("x-region".to_string(), "r1".to_string()),
                ("x-tenant".to_string(), "t1".to_string()),
            ]
        );
        assert!(!format!("{route:?}").contains("t1"));
    }

    #[tokio::test]
    async fn an_undeclared_server_header_or_a_missing_vending_identity_is_refused() {
        let (binding, _server) = binding(&[("header.x-region", "r1")]).await;
        let none = BTreeSet::new();
        let error = preflight_route(
            &binding,
            &RouteAdmission {
                vending_declared: true,
                declared_headers: &none,
            },
        )
        .expect_err("undeclared server header");
        assert!(error.message().contains("x-region"));
        assert!(
            error
                .message()
                .contains("iceberg.rest.server-routing-headers")
        );
        assert!(!error.message().contains("r1"));

        let declared = BTreeSet::from(["x-region".to_string()]);
        let error = preflight_route(
            &binding,
            &RouteAdmission {
                vending_declared: false,
                declared_headers: &declared,
            },
        )
        .expect_err("no execution-node identity");
        assert!(error.message().contains("DataCredentialVending"));
    }

    #[tokio::test]
    async fn addresses_and_header_bounds_are_checked() {
        let (binding, _server) = binding(&[]).await;
        let none = BTreeSet::new();
        let admission = RouteAdmission {
            vending_declared: true,
            declared_headers: &none,
        };
        for endpoint in [
            "https://user:pw@catalog.example/credentials",
            "https://catalog.example/credentials?x=1",
            "ftp://catalog.example/credentials",
        ] {
            assert!(
                route_for_lease(
                    &binding,
                    &admission,
                    RouteTarget::CredentialsEndpoint(endpoint)
                )
                .is_err(),
                "{endpoint}"
            );
        }
        let many: Vec<(String, String)> = (0..17)
            .map(|i| (format!("header.x-h{i:02}"), "v".to_string()))
            .collect();
        let refs: Vec<(&str, &str)> = many.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let (binding, _server) = binding_with(&refs).await;
        let declared: BTreeSet<String> = (0..17).map(|i| format!("x-h{i:02}")).collect();
        let error = route_for_lease(
            &binding,
            &RouteAdmission {
                vending_declared: true,
                declared_headers: &declared,
            },
            RouteTarget::CredentialsEndpoint("https://catalog.example/credentials"),
        )
        .expect_err("more than 16 routing headers");
        assert!(error.message().contains("bounds"));
    }

    async fn binding_with(defaults: &[(&str, &str)]) -> (Arc<SessionBinding>, TestServer) {
        binding(defaults).await
    }
}
