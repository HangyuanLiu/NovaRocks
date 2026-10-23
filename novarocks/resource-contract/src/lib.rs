// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Neutral facts about selected shared dependencies. These values carry no
//! quota, credentials, execution identity or right to perform an operation.
//! Providers and filesystem adapters project the route they actually selected;
//! governance consumes that fact without resolving catalog names or DNS aliases.

use std::{
    fmt::{self, Write as _},
    sync::Arc,
};

/// A network service's HTTP origin, independent of catalog alias, object path,
/// credentials and credential epoch. HTTP default ports use the URL author's
/// origin normalization. Distinct DNS names are not guessed to be equivalent.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NetworkServiceOrigin(Arc<str>);

/// A validated, allocation-free view of an already selected route's origin.
/// Its host borrows the original URL; parsing and input custody stay with that
/// route owner. Keeping this view grants no resource or execution authority.
#[derive(Clone, Copy, Debug)]
pub struct SelectedNetworkOrigin<'a> {
    scheme: &'a str,
    host: &'a str,
    port: Option<u16>,
    bytes: usize,
}
impl<'a> SelectedNetworkOrigin<'a> {
    pub fn from_selected_url(selected: &'a url::Url) -> Result<Self, DependencyFactError> {
        let scheme = selected.scheme();
        if !matches!(scheme, "http" | "https") || selected.host().is_none() {
            return Err(DependencyFactError::InvalidNetworkOrigin);
        }
        let host = selected
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(DependencyFactError::InvalidNetworkOrigin)?;
        let port = selected.port();
        let suffix = port.map_or(0, |port| {
            1 + port.checked_ilog10().map_or(1, |log| log as usize + 1)
        });
        let bytes = scheme
            .len()
            .checked_add(3)
            .and_then(|len| len.checked_add(host.len()))
            .and_then(|len| len.checked_add(suffix))
            .filter(|len| *len <= NetworkServiceOrigin::MAX_BYTES)
            .ok_or(DependencyFactError::EndpointTooLong)?;
        Ok(Self {
            scheme,
            host,
            port,
            bytes,
        })
    }

    /// Allocate the canonical output only after its original owner has prepaid
    /// both the String and final Arc that coexist during this conversion.
    pub fn into_owned(self) -> NetworkServiceOrigin {
        let mut origin = String::with_capacity(self.bytes);
        origin.push_str(self.scheme);
        origin.push_str("://");
        origin.push_str(self.host);
        if let Some(port) = self.port {
            write!(&mut origin, ":{port}").expect("writing to a bounded String cannot fail");
        }
        debug_assert_eq!(origin.len(), self.bytes);
        debug_assert!(origin.is_ascii());
        NetworkServiceOrigin(Arc::from(origin))
    }
}
impl NetworkServiceOrigin {
    /// Maximum canonical origin output from this fact author, in bytes.
    pub const MAX_BYTES: usize = 1024;

    /// Project the HTTP(S) URL already parsed by the route's original owner.
    /// This does not parse, resolve DNS, acquire credentials or authorize I/O.
    /// Path, user information, query and fragment are never copied or retained.
    pub fn from_selected_url(selected: &url::Url) -> Result<Self, DependencyFactError> {
        SelectedNetworkOrigin::from_selected_url(selected).map(SelectedNetworkOrigin::into_owned)
    }

    /// Compare a validated selected origin without allocating another String
    /// or Arc. Both sides use the same URL author's host/default-port facts.
    pub fn matches_selected(&self, selected: SelectedNetworkOrigin<'_>) -> bool {
        if self.0.len() != selected.bytes {
            return false;
        }
        let Some(suffix) = self
            .0
            .strip_prefix(selected.scheme)
            .and_then(|rest| rest.strip_prefix("://"))
            .and_then(|rest| rest.strip_prefix(selected.host))
        else {
            return false;
        };
        match selected.port {
            None => suffix.is_empty(),
            // This owned value can only be created by the canonical decimal
            // author above; length equality also preserves its exact width.
            Some(port) => {
                suffix
                    .strip_prefix(':')
                    .and_then(|port| port.parse::<u16>().ok())
                    == Some(port)
            }
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A shared host-local dependency is intentionally independent of directory or
/// catalog name. Network dependencies name the actual selected service origin.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SharedDependency {
    Network(NetworkServiceOrigin),
    LocalHost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyFactError {
    EndpointTooLong,
    InvalidNetworkOrigin,
}
impl fmt::Display for DependencyFactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EndpointTooLong => "shared dependency origin exceeds its bounded output profile",
            Self::InvalidNetworkOrigin => {
                "shared dependency requires a selected HTTP(S) service origin"
            }
        })
    }
}
impl std::error::Error for DependencyFactError {}

#[cfg(test)]
mod tests {
    use super::*;

    // Parsing belongs to the fixture's original route owner, outside the fact
    // constructor. Production callers must supply their already-selected Url.
    fn origin(endpoint: &str) -> NetworkServiceOrigin {
        let selected = url::Url::parse(endpoint).expect("fixture route must parse");
        NetworkServiceOrigin::from_selected_url(&selected).expect("fixture origin must fit")
    }

    #[test]
    fn selected_routes_share_one_origin_without_retaining_path_or_secrets() {
        let first =
            origin("HTTPS://user:password@Example.org:443/v1/catalog?credential=secret#private");
        let second = origin("https://example.org/oauth/token");
        assert_eq!(first, second);
        assert_eq!(first.as_str(), "https://example.org");
        assert_eq!(
            format!("{first:?}"),
            "NetworkServiceOrigin(\"https://example.org\")"
        );
        assert_ne!(first, origin("https://example.org:8443"));
        assert_ne!(first, origin("https://other-alias.example.org"));
    }

    #[test]
    fn original_url_author_controls_host_and_default_port_normalization() {
        for input in [
            "http://EXAMPLE.org:80/path",
            "https://EXAMPLE.org:443/path",
            "http://[0:0:0:0:0:0:0:1]:80/path",
            "https://[2001:0DB8:0:0:0:0:0:1]:8443/path",
            "http://127.0.0.1:80/path",
            "https://bücher.example:443/path",
            "https://example.org:0/path",
            "https://example.org:65535/path",
        ] {
            let selected = url::Url::parse(input).unwrap();
            let fact = NetworkServiceOrigin::from_selected_url(&selected).unwrap();
            // The existing URL origin author is the independent semantic
            // oracle; its allocations are confined to this test.
            assert_eq!(fact.as_str(), selected.origin().ascii_serialization());
        }
        assert_eq!(origin("http://[::1]:80").as_str(), "http://[::1]");
        assert_eq!(
            origin("https://bücher.example").as_str(),
            "https://xn--bcher-kva.example"
        );
        assert_ne!(origin("http://[::1]"), origin("https://[::1]"));
    }

    #[test]
    fn origin_projection_borrows_the_selected_url_after_original_mutation() {
        let mut selected = url::Url::parse("https://before.example/path").unwrap();
        selected.set_host(Some("AFTER.example")).unwrap();
        selected.set_port(Some(8443)).unwrap();
        selected.set_username("private-user").unwrap();
        selected.set_password(Some("private-password")).unwrap();
        selected.set_query(Some("credential=private-query"));
        let fact = NetworkServiceOrigin::from_selected_url(&selected).unwrap();
        assert_eq!(fact.as_str(), "https://after.example:8443");
        assert!(!format!("{fact:?}").contains("private"));
    }

    #[test]
    fn unsupported_parsed_routes_have_no_default_network_domain() {
        for input in [
            "file:///tmp/table",
            "s3://bucket/key",
            "ftp://example.org/path",
            "data:text/plain,private",
        ] {
            let selected = url::Url::parse(input).unwrap();
            assert_eq!(
                NetworkServiceOrigin::from_selected_url(&selected),
                Err(DependencyFactError::InvalidNetworkOrigin)
            );
        }
    }

    #[test]
    fn accepted_origin_limit_includes_every_port_width_before_output_allocation() {
        for port in [
            None,
            Some(0u16),
            Some(9),
            Some(10),
            Some(99),
            Some(100),
            Some(999),
            Some(1000),
            Some(9999),
            Some(10000),
            Some(65535),
        ] {
            let suffix = port.map_or_else(String::new, |port| format!(":{port}"));
            let overhead = "https://".len() + suffix.len();
            let host = "a".repeat(NetworkServiceOrigin::MAX_BYTES - overhead);
            // url 2.5.7 intentionally ignores DNS length in its URL host
            // parser. No DNS lookup or network operation occurs here.
            let selected = url::Url::parse(&format!("https://{host}{suffix}/path")).unwrap();
            let fact = NetworkServiceOrigin::from_selected_url(&selected).unwrap();
            assert_eq!(fact.as_str().len(), NetworkServiceOrigin::MAX_BYTES);
            let too_long = url::Url::parse(&format!("https://{host}a{suffix}/path")).unwrap();
            assert_eq!(
                NetworkServiceOrigin::from_selected_url(&too_long),
                Err(DependencyFactError::EndpointTooLong)
            );
        }
    }

    #[test]
    fn large_non_origin_parts_remain_with_the_original_url_owner() {
        let path = "x".repeat(32 * 1024);
        let selected = url::Url::parse(&format!(
            "https://user:password@example.org/{path}?credential=secret#private"
        ))
        .unwrap();
        let fact = NetworkServiceOrigin::from_selected_url(&selected).unwrap();
        assert_eq!(fact.as_str(), "https://example.org");
        assert_eq!(
            format!("{fact:?}"),
            "NetworkServiceOrigin(\"https://example.org\")"
        );
    }
    #[test]
    fn borrowed_selected_views_share_the_exact_original_canonical_author() {
        for input in [
            "HTTP://user:secret@EXAMPLE.org:80/path?q=secret#private",
            "https://EXAMPLE.org:443/other",
            "http://[::1]:65535/path",
            "https://bücher.example:8443/",
            "https://example.org:0/",
        ] {
            let selected = url::Url::parse(input).unwrap();
            let view = SelectedNetworkOrigin::from_selected_url(&selected).unwrap();
            let fact = view.into_owned();
            assert!(fact.matches_selected(view));
            assert_eq!(fact.as_str(), selected.origin().ascii_serialization());
            for different in ["https://different.example", "http://different.example:8443"] {
                let different = url::Url::parse(different).unwrap();
                assert!(!fact.matches_selected(
                    SelectedNetworkOrigin::from_selected_url(&different).unwrap()
                ));
            }
            let detail = format!("{view:?}");
            assert!(!detail.contains("secret") && !detail.contains("private"));
        }
        let owned = origin("https://example.org:8443/path");
        for input in [
            "http://example.org:8443/path",
            "https://example.org/path",
            "https://example.org.evil:8443/",
        ] {
            let selected = url::Url::parse(input).unwrap();
            assert!(
                !owned
                    .matches_selected(SelectedNetworkOrigin::from_selected_url(&selected).unwrap())
            );
        }
    }
}
