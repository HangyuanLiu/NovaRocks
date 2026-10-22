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

use std::{fmt, sync::Arc};

const MAX_ENDPOINT_INPUT_BYTES: usize = 16 * 1024;
const MAX_ORIGIN_BYTES: usize = 1024;

/// A network service's HTTP origin, independent of catalog alias, object path,
/// credentials and credential epoch. HTTP default ports use the URL author's
/// origin normalization. Distinct DNS names are not guessed to be equivalent.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NetworkServiceOrigin(Arc<str>);

impl NetworkServiceOrigin {
    /// Project an already-selected HTTP(S) route. Path, user information, query
    /// and fragment are never retained, including in an error or Debug output.
    pub fn from_selected_url(endpoint: &str) -> Result<Self, DependencyFactError> {
        if endpoint.len() > MAX_ENDPOINT_INPUT_BYTES {
            return Err(DependencyFactError::EndpointTooLong);
        }
        let selected =
            url::Url::parse(endpoint).map_err(|_| DependencyFactError::InvalidNetworkOrigin)?;
        if !matches!(selected.scheme(), "http" | "https") || selected.host().is_none() {
            return Err(DependencyFactError::InvalidNetworkOrigin);
        }
        let origin = selected.origin().ascii_serialization();
        if origin.len() > MAX_ORIGIN_BYTES {
            return Err(DependencyFactError::EndpointTooLong);
        }
        Ok(Self(Arc::from(origin)))
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
            Self::EndpointTooLong => "shared dependency endpoint exceeds its bounded input profile",
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

    #[test]
    fn selected_routes_share_one_origin_without_retaining_path_or_secrets() {
        let first = NetworkServiceOrigin::from_selected_url(
            "HTTPS://user:password@Example.org:443/v1/catalog?credential=secret#private",
        )
        .unwrap();
        let second =
            NetworkServiceOrigin::from_selected_url("https://example.org/oauth/token").unwrap();
        assert_eq!(first, second);
        assert_eq!(first.as_str(), "https://example.org");
        assert_eq!(
            format!("{first:?}"),
            "NetworkServiceOrigin(\"https://example.org\")"
        );
        assert_ne!(
            first,
            NetworkServiceOrigin::from_selected_url("https://example.org:8443").unwrap()
        );
    }

    #[test]
    fn ipv6_and_http_default_ports_follow_the_original_url_origin_author() {
        let a =
            NetworkServiceOrigin::from_selected_url("http://[0:0:0:0:0:0:0:1]:80/path").unwrap();
        let b = NetworkServiceOrigin::from_selected_url("http://[::1]").unwrap();
        assert_eq!(a, b);
        assert_eq!(a.as_str(), "http://[::1]");
        assert_ne!(
            a,
            NetworkServiceOrigin::from_selected_url("https://[::1]").unwrap()
        );
    }

    #[test]
    fn missing_or_unresolved_routes_are_rejected_without_a_default_domain() {
        for input in [
            "",
            "relative/path",
            "file:///tmp/table",
            "s3://bucket/key",
            "https://",
        ] {
            assert_eq!(
                NetworkServiceOrigin::from_selected_url(input),
                Err(DependencyFactError::InvalidNetworkOrigin)
            );
        }
        assert_eq!(
            NetworkServiceOrigin::from_selected_url(&"x".repeat(MAX_ENDPOINT_INPUT_BYTES + 1)),
            Err(DependencyFactError::EndpointTooLong)
        );
    }
}
