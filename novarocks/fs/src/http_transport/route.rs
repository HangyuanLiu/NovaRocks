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

//! Selected HTTP routing facts captured once at the original client configuration epoch.
use std::fmt;
use std::net::IpAddr;

use http::{HeaderValue, Uri};
use hyper_util::client::proxy::matcher::Matcher;
use url::{Host, Url};

use crate::{FileError, FileResult};

pub(crate) struct HttpRouteConfig {
    matcher: Option<Matcher>,
}

impl HttpRouteConfig {
    pub(crate) fn local() -> Self {
        Self { matcher: None }
    }
    pub(crate) fn remote() -> Self {
        Self {
            matcher: Some(Matcher::from_system()),
        }
    }

    pub(crate) fn prepare(&self, uri: &Uri) -> FileResult<PreparedHttpRoute> {
        let target = HttpOrigin::from_uri(uri)?;
        // Match the canonical origin, as the original URL-based client did.
        // The signed request URI and raw path/query remain untouched.
        let matcher_uri = target.connector_uri()?;
        let proxy = self
            .matcher
            .as_ref()
            .and_then(|matcher| matcher.intercept(&matcher_uri))
            .map(|selected| -> FileResult<_> {
                let origin = HttpOrigin::from_uri(selected.uri())?;
                let mut authorization = selected.basic_auth().cloned();
                if let Some(value) = &mut authorization {
                    value.set_sensitive(true);
                }
                Ok(SelectedProxy {
                    origin,
                    authorization,
                })
            })
            .transpose()?;
        Ok(PreparedHttpRoute {
            original_uri: uri.clone(),
            target,
            proxy,
        })
    }

    #[cfg(test)]
    pub(super) fn with_matcher(matcher: Matcher) -> Self {
        Self {
            matcher: Some(matcher),
        }
    }
}

impl fmt::Debug for HttpRouteConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRouteConfig")
            .field("ambient_proxy", &self.matcher.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HttpScheme {
    Http,
    Https,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReferenceHost {
    Dns(String),
    Ip(IpAddr),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HttpOrigin {
    scheme: HttpScheme,
    host: ReferenceHost,
    port: u16,
}

impl HttpOrigin {
    fn from_uri(uri: &Uri) -> FileResult<Self> {
        // URL is used only for semantic origin projection. Keep the admitted URI untouched.
        let url = Url::parse(&uri.to_string()).map_err(|error| {
            FileError::with_source(
                crate::FileErrorKind::Invalid,
                "parse HTTP route origin",
                error,
            )
        })?;
        let scheme = match url.scheme() {
            "http" => HttpScheme::Http,
            "https" => HttpScheme::Https,
            _ => return Err(FileError::unsupported("HTTP route requires HTTP or HTTPS")),
        };
        let host = match url
            .host()
            .ok_or_else(|| FileError::invalid("HTTP route host is absent"))?
        {
            Host::Domain(name) => ReferenceHost::Dns(name.to_owned()),
            Host::Ipv4(address) => ReferenceHost::Ip(IpAddr::V4(address)),
            Host::Ipv6(address) => ReferenceHost::Ip(IpAddr::V6(address)),
        };
        let port = url
            .port_or_known_default()
            .ok_or_else(|| FileError::invalid("HTTP route port is absent"))?;
        Ok(Self { scheme, host, port })
    }
    pub(crate) fn scheme(&self) -> HttpScheme {
        self.scheme
    }
    pub(crate) fn host(&self) -> &ReferenceHost {
        &self.host
    }
    pub(crate) fn port(&self) -> u16 {
        self.port
    }
    pub(crate) fn connector_uri(&self) -> FileResult<Uri> {
        let scheme = match self.scheme {
            HttpScheme::Http => "http",
            HttpScheme::Https => "https",
        };
        let host = match &self.host {
            ReferenceHost::Dns(name) => name.clone(),
            ReferenceHost::Ip(IpAddr::V4(address)) => address.to_string(),
            ReferenceHost::Ip(IpAddr::V6(address)) => format!("[{address}]"),
        };
        format!("{scheme}://{host}:{}/", self.port)
            .parse()
            .map_err(|error| {
                FileError::with_source(
                    crate::FileErrorKind::Invalid,
                    "construct HTTP connector origin",
                    error,
                )
            })
    }
}

struct SelectedProxy {
    origin: HttpOrigin,
    authorization: Option<HeaderValue>,
}

pub(crate) struct PreparedHttpRoute {
    original_uri: Uri,
    target: HttpOrigin,
    proxy: Option<SelectedProxy>,
}

impl PreparedHttpRoute {
    pub(crate) fn original_uri(&self) -> &Uri {
        &self.original_uri
    }
    pub(crate) fn target(&self) -> &HttpOrigin {
        &self.target
    }
    pub(crate) fn proxy(&self) -> Option<&HttpOrigin> {
        self.proxy.as_ref().map(|p| &p.origin)
    }
    pub(super) fn proxy_authorization(&self) -> Option<&HeaderValue> {
        self.proxy.as_ref().and_then(|p| p.authorization.as_ref())
    }
    /// Only an actual plaintext-target forward-proxy request may carry this header.
    pub(crate) fn forward_proxy_authorization(&self) -> Option<&HeaderValue> {
        if self.target.scheme == HttpScheme::Http {
            self.proxy_authorization()
        } else {
            None
        }
    }
}

impl fmt::Debug for PreparedHttpRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print original URI, Intercept, authorization, userinfo or object query.
        f.debug_struct("PreparedHttpRoute")
            .field("target", &self.target)
            .field("proxy", &self.proxy())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "route_tests.rs"]
mod tests;
