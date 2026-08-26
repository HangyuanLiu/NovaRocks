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

use std::net::SocketAddr;

use novarocks_types::{NativeEndpoint, UniqueId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeEndpoint {
    endpoint: NativeEndpoint,
}

impl RuntimeEndpoint {
    pub fn new(host: impl Into<String>, port: i32) -> Result<Self, String> {
        let host = host.into();
        if !(1..=i32::from(u16::MAX)).contains(&port) {
            return Err(format!(
                "native runtime endpoint port {port} must be in 1..={}",
                u16::MAX
            ));
        }
        Ok(Self {
            endpoint: NativeEndpoint::from_host_port(&host, port as u16)?,
        })
    }

    pub fn host(&self) -> &str {
        self.endpoint.host()
    }

    pub fn retained_host_capacity(&self) -> usize {
        self.endpoint.host_capacity()
    }

    pub fn port(&self) -> i32 {
        i32::from(self.endpoint.port())
    }

    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self {
            endpoint: NativeEndpoint::from_socket_addr(addr),
        }
    }

    /// Parse the neutral execution endpoint value used at role boundaries.
    pub fn parse(src: &str) -> Result<Self, String> {
        let endpoint = src
            .parse::<NativeEndpoint>()
            .map_err(|error| format!("native runtime endpoint is invalid: {error}"))?;
        Ok(Self { endpoint })
    }

    pub fn as_host_port(&self) -> String {
        self.endpoint.as_host_port()
    }

    pub fn native_endpoint(&self) -> &NativeEndpoint {
        &self.endpoint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentDestination {
    finst_id: UniqueId,
    endpoint: RuntimeEndpoint,
}

impl FragmentDestination {
    pub fn new(finst_id: UniqueId, endpoint: RuntimeEndpoint) -> Self {
        Self { finst_id, endpoint }
    }

    pub fn finst_id(&self) -> &UniqueId {
        &self.finst_id
    }

    pub fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProberDestination {
    fragment_instance_id: UniqueId,
    endpoint: RuntimeEndpoint,
}

impl RuntimeFilterProberDestination {
    pub fn new(fragment_instance_id: UniqueId, endpoint: RuntimeEndpoint) -> Self {
        Self {
            fragment_instance_id,
            endpoint,
        }
    }

    pub fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_host_port_endpoint() {
        let endpoint = RuntimeEndpoint::parse("BE-1.Internal:8060").expect("endpoint");

        assert_eq!(endpoint.host(), "be-1.internal");
        assert_eq!(endpoint.port(), 8060);
        assert_eq!(endpoint.as_host_port(), "be-1.internal:8060");
    }

    #[test]
    fn rejects_missing_separator() {
        let err = RuntimeEndpoint::parse("be-1.internal").expect_err("missing separator");

        assert!(err.contains("must be host:port"), "{err}");
    }

    #[test]
    fn rejects_empty_host() {
        let err = RuntimeEndpoint::parse(":8060").expect_err("empty host");

        assert!(err.contains("reference host"), "{err}");
    }

    #[test]
    fn rejects_non_numeric_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:not-a-port").expect_err("invalid port");

        assert!(err.contains("invalid port"), "{err}");
    }

    #[test]
    fn rejects_i32_overflow_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:2147483648").expect_err("overflow port");

        assert!(err.contains("invalid port"), "{err}");
    }

    #[test]
    fn rejects_zero_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:0").expect_err("zero port");

        assert!(err.contains("must be in 1..=65535"), "{err}");
    }

    #[test]
    fn rejects_negative_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:-1").expect_err("negative port");

        assert!(err.contains("invalid port"), "{err}");
    }

    #[test]
    fn rejects_invalid_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:70000").expect_err("invalid port");

        assert!(err.contains("invalid port"), "{err}");
    }
}
