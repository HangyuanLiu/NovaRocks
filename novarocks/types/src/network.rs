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

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU16;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Canonical ASCII DNS reference identity for a Native endpoint.
///
/// This is intentionally an A-label-only type. Callers must perform any IDNA
/// conversion before crossing the Native endpoint boundary; a U-label cannot
/// silently become a different TLS reference identity inside a role runtime.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CanonicalDnsName(String);

impl CanonicalDnsName {
    pub fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() || value.len() > 253 {
            return Err("native DNS reference host must contain 1..=253 ASCII bytes".to_string());
        }
        if !value.is_ascii() {
            return Err("native DNS reference host must be an ASCII A-label".to_string());
        }
        if value.ends_with('.') {
            return Err("native DNS reference host must not have a trailing dot".to_string());
        }

        let canonical = value.to_ascii_lowercase();
        for label in canonical.split('.') {
            if label.is_empty() || label.len() > 63 {
                return Err(
                    "native DNS reference host labels must contain 1..=63 ASCII bytes".to_string(),
                );
            }
            let bytes = label.as_bytes();
            if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric()
            {
                return Err(
                    "native DNS reference host labels must start and end with alphanumeric bytes"
                        .to_string(),
                );
            }
            if !bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
            {
                return Err(
                    "native DNS reference host labels may contain only ASCII alphanumeric bytes or hyphen"
                        .to_string(),
                );
            }
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CanonicalDnsName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Reference host used for Native connection identity and TLS verification.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum NativeReferenceHost {
    Ip(IpAddr),
    Dns(CanonicalDnsName),
}

impl NativeReferenceHost {
    pub fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() || value.trim() != value {
            return Err(
                "native reference host must not be empty or contain surrounding whitespace"
                    .to_string(),
            );
        }
        if let Ok(address) = value.parse::<IpAddr>() {
            return Ok(Self::Ip(address));
        }
        CanonicalDnsName::parse(value).map(Self::Dns)
    }

    pub fn is_ip(&self) -> bool {
        matches!(self, Self::Ip(_))
    }

    pub fn is_dns(&self) -> bool {
        matches!(self, Self::Dns(_))
    }
}

impl fmt::Display for NativeReferenceHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(address) => address.fmt(formatter),
            Self::Dns(name) => name.fmt(formatter),
        }
    }
}

/// Canonical Native endpoint. Its reference host is deliberately distinct
/// from a later DNS dial result so a TLS verifier and Channel cache keep the
/// same identity that topology admitted.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NativeEndpoint {
    reference_host: NativeReferenceHost,
    port: NonZeroU16,
    canonical_host: String,
}

impl NativeEndpoint {
    pub fn new(reference_host: NativeReferenceHost, port: u16) -> Result<Self, String> {
        let port = NonZeroU16::new(port)
            .ok_or_else(|| "native endpoint port must be in 1..=65535".to_string())?;
        let canonical_host = reference_host.to_string();
        Ok(Self {
            reference_host,
            port,
            canonical_host,
        })
    }

    pub fn from_host_port(host: &str, port: u16) -> Result<Self, String> {
        Self::new(NativeReferenceHost::parse(host)?, port)
    }

    pub fn from_socket_addr(address: SocketAddr) -> Self {
        Self::new(NativeReferenceHost::Ip(address.ip()), address.port())
            .expect("SocketAddr always has a nonzero port when used as a Native endpoint")
    }

    pub fn reference_host(&self) -> &NativeReferenceHost {
        &self.reference_host
    }

    pub fn host(&self) -> &str {
        &self.canonical_host
    }

    pub fn host_capacity(&self) -> usize {
        self.canonical_host.capacity()
    }

    pub fn port(&self) -> u16 {
        self.port.get()
    }

    pub fn as_host_port(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for NativeEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reference_host {
            NativeReferenceHost::Ip(IpAddr::V6(_)) => {
                write!(formatter, "[{}]:{}", self.canonical_host, self.port)
            }
            NativeReferenceHost::Ip(IpAddr::V4(_)) | NativeReferenceHost::Dns(_) => {
                write!(formatter, "{}:{}", self.canonical_host, self.port)
            }
        }
    }
}

impl FromStr for NativeEndpoint {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || value.trim() != value {
            return Err(
                "native endpoint must not be empty or contain surrounding whitespace".to_string(),
            );
        }
        if let Ok(address) = value.parse::<SocketAddr>() {
            return Self::new(NativeReferenceHost::Ip(address.ip()), address.port());
        }
        let (host, port) = value
            .rsplit_once(':')
            .ok_or_else(|| format!("native endpoint must be host:port, got '{value}'"))?;
        if host.contains(':') {
            return Err(format!(
                "native endpoint IPv6 reference must use brackets, got '{value}'"
            ));
        }
        let port = port
            .parse::<u16>()
            .map_err(|error| format!("native endpoint has invalid port '{value}': {error}"))?;
        Self::from_host_port(host, port)
    }
}

impl Serialize for NativeEndpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for NativeEndpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvertiseEndpoint {
    pub host: String,
    pub port: u16,
}

pub fn format_host_for_url(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{CanonicalDnsName, NativeEndpoint, NativeReferenceHost, format_host_for_url};

    #[test]
    fn format_host_for_url_wraps_ipv6() {
        assert_eq!(format_host_for_url("2001:db8::1"), "[2001:db8::1]");
        assert_eq!(format_host_for_url("10.0.0.9"), "10.0.0.9");
    }

    #[test]
    fn native_endpoint_round_trips_ipv4_ipv6_and_dns() {
        for (input, host, port) in [
            ("10.0.0.9:8060", "10.0.0.9", 8060),
            ("[2001:db8::9]:8060", "2001:db8::9", 8060),
            ("BE-1.Example.Internal:8060", "be-1.example.internal", 8060),
        ] {
            let endpoint: NativeEndpoint = input.parse().expect("valid endpoint");
            assert_eq!(endpoint.host(), host);
            assert_eq!(endpoint.port(), port);
            assert_eq!(endpoint.to_string().parse::<NativeEndpoint>(), Ok(endpoint));
        }
    }

    #[test]
    fn native_endpoint_dns_identity_does_not_collapse_to_ip() {
        let first: NativeEndpoint = "be-a.example:8060".parse().expect("first endpoint");
        let second: NativeEndpoint = "be-b.example:8060".parse().expect("second endpoint");

        assert_ne!(first, second);
        assert!(first.reference_host().is_dns());
        assert!(
            NativeReferenceHost::parse("127.0.0.1")
                .expect("IP reference")
                .is_ip()
        );
    }

    #[test]
    fn native_endpoint_rejects_ambiguous_dns_and_unbracketed_ipv6() {
        for input in [
            "",
            " be.example:8060",
            "be.example.:8060",
            "*.example:8060",
            "bücher.example:8060",
            "be_.example:8060",
            "2001:db8::1:8060",
            "be.example:0",
            "127.0.0.1:0",
            "[2001:db8::1]:0",
        ] {
            assert!(input.parse::<NativeEndpoint>().is_err(), "{input}");
        }
        assert!(CanonicalDnsName::parse("be-1.example").is_ok());
    }

    #[test]
    fn native_endpoint_serde_uses_canonical_host_port() {
        let endpoint: NativeEndpoint = "BE.EXAMPLE:8060".parse().expect("endpoint");
        let encoded = serde_json::to_string(&endpoint).expect("serialize endpoint");

        assert_eq!(encoded, "\"be.example:8060\"");
        assert_eq!(
            serde_json::from_str::<NativeEndpoint>(&encoded).expect("deserialize endpoint"),
            endpoint
        );
    }
}
