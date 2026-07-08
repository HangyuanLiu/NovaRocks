use std::net::SocketAddr;

use crate::common::types::UniqueId;
use crate::thrift::types;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeEndpoint {
    host: String,
    port: i32,
}

impl RuntimeEndpoint {
    pub(crate) fn new(host: impl Into<String>, port: i32) -> Result<Self, String> {
        let host = host.into();
        let host = host.trim().to_string();
        if host.is_empty() {
            return Err("native runtime endpoint host must not be empty".to_string());
        }
        if !(1..=i32::from(u16::MAX)).contains(&port) {
            return Err(format!(
                "native runtime endpoint port {port} must be in 1..={}",
                u16::MAX
            ));
        }
        Ok(Self { host, port })
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn port(&self) -> i32 {
        self.port
    }

    pub(crate) fn from_socket_addr(addr: SocketAddr) -> Self {
        Self {
            host: addr.ip().to_string(),
            port: i32::from(addr.port()),
        }
    }

    pub(crate) fn parse(src: &str) -> Result<Self, String> {
        let (host, port) = src
            .rsplit_once(':')
            .ok_or_else(|| format!("native runtime endpoint must be host:port, got '{src}'"))?;
        let port = port
            .parse::<i32>()
            .map_err(|e| format!("native runtime endpoint has invalid port '{src}': {e}"))?;
        Self::new(host, port)
    }

    pub(crate) fn as_host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub(crate) fn to_network_address(&self) -> types::TNetworkAddress {
        types::TNetworkAddress::new(self.host.clone(), self.port)
    }

    pub(crate) fn from_network_address(addr: &types::TNetworkAddress) -> Result<Self, String> {
        Self::new(addr.hostname.clone(), addr.port)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FragmentDestination {
    finst_id: types::TUniqueId,
    endpoint: RuntimeEndpoint,
}

impl FragmentDestination {
    pub(crate) fn new(finst_id: types::TUniqueId, endpoint: RuntimeEndpoint) -> Self {
        Self { finst_id, endpoint }
    }

    pub(crate) fn finst_id(&self) -> &types::TUniqueId {
        &self.finst_id
    }

    pub(crate) fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterProberDestination {
    fragment_instance_id: UniqueId,
    endpoint: RuntimeEndpoint,
}

impl RuntimeFilterProberDestination {
    pub(crate) fn new(fragment_instance_id: UniqueId, endpoint: RuntimeEndpoint) -> Self {
        Self {
            fragment_instance_id,
            endpoint,
        }
    }

    pub(crate) fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_host_port_endpoint() {
        let endpoint = RuntimeEndpoint::parse("be-1.internal:8060").expect("endpoint");

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

        assert!(err.contains("host must not be empty"), "{err}");
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

        assert!(err.contains("must be in 1..=65535"), "{err}");
    }

    #[test]
    fn rejects_invalid_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:70000").expect_err("invalid port");

        assert!(err.contains("must be in 1..=65535"), "{err}");
    }

    #[test]
    fn roundtrips_network_address() {
        let endpoint = RuntimeEndpoint::parse("be-1.internal:8060").expect("endpoint");
        let addr = endpoint.to_network_address();

        let roundtrip = RuntimeEndpoint::from_network_address(&addr).expect("roundtrip");

        assert_eq!(roundtrip, endpoint);
    }

    #[test]
    fn rejects_invalid_network_address() {
        let addr = types::TNetworkAddress::new("".to_string(), 8060);

        let err = RuntimeEndpoint::from_network_address(&addr).expect_err("invalid thrift addr");

        assert!(err.contains("host must not be empty"), "{err}");
    }
}
