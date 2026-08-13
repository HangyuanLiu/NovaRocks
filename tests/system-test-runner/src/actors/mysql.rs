use anyhow::{Context, Result};
use mysql::{Conn, OptsBuilder};
use std::time::Duration;

/// Connect through the public MySQL protocol with a bounded connect timeout.
pub fn connect(user: &str, port: u16, timeout: Duration) -> Result<Conn> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some(user))
        .tcp_connect_timeout(Some(timeout));
    Conn::new(builder).with_context(|| format!("connect MySQL actor at 127.0.0.1:{port}"))
}
