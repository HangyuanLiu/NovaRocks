use anyhow::{Context, Result};
use mysql::{Conn, OptsBuilder};
use std::time::Duration;

/// Connect through the public MySQL protocol with bounded socket operations.
pub fn connect(user: &str, port: u16, timeout: Duration) -> Result<Conn> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some(user))
        .tcp_connect_timeout(Some(timeout))
        .read_timeout(Some(timeout))
        .write_timeout(Some(timeout));
    Conn::new(builder).with_context(|| format!("connect MySQL actor at 127.0.0.1:{port}"))
}

/// Connect a query actor whose response is intentionally held until another
/// public MySQL session cancels it. The scenario deadline, not a socket read
/// timeout, bounds the wait: on macOS the synchronous client maps a socket
/// read timeout while awaiting the cancellation response to EAGAIN.
pub fn connect_for_cancellation(user: &str, port: u16, connect_timeout: Duration) -> Result<Conn> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some(user))
        .tcp_connect_timeout(Some(connect_timeout));
    Conn::new(builder)
        .with_context(|| format!("connect cancellation MySQL actor at 127.0.0.1:{port}"))
}
