use anyhow::{Context, Result};
use mysql::{Conn, OptsBuilder};
use novarocks_test_support::SocketDeadlineOwner;
use std::time::Duration;
use std::time::Instant;

/// The original connected socket remains supervised through Conn's Drop.
/// Construction retains the user-excluded per-I/O handshake limitation.
pub struct DeadlineConnection {
    connection: Option<Conn>,
    socket: SocketDeadlineOwner,
}
impl std::ops::Deref for DeadlineConnection {
    type Target = Conn;
    fn deref(&self) -> &Conn {
        self.connection.as_ref().expect("live deadline connection")
    }
}
impl std::ops::DerefMut for DeadlineConnection {
    fn deref_mut(&mut self) -> &mut Conn {
        self.connection.as_mut().expect("live deadline connection")
    }
}
impl DeadlineConnection {
    pub fn run<T>(mut self, operation: impl FnOnce(&mut Conn) -> Result<T>) -> Result<T> {
        let result = operation(self.connection.as_mut().expect("live deadline connection"));
        let cleanup = self.finish();
        match (result, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(primary), Err(cleanup)) => {
                anyhow::bail!("{primary:#}; MySQL socket cleanup: {cleanup:#}")
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }
    pub fn finish(mut self) -> Result<()> {
        drop(self.connection.take());
        self.socket
            .close_and_join()
            .context("join original MySQL socket deadline owner")
    }
}
impl Drop for DeadlineConnection {
    fn drop(&mut self) {
        drop(self.connection.take());
        if let Err(error) = self.socket.close_and_join() {
            eprintln!("MySQL deadline connection cleanup failed: {error}");
        }
    }
}
pub fn protect_until(connection: Conn, deadline: Instant) -> Result<DeadlineConnection> {
    let socket = match SocketDeadlineOwner::duplicate(&connection, deadline) {
        Ok(socket) => socket,
        Err(error) => {
            use std::os::fd::AsRawFd;
            // SAFETY: this owned, live Conn retains its original descriptor
            // for this synchronous shutdown; shutdown never closes the fd.
            let stopped = unsafe { libc::shutdown(connection.as_raw_fd(), libc::SHUT_RDWR) };
            let shutdown = (stopped != 0).then(std::io::Error::last_os_error);
            drop(connection);
            anyhow::bail!("supervise original MySQL socket: {error}; shutdown={shutdown:?}");
        }
    };
    Ok(DeadlineConnection {
        connection: Some(connection),
        socket,
    })
}
pub fn connect_until(user: &str, port: u16, deadline: Instant) -> Result<DeadlineConnection> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    anyhow::ensure!(
        !remaining.is_zero(),
        "MySQL actor original deadline expired before connection"
    );
    protect_until(connect(user, port, remaining)?, deadline)
}
pub fn connect_for_cancellation_until(
    user: &str,
    port: u16,
    deadline: Instant,
) -> Result<DeadlineConnection> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    anyhow::ensure!(
        !remaining.is_zero(),
        "MySQL actor original deadline expired before connection"
    );
    protect_until(
        connect_with_io_timeout(user, port, remaining, remaining)?,
        deadline,
    )
}

/// Connect through the public MySQL protocol with bounded socket operations.
pub fn connect(user: &str, port: u16, timeout: Duration) -> Result<Conn> {
    connect_with_io_timeout(user, port, timeout, timeout)
}

/// Connect with a distinct bound for socket I/O.
///
/// Performance scenarios use this when the SQL statement and the local job
/// deadline have the same business bound. A small transport grace lets the
/// server return that business result instead of having the synchronous
/// client surface the socket timeout first.
pub fn connect_with_io_timeout(
    user: &str,
    port: u16,
    connect_timeout: Duration,
    io_timeout: Duration,
) -> Result<Conn> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some(user))
        .tcp_connect_timeout(Some(connect_timeout))
        .read_timeout(Some(io_timeout))
        .write_timeout(Some(io_timeout));
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

/// Keep the actual SQL error when a failed phase closes its result receiver.
pub fn publish_result<T>(
    sender: &std::sync::mpsc::SyncSender<mysql::Result<T>>,
    outcome: mysql::Result<T>,
) -> Result<()> {
    match sender.send(outcome) {
        Ok(()) => Ok(()),
        Err(std::sync::mpsc::SendError(Err(actual))) => Err(anyhow::Error::from(actual)
            .context("original MySQL query failed after its result owner closed")),
        Err(std::sync::mpsc::SendError(Ok(_))) => {
            anyhow::bail!("original MySQL result owner closed before delivery")
        }
    }
}

#[cfg(test)]
mod deadline_result_tests {
    use super::*;
    #[test]
    fn closed_result_owner_keeps_the_original_query_failure() {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<mysql::Result<()>>(1);
        drop(receiver);
        let original = mysql::Error::from(std::io::Error::other("original SQL failure"));
        let error = publish_result(&sender, Err(original)).unwrap_err();
        assert!(format!("{error:#}").contains("original SQL failure"));
        assert!(publish_result(&sender, Ok(())).is_err());
    }
}
