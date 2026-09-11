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

//! Socket-level client disconnect observation.

#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::task::JoinHandle;
#[cfg(unix)]
use tracing::warn;

/// The socket observation task retained by one protocol connection.
pub struct ClientDisconnectWatcher {
    join_handle: Option<JoinHandle<()>>,
}

impl ClientDisconnectWatcher {
    /// Returns a watcher with no platform-specific socket observer.
    pub const fn inactive() -> Self {
        Self { join_handle: None }
    }
}

impl Drop for ClientDisconnectWatcher {
    fn drop(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            handle.abort();
        }
    }
}

/// Starts protocol-level disconnect observation for one accepted socket.
///
/// The adapter only reports socket evidence through `on_disconnect`; the
/// caller owns the application cancellation policy and target session.
#[cfg(unix)]
pub fn spawn_disconnect_watcher(
    stream: &TcpStream,
    on_disconnect: impl Fn() + Send + Sync + 'static,
) -> ClientDisconnectWatcher {
    let fd = unsafe { libc::dup(stream.as_raw_fd()) };
    if fd < 0 {
        return ClientDisconnectWatcher::inactive();
    }
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    if let Err(error) = std_stream.set_nonblocking(true) {
        warn!("failed to configure MySQL disconnect monitor: {error}");
        return ClientDisconnectWatcher::inactive();
    }
    let watcher_stream = match TcpStream::from_std(std_stream) {
        Ok(stream) => stream,
        Err(error) => {
            warn!("failed to create MySQL disconnect monitor: {error}");
            return ClientDisconnectWatcher::inactive();
        }
    };
    let on_disconnect = Arc::new(on_disconnect);
    let join_handle = tokio::spawn(async move {
        let mut buf = [0_u8; 1];
        loop {
            match watcher_stream.peek(&mut buf).await {
                Ok(0) => {
                    on_disconnect();
                    break;
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(_) => {
                    on_disconnect();
                    break;
                }
            }
        }
    });
    ClientDisconnectWatcher {
        join_handle: Some(join_handle),
    }
}

/// Non-Unix builds retain no socket watcher because duplicating an accepted
/// descriptor is Unix-specific in the current protocol implementation.
#[cfg(not(unix))]
pub fn spawn_disconnect_watcher(
    _stream: &TcpStream,
    _on_disconnect: impl Fn() + Send + Sync + 'static,
) -> ClientDisconnectWatcher {
    ClientDisconnectWatcher::inactive()
}

#[cfg(all(test, unix))]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn peer_close_notifies_the_injected_application_callback() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let client = TcpStream::connect(address).await.expect("connect client");
        let (accepted, _) = listener.accept().await.expect("accept client");
        let (notified, receiver) = oneshot::channel();
        let notified = Arc::new(Mutex::new(Some(notified)));
        let callback = Arc::clone(&notified);
        let watcher = spawn_disconnect_watcher(&accepted, move || {
            if let Some(sender) = callback.lock().expect("callback mutex").take() {
                let _ = sender.send(());
            }
        });

        drop(client);

        tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("peer close must notify within the watcher deadline")
            .expect("watcher callback sender remains live");
        drop(watcher);
    }
}
