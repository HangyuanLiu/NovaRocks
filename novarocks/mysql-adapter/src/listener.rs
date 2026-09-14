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

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::warn;

/// Bind a TCP listener and retain accepted protocol tasks until `shutdown`.
///
/// The protocol adapter owns listener lifetime and bounded task draining. The
/// caller owns every accepted connection's application semantics through the
/// injected handler.
pub async fn serve_tcp_until_shutdown<F, H, HFut, R>(
    bind_addr: SocketAddr,
    shutdown: F,
    session_handler: H,
    on_ready: R,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    H: FnMut(TcpStream, SocketAddr) -> HFut,
    HFut: Future<Output = ()> + Send + 'static,
    R: FnOnce(SocketAddr),
{
    serve_tcp_until_shutdown_with_drain_timeout(
        bind_addr,
        shutdown,
        session_handler,
        on_ready,
        Duration::from_secs(5),
    )
    .await
}

/// Bind a TCP listener and abort only sessions that exceed the bounded drain.
pub async fn serve_tcp_until_shutdown_with_drain_timeout<F, H, HFut, R>(
    bind_addr: SocketAddr,
    shutdown: F,
    mut session_handler: H,
    on_ready: R,
    drain_timeout: Duration,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    H: FnMut(TcpStream, SocketAddr) -> HFut,
    HFut: Future<Output = ()> + Send + 'static,
    R: FnOnce(SocketAddr),
{
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|error| format!("bind MySQL listener on {bind_addr} failed: {error}"))?;
    let bound_addr = listener
        .local_addr()
        .map_err(|error| format!("read MySQL listener address failed: {error}"))?;
    on_ready(bound_addr);

    let mut sessions = JoinSet::new();
    tokio::pin!(shutdown);
    let serve_result = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            completed = sessions.join_next(), if !sessions.is_empty() => {
                if let Some(result) = completed {
                    log_session_join_error(result);
                }
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, peer_addr)) => {
                    sessions.spawn(session_handler(stream, peer_addr));
                }
                Err(error) => break Err(format!("accept MySQL connection failed: {error}")),
            },
        }
    };

    drop(listener);
    drain_session_tasks(&mut sessions, drain_timeout).await;
    serve_result
}

/// Stop accepting at `drain`, run `finalize`, then drain existing tasks.
pub async fn serve_tcp_until_drain_then_shutdown<F, G, H, HFut, R>(
    bind_addr: SocketAddr,
    drain: F,
    finalize: G,
    mut session_handler: H,
    on_ready: R,
    cleanup_timeout: Duration,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    H: FnMut(TcpStream, SocketAddr) -> HFut,
    HFut: Future<Output = ()> + Send + 'static,
    R: FnOnce(SocketAddr),
{
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|error| format!("bind MySQL listener on {bind_addr} failed: {error}"))?;
    let bound_addr = listener
        .local_addr()
        .map_err(|error| format!("read MySQL listener address failed: {error}"))?;
    on_ready(bound_addr);

    let mut sessions = JoinSet::new();
    tokio::pin!(drain);
    let serve_result = loop {
        tokio::select! {
            biased;
            _ = &mut drain => break Ok(()),
            completed = sessions.join_next(), if !sessions.is_empty() => {
                if let Some(result) = completed {
                    log_session_join_error(result);
                }
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, peer_addr)) => {
                    sessions.spawn(session_handler(stream, peer_addr));
                }
                Err(error) => break Err(format!("accept MySQL connection failed: {error}")),
            },
        }
    };
    drop(listener);
    if serve_result.is_err() {
        drain_session_tasks(&mut sessions, cleanup_timeout).await;
        return serve_result;
    }
    finalize.await;
    drain_session_tasks(&mut sessions, cleanup_timeout).await;
    serve_result
}

fn log_session_join_error(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result
        && !error.is_cancelled()
    {
        warn!("MySQL connection task failed: {error}");
    }
}

async fn drain_session_tasks(sessions: &mut JoinSet<()>, drain_timeout: Duration) {
    let drain = async {
        while let Some(result) = sessions.join_next().await {
            log_session_join_error(result);
        }
    };
    if tokio::time::timeout(drain_timeout, drain).await.is_ok() {
        return;
    }

    sessions.abort_all();
    while let Some(result) = sessions.join_next().await {
        log_session_join_error(result);
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::net::Ipv4Addr;
    use std::sync::{Arc, Mutex};

    use super::*;
    use tokio::sync::oneshot;

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);

    #[derive(Clone)]
    struct DropProbe(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    async fn wait_until_connect_refused(addr: SocketAddr) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match TcpStream::connect(addr).await {
                    Ok(stream) => drop(stream),
                    Err(_) => break,
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("listener should stop accepting within the test timeout");
    }

    #[tokio::test]
    async fn shutdown_before_first_connection_stops_accepting() {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(serve_tcp_until_shutdown(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            async move {
                let _ = shutdown_rx.await;
            },
            |_stream, _peer_addr| async move {
                panic!("no connection should be accepted before shutdown")
            },
            move |addr| {
                let _ = ready_tx.send(addr);
            },
        ));
        let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
            .await
            .expect("server should bind within the test timeout")
            .expect("ready sender should stay alive");

        shutdown_tx.send(()).expect("send shutdown");
        tokio::time::timeout(TEST_TIMEOUT, server)
            .await
            .expect("server should stop within the test timeout")
            .expect("server task should not panic")
            .expect("server shutdown should succeed");
        assert!(TcpStream::connect(addr).await.is_err());
    }

    #[tokio::test]
    async fn drain_stops_accepts_before_active_session_finishes() {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (drain_tx, drain_rx) = oneshot::channel();
        let (finalize_tx, finalize_rx) = oneshot::channel();
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let (release_tx, release_rx) = oneshot::channel();
        let release_rx = Arc::new(Mutex::new(Some(release_rx)));
        let server = tokio::spawn(serve_tcp_until_drain_then_shutdown(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            async move {
                let _ = drain_rx.await;
            },
            async move {
                let _ = finalize_rx.await;
            },
            move |_stream, _peer_addr| {
                let started_tx = Arc::clone(&started_tx);
                let release_rx = Arc::clone(&release_rx);
                async move {
                    if let Some(sender) = started_tx.lock().expect("started lock").take() {
                        let _ = sender.send(());
                    }
                    let receiver = { release_rx.lock().expect("release lock").take() };
                    if let Some(receiver) = receiver {
                        let _ = receiver.await;
                    }
                }
            },
            move |addr| {
                let _ = ready_tx.send(addr);
            },
            TEST_TIMEOUT,
        ));
        let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
            .await
            .expect("server should bind within the test timeout")
            .expect("ready sender should stay alive");
        let _client = TcpStream::connect(addr)
            .await
            .expect("connect active session");
        tokio::time::timeout(TEST_TIMEOUT, started_rx)
            .await
            .expect("session should start within the test timeout")
            .expect("session start sender should stay alive");

        drain_tx.send(()).expect("send drain");
        wait_until_connect_refused(addr).await;
        assert!(!server.is_finished(), "drain must retain active sessions");
        finalize_tx
            .send(())
            .expect("finish role-owned finalization");
        tokio::task::yield_now().await;
        assert!(
            !server.is_finished(),
            "finalization must not abort active sessions"
        );

        release_tx.send(()).expect("release active session");
        tokio::time::timeout(TEST_TIMEOUT, server)
            .await
            .expect("server should stop after session release")
            .expect("server task should not panic")
            .expect("server shutdown should succeed");
    }

    #[tokio::test]
    async fn bounded_drain_aborts_a_stuck_session() {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let session_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let session_dropped_in_task = Arc::clone(&session_dropped);
        let server = tokio::spawn(serve_tcp_until_shutdown_with_drain_timeout(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            async move {
                let _ = shutdown_rx.await;
            },
            move |_stream, _peer_addr| {
                let started_tx = Arc::clone(&started_tx);
                let session_dropped = Arc::clone(&session_dropped_in_task);
                async move {
                    let _probe = DropProbe(session_dropped);
                    if let Some(sender) = started_tx.lock().expect("started lock").take() {
                        let _ = sender.send(());
                    }
                    pending::<()>().await;
                }
            },
            move |addr| {
                let _ = ready_tx.send(addr);
            },
            Duration::from_millis(20),
        ));
        let addr = tokio::time::timeout(TEST_TIMEOUT, ready_rx)
            .await
            .expect("server should bind within the test timeout")
            .expect("ready sender should stay alive");
        let _client = TcpStream::connect(addr)
            .await
            .expect("connect stuck session");
        tokio::time::timeout(TEST_TIMEOUT, started_rx)
            .await
            .expect("session should start within the test timeout")
            .expect("session start sender should stay alive");

        shutdown_tx.send(()).expect("send shutdown");
        tokio::time::timeout(TEST_TIMEOUT, server)
            .await
            .expect("server should abort the stuck session")
            .expect("server task should not panic")
            .expect("server shutdown should succeed");
        assert!(session_dropped.load(std::sync::atomic::Ordering::SeqCst));
        assert!(TcpStream::connect(addr).await.is_err());
    }

    #[tokio::test]
    async fn bind_failure_returns_without_ready_marker() {
        let occupied = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("reserve test address");
        let addr = occupied.local_addr().expect("reserved address");
        let ready_emitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ready_emitted_in_callback = Arc::clone(&ready_emitted);

        let err = serve_tcp_until_shutdown(
            addr,
            pending::<()>(),
            |_stream, _peer_addr| async move {},
            move |_addr| ready_emitted_in_callback.store(true, std::sync::atomic::Ordering::SeqCst),
        )
        .await
        .expect_err("occupied address should fail to bind");

        assert!(err.contains("bind MySQL listener"), "{err}");
        assert!(!ready_emitted.load(std::sync::atomic::Ordering::SeqCst));
    }
}
