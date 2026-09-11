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
