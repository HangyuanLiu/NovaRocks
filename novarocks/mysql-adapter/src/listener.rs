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

#[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
mod fixture_join_observation;
#[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
pub(crate) use fixture_join_observation::MysqlFixtureSessionJoins;
#[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
pub(crate) use fixture_join_observation::{WatcherAbortGuard, WatcherFacts, WatcherPermit};

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
    serve_tcp_until_drain_then_shutdown_admitted(
        bind_addr,
        drain,
        finalize,
        move |stream, peer| Some(session_handler(stream, peer)),
        on_ready,
        cleanup_timeout,
    )
    .await
}

/// Stop accepting at `drain`, run `finalize`, then drain existing tasks.
pub(crate) async fn serve_tcp_until_drain_then_shutdown_admitted<F, G, H, HFut, R>(
    bind_addr: SocketAddr,
    drain: F,
    finalize: G,
    session_handler: H,
    on_ready: R,
    cleanup_timeout: Duration,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    H: FnMut(TcpStream, SocketAddr) -> Option<HFut>,
    HFut: Future<Output = ()> + Send + 'static,
    R: FnOnce(SocketAddr),
{
    serve_tcp_until_drain_then_shutdown_with_joins(
        bind_addr,
        drain,
        finalize,
        session_handler,
        on_ready,
        cleanup_timeout,
        |result, _aborting| log_session_join_error(result),
        #[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
        None,
    )
    .await
}

#[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
pub(crate) async fn serve_tcp_until_drain_then_shutdown_admitted_observed<F, G, H, HFut, R>(
    bind_addr: SocketAddr,
    drain: F,
    finalize: G,
    session_handler: H,
    on_ready: R,
    cleanup_timeout: Duration,
    observation: std::sync::Arc<MysqlFixtureSessionJoins>,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    H: FnMut(TcpStream, SocketAddr) -> Option<HFut>,
    HFut: Future<Output = ()> + Send + 'static,
    R: FnOnce(SocketAddr),
{
    let watcher_observation = std::sync::Arc::clone(&observation);
    serve_tcp_until_drain_then_shutdown_with_joins(
        bind_addr,
        drain,
        finalize,
        session_handler,
        on_ready,
        cleanup_timeout,
        move |result, aborting| observation.observe(result, aborting),
        Some(watcher_observation),
    )
    .await
}

async fn serve_tcp_until_drain_then_shutdown_with_joins<F, G, H, HFut, R, J>(
    bind_addr: SocketAddr,
    drain: F,
    finalize: G,
    mut session_handler: H,
    on_ready: R,
    cleanup_timeout: Duration,
    mut observe_join: J,
    #[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))] watcher_observation: Option<
        std::sync::Arc<MysqlFixtureSessionJoins>,
    >,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    G: Future<Output = ()> + Send,
    H: FnMut(TcpStream, SocketAddr) -> Option<HFut>,
    HFut: Future<Output = ()> + Send + 'static,
    R: FnOnce(SocketAddr),
    J: FnMut(Result<(), tokio::task::JoinError>, bool),
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
        #[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
        let observe_watchers = watcher_observation.is_some();
        #[cfg(not(any(test, feature = "mem-1-m07-exact-mysql-write")))]
        let observe_watchers = false;
        tokio::select! {
            biased;
            _ = &mut drain => break Ok(()),
            _ = async {
                #[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
                std::future::poll_fn(|cx| {
                    match watcher_observation.as_ref().expect("fixture branch enabled").poll_next_watcher(cx) {
                        std::task::Poll::Ready(None) => std::task::Poll::Pending,
                        joined => joined,
                    }
                }).await;
                #[cfg(not(any(test, feature = "mem-1-m07-exact-mysql-write")))]
                std::future::pending::<()>().await;
            }, if observe_watchers => {},
            completed = sessions.join_next(), if !sessions.is_empty() => {
                if let Some(result) = completed {
                    observe_join(result, false);
                }
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, peer_addr)) => {
                    if let Some(session) = session_handler(stream, peer_addr) {
                        sessions.spawn(session);
                    }
                }
                Err(error) => break Err(format!("accept MySQL connection failed: {error}")),
            },
        }
    };
    drop(listener);
    if serve_result.is_err() {
        drain_session_tasks_with_joins(&mut sessions, cleanup_timeout, &mut observe_join).await;
        #[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
        join_original_fixture_watchers(watcher_observation.as_deref()).await;
        return serve_result;
    }
    finalize.await;
    drain_session_tasks_with_joins(&mut sessions, cleanup_timeout, &mut observe_join).await;
    #[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
    join_original_fixture_watchers(watcher_observation.as_deref()).await;
    serve_result
}

#[cfg(any(test, feature = "mem-1-m07-exact-mysql-write"))]
async fn join_original_fixture_watchers(observation: Option<&MysqlFixtureSessionJoins>) {
    if let Some(observation) = observation {
        // Sessions already joined and dropped their abort guards. Retain every
        // original watcher handle through its actual Ready result, also on accept failure.
        observation.abort_remaining_watchers();
        while observation.next_watcher().await.is_some() {}
    }
}

fn log_session_join_error(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result
        && !error.is_cancelled()
    {
        warn!("MySQL connection task failed: {error}");
    }
}

async fn drain_session_tasks(sessions: &mut JoinSet<()>, drain_timeout: Duration) {
    drain_session_tasks_with_joins(sessions, drain_timeout, &mut |result, _aborting| {
        log_session_join_error(result)
    })
    .await;
}

async fn drain_session_tasks_with_joins<J>(
    sessions: &mut JoinSet<()>,
    drain_timeout: Duration,
    observe_join: &mut J,
) where
    J: FnMut(Result<(), tokio::task::JoinError>, bool),
{
    let drain = async {
        while let Some(result) = sessions.join_next().await {
            observe_join(result, false);
        }
    };
    if tokio::time::timeout(drain_timeout, drain).await.is_ok() {
        return;
    }

    sessions.abort_all();
    while let Some(result) = sessions.join_next().await {
        observe_join(result, true);
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

    #[cfg(unix)]
    async fn actual_listener_and_watcher_exit(mode: u8) {
        use crate::{MysqlClientConnectionRegistry, spawn_disconnect_watcher};
        let registry = Arc::new(MysqlClientConnectionRegistry::new());
        let connections = Arc::clone(&registry);
        let observation = Arc::new(MysqlFixtureSessionJoins::default());
        let joins = Arc::clone(&observation);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (drain_tx, drain_rx) = oneshot::channel();
        let drain = Arc::new(Mutex::new(Some(drain_tx)));
        let client_drain = Arc::clone(&drain);
        let (release_tx, release_rx) = oneshot::channel();
        let release = Arc::new(Mutex::new(Some(release_rx)));
        let (started_tx, started_rx) = oneshot::channel();
        let started = Arc::new(Mutex::new(Some(started_tx)));
        let server = serve_tcp_until_drain_then_shutdown_admitted_observed(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            async move {
                let _ = drain_rx.await;
            },
            async {},
            move |stream, _| {
                let registration = connections.register().ok()?;
                let permit = joins.reserve_watcher(registration.retain_owner()).ok()?;
                let release = Arc::clone(&release);
                let started = Arc::clone(&started);
                Some(async move {
                    let _registration = registration;
                    let _watcher = permit.attach(spawn_disconnect_watcher(&stream, move || {
                        if mode == 0 {
                            panic!("actual original watcher callback failure");
                        }
                    }));
                    let _stream = stream;
                    if let Some(started) = started.lock().unwrap().take() {
                        let _ = started.send(());
                    }
                    if mode == 1 {
                        panic!("actual original session failure with watcher");
                    }
                    if mode == 2 {
                        pending::<()>().await;
                    }
                    let release = release.lock().unwrap().take();
                    if let Some(release) = release {
                        let _ = release.await;
                    }
                })
            },
            move |addr| {
                let _ = ready_tx.send(addr);
            },
            Duration::from_millis(20),
            Arc::clone(&observation),
        );
        tokio::pin!(server);
        let observed = Arc::clone(&observation);
        let client = async move {
            let addr = ready_rx.await.map_err(io_error)?;
            let mut client = TcpStream::connect(addr).await?;
            started_rx.await.map_err(io_error)?;
            if mode == 0 {
                drop(client);
                observed.wait_for_failure().await;
                let _ = release_tx.send(());
                if let Some(sender) = client_drain.lock().unwrap().take() {
                    let _ = sender.send(());
                }
            } else {
                // Hold the original peer until the session panic or bounded abort.
                drop(release_tx);
                if mode == 1 {
                    observed.wait_for_failure().await;
                }
                if let Some(sender) = client_drain.lock().unwrap().take() {
                    let _ = sender.send(());
                }
                use tokio::io::AsyncReadExt;
                if client.read(&mut [0; 1]).await? != 0 {
                    return Err(std::io::Error::other(
                        "fixture original socket did not close at EOF",
                    ));
                }
            }
            Ok::<(), std::io::Error>(())
        };
        tokio::pin!(client);
        let deadline = tokio::time::sleep(TEST_TIMEOUT);
        tokio::pin!(deadline);
        let mut server_result = None;
        let mut client_result = None;
        let mut timed_out = false;
        while server_result.is_none() || client_result.is_none() {
            tokio::select! {
                result = &mut server, if server_result.is_none() => server_result = Some(result),
                result = &mut client, if client_result.is_none() => {
                    if result.is_err() {
                        if let Some(sender) = drain.lock().unwrap().take() { let _ = sender.send(()); }
                    }
                    client_result = Some(result);
                },
                _ = &mut deadline => { timed_out = true; break; },
            }
        }
        if let Some(sender) = drain.lock().unwrap().take() {
            let _ = sender.send(());
        }
        // A timeout never takes or drops the original server future; finish its original owners.
        if server_result.is_none() {
            server_result = Some(server.await);
        }
        registry.wait_drained().await;
        assert!(
            !timed_out,
            "component deadline; original owners already joined"
        );
        client_result.unwrap().unwrap();
        server_result.unwrap().unwrap();
        let watchers = observation.watcher_snapshot();
        assert_eq!(watchers.reserved, 0);
        assert_eq!(watchers.joined, 1);
        if mode == 0 {
            let actual = observation.take_watcher_failure_after_join().unwrap();
            assert!(actual.is_panic());
            assert_eq!(
                *actual.into_panic().downcast::<&str>().unwrap(),
                "actual original watcher callback failure"
            );
        } else {
            assert_eq!(watchers.expected_cancelled, 1);
            assert!(!watchers.failed());
        }
        if mode == 1 {
            let actual = observation.take_failure_after_join().unwrap();
            assert!(actual.is_panic());
            assert_eq!(
                *actual.into_panic().downcast::<&str>().unwrap(),
                "actual original session failure with watcher"
            );
        }
        if mode == 2 {
            assert_eq!(observation.snapshot().aborted, 1);
        }
    }
    #[cfg(unix)]
    fn io_error(error: impl std::error::Error + Send + Sync + 'static) -> std::io::Error {
        std::io::Error::other(error)
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn fixture_active_listener_reaps_original_watcher_panic_without_new_task() {
        actual_listener_and_watcher_exit(0).await;
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn fixture_session_panic_keeps_original_watcher_handle_until_join() {
        actual_listener_and_watcher_exit(1).await;
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn fixture_bounded_session_abort_keeps_original_watcher_handle_until_join() {
        actual_listener_and_watcher_exit(2).await;
    }

    #[tokio::test]
    async fn fixture_retains_actual_session_panic_after_original_join_and_socket_drop() {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (drain_tx, drain_rx) = oneshot::channel();
        let (started_tx, started_rx) = oneshot::channel();
        let started = Arc::new(Mutex::new(Some(started_tx)));
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = Arc::clone(&dropped);
        let observation = Arc::new(MysqlFixtureSessionJoins::default());
        let server = tokio::spawn(serve_tcp_until_drain_then_shutdown_admitted_observed(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            async move {
                let _ = drain_rx.await;
            },
            async {},
            move |stream, _peer| {
                let probe = Arc::clone(&probe);
                let started = Arc::clone(&started);
                Some(async move {
                    let _stream = stream;
                    let _probe = DropProbe(probe);
                    if let Some(sender) = started.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                    panic!("intentional actual MySQL listener session panic");
                })
            },
            move |addr| {
                let _ = ready_tx.send(addr);
            },
            TEST_TIMEOUT,
            Arc::clone(&observation),
        ));
        let addr = ready_rx.await.unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        started_rx.await.unwrap();
        drain_tx.send(()).unwrap();
        tokio::time::timeout(TEST_TIMEOUT, server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        use tokio::io::AsyncReadExt;
        assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        let facts = observation.snapshot();
        assert_eq!(facts.joined, 1);
        assert_eq!(facts.panicked, 1);
        assert_eq!(
            facts.succeeded + facts.aborted + facts.unexpected_cancelled,
            0
        );
        let original = observation
            .take_failure_after_join()
            .expect("original actual JoinError");
        assert!(original.is_panic());
        assert_eq!(
            *original.into_panic().downcast::<&str>().unwrap(),
            "intentional actual MySQL listener session panic"
        );
        assert!(observation.take_failure_after_join().is_none());
    }

    #[tokio::test]
    async fn fixture_counts_actual_abort_only_after_original_bounded_drain() {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (drain_tx, drain_rx) = oneshot::channel();
        let (started_tx, started_rx) = oneshot::channel();
        let started = Arc::new(Mutex::new(Some(started_tx)));
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = Arc::clone(&dropped);
        let observation = Arc::new(MysqlFixtureSessionJoins::default());
        let server = tokio::spawn(serve_tcp_until_drain_then_shutdown_admitted_observed(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            async move {
                let _ = drain_rx.await;
            },
            async {},
            move |stream, _peer| {
                let probe = Arc::clone(&probe);
                let started = Arc::clone(&started);
                Some(async move {
                    let _stream = stream;
                    let _probe = DropProbe(probe);
                    if let Some(sender) = started.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                    pending::<()>().await;
                })
            },
            move |addr| {
                let _ = ready_tx.send(addr);
            },
            Duration::from_millis(20),
            Arc::clone(&observation),
        ));
        let addr = ready_rx.await.unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        started_rx.await.unwrap();
        drain_tx.send(()).unwrap();
        tokio::time::timeout(TEST_TIMEOUT, server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        use tokio::io::AsyncReadExt;
        assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        let facts = observation.snapshot();
        assert_eq!(facts.joined, 1);
        assert_eq!(facts.aborted, 1);
        assert_eq!(
            facts.succeeded + facts.panicked + facts.unexpected_cancelled,
            0
        );
        assert!(!facts.counter_overflow);
        assert!(observation.take_failure_after_join().is_none());
    }

    #[tokio::test]
    async fn refused_admission_closes_socket_without_creating_a_session_task() {
        use tokio::io::AsyncReadExt;
        let (ready_tx, ready_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let server = tokio::spawn(serve_tcp_until_drain_then_shutdown_admitted(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            async {
                let _ = shutdown_rx.await;
            },
            async {},
            move |_stream, _peer| {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                None::<std::future::Ready<()>>
            },
            move |addr| {
                let _ = ready_tx.send(addr);
            },
            TEST_TIMEOUT,
        ));
        let addr = ready_rx.await.unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut bytes = [0; 1];
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, client.read(&mut bytes))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
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
