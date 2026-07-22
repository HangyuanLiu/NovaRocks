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
use std::path::PathBuf;

use novarocks::common::app_config::NovaRocksConfig;
use novarocks_state_store::StateStoreAppConfig;

use crate::FrontendApplicationHost;

#[derive(Clone)]
pub struct FrontendServerConfig {
    pub config: NovaRocksConfig,
    pub config_path: Option<PathBuf>,
    pub port_override: Option<u16>,
    pub local_exchange: bool,
}

pub fn run_frontend_server(config: FrontendServerConfig) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| format!("build frontend Tokio runtime failed: {error}"))?;

    runtime.block_on(run_frontend_server_until_shutdown(config, async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("failed to listen for Ctrl-C: {error}");
        }
    }))
}

pub async fn run_frontend_server_until_shutdown<F>(
    config: FrontendServerConfig,
    shutdown: F,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
{
    run_frontend_server_until_shutdown_with_ports(
        config,
        shutdown,
        |state_store| async move {
            FrontendApplicationHost::open(state_store)
                .await
                .map_err(|error| error.to_string())
        },
        |config, shutdown| async move {
            novarocks::server::run_standalone_server_with_config_until_shutdown(
                config.config,
                config.config_path,
                config.port_override,
                config.local_exchange,
                shutdown,
            )
            .await
        },
        |host| async move { host.shutdown().await.map_err(|error| error.to_string()) },
    )
    .await
}

async fn run_frontend_server_until_shutdown_with_ports<
    F,
    Host,
    OpenHost,
    OpenHostFuture,
    Serve,
    ServeFuture,
    ShutdownHost,
    ShutdownHostFuture,
>(
    config: FrontendServerConfig,
    shutdown: F,
    open_host: OpenHost,
    serve: Serve,
    shutdown_host: ShutdownHost,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
    OpenHost: FnOnce(Option<StateStoreAppConfig>) -> OpenHostFuture,
    OpenHostFuture: Future<Output = Result<Host, String>>,
    Serve: FnOnce(FrontendServerConfig, F) -> ServeFuture,
    ServeFuture: Future<Output = Result<(), String>>,
    ShutdownHost: FnOnce(Host) -> ShutdownHostFuture,
    ShutdownHostFuture: Future<Output = Result<(), String>>,
{
    let host = open_host(config.config.state_store.clone()).await?;
    let server_result = serve(config, shutdown).await;
    let shutdown_result = shutdown_host(host).await;

    match (server_result, shutdown_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(server_error), Ok(())) => Err(server_error),
        (Ok(()), Err(shutdown_error)) => {
            Err(format!("frontend host shutdown failed: {shutdown_error}"))
        }
        (Err(server_error), Err(shutdown_error)) => Err(format!(
            "{server_error}; frontend host shutdown failed: {shutdown_error}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::{FrontendServerConfig, run_frontend_server_until_shutdown_with_ports};

    #[derive(Debug)]
    struct RecordingHostPort;

    #[derive(Clone, Debug)]
    struct RecordingServerPort {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl RecordingServerPort {
        fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self { events }
        }

        fn record(&self, event: &'static str) {
            self.events.lock().expect("events lock").push(event);
        }
    }

    fn frontend_config() -> FrontendServerConfig {
        FrontendServerConfig {
            config: novarocks::common::app_config::NovaRocksConfig::default(),
            config_path: None,
            port_override: None,
            local_exchange: false,
        }
    }

    #[tokio::test]
    async fn host_opens_before_server_bind() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let host_port = RecordingServerPort::new(Arc::clone(&events));
        let server_port = RecordingServerPort::new(Arc::clone(&events));

        run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            async {},
            move |_| {
                host_port.record("host_open");
                async { Ok(RecordingHostPort) }
            },
            move |_, shutdown| async move {
                server_port.record("server_bind");
                shutdown.await;
                Ok(())
            },
            |_| async { Ok(()) },
        )
        .await
        .expect("frontend orchestration should succeed");

        assert_eq!(
            events.lock().expect("events lock").as_slice(),
            ["host_open", "server_bind"]
        );
    }

    #[tokio::test]
    async fn normal_shutdown_drains_server_before_store() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let server_port = RecordingServerPort::new(Arc::clone(&events));
        let shutdown_port = RecordingServerPort::new(Arc::clone(&events));

        run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            async {},
            |_| async { Ok(RecordingHostPort) },
            move |_, shutdown| async move {
                server_port.record("server_started");
                shutdown.await;
                server_port.record("server_drained");
                Ok(())
            },
            move |_| async move {
                shutdown_port.record("store_shutdown");
                Ok(())
            },
        )
        .await
        .expect("frontend orchestration should succeed");

        assert_eq!(
            events.lock().expect("events lock").as_slice(),
            ["server_started", "server_drained", "store_shutdown"]
        );
    }

    #[tokio::test]
    async fn startup_failure_still_shuts_host() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let shutdown_port = RecordingServerPort::new(Arc::clone(&events));

        let error = run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            std::future::pending::<()>(),
            |_| async { Ok(RecordingHostPort) },
            |_, _| async { Err("core startup failed".to_string()) },
            move |_| async move {
                shutdown_port.record("store_shutdown");
                Ok(())
            },
        )
        .await
        .expect_err("core startup failure should be returned");

        assert_eq!(error, "core startup failed");
        assert_eq!(
            events.lock().expect("events lock").as_slice(),
            ["store_shutdown"]
        );
    }

    #[tokio::test]
    async fn server_and_shutdown_failure_preserve_server_error() {
        let error = run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            std::future::pending::<()>(),
            |_| async { Ok(RecordingHostPort) },
            |_, _| async { Err("core server failed".to_string()) },
            |_| async { Err("store shutdown failed".to_string()) },
        )
        .await
        .expect_err("both failures should be returned");

        assert_eq!(
            error,
            "core server failed; frontend host shutdown failed: store shutdown failed"
        );
    }

    #[tokio::test]
    async fn preloaded_config_is_not_reread() {
        let temp = tempfile::tempdir().expect("create tempdir");
        let unreadable_config_path: PathBuf = temp.path().join("missing.toml");
        let mut config = frontend_config();
        config.config.log_level = "sentinel-preloaded".to_string();
        config.config_path = Some(unreadable_config_path.clone());
        let server_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server_called_in_port = Arc::clone(&server_called);

        run_frontend_server_until_shutdown_with_ports(
            config,
            async {},
            |_| async { Ok(RecordingHostPort) },
            move |config, shutdown| async move {
                assert_eq!(config.config.log_level, "sentinel-preloaded");
                assert_eq!(config.config_path, Some(unreadable_config_path));
                server_called_in_port.store(true, std::sync::atomic::Ordering::SeqCst);
                shutdown.await;
                Ok(())
            },
            |_| async { Ok(()) },
        )
        .await
        .expect("preloaded config should reach the core port without a disk read");

        assert!(server_called.load(std::sync::atomic::Ordering::SeqCst));
    }
}
