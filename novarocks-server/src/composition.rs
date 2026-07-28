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
use std::time::Duration;

use anyhow::Context;
use novarocks::common::app_config::NovaRocksConfig;
use novarocks_backend::{BackendApplicationHost, BackendServerConfig};
use novarocks_frontend::{FrontendGrpcEndpointOwnership, FrontendServerConfig};

const BACKEND_SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub fn run_all_in_one(
    config: NovaRocksConfig,
    config_path: Option<PathBuf>,
    port_override: Option<u16>,
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .context("build all-in-one Tokio runtime")?;

    runtime.block_on(run_all_in_one_until(
        config,
        config_path,
        port_override,
        async {
            tokio::signal::ctrl_c()
                .await
                .map_err(|error| format!("Ctrl-C listener failed: {error}"))
        },
    ))
}

async fn run_all_in_one_until<F>(
    config: NovaRocksConfig,
    config_path: Option<PathBuf>,
    port_override: Option<u16>,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = Result<(), String>> + Send,
{
    let mut backend = BackendApplicationHost::open(BackendServerConfig {
        config: config.clone(),
    })
    .map_err(|error| anyhow::anyhow!("open all-in-one backend application failed: {error}"))?;

    let (frontend_shutdown_tx, frontend_shutdown_rx) = tokio::sync::oneshot::channel();
    let frontend = novarocks_frontend::run_frontend_server_until_shutdown(
        FrontendServerConfig {
            config,
            config_path,
            port_override,
            grpc_endpoint: FrontendGrpcEndpointOwnership::ExternallyHosted,
        },
        async move {
            let _ = frontend_shutdown_rx.await;
        },
    );
    tokio::pin!(frontend);
    tokio::pin!(shutdown);

    let mut frontend_completed = false;
    let primary = loop {
        tokio::select! {
            frontend_result = &mut frontend => {
                frontend_completed = true;
                break frontend_result.map_err(|error| error.to_string());
            }
            shutdown_result = &mut shutdown => break shutdown_result,
            _ = tokio::time::sleep(BACKEND_SUPERVISION_POLL_INTERVAL) => {
                match backend.poll_failure() {
                    Ok(Some(error)) | Err(error) => break Err(error.to_string()),
                    Ok(None) => {}
                }
            }
        }
    };

    let frontend_cleanup = if frontend_completed {
        Ok(())
    } else {
        let _ = frontend_shutdown_tx.send(());
        frontend.await.map_err(|error| error.to_string())
    };
    let backend_cleanup = backend.shutdown().map_err(|error| error.to_string());
    combine_primary_and_cleanup(primary, frontend_cleanup, backend_cleanup)
        .map_err(anyhow::Error::msg)
}

fn combine_primary_and_cleanup(
    primary: Result<(), String>,
    frontend_cleanup: Result<(), String>,
    backend_cleanup: Result<(), String>,
) -> Result<(), String> {
    let cleanup_errors = [frontend_cleanup.err(), backend_cleanup.err()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    match (primary, cleanup_errors.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Ok(()), false) => Err(format!("cleanup failed: {}", cleanup_errors.join("; "))),
        (Err(primary), true) => Err(primary),
        (Err(primary), false) => Err(format!(
            "{primary}; cleanup failed: {}",
            cleanup_errors.join("; ")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::combine_primary_and_cleanup;

    #[test]
    fn backend_failure_remains_primary_when_frontend_and_backend_cleanup_fail() {
        let error = combine_primary_and_cleanup(
            Err("backend failed".to_string()),
            Err("frontend cleanup failed".to_string()),
            Err("backend cleanup failed".to_string()),
        )
        .expect_err("backend failure must be returned");

        assert!(error.contains("backend failed"), "{error}");
        assert!(error.contains("frontend cleanup failed"), "{error}");
        assert!(error.contains("backend cleanup failed"), "{error}");
    }
}
