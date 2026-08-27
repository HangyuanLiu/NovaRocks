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

use std::env;
use std::process;

use novarocks_server::app_config::NovaRocksConfig;
use novarocks_server::{composition, launch, logging};

fn usage() {
    eprintln!("Usage:");
    eprintln!("  novarocks standalone --role fe --config <fe.toml>");
    eprintln!("  novarocks standalone --role be --config <be.toml>");
    eprintln!(
        "  novarocks standalone --role all-in-one --fe-config <fe.toml> --be-config <be.toml>"
    );
}

fn resolve_log_filter(config: &NovaRocksConfig) -> String {
    config
        .log_filter
        .clone()
        .unwrap_or_else(|| match config.log_level.as_str() {
            "debug" => "info,novarocks=debug".to_string(),
            "trace" => "info,novarocks=trace".to_string(),
            other => other.to_string(),
        })
}

fn init_process(config: &NovaRocksConfig) -> anyhow::Result<tokio::runtime::Runtime> {
    logging::init_with_level(
        &resolve_log_filter(config),
        &logging::LogFileSettings {
            dir: config.sys_log_dir.clone(),
            roll_mode: config.sys_log_roll_mode.clone(),
            roll_num: config.sys_log_roll_num,
        },
    );
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.runtime.actual_data_runtime_threads().max(1))
        .max_blocking_threads(config.runtime.data_runtime_max_blocking_threads.max(1))
        .thread_name("novarocks-data-runtime")
        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| anyhow::anyhow!("build data Tokio runtime: {error}"))
}

/// SIGTERM is the production authority for the one-way FE drain. SIGINT uses
/// the same path for local operation; neither signal is interpreted as an
/// immediate process-wide connection cancellation.
async fn termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())
            .expect("install SIGTERM handler for NovaRocks server process");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn run_frontend(role: launch::RoleConfig, runtime: &tokio::runtime::Runtime) -> anyhow::Result<()> {
    let frontend = composition::compose_frontend_server_config(
        &role.config,
        &role.native_trust,
        None,
        runtime.handle().clone(),
    )?;
    runtime
        .block_on(novarocks_frontend::run_frontend_server_until_shutdown(
            frontend,
            runtime.handle().clone(),
            termination_signal(),
        ))
        .map_err(|error| anyhow::anyhow!("role=fe: {error}"))
}

fn run_backend(role: launch::RoleConfig, runtime: &tokio::runtime::Runtime) -> anyhow::Result<()> {
    let backend = composition::compose_backend_server_config(
        &role.config,
        &role.native_trust,
        runtime.handle().clone(),
    )?;
    let data_runtime = novarocks_backend::BackendDataRuntime::new(
        runtime.handle().clone(),
        std::sync::Arc::clone(&backend.native_trust),
        backend.native_transport.clone(),
    );
    runtime
        .block_on(novarocks_backend::run_backend_server_until_shutdown(
            backend,
            data_runtime,
            termination_signal(),
        ))
        .map_err(|error| anyhow::anyhow!("role=be: {error}"))
}

async fn wait_for_stop(mut receiver: tokio::sync::watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

async fn run_all_in_one(
    fe: launch::RoleConfig,
    be: launch::RoleConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let frontend = composition::compose_frontend_server_config(
        &fe.config,
        &fe.native_trust,
        None,
        runtime.clone(),
    )?;
    let backend =
        composition::compose_backend_server_config(&be.config, &be.native_trust, runtime.clone())?;
    let backend_runtime = novarocks_backend::BackendDataRuntime::new(
        runtime.clone(),
        std::sync::Arc::clone(&backend.native_trust),
        backend.native_transport.clone(),
    );
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let frontend_runtime = runtime.clone();
    let frontend_stop = stop_rx.clone();
    let frontend_run = async move {
        novarocks_frontend::run_frontend_server_until_shutdown(
            frontend,
            frontend_runtime,
            wait_for_stop(frontend_stop),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    };
    let backend_run = async move {
        novarocks_backend::run_backend_server_until_shutdown(
            backend,
            backend_runtime,
            wait_for_stop(stop_rx),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    };
    novarocks_server::supervisor::supervise_all_in_one(
        frontend_run,
        backend_run,
        stop_tx,
        termination_signal(),
    )
    .await
}

fn run(args: launch::StandaloneLaunchArgs) -> anyhow::Result<()> {
    let resolved = launch::resolve_server_launch(args)?;
    let process_config = match &resolved {
        launch::ResolvedServerLaunch::Fe(role) | launch::ResolvedServerLaunch::Be(role) => {
            &role.config
        }
        launch::ResolvedServerLaunch::AllInOne { fe, .. } => &fe.config,
    };
    let runtime = init_process(process_config)?;
    match resolved {
        launch::ResolvedServerLaunch::Fe(role) => run_frontend(role, &runtime),
        launch::ResolvedServerLaunch::Be(role) => run_backend(role, &runtime),
        launch::ResolvedServerLaunch::AllInOne { fe, be } => {
            runtime.block_on(run_all_in_one(fe, be, runtime.handle().clone()))
        }
    }
}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_none_or(|command| command == "--help" || command == "-h")
    {
        usage();
        return;
    }
    if args.first().is_none_or(|command| command != "standalone") {
        eprintln!("the only server command is `standalone`");
        usage();
        process::exit(1);
    }
    let parsed = match launch::parse_standalone_launch_args(&args[1..]) {
        Ok(Some(parsed)) => parsed,
        Ok(None) => {
            usage();
            return;
        }
        Err(error) => {
            eprintln!("{error}");
            usage();
            process::exit(1);
        }
    };
    if let Err(error) = run(parsed) {
        eprintln!("{error:#}");
        process::exit(1);
    }
}
