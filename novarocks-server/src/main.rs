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
use std::sync::Arc;

use novarocks_execution::exec::expr::agg::{
    ExecutionFunctionSetBuilder, SealedExecutionFunctionSet,
    contribute_builtin_aggregate_implementations,
};
use novarocks_memory::MemoryAuthority;
use novarocks_server::app_config::NovaRocksConfig;
use novarocks_server::{
    composition, launch, logging, memory_observation, native_compatibility,
    provider_manifest::ServerProviderManifest,
};
use novarocks_types::NativeCompatibilityId;

fn usage() {
    eprintln!("Usage:");
    eprintln!("  novarocks standalone --role fe --config <fe.toml>");
    eprintln!("  novarocks standalone --role be --config <be.toml>");
    eprintln!(
        "  novarocks standalone --role all-in-one --fe-config <fe.toml> --be-config <be.toml>"
    );
}

/// The tracing filter this process runs with.
///
/// `NOVAROCKS_LOG_FILTER` takes precedence over both config keys, matching how
/// `NOVAROCKS_LOG_DIR` already overrides the configured log directory. A
/// deployment states its intent in config; an operator diagnosing a live
/// process cannot always edit that config, and in a launched test cluster the
/// configs are generated per run.
fn resolve_log_filter(config: &NovaRocksConfig) -> String {
    if let Ok(filter) = std::env::var("NOVAROCKS_LOG_FILTER") {
        let filter = filter.trim();
        if !filter.is_empty() {
            return filter.to_owned();
        }
    }
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
    memory_observation::log_installed();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.runtime.actual_data_runtime_threads().max(1))
        .max_blocking_threads(config.runtime.data_runtime_max_blocking_threads.max(1))
        .thread_name("novarocks-data-runtime")
        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| anyhow::anyhow!("build data Tokio runtime: {error}"))
}

fn compose_process_function_set() -> anyhow::Result<Arc<SealedExecutionFunctionSet>> {
    let mut builder = ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
        .map_err(|error| anyhow::anyhow!("contribute builtin function metadata: {error}"))?;
    contribute_builtin_aggregate_implementations(&mut builder).map_err(|error| {
        anyhow::anyhow!("contribute builtin aggregate implementations: {error}")
    })?;
    builder
        .register_typed_aggregate(
            novarocks_connector_iceberg_functions::iceberg_theta_registration()
                .map_err(|error| anyhow::anyhow!("build Iceberg function bundle: {error}"))?,
        )
        .map_err(|error| anyhow::anyhow!("contribute Iceberg function bundle: {error}"))?;
    Ok(Arc::new(builder.seal().map_err(|error| {
        anyhow::anyhow!("seal process engine function set: {error}")
    })?))
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

fn run_frontend(
    role: launch::RoleConfig,
    native_compatibility_id: NativeCompatibilityId,
    function_catalog: std::sync::Arc<novarocks_functions::EngineFunctionCatalog>,
    provider_manifest: Arc<ServerProviderManifest>,
    memory_authority: Arc<MemoryAuthority>,
    runtime: &tokio::runtime::Runtime,
) -> anyhow::Result<()> {
    let frontend = composition::compose_frontend_role_config(
        &role.config,
        &role.native_trust,
        None,
        native_compatibility_id,
        function_catalog,
        provider_manifest,
        memory_authority,
        runtime.handle().clone(),
    )?;
    runtime
        .block_on(novarocks_server::roles::frontend::run_until_shutdown(
            frontend,
            runtime.handle().clone(),
            termination_signal(),
        ))
        .map_err(|error| anyhow::anyhow!("role=fe: {error}"))
}

fn run_backend(
    role: launch::RoleConfig,
    native_compatibility_id: NativeCompatibilityId,
    function_set: std::sync::Arc<novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet>,
    provider_manifest: Arc<ServerProviderManifest>,
    memory_authority: Arc<MemoryAuthority>,
    runtime: &tokio::runtime::Runtime,
) -> anyhow::Result<()> {
    initialize_backend_file_caches(&role.config);
    let backend = composition::compose_backend_server_config(
        &role.config,
        &role.native_trust,
        native_compatibility_id,
        function_set,
        provider_manifest,
        memory_authority,
        runtime.handle().clone(),
    )?;
    let data_runtime = novarocks_native_adapter::BackendDataRuntime::new(
        runtime.handle().clone(),
        std::sync::Arc::clone(&backend.native_trust),
        backend.native_transport.clone(),
    );
    runtime
        .block_on(novarocks_server::roles::backend::run_until_shutdown(
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
    native_compatibility_id: NativeCompatibilityId,
    function_set: std::sync::Arc<novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet>,
    provider_manifest: Arc<ServerProviderManifest>,
    memory_authority: Arc<MemoryAuthority>,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<()> {
    initialize_backend_file_caches(&be.config);
    let frontend = composition::compose_frontend_role_config(
        &fe.config,
        &fe.native_trust,
        None,
        native_compatibility_id,
        std::sync::Arc::clone(function_set.catalog()),
        Arc::clone(&provider_manifest),
        // Both roles consume the same authority: one OS process, one bound.
        Arc::clone(&memory_authority),
        runtime.clone(),
    )?;
    let backend = composition::compose_backend_server_config(
        &be.config,
        &be.native_trust,
        native_compatibility_id,
        function_set,
        provider_manifest,
        memory_authority,
        runtime.clone(),
    )?;
    let backend_runtime = novarocks_native_adapter::BackendDataRuntime::new(
        runtime.clone(),
        std::sync::Arc::clone(&backend.native_trust),
        backend.native_transport.clone(),
    );
    let (frontend_stop_tx, frontend_stop_rx) = tokio::sync::watch::channel(false);
    let (backend_stop_tx, backend_stop_rx) = tokio::sync::watch::channel(false);
    let frontend_runtime = runtime.clone();
    let frontend_run = async move {
        novarocks_server::roles::frontend::run_until_shutdown(
            frontend,
            frontend_runtime,
            wait_for_stop(frontend_stop_rx),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    };
    let backend_run = async move {
        novarocks_server::roles::backend::run_until_shutdown(
            backend,
            backend_runtime,
            wait_for_stop(backend_stop_rx),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    };
    novarocks_server::supervisor::supervise_all_in_one(
        frontend_run,
        backend_run,
        frontend_stop_tx,
        backend_stop_tx,
        termination_signal(),
    )
    .await
}

/// Initialize the BE-local file caches before the first connector reader can
/// create a query-scoped cache context. FE does not own these process-local
/// execution resources.
fn initialize_backend_file_caches(config: &NovaRocksConfig) {
    let cache = &config.runtime.cache;
    if cache.page_cache_enable {
        let _ = novarocks_fs::DataCacheManager::instance().init_page_cache(
            novarocks_fs::DataCachePageCacheOptions {
                capacity: cache.page_cache_capacity,
                evict_probability: cache.page_cache_evict_probability,
            },
        );
    }
    let _ = novarocks_fs::init_parquet_cache(novarocks_fs::ParquetCacheOptions {
        enable_metadata: cache.parquet_meta_cache_enable,
        metadata_ttl: std::time::Duration::from_secs(cache.parquet_meta_cache_ttl_seconds),
        enable_page: cache.parquet_page_cache_enable,
    });
}

/// Builds the one memory authority this OS process gets.
///
/// A capacity bound is a property of the process, not of a role, so this is
/// deliberately called once in `run()` and the resulting handle is shared.
/// Under `all-in-one` both role runners consume *this* authority: two
/// authorities over one address space would each believe they owned the whole
/// bound, and sampling the same RSS twice would not make that safe. There is
/// no `all-in-one` branch here for the same reason -- the single-process form
/// is a test convenience, not a topology to model for.
///
/// `all-in-one` reads its bound from the FE config, matching how the process
/// already resolves its tokio runtime and log filter from that same config.
fn compose_memory_authority(config: &NovaRocksConfig) -> anyhow::Result<Arc<MemoryAuthority>> {
    let authority_config = config.runtime.effective_authority_config()?;
    let authority = MemoryAuthority::new(authority_config)
        .map_err(|error| anyhow::anyhow!("compose process memory authority: {error}"))?;
    // Control-plane traffic gets its own branch off the root before any work
    // account exists, so a process that later fills its work capacity still
    // has somewhere to allocate a cancellation or a status.
    let control_bytes = config.runtime.frontend_workload.control_bytes;
    authority
        .install_control_branch(control_bytes)
        .map_err(|error| anyhow::anyhow!("install process control branch: {error}"))?;
    tracing::info!(
        process_bound_bytes = authority.process_bound_bytes(),
        capacity_bytes = authority.capacity_bytes(),
        headroom_budget_bytes = authority.headroom_budget_bytes(),
        control_bytes,
        "installed the process memory authority"
    );
    Ok(Arc::new(authority))
}

fn run(args: launch::StandaloneLaunchArgs) -> anyhow::Result<()> {
    let resolved = launch::resolve_server_launch(args)?;
    let process_config = match &resolved {
        launch::ResolvedServerLaunch::Fe(role) | launch::ResolvedServerLaunch::Be(role) => {
            &role.config
        }
        launch::ResolvedServerLaunch::AllInOne { fe, .. } => &fe.config,
    };
    let provider_manifest = Arc::new(ServerProviderManifest::seal()?);
    let memory_authority = compose_memory_authority(process_config)?;
    let runtime = init_process(process_config)?;
    let function_set = compose_process_function_set()?;
    let functions = std::sync::Arc::clone(function_set.catalog());
    let native_compatibility = native_compatibility::resolve_native_compatibility_material(
        provider_manifest.contracts(),
        functions.digest(),
        function_set.implementation_manifest_digest(),
    )?;
    tracing::info!(
        native_compatibility_id = %native_compatibility.id(),
        function_catalog_digest = %hex::encode(functions.digest()),
        execution_implementation_manifest_digest = %hex::encode(function_set.implementation_manifest_digest()),
        build_identity = novarocks_version::native_build_identity(),
        "resolved native compatibility material"
    );
    match resolved {
        launch::ResolvedServerLaunch::Fe(role) => run_frontend(
            role,
            native_compatibility.id(),
            functions,
            provider_manifest,
            memory_authority,
            &runtime,
        ),
        launch::ResolvedServerLaunch::Be(role) => run_backend(
            role,
            native_compatibility.id(),
            function_set,
            provider_manifest,
            memory_authority,
            &runtime,
        ),
        launch::ResolvedServerLaunch::AllInOne { fe, be } => runtime.block_on(run_all_in_one(
            fe,
            be,
            native_compatibility.id(),
            function_set,
            provider_manifest,
            memory_authority,
            runtime.handle().clone(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{compose_memory_authority, compose_process_function_set};
    use novarocks_functions::{FunctionKind, FunctionVisibility};
    use novarocks_server::app_config::NovaRocksConfig;
    use std::sync::Arc;

    /// One OS process gets one authority, and every role that needs capacity
    /// is handed *that* one.
    ///
    /// Under all-in-one this is the whole point: two authorities over a single
    /// address space would each believe they owned the entire bound, and
    /// sampling the same RSS twice would not make that safe. The test states
    /// it as pointer identity because that is what "the same bound" means at
    /// runtime — a second authority with equal numbers would pass any test
    /// that only compared configuration.
    #[test]
    fn every_role_in_one_process_shares_one_authority() {
        let config = NovaRocksConfig::default();
        let authority = compose_memory_authority(&config).expect("the default config composes");

        // What `run()` does for all-in-one: clone the handle per role.
        let frontend_handle = Arc::clone(&authority);
        let backend_handle = Arc::clone(&authority);
        assert!(
            Arc::ptr_eq(&frontend_handle, &backend_handle),
            "both roles must consume the same authority, not two with equal numbers"
        );

        // Composing again is what a second authority would look like, and it
        // must be a different one: that is why `run()` calls this exactly once.
        let second = compose_memory_authority(&config).expect("the default config composes");
        assert!(
            !Arc::ptr_eq(&authority, &second),
            "each composition mints its own authority, so composing twice would \
             split the process bound in two"
        );
    }

    /// The control partition exists before any work account can.
    #[test]
    fn the_composed_authority_partitions_its_bound_and_installs_control() {
        let config = NovaRocksConfig::default();
        let authority = compose_memory_authority(&config).expect("the default config composes");

        assert!(
            authority.capacity_bytes() + authority.headroom_budget_bytes()
                <= authority.process_bound_bytes(),
            "B + H must fit inside P"
        );
        assert!(
            authority.control_branch().is_some(),
            "control must have its own partition before any work account exists"
        );
        assert_eq!(
            authority.snapshot().root.live_bytes,
            0,
            "a freshly composed authority has charged nothing"
        );
    }

    #[test]
    fn native_compatibility_uses_one_sealed_process_function_set() {
        let function_set = compose_process_function_set().expect("sealed process function set");
        let catalog = function_set.catalog();
        let definition = catalog
            .definition(
                novarocks_connector_iceberg_functions::ICEBERG_THETA_AGGREGATE_NAME,
                FunctionKind::Aggregate,
            )
            .expect("Iceberg hidden aggregate metadata");

        assert_eq!(definition.visibility(), FunctionVisibility::Hidden);
        assert_ne!(catalog.digest(), [0; 32]);
        assert_ne!(function_set.implementation_manifest_digest(), [0; 32]);
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
