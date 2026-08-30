use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::loopback_s3::{
    LoopbackS3Config, LoopbackS3Fixture, LoopbackS3Object, LoopbackS3Request,
};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, QueryExecutionResourceSnapshot,
    ServerHandle,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const CONNECTOR_READER_OPEN: &str = "NOVAROCKS_CONNECTOR_UNIT_READER_OPEN";
const TYPED_SPLIT_ACCEPTED: &str = "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED";
const TYPED_SPLIT_NO_MORE: &str = "NOVAROCKS_TASK_SPLIT_NO_MORE";
const TYPED_PAGE_SOURCE_OPEN: &str = "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN";
const TYPED_PAGE_SOURCE_CLOSE: &str = "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE";
const CONNECTOR_READER_CLOSE: &str = "NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE";
const READER_CACHE_OVERLAY: &str = r#"
[runtime.cache]
page_cache_enable = true
# This fixture writes several real Parquet files. Capacity is bytes at the
# filesystem boundary, so retain enough ranges to make a repeated typed read
# a meaningful same-process cache hit check.
page_cache_capacity = 67108864
page_cache_evict_probability = 100
parquet_meta_cache_enable = true
parquet_meta_cache_ttl_seconds = 3600
parquet_page_cache_enable = true
"#;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(DistributedReaderCancel),
        Box::new(DistributedReaderKillConnection),
        Box::new(CatalogFeRestartCache),
        Box::new(CatalogReadyLifecycle),
        Box::new(CatalogReadWriteRuntime),
        Box::new(CatalogVersionDrain),
        Box::new(GenerationReplacement),
        Box::new(StaticCredentialGeneration::default()),
        Box::new(AccessDomainCacheIsolation::default()),
        Box::new(PredicatePageIndexPruning),
        Box::new(TypedReadData),
    ]
}

/// Proves a typed connector read works on the real 1FE+3BE topology, and that
/// its splits are delivered at runtime rather than frozen into the plan.
///
/// A correct result alone would not show that: a single backend reading every
/// file would produce exactly the same rows. The evidence that distinguishes
/// the two is which processes accepted split assignments.
struct TypedReadData;

impl Scenario for TypedReadData {
    fn name(&self) -> &'static str {
        "connector/iceberg-typed-read-data"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect typed read control session")?,
        )?;

        const CATALOG: &str = "typed_read_catalog";
        const DATABASE: &str = "typed_read_db";
        const TABLE: &str = "typed_read_data";
        let warehouse = create_warehouse(context, "iceberg-typed-read-data")?;

        // Three files, three backends: fewer splits than backends could not
        // show distribution even if it worked.
        context.action("create three independent Iceberg data files");
        create_catalog_table_and_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

        let counted_query = format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}");
        context.action("warm the enabled role-local cache through a typed connector read");
        let warm_profile: Vec<String> =
            control
                .query(format!("EXPLAIN ANALYZE {counted_query}"))
                .context("collect cache-warming typed connector EXPLAIN ANALYZE profile")?;
        let warm_profile = warm_profile.join("\n");
        assert_positive_profile_counter(&warm_profile, "ConnectorFileCacheMisses")?;

        context.action("repeat the same typed connector read from the role-local cache");
        let cached_profile: Vec<String> = control
            .query(format!("EXPLAIN ANALYZE {counted_query}"))
            .context("collect cached typed connector EXPLAIN ANALYZE profile")?;
        let cached_profile = cached_profile.join("\n");
        assert_positive_profile_counter(&cached_profile, "ConnectorFileCacheHits")?;

        context.action("read every row through the typed connector stack");
        let counted: Vec<i64> = control
            .query(&counted_query)
            .context("count rows through the typed connector read")?;
        if counted != [300_000] {
            bail!("typed connector read returned {counted:?} rows, expected [300000]");
        }
        // A count alone can be right while the values are not; the sum pins
        // which rows were read, not just how many.
        let summed: Vec<i64> = control
            .query(format!("SELECT sum(v) FROM {CATALOG}.{DATABASE}.{TABLE}"))
            .context("sum values through the typed connector read")?;
        if summed != [45_000_150_000] {
            bail!("typed connector read summed {summed:?}, expected [45000150000]");
        }

        context.action("assert splits reached more than one backend process");
        let logs = wait_for_backend_logs(context, "observe typed split assignments", |logs| {
            logs.iter()
                .filter(|log| log.contains(TYPED_SPLIT_ACCEPTED))
                .count()
                >= 2
        })?;
        assert_typed_split_evidence(&logs)?;

        await_resource_convergence(context, &baseline, "typed connector read")?;
        Ok(())
    }
}

/// Every backend that accepted a split must have opened a page source for it
/// and closed it, and must have been told the assignment is terminal.
///
/// Counting opens against closes is what separates "read and released" from
/// "read and leaked"; counting accepted backends is what separates a
/// distributed read from one backend doing all of it.
fn assert_typed_split_evidence(logs: &[String]) -> Result<()> {
    let mut accepting_backends = 0_usize;
    for (index, log) in logs.iter().enumerate() {
        let accepted = log.matches(TYPED_SPLIT_ACCEPTED).count();
        if accepted == 0 {
            continue;
        }
        accepting_backends += 1;
        if !log.contains(TYPED_SPLIT_NO_MORE) {
            bail!(
                "BE[{index}] accepted {accepted} split assignments but was never told the \
                 assignment is terminal, so its scan could still be waiting"
            );
        }
        let opens = log.matches(TYPED_PAGE_SOURCE_OPEN).count();
        let closes = log.matches(TYPED_PAGE_SOURCE_CLOSE).count();
        if opens == 0 {
            bail!(
                "BE[{index}] accepted {accepted} split assignments and opened no page source: \
                 the splits arrived and were never read"
            );
        }
        if opens != closes {
            bail!("BE[{index}] opened {opens} page sources and closed {closes}");
        }
    }
    if accepting_backends < 2 {
        bail!(
            "only {accepting_backends} backend accepted a split assignment; a read served by one \
             backend cannot show that assignment is distributed at runtime"
        );
    }
    Ok(())
}

struct DistributedReaderCancel;

impl Scenario for DistributedReaderCancel {
    fn name(&self) -> &'static str {
        "connector/distributed-reader-cancel"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect connector cancellation control session")?,
        )?;

        let warehouse = create_warehouse(context, "distributed-reader-cancel")?;
        context.action("create Hadoop Iceberg catalog and three independent data files");
        create_catalog_table_and_data(
            &mut control,
            "connector_cancel_catalog",
            "connector_cancel_db",
            "connector_cancel_data",
            &warehouse,
        )?;

        context.action("start a public-MySQL distributed read that retains connector readers");
        let target = start_connector_read(
            &user,
            port,
            "connector_cancel_catalog",
            "connector_cancel_db",
            "connector_cancel_data",
        )?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive connector read connection id")?)
            .context("connector read terminated before publishing its connection id")?;

        wait_for_in_flight_reader_on_every_backend(
            context,
            "connector_cancel_catalog",
            "wait for every BE to open a distributed connector reader",
        )?;
        if let Ok(result) = target.done.try_recv() {
            bail!("connector read completed before cancellation was issued: {result:?}");
        }

        context.action(format!(
            "cancel connector read through KILL QUERY {connection_id}"
        ));
        control
            .query_drop(format!("KILL QUERY {connection_id}"))
            .context("issue public MySQL KILL QUERY for connector read")?;
        assert_cancelled_query(
            &target.done,
            context.remaining("await connector read cancellation")?,
        )?;
        assert_target_connection_remains_usable(
            &target,
            context.remaining("verify KILL QUERY target connection remains usable")?,
        )?;
        assert_idle_query(&mut control, connection_id)?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("connector read thread panicked"))??;

        let reader_logs = wait_for_balanced_reader_lifecycle(
            context,
            "wait for connector reader close after cancellation",
        )?;
        assert_no_reader_open_after_abort(&reader_logs)?;
        await_resource_convergence(context, &baseline, "cancelled connector read")?;

        context.action("verify a subsequent distributed query succeeds after connector cleanup");
        let rows: Vec<i64> = control
            .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
            .context("run post-cancellation distributed query")?;
        if rows != [1, 2] {
            bail!("post-cancellation distributed query returned {rows:?}, expected [1, 2]");
        }
        Ok(())
    }
}

struct DistributedReaderKillConnection;

impl Scenario for DistributedReaderKillConnection {
    fn name(&self) -> &'static str {
        "connector/distributed-reader-kill-connection"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect connector KILL CONNECTION control session")?,
        )?;

        let warehouse = create_warehouse(context, "distributed-reader-kill-connection")?;
        context.action("create Hadoop Iceberg catalog and three independent data files");
        create_catalog_table_and_data(
            &mut control,
            "connector_kill_connection_catalog",
            "connector_kill_connection_db",
            "connector_kill_connection_data",
            &warehouse,
        )?;

        context.action("start a public-MySQL distributed read that retains connector readers");
        let target = start_connector_read(
            &user,
            port,
            "connector_kill_connection_catalog",
            "connector_kill_connection_db",
            "connector_kill_connection_data",
        )?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive KILL CONNECTION target id")?)
            .context("KILL CONNECTION target terminated before publishing its connection id")?;
        wait_for_in_flight_reader_on_every_backend(
            context,
            "connector_kill_connection_catalog",
            "wait for every BE to open a KILL CONNECTION target reader",
        )?;

        context.action(format!(
            "terminate the active public-MySQL reader through KILL CONNECTION {connection_id}"
        ));
        control
            .query_drop(format!("KILL CONNECTION {connection_id}"))
            .context("issue public MySQL KILL CONNECTION for connector read")?;
        assert_connection_killed_query(
            &target.done,
            context.remaining("await KILL CONNECTION target query termination")?,
        )?;
        assert_target_connection_is_closed(
            &target,
            context.remaining("verify KILL CONNECTION closes the target socket")?,
        )?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("KILL CONNECTION target thread panicked"))??;

        let reader_logs = wait_for_balanced_reader_lifecycle(
            context,
            "wait for connector reader close after KILL CONNECTION",
        )?;
        assert_no_reader_open_after_abort(&reader_logs)?;
        await_resource_convergence(context, &baseline, "KILL CONNECTION connector read")?;

        context.action("verify bare KILL closes an idle public-MySQL target socket");
        let idle_target = start_idle_mysql_connection(&user, port)?;
        let idle_connection_id = idle_target
            .ready
            .recv_timeout(context.remaining("receive bare KILL target id")?)
            .context("bare KILL target terminated before publishing its connection id")?;
        control
            .query_drop(format!("KILL {idle_connection_id}"))
            .context("issue bare public MySQL KILL for idle target")?;
        assert_idle_target_connection_is_closed(
            &idle_target,
            context.remaining("verify bare KILL closes the idle target socket")?,
        )?;
        idle_target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("bare KILL target thread panicked"))??;

        context.action("verify the KILL requester remains usable after both target terminations");
        let rows: Vec<i64> = control
            .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
            .context("run requester query after KILL CONNECTION and bare KILL")?;
        if rows != [1, 2] {
            bail!("post-KILL requester query returned {rows:?}, expected [1, 2]");
        }
        Ok(())
    }
}

struct CatalogVersionDrain;

/// Drives one CatalogSet through its observable cold and warm lifecycle on
/// the real control stream, including cancellation while installation is held.
struct CatalogReadyLifecycle;

impl Scenario for CatalogReadyLifecycle {
    fn name(&self) -> &'static str {
        "connector/catalog-ready-lifecycle"
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut config = connector_launch_config();
        let hold_file = scenario_root.join("catalog-install-hold");
        config.child_environment.be.insert(
            "NOVAROCKS_SQL_TEST_CATALOG_INSTALL_HOLD_FILE".to_string(),
            hold_file.to_string_lossy().into_owned(),
        );
        config.child_environment.be.insert(
            "NOVAROCKS_SQL_TEST_EMIT_CATALOG_LIFECYCLE_MARKER".to_string(),
            "1".to_string(),
        );
        config
            .child_environment
            .be_by_index
            .entry(1)
            .or_default()
            .insert(
                "NOVAROCKS_SQL_TEST_CATALOG_INSTALL_FAILURE_FILE".to_string(),
                scenario_root
                    .join("catalog-install-failure")
                    .to_string_lossy()
                    .into_owned(),
            );
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect catalog ready lifecycle control session")?,
        )?;

        const CATALOG: &str = "catalog_ready_lifecycle";
        const DATABASE: &str = "catalog_ready_db";
        const TABLE: &str = "catalog_ready_data";
        let warehouse = create_warehouse(context, "catalog-ready-lifecycle")?;
        create_catalog_table_and_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

        control
            .query_drop(format!("DROP CATALOG {CATALOG}"))
            .context("replace warm catalog before held cancellation")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        let hold_file = context.scenario_root().join("catalog-install-hold");
        let before_cancel = backend_log_snapshots(context)?;
        std::fs::write(&hold_file, "hold\n")
            .with_context(|| format!("create catalog-install hold file {}", hold_file.display()))?;
        context.action("hold cold catalog install, then cancel before CatalogReady");
        let cancelled = start_connector_read(&user, port, CATALOG, DATABASE, TABLE)?;
        let connection_id = cancelled
            .ready
            .recv_timeout(context.remaining("receive held catalog query connection id")?)
            .context("held catalog query terminated before publishing connection id")?;
        wait_for_catalog_lifecycle_marker(
            context,
            &before_cancel,
            "NOVAROCKS_CATALOG_LOADING",
            "observe Loading on every Backend before cancellation",
        )?;
        assert_no_appended_catalog_stage_admitted(context, &before_cancel)?;
        control
            .query_drop(format!("KILL QUERY {connection_id}"))
            .context("cancel query while catalog install is held")?;
        assert_cancelled_query(
            &cancelled.done,
            context.remaining("await held catalog query cancellation")?,
        )?;
        assert_target_connection_remains_usable(
            &cancelled,
            context.remaining("verify KILL QUERY preserves held client connection")?,
        )?;
        release_catalog_install_hold(&hold_file)?;
        release_connector_read(&cancelled)?;
        cancelled
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("held catalog reader thread panicked"))??;
        await_resource_convergence(context, &baseline, "cancelled catalog install")?;
        assert_no_appended_catalog_ready(context, &before_cancel)?;

        control
            .query_drop(format!("DROP CATALOG {CATALOG}"))
            .context("replace cancelled catalog before injected install failure")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        let failure_file = context.scenario_root().join("catalog-install-failure");
        let before_failure = backend_log_snapshots(context)?;
        std::fs::write(&failure_file, "fail\n").with_context(|| {
            format!(
                "create catalog-install failure trigger {}",
                failure_file.display()
            )
        })?;
        context
            .action("fail catalog installation on one Backend and reject the query before Stage");
        let failed_query: Result<Vec<i64>, mysql::Error> =
            control.query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"));
        if let Ok(rows) = failed_query {
            bail!("catalog install failure query unexpectedly succeeded: {rows:?}");
        }
        wait_for_catalog_lifecycle_marker_on_backend(
            context,
            &before_failure,
            1,
            "NOVAROCKS_CATALOG_FAILED",
            "observe the injected CatalogLoadFailed from BE[1]",
        )?;
        assert_no_appended_catalog_stage_admitted(context, &before_failure)?;
        std::fs::remove_file(&failure_file).with_context(|| {
            format!(
                "remove catalog-install failure trigger {}",
                failure_file.display()
            )
        })?;
        let before_retry = backend_log_snapshots(context)?;
        context.action("retry the failed catalog version after clearing the one-Backend failure");
        let rows: Vec<i64> = control
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
            .context("retry catalog query after clearing injected install failure")?;
        if rows != [300_000] {
            bail!("catalog install retry returned {rows:?}, expected [300000]");
        }
        wait_for_catalog_lifecycle_marker_on_backend(
            context,
            &before_retry,
            1,
            "NOVAROCKS_CATALOG_READY",
            "observe retry CatalogReady from the formerly failing Backend",
        )?;
        wait_for_catalog_lifecycle_marker(
            context,
            &before_retry,
            "NOVAROCKS_CATALOG_STAGE_ADMITTED",
            "observe retry Stage after the failed catalog version becomes Ready",
        )?;

        control
            .query_drop(format!("DROP CATALOG {CATALOG}"))
            .context("replace retried catalog before cold ready path")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        let before_cold = backend_log_snapshots(context)?;
        std::fs::write(&hold_file, "hold\n").with_context(|| {
            format!("recreate catalog-install hold file {}", hold_file.display())
        })?;
        context.action("hold cold catalog install until all Backends report Loading");
        let cold = start_connector_read(&user, port, CATALOG, DATABASE, TABLE)?;
        let cold_connection_id = cold
            .ready
            .recv_timeout(context.remaining("receive cold catalog query connection id")?)
            .context("cold catalog query terminated before publishing connection id")?;
        wait_for_catalog_lifecycle_marker(
            context,
            &before_cold,
            "NOVAROCKS_CATALOG_LOADING",
            "observe Loading on every Backend before releasing cold install",
        )?;
        assert_no_appended_catalog_stage_admitted(context, &before_cold)?;
        context.action("release cold catalog install and require Ready before Stage");
        release_catalog_install_hold(&hold_file)?;
        wait_for_catalog_lifecycle_marker(
            context,
            &before_cold,
            "NOVAROCKS_CATALOG_READY",
            "observe CatalogReady on every Backend",
        )?;
        wait_for_catalog_lifecycle_marker(
            context,
            &before_cold,
            "NOVAROCKS_CATALOG_STAGE_ADMITTED",
            "observe Stage only after CatalogReady",
        )?;
        wait_for_open_reader_on_every_backend(
            context,
            CATALOG,
            "observe readers after cold CatalogReady",
        )?;
        control
            .query_drop(format!("KILL QUERY {cold_connection_id}"))
            .context("cancel cold catalog reader after Ready")?;
        assert_cancelled_query(
            &cold.done,
            context.remaining("await cold catalog reader cancellation")?,
        )?;
        assert_target_connection_remains_usable(
            &cold,
            context.remaining("verify cold catalog KILL QUERY connection")?,
        )?;
        release_connector_read(&cold)?;
        cold.thread
            .join()
            .map_err(|_| anyhow::anyhow!("cold catalog reader thread panicked"))??;

        let before_warm = backend_log_snapshots(context)?;
        context.action("execute a warm query without another catalog load event");
        let rows: Vec<i64> = control
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
            .context("run warm catalog query")?;
        if rows != [300_000] {
            bail!("warm catalog query returned {rows:?}, expected [300000]");
        }
        let after_warm = backend_log_snapshots(context)?;
        assert_no_new_catalog_lifecycle_markers(&before_warm, &after_warm)?;

        control
            .query_drop(format!("DROP CATALOG {CATALOG}"))
            .context("replace warm catalog before InitAck retry coverage")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        let before_init_retry = backend_log_snapshots(context)?;
        context
            .handle()
            .arm_init_ack_drop(1)
            .context("arm InitAck drop on BE[1] for cold catalog retry")?;
        context.action("retry the exact cold Init after an Applied InitAck is dropped");
        let rows: Vec<i64> = control
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
            .context("execute cold catalog query with dropped InitAck")?;
        context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear catalog InitAck drop fault")?;
        if rows != [300_000] {
            bail!("InitAck retry catalog query returned {rows:?}, expected [300000]");
        }
        let after_init_retry = backend_log_snapshots(context)?;
        assert_exactly_one_new_catalog_marker_per_backend(
            &before_init_retry,
            &after_init_retry,
            "NOVAROCKS_CATALOG_RUNTIME_MATERIALIZED",
        )?;
        await_resource_convergence(context, &baseline, "catalog ready lifecycle")?;
        Ok(())
    }
}

/// Exercises distributed writes and reads through one exact catalog runtime,
/// then proves a replacement Backend can rebuild that runtime from the frozen
/// catalog properties.
struct CatalogReadWriteRuntime;

impl Scenario for CatalogReadWriteRuntime {
    fn name(&self) -> &'static str {
        "connector/catalog-read-write-runtime"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect catalog read-write control session")?,
        )?;

        const CATALOG: &str = "catalog_read_write_runtime";
        const DATABASE: &str = "catalog_read_write_db";
        const TABLE: &str = "catalog_read_write_data";
        let warehouse = create_warehouse(context, "catalog-read-write-runtime")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        control
            .query_drop(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))
            .context("create catalog read-write database")?;
        control
            .query_drop(format!(
                "CREATE TABLE {CATALOG}.{DATABASE}.{TABLE} (v BIGINT)"
            ))
            .context("create catalog read-write table")?;

        context.action("write and read through the catalog runtime on every Backend");
        for range in ["1, 1000", "1001, 2000", "2001, 3000"] {
            control
                .query_drop(format!(
                    "INSERT INTO {CATALOG}.{DATABASE}.{TABLE} SELECT generate_series FROM TABLE(generate_series({range}))"
                ))
                .with_context(|| format!("distributed insert range {range} through catalog writer runtime"))?;
        }
        assert_catalog_read_summary(&mut control, CATALOG, DATABASE, TABLE, 3_000, 4_501_500)?;
        let before_restart_logs = wait_for_open_reader_on_every_backend(
            context,
            CATALOG,
            "observe catalog readers after distributed write",
        )?;
        let before_restart_versions = reader_catalog_versions(&before_restart_logs, CATALOG)?;
        let before_restart_materializations = catalog_materialization_counts(&before_restart_logs);

        context
            .action("replace one Backend and rebuild its catalog runtime from CatalogProperties");
        let original_process = context.handle().backend_process_id(0)?;
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(0, deadline)
            .context("restart BE[0] for catalog runtime reconstruction")?;
        let replacement_process = context.handle().backend_process_id(0)?;
        if replacement_process == original_process {
            bail!("replacement BE[0] retained its previous process identity");
        }

        control
            .query_drop(format!(
                "INSERT INTO {CATALOG}.{DATABASE}.{TABLE} VALUES (1001)"
            ))
            .context("distributed insert after BE replacement")?;
        assert_catalog_read_summary(&mut control, CATALOG, DATABASE, TABLE, 3_001, 4_502_501)?;
        let after_restart_logs = wait_for_backend_logs(
            context,
            "observe rebuilt catalog runtime after BE replacement",
            |logs| {
                let counts = catalog_materialization_counts(logs);
                counts[0] > 0
                    && counts[1] == before_restart_materializations[1]
                    && counts[2] == before_restart_materializations[2]
            },
        )?;
        let after_restart_versions = reader_catalog_versions(&after_restart_logs, CATALOG)?;
        if after_restart_versions != before_restart_versions {
            bail!(
                "BE replacement changed catalog versions: before={before_restart_versions:?}, after={after_restart_versions:?}"
            );
        }

        await_resource_convergence(context, &baseline, "catalog read-write runtime")?;
        Ok(())
    }
}

/// Proves a Frontend restart reconstructs its durable catalog projection
/// without invalidating catalog runtimes retained by the live Backends.
struct CatalogFeRestartCache;

impl Scenario for CatalogFeRestartCache {
    fn name(&self) -> &'static str {
        "connector/catalog-fe-restart-cache"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect FE restart cache control session")?,
        )?;

        const CATALOG: &str = "catalog_fe_restart_cache";
        const DATABASE: &str = "catalog_fe_restart_db";
        const TABLE: &str = "catalog_fe_restart_data";
        let warehouse = create_warehouse(context, "catalog-fe-restart-cache")?;
        create_catalog_table_and_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

        context.action("warm the exact catalog runtime on every Backend");
        let warm_rows: Vec<i64> = control
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
            .context("warm catalog runtime before FE restart")?;
        if warm_rows != [300_000] {
            bail!("warm catalog read returned {warm_rows:?}, expected [300000]");
        }
        let warm_logs = wait_for_open_reader_on_every_backend(
            context,
            CATALOG,
            "observe warm catalog readers on every Backend",
        )?;
        let warm_versions = reader_catalog_versions(&warm_logs, CATALOG)?;
        let materializations_before = catalog_materialization_counts(&warm_logs);

        context.action("start an in-flight query before the Frontend restart");
        let target = start_connector_read(&user, port, CATALOG, DATABASE, TABLE)?;
        target
            .ready
            .recv_timeout(context.remaining("receive in-flight reader connection id")?)
            .context("in-flight reader terminated before FE restart")?;
        wait_for_open_reader_on_every_backend(
            context,
            CATALOG,
            "observe in-flight readers before FE restart",
        )?;

        context.action("restart the Frontend and require its in-flight client query to fail");
        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE while catalog reader is in flight")?;
        assert_connection_killed_query(
            &target.done,
            context.remaining("await in-flight query failure after FE restart")?,
        )?;
        assert_target_connection_is_closed(
            &target,
            context.remaining("verify FE restart closed in-flight client connection")?,
        )?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("FE restart reader thread panicked"))??;

        context.action("read through the restored Frontend catalog projection");
        let mut restored = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect restored FE catalog control session")?,
        )?;
        let restored_rows: Vec<i64> = restored
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
            .context("read catalog after FE restart")?;
        if restored_rows != [300_000] {
            bail!("restored catalog read returned {restored_rows:?}, expected [300000]");
        }
        let restored_logs = wait_for_backend_logs(
            context,
            "observe post-restart readers on every Backend",
            |logs| {
                logs.iter().zip(&warm_logs).all(|(current, previous)| {
                    reader_open_lines(current, CATALOG).count()
                        > reader_open_lines(previous, CATALOG).count()
                })
            },
        )?;
        let restored_versions = reader_catalog_versions_after(&restored_logs, &warm_logs, CATALOG)?;
        if restored_versions != warm_versions {
            bail!(
                "FE restart changed retained catalog versions: before={warm_versions:?}, after={restored_versions:?}"
            );
        }
        let materializations_after = catalog_materialization_counts(&restored_logs);
        if materializations_after != materializations_before {
            bail!(
                "FE restart rematerialized retained catalog runtimes: before={materializations_before:?}, after={materializations_after:?}"
            );
        }

        await_resource_convergence(context, &baseline, "FE restart catalog cache")?;
        Ok(())
    }
}

/// A static-file catalog source with two simultaneously active, role-local
/// object-store credential generations. The two fixtures deliberately accept
/// different key IDs, so a successful read plus their request logs proves that
/// each catalog stayed on its exact declared credential generation.
#[derive(Default)]
struct StaticCredentialGeneration {
    fixtures: Mutex<Option<StaticCredentialFixtures>>,
}

/// Exercises the cache key boundary on the same native topology as static
/// credentials, but is independently selectable by the M1 acceptance gate.
#[derive(Default)]
struct AccessDomainCacheIsolation {
    fixtures: Mutex<Option<StaticCredentialFixtures>>,
}

struct StaticCredentialFixtures {
    blue: LoopbackS3Fixture,
    green: LoopbackS3Fixture,
}

const STATIC_BLUE_CATALOG: &str = "cca_static_blue";
const STATIC_GREEN_CATALOG: &str = "cca_static_green";
const STATIC_BLUE_CREDENTIAL_NAME: &str = "cca-static-blue";
const STATIC_GREEN_CREDENTIAL_NAME: &str = "cca-static-green";
const STATIC_BLUE_CREDENTIAL_GENERATION: &str = "v1";
const STATIC_GREEN_CREDENTIAL_GENERATION: &str = "v2";
const STATIC_BLUE_KEY_ID: &str = "cca-static-blue-key";
const STATIC_GREEN_KEY_ID: &str = "cca-static-green-key";
const STATIC_BLUE_KEY_SECRET: &str = "cca-static-blue-secret";
const STATIC_GREEN_KEY_SECRET: &str = "cca-static-green-secret";
const COLLISION_BLUE_CATALOG: &str = "cca_cache_domain_blue";
const COLLISION_GREEN_CATALOG: &str = "cca_cache_domain_green";
const COLLISION_DATABASE: &str = "cache_domain_db";
const COLLISION_TABLE: &str = "cache_domain_data";
const COLLISION_WAREHOUSE: &str = "s3://cca-cache-domain-collision/warehouse";

impl Scenario for StaticCredentialGeneration {
    fn name(&self) -> &'static str {
        "connector/static-credential-generation"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        static_credential_launch_config(&self.fixtures, scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect static credential control session")?,
        )?;

        run_static_credential_generation(context, &mut control, &self.fixtures)?;
        await_resource_convergence(context, &baseline, "static credential generation reads")?;
        Ok(())
    }
}

impl Scenario for AccessDomainCacheIsolation {
    fn name(&self) -> &'static str {
        "connector/access-domain-cache-isolation"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        access_domain_collision_launch_config(&self.fixtures, scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect access-domain cache control session")?,
        )?;

        run_access_domain_cache_isolation(context, &mut control, &self.fixtures)?;
        await_resource_convergence(context, &baseline, "access-domain cache isolation")?;
        Ok(())
    }
}

fn static_credential_launch_config(
    fixtures: &Mutex<Option<StaticCredentialFixtures>>,
    scenario_root: &Path,
) -> Result<ScenarioLaunchConfig> {
    let blue = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key(STATIC_BLUE_KEY_ID))
        .context("start blue loopback S3 fixture")?;
    let green = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key(STATIC_GREEN_KEY_ID))
        .context("start green loopback S3 fixture")?;
    let snapshot = scenario_root.join("static-credential-catalogs.toml");
    write_static_credential_snapshot(&snapshot, blue.endpoint(), green.endpoint())?;
    let mut fixtures = fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("static credential fixture lock poisoned"))?;
    if fixtures.is_some() {
        bail!("static credential fixtures were initialized more than once");
    }
    *fixtures = Some(StaticCredentialFixtures { blue, green });

    Ok(ScenarioLaunchConfig {
        child_environment: connector_reader_environment(),
        config_overlay: static_credential_launch_overlay(&snapshot),
        ..Default::default()
    })
}

fn access_domain_collision_launch_config(
    fixtures: &Mutex<Option<StaticCredentialFixtures>>,
    scenario_root: &Path,
) -> Result<ScenarioLaunchConfig> {
    let blue = LoopbackS3Fixture::start(collision_loopback_s3_config(STATIC_BLUE_KEY_ID))
        .context("start blue collision loopback S3 fixture")?;
    let green = LoopbackS3Fixture::start(collision_loopback_s3_config(STATIC_GREEN_KEY_ID))
        .context("start green collision loopback S3 fixture")?;
    let snapshot = scenario_root.join("access-domain-collision-catalogs.toml");
    write_access_domain_collision_snapshot(&snapshot, blue.endpoint(), green.endpoint())?;
    let mut fixtures = fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("access-domain collision fixture lock poisoned"))?;
    if fixtures.is_some() {
        bail!("access-domain collision fixtures were initialized more than once");
    }
    *fixtures = Some(StaticCredentialFixtures { blue, green });

    Ok(ScenarioLaunchConfig {
        child_environment: connector_reader_environment(),
        config_overlay: static_credential_launch_overlay(&snapshot),
        ..Default::default()
    })
}

fn collision_loopback_s3_config(access_key_id: &str) -> LoopbackS3Config {
    let mut config = LoopbackS3Config::for_access_key(access_key_id);
    // The source corpus and its same-length canonical copies are both kept
    // only for this bounded collision setup. The default 128-object ceiling
    // cannot retain both graphs after three Iceberg commits.
    config.max_objects = 256;
    config
}

fn run_static_credential_generation(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    fixtures: &Mutex<Option<StaticCredentialFixtures>>,
) -> Result<()> {
    context.action("write three blue and three green S3 Iceberg files through StaticFile catalogs");
    create_static_catalog_table_and_data(
        control,
        STATIC_BLUE_CATALOG,
        "static_blue_db",
        "static_blue_data",
        ["1, 100000", "100001, 200000", "200001, 300000"],
    )?;
    create_static_catalog_table_and_data(
        control,
        STATIC_GREEN_CATALOG,
        "static_green_db",
        "static_green_data",
        ["300001, 400000", "400001, 500000", "500001, 600000"],
    )?;

    context.action("read the blue static credential catalog through every backend");
    let _blue_profile = static_catalog_profile(
        control,
        STATIC_BLUE_CATALOG,
        "static_blue_db",
        "static_blue_data",
    )?;
    assert_static_catalog_sum(
        control,
        STATIC_BLUE_CATALOG,
        "static_blue_db",
        "static_blue_data",
        45_000_150_000,
    )?;
    let blue_logs = wait_for_open_reader_on_every_backend(
        context,
        STATIC_BLUE_CATALOG,
        "observe every BE read the blue static credential generation",
    )?;
    assert_static_reader_opened_on_every_backend(&blue_logs, STATIC_BLUE_CATALOG)?;

    context.action("read the green static credential catalog through every backend");
    let _green_profile = static_catalog_profile(
        control,
        STATIC_GREEN_CATALOG,
        "static_green_db",
        "static_green_data",
    )?;
    assert_static_catalog_sum(
        control,
        STATIC_GREEN_CATALOG,
        "static_green_db",
        "static_green_data",
        135_000_150_000,
    )?;
    let green_logs = wait_for_open_reader_on_every_backend(
        context,
        STATIC_GREEN_CATALOG,
        "observe every BE read the green static credential generation",
    )?;
    assert_static_reader_opened_on_every_backend(&green_logs, STATIC_GREEN_CATALOG)?;

    let fixtures = fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("static credential fixture lock poisoned"))?;
    let fixtures = fixtures
        .as_ref()
        .context("static credential fixtures were not retained through scenario execution")?;
    assert_fixture_used_only_expected_key(
        "blue",
        &fixtures.blue.request_log(),
        STATIC_BLUE_KEY_ID,
    )?;
    assert_fixture_used_only_expected_key(
        "green",
        &fixtures.green.request_log(),
        STATIC_GREEN_KEY_ID,
    )?;
    context.action("proved blue/v1 and green/v2 reads used distinct exact role-local S3 keys");
    Ok(())
}

fn run_access_domain_cache_isolation(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    fixtures: &Mutex<Option<StaticCredentialFixtures>>,
) -> Result<()> {
    context.action("write colliding blue and green Iceberg corpora at one S3 URI");
    create_static_catalog_table_and_data(
        control,
        COLLISION_BLUE_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
        ["100001, 200000", "200001, 300000", "300001, 400000"],
    )?;
    create_static_catalog_table_and_data(
        control,
        COLLISION_GREEN_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
        ["100002, 200001", "200002, 300001", "300002, 400001"],
    )?;

    let fixtures = fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("static credential fixture lock poisoned"))?;
    let fixtures = fixtures
        .as_ref()
        .context("static credential fixtures were not retained through scenario execution")?;
    let canonical_data_paths = canonicalize_collision_corpus(&fixtures.blue, &fixtures.green)?;

    context.action("warm the blue access domain for the colliding S3 files");
    let blue_profile = static_catalog_profile(
        control,
        COLLISION_BLUE_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
    )?;
    assert_positive_profile_counter(&blue_profile, "ConnectorFileCacheMisses")?;
    assert_static_catalog_sum(
        control,
        COLLISION_BLUE_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
        75_000_150_000,
    )?;
    let blue_logs = wait_for_open_reader_on_every_backend(
        context,
        COLLISION_BLUE_CATALOG,
        "observe every BE read the blue collision access domain",
    )?;
    assert_static_reader_opened_on_every_backend(&blue_logs, COLLISION_BLUE_CATALOG)?;

    context.action("repeat the blue collision read from its role-local cache");
    let blue_cached_profile = static_catalog_profile(
        control,
        COLLISION_BLUE_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
    )?;
    assert_positive_profile_counter(&blue_cached_profile, "ConnectorFileCacheHits")?;

    let green_data_reads_before =
        successful_gets_for_paths(&fixtures.green.request_log(), &canonical_data_paths);
    context.action("read the same S3 URI through the green endpoint and access domain");
    let green_profile = static_catalog_profile(
        control,
        COLLISION_GREEN_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
    )?;
    assert_positive_profile_counter(&green_profile, "ConnectorFileCacheMisses")?;
    assert_static_catalog_sum(
        control,
        COLLISION_GREEN_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
        75_000_450_000,
    )?;
    let green_data_reads_after =
        successful_gets_for_paths(&fixtures.green.request_log(), &canonical_data_paths);
    if green_data_reads_after <= green_data_reads_before {
        bail!(
            "green collision endpoint received no canonical Parquet GET after blue cache warm: before={green_data_reads_before}, after={green_data_reads_after}"
        );
    }
    let green_logs = wait_for_open_reader_on_every_backend(
        context,
        COLLISION_GREEN_CATALOG,
        "observe every BE read the green collision access domain",
    )?;
    assert_static_reader_opened_on_every_backend(&green_logs, COLLISION_GREEN_CATALOG)?;
    assert_fixture_used_only_expected_key(
        "blue collision",
        &fixtures.blue.request_log(),
        STATIC_BLUE_KEY_ID,
    )?;
    assert_fixture_used_only_expected_key(
        "green collision",
        &fixtures.green.request_log(),
        STATIC_GREEN_KEY_ID,
    )?;
    context.action(
        "proved a same-URI, same-size, same-mtime cross-endpoint corpus did not reuse blue cache data",
    );
    Ok(())
}

fn canonicalize_collision_corpus(
    blue: &LoopbackS3Fixture,
    green: &LoopbackS3Fixture,
) -> Result<BTreeSet<String>> {
    let blue_objects = blue.object_snapshot_for_test();
    let green_objects = green.object_snapshot_for_test();
    let mappings = collision_data_path_mapping(&blue_objects, &green_objects)?;
    for blue_object in blue_objects
        .iter()
        .filter(|object| object.key.ends_with(".avro"))
    {
        let bytes = rewrite_fixed_width_paths(&blue_object.bytes, &mappings.replacements)?;
        blue.replace_object_for_test(LoopbackS3Object {
            bucket: blue_object.bucket.clone(),
            key: blue_object.key.clone(),
            bytes,
        })?;
    }
    for blue_object in blue_objects
        .iter()
        .filter(|object| object.key.ends_with(".parquet"))
    {
        let target_key = mappings
            .replacements
            .get(&blue_object.key)
            .context("blue collision data object has no green canonical path")?;
        blue.replace_object_for_test(LoopbackS3Object {
            bucket: blue_object.bucket.clone(),
            key: target_key.clone(),
            bytes: blue_object.bytes.clone(),
        })?;
    }
    Ok(mappings.canonical_paths)
}

struct CollisionDataPathMapping {
    replacements: BTreeMap<String, String>,
    canonical_paths: BTreeSet<String>,
}

fn collision_data_path_mapping(
    blue_objects: &[LoopbackS3Object],
    green_objects: &[LoopbackS3Object],
) -> Result<CollisionDataPathMapping> {
    let blue_data = parquet_objects_by_bucket_and_length(blue_objects);
    let green_data = parquet_objects_by_bucket_and_length(green_objects);
    let blue_count = blue_data.values().map(Vec::len).sum::<usize>();
    let green_count = green_data.values().map(Vec::len).sum::<usize>();
    if blue_data.is_empty() || blue_count != green_count {
        bail!(
            "collision corpus must contain equal non-empty blue and green Parquet data files, got {} and {}",
            blue_count,
            green_count
        );
    }
    if blue_data.keys().collect::<Vec<_>>() != green_data.keys().collect::<Vec<_>>() {
        bail!(
            "collision Parquet `(bucket, byte_length)` multisets differ: blue={:?}, green={:?}",
            parquet_length_multiset(&blue_data),
            parquet_length_multiset(&green_data)
        );
    }

    let mut replacements = BTreeMap::new();
    let mut canonical_paths = BTreeSet::new();
    let mut distinct_payloads = 0;
    for ((bucket, byte_length), mut blue_group) in blue_data {
        let mut green_group = green_data
            .get(&(bucket.clone(), byte_length))
            .cloned()
            .expect("validated collision Parquet multiset has green group");
        // UUID suffixes are fixed-width. Sorting within equal-length groups makes
        // the fixture-only rewriting deterministic without changing any bytes.
        blue_group.sort_by_key(|object| &object.key);
        green_group.sort_by_key(|object| &object.key);
        if blue_group.len() != green_group.len() {
            bail!(
                "collision Parquet `(bucket, byte_length)` multiplicity differs for {bucket}/{byte_length}: blue={}, green={}",
                blue_group.len(),
                green_group.len()
            );
        }
        for (blue_object, green_object) in blue_group.into_iter().zip(green_group) {
            if blue_object.key.len() != green_object.key.len() {
                bail!(
                    "collision data file paths must have equal width: {}/{} vs {}/{}",
                    blue_object.bucket,
                    blue_object.key,
                    green_object.bucket,
                    green_object.key
                );
            }
            if blue_object.bytes != green_object.bytes {
                distinct_payloads += 1;
            }
            replacements.insert(blue_object.key.clone(), green_object.key.clone());
            canonical_paths.insert(format!("/{}/{}", green_object.bucket, green_object.key));
        }
    }
    if distinct_payloads == 0 {
        bail!("collision Parquet corpora have no distinct blue and green payloads");
    }
    Ok(CollisionDataPathMapping {
        replacements,
        canonical_paths,
    })
}

fn parquet_objects_by_bucket_and_length(
    objects: &[LoopbackS3Object],
) -> BTreeMap<(String, usize), Vec<&LoopbackS3Object>> {
    let mut groups = BTreeMap::new();
    for object in objects
        .iter()
        .filter(|object| object.key.ends_with(".parquet"))
    {
        groups
            .entry((object.bucket.clone(), object.bytes.len()))
            .or_insert_with(Vec::new)
            .push(object);
    }
    groups
}

fn parquet_length_multiset(
    groups: &BTreeMap<(String, usize), Vec<&LoopbackS3Object>>,
) -> Vec<((String, usize), usize)> {
    groups
        .iter()
        .map(|(group, objects)| (group.clone(), objects.len()))
        .collect()
}

fn successful_gets_for_paths(requests: &[LoopbackS3Request], paths: &BTreeSet<String>) -> usize {
    requests
        .iter()
        .filter(|request| {
            request.method == "GET" && request.status < 400 && paths.contains(&request.path)
        })
        .count()
}

fn rewrite_fixed_width_paths(
    input: &[u8],
    mappings: &std::collections::BTreeMap<String, String>,
) -> Result<Vec<u8>> {
    let mut output = input.to_vec();
    for (source, target) in mappings {
        if source.len() != target.len() {
            bail!("collision path replacement changes byte width: {source} -> {target}");
        }
        let source = source.as_bytes();
        let target = target.as_bytes();
        let mut offset = 0;
        while let Some(relative) = output[offset..]
            .windows(source.len())
            .position(|window| window == source)
        {
            let start = offset + relative;
            output[start..start + source.len()].copy_from_slice(target);
            offset = start + source.len();
        }
    }
    Ok(output)
}

struct PredicatePageIndexPruning;

impl Scenario for PredicatePageIndexPruning {
    fn name(&self) -> &'static str {
        "connector/predicate-page-index-pruning"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect page-index control session")?,
        )?;
        let warehouse = create_warehouse(context, "predicate-page-index-pruning")?;
        const CATALOG: &str = "page_index_catalog";
        const DATABASE: &str = "page_index_db";
        const TABLE: &str = "page_index_data";
        const PREDICATE: &str = "v >= 199000";

        context.action("create three dense Iceberg files that each require page-level pruning");
        create_catalog_table_and_dense_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

        let select = format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE} WHERE {PREDICATE}");
        context.action("run the static predicate with page-index reader disabled");
        control
            .query_drop("SET enable_parquet_reader_page_index = false")
            .context("disable predicate-driven page-index pruning")?;
        let disabled: Vec<i64> = control
            .query(&select)
            .context("query dense Iceberg files with page-index disabled")?;

        context.action("run the same static predicate with page-index reader enabled");
        control
            .query_drop("SET enable_parquet_reader_page_index = true")
            .context("enable predicate-driven page-index pruning")?;
        let enabled: Vec<i64> = control
            .query(&select)
            .context("query dense Iceberg files with page-index enabled")?;
        if enabled != disabled || enabled != [3_003] {
            bail!(
                "page-index toggle changed query correctness: disabled={disabled:?}, enabled={enabled:?}, expected=[3003]"
            );
        }

        context.action("assert EXPLAIN ANALYZE surfaces typed connector scan activity");
        let explain: Vec<String> = control
            .query(format!("EXPLAIN ANALYZE {select}"))
            .context("collect typed connector EXPLAIN ANALYZE profile")?;
        let explain = explain.join("\n");
        if !explain.contains("TypedConnectorMetrics:") {
            bail!("page-index EXPLAIN ANALYZE has no typed connector metrics; profile={explain}");
        }
        assert_positive_profile_counter(&explain, "TypedConnectorPageSourcesOpened")?;
        Ok(())
    }
}

impl Scenario for CatalogVersionDrain {
    fn name(&self) -> &'static str {
        "connector/catalog-version-drain"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect catalog version drain control session")?,
        )?;

        let warehouse = create_warehouse(context, "catalog-version-drain")?;
        context.action("create the first Iceberg catalog version and three data files");
        create_catalog_table_and_data(
            &mut control,
            "connector_generation_catalog",
            "connector_generation_db",
            "connector_generation_data",
            &warehouse,
        )?;

        context.action("start a read pinned to the first catalog version");
        let target = start_connector_read(
            &user,
            port,
            "connector_generation_catalog",
            "connector_generation_db",
            "connector_generation_data",
        )?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive old-version connection id")?)
            .context("old-version connector read terminated before publishing its connection id")?;
        let old_logs = wait_for_in_flight_reader_on_every_backend(
            context,
            "connector_generation_catalog",
            "wait for every BE to open an old-version connector reader",
        )?;
        let old_versions = reader_catalog_versions(&old_logs, "connector_generation_catalog")?;
        if let Ok(result) = target.done.try_recv() {
            bail!(
                "old-version connector read completed before replacement was published: {result:?}"
            );
        }

        context.action("drop and recreate the catalog while old readers remain in flight");
        control
            .query_drop("DROP CATALOG connector_generation_catalog")
            .context("retire first catalog version")?;
        create_catalog(&mut control, "connector_generation_catalog", &warehouse)?;

        context.action("read the replacement catalog version while the old version is leased");
        let replacement_rows: Vec<i64> = control
            .query(
                "SELECT count(*) FROM connector_generation_catalog.connector_generation_db.connector_generation_data",
            )
            .context("read table through replacement catalog version while old version is leased")?;
        if replacement_rows != [300_000] {
            bail!("replacement catalog version returned {replacement_rows:?}, expected [300000]");
        }
        wait_for_replacement_reader_on_every_backend(
            context,
            "connector_generation_catalog",
            &old_versions,
        )?;
        if let Ok(result) = target.done.try_recv() {
            bail!(
                "old-version connector read completed while replacement was being verified: {result:?}"
            );
        }

        context.action(format!(
            "cancel old-version reader through KILL QUERY {connection_id}"
        ));
        control
            .query_drop(format!("KILL QUERY {connection_id}"))
            .context("issue public MySQL KILL QUERY for old-generation reader")?;
        assert_cancelled_query(
            &target.done,
            context.remaining("await old-version reader cancellation")?,
        )?;
        assert_target_connection_remains_usable(
            &target,
            context.remaining("verify old-generation KILL QUERY target remains usable")?,
        )?;
        assert_idle_query(&mut control, connection_id)?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("old-generation connector read thread panicked"))??;

        wait_for_retired_catalog_version_close(
            context,
            "connector_generation_catalog",
            &old_versions,
        )?;

        await_resource_convergence(context, &baseline, "catalog version drain")?;
        Ok(())
    }
}

struct ConnectorRead {
    ready: mpsc::Receiver<u32>,
    done: mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    probe: mpsc::SyncSender<()>,
    probe_result: mpsc::Receiver<std::result::Result<Option<i64>, mysql::Error>>,
    release: mpsc::Sender<()>,
    thread: thread::JoinHandle<Result<()>>,
}

struct IdleMysqlConnection {
    ready: mpsc::Receiver<u32>,
    probe: mpsc::SyncSender<()>,
    probe_result: mpsc::Receiver<std::result::Result<Option<i64>, mysql::Error>>,
    thread: thread::JoinHandle<Result<()>>,
}

fn connector_reader_environment() -> CrossProcessChildEnvironment {
    let mut environment = CrossProcessChildEnvironment::default();
    // This is a generic child launch input, not a connector-specific harness
    // API. The runner uses the marker only to establish the observable
    // in-flight reader/retirement boundary for these scenarios.
    environment.be.insert(
        "NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER".to_string(),
        "1".to_string(),
    );
    environment.be.insert(
        "NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER".to_string(),
        "1".to_string(),
    );
    environment.be.insert(
        "NOVAROCKS_SQL_TEST_EMIT_CATALOG_MATERIALIZATION_MARKER".to_string(),
        "1".to_string(),
    );
    environment.be.insert(
        "NOVAROCKS_SQL_TEST_EMIT_CANCEL_MARKER".to_string(),
        "1".to_string(),
    );
    environment
}

fn connector_launch_config() -> ScenarioLaunchConfig {
    ScenarioLaunchConfig {
        child_environment: connector_reader_environment(),
        config_overlay: CrossProcessConfigOverlay {
            fe: Some(READER_CACHE_OVERLAY.to_string()),
            be: Some(format!(
                r#"
[runtime]
operator_buffer_chunks = 1
query_control_terminal_drain_timeout_ms = 1000
{READER_CACHE_OVERLAY}
"#
            )),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn static_credential_launch_overlay(snapshot: &Path) -> CrossProcessConfigOverlay {
    let credentials = static_credential_registry_overlay();
    CrossProcessConfigOverlay {
        fe: Some(format!(
            "[catalog_source]\nmode = \"static-file\"\nstatic_file_path = \"{}\"\n{credentials}\n{READER_CACHE_OVERLAY}",
            snapshot.display()
        )),
        be: Some(format!("{credentials}\n{READER_CACHE_OVERLAY}")),
        ..Default::default()
    }
}

fn static_credential_registry_overlay() -> String {
    format!(
        r#"
[[connector.credentials]]
purpose = "object-store-data"
name = "{STATIC_BLUE_CREDENTIAL_NAME}"
generation = "{STATIC_BLUE_CREDENTIAL_GENERATION}"
kind = "s3"
access_key_id = "{STATIC_BLUE_KEY_ID}"
access_key_secret = "{STATIC_BLUE_KEY_SECRET}"

[[connector.credentials]]
purpose = "object-store-data"
name = "{STATIC_GREEN_CREDENTIAL_NAME}"
generation = "{STATIC_GREEN_CREDENTIAL_GENERATION}"
kind = "s3"
access_key_id = "{STATIC_GREEN_KEY_ID}"
access_key_secret = "{STATIC_GREEN_KEY_SECRET}"
"#
    )
}

fn write_static_credential_snapshot(
    snapshot: &Path,
    blue_endpoint: &str,
    green_endpoint: &str,
) -> Result<()> {
    std::fs::write(
        snapshot,
        format!(
            "format_version = 3\n\
             [[catalogs]]\n\
             instance_id = \"{STATIC_BLUE_CATALOG}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{STATIC_BLUE_CATALOG}\"\n\
             config_format_version = 3\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"frontend-and-backend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_BLUE_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_BLUE_CREDENTIAL_GENERATION}\"\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"hadoop\"\n\
             \"iceberg.catalog.warehouse\" = \"s3://cca-static-blue/warehouse\"\n\
             \"aws.s3.endpoint\" = \"{blue_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n\
             [[catalogs]]\n\
             instance_id = \"{STATIC_GREEN_CATALOG}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{STATIC_GREEN_CATALOG}\"\n\
             config_format_version = 3\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"frontend-and-backend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_GREEN_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_GREEN_CREDENTIAL_GENERATION}\"\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"hadoop\"\n\
             \"iceberg.catalog.warehouse\" = \"s3://cca-static-green/warehouse\"\n\
             \"aws.s3.endpoint\" = \"{green_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n"
        ),
    )
    .with_context(|| format!("write static credential snapshot {}", snapshot.display()))
}

fn write_access_domain_collision_snapshot(
    snapshot: &Path,
    blue_endpoint: &str,
    green_endpoint: &str,
) -> Result<()> {
    std::fs::write(
        snapshot,
        format!(
            "format_version = 3\n\
             [[catalogs]]\n\
             instance_id = \"{COLLISION_BLUE_CATALOG}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{COLLISION_BLUE_CATALOG}\"\n\
             config_format_version = 3\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"frontend-and-backend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_BLUE_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_BLUE_CREDENTIAL_GENERATION}\"\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"hadoop\"\n\
             \"iceberg.catalog.warehouse\" = \"{COLLISION_WAREHOUSE}\"\n\
             \"aws.s3.endpoint\" = \"{blue_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n\
             [[catalogs]]\n\
             instance_id = \"{COLLISION_GREEN_CATALOG}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{COLLISION_GREEN_CATALOG}\"\n\
             config_format_version = 3\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"frontend-and-backend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_GREEN_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_GREEN_CREDENTIAL_GENERATION}\"\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"hadoop\"\n\
             \"iceberg.catalog.warehouse\" = \"{COLLISION_WAREHOUSE}\"\n\
             \"aws.s3.endpoint\" = \"{green_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n"
        ),
    )
    .with_context(|| {
        format!(
            "write access-domain collision snapshot {}",
            snapshot.display()
        )
    })
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let count = context.handle().be_count();
    if count != 3 {
        bail!(
            "{} requires the native acceptance topology 1FE+3BE, received 1FE+{count}BE",
            context.name()
        );
    }
    Ok(())
}

fn mysql_endpoint(context: &ScenarioContext) -> (String, u16) {
    (context.mysql_user().to_string(), context.mysql_port())
}

fn resource_baseline(context: &mut ScenarioContext) -> Result<QueryExecutionResourceSnapshot> {
    context
        .handle()
        .query_execution_resource_snapshot()?
        .context("cross-process system scenario requires the query resource oracle")
}

fn await_resource_convergence(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
    operation: &str,
) -> Result<()> {
    let deadline = context.deadline();
    context.action(format!(
        "await query-execution resource convergence after {operation}"
    ));
    context
        .handle()
        .await_query_execution_resource_convergence(baseline, true, deadline)
        .with_context(|| format!("resource convergence after {operation}"))
}

fn create_warehouse(context: &ScenarioContext, name: &str) -> Result<std::path::PathBuf> {
    let warehouse = context.runtime_dir().join("warehouses").join(name);
    std::fs::create_dir_all(&warehouse)
        .with_context(|| format!("create Iceberg warehouse {}", warehouse.display()))?;
    Ok(warehouse)
}

fn create_catalog_table_and_data(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    warehouse: &std::path::Path,
) -> Result<()> {
    create_catalog(control, catalog, warehouse)?;
    control
        .query_drop(format!("CREATE DATABASE {catalog}.{database}"))
        .with_context(|| format!("create {catalog}.{database}"))?;
    control
        .query_drop(format!(
            "CREATE TABLE {catalog}.{database}.{table} (v BIGINT)"
        ))
        .with_context(|| format!("create {catalog}.{database}.{table}"))?;
    for range in ["1, 100000", "100001, 200000", "200001, 300000"] {
        control
            .query_drop(format!(
                "INSERT INTO {catalog}.{database}.{table} SELECT generate_series FROM TABLE(generate_series({range}))"
            ))
            .with_context(|| format!("write data range {range} to {catalog}.{database}.{table}"))?;
    }
    Ok(())
}

fn create_static_catalog_table_and_data(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    ranges: [&str; 3],
) -> Result<()> {
    control
        .query_drop(format!("CREATE DATABASE {catalog}.{database}"))
        .with_context(|| format!("create {catalog}.{database} from StaticFile source"))?;
    control
        .query_drop(format!(
            "CREATE TABLE {catalog}.{database}.{table} (v BIGINT)"
        ))
        .with_context(|| format!("create {catalog}.{database}.{table} from StaticFile source"))?;
    // One committed data file per range gives the distributed 1FE+3BE query
    // enough independent S3 work to prove every BE read the declared catalog.
    for range in ranges {
        control
            .query_drop(format!(
                "INSERT INTO {catalog}.{database}.{table} SELECT generate_series FROM TABLE(generate_series({range}))"
            ))
            .with_context(|| {
                format!("write static credential range {range} to {catalog}.{database}.{table}")
            })?;
    }
    Ok(())
}

fn static_catalog_profile(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
) -> Result<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rows: Vec<String> = control
            .query(format!(
                "EXPLAIN ANALYZE SELECT count(*) FROM {catalog}.{database}.{table}"
            ))
            .with_context(|| {
                format!("profile {catalog}.{database}.{table} through typed connector")
            })?;
        let profile = rows.join("\n");
        if profile.contains("TypedConnectorMetrics:") {
            return Ok(profile);
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "static credential catalog {catalog}.{database}.{table} EXPLAIN ANALYZE has no typed connector metrics after bounded metadata visibility wait; profile={profile}"
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn assert_static_catalog_sum(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    expected: i64,
) -> Result<()> {
    let rows: Vec<i64> = control
        .query(format!("SELECT sum(v) FROM {catalog}.{database}.{table}"))
        .with_context(|| format!("sum {catalog}.{database}.{table} through typed connector"))?;
    if rows != [expected] {
        bail!(
            "static credential catalog {catalog}.{database}.{table} returned sum {rows:?}, expected [{expected}]"
        );
    }
    Ok(())
}

fn assert_static_reader_opened_on_every_backend(logs: &[String], catalog: &str) -> Result<()> {
    for (index, log) in logs.iter().enumerate() {
        if reader_open_lines(log, catalog).next().is_none() {
            bail!("BE[{index}] did not open a typed reader for static catalog {catalog}");
        }
    }
    Ok(())
}

fn assert_fixture_used_only_expected_key(
    fixture: &str,
    requests: &[LoopbackS3Request],
    expected_key_id: &str,
) -> Result<()> {
    let reads = requests
        .iter()
        .filter(|request| request.method == "GET" && request.status < 400)
        .collect::<Vec<_>>();
    if reads.is_empty() {
        bail!("{fixture} loopback S3 fixture recorded no successful GET request");
    }
    for request in reads {
        if request.credential_key_id.as_deref() != Some(expected_key_id) {
            bail!(
                "{fixture} loopback S3 GET {} used credential key {:?}, expected {expected_key_id}",
                request.path,
                request.credential_key_id
            );
        }
    }
    Ok(())
}

fn create_catalog_table_and_dense_data(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    warehouse: &std::path::Path,
) -> Result<()> {
    create_catalog(control, catalog, warehouse)?;
    control
        .query_drop(format!("CREATE DATABASE {catalog}.{database}"))
        .with_context(|| format!("create {catalog}.{database}"))?;
    control
        .query_drop(format!(
            "CREATE TABLE {catalog}.{database}.{table} (v BIGINT)"
        ))
        .with_context(|| format!("create {catalog}.{database}.{table}"))?;
    // Each transaction writes one file. The duplicated ordered range prevents
    // Iceberg file-metric pruning from eliminating an entire file, while its
    // size forces multiple Parquet data pages per file for the FS page-index
    // path under test.
    for _ in 0..3 {
        control
            .query_drop(format!(
                "INSERT INTO {catalog}.{database}.{table} SELECT generate_series FROM TABLE(generate_series(1, 200000))"
            ))
            .with_context(|| format!("write dense page-index data to {catalog}.{database}.{table}"))?;
    }
    Ok(())
}

fn create_catalog(
    control: &mut mysql::Conn,
    catalog: &str,
    warehouse: &std::path::Path,
) -> Result<()> {
    let warehouse = warehouse.to_string_lossy().replace('"', "\\\"");
    control
        .query_drop(format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
        ))
        .with_context(|| format!("create Hadoop Iceberg catalog {catalog}"))
}

fn assert_catalog_read_summary(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    expected_count: i64,
    expected_sum: i64,
) -> Result<()> {
    let rows: Vec<(i64, i64)> = control
        .query(format!(
            "SELECT count(*), sum(v) FROM {catalog}.{database}.{table}"
        ))
        .context("read catalog table after distributed write")?;
    if rows != [(expected_count, expected_sum)] {
        bail!(
            "catalog read-write summary returned {rows:?}, expected [({expected_count}, {expected_sum})]"
        );
    }
    Ok(())
}

fn assert_positive_profile_counter(profile: &str, name: &str) -> Result<()> {
    let marker = format!("{name}=");
    let value = profile
        .split(&marker)
        .nth(1)
        .and_then(|tail| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<u64>()
                .ok()
        })
        .context(format!(
            "page-index EXPLAIN ANALYZE profile is missing {marker}; profile={profile}"
        ))?;
    if value == 0 {
        bail!("page-index EXPLAIN ANALYZE counter {name} must be positive; profile={profile}");
    }
    Ok(())
}

fn start_connector_read(
    user: &str,
    port: u16,
    catalog: &str,
    database: &str,
    table: &str,
) -> Result<ConnectorRead> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::sync_channel(1);
    let (probe, probe_rx) = mpsc::sync_channel(1);
    let (probe_result_tx, probe_result) = mpsc::sync_channel(1);
    let (release, release_rx) = mpsc::channel();
    let user = user.to_string();
    // Keep every file reader in flight long enough to observe and cancel it,
    // while bounding each synchronous SLEEP evaluation to one second per
    // 4,096-row connector batch. Sleeping once for every input row would keep
    // the driver inside a single expression evaluation for hours after abort.
    let query = format!(
        "SELECT t.s FROM (SELECT sleep(1) AS s FROM {catalog}.{database}.{table} WHERE v % 4096 = 0) AS t CROSS JOIN TABLE(generate_series(1, 1000000000)) AS gs(x)"
    );
    let thread = thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect(&user, port, Duration::from_secs(10))
            .context("connect connector reader MySQL client")?;
        ready_tx
            .send(connection.connection_id())
            .context("publish connector reader MySQL connection id")?;
        let result = connection.query::<i64, _>(query);
        done_tx
            .send(result)
            .context("publish connector reader MySQL result")?;
        probe_rx
            .recv()
            .context("receive connector reader connection probe")?;
        probe_result_tx
            .send(connection.query_first::<i64, _>("SELECT 1"))
            .context("publish connector reader connection probe result")?;
        release_rx
            .recv()
            .context("release connector reader MySQL session")?;
        Ok(())
    });
    Ok(ConnectorRead {
        ready,
        done,
        probe,
        probe_result,
        release,
        thread,
    })
}

fn start_idle_mysql_connection(user: &str, port: u16) -> Result<IdleMysqlConnection> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (probe, probe_rx) = mpsc::sync_channel(1);
    let (probe_result_tx, probe_result) = mpsc::sync_channel(1);
    let user = user.to_string();
    let thread = thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect(&user, port, Duration::from_secs(10))
            .context("connect idle MySQL target")?;
        ready_tx
            .send(connection.connection_id())
            .context("publish idle MySQL target connection id")?;
        probe_rx
            .recv()
            .context("receive idle MySQL target connection probe")?;
        probe_result_tx
            .send(connection.query_first::<i64, _>("SELECT 1"))
            .context("publish idle MySQL target connection probe result")?;
        Ok(())
    });
    Ok(IdleMysqlConnection {
        ready,
        probe,
        probe_result,
        thread,
    })
}

fn assert_idle_query(control: &mut mysql::Conn, connection_id: u32) -> Result<()> {
    control
        .query_drop(format!("KILL QUERY {connection_id}"))
        .context("idle KILL QUERY must succeed for a live target connection")
}

fn assert_target_connection_remains_usable(
    target: &ConnectorRead,
    timeout: Duration,
) -> Result<()> {
    target
        .probe
        .send(())
        .context("request KILL QUERY target connection probe")?;
    match target
        .probe_result
        .recv_timeout(timeout)
        .context("KILL QUERY target did not answer the connection probe")?
    {
        Ok(Some(1)) => Ok(()),
        Ok(result) => bail!("KILL QUERY target probe returned {result:?}, expected Some(1)"),
        Err(error) => bail!("KILL QUERY unexpectedly closed target connection: {error}"),
    }
}

fn assert_target_connection_is_closed(target: &ConnectorRead, timeout: Duration) -> Result<()> {
    target
        .probe
        .send(())
        .context("request KILL CONNECTION target connection probe")?;
    match target
        .probe_result
        .recv_timeout(timeout)
        .context("KILL CONNECTION target did not answer the connection probe")?
    {
        Err(_) => Ok(()),
        Ok(result) => bail!("KILL CONNECTION left the target connection usable: {result:?}"),
    }
}

fn assert_idle_target_connection_is_closed(
    target: &IdleMysqlConnection,
    timeout: Duration,
) -> Result<()> {
    target
        .probe
        .send(())
        .context("request bare KILL target connection probe")?;
    match target
        .probe_result
        .recv_timeout(timeout)
        .context("bare KILL target did not answer the connection probe")?
    {
        Err(_) => Ok(()),
        Ok(result) => bail!("bare KILL left the idle target connection usable: {result:?}"),
    }
}

fn release_connector_read(target: &ConnectorRead) -> Result<()> {
    target
        .release
        .send(())
        .context("release connector reader session after cancellation")
}

fn assert_cancelled_query(
    done: &mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    timeout: Duration,
) -> Result<()> {
    let result = done
        .recv_timeout(timeout)
        .context("connector reader did not terminate before the scenario deadline")?;
    let error = match result {
        Ok(rows) => bail!("connector reader unexpectedly succeeded after KILL QUERY: {rows:?}"),
        Err(error) => error,
    };
    match error {
        mysql::Error::MySqlError(error) if error.code == 1317 => Ok(()),
        other => bail!("expected MySQL cancellation error 1317, received {other}"),
    }
}

fn assert_connection_killed_query(
    done: &mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    timeout: Duration,
) -> Result<()> {
    match done
        .recv_timeout(timeout)
        .context("KILL CONNECTION target query did not terminate before the scenario deadline")?
    {
        Ok(rows) => bail!("KILL CONNECTION target query unexpectedly succeeded: {rows:?}"),
        Err(_) => Ok(()),
    }
}

fn wait_for_open_reader_on_every_backend(
    context: &mut ScenarioContext,
    catalog: &str,
    operation: &str,
) -> Result<Vec<String>> {
    let marker = format!("{CONNECTOR_READER_OPEN} provider=iceberg instance={catalog}");
    wait_for_backend_logs(context, operation, |logs| {
        logs.iter().all(|log| log.contains(&marker))
    })
}

fn wait_for_in_flight_reader_on_every_backend(
    context: &mut ScenarioContext,
    catalog: &str,
    operation: &str,
) -> Result<Vec<String>> {
    let marker = format!("{CONNECTOR_READER_OPEN} provider=iceberg instance={catalog}");
    wait_for_backend_logs(context, operation, |logs| {
        logs.iter().all(|log| {
            let (opens, closes) = reader_counts(log);
            log.contains(&marker) && opens > closes
        })
    })
}

fn wait_for_replacement_reader_on_every_backend(
    context: &mut ScenarioContext,
    catalog: &str,
    old_versions: &[String],
) -> Result<Vec<String>> {
    wait_for_backend_logs(
        context,
        "wait for every BE to resolve the replacement catalog version",
        |logs| {
            logs.iter().zip(old_versions).all(|(log, old)| {
                reader_open_lines(log, catalog)
                    .any(|line| reader_catalog_version(line).is_some_and(|current| current != old))
            })
        },
    )
}

fn wait_for_balanced_reader_lifecycle(
    context: &mut ScenarioContext,
    operation: &str,
) -> Result<Vec<String>> {
    wait_for_backend_logs(context, operation, |logs| {
        logs.iter().all(|log| {
            let (opens, closes) = reader_counts(log);
            opens > 0 && opens == closes
        })
    })
}

fn wait_for_retired_catalog_version_close(
    context: &mut ScenarioContext,
    catalog: &str,
    old_versions: &[String],
) -> Result<Vec<String>> {
    wait_for_backend_logs(
        context,
        "wait for every retired catalog-version reader to close",
        |logs| {
            logs.iter().zip(old_versions).all(|(log, version)| {
                let (opens, closes) = reader_counts_for_catalog_version(log, catalog, version);
                opens > 0 && opens == closes
            })
        },
    )
}

fn wait_for_backend_logs(
    context: &mut ScenarioContext,
    operation: &str,
    predicate: impl Fn(&[String]) -> bool,
) -> Result<Vec<String>> {
    loop {
        let logs = (0..context.handle().be_count())
            .map(|index| context.handle().be_current_log_contents(index))
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!("read BE logs while waiting to {operation}"))?;
        if predicate(&logs) {
            return Ok(logs);
        }
        let remaining = context.remaining(operation)?;
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn backend_log_snapshots(context: &mut ScenarioContext) -> Result<Vec<String>> {
    (0..context.handle().be_count())
        .map(|index| context.handle().be_current_log_contents(index))
        .collect::<Result<Vec<_>>>()
        .context("read Backend log snapshots")
}

fn wait_for_catalog_lifecycle_marker(
    context: &mut ScenarioContext,
    before: &[String],
    marker: &str,
    operation: &str,
) -> Result<Vec<String>> {
    wait_for_backend_logs(context, operation, |logs| {
        logs.iter().zip(before).all(|(log, previous)| {
            log.get(previous.len()..)
                .is_some_and(|appended| appended.contains(marker))
        })
    })
}

fn wait_for_catalog_lifecycle_marker_on_backend(
    context: &mut ScenarioContext,
    before: &[String],
    backend: usize,
    marker: &str,
    operation: &str,
) -> Result<()> {
    let previous = before
        .get(backend)
        .with_context(|| format!("missing BE[{backend}] log snapshot"))?;
    wait_for_backend_logs(context, operation, |logs| {
        logs.get(backend)
            .and_then(|log| log.get(previous.len()..))
            .is_some_and(|appended| appended.contains(marker))
    })
    .map(|_| ())
}

fn assert_no_appended_catalog_stage_admitted(
    context: &mut ScenarioContext,
    before: &[String],
) -> Result<()> {
    let logs = backend_log_snapshots(context)?;
    assert_no_new_catalog_marker(before, &logs, "NOVAROCKS_CATALOG_STAGE_ADMITTED")
}

fn assert_no_appended_catalog_ready(
    context: &mut ScenarioContext,
    before: &[String],
) -> Result<()> {
    let logs = backend_log_snapshots(context)?;
    assert_no_new_catalog_marker(before, &logs, "NOVAROCKS_CATALOG_READY")
}

fn assert_no_new_catalog_lifecycle_markers(before: &[String], after: &[String]) -> Result<()> {
    for marker in ["NOVAROCKS_CATALOG_LOADING", "NOVAROCKS_CATALOG_READY"] {
        assert_no_new_catalog_marker(before, after, marker)?;
    }
    Ok(())
}

fn assert_no_new_catalog_marker(before: &[String], after: &[String], marker: &str) -> Result<()> {
    for (index, (previous, current)) in before.iter().zip(after).enumerate() {
        let appended = current.get(previous.len()..).with_context(|| {
            format!("BE[{index}] log was truncated while checking marker {marker}")
        })?;
        if appended.contains(marker) {
            bail!("BE[{index}] emitted unexpected catalog lifecycle marker {marker}");
        }
    }
    Ok(())
}

fn assert_exactly_one_new_catalog_marker_per_backend(
    before: &[String],
    after: &[String],
    marker: &str,
) -> Result<()> {
    for (index, (previous, current)) in before.iter().zip(after).enumerate() {
        let appended = current.get(previous.len()..).with_context(|| {
            format!("BE[{index}] log was truncated while counting marker {marker}")
        })?;
        let count = appended.matches(marker).count();
        if count != 1 {
            bail!("BE[{index}] emitted {count} new {marker} markers, expected exactly one");
        }
    }
    Ok(())
}

fn release_catalog_install_hold(hold_file: &std::path::Path) -> Result<()> {
    std::fs::remove_file(hold_file)
        .with_context(|| format!("release catalog-install hold file {}", hold_file.display()))
}

fn assert_no_reader_open_after_abort(logs: &[String]) -> Result<()> {
    for (index, log) in logs.iter().enumerate() {
        let Some(abort_offset) = log.find("NOVAROCKS_QUERY_LIFECYCLE_ABORT") else {
            bail!("BE[{index}] did not record lifecycle Abort after KILL QUERY");
        };
        if log[abort_offset..].contains(CONNECTOR_READER_OPEN) {
            bail!("BE[{index}] opened a connector reader after lifecycle Abort");
        }
    }
    Ok(())
}

fn reader_catalog_versions(logs: &[String], catalog: &str) -> Result<Vec<String>> {
    logs.iter()
        .enumerate()
        .map(|(index, log)| {
            reader_open_lines(log, catalog)
                .find_map(reader_catalog_version)
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!("BE[{index}] reader marker did not include catalog version")
                })
        })
        .collect()
}

fn reader_catalog_versions_after(
    logs: &[String],
    previous_logs: &[String],
    catalog: &str,
) -> Result<Vec<String>> {
    logs.iter()
        .zip(previous_logs)
        .enumerate()
        .map(|(index, (log, previous))| {
            let appended = log.get(previous.len()..).with_context(|| {
                format!("BE[{index}] log was truncated while checking FE restart catalog version")
            })?;
            reader_open_lines(appended, catalog)
                .find_map(reader_catalog_version)
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!(
                        "BE[{index}] post-restart reader marker did not include catalog version"
                    )
                })
        })
        .collect()
}

fn reader_open_lines<'a>(log: &'a str, catalog: &str) -> impl Iterator<Item = &'a str> {
    log.lines().filter(move |line| {
        line.contains(CONNECTOR_READER_OPEN)
            && line.contains("provider=iceberg")
            && line.contains(&format!("instance={catalog}"))
    })
}

fn reader_catalog_version(line: &str) -> Option<&str> {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("catalog_version="))
}

fn reader_counts(log: &str) -> (usize, usize) {
    (
        log.match_indices(CONNECTOR_READER_OPEN).count(),
        log.match_indices(CONNECTOR_READER_CLOSE).count(),
    )
}

fn catalog_materialization_counts(logs: &[String]) -> Vec<usize> {
    logs.iter()
        .map(|log| {
            log.matches("NOVAROCKS_CATALOG_RUNTIME_MATERIALIZED")
                .count()
        })
        .collect()
}

fn reader_counts_for_catalog_version(log: &str, catalog: &str, version: &str) -> (usize, usize) {
    let count = |event| {
        log.lines()
            .filter(|line| {
                line.contains(event)
                    && line.contains("provider=iceberg")
                    && line.contains(&format!("instance={catalog}"))
                    && reader_catalog_version(line) == Some(version)
            })
            .count()
    };
    (count(CONNECTOR_READER_OPEN), count(CONNECTOR_READER_CLOSE))
}
