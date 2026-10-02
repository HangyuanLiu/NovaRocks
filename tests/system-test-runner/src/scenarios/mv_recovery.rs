use super::mv_uea7::{
    ManagedMvRestFixture, property, require_status_phase, status, wait_for_status_phase,
};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use ::mysql::prelude::{FromRow, Queryable};
use ::mysql::{Conn, Row};
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::ServerHandle;
use reqwest::blocking::Client;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The task protocol's only fault that fails a participant which was admitted,
/// published RUNNING, and then failed on its own. It replaces the retired
/// protocol's `start-ack-suppress` here because a suppressed start acknowledged
/// nothing, while a staged MV snapshot only exists once a task really ran.
const TASK_EXECUTION_FAILURE: &str = "task-execution-failure";

/// Stable evidence that the injection above actually fired.
const TASK_EXECUTION_FAILURE_MARKER: &str = "NOVAROCKS_TASK_EXECUTION_FAILURE_INJECTED";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(MvStateStoreRestart::default()),
        Box::new(MvSchedulerRecovery::default()),
        Box::new(MvRewriteBindingBarrier::default()),
        Box::new(MvCurrentDependencyRecheck::default()),
        Box::new(MvLegacyInterpretationRebuild::default()),
        Box::new(MvCapacityStop::default()),
        Box::new(MvInvalidRestart::default()),
        Box::new(MvCommitResponseLoss::default()),
        Box::new(MvValidationRecovery::new(
            ValidationRecoveryCase::FrontendCrash,
        )),
        Box::new(MvValidationRecovery::new(
            ValidationRecoveryCase::BackendFailure,
        )),
        Box::new(MvRefreshConfigurationInterleaving::default()),
        Box::new(MvStagedPublishedRecovery::default()),
        Box::new(MvFirstRefreshStaging::default()),
        Box::new(MvBaseIdentityReplacement::default()),
        Box::new(MvLakePublicationRestartRebuild::default()),
        Box::new(MvRecursiveTypeRestart::default()),
    ]
}

#[derive(Default)]
struct MvStateStoreRestart {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvStateStoreRestart {
    fn name(&self) -> &'static str {
        "mv/state-store-restart"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) = ManagedMvRestFixture::start(scenario_root, "system_mv_restart")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_restart";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;

        execute(
            context,
            &mut conn,
            "create StateStore-backed materialized view",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read first MV publication before FE restart",
        )?;
        drop(conn);

        restart_frontend(context, "restart FE with the persisted StateStore")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read existing MV after FE restart",
        )?;
        require_status_phase(
            context,
            &mut conn,
            catalog,
            "orders_mv",
            "AWAITING_EFFECT_SETTLEMENT",
            "confirm management is closed after FE restart",
        )?;
        let closed = status(context, &mut conn, catalog, "orders_mv")?;
        let challenge = property(&closed, "Challenge")?;
        let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
        context.action("declare the old FE isolated and resume exact MV management");
        let resumed: Vec<(String, Option<String>)> = conn
            .query(format!(
                "CALL novarocks_mv_resume_management('{catalog}', 'ns', 'orders_mv', \
                 '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
                 'the system scenario replaced the declared frontend process before this statement')"
            ))
            .context("resume managed MV after StateStore restart")?;
        if property(&resumed, "SettledEffects")? != "1" {
            bail!("StateStore restart readmission did not settle the old incarnation");
        }
        refresh(context, &mut conn, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create a second MV after StateStore recovery",
            "CREATE MATERIALIZED VIEW orders_mv_2 DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        let views: Vec<Row> = query(
            context,
            &mut conn,
            "SHOW MATERIALIZED VIEWS FROM ns",
            "list MV definitions after FE restart",
        )?;
        let names = views
            .iter()
            .map(|row| {
                row.get::<String, _>(0)
                    .context("SHOW MATERIALIZED VIEWS name column")
            })
            .collect::<Result<Vec<_>>>()?;
        if names != ["orders_mv", "orders_mv_2"] {
            bail!(
                "MV definitions did not survive StateStore restart: names={names:?}; {}",
                context.diagnostics()
            );
        }
        context
            .action("StateStore-backed MV definitions and visible publication survived FE restart");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvSchedulerRecovery {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvSchedulerRecovery {
    fn name(&self) -> &'static str {
        "mv/scheduler-recovery"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let barrier_dir = scenario_root.join("mv-scheduler-barrier");
        fs::create_dir_all(&barrier_dir).with_context(|| {
            format!(
                "create scheduler barrier directory {}",
                barrier_dir.display()
            )
        })?;
        clear_scheduler_markers(&barrier_dir)?;
        remove_if_exists(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.consumed"),
        )?;
        remove_if_exists(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.trigger"),
        )?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_scheduler")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        launch.child_environment.fe.insert(
            "NOVAROCKS_MVX4_SCHEDULER_TEST_DIR".to_string(),
            barrier_dir.to_string_lossy().into_owned(),
        );
        let mut fe_overlay = launch.config_overlay.fe.take().unwrap_or_default();
        fe_overlay.push_str(
            r#"
[standalone_server]
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1000
"#,
        );
        launch.config_overlay.fe = Some(fe_overlay);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let barrier_dir = context.scenario_root().join("mv-scheduler-barrier");
        let hold_trigger = barrier_dir.join("mvx4-scheduler-hold.trigger");
        let _hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        context.action("armed scheduler admission barrier");

        let catalog = "system_mv_scheduler";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, false)?;
        execute(
            context,
            &mut conn,
            "seed asynchronous MV source rows",
            "INSERT INTO orders VALUES (1, 10), (2, 20)",
        )?;
        execute(
            context,
            &mut conn,
            "create first asynchronous scheduler MV",
            "CREATE MATERIALIZED VIEW orders_mv_a DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        execute(
            context,
            &mut conn,
            "create second asynchronous scheduler MV",
            "CREATE MATERIALIZED VIEW orders_mv_b DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        wait_for_marker_count(
            context,
            &barrier_dir,
            1,
            "observe first scheduler admission",
        )?;
        thread::sleep(POLL_INTERVAL * 3);
        let admitted = marker_count(&barrier_dir)?;
        if admitted != 1 {
            bail!(
                "scheduler max_concurrent_refreshes=1 admitted {admitted} refreshes while held; {}",
                context.diagnostics()
            );
        }
        context.action("verified scheduler permit admitted exactly one native refresh");

        _hold.remove()?;
        context.action("released scheduler admission barrier");
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
            &[(1, 10), (2, 20)],
            "wait for first scheduler MV initial catch-up",
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
            &[(1, 10), (2, 20)],
            "wait for second scheduler MV initial catch-up",
        )?;
        execute(
            context,
            &mut conn,
            "mutate asynchronous MV source rows",
            "INSERT INTO orders VALUES (3, 30)",
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "wait for first scheduler MV incremental catch-up",
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "wait for second scheduler MV incremental catch-up",
        )?;
        execute(
            context,
            &mut conn,
            "pause first scheduler MV before recovery fault",
            "ALTER MATERIALIZED VIEW orders_mv_a PAUSE REFRESH",
        )?;
        execute(
            context,
            &mut conn,
            "pause second scheduler MV before recovery fault",
            "ALTER MATERIALIZED VIEW orders_mv_b PAUSE REFRESH",
        )?;

        clear_scheduler_markers(&barrier_dir)?;
        let recovery_hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        execute(
            context,
            &mut conn,
            "create a scheduler MV for FE recovery",
            "CREATE MATERIALIZED VIEW orders_mv_recovery DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        wait_for_file(
            context,
            &barrier_dir.join("mvx4-scheduler-admitted-orders_mv_recovery.marker"),
            "hold recovery MV scheduler refresh before FE replacement",
        )?;
        execute(
            context,
            &mut conn,
            "add rows while scheduler refresh is held",
            "INSERT INTO orders VALUES (4, 40)",
        )?;
        let _transient_preparation_fault = FileTrigger::create(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.trigger"),
            "inject one typed connector unavailability after FE restart\n",
        )?;
        context.action("armed one transient scheduler preparation fault for FE recovery");
        drop(conn);
        context.action("terminate held scheduler FE attempt for recovery");
        context
            .handle()
            .kill_fe()
            .context("kill FE during held scheduler refresh")?;
        recovery_hold.remove()?;
        restart_frontend(context, "restart FE after interrupted scheduler refresh")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        resume_management_after_fe_restart(context, &mut conn, catalog, "orders_mv_recovery")?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_recovery ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            "wait for scheduler recovery to catch up durable MV",
        )?;
        let consumed_fault =
            barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.consumed");
        if !consumed_fault.exists() {
            bail!(
                "scheduler caught up without consuming the injected preparation fault; {}",
                context.diagnostics()
            );
        }
        context.action("verified the scheduler consumed its transient preparation fault");
        context.action("scheduler caught up after explicit FE readmission and one transient preparation failure");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

/// Proves that a distributed rewritten query consumes the M1 target snapshot
/// whose completed physical plan and read access were frozen, even if a normal
/// refresh publishes M2 before the query is dispatched to its backend tasks.
#[derive(Default)]
struct MvRewriteBindingBarrier {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvRewriteBindingBarrier {
    fn name(&self) -> &'static str {
        "mv/rewrite-final-target-binding"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let barrier_dir = scenario_root.join("mv-rewrite-barrier");
        fs::create_dir_all(&barrier_dir).with_context(|| {
            format!(
                "create MV rewrite barrier directory {}",
                barrier_dir.display()
            )
        })?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_rewrite_binding")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        launch.child_environment.fe.insert(
            "NOVAROCKS_MVX4_REWRITE_TEST_DIR".to_string(),
            barrier_dir.to_string_lossy().into_owned(),
        );
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_rewrite_binding";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let barrier_dir = context.scenario_root().join("mv-rewrite-barrier");
        let hold_trigger = barrier_dir.join("mvx4-rewrite-hold.trigger");
        let frozen_marker = barrier_dir.join("mvx4-completed-mv-target-frozen.marker");
        if frozen_marker.exists() {
            fs::remove_file(&frozen_marker).context("remove stale completed MV target marker")?;
        }
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, false)?;
        execute(
            context,
            &mut conn,
            "seed MV rewrite cost fixture",
            "INSERT INTO orders SELECT number % 3, CAST(number % 10 AS BIGINT) FROM TABLE(generate_series(1, 1200)) t(number)",
        )?;
        execute(
            context,
            &mut conn,
            "create aggregate MV for strict target binding",
            "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
        )?;
        refresh(context, &mut conn, "orders_agg_mv")?;

        let rewritten: Vec<(String,)> = query(
            context,
            &mut conn,
            "EXPLAIN SELECT k1, SUM(v2) FROM orders GROUP BY k1 ORDER BY k1",
            "confirm the query selects the fresh MV target",
        )?;
        if !rewritten
            .iter()
            .any(|(line,)| line.contains("rewritten with mv: orders_agg_mv"))
        {
            bail!(
                "MV rewrite was not selected before the binding barrier; {}",
                context.diagnostics()
            );
        }
        context.action("confirmed query selection uses the M1 MV publication");

        let _hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        let query = spawn_aggregate_query(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            context.remaining("start rewritten query at final target barrier")?,
        );
        wait_for_file_or_query(
            context,
            &frozen_marker,
            &query,
            "wait for completed M1 plan and read access to freeze",
        )?;
        context.action("observed completed query plan and access frozen on M1");

        execute(
            context,
            &mut conn,
            "advance source to S102 while query is held after M1 target freeze",
            "INSERT INTO orders VALUES (3, 99)",
        )?;
        refresh(context, &mut conn, "orders_agg_mv")?;
        context.action("published the normal M2 MV target before releasing the query");

        _hold.remove()?;
        context.action("release rewritten query after M2 publication");
        let rows =
            receive_aggregate_query(context, query, "wait for rewritten query frozen against M1")?;
        if rows != [(0, 1800), (1, 1800), (2, 1800)] {
            bail!(
                "rewritten query did not consume its frozen M1 target: rows={rows:?}; {}",
                context.diagnostics()
            );
        }
        context.action(
            "verified native 1FE+3BE query consumed M1 after concurrent S102/M2 publication",
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvCurrentDependencyRecheck {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

#[derive(Default)]
struct MvRefreshConfigurationInterleaving {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvRefreshConfigurationInterleaving {
    fn name(&self) -> &'static str {
        "mv/refresh-configuration-interleaving"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir)?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_config_interleaving")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        launch.child_environment.fe.insert(
            "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR".to_string(),
            fault_dir.to_string_lossy().into_owned(),
        );
        let mut fe_overlay = launch.config_overlay.fe.take().unwrap_or_default();
        fe_overlay.push_str("\n[runtime]\nquery_blocking_worker_threads = 2\n");
        launch.config_overlay.fe = Some(fe_overlay);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_config_interleaving";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV for concurrent configuration and refresh",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        let initial_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create another independently managed MV",
            "CREATE MATERIALIZED VIEW parallel_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        let initial_parallel_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "parallel_mv")?;
        execute(
            context,
            &mut conn,
            "advance source before the held refresh",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let prepared = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-data-prepared.trigger"),
            "token=before-configuration-write\n",
        )?;
        let held_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.remaining("start held refresh before configuration write")?,
        );
        wait_for_fe_marker(
            context,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=data-prepared token=before-configuration-write",
            "wait for completed MV computation before configuration write",
        )?;
        execute(
            context,
            &mut conn,
            "pause another MV while orders_mv refresh remains held",
            "ALTER MATERIALIZED VIEW parallel_mv PAUSE REFRESH",
        )?;
        let final_parallel_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "parallel_mv")?;
        if final_parallel_configuration == initial_parallel_configuration {
            bail!("other MV configuration did not commit while orders_mv was held");
        }

        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let timeout = context.remaining("start concurrent MV configuration write")?;
        thread::spawn(move || {
            let result = (|| -> Result<()> {
                let mut writer = mysql_actor::connect(&user, port, timeout)?;
                writer.query_drop(format!("SET CATALOG {catalog}"))?;
                writer.query_drop("USE ns")?;
                started_tx
                    .send(())
                    .context("signal configuration writer readiness")?;
                writer.query_drop("ALTER MATERIALIZED VIEW orders_mv PAUSE REFRESH")?;
                Ok(())
            })()
            .map_err(|error| format!("{error:#}"));
            let _ = result_tx.send(result);
        });
        started_rx
            .recv_timeout(context.remaining("wait for configuration writer readiness")?)
            .context("configuration writer did not reach its SQL request")?;
        context.action("configuration writer reached SQL while the refresh is held");
        prepared.remove()?;
        context.action("release the refresh and settle the waiting configuration write");
        match held_refresh.recv_timeout(context.remaining("receive held refresh")?) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => bail!("held refresh failed: {error}"),
            Err(error) => bail!("held refresh did not finish: {error}"),
        }
        match result_rx.recv_timeout(context.remaining("receive configuration write")?) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => bail!("concurrent configuration write failed: {error}"),
            Err(error) => bail!("concurrent configuration write did not finish: {error}"),
        }
        require_refresh_paused(context, &mut conn, "orders_mv", true)?;
        require_refresh_paused(context, &mut conn, "parallel_mv", true)?;
        require_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 2)?;
        require_rest_snapshot_count(context, &rest_uri, "ns", "parallel_mv", 0)?;
        let final_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "orders_mv")?;
        if final_configuration == initial_configuration {
            bail!("concurrent configuration write did not change the lake C revision");
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "read the completed refresh after the configuration write",
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

fn require_refresh_paused(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    view: &str,
    expected: bool,
) -> Result<()> {
    let rows: Vec<Row> = conn.query("SHOW MATERIALIZED VIEWS")?;
    let row = rows
        .iter()
        .find(|row| row.get::<String, _>("Name").as_deref() == Some(view))
        .with_context(|| format!("SHOW MATERIALIZED VIEWS omitted {view}"))?;
    let actual = row
        .get::<String, _>("RefreshPaused")
        .context("SHOW MATERIALIZED VIEWS omitted RefreshPaused")?;
    if actual != expected.to_string() {
        bail!(
            "{view} RefreshPaused is {actual}, expected {expected}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn require_rest_snapshot_count(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
    expected: usize,
) -> Result<()> {
    context.action("check the exact number of retained MV outputs through REST");
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact MV output count")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let snapshots = loaded["metadata"]["snapshots"]
        .as_array()
        .context("REST MV metadata lacks snapshots")?;
    if snapshots.len() != expected {
        bail!("MV has {} snapshots, expected {expected}", snapshots.len());
    }
    Ok(())
}

fn rest_configuration_revision(
    context: &ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<Vec<u8>> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact lake configuration revision")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let encoded = loaded["metadata"]["properties"]["novarocks.documents.v1"]
        .as_str()
        .context("REST MV metadata lacks document manifest")?;
    let manifest: serde_json::Value = serde_json::from_str(encoded)?;
    let documents = manifest["documents"]
        .as_array()
        .context("REST MV document manifest lacks documents")?;
    let configuration = documents
        .iter()
        .find(|document| document["owner"] == "novarocks.mv" && document["name"] == "configuration")
        .context("REST MV document manifest lacks C")?;
    let revision: Vec<u8> = serde_json::from_value(configuration["revision"].clone())?;
    if revision.len() != 32 {
        bail!(
            "REST MV C revision has {} bytes instead of 32",
            revision.len()
        );
    }
    Ok(revision)
}

impl Scenario for MvCurrentDependencyRecheck {
    fn name(&self) -> &'static str {
        "mv/current-dependency-recheck"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir)?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_dependency_recheck")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        launch.child_environment.fe.insert(
            "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR".to_string(),
            fault_dir.to_string_lossy().into_owned(),
        );
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_dependency_recheck";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV for Current dependency recheck",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "advance the source before the held refresh",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let prepared = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-data-prepared.trigger"),
            "token=before-current-dependency-recheck\n",
        )?;
        let held_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.remaining("start held MV refresh")?,
        );
        wait_for_fe_marker(
            context,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=data-prepared token=before-current-dependency-recheck",
            "wait for BE computation before Current recheck",
        )?;
        let snapshot_id =
            externally_remove_current_definition_document(context, &rest_uri, "ns", "orders_mv")?;
        prepared.remove()?;
        context.action("verify an externally changed D stops the old refresh before commit");
        match held_refresh.recv_timeout(context.remaining("receive stale MV refresh")?) {
            Ok(Err(error)) if error.contains("reobserve Current MV publication documents") => {}
            Ok(Err(error)) => bail!("stale MV refresh failed for another reason: {error}"),
            Ok(Ok(())) => bail!("stale MV refresh published after D changed"),
            Err(error) => bail!("stale MV refresh did not finish: {error}"),
        }
        assert_rest_snapshot_unchanged(context, &rest_uri, "ns", "orders_mv", snapshot_id)?;
        context.action("Current D drift rejected without a second MV snapshot");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvLegacyInterpretationRebuild {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvLegacyInterpretationRebuild {
    fn name(&self) -> &'static str {
        "mv/legacy-interpretation-rebuild"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_legacy_interpretation")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_legacy_interpretation";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        let create = "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders";
        execute(context, &mut conn, "create an unpublished MV", create)?;
        externally_persist_legacy_nonaggregate_interpretation(
            context,
            &rest_uri,
            "ns",
            "orders_mv",
        )?;
        drop(conn);

        restart_frontend(
            context,
            "restart FE over a retired nonaggregate identity interpretation",
        )?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        context.action("legacy nonaggregate L must reject incremental and full refresh");
        for sql in [
            "REFRESH MATERIALIZED VIEW orders_mv",
            "REFRESH MATERIALIZED VIEW orders_mv FULL",
        ] {
            let error = conn
                .query_drop(sql)
                .expect_err("legacy persisted row identities must fail closed")
                .to_string();
            if !error.contains("legacy nonaggregate MV interpretation") || !error.contains("DROP") {
                bail!("legacy L refresh failed for another reason: {error}");
            }
        }
        assert_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 0)?;

        let drop_error = conn
            .query_drop("DROP MATERIALIZED VIEW orders_mv")
            .expect_err("old incarnation requires an explicit management declaration before DROP")
            .to_string();
        if !drop_error.contains("resume") && !drop_error.contains("Resume") {
            bail!(
                "legacy DROP preflight failed without a management readmission requirement: {drop_error}"
            );
        }
        assert_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 0)?;
        resume_management_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
        // Readmission settles the old incarnation's catalog/object effects.
        // It permits DROP only; it does not rehabilitate the retired L.
        for sql in [
            "REFRESH MATERIALIZED VIEW orders_mv",
            "REFRESH MATERIALIZED VIEW orders_mv FULL",
        ] {
            let error = conn
                .query_drop(sql)
                .expect_err("readmission must not rehabilitate legacy L")
                .to_string();
            if !error.contains("legacy nonaggregate MV interpretation") {
                bail!("readmitted legacy L failed for another reason: {error}");
            }
        }
        execute(
            context,
            &mut conn,
            "drop the old-format MV",
            "DROP MATERIALIZED VIEW orders_mv",
        )?;
        execute(
            context,
            &mut conn,
            "recreate the MV using current L format",
            create,
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 1)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read the rebuilt MV",
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvInvalidRestart {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvInvalidRestart {
    fn name(&self) -> &'static str {
        "mv/invalid-restart"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_invalid_restart")?;
        *self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))? = Some(fixture);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_invalid_restart";
        let (create_catalog, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot.as_ref().context("managed MV fixture is missing")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog, true)?;
        execute(
            context,
            &mut conn,
            "create baseline for physical corruption fault",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        require_eligibility(context, &mut conn, "orders_mv", "ELIGIBLE")?;
        let baseline = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
        context.action(
            "replace one visible tuple in the private target Parquet without changing any metadata",
        );
        let evidence = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            slot.as_ref()
                .context("managed MV fixture is missing")?
                .corrupt_private_visible_tuple(
                    "ns",
                    "orders_mv",
                    context.remaining("apply bounded private target physical corruption")?,
                )?
        };
        context.action(evidence);
        if lake_validation_baseline(context, &rest_uri, "orders_mv")? != baseline {
            bail!("physical corruption altered S/P/E metadata");
        }
        execute(
            context,
            &mut conn,
            "produce a real negative delta for the physically missing target tuple",
            "DELETE FROM orders WHERE k1 = 1",
        )?;
        conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
            .expect_err("complete matching must refuse the physically missing target occurrence");
        let invalid = require_invalid_deficit(context, &mut conn)?;
        let generation = nullable_mv_status_string(&invalid, "EligibilityGeneration")?
            .context("Invalid generation absent")?;
        let invalid_baseline = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
        if invalid_baseline.snapshot != baseline.snapshot
            || invalid_baseline.publication != baseline.publication
            || invalid_baseline.eligibility == baseline.eligibility
        {
            bail!(
                "completed deficit did not preserve data/progress and persist its invalidity conclusion"
            );
        }
        assert_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 1)?;
        drop(conn);
        restart_frontend(
            context,
            "restart FE over the durable complete deficit conclusion",
        )?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        resume_management_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
        let recovered = require_invalid_deficit(context, &mut conn)?;
        if nullable_mv_status_string(&recovered, "EligibilityGeneration")?.as_deref()
            != Some(generation.as_str())
            || lake_validation_baseline(context, &rest_uri, "orders_mv")? != invalid_baseline
        {
            bail!("FE readmission changed the exact durable Invalid conclusion");
        }
        execute(
            context,
            &mut conn,
            "source progress cannot clear proved invalidity",
            "INSERT INTO orders VALUES (3, 30)",
        )?;
        conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
            .expect_err("ordinary refresh must not rehabilitate Invalid");
        require_invalid_deficit(context, &mut conn)?;
        if lake_validation_baseline(context, &rest_uri, "orders_mv")? != invalid_baseline {
            bail!("source progress or rejected refresh cleared durable Invalid");
        }
        execute(
            context,
            &mut conn,
            "explicit full rebuild replaces physically corrupted target",
            "REFRESH MATERIALIZED VIEW orders_mv FULL WITH SYNC MODE",
        )?;
        require_eligibility(context, &mut conn, "orders_mv", "ELIGIBLE")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(2, 20), (3, 30)],
            "read full rebuild after durable Invalid and FE replacement",
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        if let Some(mut fixture) = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take()
        {
            fixture.shutdown()?;
        }
        Ok(())
    }
}

fn require_invalid_deficit(context: &mut ScenarioContext, conn: &mut Conn) -> Result<Row> {
    let row = require_eligibility(context, conn, "orders_mv", "INVALID")?;
    if nullable_mv_status_string(&row, "EligibilityRequested")?.as_deref() != Some("1")
        || nullable_mv_status_string(&row, "EligibilityMatched")?.as_deref() != Some("0")
    {
        bail!("Invalid lacks exact complete requested=1/matched=0 evidence");
    }
    Ok(row)
}

/// Uses a dedicated FE budget rather than a synthetic capacity error.
#[derive(Default)]
struct MvCapacityStop {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvCapacityStop {
    fn name(&self) -> &'static str {
        "mv/capacity-stop"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_capacity_stop")?;
        *self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))? = Some(fixture);
        let overlay = launch.config_overlay.fe.get_or_insert_with(String::new);
        overlay.push_str(
            r#"
[runtime]
optimizer_query_mem_limit_bytes = 524288
[standalone_server]
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1000
"#,
        );
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_capacity_stop";
        let (create_catalog, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot.as_ref().context("managed MV fixture is missing")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog, false)?;
        execute(
            context,
            &mut conn,
            "seed distinct visible tuples exceeding the exact quota budget",
            "INSERT INTO orders SELECT CAST(number AS INT), CAST(number * 10 AS BIGINT) FROM TABLE(generate_series(0, 32768)) t(number)",
        )?;
        execute(
            context,
            &mut conn,
            "create capacity baseline without quota application",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        require_eligibility(context, &mut conn, "orders_mv", "ELIGIBLE")?;
        let baseline = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create actual large negative demand",
            "DELETE FROM orders WHERE k1 > 0",
        )?;
        execute(
            context,
            &mut conn,
            "enable real automatic quota application",
            "ALTER MATERIALIZED VIEW orders_mv SET REFRESH ASYNC EVERY INTERVAL 1 SECOND",
        )?;
        context.action("wait for typed capacity refusal from the real automatic refresh");
        let stopped = loop {
            let row = eligibility_row(context, &mut conn, "orders_mv")?;
            if nullable_mv_status_string(&row, "AutomaticRefreshStopReason")?.as_deref()
                == Some("CAPACITY_REFUSED")
            {
                break row;
            }
            context.remaining("wait for automatic capacity stop")?;
            thread::sleep(POLL_INTERVAL);
        };
        let state = nullable_mv_status_string(&stopped, "EligibilityState")?
            .context("capacity eligibility is missing")?;
        if !matches!(state.as_str(), "ELIGIBLE" | "VALIDATION_PENDING") {
            bail!("capacity stop invented a completed-deficit conclusion: {state}");
        }
        let stopped_baseline = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
        if stopped_baseline.snapshot != baseline.snapshot
            || stopped_baseline.publication != baseline.publication
        {
            bail!("capacity refusal published partial data or progress");
        }
        execute(
            context,
            &mut conn,
            "source progress must not clear a capacity stop",
            "INSERT INTO orders VALUES (40000, 400000)",
        )?;
        // Observe across multiple scheduler opportunities, without rewriting
        // the stop or clearing its retained typed cause in the test owner.
        let observation_end = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < observation_end {
            context.remaining("observe capacity stop after source progress")?;
            let row = eligibility_row(context, &mut conn, "orders_mv")?;
            if nullable_mv_status_string(&row, "AutomaticRefreshStopReason")?.as_deref()
                != Some("CAPACITY_REFUSED")
                || nullable_mv_status_string(&row, "EligibilityState")?.as_deref()
                    != Some(state.as_str())
            {
                bail!("source progress cleared the retained capacity stop or eligibility fence");
            }
            if lake_validation_baseline(context, &rest_uri, "orders_mv")? != stopped_baseline {
                bail!("stopped automatic refresh changed data, progress or eligibility");
            }
            thread::sleep(POLL_INTERVAL);
        }
        execute(
            context,
            &mut conn,
            "explicit full rebuild restores the stopped MV",
            "REFRESH MATERIALIZED VIEW orders_mv FULL WITH SYNC MODE",
        )?;
        let recovered = require_eligibility(context, &mut conn, "orders_mv", "ELIGIBLE")?;
        if nullable_mv_status_string(&recovered, "AutomaticRefreshStopReason")?.is_some() {
            bail!("successful manual full refresh did not clear the capacity stop");
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(0, 0), (40000, 400000)],
            "read full rebuild after real quota capacity refusal",
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        if let Some(mut fixture) = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take()
        {
            fixture.shutdown()?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct MvCommitResponseLoss {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvCommitResponseLoss {
    fn name(&self) -> &'static str {
        "mv/commit-response-loss-reconciliation"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-commit-response-faults");
        fs::create_dir_all(&fault_dir)?;
        let (fixture, mut launch) = ManagedMvRestFixture::start_with_catalog_proxy(
            scenario_root,
            "system_mv_response_loss",
        )?;
        *self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))? = Some(fixture);
        launch.child_environment.fe.insert(
            "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR".into(),
            fault_dir.to_string_lossy().into_owned(),
        );
        // The refresh holds one blocking worker at the runner barrier;
        // independent SHOW observations need a second ordinary worker.
        let mut fe_overlay = launch.config_overlay.fe.take().unwrap_or_default();
        fe_overlay.push_str("\n[runtime]\nquery_blocking_worker_threads = 2\n");
        launch.config_overlay.fe = Some(fe_overlay);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_response_loss";
        let (create_catalog, rest_uri, proxy) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot.as_ref().context("managed MV fixture is missing")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
                fixture.catalog_proxy_control(context.deadline())?,
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog, true)?;
        execute(
            context,
            &mut conn,
            "create MV for real commit response loss",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        let baseline = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
        assert_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 1)?;
        execute(
            context,
            &mut conn,
            "produce the negative tuple whose commit reply will be lost",
            "DELETE FROM orders WHERE k1 = 1",
        )?;
        let hold = FileTrigger::create(
            &context
                .scenario_root()
                .join("mv-commit-response-faults/mv-refresh-at-data-prepared.trigger"),
            "token=before-success-response-loss\n",
        )?;
        let pending_refresh = spawn_refresh(
            context.mysql_user().to_owned(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.remaining("start real response-loss refresh")?,
        );
        wait_for_fe_marker(
            context,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=data-prepared token=before-success-response-loss",
            "wait for full matching before arming the commit response fault",
        )?;
        require_eligibility(context, &mut conn, "orders_mv", "VALIDATION_PENDING")?;
        if let Ok(result) = pending_refresh.try_recv() {
            bail!("refresh exited before the runner armed commit response loss: {result:?}");
        }
        let before = proxy.successful_table_commits("ns", "orders_mv");
        let fault = proxy.arm_next_table_commit("ns", "orders_mv")?;
        hold.remove()?;
        match pending_refresh
            .recv_timeout(context.remaining("wait for exact provider reconciliation")?)
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => bail!("committed MV response loss did not reconcile: {error}"),
            Err(error) => bail!("MV response-loss refresh did not finish: {error}"),
        }
        let evidence = fault.finish(context.deadline())?;
        context.action(format!(
            "actual downstream commit response loss: {evidence}"
        ));
        if proxy.successful_table_commits("ns", "orders_mv") != before + 1 {
            bail!("response loss dispatched more than one successful MV commit");
        }
        assert_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 2)?;
        let committed = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
        if committed.snapshot == baseline.snapshot
            || committed.publication == baseline.publication
            || committed.eligibility == baseline.eligibility
        {
            bail!("reconciled commit did not atomically advance data, progress and eligibility");
        }
        require_eligibility(context, &mut conn, "orders_mv", "ELIGIBLE")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(2, 20)],
            "read reconciled exact-once tuple deletion",
        )?;
        // A subsequent source delta must use the committed baseline rather
        // than applying the lost-response deletion a second time.
        execute(
            context,
            &mut conn,
            "advance source after reconciled response loss",
            "INSERT INTO orders VALUES (3, 30)",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        require_eligibility(context, &mut conn, "orders_mv", "ELIGIBLE")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(2, 20), (3, 30)],
            "subsequent refresh starts from the reconciled publication",
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        if let Some(mut fixture) = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take()
        {
            fixture.shutdown()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum ValidationRecoveryCase {
    FrontendCrash,
    BackendFailure,
}

struct MvValidationRecovery {
    case: ValidationRecoveryCase,
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl MvValidationRecovery {
    fn new(case: ValidationRecoveryCase) -> Self {
        Self {
            case,
            fixture: Mutex::default(),
        }
    }
    fn catalog(&self) -> &'static str {
        match self.case {
            ValidationRecoveryCase::FrontendCrash => "system_mv_pending_restart",
            ValidationRecoveryCase::BackendFailure => "system_mv_before_matching_failure",
        }
    }
}

impl Scenario for MvValidationRecovery {
    fn name(&self) -> &'static str {
        match self.case {
            ValidationRecoveryCase::FrontendCrash => "mv/validation-pending-restart",
            ValidationRecoveryCase::BackendFailure => "mv/before-matching-backend-failure",
        }
    }
    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-validation-faults");
        fs::create_dir_all(&fault_dir)?;
        let (fixture, mut launch) = ManagedMvRestFixture::start(scenario_root, self.catalog())?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture initialized twice");
        }
        *slot = Some(fixture);
        if matches!(self.case, ValidationRecoveryCase::FrontendCrash) {
            launch.child_environment.fe.insert(
                "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR".to_string(),
                fault_dir.to_string_lossy().into_owned(),
            );
        }
        // Backend failure arms must share the harness-owned FE/BE fault root:
        // FE binds the arm to the exact attempt before a BE can claim it.
        // The refresh holds one blocking worker at the runner barrier;
        // independent SHOW observations need a second ordinary worker.
        let mut fe_overlay = launch.config_overlay.fe.take().unwrap_or_default();
        fe_overlay.push_str("\n[runtime]\nquery_blocking_worker_threads = 2\n");
        launch.config_overlay.fe = Some(fe_overlay);
        Ok(launch)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = self.catalog();
        let (create_catalog, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot.as_ref().context("managed MV fixture is missing")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog, true)?;
        execute(
            context,
            &mut conn,
            "create visible tuple recovery MV",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        require_eligibility(context, &mut conn, "orders_mv", "ELIGIBLE")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "establish exact published baseline before recovery fault",
        )?;
        let baseline = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "produce an actual negative visible tuple delta",
            "DELETE FROM orders WHERE k1 = 1",
        )?;
        let validation_fault_dir = match self.case {
            ValidationRecoveryCase::FrontendCrash => {
                context.scenario_root().join("mv-validation-faults")
            }
            ValidationRecoveryCase::BackendFailure => context
                .handle()
                .runtime_dir()
                .join("query-lifecycle-faults"),
        };
        let trigger = FileTrigger::create(
            &validation_fault_dir.join("mv-refresh-at-validation-pending-saved.trigger"),
            "token=validation-pending-saved\n",
        )?;
        let pending_refresh = spawn_refresh(
            context.mysql_user().to_owned(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.remaining("start pending validation refresh")?,
        );
        wait_for_fe_marker(
            context,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=validation-pending-saved token=validation-pending-saved",
            "observe lake-confirmed pending fence before matching dispatch",
        )?;
        let pending = require_eligibility(context, &mut conn, "orders_mv", "VALIDATION_PENDING")?;
        let pending_attempt = pending
            .get::<String, _>("EligibilityAttempt")
            .context("pending attempt is missing")?;
        let pending_baseline = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
        if baseline.snapshot != pending_baseline.snapshot
            || baseline.publication != pending_baseline.publication
        {
            bail!("pending fence changed target data or publication");
        }
        if baseline.eligibility == pending_baseline.eligibility {
            bail!("pending fence did not change lake eligibility");
        }

        if let Ok(result) = pending_refresh.try_recv() {
            bail!("refresh exited before the runner injected the recovery fault: {result:?}");
        }
        match self.case {
            ValidationRecoveryCase::FrontendCrash => {
                drop(conn);
                context
                    .handle()
                    .kill_fe()
                    .context("kill FE after confirmed pending CAS")?;
                trigger.remove()?;
                expect_refresh_failure(
                    context,
                    pending_refresh,
                    "interrupted pending refresh must unwind",
                )?;
                restart_frontend(context, "restart FE with lake pending fence intact")?;
                conn = connect(context)?;
                select_catalog_and_database(context, &mut conn, catalog)?;
                resume_management_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
                let recovered =
                    require_eligibility(context, &mut conn, "orders_mv", "VALIDATION_PENDING")?;
                if recovered.get::<String, _>("EligibilityAttempt").as_deref()
                    != Some(pending_attempt.as_str())
                {
                    bail!("FE replacement changed the exact pending attempt");
                }
                if lake_validation_baseline(context, &rest_uri, "orders_mv")? != pending_baseline {
                    bail!(
                        "management readmission changed pending eligibility, data or publication"
                    );
                }
                execute(
                    context,
                    &mut conn,
                    "source progress must not clear pending",
                    "INSERT INTO orders VALUES (3, 30)",
                )?;
                let error = conn
                    .query_drop("REFRESH MATERIALIZED VIEW orders_mv")
                    .expect_err("pending baseline must reject ordinary refresh");
                if error.to_string().is_empty() {
                    bail!("pending rejection lacks diagnostics");
                }
                require_eligibility(context, &mut conn, "orders_mv", "VALIDATION_PENDING")?;
                if lake_validation_baseline(context, &rest_uri, "orders_mv")? != pending_baseline {
                    bail!("rejected refresh changed the lake pending fence or publication");
                }
            }
            ValidationRecoveryCase::BackendFailure => {
                let markers = total_be_marker_count(context, TASK_EXECUTION_FAILURE_MARKER)?;
                let backends = context.handle().be_count();
                for index in 0..backends {
                    context
                        .handle()
                        .arm_query_lifecycle_fault(index, TASK_EXECUTION_FAILURE)?;
                }
                trigger.remove()?;
                let result = expect_refresh_failure(
                    context,
                    pending_refresh,
                    "BE failure must fail the frozen matching attempt",
                );
                context.handle().clear_query_lifecycle_faults()?;
                result?;
                if total_be_marker_count(context, TASK_EXECUTION_FAILURE_MARKER)? <= markers {
                    bail!("refresh failed without the actual BE execution fault firing");
                }
                let recovered = eligibility_row(context, &mut conn, "orders_mv")?;
                let state = recovered
                    .get::<String, _>("EligibilityState")
                    .context("eligibility state is missing")?;
                let unchanged = lake_validation_baseline(context, &rest_uri, "orders_mv")?;
                if unchanged.snapshot != baseline.snapshot
                    || unchanged.publication != baseline.publication
                {
                    bail!("BE failure published partial target effects");
                }
                // Exact NotStarted receipts may recover; missing/unavailable
                // preparation facts remain Pending. Task failure is no proof.
                match state.as_str() {
                    "ELIGIBLE" => {
                        refresh(context, &mut conn, "orders_mv")?;
                        assert_rows(
                            context,
                            &mut conn,
                            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
                            &[(2, 20)],
                            "retry only after exact unstarted verification recovered eligibility",
                        )?;
                        return Ok(());
                    }
                    "VALIDATION_PENDING" => {
                        if recovered.get::<String, _>("EligibilityAttempt").as_deref()
                            != Some(pending_attempt.as_str())
                        {
                            bail!("BE failure changed its pending attempt identity");
                        }
                    }
                    other => bail!("BE failure invented a verification conclusion: {other}"),
                }
            }
        }
        execute(
            context,
            &mut conn,
            "explicit full rebuild may restore pending eligibility",
            "REFRESH MATERIALIZED VIEW orders_mv FULL WITH SYNC MODE",
        )?;
        require_eligibility(context, &mut conn, "orders_mv", "ELIGIBLE")?;
        let expected: &[(i32, i64)] = match self.case {
            ValidationRecoveryCase::FrontendCrash => &[(2, 20), (3, 30)],
            ValidationRecoveryCase::BackendFailure => &[(2, 20)],
        };
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            expected,
            "full rebuild atomically restores data, publication and eligibility",
        )?;
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        if let Some(mut fixture) = fixture {
            fixture.shutdown()?;
        }
        Ok(())
    }
}

// SQL NULL is an absent observation, not an invented status or counter.
fn nullable_mv_status_string(row: &Row, column: &str) -> Result<Option<String>> {
    row.get_opt::<Option<String>, _>(column)
        .with_context(|| format!("SHOW MV lacks column {column}"))?
        .with_context(|| format!("SHOW MV column {column} is not nullable text"))
}

fn eligibility_row(context: &mut ScenarioContext, conn: &mut Conn, mv: &str) -> Result<Row> {
    let rows: Vec<Row> = query(
        context,
        conn,
        "SHOW MATERIALIZED VIEWS FROM ns",
        "observe lake-backed maintenance eligibility",
    )?;
    for row in rows {
        if nullable_mv_status_string(&row, "Name")?.as_deref() == Some(mv) {
            return Ok(row);
        }
    }
    bail!("SHOW MATERIALIZED VIEWS omitted the recovery target")
}

fn require_eligibility(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    mv: &str,
    expected: &str,
) -> Result<Row> {
    let row = eligibility_row(context, conn, mv)?;
    let state = nullable_mv_status_string(&row, "EligibilityState")?
        .context("SHOW MV lacks eligibility state")?;
    if state != expected {
        bail!("MV eligibility is {state}, expected {expected}");
    }
    Ok(row)
}

#[derive(Debug, PartialEq, Eq)]
struct LakeValidationBaseline {
    snapshot: i64,
    publication: Vec<u8>,
    eligibility: Vec<u8>,
}

fn lake_validation_baseline(
    context: &ScenarioContext,
    rest_uri: &str,
    mv: &str,
) -> Result<LakeValidationBaseline> {
    let url = format!(
        "{}/v1/namespaces/ns/tables/{mv}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact lake validation baseline")?)
        .build()?
        .get(url)
        .send()?
        .error_for_status()?
        .json()?;
    validation_baseline_from_metadata(&loaded["metadata"])
}

fn validation_baseline_from_metadata(
    metadata: &serde_json::Value,
) -> Result<LakeValidationBaseline> {
    let snapshot = metadata["current-snapshot-id"]
        .as_i64()
        .context("target snapshot is missing")?;
    let current = metadata["snapshots"]
        .as_array()
        .context("target snapshots are missing")?
        .iter()
        .find(|entry| entry["snapshot-id"].as_i64() == Some(snapshot))
        .context("exact current target snapshot is missing")?;
    // P belongs to the exact committed output snapshot. E is independently
    // attached to current table metadata; they are different carrier domains.
    let revision =
        |properties: &serde_json::Value, name: &str, attachment: &str| -> Result<Vec<u8>> {
            let encoded = properties["novarocks.documents.v1"]
                .as_str()
                .with_context(|| format!("{name} document manifest is missing"))?;
            let manifest: serde_json::Value = serde_json::from_str(encoded)?;
            if manifest["version"].as_u64() != Some(1) {
                bail!("{name} document manifest has an unsupported version");
            }
            let documents = manifest["documents"]
                .as_array()
                .context("lake manifest document list is missing")?;
            let mut matching = documents
                .iter()
                .filter(|doc| doc["owner"] == "novarocks.mv" && doc["name"] == name);
            let document = matching
                .next()
                .with_context(|| format!("lake manifest lacks {name}"))?;
            if matching.next().is_some()
                || document["attachment"]["kind"].as_str() != Some(attachment)
            {
                bail!("{name} attachment is ambiguous or belongs to another carrier domain");
            }
            if attachment == "exact-output"
                && document["attachment"]["snapshot_id"].as_i64() != Some(snapshot)
            {
                bail!("publication does not bind the exact current output");
            }
            let revision: Vec<u8> = serde_json::from_value(document["revision"].clone())?;
            if revision.len() != 32 {
                bail!("{name} revision is not exact SHA-256");
            }
            Ok(revision)
        };
    Ok(LakeValidationBaseline {
        snapshot,
        publication: revision(&current["summary"], "publication", "exact-output")?,
        eligibility: revision(&metadata["properties"], "eligibility", "table-metadata")?,
    })
}

#[derive(Default)]
struct MvStagedPublishedRecovery {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvStagedPublishedRecovery {
    fn name(&self) -> &'static str {
        "mv/staged-published-recovery"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir).with_context(|| {
            format!("create MV recovery fault directory {}", fault_dir.display())
        })?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_recovery")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        launch.child_environment.fe.insert(
            "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR".to_string(),
            fault_dir.to_string_lossy().into_owned(),
        );
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let catalog = "system_mv_recovery";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV for staged and published recovery",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;

        // A canonical publication is one commit: the rows and the publication
        // document land together, so there is no staged-but-unpublished state
        // to crash into. What this window still proves is that a crash between
        // that commit and the frontend's own record of it leaves the committed
        // publication visible and re-refreshable.
        let staged = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-write-committed.trigger"),
            "token=staged-before-publication\n",
        )?;
        context.action("armed committed-before-record crash barrier");
        let staged_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.remaining("start staged recovery refresh")?,
        );
        wait_for_fe_marker(
            context,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=write-committed token=staged-before-publication",
            "wait for committed-before-record barrier",
        )?;
        context.action("kill FE between the publication commit and its record");
        context
            .handle()
            .kill_fe()
            .context("kill FE at staged recovery barrier")?;
        staged.remove()?;
        expect_refresh_failure(
            context,
            staged_refresh,
            "staged refresh client after FE termination",
        )?;
        restart_frontend(context, "restart FE after committed-before-record crash")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify the committed publication survived the unrecorded crash",
        )?;
        resume_and_refresh_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify recovery re-refreshes onto the same published rows",
        )?;
        execute(
            context,
            &mut conn,
            "add source row before publication-committed crash",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let published = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-publication-committed.trigger"),
            "token=published-before-cleanup\n",
        )?;
        context.action("armed publication-committed crash barrier");
        let published_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.remaining("start publication recovery refresh")?,
        );
        wait_for_fe_marker(
            context,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=publication-committed token=published-before-cleanup",
            "wait for publication recovery barrier",
        )?;
        context.action("kill FE after MV main publication and before cleanup");
        context
            .handle()
            .kill_fe()
            .context("kill FE at publication recovery barrier")?;
        published.remove()?;
        expect_refresh_failure(
            context,
            published_refresh,
            "publication refresh client after FE termination",
        )?;
        restart_frontend(context, "restart FE after publication-committed crash")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "verify published snapshot remains visible after recovery",
        )?;
        resume_and_refresh_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
        context.action("staged and published crash windows converged through public MV behavior");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvFirstRefreshStaging {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvFirstRefreshStaging {
    fn name(&self) -> &'static str {
        "mv/first-refresh-staging"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) = ManagedMvRestFixture::start(scenario_root, "system_mv_staging")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_staging";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;

        execute(
            context,
            &mut conn,
            "create first-refresh projection MV",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify staged projection first refresh publishes only completed main snapshot",
        )?;

        execute(
            context,
            &mut conn,
            "create first-refresh aggregate MV",
            "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
        )?;
        refresh(context, &mut conn, "orders_agg_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total_v2 FROM orders_agg_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify staged aggregate first refresh",
        )?;

        execute(
            context,
            &mut conn,
            "create MV used to prove failed first refresh is not published",
            "CREATE MATERIALIZED VIEW orders_start_fault_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        // The refresh has to fail from inside a task that was really admitted
        // and really started, because that is what leaves a staged main
        // snapshot behind for the publication rule to discard. A refused
        // admission would prove nothing about staging. The fault fires on the
        // backend it is armed for; the MV's base table is a connector scan, so
        // its scan fragment is placed on every live backend and that backend
        // does get a task.
        let injected_baseline = total_be_marker_count(context, TASK_EXECUTION_FAILURE_MARKER)?;
        context
            .handle()
            .arm_query_lifecycle_fault(0, TASK_EXECUTION_FAILURE)?;
        context.action("armed an injected task execution failure for MV first refresh");
        let refresh_result = conn.query_drop("REFRESH MATERIALIZED VIEW orders_start_fault_mv");
        let cleanup_result = context.handle().clear_query_lifecycle_faults();
        cleanup_result.context("clear injected task execution failure")?;
        let error = refresh_result
            .expect_err("an injected task execution failure must fail the MV first refresh");
        if error.to_string().is_empty() {
            bail!("injected task execution failure returned an empty MV refresh error");
        }
        // Without this the remaining assertions would also hold for a refresh
        // that failed for an unrelated reason, or for one that never ran a
        // distributed task at all.
        let injected = total_be_marker_count(context, TASK_EXECUTION_FAILURE_MARKER)?;
        if injected <= injected_baseline {
            bail!(
                "MV first refresh failed without the injected task execution failure firing; \
                 expected a new {TASK_EXECUTION_FAILURE_MARKER} across the BE logs, \
                 count stayed at {injected_baseline}"
            );
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_start_fault_mv ORDER BY k1",
            &[],
            "verify failed first refresh never publishes a partial main snapshot",
        )?;
        context.action("validated native first-refresh staging publishes no partial main snapshot");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvBaseIdentityReplacement {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvBaseIdentityReplacement {
    fn name(&self) -> &'static str {
        "mv/base-identity-replacement"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_base_identity")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_base_identity";
        let (create_catalog_sql, rest_uri) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = fixture
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV with a durable base-object binding",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read publication before replacing its base table",
        )?;

        drop(conn);
        externally_drop_rest_table(context, &rest_uri, "ns", "orders")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        execute(
            context,
            &mut conn,
            "recreate base table under the same logical name",
            "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
        )?;
        execute(
            context,
            &mut conn,
            "seed replacement base table incarnation",
            "INSERT INTO orders VALUES (9, 90)",
        )?;
        drop(conn);

        restart_frontend(context, "restart FE after same-name base replacement")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_mv_quarantined_after_base_replacement(context, &mut conn, catalog, "orders_mv")?;
        context.action(
            "verified FE restart quarantines the MV rather than bind a same-name replacement base",
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvLakePublicationRestartRebuild {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvLakePublicationRestartRebuild {
    fn name(&self) -> &'static str {
        "mv/lake-publication-restart-rebuild"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_lake_rebuild")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        launch.child_environment.fe.insert(
            "NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD".to_string(),
            "1".to_string(),
        );
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_lake_rebuild";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV with canonical lake documents",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read newly published lake-native MV before FE restart",
        )?;
        let rows: Vec<Row> = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => 'ns.orders_mv', level => 'provenance')"
            ),
            "confirm the published MV has exact lake documents",
        )?;
        let report = rows
            .first()
            .context("lake document observation returned no report row")?;
        let level = report
            .get::<String, _>(0)
            .context("lake document observation AvailableLevel column")?;
        let source = report
            .get::<String, _>(4)
            .context("lake document observation RebuildSource column")?;
        if level != "provenance" || source != "lake-documents" {
            bail!(
                "unexpected lake document report level={level:?}, source={source:?}; {}",
                context.diagnostics()
            );
        }
        let wiped: Vec<Row> = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => 'ns.orders_mv', level => 'wipe')"
            ),
            "wipe only the MV Accelerator after proving lake documents",
        )?;
        let wipe_report = wiped
            .first()
            .context("MV Accelerator wipe returned no report row")?;
        if wipe_report.get::<String, _>(0).as_deref() != Some("wipe")
            || wipe_report.get::<String, _>(4).as_deref() != Some("accelerator-wiped")
        {
            bail!("unexpected MV Accelerator wipe report: {wipe_report:?}");
        }
        drop(conn);
        restart_frontend(context, "restart FE after MV Accelerator wipe")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read MV rediscovered from canonical lake publication",
        )?;
        resume_and_refresh_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
        context.action("verified MV wipe, restart, readmission and refresh from lake documents");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

/// Counts one marker across every backend's log.
///
/// A single injected failure happens on one backend, so the sum is the honest
/// quantity here: counting the number of distinct backends that saw it would
/// assert a fanout the injection never claims.
fn total_be_marker_count(context: &mut ScenarioContext, marker: &str) -> Result<usize> {
    let be_count = context.handle().be_count();
    let mut total = 0;
    for index in 0..be_count {
        total += context
            .handle()
            .be_log_count(index, marker)
            .with_context(|| format!("count {marker} in BE[{index}] log"))?;
    }
    Ok(total)
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let be_count = context.handle().be_count();
    if be_count != 3 {
        bail!(
            "{} requires native 1FE+3BE, but runner launched {} BE(s)",
            context.name(),
            be_count
        );
    }
    context.action("confirmed native 1FE+3BE topology");
    Ok(())
}

fn connect(context: &mut ScenarioContext) -> Result<Conn> {
    let timeout = context.remaining("connect MySQL client")?;
    context.action("connect through public MySQL protocol");
    mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)
}

fn setup_orders_fixture_rest(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    create_catalog_sql: &str,
    seed_rows: bool,
) -> Result<()> {
    execute(
        context,
        conn,
        "create private REST Iceberg catalog",
        create_catalog_sql,
    )?;
    execute(
        context,
        conn,
        "create MV fixture namespace",
        &format!("CREATE DATABASE {catalog}.ns"),
    )?;
    select_catalog_and_database(context, conn, catalog)?;
    execute(
        context,
        conn,
        "create MV source table",
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )?;
    if seed_rows {
        execute(
            context,
            conn,
            "seed MV source table",
            "INSERT INTO orders VALUES (1, 10), (2, 20)",
        )?;
    }
    Ok(())
}

fn select_catalog_and_database(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "select MV catalog",
        &format!("SET CATALOG {catalog}"),
    )?;
    execute(context, conn, "select MV namespace", "USE ns")
}

fn externally_drop_rest_table(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    context.action("drop original base table through the private Iceberg REST catalog");
    Client::builder()
        .no_proxy()
        .timeout(context.remaining("drop base table through external REST catalog")?)
        .build()
        .context("build external REST catalog client")?
        .delete(&url)
        .send()
        .with_context(|| format!("delete original base table at {url}"))?
        .error_for_status()
        .with_context(|| {
            format!("REST catalog rejected deletion of original base table at {url}")
        })?;
    Ok(())
}

/// An unsupported external metadata writer removes only D from the target's
/// table-level manifest. It leaves `main` untouched, so physical snapshot OCC
/// cannot stand in for the frontend's post-compute D/L/P reobservation.
fn externally_remove_current_definition_document(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<i64> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let client = Client::builder()
        .no_proxy()
        .timeout(context.remaining("mutate Current D through external REST")?)
        .build()?;
    let loaded: serde_json::Value = client.get(&url).send()?.error_for_status()?.json()?;
    let metadata = loaded
        .get("metadata")
        .context("REST table load has no metadata")?;
    let table_uuid = metadata
        .get("table-uuid")
        .and_then(serde_json::Value::as_str)
        .context("REST table load has no UUID")?;
    let snapshot_id = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .context("REST table load has no current MV snapshot")?;
    let encoded = metadata
        .get("properties")
        .and_then(|properties| properties.get("novarocks.documents.v1"))
        .and_then(serde_json::Value::as_str)
        .context("REST table load has no MV document manifest")?;
    let mut manifest: serde_json::Value = serde_json::from_str(encoded)?;
    let documents = manifest
        .get_mut("documents")
        .and_then(serde_json::Value::as_array_mut)
        .context("MV document manifest has no document array")?;
    let before = documents.len();
    documents.retain(|document| {
        document.get("owner").and_then(serde_json::Value::as_str) != Some("novarocks.mv")
            || document.get("name").and_then(serde_json::Value::as_str) != Some("definition")
    });
    if documents.len() + 1 != before {
        bail!("external D mutation did not remove exactly one definition document");
    }
    context.action("commit external table-metadata D removal without changing main");
    let update = serde_json::json!({
        "requirements": [
            {"type": "assert-table-uuid", "uuid": table_uuid},
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": snapshot_id}
        ],
        "updates": [{
            "action": "set-properties",
            "updates": {"novarocks.documents.v1": manifest.to_string()}
        }]
    });
    client.post(&url).json(&update).send()?.error_for_status()?;
    assert_rest_snapshot_unchanged(context, rest_uri, namespace, table, snapshot_id)?;
    Ok(snapshot_id)
}

/// Persist the former nonaggregate row-identity layout on an unpublished
/// target. Its physical schema and L refer to the same exact field generation;
/// only the retired interpretation prevents refresh admission.
fn externally_persist_legacy_nonaggregate_interpretation(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let client = Client::builder()
        .no_proxy()
        .timeout(context.remaining("construct old-format L in private REST")?)
        .build()?;
    let loaded: serde_json::Value = client.get(&url).send()?.error_for_status()?.json()?;
    let metadata = loaded
        .get("metadata")
        .context("REST table load has no metadata")?;
    if metadata["snapshots"]
        .as_array()
        .is_some_and(|snapshots| !snapshots.is_empty())
    {
        bail!("old-format L fixture requires an unpublished MV target");
    }
    let table_uuid = metadata["table-uuid"]
        .as_str()
        .context("REST MV metadata has no table UUID")?;
    let current_schema_id = metadata["current-schema-id"]
        .as_i64()
        .context("REST MV metadata has no current schema ID")?;
    let next_schema_id = current_schema_id
        .checked_add(1)
        .context("REST MV schema ID overflow")?;
    if next_schema_id <= 0 {
        bail!("old-format L fixture requires a positive physical schema ID");
    }
    let last_column_id = metadata["last-column-id"]
        .as_i64()
        .context("REST MV metadata has no last column ID")?;
    let legacy_field_id = last_column_id
        .checked_add(1)
        .context("legacy field ID overflow")?;
    let legacy_field_identity = i32::try_from(legacy_field_id)?.to_le_bytes();
    let mut new_schema = metadata["schemas"]
        .as_array()
        .context("REST MV metadata has no schemas")?
        .iter()
        .find(|schema| schema["schema-id"].as_i64() == Some(current_schema_id))
        .cloned()
        .context("REST MV metadata has no current schema")?;
    new_schema["schema-id"] = serde_json::json!(next_schema_id);
    let fields = new_schema["fields"]
        .as_array_mut()
        .context("REST MV schema has no fields")?;
    if fields.len() < 2 {
        bail!("old-format L fixture requires two target fields to reorder");
    }
    fields.push(serde_json::json!({
        "id": legacy_field_id,
        "name": "__nova_base_row_id",
        "required": true,
        "type": "long"
    }));
    let encoded = metadata["properties"]["novarocks.documents.v1"]
        .as_str()
        .context("REST MV metadata has no table document manifest")?;
    let mut manifest: serde_json::Value = serde_json::from_str(encoded)?;
    let documents = manifest["documents"]
        .as_array_mut()
        .context("REST MV document manifest has no documents")?;
    let interpretation = documents
        .iter_mut()
        .find(|document| {
            document["owner"] == "novarocks.mv" && document["name"] == "interpretation"
        })
        .context("REST MV manifest has no L")?;
    if interpretation["carrier"]["kind"] != "available" {
        bail!("old-format L fixture requires an inline interpretation document");
    }
    let mut content: Vec<u8> =
        serde_json::from_value(interpretation["carrier"]["content"].clone())?;
    let target = protobuf_bytes_field(&content, 9)?;
    let schema = protobuf_bytes_field(&content[target.clone()], 2)?;
    if schema.len() != 4 {
        bail!("old-format L fixture expected a four-byte schema version");
    }
    let schema = target.start + schema.start..target.start + schema.end;
    let original_id = i32::from_le_bytes(content[schema.clone()].try_into()?);
    if i64::from(original_id) != current_schema_id || next_schema_id > i64::from(i32::MAX) {
        bail!("old-format L fixture schema version does not match the exact physical target");
    }
    content[schema].copy_from_slice(&(next_schema_id as i32).to_le_bytes());
    let logical_id = Sha256::digest(b"system-test-legacy-base-row-id");
    let mut component = protobuf_encode_bytes(1, logical_id.as_slice());
    component.extend(protobuf_encode_bytes(2, &legacy_field_identity));
    // Retired BaseRowId = 1. Preserve an actual former apply-key component
    // and its corresponding hidden physical target field, not malformed bytes.
    let mut key = vec![0x08, 0x01];
    key.extend(protobuf_encode_bytes(2, &component));
    let mut physical = vec![0x08, 0x03];
    physical.extend(protobuf_encode_bytes(2, logical_id.as_slice()));
    physical.extend(protobuf_encode_bytes(3, &legacy_field_identity));
    physical.extend(protobuf_encode_bytes(4, b"bigint"));
    physical.extend([0x28, 0x00]);
    let mut target_content = content[target.clone()].to_vec();
    target_content.extend(protobuf_encode_bytes(4, &physical));
    let target_field = protobuf_field_span(&content, 9)?;
    content.splice(
        target_field.clone(),
        protobuf_encode_bytes(9, &target_content),
    );
    content.splice(
        target_field.start..target_field.start,
        protobuf_encode_bytes(6, &key),
    );
    interpretation["carrier"]["content"] = serde_json::to_value(&content)?;
    interpretation["encoded_len"] = serde_json::json!(content.len());
    interpretation["revision"] =
        serde_json::to_value(Vec::from(Sha256::digest(&content).as_slice()))?;
    context.action("persist retired BaseRowId L with an exact hidden physical identity field");
    let update = serde_json::json!({
        "requirements": [
            {"type": "assert-table-uuid", "uuid": table_uuid},
            {"type": "assert-current-schema-id", "current-schema-id": current_schema_id},
            {"type": "assert-last-assigned-field-id", "last-assigned-field-id": last_column_id}
        ],
        "updates": [
            {"action": "add-schema", "schema": new_schema, "last-column-id": legacy_field_id},
            {"action": "set-current-schema", "schema-id": -1},
            {"action": "set-properties", "updates": {"novarocks.documents.v1": manifest.to_string()}}
        ]
    });
    let response = client.post(&url).json(&update).send()?;
    if !response.status().is_success() {
        bail!(
            "old-format L REST mutation failed: {} {}",
            response.status(),
            response.text()?
        );
    }
    let observed: serde_json::Value = client.get(&url).send()?.error_for_status()?.json()?;
    if observed["metadata"]["current-schema-id"].as_i64() != Some(next_schema_id) {
        bail!("old-format L REST mutation did not advance the physical schema ID");
    }
    let persisted = observed["metadata"]["properties"]["novarocks.documents.v1"]
        .as_str()
        .context("old-format L REST mutation lost the document manifest")?;
    let persisted: serde_json::Value = serde_json::from_str(persisted)?;
    let persisted_l = persisted["documents"]
        .as_array()
        .context("old-format L REST mutation lost the document list")?
        .iter()
        .find(|document| {
            document["owner"] == "novarocks.mv" && document["name"] == "interpretation"
        })
        .context("old-format L REST mutation lost L")?;
    let persisted_content: Vec<u8> =
        serde_json::from_value(persisted_l["carrier"]["content"].clone())?;
    if persisted_l["encoded_len"].as_u64() != Some(persisted_content.len() as u64)
        || persisted_l["revision"]
            != serde_json::to_value(Vec::from(Sha256::digest(&persisted_content).as_slice()))?
    {
        bail!("legacy L REST mutation did not retain its exact content envelope");
    }
    let target = protobuf_bytes_field(&persisted_content, 9)?;
    let schema = protobuf_bytes_field(&persisted_content[target.clone()], 2)?;
    let persisted_schema = target.start + schema.start..target.start + schema.end;
    if persisted_content[persisted_schema] != (next_schema_id as i32).to_le_bytes() {
        bail!("legacy L REST mutation did not retain the exact schema generation");
    }
    if persisted_content[protobuf_bytes_field(&persisted_content, 6)?] != key {
        bail!("legacy L REST mutation did not retain its retired apply-key facts");
    }
    Ok(())
}

fn protobuf_encode_bytes(field: u64, bytes: &[u8]) -> Vec<u8> {
    fn varint(mut value: u64, output: &mut Vec<u8>) {
        while value >= 128 {
            output.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }
    let mut output = Vec::new();
    varint((field << 3) | 2, &mut output);
    varint(bytes.len() as u64, &mut output);
    output.extend_from_slice(bytes);
    output
}

fn protobuf_field_span(input: &[u8], expected: u64) -> Result<std::ops::Range<usize>> {
    let mut offset = 0usize;
    while offset < input.len() {
        let start = offset;
        let key = protobuf_varint(input, &mut offset)?;
        match key & 7 {
            0 => {
                protobuf_varint(input, &mut offset)?;
            }
            2 => {
                let length = usize::try_from(protobuf_varint(input, &mut offset)?)?;
                offset = offset
                    .checked_add(length)
                    .context("protobuf fixture length overflow")?;
                if offset > input.len() {
                    bail!("protobuf fixture exceeds its document");
                }
            }
            wire => bail!("unsupported protobuf fixture wire type {wire}"),
        }
        if key >> 3 == expected {
            return Ok(start..offset);
        }
    }
    bail!("protobuf fixture lacks field {expected}")
}

fn protobuf_bytes_field(input: &[u8], expected_field: u64) -> Result<std::ops::Range<usize>> {
    let mut offset = 0;
    let mut selected = None;
    while offset < input.len() {
        let key = protobuf_varint(input, &mut offset)?;
        let field = key >> 3;
        match key & 7 {
            0 => {
                protobuf_varint(input, &mut offset)?;
            }
            2 => {
                let len = usize::try_from(protobuf_varint(input, &mut offset)?)?;
                let end = offset
                    .checked_add(len)
                    .context("protobuf field length overflow")?;
                if end > input.len() {
                    bail!("protobuf field exceeds L document");
                }
                if field == expected_field {
                    if selected.replace(offset..end).is_some() {
                        bail!("L document repeats protobuf field {expected_field}");
                    }
                }
                offset = end;
            }
            wire => bail!("unsupported L fixture protobuf wire type {wire}"),
        }
    }
    selected.context("L document lacks its target schema field")
}

fn protobuf_varint(input: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..=63).step_by(7) {
        let byte = *input.get(*offset).context("truncated L protobuf varint")?;
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("L protobuf varint exceeds 64 bits")
}

fn assert_rest_snapshot_count(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
    expected: usize,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read old-format MV snapshot count")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let count = loaded["metadata"]["snapshots"]
        .as_array()
        .context("REST MV metadata has no snapshots")?
        .len();
    if count != expected {
        bail!("old-format MV has {count} snapshots, expected {expected}");
    }
    Ok(())
}

fn assert_rest_snapshot_unchanged(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
    expected_snapshot_id: i64,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact Current MV snapshots")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let metadata = loaded
        .get("metadata")
        .context("REST table load has no metadata")?;
    let current = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .context("REST table load has no current snapshot")?;
    let count = metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .context("REST table load has no snapshot list")?
        .len();
    if current != expected_snapshot_id || count != 1 {
        bail!(
            "stale MV refresh changed main or added a snapshot: current={current}, expected={expected_snapshot_id}, snapshots={count}"
        );
    }
    Ok(())
}

fn execute(context: &mut ScenarioContext, conn: &mut Conn, action: &str, sql: &str) -> Result<()> {
    context.remaining(action)?;
    context.action(action);
    conn.query_drop(sql)
        .with_context(|| format!("{action}: {sql}"))
}

fn query<T: FromRow>(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    sql: &str,
    action: &str,
) -> Result<Vec<T>> {
    context.remaining(action)?;
    context.action(action);
    conn.query(sql).with_context(|| format!("{action}: {sql}"))
}

fn refresh(context: &mut ScenarioContext, conn: &mut Conn, mv: &str) -> Result<()> {
    execute(
        context,
        conn,
        "refresh materialized view",
        &format!("REFRESH MATERIALIZED VIEW {mv}"),
    )
}

fn assert_mv_quarantined_after_base_replacement(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    context.remaining("verify MV is quarantined after same-name base replacement")?;
    context.action("verify MV is quarantined after same-name base replacement");
    let views: Vec<Row> = conn
        .query("SHOW MATERIALIZED VIEWS FROM ns")
        .context("list MVs after same-name base replacement")?;
    let manageability = views
        .iter()
        .find(|row| row.get::<String, _>("Name").as_deref() == Some(mv))
        .and_then(|row| row.get::<String, _>("Manageability"))
        .context("quarantined MV is missing from SHOW MATERIALIZED VIEWS")?;
    if !manageability.starts_with("UNAVAILABLE:")
        || !manageability
            .contains("published MV base occurrence 0 no longer resolves to its frozen object")
    {
        bail!(
            "same-name replacement MV is listed as {manageability:?}, expected source identity quarantine; {}",
            context.diagnostics()
        );
    }
    let closed = status(context, conn, catalog, mv)?;
    let challenge = property(&closed, "Challenge")?;
    let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
    context.remaining("reject readmission onto a same-name replacement base")?;
    context.action("reject readmission onto a same-name replacement base");
    let readmission_error = match conn.query_drop(format!(
        "CALL novarocks_mv_resume_management('{catalog}', 'ns', '{mv}', \
         '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
         'the system scenario replaced the declared frontend process before this statement')"
    )) {
        Err(error) => error,
        Ok(()) => bail!("a replacement base readmitted the old MV unexpectedly"),
    };
    if !readmission_error
        .to_string()
        .contains("same-name relation was rebuilt with a different object identity")
    {
        bail!("replacement-base readmission failed for another reason: {readmission_error}");
    }
    let error = match conn.query_drop(format!("REFRESH MATERIALIZED VIEW {mv}")) {
        Err(error) => error,
        Ok(()) => {
            bail!(
                "a quarantined MV accepted refresh unexpectedly; {}",
                context.diagnostics()
            )
        }
    };
    let message = error.to_string();
    if !message.contains("MV target requires a successful fresh Current observation") {
        bail!(
            "refresh after same-name base replacement returned unexpected error {message:?}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn assert_rows(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    sql: &str,
    expected: &[(i32, i64)],
    action: &str,
) -> Result<()> {
    let actual: Vec<(i32, i64)> = query(context, conn, sql, action)?;
    if actual != expected {
        bail!(
            "{action} returned {actual:?}, expected {expected:?}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn wait_for_rows(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    sql: &str,
    expected: &[(i32, i64)],
    action: &str,
) -> Result<()> {
    context.action(action);
    loop {
        let actual = conn.query::<(i32, i64), _>(sql);
        if let Ok(rows) = actual
            && rows == expected
        {
            return Ok(());
        }
        if context.remaining(action).is_err() {
            let observed = conn.query::<(i32, i64), _>(sql).ok();
            // Keep the controlled fixture result in the structured evidence so
            // an asynchronous refresh timeout remains diagnosable after the
            // harness redacts its full process-log diagnostic.
            context.action(format!(
                "{action} timed out with observed rows {observed:?}"
            ));
            bail!(
                "timed out waiting for {action}; expected={expected:?}; observed={observed:?}; {}",
                context.diagnostics()
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn restart_frontend(context: &mut ScenarioContext, action: &str) -> Result<()> {
    context.action(action);
    let deadline = context.deadline();
    let action = action.to_owned();
    context
        .handle()
        .restart_fe_until(deadline)
        .with_context(|| action)
}

fn wait_for_marker_count(
    context: &mut ScenarioContext,
    directory: &Path,
    expected: usize,
    action: &str,
) -> Result<()> {
    context.action(action);
    loop {
        if marker_count(directory)? >= expected {
            return Ok(());
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_file(context: &mut ScenarioContext, path: &Path, action: &str) -> Result<()> {
    context.action(action);
    while !path.exists() {
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn wait_for_file_or_query(
    context: &mut ScenarioContext,
    path: &Path,
    query: &Receiver<std::result::Result<Vec<(i32, i64)>, String>>,
    action: &str,
) -> Result<()> {
    context.action(action);
    while !path.exists() {
        match query.try_recv() {
            Ok(result) => bail!(
                "rewritten query completed before its completed-plan barrier: {result:?}; {}",
                context.diagnostics()
            ),
            Err(TryRecvError::Disconnected) => {
                bail!("rewritten query channel closed before its completed-plan barrier")
            }
            Err(TryRecvError::Empty) => {}
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn marker_count(directory: &Path) -> Result<usize> {
    let count = fs::read_dir(directory)
        .with_context(|| format!("read scheduler marker directory {}", directory.display()))?
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("mvx4-scheduler-admitted-")
        })
        .count();
    Ok(count)
}

fn clear_scheduler_markers(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read scheduler marker directory {}", directory.display()))?
    {
        let entry = entry
            .with_context(|| format!("read scheduler marker entry in {}", directory.display()))?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("mvx4-scheduler-admitted-")
        {
            fs::remove_file(entry.path()).with_context(|| {
                format!("remove stale scheduler marker {}", entry.path().display())
            })?;
        }
    }
    Ok(())
}

fn wait_for_fe_marker(context: &mut ScenarioContext, marker: &str, action: &str) -> Result<()> {
    context.action(action);
    loop {
        if context.handle().fe_log_contents()?.contains(marker) {
            return Ok(());
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn spawn_refresh(
    user: String,
    port: u16,
    catalog: &str,
    mv: &str,
    timeout: Duration,
) -> Receiver<std::result::Result<(), String>> {
    let catalog = catalog.to_string();
    let mv = mv.to_string();
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = (|| -> Result<()> {
            let mut conn = mysql_actor::connect(&user, port, timeout)?;
            conn.query_drop(format!("SET CATALOG {catalog}"))?;
            conn.query_drop("USE ns")?;
            conn.query_drop(format!("REFRESH MATERIALIZED VIEW {mv}"))?;
            Ok(())
        })()
        .map_err(|error| format!("{error:#}"));
        let _ = sender.send(result);
    });
    receiver
}

fn spawn_aggregate_query(
    user: String,
    port: u16,
    catalog: &str,
    timeout: Duration,
) -> Receiver<std::result::Result<Vec<(i32, i64)>, String>> {
    let catalog = catalog.to_string();
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = (|| -> Result<Vec<(i32, i64)>> {
            let mut conn = mysql_actor::connect(&user, port, timeout)?;
            conn.query_drop(format!("SET CATALOG {catalog}"))?;
            conn.query_drop("USE ns")?;
            conn.query("SELECT k1, SUM(v2) FROM orders GROUP BY k1 ORDER BY k1")
                .context("execute rewritten aggregate query")
        })()
        .map_err(|error| format!("{error:#}"));
        let _ = sender.send(result);
    });
    receiver
}

fn receive_aggregate_query(
    context: &mut ScenarioContext,
    receiver: Receiver<std::result::Result<Vec<(i32, i64)>, String>>,
    action: &str,
) -> Result<Vec<(i32, i64)>> {
    context.action(action);
    let timeout = context.remaining(action)?;
    match receiver.recv_timeout(timeout) {
        Ok(Ok(rows)) => Ok(rows),
        Ok(Err(error)) => bail!("{action} failed: {error}"),
        Err(error) => bail!("{action} did not finish before deadline: {error}"),
    }
}

fn expect_refresh_failure(
    context: &mut ScenarioContext,
    receiver: Receiver<std::result::Result<(), String>>,
    action: &str,
) -> Result<()> {
    context.action(action);
    let timeout = context.remaining(action)?;
    match receiver.recv_timeout(timeout) {
        Ok(Err(error)) if !error.is_empty() => Ok(()),
        Ok(Err(_)) => bail!("{action} returned an empty error"),
        Ok(Ok(())) => bail!("{action} unexpectedly succeeded"),
        Err(error) => bail!("{action} did not finish before deadline: {error}"),
    }
}

fn resume_and_refresh_after_fe_restart(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    resume_management_after_fe_restart(context, conn, catalog, mv)?;
    refresh(context, conn, mv)
}

fn resume_management_after_fe_restart(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    let closed = wait_for_status_phase(
        context,
        conn,
        catalog,
        mv,
        "AWAITING_EFFECT_SETTLEMENT",
        "wait for post-crash MV management to require effect settlement",
    )?;
    let challenge = property(&closed, "Challenge")?;
    let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
    context.action("declare the crashed FE isolated and resume exact MV management");
    let resumed: Vec<(String, Option<String>)> = conn
        .query(format!(
            "CALL novarocks_mv_resume_management('{catalog}', 'ns', '{mv}', \
             '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
             'the system scenario replaced the declared frontend process before this statement')"
        ))
        .context("resume managed MV after frontend replacement")?;
    if property(&resumed, "SettledEffects")? != "1" {
        bail!("frontend replacement did not settle the old FE effect");
    }
    Ok(())
}

struct FileTrigger {
    path: PathBuf,
    removed: bool,
}

impl FileTrigger {
    fn create(path: &Path, contents: &str) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create trigger directory {}", parent.display()))?;
        }
        fs::write(path, contents).with_context(|| format!("write trigger {}", path.display()))?;
        Ok(Self {
            path: path.to_owned(),
            removed: false,
        })
    }

    fn remove(mut self) -> Result<()> {
        remove_if_exists(&self.path)?;
        self.removed = true;
        Ok(())
    }
}

impl Drop for FileTrigger {
    fn drop(&mut self) {
        if !self.removed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove trigger {}", path.display())),
    }
}

#[cfg(test)]
mod validation_baseline_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn lake_validation_baseline_reads_p_from_exact_snapshot_and_e_from_metadata() {
        let p = json!({"owner":"novarocks.mv","name":"publication","revision":vec![1u8;32],"attachment":{"kind":"exact-output","snapshot_id":41}});
        let e = json!({"owner":"novarocks.mv","name":"eligibility","revision":vec![2u8;32],"attachment":{"kind":"table-metadata"}});
        let mut metadata = json!({"current-snapshot-id":41,"properties":{"novarocks.documents.v1":json!({"version":1,"documents":[e]}).to_string()},"snapshots":[{"snapshot-id":41,"summary":{"novarocks.documents.v1":json!({"version":1,"documents":[p]}).to_string()}}]});
        assert_eq!(
            validation_baseline_from_metadata(&metadata).unwrap(),
            LakeValidationBaseline {
                snapshot: 41,
                publication: vec![1u8; 32],
                eligibility: vec![2u8; 32]
            }
        );
        metadata["current-snapshot-id"] = json!(42);
        assert!(validation_baseline_from_metadata(&metadata).is_err());
    }
}

#[derive(Default)]
struct MvRecursiveTypeRestart {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}
impl Scenario for MvRecursiveTypeRestart {
    fn name(&self) -> &'static str {
        "mv/recursive-type-restart"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) = ManagedMvRestFixture::start(root, "system_mv_recursive")?;
        launch.child_environment.fe.insert(
            "NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD".into(),
            "1".into(),
        );
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("recursive fixture initialized twice");
        }
        *slot = Some(fixture);
        Ok(launch)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_recursive";
        let mut conn = connect(context)?;
        let call_spark = |context: &ScenarioContext,
                          stage: &str,
                          invocation: &str|
         -> Result<serde_json::Value> {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            // Execute on the exact isolated publication under the scenario deadline.
            // It creates/reaps one owned Spark job and returns one bounded parsed receipt.
            slot.as_ref()
                .context("recursive fixture missing")?
                .run_recursive_spark_until(
                    context.scenario_root(),
                    stage,
                    invocation,
                    context.deadline(),
                )
        };
        let create = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("recursive fixture missing")?
            .create_catalog_sql()
            .to_owned();
        execute(
            context,
            &mut conn,
            "create private recursive catalog",
            &create,
        )?;
        let initialized = call_spark(
            context,
            "initialize",
            "RecursiveTypeFixture.initialize(\"ns\")",
        )?;
        validate_recursive_receipt(&initialized, "initialize")?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        execute(
            context,
            &mut conn,
            "create recursive output MV",
            "CREATE MATERIALIZED VIEW recursive_mv DISTRIBUTED BY HASH(label) BUCKETS 3 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine'='iceberg') AS SELECT label,payload,ordered FROM recursive_source",
        )?;
        refresh(context, &mut conn, "recursive_mv")?;
        let initial = call_spark(
            context,
            "initial",
            "RecursiveTypeFixture.observe(\"ns\",\"initial\")",
        )?;
        validate_recursive_receipt(&initial, "initial")?;
        for (source_key, target_key) in [
            ("source_uuid", "source_uuid"),
            ("schema_id", "source_schema_id"),
            ("schema_json", "source_schema_json"),
            ("snapshot", "source_snapshot"),
            ("fields", "source_fields"),
        ] {
            if recursive_required(&initialized, source_key)?
                != recursive_required(&initial, target_key)?
            {
                bail!("initial target observation lost exact source identity/schema/frontier");
            }
        }
        let initial_native: Vec<Row> = query(
            context,
            &mut conn,
            "SELECT label,payload,ordered FROM recursive_mv",
            "freeze complete native recursive result before FE replacement",
        )?;
        if initial_native.len() != 6 {
            bail!("initial native recursive visible bag has wrong cardinality");
        }
        let initial_native = initial_native
            .into_iter()
            .map(Row::unwrap)
            .collect::<Vec<_>>();
        let target_uuid = recursive_uuid(&initial, "table_uuid")?;
        let rest_uri = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("recursive fixture missing")?
            .rest_uri()
            .to_owned();
        // Bounded REST GET compares exact D/L/P/E manifests
        // (P from exact current snapshot; D/L/E from table metadata), strict versions,
        // 32-byte revisions, attachment identity, table UUID and exact schema JSON.
        let before = recursive_lake_binding(context, &rest_uri, "recursive_mv", &target_uuid)?;
        let sdk_schema: serde_json::Value =
            serde_json::from_str(recursive_string(&initial, "schema_json", 256 * 1024)?)?;
        if recursive_required(&before, "snapshot")? != recursive_required(&initial, "snapshot")?
            || recursive_required(&before, "schema_id")?
                != recursive_required(&initial, "schema_id")?
            || recursive_required(&before, "schema")? != &sdk_schema
        {
            bail!("REST lake baseline differs from exact SDK snapshot/schema binding");
        }
        let proof: Vec<Row> = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => 'ns.recursive_mv', level => 'provenance')"
            ),
            "prove canonical recursive lake documents",
        )?;
        if proof.first().and_then(|r| r.get::<String, _>(0)).as_deref() != Some("provenance")
            || proof.first().and_then(|r| r.get::<String, _>(4)).as_deref()
                != Some("lake-documents")
        {
            bail!("recursive provenance is not authoritative lake documents");
        }
        let wiped: Vec<Row> = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => 'ns.recursive_mv', level => 'wipe')"
            ),
            "wipe MV Accelerator after lake document proof",
        )?;
        if wiped.first().and_then(|r| r.get::<String, _>(0)).as_deref() != Some("wipe")
            || wiped.first().and_then(|r| r.get::<String, _>(4)).as_deref()
                != Some("accelerator-wiped")
        {
            bail!("recursive Accelerator wipe was not confirmed");
        }
        let (fe_before, be_before) = context.process_launch_identities();
        let fe_before = fe_before.clone();
        let be_before = be_before.to_vec();
        drop(conn);
        restart_frontend(context, "restart FE after recursive Accelerator wipe")?;
        let (fe_after, be_after) = context.process_launch_identities();
        if *fe_after == fe_before || be_after != be_before.as_slice() {
            bail!("recursive recovery did not replace only the exact FE process");
        }
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        // Force native decoding/reading before any rebuilding refresh. The independent
        // Spark comparison alone would only establish external read compatibility.
        let rows: Vec<Row> = query(
            context,
            &mut conn,
            "SELECT label,payload,ordered FROM recursive_mv",
            "read recursive restored MV through native FE/BE",
        )?;
        if rows.len() != 6 {
            bail!("restored recursive visible bag has wrong cardinality");
        }
        let mut unmatched = initial_native.clone();
        for row in rows {
            let values = row.unwrap();
            let position = unmatched
                .iter()
                .position(|old| *old == values)
                .context("native recursive restart changed a complete visible row")?;
            unmatched.swap_remove(position);
        }
        if !unmatched.is_empty() {
            bail!("native recursive restart lost visible row multiplicity");
        }
        let after = recursive_lake_binding(context, &rest_uri, "recursive_mv", &target_uuid)?;
        if before != after {
            bail!("FE restart changed frozen D/L/P/E or provider schema binding");
        }
        let restored = call_spark(
            context,
            "restored",
            "RecursiveTypeFixture.observe(\"ns\",\"restored\")",
        )?;
        validate_recursive_receipt(&restored, "restored")?;
        for key in [
            "table_uuid",
            "source_uuid",
            "source_schema_json",
            "source_schema_id",
            "source_snapshot",
            "source_fields",
            "schema_json",
            "schema_id",
            "fields",
            "snapshot",
            "bag",
            "data_files",
            "delete_files",
            "summary",
        ] {
            if recursive_required(&initial, key)? != recursive_required(&restored, key)? {
                bail!("recursive restart changed exact receipt field {key}");
            }
        }
        resume_management_after_fe_restart(context, &mut conn, catalog, "recursive_mv")?;
        let changed = call_spark(context, "mutate", "RecursiveTypeFixture.mutate(\"ns\")")?;
        validate_recursive_receipt(&changed, "mutate")?;
        for key in ["source_uuid", "schema_id", "schema_json", "fields"] {
            if recursive_required(&initialized, key)? != recursive_required(&changed, key)? {
                bail!("source mutation replaced its exact schema/object");
            }
        }
        if recursive_required(&changed, "from_snapshot")?
            != recursive_required(&initialized, "snapshot")?
        {
            bail!("source mutation starts from another frozen endpoint");
        }
        refresh(context, &mut conn, "recursive_mv")?;
        let incremental = call_spark(
            context,
            "incremental",
            "RecursiveTypeFixture.observe(\"ns\",\"incremental\")",
        )?;
        validate_recursive_receipt(&incremental, "incremental")?;
        execute(
            context,
            &mut conn,
            "full recursive rebuild",
            "REFRESH MATERIALIZED VIEW recursive_mv FULL WITH SYNC MODE",
        )?;
        let full = call_spark(
            context,
            "full",
            "RecursiveTypeFixture.observe(\"ns\",\"full\")",
        )?;
        validate_recursive_receipt(&full, "full")?;
        for endpoint in [&incremental, &full] {
            for key in [
                "table_uuid",
                "source_uuid",
                "source_schema_json",
                "source_schema_id",
                "source_fields",
                "schema_json",
                "schema_id",
                "fields",
            ] {
                if recursive_required(&initial, key)? != recursive_required(endpoint, key)? {
                    bail!("refresh replaced exact recursive binding field {key}");
                }
            }
            if recursive_required(endpoint, "source_snapshot")?
                != recursive_required(&changed, "snapshot")?
                || recursive_required(endpoint, "bag")? != recursive_required(&changed, "bag")?
            {
                bail!("refresh is not the exact complete source endpoint");
            }
        }
        if recursive_required(&full, "snapshot")? == recursive_required(&incremental, "snapshot")? {
            bail!("FULL did not create its exact replacement snapshot");
        }
        context.action("verified recursive provider identities, complete independent ordered bags and lake-only recovery on native 1FE+3BE");
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        if let Some(mut fixture) = fixture {
            fixture.shutdown()?;
        }
        Ok(())
    }
}

// D/L/E belong to table metadata; P belongs to the exact current output snapshot.
// This comparison supplements native provenance loading; it is not a codec decoder.
fn recursive_lake_binding(
    context: &ScenarioContext,
    rest: &str,
    table: &str,
    expected_uuid: &str,
) -> Result<serde_json::Value> {
    use std::io::Read;
    if table != "recursive_mv" {
        bail!("recursive binding table is not frozen");
    }
    let mut response = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read recursive exact lake binding")?)
        .build()?
        .get(format!(
            "{}/v1/namespaces/ns/tables/{table}",
            rest.trim_end_matches('/')
        ))
        .send()?
        .error_for_status()?;
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    context.remaining("finish recursive exact lake binding")?;
    if bytes.is_empty() || bytes.len() > 1024 * 1024 {
        bail!("recursive REST metadata exceeds fixture budget");
    }
    let loaded: serde_json::Value = serde_json::from_slice(&bytes)?;
    let m = recursive_required(&loaded, "metadata")?;
    if recursive_required(m, "format-version")?.as_u64() != Some(3) {
        bail!("recursive REST table format version differs");
    }
    let uuid = recursive_uuid(m, "table-uuid")?;
    if uuid != expected_uuid {
        bail!("recursive REST table has a different exact provider UUID");
    }
    let snapshot = recursive_positive(m, "current-snapshot-id")?;
    let snapshots = recursive_required(m, "snapshots")?
        .as_array()
        .context("recursive snapshots absent/not an array")?;
    if snapshots.len() > 64 {
        bail!("recursive snapshot-list fixture budget exceeded");
    }
    let mut matching = snapshots
        .iter()
        .filter(|s| s.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(snapshot));
    let current = matching
        .next()
        .context("exact recursive current snapshot absent")?;
    if matching.next().is_some() {
        bail!("exact recursive current snapshot duplicated");
    }
    let schema_id = recursive_schema_id(m, "current-schema-id")?;
    let schemas = recursive_required(m, "schemas")?
        .as_array()
        .context("recursive schemas absent/not an array")?;
    if schemas.len() > 64 {
        bail!("recursive schema-list fixture budget exceeded");
    }
    let mut matching = schemas
        .iter()
        .filter(|s| s.get("schema-id").and_then(serde_json::Value::as_i64) == Some(schema_id));
    let schema = matching
        .next()
        .context("recursive exact current schema absent")?;
    if matching.next().is_some() {
        bail!("recursive exact current schema duplicated");
    }
    recursive_json_budget(schema)?;
    let location = recursive_string(m, "location", 4096)?;
    let table_properties = recursive_required(m, "properties")?;
    let summary = recursive_required(current, "summary")?;
    Ok(
        serde_json::json!({"table_uuid":uuid,"snapshot":snapshot,"schema_id":schema_id,"schema":schema,
        "definition":recursive_document(table_properties,"definition","table-metadata",snapshot,location)?,
        "interpretation":recursive_document(table_properties,"interpretation","table-metadata",snapshot,location)?,
        "eligibility":recursive_document(table_properties,"eligibility","table-metadata",snapshot,location)?,
        "publication":recursive_document(summary,"publication","exact-output",snapshot,location)?}),
    )
}

// Bounded evidence checks. Native provenance remains the payload authority.
fn recursive_required<'a>(
    value: &'a serde_json::Value,
    key: &str,
) -> Result<&'a serde_json::Value> {
    value
        .as_object()
        .context("recursive receipt is not an object")?
        .get(key)
        .with_context(|| format!("recursive required field {key} absent"))
}
fn recursive_string<'a>(value: &'a serde_json::Value, key: &str, cap: usize) -> Result<&'a str> {
    let text = recursive_required(value, key)?
        .as_str()
        .with_context(|| format!("recursive {key} is not a string"))?;
    if text.is_empty() || text.len() > cap {
        bail!("recursive {key} string budget invalid");
    }
    Ok(text)
}
fn recursive_positive(value: &serde_json::Value, key: &str) -> Result<i64> {
    let number = recursive_required(value, key)?
        .as_i64()
        .with_context(|| format!("recursive {key} is not an exact signed integer"))?;
    if number <= 0 {
        bail!("recursive {key} is not positive");
    }
    Ok(number)
}
fn recursive_schema_id(value: &serde_json::Value, key: &str) -> Result<i64> {
    let number = recursive_required(value, key)?
        .as_i64()
        .context("recursive schema ID is not an exact integer")?;
    if !(0..=i64::from(i32::MAX)).contains(&number) {
        bail!("recursive schema ID is outside its provider domain");
    }
    Ok(number)
}
fn recursive_uuid(value: &serde_json::Value, key: &str) -> Result<String> {
    let text = recursive_string(value, key, 36)?;
    let parts = text.split('-').collect::<Vec<_>>();
    if parts.iter().map(|p| p.len()).collect::<Vec<_>>() != [8, 4, 4, 4, 12]
        || !parts.iter().all(|p| {
            p.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        || parts.iter().all(|p| p.bytes().all(|b| b == b'0'))
    {
        bail!("recursive {key} is not a canonical nonzero UUID");
    }
    Ok(text.into())
}
fn recursive_json_budget(value: &serde_json::Value) -> Result<()> {
    let mut pending = vec![(value, 1usize)];
    let mut nodes = 0usize;
    let mut text = 0usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if nodes > 32_768 || depth > 64 {
            bail!("recursive JSON structural budget exceeded");
        }
        match value {
            serde_json::Value::Object(fields) => {
                for (key, child) in fields {
                    text = text
                        .checked_add(key.len())
                        .context("recursive JSON text overflow")?;
                    pending.push((child, depth + 1));
                }
            }
            serde_json::Value::Array(values) => {
                if values.len() > 32_768 {
                    bail!("recursive JSON array budget exceeded");
                }
                pending.extend(values.iter().map(|v| (v, depth + 1)));
            }
            serde_json::Value::String(s) => {
                text = text
                    .checked_add(s.len())
                    .context("recursive JSON text overflow")?;
            }
            _ => (),
        }
        if text > 256 * 1024 {
            bail!("recursive JSON text budget exceeded");
        }
    }
    Ok(())
}
fn recursive_fields(value: &serde_json::Value, key: &str) -> Result<()> {
    let fields = recursive_required(value, key)?
        .as_array()
        .context("recursive field facts are not an array")?;
    if fields.is_empty() || fields.len() > 256 {
        bail!("recursive field-fact count invalid");
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut paths = std::collections::BTreeSet::new();
    for field in fields {
        let path = recursive_string(field, "path", 4096)?;
        let id = recursive_positive(field, "id")?;
        if id > i64::from(i32::MAX) || !ids.insert(id) || !paths.insert(path) {
            bail!("recursive field identity absent/duplicate/out of range");
        }
        recursive_required(field, "required")?
            .as_bool()
            .context("recursive required fact is not Boolean")?;
        recursive_string(field, "kind", 64)?;
    }
    Ok(())
}
fn recursive_files(value: &serde_json::Value, key: &str, delete: bool) -> Result<()> {
    let files = recursive_required(value, key)?
        .as_array()
        .context("recursive file facts are not an array")?;
    if files.len() > 64 || (!delete && files.is_empty()) {
        bail!("recursive file count invalid");
    }
    let mut identities = std::collections::BTreeSet::new();
    for file in files {
        let path = recursive_string(file, "path", 4096)?;
        let content = recursive_string(file, "content", 64)?;
        recursive_string(file, "format", 32)?;
        let spec_id = recursive_required(file, "spec_id")?
            .as_i64()
            .context("recursive spec ID is not an exact signed integer")?;
        if !(0..=i64::from(i32::MAX)).contains(&spec_id) {
            bail!("recursive spec ID is outside its provider domain");
        }
        for key in ["data_sequence", "file_sequence"] {
            let sequence = recursive_required(file, key)?;
            if !sequence.is_null() {
                let number = sequence
                    .as_i64()
                    .with_context(|| format!("recursive {key} is not an exact signed integer"))?;
                if number < 0 {
                    bail!("recursive {key} is negative");
                }
            }
        }
        let count = recursive_required(file, "record_count")?
            .as_u64()
            .context("recursive record count is not an exact unsigned integer")?;
        let size = recursive_positive(file, "file_size")?;
        if count > 1000 || size > 16 * 1024 * 1024 || (!delete && content != "DATA") {
            bail!("recursive file facts exceed exact fixture domain");
        }
        let identity = if delete {
            if content != "POSITION_DELETES" || recursive_string(file, "format", 32)? != "PUFFIN" {
                bail!("recursive expected real position DV absent");
            }
            let equality_ids = recursive_required(file, "equality_ids")?;
            if !equality_ids.is_null() && !equality_ids.as_array().is_some_and(|ids| ids.is_empty())
            {
                bail!("recursive position DV equality IDs are neither null nor an empty array");
            }
            let referenced = recursive_string(file, "referenced_data_file", 4096)?;
            let offset = recursive_required(file, "content_offset")?
                .as_u64()
                .context("recursive DV offset absent/noninteger")?;
            let length = recursive_positive(file, "content_size")? as u64;
            if offset
                .checked_add(length)
                .context("recursive DV slice overflow")?
                > size as u64
            {
                bail!("recursive DV slice leaves physical file");
            }
            format!("{path}|{referenced}|{offset}|{length}")
        } else {
            path.to_owned()
        };
        if !identities.insert(identity) {
            bail!("recursive exact file identity repeated");
        }
    }
    Ok(())
}

#[cfg(test)]
mod recursive_file_receipt_tests {
    use super::recursive_files;
    use serde_json::{Value, json};

    fn data_file() -> Value {
        json!({
            "path": "s3://private/ns/recursive_source/data/initial.parquet",
            "content": "DATA",
            "format": "PARQUET",
            "spec_id": 0,
            "record_count": 6,
            "file_size": 256,
            "data_sequence": null,
            "file_sequence": null
        })
    }

    fn position_dv() -> Value {
        json!({
            "path": "s3://private/ns/recursive_source/data/delete.puffin",
            "content": "POSITION_DELETES",
            "format": "PUFFIN",
            "spec_id": 0,
            "record_count": 1,
            "file_size": 256,
            "data_sequence": null,
            "file_sequence": null,
            "referenced_data_file": "s3://private/ns/recursive_source/data/initial.parquet",
            "content_offset": 8,
            "content_size": 64,
            "equality_ids": null
        })
    }

    fn validate(file: Value, delete: bool) -> anyhow::Result<()> {
        recursive_files(&json!({"files": [file]}), "files", delete)
    }

    #[test]
    fn explicit_null_sequences_and_position_dv_equality_ids_are_valid() {
        validate(data_file(), false).unwrap();
        validate(position_dv(), true).unwrap();
        let mut dv = position_dv();
        dv["equality_ids"] = json!([]);
        validate(dv, true).unwrap();
        for mut file in [data_file(), position_dv()] {
            file["spec_id"] = json!(i32::MAX);
            file["data_sequence"] = json!(0);
            file["file_sequence"] = json!(i64::MAX);
            let delete = file["content"] == "POSITION_DELETES";
            validate(file, delete).unwrap();
        }
    }

    #[test]
    fn required_file_fact_keys_cannot_be_omitted() {
        for (file, delete) in [(data_file(), false), (position_dv(), true)] {
            for key in ["spec_id", "data_sequence", "file_sequence"] {
                let mut missing = file.clone();
                missing.as_object_mut().unwrap().remove(key);
                assert!(
                    validate(missing, delete).is_err(),
                    "missing {key}, delete={delete}"
                );
            }
        }
        let mut missing = position_dv();
        missing.as_object_mut().unwrap().remove("equality_ids");
        assert!(validate(missing, true).is_err());
    }

    #[test]
    fn spec_id_and_present_sequences_reject_nonintegral_or_out_of_range_values() {
        for (file, delete) in [(data_file(), false), (position_dv(), true)] {
            for key in ["spec_id", "data_sequence", "file_sequence"] {
                let mut invalid = vec![
                    json!(-1),
                    json!(0.0),
                    json!(true),
                    json!("0"),
                    json!([]),
                    json!({}),
                    json!(u64::MAX),
                ];
                if key == "spec_id" {
                    invalid.extend([Value::Null, json!(i64::from(i32::MAX) + 1)]);
                }
                for value in invalid {
                    let mut malformed = file.clone();
                    malformed[key] = value.clone();
                    assert!(
                        validate(malformed, delete).is_err(),
                        "accepted invalid {key}={value}, delete={delete}"
                    );
                }
            }
        }
    }

    #[test]
    fn position_dv_equality_ids_reject_nonempty_or_scalar_values() {
        for value in [
            json!([1]),
            json!([null]),
            json!(0),
            json!(false),
            json!(""),
            json!({}),
        ] {
            let mut dv = position_dv();
            dv["equality_ids"] = value.clone();
            assert!(
                validate(dv, true).is_err(),
                "accepted invalid equality_ids={value}"
            );
        }
    }
}
fn validate_recursive_receipt(value: &serde_json::Value, stage: &str) -> Result<()> {
    if serde_json::to_vec(value)?.len() > 256 * 1024 {
        bail!("recursive receipt exceeds byte budget");
    }
    recursive_json_budget(value)?;
    let expected = match stage {
        "initialize" => "recursive_source_initial",
        "mutate" => "recursive_source_changed",
        "initial" | "restored" | "incremental" | "full" => "recursive_mv_observed",
        _ => bail!("unknown exact recursive stage"),
    };
    if recursive_string(value, "record", 64)? != expected {
        bail!("recursive receipt belongs to another stage");
    }
    recursive_uuid(value, "source_uuid")?;
    recursive_schema_id(value, "schema_id")?;
    recursive_positive(value, "snapshot")?;
    let schema: serde_json::Value =
        serde_json::from_str(recursive_string(value, "schema_json", 256 * 1024)?)?;
    recursive_json_budget(&schema)?;
    schema
        .as_object()
        .context("recursive schema is not an object")?;
    recursive_fields(value, "fields")?;
    recursive_files(value, "data_files", false)?;
    recursive_files(value, "delete_files", true)?;
    recursive_required(value, "summary")?
        .as_object()
        .context("recursive exact snapshot summary absent/not an object")?;
    let bag = recursive_required(value, "bag")?
        .as_array()
        .context("recursive bag absent/not an array")?;
    if bag.len() > 1000 {
        bail!("recursive bag count exceeded");
    }
    let mut total = 0u64;
    let mut contents = std::collections::BTreeSet::new();
    for entry in bag {
        let content = recursive_string(entry, "content", 16 * 1024)?;
        let node: serde_json::Value = serde_json::from_str(content)?;
        recursive_json_budget(&node)?;
        let count = recursive_positive(entry, "count")? as u64;
        total = total
            .checked_add(count)
            .context("recursive bag count overflow")?;
        if !contents.insert(content) || total > 1000 {
            bail!("recursive bag identity/count invalid");
        }
    }
    let expected_rows = if matches!(stage, "initialize" | "initial" | "restored") {
        6
    } else {
        7
    };
    if total != expected_rows {
        bail!("recursive complete bag endpoint count differs");
    }
    if matches!(stage, "initialize" | "mutate") {
        if recursive_positive(value, "to_snapshot")? != recursive_positive(value, "snapshot")? {
            bail!("recursive to-snapshot does not identify emitted source");
        }
        recursive_files(value, "added_files", false)?;
        if stage == "initialize" {
            if !recursive_required(value, "from_snapshot")?.is_null() {
                bail!("initial source has a prior snapshot");
            }
        } else if recursive_positive(value, "from_snapshot")?
            == recursive_positive(value, "to_snapshot")?
        {
            bail!("recursive mutation did not advance its frontier");
        }
    } else {
        if recursive_string(value, "stage", 32)? != stage {
            bail!("recursive observation stage mismatch");
        }
        recursive_uuid(value, "table_uuid")?;
        recursive_schema_id(value, "source_schema_id")?;
        recursive_positive(value, "source_snapshot")?;
        recursive_fields(value, "source_fields")?;
        let source: serde_json::Value =
            serde_json::from_str(recursive_string(value, "source_schema_json", 256 * 1024)?)?;
        recursive_json_budget(&source)?;
        source
            .as_object()
            .context("recursive source schema is not an object")?;
    }
    if stage == "full" {
        let files = recursive_required(value, "data_files")?.as_array().unwrap();
        if !recursive_required(value, "delete_files")?
            .as_array()
            .unwrap()
            .is_empty()
        {
            bail!("FULL retains actual delete facts");
        }
        let records = files.iter().try_fold(0u64, |n, f| {
            n.checked_add(
                recursive_required(f, "record_count")?
                    .as_u64()
                    .context("FULL file count not integer")?,
            )
            .context("FULL count overflow")
        })?;
        let bytes = files.iter().try_fold(0u64, |n, f| {
            n.checked_add(recursive_positive(f, "file_size")? as u64)
                .context("FULL size overflow")
        })?;
        let summary = recursive_required(value, "summary")?;
        for (key, expected) in [
            ("total-data-files", files.len() as u64),
            ("total-delete-files", 0),
            ("total-records", records),
            ("total-files-size", bytes),
            ("total-position-deletes", 0),
            ("total-equality-deletes", 0),
        ] {
            let text = recursive_string(summary, key, 20)?;
            let actual = text
                .parse::<u64>()
                .context("FULL summary total is not a bounded unsigned integer")?;
            if text != actual.to_string() || actual != expected {
                bail!("FULL summary {key} differs from actual complete files");
            }
        }
        if records != total {
            bail!("FULL total physical records differ from full independent bag");
        }
    }
    Ok(())
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecursiveManifest {
    version: u16,
    documents: Vec<RecursiveEnvelope>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecursiveEnvelope {
    version: u16,
    owner: String,
    name: String,
    format_owner: String,
    format_name: String,
    format_version: u32,
    revision: [u8; 32],
    encoded_len: u64,
    references: Vec<RecursiveReference>,
    attachment: RecursiveAttachment,
    carrier: RecursiveCarrier,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecursiveReference {
    relationship: String,
    owner: String,
    name: String,
    revision: [u8; 32],
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum RecursiveAttachment {
    TableMetadata,
    ExactOutput {
        committed_version: Vec<u8>,
        snapshot_id: Option<i64>,
    },
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum RecursiveCarrier {
    Available { content: Vec<u8> },
    Deferred { location: String },
}
fn recursive_document(
    properties: &serde_json::Value,
    name: &str,
    attachment: &str,
    snapshot: i64,
    _table_location: &str,
) -> Result<serde_json::Value> {
    let encoded = recursive_string(properties, "novarocks.documents.v1", 256 * 1024)?;
    let manifest: RecursiveManifest = serde_json::from_str(encoded)?;
    if manifest.version != 1 || manifest.documents.is_empty() || manifest.documents.len() > 64 {
        bail!("recursive exact manifest version/count invalid");
    }
    let mut identities = std::collections::BTreeSet::new();
    let mut references = 0usize;
    let mut selected = None;
    for document in &manifest.documents {
        if document.version != 1
            || document.format_version == 0
            || document.encoded_len == 0
            || document.encoded_len > 8 * 1024 * 1024
        {
            bail!("recursive document version/length invalid");
        }
        for text in [
            &document.owner,
            &document.name,
            &document.format_owner,
            &document.format_name,
        ] {
            if text.is_empty() || text.len() > 128 {
                bail!("recursive document identity budget invalid");
            }
        }
        if !identities.insert((&document.owner, &document.name, document.revision)) {
            bail!("recursive exact envelope identity duplicate");
        }
        references = references
            .checked_add(document.references.len())
            .context("recursive reference overflow")?;
        if references > 256 {
            bail!("recursive manifest reference budget exceeded");
        }
        let mut edges = std::collections::BTreeSet::new();
        for edge in &document.references {
            for text in [&edge.relationship, &edge.owner, &edge.name] {
                if text.is_empty() || text.len() > 128 {
                    bail!("recursive reference identity budget invalid");
                }
            }
            if !edges.insert((&edge.relationship, &edge.owner, &edge.name, edge.revision)) {
                bail!("recursive exact reference duplicate");
            }
        }
        match &document.carrier {
            RecursiveCarrier::Available { content } => {
                if content.len() as u64 != document.encoded_len
                    || Sha256::digest(content).as_slice() != document.revision
                {
                    bail!("recursive available carrier length/revision differs");
                }
            }
            RecursiveCarrier::Deferred { location } => {
                if location.is_empty() || location.len() > 4096 {
                    bail!("recursive deferred carrier location budget invalid");
                }
                reqwest::Url::parse(location)
                    .context("recursive deferred carrier is not an absolute URI")?;
                // Freeze the exact opaque reference. Native provenance validates its
                // admitted owner; creation storage roots need not equal table location.
            }
        }
        if let RecursiveAttachment::ExactOutput {
            committed_version,
            snapshot_id,
        } = &document.attachment
            && (committed_version.is_empty()
                || committed_version.len() > 1024
                || snapshot_id.is_none_or(|id| id <= 0))
        {
            bail!("recursive exact-output attachment invalid");
        }
        if document.owner == "novarocks.mv" && document.name == name {
            if document.format_owner != "novarocks.mv"
                || document.format_name != name
                || document.format_version != 1
            {
                bail!("recursive selected document format differs from its exact contract");
            }
            if selected.is_some() {
                bail!("recursive selected document name is ambiguous");
            }
            let valid = match (&document.attachment, attachment) {
                (RecursiveAttachment::TableMetadata, "table-metadata") => true,
                (
                    RecursiveAttachment::ExactOutput {
                        snapshot_id: Some(id),
                        ..
                    },
                    "exact-output",
                ) => *id == snapshot,
                _ => false,
            };
            if !valid {
                bail!("recursive document has wrong exact carrier attachment");
            }
            selected = Some(serde_json::to_value(document)?);
        }
    }
    selected.context("recursive exact selected document absent")
}
