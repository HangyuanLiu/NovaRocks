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
        Box::new(MvStagedPublishedRecovery::default()),
        Box::new(MvFirstRefreshStaging::default()),
        Box::new(MvBaseIdentityReplacement::default()),
        Box::new(MvLakePublicationRestartRebuild::default()),
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
