use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use ::mysql::prelude::{FromRow, Queryable};
use ::mysql::{Conn, Row};
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, ServerHandle,
};
use novarocks_connector_iceberg::access_binding::IcebergReadBinding;
use novarocks_connector_iceberg::catalog_config::parse_catalog_configuration;
use novarocks_connector_iceberg::catalog_runtime::build_rest_catalog;
use novarocks_connector_iceberg::iceberg::{Catalog, TableIdent};
use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const MV_CREDENTIAL_NAME: &str = "mv-rest-static";
const MV_ACCESS_KEY_ENV: &str = "NOVAROCKS_MV_REST_S3_ACCESS_KEY_ID";
const MV_SECRET_KEY_ENV: &str = "NOVAROCKS_MV_REST_S3_SECRET_ACCESS_KEY";
const MV_REST_BINDING_FILE: &str = "mv-rest-binding.json";

#[derive(Deserialize, Serialize)]
struct MvRestBinding {
    rest_uri: String,
    rest_warehouse: String,
    minio_endpoint: String,
}

struct RestBackedMvScenario {
    inner: Box<dyn Scenario>,
    fixture: Mutex<Option<IsolatedIcebergRestFixture>>,
}

impl RestBackedMvScenario {
    fn new(inner: Box<dyn Scenario>) -> Self {
        Self {
            inner,
            fixture: Mutex::new(None),
        }
    }
}

impl Scenario for RestBackedMvScenario {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = self.inner.launch_config(scenario_root)?;
        let rest = IsolatedIcebergRestFixture::start(scenario_root)
            .context("start private Iceberg REST fixture for MV recovery")?;
        let endpoints = rest.endpoints();
        let binding = MvRestBinding {
            rest_uri: endpoints.rest_uri.clone(),
            rest_warehouse: endpoints.rest_warehouse.clone(),
            minio_endpoint: endpoints.minio_endpoint.clone(),
        };
        fs::write(
            scenario_root.join(MV_REST_BINDING_FILE),
            serde_json::to_vec(&binding).context("encode private MV REST binding")?,
        )
        .context("write private MV REST binding")?;
        let identity = rest.static_s3_identity();
        for environment in [
            &mut launch.child_environment.fe,
            &mut launch.child_environment.be,
        ] {
            environment.insert(
                MV_ACCESS_KEY_ENV.to_string(),
                identity.access_key_id.clone(),
            );
            environment.insert(
                MV_SECRET_KEY_ENV.to_string(),
                identity.secret_access_key.clone(),
            );
        }
        let credential = |purpose: &str| {
            format!(
                "\n[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{MV_CREDENTIAL_NAME}\"\ngeneration = \"v1\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{MV_ACCESS_KEY_ENV}}}\"\naccess_key_secret = \"${{ENV:{MV_SECRET_KEY_ENV}}}\"\n"
            )
        };
        launch
            .config_overlay
            .fe
            .get_or_insert_with(String::new)
            .push_str(&credential("object-store-metadata"));
        launch
            .config_overlay
            .be
            .get_or_insert_with(String::new)
            .push_str(&credential("object-store-data"));
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("MV REST fixture initialized more than once");
        }
        *fixture = Some(rest);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        self.inner.run(context)
    }

    fn teardown(&self) -> Result<()> {
        let inner_result = self.inner.teardown();
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?
            .take();
        let fixture_result = if let Some(mut rest) = fixture {
            rest.shutdown()
                .context("shutdown private MV Iceberg REST fixture")
        } else {
            Ok(())
        };
        inner_result.and(fixture_result)
    }
}

/// The task protocol's only fault that fails a participant which was admitted,
/// published RUNNING, and then failed on its own. It replaces the retired
/// protocol's `start-ack-suppress` here because a suppressed start acknowledged
/// nothing, while a staged MV snapshot only exists once a task really ran.
const TASK_EXECUTION_FAILURE: &str = "task-execution-failure";

/// Stable evidence that the injection above actually fired.
const TASK_EXECUTION_FAILURE_MARKER: &str = "NOVAROCKS_TASK_EXECUTION_FAILURE_INJECTED";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(MvStateStoreRestart) as Box<dyn Scenario>,
        Box::new(MvSchedulerRecovery),
        Box::new(MvRewriteBindingBarrier),
        Box::new(MvStagedPublishedRecovery),
        Box::new(MvFirstRefreshStaging),
        Box::new(MvBaseIdentityReplacement),
        Box::new(MvLakePublicationRestartRebuild),
    ]
    .into_iter()
    .map(|scenario| Box::new(RestBackedMvScenario::new(scenario)) as Box<dyn Scenario>)
    .collect()
}

struct MvStateStoreRestart;

impl Scenario for MvStateStoreRestart {
    fn name(&self) -> &'static str {
        "mv/state-store-restart"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_restart";
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, true)?;

        execute(
            context,
            &mut conn,
            "create StateStore-backed materialized view",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
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
        refresh_after_owner_crash(context, &mut conn, catalog, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create a second MV after StateStore recovery",
            "CREATE MATERIALIZED VIEW orders_mv_2 DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
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
}

struct MvSchedulerRecovery;

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
        let mut child_environment = CrossProcessChildEnvironment::default();
        child_environment.fe.insert(
            "NOVAROCKS_MVX4_SCHEDULER_TEST_DIR".to_string(),
            barrier_dir.to_string_lossy().into_owned(),
        );
        Ok(ScenarioLaunchConfig {
            child_environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(
                    r#"
[standalone_server]
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1000
"#
                    .to_string(),
                ),
                be: None,
                ..Default::default()
            },
            native_trust_fixture: Default::default(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let barrier_dir = context.scenario_root().join("mv-scheduler-barrier");
        let hold_trigger = barrier_dir.join("mvx4-scheduler-hold.trigger");
        let _hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        context.action("armed scheduler admission barrier");

        let catalog = "system_mv_scheduler";
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, false)?;
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
            "CREATE MATERIALIZED VIEW orders_mv_a DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
        )?;
        execute(
            context,
            &mut conn,
            "create second asynchronous scheduler MV",
            "CREATE MATERIALIZED VIEW orders_mv_b DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
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

        clear_scheduler_markers(&barrier_dir)?;
        let recovery_hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        execute(
            context,
            &mut conn,
            "create a scheduler MV for FE recovery",
            "CREATE MATERIALIZED VIEW orders_mv_recovery DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
        )?;
        wait_for_marker_count(
            context,
            &barrier_dir,
            1,
            "hold scheduler refresh before FE recovery",
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
        resume_mv_management_after_owner_crash(context, &mut conn, catalog, "orders_mv_recovery")?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_recovery ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            "wait for scheduler recovery to catch up durable MV",
        )?;
        context.action("scheduler recovered the interrupted durable refresh after FE restart");
        Ok(())
    }
}

/// Proves that a distributed rewritten query consumes the M1 target snapshot
/// whose strict final receipt it froze, even if a normal refresh publishes M2
/// before the query is dispatched to its backend tasks.
struct MvRewriteBindingBarrier;

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
        let mut child_environment = CrossProcessChildEnvironment::default();
        child_environment.fe.insert(
            "NOVAROCKS_MVX4_REWRITE_TEST_DIR".to_string(),
            barrier_dir.to_string_lossy().into_owned(),
        );
        Ok(ScenarioLaunchConfig {
            child_environment,
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_rewrite_binding";
        let barrier_dir = context.scenario_root().join("mv-rewrite-barrier");
        let hold_trigger = barrier_dir.join("mvx4-rewrite-hold.trigger");
        let frozen_marker = barrier_dir.join("mvx4-rewrite-final-target-frozen.marker");
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, false)?;
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
            "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
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
        context.action("wait for strict final M1 target receipt to freeze");
        while !frozen_marker.exists() {
            match query.try_recv() {
                Ok(Ok(rows)) => {
                    bail!("rewritten query completed before the M1 freeze barrier: {rows:?}")
                }
                Ok(Err(error)) => {
                    bail!("rewritten query failed before the M1 freeze barrier: {error}")
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    bail!("rewritten query disconnected before the M1 freeze barrier")
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            context.remaining("wait for strict final M1 target receipt to freeze")?;
            thread::sleep(POLL_INTERVAL);
        }
        context.action("observed strict final target proof frozen on M1");

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
}

struct MvStagedPublishedRecovery;

impl Scenario for MvStagedPublishedRecovery {
    fn name(&self) -> &'static str {
        "mv/staged-published-recovery"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir).with_context(|| {
            format!("create MV recovery fault directory {}", fault_dir.display())
        })?;
        let mut child_environment = CrossProcessChildEnvironment::default();
        child_environment.fe.insert(
            "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR".to_string(),
            fault_dir.to_string_lossy().into_owned(),
        );
        Ok(ScenarioLaunchConfig {
            child_environment,
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let catalog = "system_mv_recovery";
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, true)?;
        execute(
            context,
            &mut conn,
            "create MV for staged and published recovery",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
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
        refresh_after_owner_crash(context, &mut conn, catalog, "orders_mv")?;
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
        refresh_after_owner_crash(context, &mut conn, catalog, "orders_mv")?;
        context.action("staged and published crash windows converged through public MV behavior");
        Ok(())
    }
}

struct MvFirstRefreshStaging;

impl Scenario for MvFirstRefreshStaging {
    fn name(&self) -> &'static str {
        "mv/first-refresh-staging"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_staging";
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, true)?;

        execute(
            context,
            &mut conn,
            "create first-refresh projection MV",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
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
            "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
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
            "CREATE MATERIALIZED VIEW orders_start_fault_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
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
}

struct MvBaseIdentityReplacement;

impl Scenario for MvBaseIdentityReplacement {
    fn name(&self) -> &'static str {
        "mv/base-identity-replacement"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_base_identity";
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, true)?;
        execute(
            context,
            &mut conn,
            "create MV with a durable base-object binding",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
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
        externally_drop_rest_table(context, catalog, "ns", "orders")?;
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
        assert_mv_not_recovered_after_base_replacement(context, &mut conn, catalog, "orders_mv")?;
        context.action(
            "verified FE restart fail-closed removes the MV rather than bind a same-name replacement base",
        );
        Ok(())
    }
}

struct MvLakePublicationRestartRebuild;

impl Scenario for MvLakePublicationRestartRebuild {
    fn name(&self) -> &'static str {
        "mv/lake-publication-restart-rebuild"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_lake_rebuild";
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, true)?;
        execute(
            context,
            &mut conn,
            "create MV with a lake-native descriptor",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
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
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => 'ns.orders_mv', level => 'wipe')"
            ),
            "wipe only the MV Accelerator after proving its lake documents exist",
        )?;
        let report = rows
            .first()
            .context("MV Accelerator wipe returned no report row")?;
        let level = report
            .get::<String, _>(0)
            .context("MV Accelerator wipe AvailableLevel column")?;
        let source = report
            .get::<String, _>(4)
            .context("MV Accelerator wipe RebuildSource column")?;
        if level != "wipe" || source != "accelerator-wiped" {
            bail!(
                "unexpected MV Accelerator wipe report level={level:?}, source={source:?}; {}",
                context.diagnostics()
            );
        }
        drop(conn);

        restart_frontend(
            context,
            "restart FE to rediscover the wiped MV from lake documents",
        )?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read MV restored from its new-format lake publication",
        )?;
        refresh_after_owner_crash(context, &mut conn, catalog, "orders_mv")?;
        context.action(
            "verified startup rediscovery restored the wiped MV from lake documents and explicit readmission restored management",
        );
        Ok(())
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

fn setup_orders_fixture(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    seed_rows: bool,
) -> Result<()> {
    let binding = read_mv_rest_binding(context)?;
    execute(
        context,
        conn,
        "create private REST Iceberg catalog",
        &format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"warehouse\"=\"{}\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{MV_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"v1\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"static\",\"credential.object-store-data.name\"=\"{MV_CREDENTIAL_NAME}\",\"credential.object-store-data.generation\"=\"v1\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\")",
            binding.rest_uri, binding.rest_warehouse, binding.minio_endpoint
        ),
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
    catalog_name: &str,
    namespace: &str,
    table: &str,
) -> Result<()> {
    context.remaining("drop base table through external REST catalog client")?;
    context.action("drop original base table through external REST catalog client");
    let rest = read_mv_rest_binding(context)?;
    let configuration = parse_catalog_configuration(
        catalog_name,
        &[
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), rest.rest_uri),
            ("warehouse".to_string(), rest.rest_warehouse),
        ],
    )
    .map_err(anyhow::Error::msg)
    .context("configure external REST catalog client")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("create external REST catalog runtime")?;
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
        Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
    );
    let catalog = runtime
        .block_on(build_rest_catalog(&configuration, binding))
        .map_err(anyhow::Error::msg)
        .context("construct external REST catalog client")?;
    let table = TableIdent::from_strs([namespace, table])
        .context("construct external REST table identifier")?;
    runtime
        .block_on(catalog.drop_table(&table))
        .context("drop original table through external REST catalog client")
}

fn read_mv_rest_binding(context: &ScenarioContext) -> Result<MvRestBinding> {
    let path = context.scenario_root().join(MV_REST_BINDING_FILE);
    let bytes =
        fs::read(&path).with_context(|| format!("read MV REST binding {}", path.display()))?;
    serde_json::from_slice(&bytes).context("decode MV REST binding")
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

fn assert_mv_not_recovered_after_base_replacement(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    context.remaining("verify MV is not recovered after same-name base replacement")?;
    context.action("verify MV is not recovered after same-name base replacement");
    let views: Vec<Row> = conn
        .query("SHOW MATERIALIZED VIEWS FROM ns")
        .context("list MVs after same-name base replacement")?;
    let listed = views
        .iter()
        .find(|row| row.get::<String, _>(0).as_deref() == Some(mv))
        .context("quarantined MV must remain visible in management inventory")?;
    let manageability = listed
        .get::<String, _>(15)
        .context("SHOW MATERIALIZED VIEWS Manageability column")?;
    if !manageability.starts_with("UNAVAILABLE:") {
        bail!(
            "same-name base replacement unexpectedly restored MV management: {manageability:?}; {}",
            context.diagnostics()
        );
    }
    let status: Vec<Row> = query(
        context,
        conn,
        &format!("CALL novarocks_mv_management_status('{catalog}', 'ns', '{mv}')"),
        "inspect replacement-base MV management barrier",
    )?;
    let property = |name: &str| -> Result<String> {
        status
            .iter()
            .find(|row| row.get::<String, _>(0).as_deref() == Some(name))
            .and_then(|row| row.get::<String, _>(1))
            .with_context(|| format!("replacement-base MV status omitted {name}"))
    };
    let challenge = property("Challenge")?;
    let old_incarnation = property("UnsettledEffect1Incarnation")?;
    let resume_sql = format!(
        "CALL novarocks_mv_resume_management('{catalog}', 'ns', '{mv}', '{challenge}', '{old_incarnation}', 'system-test-runner', 'the runner replaced the declared frontend process before this statement')"
    );
    context.remaining("reject readmission onto a same-name replacement base")?;
    context.action("reject readmission onto a same-name replacement base");
    let readmission_error = match conn.query_drop(resume_sql) {
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

fn refresh_after_owner_crash(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    resume_mv_management_after_owner_crash(context, conn, catalog, mv)?;
    context.action("wait for durable MV refresh ownership takeover");
    let sql = format!("REFRESH MATERIALIZED VIEW {mv}");
    loop {
        match conn.query_drop(&sql) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string();
                if !message.contains("another frontend currently owns") {
                    return Err(anyhow::Error::new(error).context(
                        "recovery refresh returned an error other than ownership refusal",
                    ));
                }
                context.remaining("wait for durable MV refresh ownership takeover")?;
                thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

fn resume_mv_management_after_owner_crash(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    let status_sql = format!("CALL novarocks_mv_management_status('{catalog}', 'ns', '{mv}')");
    let status: Vec<Row> = query(
        context,
        conn,
        &status_sql,
        "inspect durable MV management barrier",
    )?;
    let property = |name: &str| -> Result<String> {
        status
            .iter()
            .find(|row| row.get::<String, _>(0).as_deref() == Some(name))
            .and_then(|row| row.get::<String, _>(1))
            .filter(|value| !value.is_empty())
            .with_context(|| format!("MV management status omitted {name}"))
    };
    let challenge = property("Challenge")?;
    let old_incarnation = property("UnsettledEffect1Incarnation")?;
    if property("Catalog")? != catalog {
        bail!("MV management status returned another catalog");
    }
    let resume_sql = format!(
        "CALL novarocks_mv_resume_management('{catalog}', 'ns', '{mv}', '{challenge}', '{old_incarnation}', 'system-test-runner', 'the runner replaced the declared frontend process before this statement')"
    );
    let resumed: Vec<Row> = query(
        context,
        conn,
        &resume_sql,
        "declare old MV process isolated and readmit management",
    )?;
    let settled = resumed
        .iter()
        .find(|row| row.get::<String, _>(0).as_deref() == Some("SettledEffects"))
        .and_then(|row| row.get::<String, _>(1))
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default();
    if settled == 0 {
        bail!("MV management declaration settled no old process effect");
    }
    context.action("old MV process isolation declaration settled durable effects");
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
