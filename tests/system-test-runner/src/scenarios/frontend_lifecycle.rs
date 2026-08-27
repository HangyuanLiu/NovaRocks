use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{CrossProcessConfigOverlay, ServerHandle};
use std::fs;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const POLL_INTERVAL: Duration = Duration::from_millis(25);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(StaticFileDisposableCarrier),
        Box::new(GracefulDrain),
    ]
}

struct StaticFileDisposableCarrier;

impl Scenario for StaticFileDisposableCarrier {
    fn name(&self) -> &'static str {
        "frontend-lifecycle/static-file-disposable-carrier"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let warehouse = scenario_root.join("static-file-warehouse");
        fs::create_dir_all(&warehouse)
            .with_context(|| format!("create StaticFile warehouse {}", warehouse.display()))?;
        let snapshot = scenario_root.join("catalogs.toml");
        fs::write(
            &snapshot,
            format!(
                "format_version = 1\n\
                 [[catalogs]]\n\
                 instance_id = \"catalog.lnp8_static\"\n\
                 provider_id = \"iceberg\"\n\
                 display_name = \"lnp8_static\"\n\
                 config_format_version = 1\n\
                 [catalogs.properties]\n\
                 type = \"iceberg\"\n\
                 \"iceberg.catalog.type\" = \"hadoop\"\n\
                 \"iceberg.catalog.warehouse\" = \"{}\"\n",
                warehouse.display()
            ),
        )
        .with_context(|| format!("write StaticFile snapshot {}", snapshot.display()))?;
        Ok(ScenarioLaunchConfig {
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(format!(
                    "[catalog_source]\nmode = \"static-file\"\nstatic_file_path = \"{}\"\n",
                    snapshot.display()
                )),
                be: None,
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let state_before = context
            .handle()
            .frontend_management_get("/v1/frontend/state", Duration::from_secs(2))?;
        let digest_before = static_snapshot_digest(&state_before.body)?;
        ensure!(
            state_before
                .body
                .contains("\"source_mode\":\"static-file\"")
                && state_before.body.contains("\"desired\":1"),
            "StaticFile bootstrap state is incomplete: {}",
            state_before.body
        );
        context.action("captured the bootstrapped StaticFile catalog snapshot identity");

        let deadline = context.deadline();
        context
            .handle()
            .wipe_fe_state_store_and_restart_until(deadline)
            .context("dispose FE SQLite state and restart from StaticFile")?;
        let state_after = context
            .handle()
            .frontend_management_get("/v1/frontend/state", Duration::from_secs(2))?;
        ensure!(
            static_snapshot_digest(&state_after.body)? == digest_before,
            "StaticFile snapshot digest changed after FE disposal"
        );
        context.action("proved an empty FE SQLite store rebuilt the same StaticFile catalog state");
        Ok(())
    }
}

struct GracefulDrain;

impl Scenario for GracefulDrain {
    fn name(&self) -> &'static str {
        "frontend-lifecycle/graceful-drain"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "graceful drain requires native 1FE+3BE"
        );
        context.action("verified native 1FE+3BE topology");

        let mut idle_connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            Duration::from_secs(10),
        )
        .context("open pre-drain idle MySQL session")?;
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let (query_tx, query_rx) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let result = (|| -> Result<Vec<i64>> {
                let mut connection = mysql_actor::connect(&user, port, Duration::from_secs(30))?;
                connection
                    .query("SELECT sleep(10)")
                    .context("run admitted graceful-drain query")
            })();
            let _ = query_tx.send(result);
        });
        wait_for_active_statement(context)?;
        context.action("observed an admitted public MySQL statement lease");

        context
            .handle()
            .begin_fe_drain()
            .context("begin graceful FE drain through SIGTERM")?;
        wait_for_drain_observation(context)?;
        context.action(
            "observed ready=503, live=200, and Draining from the real FE management listener",
        );

        assert_idle_session_statement_is_rejected(&mut idle_connection)?;
        context
            .action("proved a pre-drain idle MySQL session receives the typed draining rejection");

        let query = query_rx
            .recv_timeout(context.remaining("wait for pre-drain statement")?)
            .map_err(|error| anyhow::anyhow!("pre-drain statement did not finish: {error}"))??;
        ensure!(
            query.len() == 1,
            "graceful-drain query did not return one completed row: {query:?}"
        );
        context.action("pre-drain statement completed normally after drain began");

        let deadline = context.deadline();
        context
            .handle()
            .wait_fe_exit_until(deadline)
            .context("wait for FE exit after graceful drain")?;
        context.action("FE exited successfully only after its admitted work completed");
        Ok(())
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let count = context.handle().be_count();
    ensure!(
        count == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, received 1FE+{count}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}

fn static_snapshot_digest(state: &str) -> Result<String> {
    let marker = "\"digest\":\"";
    let start = state
        .find(marker)
        .map(|offset| offset + marker.len())
        .context("StaticFile management state omitted catalog snapshot digest")?;
    let end = state[start..]
        .find('"')
        .map(|offset| start + offset)
        .context("StaticFile management state has an unterminated catalog digest")?;
    Ok(state[start..end].to_string())
}

fn wait_for_active_statement(context: &mut ScenarioContext) -> Result<()> {
    loop {
        let timeout = context
            .remaining("observe admitted statement")?
            .min(Duration::from_secs(1));
        let response = context
            .handle()
            .frontend_management_get("/v1/frontend/state", timeout)?;
        if response.status == 200 && response.body.contains("\"statement\":1") {
            return Ok(());
        }
        if context.remaining("observe admitted statement")?.is_zero() {
            bail!("timed out observing active statement: {response:?}");
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_drain_observation(context: &mut ScenarioContext) -> Result<()> {
    loop {
        let timeout = context
            .remaining("observe FE drain")?
            .min(Duration::from_secs(1));
        let ready = context
            .handle()
            .frontend_management_get("/readyz", timeout)?;
        let live = context
            .handle()
            .frontend_management_get("/livez", timeout)?;
        let state = context
            .handle()
            .frontend_management_get("/v1/frontend/state", timeout)?;
        if ready.status == 503
            && live.status == 200
            && state.status == 200
            && state.body.contains("\"draining\"")
        {
            return Ok(());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn assert_idle_session_statement_is_rejected(connection: &mut mysql::Conn) -> Result<()> {
    let outcome = connection
        .query_drop("SELECT 1")
        .context("execute statement from pre-drain idle MySQL session");
    let error = format!(
        "{:#}",
        outcome.expect_err("post-drain SQL must be rejected")
    );
    ensure!(
        error.contains("FRONTEND_DRAINING") || error.contains("frontend is draining"),
        "post-drain SQL returned an unexpected error: {error}"
    );
    Ok(())
}
