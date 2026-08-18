use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use ::mysql::prelude::{FromRow, Queryable};
use ::mysql::{Conn, Row};
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::{CrossProcessChildEnvironment, ServerHandle};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const TEST_DIR_ENV: &str = "NOVAROCKS_STAT2F_MAINTENANCE_TEST_DIR";
const MARKER_PREFIX: &str = "stat2f-maintenance-optimize-";
const READY_SUFFIX: &str = ".before-rebind.ready";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(OptimizeTargetReplacement)]
}

struct OptimizeTargetReplacement;

impl Scenario for OptimizeTargetReplacement {
    fn name(&self) -> &'static str {
        "table-maintenance/target-replacement"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let test_dir = scenario_root.join("stat2f-maintenance-barrier");
        fs::create_dir_all(&test_dir).with_context(|| {
            format!(
                "create STAT-2F maintenance barrier directory {}",
                test_dir.display()
            )
        })?;
        let mut child_environment = CrossProcessChildEnvironment::default();
        child_environment.fe.insert(
            TEST_DIR_ENV.to_string(),
            test_dir.to_string_lossy().into_owned(),
        );
        Ok(ScenarioLaunchConfig {
            child_environment,
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_stat2f_maintenance";
        let warehouse = context.runtime_dir().join("warehouse");
        let barrier_dir = context.scenario_root().join("stat2f-maintenance-barrier");
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, &warehouse)?;

        execute(
            context,
            &mut conn,
            "submit optimize for the original table incarnation",
            "ALTER TABLE orders OPTIMIZE",
        )?;
        let first = wait_for_ready_marker(context, &barrier_dir, None)?;
        context.action("observed durable optimize claim before its first rebind");

        execute(
            context,
            &mut conn,
            "drop original optimize target incarnation",
            "DROP TABLE orders FORCE",
        )?;
        execute(
            context,
            &mut conn,
            "recreate optimize target under the same logical name",
            "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
        )?;
        execute(
            context,
            &mut conn,
            "seed replacement optimize target table",
            "INSERT INTO orders VALUES (9, 90)",
        )?;
        write_resume(&first)?;
        context.action("released optimize rebind barrier after same-name replacement");

        wait_for_job_state(context, &mut conn, catalog, first.job_id, "TARGET_REPLACED")?;
        assert_dispatch_count(context, &first, 0)?;
        context.action("verified replacement became TARGET_REPLACED with zero provider dispatch");

        execute(
            context,
            &mut conn,
            "submit a new optimize to prove the terminal job released its active fence",
            "ALTER TABLE orders OPTIMIZE",
        )?;
        let second = wait_for_ready_marker(context, &barrier_dir, Some(first.job_id))?;
        write_resume(&second)?;
        context.action("a second durable optimize claim acquired the same table fence");
        wait_for_terminal_job(context, &mut conn, catalog, second.job_id)?;

        drop(conn);
        restart_frontend(
            context,
            "restart FE after durable TARGET_REPLACED transition",
        )?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        wait_for_job_state(context, &mut conn, catalog, first.job_id, "TARGET_REPLACED")?;
        assert_dispatch_count(context, &first, 0)?;
        context.action(
            "verified TARGET_REPLACED and its zero-dispatch evidence survive FE restart in native 1FE+3BE",
        );
        Ok(())
    }
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
    warehouse: &Path,
) -> Result<()> {
    fs::create_dir_all(warehouse)
        .with_context(|| format!("create maintenance warehouse {}", warehouse.display()))?;
    execute(
        context,
        conn,
        "create Hadoop Iceberg catalog",
        &format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
            warehouse.display()
        ),
    )?;
    execute(
        context,
        conn,
        "create maintenance fixture namespace",
        &format!("CREATE DATABASE {catalog}.ns"),
    )?;
    select_catalog_and_database(context, conn, catalog)?;
    execute(
        context,
        conn,
        "create maintenance target table",
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )?;
    execute(
        context,
        conn,
        "seed maintenance target table",
        "INSERT INTO orders VALUES (1, 10), (2, 20)",
    )
}

fn select_catalog_and_database(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "select maintenance catalog",
        &format!("SET CATALOG {catalog}"),
    )?;
    execute(context, conn, "select maintenance namespace", "USE ns")
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

#[derive(Debug)]
struct BarrierPaths {
    job_id: i64,
    resume: PathBuf,
    dispatch_count: PathBuf,
}

fn wait_for_ready_marker(
    context: &mut ScenarioContext,
    directory: &Path,
    excluded_job_id: Option<i64>,
) -> Result<BarrierPaths> {
    context.action("wait for STAT-2F optimize before-rebind barrier");
    loop {
        if let Some(paths) = find_ready_marker(directory, excluded_job_id)? {
            return Ok(paths);
        }
        context.remaining("wait for STAT-2F optimize before-rebind barrier")?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn find_ready_marker(
    directory: &Path,
    excluded_job_id: Option<i64>,
) -> Result<Option<BarrierPaths>> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read maintenance barrier directory {}", directory.display()))?
    {
        let entry = entry.with_context(|| {
            format!("read maintenance barrier entry in {}", directory.display())
        })?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(job_id) = name
            .strip_prefix(MARKER_PREFIX)
            .and_then(|raw| raw.strip_suffix(READY_SUFFIX))
            .and_then(|raw| raw.parse::<i64>().ok())
        else {
            continue;
        };
        if Some(job_id) == excluded_job_id {
            continue;
        }
        let stem = format!("{MARKER_PREFIX}{job_id}");
        return Ok(Some(BarrierPaths {
            job_id,
            resume: directory.join(format!("{stem}.before-rebind.resume")),
            dispatch_count: directory.join(format!("{stem}.dispatch-count")),
        }));
    }
    Ok(None)
}

fn write_resume(paths: &BarrierPaths) -> Result<()> {
    fs::write(&paths.resume, "resume\n").with_context(|| {
        format!(
            "write maintenance resume trigger {}",
            paths.resume.display()
        )
    })
}

fn assert_dispatch_count(
    context: &mut ScenarioContext,
    paths: &BarrierPaths,
    expected: u64,
) -> Result<()> {
    let raw = fs::read_to_string(&paths.dispatch_count).with_context(|| {
        format!(
            "read provider dispatch counter {}",
            paths.dispatch_count.display()
        )
    })?;
    let actual = raw.trim().parse::<u64>().with_context(|| {
        format!(
            "parse provider dispatch counter {}",
            paths.dispatch_count.display()
        )
    })?;
    if actual != expected {
        bail!(
            "optimize job {} provider dispatch count was {actual}, expected {expected}; {}",
            paths.job_id,
            context.diagnostics()
        );
    }
    Ok(())
}

fn wait_for_job_state(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    job_id: i64,
    expected_state: &str,
) -> Result<()> {
    context.action(format!(
        "wait for optimize job {job_id} state {expected_state}"
    ));
    loop {
        let jobs = list_optimize_jobs(context, conn, catalog)?;
        if let Some(state) = jobs
            .iter()
            .find(|(observed_job_id, _)| *observed_job_id == job_id)
            .map(|(_, state)| state.as_str())
            && state == expected_state
        {
            return Ok(());
        }
        context.remaining("wait for durable optimize job state")?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_terminal_job(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    job_id: i64,
) -> Result<()> {
    context.action(format!(
        "wait for resubmitted optimize job {job_id} terminal state"
    ));
    loop {
        let jobs = list_optimize_jobs(context, conn, catalog)?;
        if let Some(state) = jobs
            .iter()
            .find(|(observed_job_id, _)| *observed_job_id == job_id)
            .map(|(_, state)| state.as_str())
            && matches!(state, "FINISHED" | "FAILED" | "TARGET_REPLACED")
        {
            return Ok(());
        }
        context.remaining("wait for resubmitted optimize job terminal state")?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn list_optimize_jobs(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
) -> Result<Vec<(i64, String)>> {
    let rows: Vec<Row> = query(
        context,
        conn,
        &format!(
            "SHOW ALTER TABLE OPTIMIZE FROM {catalog}.ns WHERE TableName = 'orders' ORDER BY CreateTime DESC"
        ),
        "read durable optimize job state",
    )?;
    rows.into_iter()
        .map(|row| {
            let job_id = row
                .get::<String, _>(0)
                .context("SHOW ALTER TABLE OPTIMIZE JobId column")?
                .parse::<i64>()
                .context("parse SHOW ALTER TABLE OPTIMIZE JobId")?;
            let state = row
                .get::<String, _>(2)
                .context("SHOW ALTER TABLE OPTIMIZE State column")?;
            Ok((job_id, state))
        })
        .collect()
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
