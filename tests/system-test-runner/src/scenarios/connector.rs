use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, QueryExecutionResourceSnapshot,
    ServerHandle,
};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const CONNECTOR_READER_OPEN: &str = "NOVAROCKS_CONNECTOR_UNIT_READER_OPEN";
const CONNECTOR_READER_CLOSE: &str = "NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(DistributedReaderCancel),
        Box::new(GenerationReplacement),
    ]
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

        let reader_logs = wait_for_open_reader_on_every_backend(
            context,
            "connector_cancel_catalog",
            "wait for every BE to open a distributed connector reader",
        )?;
        assert_readers_are_in_flight(&reader_logs, "before KILL QUERY")?;
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

struct GenerationReplacement;

impl Scenario for GenerationReplacement {
    fn name(&self) -> &'static str {
        "connector/generation-replacement"
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
            context.remaining("connect connector generation control session")?,
        )?;

        let warehouse = create_warehouse(context, "generation-replacement")?;
        context.action("create first Iceberg connector generation and three data files");
        create_catalog_table_and_data(
            &mut control,
            "connector_generation_catalog",
            "connector_generation_db",
            "connector_generation_data",
            &warehouse,
        )?;

        context.action("start a read pinned to the first connector generation");
        let target = start_connector_read(
            &user,
            port,
            "connector_generation_catalog",
            "connector_generation_db",
            "connector_generation_data",
        )?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive old-generation connection id")?)
            .context(
                "old-generation connector read terminated before publishing its connection id",
            )?;
        let old_logs = wait_for_open_reader_on_every_backend(
            context,
            "connector_generation_catalog",
            "wait for every BE to open an old-generation connector reader",
        )?;
        assert_readers_are_in_flight(&old_logs, "before catalog replacement")?;
        let old_incarnations = reader_incarnations(&old_logs, "connector_generation_catalog")?;
        if let Ok(result) = target.done.try_recv() {
            bail!(
                "old-generation connector read completed before replacement was published: {result:?}"
            );
        }

        context.action("drop and recreate the catalog while old readers remain in flight");
        control
            .query_drop("DROP CATALOG connector_generation_catalog")
            .context("retire first connector generation")?;
        create_catalog(&mut control, "connector_generation_catalog", &warehouse)?;

        context.action("read the same table through the replacement connector generation");
        let rows: Vec<i64> = control
            .query(
                "SELECT count(*) FROM connector_generation_catalog.connector_generation_db.connector_generation_data",
            )
            .context("read table through replacement connector generation")?;
        if rows != [300_000] {
            bail!("replacement connector generation returned {rows:?}, expected [300000]");
        }
        wait_for_replacement_reader_on_every_backend(
            context,
            "connector_generation_catalog",
            &old_incarnations,
        )?;

        context.action(format!(
            "cancel old-generation reader through KILL QUERY {connection_id}"
        ));
        control
            .query_drop(format!("KILL QUERY {connection_id}"))
            .context("issue public MySQL KILL QUERY for old-generation reader")?;
        assert_cancelled_query(
            &target.done,
            context.remaining("await old-generation reader cancellation")?,
        )?;
        assert_idle_query(&mut control, connection_id)?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("old-generation connector read thread panicked"))??;

        wait_for_retired_incarnation_close(
            context,
            "connector_generation_catalog",
            &old_incarnations,
        )?;
        await_resource_convergence(context, &baseline, "connector generation replacement")?;
        Ok(())
    }
}

struct ConnectorRead {
    ready: mpsc::Receiver<u32>,
    done: mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    release: mpsc::Sender<()>,
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
        "NOVAROCKS_SQL_TEST_EMIT_CANCEL_MARKER".to_string(),
        "1".to_string(),
    );
    environment
}

fn connector_launch_config() -> ScenarioLaunchConfig {
    ScenarioLaunchConfig {
        child_environment: connector_reader_environment(),
        config_overlay: CrossProcessConfigOverlay {
            be: Some(
                r#"
[runtime]
operator_buffer_chunks = 1
query_control_terminal_drain_timeout_ms = 1000
"#
                .to_string(),
            ),
            ..Default::default()
        },
        ..Default::default()
    }
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

fn start_connector_read(
    user: &str,
    port: u16,
    catalog: &str,
    database: &str,
    table: &str,
) -> Result<ConnectorRead> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::sync_channel(1);
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
        release_rx
            .recv()
            .context("release connector reader MySQL session")?;
        Ok(())
    });
    Ok(ConnectorRead {
        ready,
        done,
        release,
        thread,
    })
}

fn assert_idle_query(control: &mut mysql::Conn, connection_id: u32) -> Result<()> {
    let error = control
        .query_drop(format!("KILL QUERY {connection_id}"))
        .expect_err("a cancelled connector session must have no active query");
    match error {
        mysql::Error::MySqlError(error) if error.code == 1094 => Ok(()),
        other => bail!("expected idle KILL QUERY error 1094, received {other}"),
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

fn wait_for_replacement_reader_on_every_backend(
    context: &mut ScenarioContext,
    catalog: &str,
    old_incarnations: &[String],
) -> Result<Vec<String>> {
    wait_for_backend_logs(
        context,
        "wait for every BE to resolve the replacement connector incarnation",
        |logs| {
            logs.iter().zip(old_incarnations).all(|(log, old)| {
                reader_open_lines(log, catalog)
                    .any(|line| reader_incarnation(line).is_some_and(|current| current != old))
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

fn wait_for_retired_incarnation_close(
    context: &mut ScenarioContext,
    catalog: &str,
    old_incarnations: &[String],
) -> Result<Vec<String>> {
    wait_for_backend_logs(
        context,
        "wait for every retired-generation connector reader to close",
        |logs| {
            logs.iter().zip(old_incarnations).all(|(log, incarnation)| {
                let (opens, closes) = reader_counts_for_incarnation(log, catalog, incarnation);
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

fn assert_readers_are_in_flight(logs: &[String], phase: &str) -> Result<()> {
    for (index, log) in logs.iter().enumerate() {
        let (opens, closes) = reader_counts(log);
        if opens <= closes {
            bail!(
                "BE[{index}] has no in-flight connector reader {phase}: opens={opens} closes={closes}"
            );
        }
    }
    Ok(())
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

fn reader_incarnations(logs: &[String], catalog: &str) -> Result<Vec<String>> {
    logs.iter()
        .enumerate()
        .map(|(index, log)| {
            reader_open_lines(log, catalog)
                .find_map(reader_incarnation)
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!("BE[{index}] reader marker did not include connector incarnation")
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

fn reader_incarnation(line: &str) -> Option<&str> {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("incarnation="))
}

fn reader_counts(log: &str) -> (usize, usize) {
    (
        log.match_indices(CONNECTOR_READER_OPEN).count(),
        log.match_indices(CONNECTOR_READER_CLOSE).count(),
    )
}

fn reader_counts_for_incarnation(log: &str, catalog: &str, incarnation: &str) -> (usize, usize) {
    let count = |event| {
        log.lines()
            .filter(|line| {
                line.contains(event)
                    && line.contains("provider=iceberg")
                    && line.contains(&format!("instance={catalog}"))
                    && reader_incarnation(line) == Some(incarnation)
            })
            .count()
    };
    (count(CONNECTOR_READER_OPEN), count(CONNECTOR_READER_CLOSE))
}
