use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use std::thread;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(MembershipRestart),
        Box::new(CatalogAttachmentRestart),
    ]
}

struct MembershipRestart;

impl Scenario for MembershipRestart {
    fn name(&self) -> &'static str {
        "catalog-state/membership-restart"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            initial_backend_seeds: Some(Vec::new()),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect dynamic membership client")?,
        )?;
        ensure!(
            show_backend_ports(&mut connection)?.is_empty(),
            "dynamic membership scenario must start with an empty durable registry"
        );
        context.action("verified FE starts without configured durable backend members");

        let ports = context
            .handle()
            .runtime()
            .be
            .iter()
            .map(|be| be.grpc)
            .collect::<Vec<_>>();
        for port in &ports {
            connection
                .query_drop(format!("ADD BACKEND '127.0.0.1:{port}'"))
                .with_context(|| format!("add dynamic backend {port}"))?;
        }
        wait_for_backend_ports(context, &mut connection, &ports)?;
        context.action("added all three live BEs through the public SQL membership API");

        drop(connection);
        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE after dynamic membership publication")?;
        context.action("restarted FE after durable membership publication");
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("reconnect after dynamic membership restart")?,
        )?;
        wait_for_backend_ports(context, &mut connection, &ports)?;
        let rows: Vec<i64> = connection
            .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
            .context("execute distributed query after membership restart")?;
        ensure!(
            rows == vec![1, 2],
            "unexpected rows after membership restart: {rows:?}"
        );
        context
            .action("verified durable 1FE+3BE membership and distributed query after FE restart");

        connection
            .query_drop(format!("DROP BACKEND '127.0.0.1:{}' FORCE", ports[2]))
            .context("drop dynamic backend through public SQL membership API")?;
        wait_for_backend_ports(context, &mut connection, &ports[..2])?;
        context.action("dropped one dynamic backend and observed the remaining live pair");
        Ok(())
    }
}

struct CatalogAttachmentRestart;

impl Scenario for CatalogAttachmentRestart {
    fn name(&self) -> &'static str {
        "catalog-state/catalog-attachment-restart"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect catalog attachment client")?,
        )?;
        let warehouse = context
            .runtime_dir()
            .join("warehouses")
            .join("catalog-attachment");
        std::fs::create_dir_all(&warehouse).with_context(|| {
            format!(
                "create catalog attachment warehouse {}",
                warehouse.display()
            )
        })?;
        let warehouse = warehouse.to_string_lossy().replace('"', "\\\"");
        connection
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG tst3_attachment PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
            ))
            .context("create durable catalog attachment")?;
        connection
            .query_drop("CREATE DATABASE tst3_attachment.ns")
            .context("create catalog attachment namespace")?;
        connection
            .query_drop("CREATE TABLE tst3_attachment.ns.orders (id INT, amount INT)")
            .context("create catalog attachment table")?;
        connection
            .query_drop("INSERT INTO tst3_attachment.ns.orders VALUES (1, 10), (2, 20), (3, 30)")
            .context("write catalog attachment rows")?;
        let before: Vec<(i32, i32)> = connection
            .query("SELECT id, amount FROM tst3_attachment.ns.orders ORDER BY id")
            .context("query catalog attachment before FE restart")?;
        ensure!(
            before == vec![(1, 10), (2, 20), (3, 30)],
            "unexpected attachment rows: {before:?}"
        );
        context.action("created attachment and verified a distributed public-SQL read");

        drop(connection);
        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE with durable catalog attachment")?;
        context.action("restarted FE with the attachment in its durable StateStore");
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("reconnect after catalog attachment restart")?,
        )?;
        let restored: Vec<(i32, i32)> = connection
            .query("SELECT id, amount FROM tst3_attachment.ns.orders ORDER BY id")
            .context("query catalog attachment restored by FE restart")?;
        ensure!(
            restored == before,
            "attachment rows changed after FE restart: {restored:?}"
        );
        context
            .action("verified catalog attachment restored and served the same distributed query");
        connection
            .query_drop("DROP CATALOG tst3_attachment")
            .context("drop durable catalog attachment")?;
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

fn show_backend_ports(connection: &mut mysql::Conn) -> Result<Vec<u16>> {
    let rows: Vec<mysql::Row> = connection.query("SHOW BACKENDS").context("SHOW BACKENDS")?;
    rows.into_iter()
        .map(|row| {
            row.get::<String, usize>(2)
                .context("SHOW BACKENDS row missing GrpcPort")?
                .parse::<u16>()
                .context("parse SHOW BACKENDS GrpcPort")
        })
        .collect()
}

fn wait_for_backend_ports(
    context: &ScenarioContext,
    connection: &mut mysql::Conn,
    expected: &[u16],
) -> Result<()> {
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    loop {
        let mut observed = show_backend_ports(connection)?;
        observed.sort_unstable();
        if observed == expected {
            return Ok(());
        }
        let remaining = context.remaining("wait for durable backend membership")?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}
