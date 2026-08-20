use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{
    ParticipantTerminalOutcomeKind, QueryExecutionResourceSnapshot,
    QueryLifecycleStructuredSnapshot, ServerHandle,
};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const IO_TIMEOUT_CAP: Duration = Duration::from_secs(10);
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const BASELINE_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(DistributedBaseline),
        Box::new(MysqlDisconnect),
        Box::new(QueryTimeout),
    ]
}

struct DistributedBaseline;

impl Scenario for DistributedBaseline {
    fn name(&self) -> &'static str {
        "query-lifecycle/distributed-baseline"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let connect_timeout = bounded_io_timeout(context, "connect baseline MySQL client")?;
        let mut connection =
            mysql_actor::connect(context.mysql_user(), context.mysql_port(), connect_timeout)?;
        context.action("connected baseline client through public MySQL protocol");

        let before_first = latest_execution_id(context)?;
        execute_baseline_query(&mut connection, "first")?;
        let first = await_terminal_snapshot(context, before_first.as_deref())?;

        let before_second = first.execution_id.clone();
        execute_baseline_query(&mut connection, "second")?;
        let second = await_terminal_snapshot(context, before_second.as_deref())?;
        let before_third = second.execution_id.clone();
        execute_baseline_query(&mut connection, "third")?;
        let third = await_terminal_snapshot(context, before_third.as_deref())?;
        assert_process_attribution(&[&first, &second, &third])?;
        assert_process_attribution_diagnostics(context, &third)?;
        context.action(format!(
            "verified three native terminal snapshots share namespace=0x{:016x}, use consecutive sequence {}, {}, {}, attempt=1, and cover all 3 backend diagnostics",
            first.process_namespace,
            first.local_sequence,
            second.local_sequence,
            third.local_sequence,
        ));

        await_resource_convergence(context, &baseline, false)
    }
}

struct MysqlDisconnect;

impl Scenario for MysqlDisconnect {
    fn name(&self) -> &'static str {
        "query-lifecycle/mysql-disconnect"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let stream = send_raw_mysql_query(
            context.mysql_user(),
            context.mysql_port(),
            "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v",
            bounded_io_timeout(context, "open disconnecting MySQL client")?,
        )?;
        context.action("sent a blocking query through a raw public MySQL connection");
        await_resource_activity(context, &baseline)?;
        context.action("observed in-flight distributed query resources");

        stream
            .shutdown(Shutdown::Both)
            .context("close raw public MySQL client connection")?;
        context.action("closed the raw public MySQL client connection");

        await_resource_convergence(context, &baseline, true)
    }
}

struct QueryTimeout;

impl Scenario for QueryTimeout {
    fn name(&self) -> &'static str {
        "query-lifecycle/query-timeout"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let mut stream = connect_raw_mysql(
            context.mysql_user(),
            context.mysql_port(),
            bounded_io_timeout(context, "open timeout MySQL client")?,
        )?;
        context.action("connected timeout client through public MySQL protocol");
        send_query(&mut stream, "SET query_timeout = 1")?;
        expect_ok_packet(&mut stream, "SET query_timeout")?;
        context.action("set query_timeout = 1 through the public MySQL protocol");

        send_query(
            &mut stream,
            "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v",
        )?;
        context.action("sent a blocking query expected to time out");
        await_resource_activity(context, &baseline)?;
        context.action("observed in-flight distributed query resources before timeout");
        let (_, response) = read_packet(&mut stream).context("read timed query response")?;
        ensure!(
            response.first().copied() == Some(0xff),
            "expected timed query to return a MySQL error packet, got payload={response:?}"
        );
        let error = mysql_error_text(&response)?;
        ensure!(
            error.contains("timed out") || error.contains("timeout"),
            "expected MySQL timeout error, got: {error}"
        );
        context.action(format!("received expected MySQL timeout error: {error}"));

        await_resource_convergence(context, &baseline, true)
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let actual = context.handle().be_count();
    ensure!(
        actual == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, but the runner launched 1FE+{actual}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}

fn resource_snapshot(context: &mut ScenarioContext) -> Result<QueryExecutionResourceSnapshot> {
    context
        .handle()
        .query_execution_resource_snapshot()?
        .context("cross-process harness did not expose the query-resource oracle")
}

fn latest_execution_id(context: &mut ScenarioContext) -> Result<Option<String>> {
    Ok(context
        .handle()
        .query_lifecycle_structured_snapshot()?
        .and_then(|snapshot| snapshot.execution_id))
}

fn execute_baseline_query(connection: &mut mysql::Conn, ordinal: &str) -> Result<()> {
    let rows: Vec<i64> = connection
        .query(BASELINE_QUERY)
        .with_context(|| format!("execute {ordinal} distributed baseline query"))?;
    ensure!(
        rows == vec![1, 2],
        "{ordinal} distributed baseline query returned unexpected rows: {rows:?}"
    );
    Ok(())
}

fn await_terminal_snapshot(
    context: &mut ScenarioContext,
    before_execution_id: Option<&str>,
) -> Result<QueryLifecycleStructuredSnapshot> {
    let deadline = context.deadline();
    context
        .handle()
        .await_query_lifecycle_structured_snapshot_after(before_execution_id, deadline)
        .context("await a new typed query lifecycle terminal snapshot")
}

fn assert_process_attribution(snapshots: &[&QueryLifecycleStructuredSnapshot]) -> Result<()> {
    ensure!(
        !snapshots.is_empty(),
        "attribution requires at least one snapshot"
    );
    for pair in snapshots.windows(2) {
        let previous = pair[0];
        let next = pair[1];
        ensure!(
            previous.process_namespace == next.process_namespace,
            "consecutive baseline queries changed process namespace: previous=0x{:016x}, next=0x{:016x}",
            previous.process_namespace,
            next.process_namespace,
        );
        ensure!(
            previous.local_sequence.checked_add(1) == Some(next.local_sequence),
            "consecutive baseline query sequences are not adjacent: previous={}, next={}",
            previous.local_sequence,
            next.local_sequence,
        );
    }
    for (ordinal, snapshot) in snapshots.iter().enumerate() {
        let ordinal = ordinal + 1;
        ensure!(
            snapshot.attempt_id == 1,
            "baseline query {ordinal} used unexpected attempt id {}",
            snapshot.attempt_id
        );
        ensure!(
            !snapshot.participant_outcomes.is_empty(),
            "baseline query {ordinal} terminal snapshot had no participant outcome",
        );
        ensure!(
            snapshot.participant_outcomes.len() <= REQUIRED_BACKENDS,
            "baseline query {ordinal} terminal snapshot covered {} participants, exceeding the 1FE+{REQUIRED_BACKENDS} topology",
            snapshot.participant_outcomes.len()
        );
        ensure!(
            snapshot
                .participant_outcomes
                .iter()
                .all(|outcome| matches!(outcome, ParticipantTerminalOutcomeKind::Proof)),
            "baseline query {ordinal} terminal snapshot contains a non-proof participant outcome: {:?}",
            snapshot.participant_outcomes
        );
    }
    ensure!(
        snapshots.len() >= 2,
        "attribution acceptance requires at least two consecutive queries"
    );
    Ok(())
}

fn assert_process_attribution_diagnostics(
    context: &mut ScenarioContext,
    snapshot: &QueryLifecycleStructuredSnapshot,
) -> Result<()> {
    let namespace = format!("0x{:016x}", snapshot.process_namespace);
    let namespace_field = format!("query_process_namespace={namespace}");
    let startup_message = "NOVAROCKS_QUERY_PROCESS_NAMESPACE";
    let startup_count = context.handle().fe_log_count(startup_message)?;
    ensure!(
        startup_count == 1,
        "expected exactly one FE process namespace startup publication, found {startup_count}"
    );
    ensure!(
        context
            .handle()
            .fe_log_contents()?
            .contains(&namespace_field),
        "FE startup diagnostics did not publish {namespace_field}"
    );
    for backend in 0..REQUIRED_BACKENDS {
        context
            .handle()
            .assert_be_log(backend, "NOVAROCKS_QUERY_INIT_APPLIED")?;
        context.handle().assert_be_log(backend, &namespace_field)?;
    }
    context.action(format!(
        "verified one FE startup namespace publication and matching BE lifecycle diagnostics for {namespace}"
    ));
    Ok(())
}

fn await_resource_activity(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    loop {
        if resource_snapshot(context)? != *baseline {
            return Ok(());
        }
        let remaining = context.remaining("observe in-flight distributed query resources")?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

fn await_resource_convergence(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
    permits_terminal_retention: bool,
) -> Result<()> {
    let deadline = context.deadline();
    context
        .handle()
        .await_query_execution_resource_convergence(baseline, permits_terminal_retention, deadline)
        .context("await query-resource convergence after terminal lifecycle outcome")?;
    context.action("verified query resources converged after the terminal lifecycle outcome");
    Ok(())
}

fn bounded_io_timeout(context: &ScenarioContext, operation: &str) -> Result<Duration> {
    Ok(context.remaining(operation)?.min(IO_TIMEOUT_CAP))
}

fn connect_raw_mysql(user: &str, port: u16, timeout: Duration) -> Result<TcpStream> {
    const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
    const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&address, timeout)
        .with_context(|| format!("connect raw public MySQL client at {address}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .context("set raw MySQL read timeout")?;
    stream
        .set_write_timeout(Some(timeout))
        .context("set raw MySQL write timeout")?;

    let (_, handshake) = read_packet(&mut stream).context("read MySQL handshake")?;
    ensure!(
        handshake.first().copied() == Some(10),
        "expected MySQL protocol v10 handshake, got payload={handshake:?}"
    );

    let client_flags = CLIENT_LONG_PASSWORD
        | CLIENT_LONG_FLAG
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH;
    let mut response = Vec::with_capacity(user.len() + 64);
    response.extend_from_slice(&client_flags.to_le_bytes());
    response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
    response.push(45);
    response.extend_from_slice(&[0u8; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(0);
    response.extend_from_slice(b"mysql_native_password");
    response.push(0);
    write_packet(&mut stream, 1, &response).context("write MySQL handshake response")?;

    let (_, auth_result) = read_packet(&mut stream).context("read MySQL authentication result")?;
    if auth_result.first().copied() == Some(0xff) {
        bail!(
            "raw public MySQL authentication failed: {}",
            mysql_error_text(&auth_result)?
        );
    }
    ensure!(
        auth_result.first().copied() == Some(0),
        "unexpected raw MySQL authentication response: {auth_result:?}"
    );
    Ok(stream)
}

fn send_raw_mysql_query(user: &str, port: u16, sql: &str, timeout: Duration) -> Result<TcpStream> {
    let mut stream = connect_raw_mysql(user, port, timeout)?;
    send_query(&mut stream, sql)?;
    Ok(stream)
}

fn send_query(stream: &mut TcpStream, sql: &str) -> Result<()> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_packet(stream, 0, &payload).context("write MySQL COM_QUERY packet")
}

fn expect_ok_packet(stream: &mut TcpStream, operation: &str) -> Result<()> {
    let (_, response) =
        read_packet(stream).with_context(|| format!("read response for {operation}"))?;
    if response.first().copied() == Some(0xff) {
        bail!("{operation} failed: {}", mysql_error_text(&response)?);
    }
    ensure!(
        response.first().copied() == Some(0),
        "{operation} expected a MySQL OK packet, got payload={response:?}"
    );
    Ok(())
}

fn mysql_error_text(payload: &[u8]) -> Result<String> {
    ensure!(
        payload.first().copied() == Some(0xff),
        "expected a MySQL error packet, got payload={payload:?}"
    );
    ensure!(
        payload.len() >= 3,
        "truncated MySQL error packet: {payload:?}"
    );
    let message_offset = if payload.get(3).copied() == Some(b'#') {
        9
    } else {
        3
    };
    Ok(String::from_utf8_lossy(&payload[message_offset..]).into_owned())
}

fn read_packet(stream: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .context("read MySQL packet header")?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0u8; length];
    stream
        .read_exact(&mut payload)
        .context("read MySQL packet payload")?;
    Ok((header[3], payload))
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len()).context("MySQL packet payload length fits u32")?;
    ensure!(length <= 0x00ff_ffff, "MySQL packet payload is too large");
    let header = [
        (length & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        ((length >> 16) & 0xff) as u8,
        sequence,
    ];
    stream
        .write_all(&header)
        .context("write MySQL packet header")?;
    stream
        .write_all(payload)
        .context("write MySQL packet payload")?;
    stream.flush().context("flush MySQL packet")
}
