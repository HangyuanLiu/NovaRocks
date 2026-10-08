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

//! JSON value membership lifecycle across the native 1FE+3BE boundary.
//!
//! A proven-JSON `IN` / `NOT IN` value subquery runs on its own membership
//! owner (`execution/src/exec/operators/membership`): one shared RHS per exact
//! placed task, `Building` until every frozen sender and the local build reach
//! EOS, a terminal `Failed` / `Stopped` that wakes waiting probes, and a typed
//! capacity refusal when the RHS cannot be held. These scenarios drive that
//! owner through real tasks on three backends:
//!
//! * `query-lifecycle/json-membership-full-eos` reads a two-file RHS through
//!   three placed RHS producer tasks, so at least one producer owns no split
//!   and contributes only its EOS. Every result is compared row by row, or
//!   group by group, with an oracle derived from the fixed inputs here; the
//!   grouped form must place the membership on several broadcast-fed probe
//!   tasks. An RHS whose filter selects nothing makes every sender empty.
//! * `query-lifecycle/json-membership-cancel` holds the RHS producers in a
//!   cancellation-aware `sleep` so every placed probe task waits on a
//!   `Building` RHS, issues `KILL QUERY` through the public control path, and
//!   requires ER_QUERY_INTERRUPTED, aborted and terminally completed contexts,
//!   converged resources and an exact next statement on the same connection.
//! * `query-lifecycle/json-membership-capacity` freezes the existing
//!   `SET_VAR(query_mem_limit=...)` hint below one RHS copy. A streaming read
//!   of the same RHS and a membership over the small RHS both pass under that
//!   limit first; the large membership must then fail with the typed
//!   `CAPACITY_REFUSED` task-failure category on a single attempt, release
//!   every context and leave the same connection usable.
//!
//! The oracle follows the frozen three-valued table: an empty RHS answers
//! FALSE (NOT IN: TRUE) even for an SQL NULL probe; otherwise an SQL NULL
//! probe is UNKNOWN, any matching candidate is TRUE, and a non-matching RHS
//! that contains an SQL NULL is UNKNOWN. JSON value equality is serde_json
//! value equality; the inputs use only integers, strings, arrays and objects
//! whose key order differs, so no number, escape or duplicate-key rule is in
//! play. Those rules belong to the `complex-type` golden cases.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{QueryLifecycleStructuredSnapshot, ServerHandle};
use serde_json::{Value, json};

use super::connector::{create_catalog, create_warehouse};
use super::query_lifecycle::{await_resource_convergence, resource_snapshot};
use super::task_evidence;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext};

const REQUIRED_BACKENDS: usize = 3;
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const DATABASE: &str = "json_membership_db";

const TASK_CREATE_APPLIED: &str = "NOVAROCKS_TASK_CREATE_APPLIED";
const TASK_PREPARED_DOP: &str = "NOVAROCKS_TASK_PREPARED_DOP";
const CONTEXT_ABORT_APPLIED: &str = "NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED";
const CONTEXT_TERMINATION_COMPLETED: &str = "NOVAROCKS_TASK_CONTEXT_TERMINATION_COMPLETED";

/// MySQL `ER_QUERY_INTERRUPTED`, the outcome `KILL QUERY` owes its target.
const MYSQL_QUERY_INTERRUPTED: u16 = 1317;
/// MySQL `ER_UNKNOWN_ERROR`, which carries every runtime task failure. The
/// typed task-failure category travels inside its message.
const MYSQL_UNKNOWN_ERROR: u16 = 1105;
/// `TaskFailureCategory::CapacityRefused::as_str()`, the stable rendering of
/// a checked reservation refusal.
const CAPACITY_REFUSED_CATEGORY: &str = "CAPACITY_REFUSED";

/// Three probe commits, one data file each: 60 000 rows.
const PROBE_FILES: [(i64, i64); 3] = [(1, 20_000), (20_001, 40_000), (40_001, 60_000)];
const PROBE_ROWS: i64 = 60_000;
/// Every 97th probe row is an SQL NULL.
const PROBE_NULL_MODULUS: i64 = 97;
/// The probe key cycles through 0..13.
const PROBE_KEY_MODULUS: i64 = 13;

/// Two RHS commits, one data file each, nine rows in total.
///
/// The matches are split across both files -- objects with `k` 1 and 2 plus
/// the array with 3 in the first, the array with 6 and the object with 8 in
/// the second -- so a build that completed before either file's sender
/// reached EOS answers differently. The non-matching candidates differ from a
/// probe value only by array order, one string value or one extra key. The
/// single SQL NULL turns every non-match into UNKNOWN.
type RhsFile = (i64, &'static [Option<&'static str>]);
const RHS_FILES: [RhsFile; 2] = [
    (
        1,
        &[
            Some(r#"{"k":1,"s":"v"}"#),
            Some(r#"{"s":"v","k":2}"#),
            Some(r#"[3,"v"]"#),
            Some(r#"["v",4]"#),
        ],
    ),
    (
        2,
        &[
            Some(r#"{"k":5,"s":"w"}"#),
            Some(r#"[6,"v"]"#),
            Some(r#"{"k":7,"s":"v","x":0}"#),
            Some(r#"{"k":8,"s":"v"}"#),
            None,
        ],
    ),
];
const RHS_ROWS: i64 = 9;

/// Seconds each RHS row sleeps in the cancellation case. Every non-empty
/// producer therefore needs at least this long to reach EOS, so a `KILL`
/// issued sooner always meets a `Building` RHS.
const RHS_SLEEP_SECONDS: u64 = 60;

/// The frozen per-query memory limit of the capacity case: the existing
/// `SET_VAR(query_mem_limit=...)` hint, installed on each backend's query
/// memory tracker that every task of the query charges.
const CAPACITY_QUERY_MEM_LIMIT: i64 = 32 * 1024 * 1024;
/// The large RHS: eight commits of 50 000 rows, each value carrying a
/// 150-byte string. One RHS copy retains at least 400 000 x 150 bytes,
/// nearly twice the frozen limit, while a streaming read holds only bounded
/// batches of it.
const RHS_BIG_FILES: i64 = 8;
const RHS_BIG_ROWS_PER_FILE: i64 = 50_000;
const RHS_BIG_ROWS: i64 = RHS_BIG_FILES * RHS_BIG_ROWS_PER_FILE;
const RHS_BIG_PAD: i64 = 150;
// The capacity input is frozen beyond the limit before any run: the pad bytes
// alone of one RHS copy exceed one and a half times the frozen limit.
const _: () = assert!(RHS_BIG_ROWS * RHS_BIG_PAD > CAPACITY_QUERY_MEM_LIMIT * 3 / 2);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(JsonMembershipFullEos),
        Box::new(JsonMembershipCancel),
        Box::new(JsonMembershipCapacity),
    ]
}

// ---------------------------------------------------------------------------
// query-lifecycle/json-membership-full-eos
// ---------------------------------------------------------------------------

/// Every frozen sender's EOS, including empty senders, completes the RHS.
///
/// The RHS has two data files and is read by one producer task per backend,
/// so at least one producer has no split and only sends EOS. The per-row form
/// lets the optimizer choose the membership distribution; the grouped form
/// keeps the probe distributed and must broadcast the RHS to several placed
/// probe tasks, each of which owns its own copy. All three RHS forms are
/// checked: one containing SQL NULL, one without, and one whose filter selects
/// nothing so that every sender is empty.
struct JsonMembershipFullEos;

impl Scenario for JsonMembershipFullEos {
    fn name(&self) -> &'static str {
        "query-lifecycle/json-membership-full-eos"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let mut control = connect(context, "connect JSON membership fixture session")?;
        let fixture = create_fixture(context, &mut control, "full_eos", false)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline after the fixture commits");

        for variant in RhsVariant::ALL {
            let rhs = variant.rhs_values()?;
            for negated in [false, true] {
                let subject = format!("{} {}", variant.label(), operator(negated));

                let sql = row_query(&fixture, variant, negated);
                let build_edge = membership_build_edge(&explain(&mut control, &sql)?, &sql)?;
                let scan = ExecutionScan::capture(context)?;
                let rows = read_rows(&mut control, &sql)
                    .with_context(|| format!("run per-row {subject} membership"))?;
                assert_rows(&rows, &expected_rows(&rhs, negated), &subject)?;
                let execution = scan.await_single_new(context, &subject)?;
                let participants = complete_across_boundary(context, &execution, &subject)?;
                context.action(format!(
                    "per-row {subject}: {PROBE_ROWS} exact rows from execution {execution} with \
                     {build_edge} RHS input; released contexts on {participants:?}"
                ));

                let sql = group_query(
                    &fixture.probe(),
                    &format!("{}{}", fixture.rhs(), variant.filter()),
                    negated,
                    None,
                );
                require_broadcast_build(&explain(&mut control, &sql)?, &sql)?;
                let scan = ExecutionScan::capture(context)?;
                let groups = read_groups(&mut control, &sql)
                    .with_context(|| format!("run grouped {subject} membership"))?;
                let expected = expected_groups(&rhs, negated);
                ensure!(
                    groups == expected,
                    "grouped {subject} membership returned {groups:?}, expected {expected:?}"
                );
                let execution = scan.await_single_new(context, &subject)?;
                let participants = complete_across_boundary(context, &execution, &subject)?;
                let placement = task_placement(context, &execution)?;
                let full_stages = full_width_stages(&placement);
                ensure!(
                    full_stages.len() >= 2,
                    "grouped {subject} membership did not place both its RHS producers and its \
                     probe tasks on every backend: {placement:?}"
                );
                context.action(format!(
                    "grouped {subject}: exact groups {expected:?} from execution {execution}; \
                     BROADCAST RHS into MEMBERSHIP; stages {full_stages:?} placed on all \
                     {REQUIRED_BACKENDS} backends (RHS {} rows in 2 files over 3 producers); \
                     released contexts on {participants:?}",
                    rhs.len()
                ));
            }
        }

        await_resource_convergence(context, &baseline)
    }
}

// ---------------------------------------------------------------------------
// query-lifecycle/json-membership-cancel
// ---------------------------------------------------------------------------

/// `KILL QUERY` while every placed probe task waits on a `Building` RHS.
///
/// The RHS filter sleeps per row, so no producer with a split can reach EOS
/// for at least `RHS_SLEEP_SECONDS`, while the empty producers finish at once.
/// The kill is issued after every created task of the attempt has installed
/// its pipeline and before that bound; the statement must then end with
/// ER_QUERY_INTERRUPTED well before any producer could have finished, every
/// context the attempt established must record its abort and its terminal
/// completion, the resource oracle must converge, and the next statement on
/// the same connection must return its exact result.
struct JsonMembershipCancel;

impl Scenario for JsonMembershipCancel {
    fn name(&self) -> &'static str {
        "query-lifecycle/json-membership-cancel"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let mut control = connect(context, "connect JSON membership fixture session")?;
        let fixture = create_fixture(context, &mut control, "cancel", false)?;
        let held_sql = group_query(
            &fixture.probe(),
            &format!("{} WHERE sleep({RHS_SLEEP_SECONDS})", fixture.rhs()),
            false,
            None,
        );
        let follow_sql = group_query(&fixture.probe(), &fixture.rhs(), false, None);
        require_broadcast_build(&explain(&mut control, &held_sql)?, &held_sql)?;
        let expected_follow = expected_groups(&RhsVariant::All.rhs_values()?, false);
        let baseline = resource_snapshot(context)?;
        let scan = ExecutionScan::capture(context)?;

        let session = HeldSession::start(context, held_sql)?;
        let started = Instant::now();
        context.action(format!(
            "started a broadcast JSON membership whose RHS producers sleep {RHS_SLEEP_SECONDS}s \
             per row, on connection {}",
            session.connection_id
        ));
        let execution = await_installed_on_every_backend(context, &scan, &session)?;
        let waited = started.elapsed();
        ensure!(
            waited < Duration::from_secs(RHS_SLEEP_SECONDS),
            "the held membership took {waited:?} to install, so its RHS could already be complete"
        );
        let placement = task_placement(context, &execution)?;
        context.action(format!(
            "execution {execution} installed every created task after {waited:?}, before any \
             RHS producer with a split could reach EOS; placement {placement:?}"
        ));

        let deadline = context.deadline();
        context
            .handle()
            .kill_query_until(session.connection_id, deadline)
            .context("KILL QUERY the held JSON membership")?;
        let killed_at = Instant::now();
        let outcome = session
            .first
            .recv_timeout(context.remaining("await the killed JSON membership")?)
            .context("the killed JSON membership did not end")?;
        let cancelled_after = killed_at.elapsed();
        match outcome {
            Ok(rows) => bail!("the killed JSON membership returned {rows:?}"),
            Err(mysql::Error::MySqlError(error)) if error.code == MYSQL_QUERY_INTERRUPTED => {}
            Err(other) => bail!(
                "expected MySQL cancellation error {MYSQL_QUERY_INTERRUPTED}, received {other}"
            ),
        }
        ensure!(
            cancelled_after < Duration::from_secs(RHS_SLEEP_SECONDS),
            "KILL QUERY took {cancelled_after:?}, as long as a sleeping RHS producer"
        );
        context.action(format!(
            "KILL QUERY ended execution {execution} with ER_QUERY_INTERRUPTED after \
             {cancelled_after:?}"
        ));

        let contexts = await_aborted_contexts(context, &execution, "killed JSON membership")?;
        await_resource_convergence(context, &baseline)?;
        context.action(format!(
            "every established context of {execution} on {contexts:?} recorded its abort and its \
             terminal completion; query resources converged"
        ));

        let scan = ExecutionScan::capture(context)?;
        session
            .proceed
            .send(follow_sql)
            .context("ask the killed connection for its next statement")?;
        let follow = session
            .second
            .recv_timeout(context.remaining("await the statement after KILL")?)
            .context("the statement after KILL did not end")?
            .context("the statement after KILL failed on the same connection")?;
        session
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("the held JSON membership session panicked"))??;
        let follow = decode_groups(follow)?;
        ensure!(
            follow == expected_follow,
            "the statement after KILL returned {follow:?}, expected {expected_follow:?}"
        );
        let next = scan.await_single_new(context, "statement after KILL")?;
        let participants = complete_across_boundary(context, &next, "statement after KILL")?;
        context.action(format!(
            "the same connection then ran execution {next} to its exact groups \
             {expected_follow:?}; released contexts on {participants:?}"
        ));
        await_resource_convergence(context, &baseline)
    }
}

type GroupRow = (Option<i64>, i64, i64, i64);

/// One connection that runs a held statement and, once told, one more.
struct HeldSession {
    connection_id: u32,
    first: mpsc::Receiver<std::result::Result<Vec<GroupRow>, mysql::Error>>,
    proceed: mpsc::SyncSender<String>,
    second: mpsc::Receiver<std::result::Result<Vec<GroupRow>, mysql::Error>>,
    thread: thread::JoinHandle<Result<()>>,
}

impl HeldSession {
    fn start(context: &mut ScenarioContext, sql: String) -> Result<Self> {
        let (id_tx, id_rx) = mpsc::sync_channel(1);
        let (first_tx, first) = mpsc::sync_channel(1);
        let (proceed, proceed_rx) = mpsc::sync_channel::<String>(1);
        let (second_tx, second) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let connect_timeout = bounded_io_timeout(context, "connect the held JSON membership")?;
        let thread = thread::Builder::new()
            .name("json-membership-held-session".to_owned())
            .spawn(move || -> Result<()> {
                // The held statement waits for another session's KILL, so its
                // wait is bounded by the scenario, not by a socket timeout.
                let mut connection =
                    mysql_actor::connect_for_cancellation(&user, port, connect_timeout)?;
                id_tx
                    .send(connection.connection_id())
                    .context("publish the held session connection id")?;
                first_tx
                    .send(connection.query::<GroupRow, _>(sql))
                    .context("publish the held statement outcome")?;
                let Ok(next) = proceed_rx.recv() else {
                    return Ok(());
                };
                second_tx
                    .send(connection.query::<GroupRow, _>(next))
                    .context("publish the next statement outcome")
            })
            .context("start the held JSON membership session")?;
        let connection_id = id_rx
            .recv_timeout(context.remaining("receive the held session connection id")?)
            .context("the held session ended before publishing its connection id")?;
        Ok(Self {
            connection_id,
            first,
            proceed,
            second,
            thread,
        })
    }
}

/// Waits until the new attempt has created tasks on every backend and every
/// created task has installed its pipeline, with at least two stages placed
/// on every backend: the RHS producers and the broadcast-fed probe tasks.
fn await_installed_on_every_backend(
    context: &mut ScenarioContext,
    scan: &ExecutionScan,
    session: &HeldSession,
) -> Result<String> {
    loop {
        let created = scan.new_executions(context, TASK_CREATE_APPLIED)?;
        ensure!(
            created.len() <= 1,
            "the held membership started more than one attempt: {created:?}"
        );
        if let Some((execution, created)) = created.into_iter().next() {
            let installed = stage_backends(context, TASK_PREPARED_DOP, &execution)?;
            let all_installed = created.iter().all(|(stage, backends)| {
                installed
                    .get(stage)
                    .is_some_and(|ready| backends.is_subset(ready))
            });
            if all_installed && full_width_stages(&installed).len() >= 2 {
                return Ok(execution);
            }
        }
        match session.first.try_recv() {
            Err(mpsc::TryRecvError::Empty) => {}
            Ok(outcome) => bail!("the held membership ended before KILL QUERY: {outcome:?}"),
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("the held membership session disconnected before KILL QUERY")
            }
        }
        let remaining =
            context.remaining("observe the held membership installed on every backend")?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}

/// Every context the attempt established recorded its abort, and every one
/// of them completed termination: all of its tasks are terminal records.
fn await_aborted_contexts(
    context: &mut ScenarioContext,
    execution: &str,
    subject: &str,
) -> Result<BTreeSet<usize>> {
    loop {
        let established = task_evidence::backends_with_marker(
            context,
            task_evidence::CONTEXT_ESTABLISH_APPLIED,
            execution,
        )?;
        let aborted =
            task_evidence::backends_with_marker(context, CONTEXT_ABORT_APPLIED, execution)?;
        let completed =
            task_evidence::backends_with_marker(context, CONTEXT_TERMINATION_COMPLETED, execution)?;
        if !established.is_empty()
            && established.is_subset(&aborted)
            && established.is_subset(&completed)
        {
            return Ok(established);
        }
        let remaining = context.remaining(&format!(
            "{subject} {execution} awaiting context termination (established={established:?}, \
             aborted={aborted:?}, completed={completed:?})"
        ))?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}

// ---------------------------------------------------------------------------
// query-lifecycle/json-membership-capacity
// ---------------------------------------------------------------------------

/// A JSON membership whose RHS cannot fit the frozen per-query memory limit.
///
/// The limit is the existing `query_mem_limit` hint. Two controls run under
/// the same limit before the refusal: a streaming read of every large-RHS
/// value, which holds only bounded batches, and the grouped membership over
/// the small RHS with its exact result. The grouped membership over the large
/// RHS must then fail with the typed `CAPACITY_REFUSED` category, which the
/// frontend classifies as resource governance and never retries: exactly one
/// attempt exists. Every established context must leave `Active` through an
/// abort that completes or a release, resources must converge, and the next
/// statement on the same connection must return its exact result.
struct JsonMembershipCapacity;

impl Scenario for JsonMembershipCapacity {
    fn name(&self) -> &'static str {
        "query-lifecycle/json-membership-capacity"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let mut control = connect(context, "connect JSON membership fixture session")?;
        let fixture = create_fixture(context, &mut control, "capacity", true)?;
        let mut session = connect(context, "connect JSON membership capacity session")?;
        let expected_small = expected_groups(&RhsVariant::All.rhs_values()?, false);

        let streaming_sql = streaming_control_query(&fixture.rhs_big());
        let streamed: Vec<i64> = session
            .query(&streaming_sql)
            .with_context(|| format!("run the streaming control: {streaming_sql}"))?;
        ensure!(
            streamed == [RHS_BIG_ROWS],
            "the streaming control returned {streamed:?}, expected [{RHS_BIG_ROWS}]"
        );
        let small_sql = group_query(
            &fixture.probe(),
            &fixture.rhs(),
            false,
            Some(CAPACITY_QUERY_MEM_LIMIT),
        );
        let small = read_groups(&mut session, &small_sql)
            .context("run the small-RHS membership under the frozen limit")?;
        ensure!(
            small == expected_small,
            "the small-RHS membership under the frozen limit returned {small:?}, expected \
             {expected_small:?}"
        );
        context.action(format!(
            "under query_mem_limit={CAPACITY_QUERY_MEM_LIMIT}: streamed all {RHS_BIG_ROWS} \
             large-RHS values and returned the exact small-RHS membership groups"
        ));

        let refused_sql = group_query(
            &fixture.probe(),
            &fixture.rhs_big(),
            false,
            Some(CAPACITY_QUERY_MEM_LIMIT),
        );
        let build_edge =
            membership_build_edge(&explain(&mut control, &refused_sql)?, &refused_sql)?;
        let baseline = resource_snapshot(context)?;
        let scan = ExecutionScan::capture(context)?;
        let error = match read_groups_raw(&mut session, &refused_sql) {
            Ok(rows) => bail!(
                "a membership retaining at least {} RHS bytes per copy fit \
                 query_mem_limit={CAPACITY_QUERY_MEM_LIMIT}: {rows:?}",
                RHS_BIG_ROWS * RHS_BIG_PAD
            ),
            Err(error) => error,
        };
        let message = match &error {
            mysql::Error::MySqlError(error) if error.code == MYSQL_UNKNOWN_ERROR => {
                error.message.clone()
            }
            other => bail!(
                "expected a MySQL {MYSQL_UNKNOWN_ERROR} task failure for the capacity refusal, \
                 received {other}"
            ),
        };
        ensure!(
            message.contains(CAPACITY_REFUSED_CATEGORY),
            "the refused membership did not carry the {CAPACITY_REFUSED_CATEGORY} task-failure \
             category: {message}"
        );
        let execution = scan.await_single_new(context, "capacity-refused membership")?;
        ensure!(
            execution.ends_with(":1"),
            "a capacity refusal must not start a replacement attempt: {execution}"
        );
        context.action(format!(
            "the {build_edge} membership over {RHS_BIG_ROWS} large-RHS rows failed on its only \
             attempt {execution} with {CAPACITY_REFUSED_CATEGORY}: {message}"
        ));

        let contexts = await_left_active(context, &execution, "capacity-refused membership")?;
        await_resource_convergence(context, &baseline)?;
        context.action(format!(
            "every established context of {execution} on {contexts:?} left Active; query \
             resources converged"
        ));

        let follow_sql = group_query(&fixture.probe(), &fixture.rhs(), false, None);
        let scan = ExecutionScan::capture(context)?;
        let follow = read_groups(&mut session, &follow_sql)
            .context("run the statement after the capacity refusal on the same connection")?;
        ensure!(
            follow == expected_small,
            "the statement after the capacity refusal returned {follow:?}, expected \
             {expected_small:?}"
        );
        let next = scan.await_single_new(context, "statement after capacity refusal")?;
        let participants =
            complete_across_boundary(context, &next, "statement after capacity refusal")?;
        context.action(format!(
            "the same connection then ran execution {next} to its exact groups; released \
             contexts on {participants:?}"
        ));
        await_resource_convergence(context, &baseline)
    }
}

/// Every context the attempt established left `Active`: either its abort
/// applied and completed with every task a terminal record, or it released.
fn await_left_active(
    context: &mut ScenarioContext,
    execution: &str,
    subject: &str,
) -> Result<BTreeSet<usize>> {
    loop {
        let established = task_evidence::backends_with_marker(
            context,
            task_evidence::CONTEXT_ESTABLISH_APPLIED,
            execution,
        )?;
        let aborted =
            task_evidence::backends_with_marker(context, CONTEXT_ABORT_APPLIED, execution)?;
        let completed =
            task_evidence::backends_with_marker(context, CONTEXT_TERMINATION_COMPLETED, execution)?;
        let released = task_evidence::backends_with_marker(
            context,
            task_evidence::RELEASE_APPLIED,
            execution,
        )?;
        if !established.is_empty()
            && established.iter().all(|backend| {
                (aborted.contains(backend) && completed.contains(backend))
                    || released.contains(backend)
            })
        {
            return Ok(established);
        }
        let remaining = context.remaining(&format!(
            "{subject} {execution} awaiting its contexts to leave Active (established=\
             {established:?}, aborted={aborted:?}, completed={completed:?}, released={released:?})"
        ))?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    catalog: String,
}

impl Fixture {
    fn probe(&self) -> String {
        format!("{}.{DATABASE}.probe", self.catalog)
    }

    fn rhs(&self) -> String {
        format!("{}.{DATABASE}.rhs", self.catalog)
    }

    fn rhs_big(&self) -> String {
        format!("{}.{DATABASE}.rhs_big", self.catalog)
    }
}

/// Creates the persisted JSON tables in a local Hadoop Iceberg warehouse.
/// Every INSERT is one commit and one data file.
fn create_fixture(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    label: &str,
    with_big_rhs: bool,
) -> Result<Fixture> {
    let warehouse = create_warehouse(context, &format!("json-membership-{label}"))?;
    let fixture = Fixture {
        catalog: format!("json_membership_{label}"),
    };
    create_catalog(control, &fixture.catalog, &warehouse)?;
    execute(
        control,
        &format!("CREATE DATABASE {}.{DATABASE}", fixture.catalog),
    )?;
    execute(
        control,
        &format!(
            "CREATE TABLE {} (id BIGINT NOT NULL, j JSON) TBLPROPERTIES (\"format-version\" = \"3\")",
            fixture.probe()
        ),
    )?;
    for (first, last) in PROBE_FILES {
        execute(control, &probe_insert(&fixture.probe(), first, last))?;
    }
    execute(
        control,
        &format!(
            "CREATE TABLE {} (grp BIGINT NOT NULL, r JSON) TBLPROPERTIES (\"format-version\" = \"3\")",
            fixture.rhs()
        ),
    )?;
    for (grp, values) in RHS_FILES {
        execute(control, &rhs_insert(&fixture.rhs(), grp, values))?;
    }
    let probe_nulls = PROBE_ROWS / PROBE_NULL_MODULUS;
    let counted: Vec<(i64, i64)> = control
        .query(format!(
            "SELECT count(*), count(j) FROM {}",
            fixture.probe()
        ))
        .context("count the committed probe rows")?;
    ensure!(
        counted == [(PROBE_ROWS, PROBE_ROWS - probe_nulls)],
        "probe fixture holds {counted:?}, expected [({PROBE_ROWS}, {})]",
        PROBE_ROWS - probe_nulls
    );
    let counted: Vec<(i64, i64)> = control
        .query(format!("SELECT count(*), count(r) FROM {}", fixture.rhs()))
        .context("count the committed RHS rows")?;
    ensure!(
        counted == [(RHS_ROWS, RHS_ROWS - 1)],
        "RHS fixture holds {counted:?}, expected [({RHS_ROWS}, {})]",
        RHS_ROWS - 1
    );
    if with_big_rhs {
        execute(
            control,
            &format!(
                "CREATE TABLE {} (id BIGINT NOT NULL, r JSON) TBLPROPERTIES (\"format-version\" = \"3\")",
                fixture.rhs_big()
            ),
        )?;
        for file in 0..RHS_BIG_FILES {
            let first = file * RHS_BIG_ROWS_PER_FILE + 1;
            let last = first + RHS_BIG_ROWS_PER_FILE - 1;
            execute(control, &rhs_big_insert(&fixture.rhs_big(), first, last))?;
        }
    }
    context.action(format!(
        "created {label} fixture in {}: probe {PROBE_ROWS} rows in {} files, RHS {RHS_ROWS} rows \
         in {} files{}",
        fixture.catalog,
        PROBE_FILES.len(),
        RHS_FILES.len(),
        if with_big_rhs {
            format!(", large RHS {RHS_BIG_ROWS} rows in {RHS_BIG_FILES} files")
        } else {
            String::new()
        }
    ));
    Ok(fixture)
}

/// The probe value of row `id`, as [`probe_value`] states it.
fn probe_insert(table: &str, first: i64, last: i64) -> String {
    format!(
        "INSERT INTO {table} SELECT i, CASE \
         WHEN i % {PROBE_NULL_MODULUS} = 0 THEN NULL \
         WHEN i % 3 = 0 THEN json_object('k', i % {PROBE_KEY_MODULUS}, 's', 'v') \
         WHEN i % 3 = 1 THEN json_object('s', 'v', 'k', i % {PROBE_KEY_MODULUS}) \
         ELSE parse_json(concat('[', CAST(i % {PROBE_KEY_MODULUS} AS VARCHAR), ',\"v\"]')) END \
         FROM TABLE(generate_series({first}, {last})) AS g(i)"
    )
}

fn rhs_insert(table: &str, grp: i64, values: &[Option<&str>]) -> String {
    let rows = values
        .iter()
        .map(|value| match value {
            Some(text) => format!("({grp}, parse_json('{text}'))"),
            None => format!("({grp}, NULL)"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} VALUES {rows}")
}

fn rhs_big_insert(table: &str, first: i64, last: i64) -> String {
    format!(
        "INSERT INTO {table} SELECT i, json_object('k', i, 'pad', repeat('x', {RHS_BIG_PAD})) \
         FROM TABLE(generate_series({first}, {last})) AS g(i)"
    )
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RhsVariant {
    /// Every RHS row, including the SQL NULL.
    All,
    /// Every non-NULL RHS row.
    NonNull,
    /// No row: the filter selects nothing, so every producer is empty.
    Empty,
}

impl RhsVariant {
    const ALL: [Self; 3] = [Self::All, Self::NonNull, Self::Empty];

    const fn label(self) -> &'static str {
        match self {
            Self::All => "rhs-with-null",
            Self::NonNull => "rhs-without-null",
            Self::Empty => "empty-rhs",
        }
    }

    const fn filter(self) -> &'static str {
        match self {
            Self::All => "",
            Self::NonNull => " WHERE r IS NOT NULL",
            Self::Empty => " WHERE grp > 2",
        }
    }

    /// The rows this variant's SQL filter keeps.
    fn admits(self, grp: i64, value: Option<&str>) -> bool {
        match self {
            Self::All => true,
            Self::NonNull => value.is_some(),
            Self::Empty => grp > 2,
        }
    }

    fn rhs_values(self) -> Result<Vec<Option<Value>>> {
        RHS_FILES
            .iter()
            .flat_map(|(grp, values)| values.iter().map(move |value| (*grp, *value)))
            .filter(|(grp, value)| self.admits(*grp, *value))
            .map(|(_, value)| {
                value
                    .map(|text| {
                        serde_json::from_str(text)
                            .with_context(|| format!("parse RHS fixture value {text}"))
                    })
                    .transpose()
            })
            .collect()
    }
}

const fn operator(negated: bool) -> &'static str {
    if negated { "NOT IN" } else { "IN" }
}

fn row_query(fixture: &Fixture, variant: RhsVariant, negated: bool) -> String {
    format!(
        "SELECT id, j {} (SELECT r FROM {}{}) AS m FROM {}",
        operator(negated),
        fixture.rhs(),
        variant.filter(),
        fixture.probe()
    )
}

/// Groups the membership result so the probe stays distributed: the local
/// aggregate shrinks each placed probe task's output, which is what makes a
/// broadcast RHS cheaper than gathering every probe row to one task.
fn group_query(probe: &str, rhs: &str, negated: bool, mem_limit: Option<i64>) -> String {
    let hint = mem_limit
        .map(|bytes| format!(" /*+ SET_VAR(query_mem_limit={bytes}) */"))
        .unwrap_or_default();
    format!(
        "SELECT{hint} m, count(*) AS c, CAST(sum(id) AS BIGINT) AS s, \
         CAST(sum(id * id) AS BIGINT) AS q \
         FROM (SELECT id, j {} (SELECT r FROM {rhs}) AS m FROM {probe}) t GROUP BY m",
        operator(negated)
    )
}

/// Reads every large-RHS value without retaining it: no value is shorter
/// than its 150-byte pad, so all of them must be counted.
fn streaming_control_query(rhs_big: &str) -> String {
    format!(
        "SELECT /*+ SET_VAR(query_mem_limit={CAPACITY_QUERY_MEM_LIMIT}) */ count(*) \
         FROM {rhs_big} WHERE length(CAST(r AS VARCHAR)) > {RHS_BIG_PAD}"
    )
}

fn explain(control: &mut mysql::Conn, sql: &str) -> Result<String> {
    let lines: Vec<String> = control
        .query(format!("EXPLAIN {sql}"))
        .with_context(|| format!("EXPLAIN {sql}"))?;
    Ok(lines.join("\n"))
}

/// Which input feeds the membership's RHS. A broadcast RHS appears as the
/// plan's only `BROADCAST EXCHANGE`; otherwise both inputs are gathered.
fn membership_build_edge(explain: &str, sql: &str) -> Result<&'static str> {
    ensure!(
        explain.contains("MEMBERSHIP"),
        "the plan has no dedicated MEMBERSHIP node for {sql}: {explain}"
    );
    Ok(if explain.contains("BROADCAST EXCHANGE") {
        "BROADCAST"
    } else {
        "GATHER"
    })
}

fn require_broadcast_build(explain: &str, sql: &str) -> Result<()> {
    ensure!(
        membership_build_edge(explain, sql)? == "BROADCAST",
        "the grouped membership did not broadcast its RHS to placed probe tasks for {sql}: \
         {explain}"
    );
    Ok(())
}

fn read_rows(connection: &mut mysql::Conn, sql: &str) -> Result<BTreeMap<i64, Option<bool>>> {
    let rows: Vec<(i64, Option<i64>)> = connection.query(sql)?;
    let mut decoded = BTreeMap::new();
    for (id, value) in rows {
        ensure!(
            decoded.insert(id, decode_bool(value)?).is_none(),
            "membership output repeated probe row {id}"
        );
    }
    Ok(decoded)
}

fn read_groups_raw(
    connection: &mut mysql::Conn,
    sql: &str,
) -> std::result::Result<Vec<GroupRow>, mysql::Error> {
    connection.query(sql)
}

fn read_groups(
    connection: &mut mysql::Conn,
    sql: &str,
) -> Result<BTreeMap<Option<bool>, GroupFacts>> {
    decode_groups(read_groups_raw(connection, sql)?)
}

fn decode_groups(rows: Vec<GroupRow>) -> Result<BTreeMap<Option<bool>, GroupFacts>> {
    let mut groups = BTreeMap::new();
    for (value, count, sum, sum_squares) in rows {
        let key = decode_bool(value)?;
        ensure!(
            groups
                .insert(
                    key,
                    GroupFacts {
                        count,
                        sum,
                        sum_squares
                    }
                )
                .is_none(),
            "membership groups repeated {key:?}"
        );
    }
    Ok(groups)
}

/// A nullable Boolean travels as MySQL TINY 1 / 0 / NULL.
fn decode_bool(value: Option<i64>) -> Result<Option<bool>> {
    match value {
        None => Ok(None),
        Some(0) => Ok(Some(false)),
        Some(1) => Ok(Some(true)),
        Some(other) => bail!("membership result {other} is not a Boolean"),
    }
}

fn assert_rows(
    actual: &BTreeMap<i64, Option<bool>>,
    expected: &BTreeMap<i64, Option<bool>>,
    subject: &str,
) -> Result<()> {
    if actual == expected {
        return Ok(());
    }
    let missing = expected
        .keys()
        .filter(|id| !actual.contains_key(id))
        .take(8)
        .collect::<Vec<_>>();
    let unexpected = actual
        .keys()
        .filter(|id| !expected.contains_key(id))
        .take(8)
        .collect::<Vec<_>>();
    let wrong = expected
        .iter()
        .filter_map(|(id, value)| {
            actual
                .get(id)
                .filter(|found| *found != value)
                .map(|found| (*id, *value, *found))
        })
        .take(8)
        .collect::<Vec<_>>();
    bail!(
        "per-row {subject} membership returned {} rows, expected {}; missing {missing:?}, \
         unexpected {unexpected:?}, (id, expected, actual) {wrong:?}",
        actual.len(),
        expected.len()
    )
}

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

/// The probe value of row `id`, exactly as [`probe_insert`] writes it.
fn probe_value(id: i64) -> Option<Value> {
    if id % PROBE_NULL_MODULUS == 0 {
        return None;
    }
    let key = id % PROBE_KEY_MODULUS;
    Some(match id % 3 {
        0 | 1 => json!({"k": key, "s": "v"}),
        _ => json!([key, "v"]),
    })
}

/// The three-valued membership of one probe value, negated once at the end.
fn membership(probe: Option<&Value>, rhs: &[Option<Value>], negated: bool) -> Option<bool> {
    let positive = if rhs.is_empty() {
        Some(false)
    } else if let Some(probe) = probe {
        if rhs.iter().flatten().any(|candidate| candidate == probe) {
            Some(true)
        } else if rhs.iter().any(Option::is_none) {
            None
        } else {
            Some(false)
        }
    } else {
        None
    };
    positive.map(|value| value != negated)
}

fn expected_rows(rhs: &[Option<Value>], negated: bool) -> BTreeMap<i64, Option<bool>> {
    (1..=PROBE_ROWS)
        .map(|id| (id, membership(probe_value(id).as_ref(), rhs, negated)))
        .collect()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct GroupFacts {
    count: i64,
    sum: i64,
    sum_squares: i64,
}

fn expected_groups(rhs: &[Option<Value>], negated: bool) -> BTreeMap<Option<bool>, GroupFacts> {
    let mut groups = BTreeMap::<Option<bool>, GroupFacts>::new();
    for (id, value) in expected_rows(rhs, negated) {
        let group = groups.entry(value).or_default();
        group.count += 1;
        group.sum += id;
        group.sum_squares += id * id;
    }
    groups
}

// ---------------------------------------------------------------------------
// Task-protocol evidence
// ---------------------------------------------------------------------------

/// The execution identities that already created tasks before a statement,
/// so the statement's own attempt is the one that appears afterwards.
struct ExecutionScan {
    known: BTreeSet<String>,
}

impl ExecutionScan {
    /// Backend markers reach the harness through an asynchronous log pump,
    /// so the earlier statements' creates are read until two consecutive
    /// reads agree; a straggler cannot then pose as the next attempt.
    fn capture(context: &mut ScenarioContext) -> Result<Self> {
        let mut known = Self::read_known(context)?;
        loop {
            thread::sleep(
                context
                    .remaining("settle the task creates of earlier statements")?
                    .min(POLL_INTERVAL),
            );
            let again = Self::read_known(context)?;
            if again == known {
                return Ok(Self { known });
            }
            known = again;
        }
    }

    fn read_known(context: &mut ScenarioContext) -> Result<BTreeSet<String>> {
        let mut known = BTreeSet::new();
        for index in 0..context.handle().be_count() {
            let log = context
                .handle()
                .be_log_contents(index)
                .with_context(|| format!("read BE[{index}] log for task creates"))?;
            known.extend(
                log.lines()
                    .filter(|line| line.contains(TASK_CREATE_APPLIED))
                    .filter_map(|line| marker_field(line, "execution_id"))
                    .map(str::to_owned),
            );
        }
        Ok(known)
    }

    /// New executions with `marker`, by stage, by backend.
    fn new_executions(
        &self,
        context: &mut ScenarioContext,
        marker: &str,
    ) -> Result<BTreeMap<String, BTreeMap<String, BTreeSet<usize>>>> {
        let mut executions = BTreeMap::<String, BTreeMap<String, BTreeSet<usize>>>::new();
        for index in 0..context.handle().be_count() {
            let log = context
                .handle()
                .be_log_contents(index)
                .with_context(|| format!("read BE[{index}] log for {marker}"))?;
            for line in log.lines().filter(|line| line.contains(marker)) {
                let (Some(execution), Some(stage)) = (
                    marker_field(line, "execution_id"),
                    marker_field(line, "stage"),
                ) else {
                    continue;
                };
                if self.known.contains(execution) {
                    continue;
                }
                executions
                    .entry(execution.to_owned())
                    .or_default()
                    .entry(stage.to_owned())
                    .or_default()
                    .insert(index);
            }
        }
        Ok(executions)
    }

    /// The one attempt a completed statement created. A second attempt would
    /// be a replacement, which none of these statements may start.
    fn await_single_new(&self, context: &mut ScenarioContext, subject: &str) -> Result<String> {
        loop {
            let executions = self.new_executions(context, TASK_CREATE_APPLIED)?;
            match executions.len() {
                0 => {}
                1 => {
                    return Ok(executions
                        .into_keys()
                        .next()
                        .expect("one execution was counted"));
                }
                _ => bail!(
                    "{subject} created tasks under more than one attempt: {:?}",
                    executions.keys().collect::<Vec<_>>()
                ),
            }
            let remaining = context.remaining(&format!("observe the task creates of {subject}"))?;
            thread::sleep(remaining.min(POLL_INTERVAL));
        }
    }
}

/// Stage to backends for every `marker` line of exactly `execution`.
fn stage_backends(
    context: &mut ScenarioContext,
    marker: &str,
    execution: &str,
) -> Result<BTreeMap<String, BTreeSet<usize>>> {
    let mut stages = BTreeMap::<String, BTreeSet<usize>>::new();
    for index in 0..context.handle().be_count() {
        let log = context
            .handle()
            .be_log_contents(index)
            .with_context(|| format!("read BE[{index}] log for {marker}"))?;
        for line in log.lines().filter(|line| line.contains(marker)) {
            if marker_field(line, "execution_id") != Some(execution) {
                continue;
            }
            if let Some(stage) = marker_field(line, "stage") {
                stages.entry(stage.to_owned()).or_default().insert(index);
            }
        }
    }
    Ok(stages)
}

/// Where the attempt placed its tasks: stage to the backends that admitted
/// one of its tasks.
fn task_placement(
    context: &mut ScenarioContext,
    execution: &str,
) -> Result<BTreeMap<String, BTreeSet<usize>>> {
    stage_backends(context, TASK_CREATE_APPLIED, execution)
}

/// Stages with one task on every backend. A fragment places at most one task
/// per backend, so these are the scan-driven stages.
fn full_width_stages(placement: &BTreeMap<String, BTreeSet<usize>>) -> Vec<String> {
    placement
        .iter()
        .filter(|(_, backends)| backends.len() == REQUIRED_BACKENDS)
        .map(|(stage, _)| stage.clone())
        .collect()
}

/// The value of ` key=value` in one marker line.
fn marker_field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|word| word.strip_prefix(key)?.strip_prefix('='))
}

/// The attempt's terminal projection, then the shared cross-boundary proof:
/// every context it established released.
fn complete_across_boundary(
    context: &mut ScenarioContext,
    execution: &str,
    subject: &str,
) -> Result<BTreeSet<usize>> {
    let snapshot = await_snapshot_for(context, execution, subject)?;
    task_evidence::assert_query_completed_across_boundary(context, &snapshot, subject)
}

/// The frontend publishes terminal projections in completion order, so the
/// statement's own projection is the latest one once it appears.
fn await_snapshot_for(
    context: &mut ScenarioContext,
    execution: &str,
    subject: &str,
) -> Result<QueryLifecycleStructuredSnapshot> {
    // A read can race the publication, so a failed read is retried until the
    // scenario deadline, as the harness's own snapshot wait does.
    let mut latest = None;
    let mut latest_error = None;
    loop {
        match context.handle().query_lifecycle_structured_snapshot() {
            Ok(Some(snapshot)) if snapshot.execution_id.as_deref() == Some(execution) => {
                return Ok(snapshot);
            }
            Ok(Some(snapshot)) => latest = snapshot.execution_id,
            Ok(None) => {}
            Err(error) => latest_error = Some(format!("{error:#}")),
        }
        let remaining = context.remaining(&format!(
            "observe the terminal projection of {subject} {execution} (latest={latest:?}, \
             latest_error={latest_error:?})"
        ))?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}

// ---------------------------------------------------------------------------
// Common
// ---------------------------------------------------------------------------

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

fn connect(context: &ScenarioContext, operation: &str) -> Result<mysql::Conn> {
    mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining(operation)?,
    )
}

fn bounded_io_timeout(context: &ScenarioContext, operation: &str) -> Result<Duration> {
    Ok(context.remaining(operation)?.min(Duration::from_secs(10)))
}

fn execute(control: &mut mysql::Conn, sql: &str) -> Result<()> {
    control
        .query_drop(sql)
        .with_context(|| format!("execute {sql}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rhs(texts: &[Option<&str>]) -> Vec<Option<Value>> {
        texts
            .iter()
            .map(|text| text.map(|text| serde_json::from_str(text).unwrap()))
            .collect()
    }

    #[test]
    fn oracle_follows_the_three_valued_membership_table() {
        let probe = json!({"a": 1, "b": 2});
        let matching = rhs(&[Some(r#"{"b":2,"a":1}"#), None]);
        let missing = rhs(&[Some(r#"{"a":1}"#), None]);
        let all_false = rhs(&[Some(r#"{"a":1}"#), Some("[1,2]")]);
        // Empty RHS, including an SQL NULL probe.
        assert_eq!(membership(Some(&probe), &[], false), Some(false));
        assert_eq!(membership(None, &[], false), Some(false));
        assert_eq!(membership(None, &[], true), Some(true));
        // At least one TRUE wins over UNKNOWN; no TRUE with UNKNOWN is NULL.
        assert_eq!(membership(Some(&probe), &matching, false), Some(true));
        assert_eq!(membership(Some(&probe), &matching, true), Some(false));
        assert_eq!(membership(Some(&probe), &missing, false), None);
        assert_eq!(membership(Some(&probe), &missing, true), None);
        // All FALSE, and an SQL NULL probe against a non-empty RHS.
        assert_eq!(membership(Some(&probe), &all_false, false), Some(false));
        assert_eq!(membership(Some(&probe), &all_false, true), Some(true));
        assert_eq!(membership(None, &all_false, false), None);
        // JSON null is a value, not SQL NULL.
        let json_null = rhs(&[Some("null")]);
        assert_eq!(
            membership(Some(&Value::Null), &json_null, false),
            Some(true)
        );
    }

    #[test]
    fn every_fixture_class_is_reached_and_counts_cover_the_probe() {
        let classes = |variant: RhsVariant, negated: bool| {
            let groups = expected_groups(&variant.rhs_values().unwrap(), negated);
            assert_eq!(
                groups.values().map(|group| group.count).sum::<i64>(),
                PROBE_ROWS
            );
            groups.keys().copied().collect::<Vec<_>>()
        };
        assert_eq!(classes(RhsVariant::All, false), [None, Some(true)]);
        assert_eq!(classes(RhsVariant::All, true), [None, Some(false)]);
        assert_eq!(
            classes(RhsVariant::NonNull, false),
            [None, Some(false), Some(true)]
        );
        assert_eq!(
            classes(RhsVariant::NonNull, true),
            [None, Some(false), Some(true)]
        );
        assert_eq!(classes(RhsVariant::Empty, false), [Some(false)]);
        assert_eq!(classes(RhsVariant::Empty, true), [Some(true)]);
        assert_eq!(RhsVariant::All.rhs_values().unwrap().len() as i64, RHS_ROWS);
        assert_eq!(
            RhsVariant::NonNull.rhs_values().unwrap().len() as i64,
            RHS_ROWS - 1
        );
        assert!(RhsVariant::Empty.rhs_values().unwrap().is_empty());
    }

    #[test]
    fn the_result_depends_on_every_rhs_file() {
        // A build that published Complete before either file's sender reached
        // EOS would answer differently, with or without the SQL NULL.
        for variant in [RhsVariant::All, RhsVariant::NonNull] {
            let complete = expected_rows(&variant.rhs_values().unwrap(), false);
            for skipped in 0..RHS_FILES.len() {
                let partial = RHS_FILES
                    .iter()
                    .enumerate()
                    .filter(|(file, _)| *file != skipped)
                    .flat_map(|(_, (grp, values))| values.iter().map(move |value| (*grp, *value)))
                    .filter(|(grp, value)| variant.admits(*grp, *value))
                    .map(|(_, value)| value.map(|text| serde_json::from_str(text).unwrap()))
                    .collect::<Vec<_>>();
                assert_ne!(
                    expected_rows(&partial, false),
                    complete,
                    "{variant:?} without file {skipped}"
                );
            }
        }
    }

    #[test]
    fn probe_values_cover_every_shape_and_sql_null() {
        assert_eq!(probe_value(97), None);
        assert_eq!(probe_value(3), Some(json!({"k": 3, "s": "v"})));
        assert_eq!(probe_value(1), Some(json!({"s": "v", "k": 1})));
        assert_eq!(probe_value(2), Some(json!([2, "v"])));
        assert_eq!(PROBE_FILES.last().map(|(_, last)| *last), Some(PROBE_ROWS));
        assert!(
            PROBE_FILES
                .windows(2)
                .all(|pair| pair[0].1 + 1 == pair[1].0)
        );
    }

    #[test]
    fn capacity_inputs_are_frozen_beyond_the_limit() {
        assert_eq!(CAPACITY_QUERY_MEM_LIMIT, 33_554_432);
        assert_eq!(RHS_BIG_ROWS * RHS_BIG_PAD, 60_000_000);
        let refused = group_query("p", "r", false, Some(CAPACITY_QUERY_MEM_LIMIT));
        assert!(refused.starts_with("SELECT /*+ SET_VAR(query_mem_limit=33554432) */ m,"));
        assert!(refused.contains("j IN (SELECT r FROM r)"));
        assert!(
            streaming_control_query("b")
                .starts_with("SELECT /*+ SET_VAR(query_mem_limit=33554432) */ count(*)")
        );
        assert!(!group_query("p", "r", true, None).contains("SET_VAR"));
        assert!(group_query("p", "r", true, None).contains("j NOT IN (SELECT r FROM r)"));
    }

    #[test]
    fn fixture_sql_states_the_oracle_inputs() {
        let insert = probe_insert("c.d.probe", 1, 20_000);
        assert!(insert.contains("WHEN i % 97 = 0 THEN NULL"));
        assert!(insert.contains("json_object('k', i % 13, 's', 'v')"));
        assert!(insert.contains("json_object('s', 'v', 'k', i % 13)"));
        assert!(insert.contains("generate_series(1, 20000)"));
        assert_eq!(
            rhs_insert("c.d.rhs", 2, &[Some(r#"[6,"v"]"#), None]),
            r#"INSERT INTO c.d.rhs VALUES (2, parse_json('[6,"v"]')), (2, NULL)"#
        );
        assert!(rhs_big_insert("c.d.big", 1, 50_000).contains("repeat('x', 150)"));
    }

    #[test]
    fn marker_fields_are_read_from_exact_words() {
        let line = "NOVAROCKS_TASK_CREATE_APPLIED execution_id=-42:7:1 stage=3 task=11 backend=abc";
        assert_eq!(marker_field(line, "execution_id"), Some("-42:7:1"));
        assert_eq!(marker_field(line, "stage"), Some("3"));
        assert_eq!(marker_field(line, "task"), Some("11"));
        assert_eq!(marker_field(line, "missing"), None);
        let placement = BTreeMap::from([
            ("1".to_owned(), BTreeSet::from([0, 1, 2])),
            ("2".to_owned(), BTreeSet::from([1])),
            ("3".to_owned(), BTreeSet::from([0, 1, 2])),
        ]);
        assert_eq!(full_width_stages(&placement), ["1", "3"]);
    }

    #[test]
    fn plans_are_classified_by_the_membership_build_input() {
        let broadcast = "MEMBERSHIP [JsonInListV1, negated=false]\n  BROADCAST EXCHANGE";
        let gathered = "MEMBERSHIP [JsonInListV1, negated=true]\n  GATHER";
        assert_eq!(membership_build_edge(broadcast, "q").unwrap(), "BROADCAST");
        assert_eq!(membership_build_edge(gathered, "q").unwrap(), "GATHER");
        assert!(membership_build_edge("HASH JOIN", "q").is_err());
        assert!(require_broadcast_build(gathered, "q").is_err());
        assert_eq!(decode_bool(Some(1)).unwrap(), Some(true));
        assert!(decode_bool(Some(2)).is_err());
    }

    #[test]
    fn registers_three_default_query_lifecycle_scenarios() {
        let registered = scenarios();
        assert_eq!(
            registered
                .iter()
                .map(|scenario| scenario.name())
                .collect::<Vec<_>>(),
            [
                "query-lifecycle/json-membership-full-eos",
                "query-lifecycle/json-membership-cancel",
                "query-lifecycle/json-membership-capacity",
            ]
        );
        assert!(
            registered
                .iter()
                .all(|scenario| !scenario.is_explicit_stage())
        );
    }
}
