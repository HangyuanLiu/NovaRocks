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

//! MySQL logical mutation counts, selected-action multiplicity and atomicity.
//!
//! These checks extend the original row-level scenario, using its isolated
//! warehouse, connection, process topology and resource/exit owners. SQL
//! golden output does not expose a DML OK packet's affected rows. Expected
//! rows below are calculated from the stated I/U/D inputs, never from writer
//! artifacts, a profile or a second implementation of event expansion.
use super::*;
#[path = "distributed_writer_dop_observation.rs"]
mod dop_observation;
#[path = "distributed_writer_same_file.rs"]
mod same_file;

pub(super) const CATALOG: &str = "row_level_writer";
pub(super) const DATABASE: &str = "row_level_writer_db";

fn unmatched_merge_sql(catalog: &str, database: &str, dop: i32) -> String {
    let table = format!("{catalog}.{database}.unmatched_target_{dop}");
    let source = format!("{catalog}.{database}.unmatched_source_{dop}");
    format!(
        "MERGE INTO {table} t USING {source} s ON t.id=s.id \
         WHEN MATCHED AND s.selected THEN UPDATE SET v=s.v \
         WHEN NOT MATCHED AND s.selected THEN INSERT (id,v) VALUES(s.id,s.v)"
    )
}

pub(super) fn observation_specs(cluster_size: usize) -> Vec<StatementSpec> {
    [1, 4]
        .into_iter()
        .map(|dop| StatementSpec {
            sql: unmatched_merge_sql(CATALOG, DATABASE, dop),
            faults: vec![],
            observations: (0..cluster_size)
                .flat_map(|index| {
                    [
                        (index, "writer-marker", None),
                        (index, "grpc-fragment", None),
                    ]
                })
                .collect(),
        })
        .chain(same_file::observation_specs(cluster_size))
        .collect()
}

pub(super) fn assert_affected(control: &mysql::Conn, expected: u64, subject: &str) -> Result<()> {
    let actual = control.affected_rows();
    anyhow::ensure!(
        actual == expected,
        "{subject}: MySQL affected_rows={actual}, expected logical I+U+D={expected}"
    );
    Ok(())
}

fn mutate(
    control: &mut mysql::Conn,
    table: &str,
    sql: &str,
    affected: u64,
    snapshots: i64,
    subject: &str,
) -> Result<()> {
    let before = snapshot_count(control, table)?;
    control
        .query_drop(sql)
        .with_context(|| subject.to_owned())?;
    // Read the OK packet before any metadata/read statement changes it.
    assert_affected(control, affected, subject)?;
    let after = snapshot_count(control, table)?;
    anyhow::ensure!(
        after - before == snapshots,
        "{subject}: snapshot delta={}, expected {snapshots}",
        after - before
    );
    Ok(())
}

fn assert_rows(
    control: &mut mysql::Conn,
    table: &str,
    expected: &[(i64, i64)],
    subject: &str,
) -> Result<()> {
    let actual: Vec<(i64, i64)> = control
        .query(format!("SELECT id, v FROM {table} ORDER BY id, v"))
        .with_context(|| format!("{subject}: read complete target row bag"))?;
    anyhow::ensure!(
        actual == expected,
        "{subject}: target rows={actual:?}, expected {expected:?}"
    );
    Ok(())
}

pub(super) fn run(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    observations: &SqlIntentOwner,
) -> Result<()> {
    for (index, dop) in [1, 4].into_iter().enumerate() {
        context.action(format!(
            "check logical row-level write contracts with requested DOP {dop}"
        ));
        control.query_drop(format!("SET pipeline_dop = {dop}"))?;
        mixed_actions(control, catalog, database, dop)?;
        partition_movement(control, catalog, database, dop)?;
        for action in [MatchedAction::Update, MatchedAction::Delete] {
            selected_actions(context, control, catalog, database, dop, action)?;
        }
        unmatched_inserts(
            context,
            control,
            catalog,
            database,
            dop,
            observations.take(index)?,
        )?;
        same_file::run(
            context,
            control,
            catalog,
            database,
            dop,
            observations.take(index + 2)?,
        )?;
    }
    Ok(())
}

fn mixed_actions(control: &mut mysql::Conn, catalog: &str, database: &str, dop: i32) -> Result<()> {
    let table = format!("{catalog}.{database}.logical_mix_{dop}");
    let source = format!("{catalog}.{database}.logical_mix_source_{dop}");
    control.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT, v BIGINT) TBLPROPERTIES \
         (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\", \
         \"novarocks.update.mode\"=\"merge-on-read\")"
    ))?;
    control.query_drop(format!("CREATE TABLE {source} (id BIGINT, v BIGINT)"))?;
    control.query_drop(format!(
        "INSERT INTO {table} VALUES (1, 10), (2, 20), (3, 30), (4, 40)"
    ))?;
    // Each INSERT seals an independent nonempty source file. The result is
    // U=2/I=3, even though each Replace travels on two physical routes.
    for rows in ["(1, 101), (10, 1000)", "(2, 202), (11, 1100)", "(12, 1200)"] {
        control.query_drop(format!("INSERT INTO {source} VALUES {rows}"))?;
    }
    mutate(
        control,
        &table,
        &format!(
            "MERGE INTO {table} t USING {source} s ON t.id=s.id \
         WHEN MATCHED THEN UPDATE SET v=s.v \
         WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)"
        ),
        5,
        1,
        "mixed MERGE U=2/I=3",
    )?;
    assert_rows(
        control,
        &table,
        &[
            (1, 101),
            (2, 202),
            (3, 30),
            (4, 40),
            (10, 1000),
            (11, 1100),
            (12, 1200),
        ],
        "mixed update/insert",
    )?;
    mutate(
        control,
        &table,
        &format!(
            "MERGE INTO {table} t USING {source} s ON t.id=s.id \
         WHEN MATCHED AND s.id < 3 THEN DELETE"
        ),
        2,
        1,
        "matched MERGE D=2",
    )?;
    let remaining = [(3, 30), (4, 40), (10, 1000), (11, 1100), (12, 1200)];
    assert_rows(control, &table, &remaining, "matched delete")?;
    for (subject, sql) in [
        (
            "no-match UPDATE",
            format!("UPDATE {table} SET v=0 WHERE id=-1"),
        ),
        (
            "no-match DELETE",
            format!("DELETE FROM {table} WHERE id=-1"),
        ),
        (
            "no-match MERGE",
            format!(
                "MERGE INTO {table} t USING {source} s ON t.id=s.id WHEN MATCHED AND s.id < 0 THEN DELETE"
            ),
        ),
    ] {
        mutate(control, &table, &sql, 0, 0, subject)?;
        assert_rows(control, &table, &remaining, subject)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum MatchedAction {
    Update,
    Delete,
}
impl MatchedAction {
    fn label(self) -> &'static str {
        match self {
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
    fn clause(self) -> &'static str {
        match self {
            Self::Update => "UPDATE SET v=s.new_v",
            Self::Delete => "DELETE",
        }
    }
}

fn selected_actions(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    dop: i32,
    action: MatchedAction,
) -> Result<()> {
    let label = action.label();
    let table = format!("{catalog}.{database}.selected_{label}_{dop}");
    let source = format!("{catalog}.{database}.selected_source_{label}_{dop}");
    control.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT, v BIGINT) TBLPROPERTIES \
         (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\", \
         \"novarocks.update.mode\"=\"merge-on-read\")"
    ))?;
    control.query_drop(format!(
        "INSERT INTO {table} VALUES (1,10),(2,20),(3,30),(4,40)"
    ))?;
    control.query_drop(format!(
        "CREATE TABLE {source} (id BIGINT, new_v BIGINT, selected BOOLEAN, case_id BIGINT)"
    ))?;
    // Candidate pairs are split across independent files, so an inactive row
    // cannot disappear merely because its duplicate was in the same batch.
    for rows in [
        "(1,101,TRUE,1),(2,202,TRUE,2),(4,404,TRUE,4),(3,303,TRUE,4)",
        "(1,999,FALSE,1),(2,999,NULL,2),(3,999,NULL,3),(4,444,TRUE,4)",
        "(3,999,FALSE,3)",
    ] {
        control.query_drop(format!("INSERT INTO {source} VALUES {rows}"))?;
    }
    let sql = |case_id| {
        format!(
            "MERGE INTO {table} t USING {source} s ON t.id=s.id AND s.case_id={case_id} \
         WHEN MATCHED AND s.selected THEN {}",
            action.clause()
        )
    };
    let mut expected = vec![(1, 10), (2, 20), (3, 30), (4, 40)];
    for (case_id, subject) in [(1, "TRUE+FALSE"), (2, "TRUE+NULL")] {
        mutate(
            control,
            &table,
            &sql(case_id),
            1,
            1,
            &format!("{label} {subject}"),
        )?;
        match action {
            MatchedAction::Update => expected[case_id as usize - 1].1 = case_id * 101,
            MatchedAction::Delete => expected.retain(|(id, _)| *id != case_id),
        }
        assert_rows(control, &table, &expected, subject)?;
    }
    mutate(
        control,
        &table,
        &sql(3),
        0,
        0,
        &format!("{label} FALSE+NULL"),
    )?;
    assert_rows(control, &table, &expected, "inactive duplicate pair")?;

    // A valid id=3 action accompanies the two active id=4 matches. Even if
    // another route stages its output first, the statement must publish none.
    let before = snapshot_count(control, &table)?;
    let before_terminals = publication_terminals(context, MUTATION_FAMILY)?;
    let result = control.query_drop(sql(4));
    match result {
        Err(mysql::Error::MySqlError(error)) => anyhow::ensure!(
            error
                .message
                .starts_with("[query.execution.invalid_data] Data failure (reason 7:317)"),
            "{label} TRUE+TRUE returned the wrong public failure: {error:?}"
        ),
        other => {
            bail!("{label} TRUE+TRUE must fail with the public duplicate-key error, got {other:?}")
        }
    }
    let after_terminals = publication_terminals(context, MUTATION_FAMILY)?;
    assert_commit_never_invoked(
        before_terminals,
        after_terminals,
        &format!("{label} TRUE+TRUE"),
    )?;
    super::writer_statement_actor::require_public_error_reentry(control)?;
    anyhow::ensure!(
        snapshot_count(control, &table)? == before,
        "{label} TRUE+TRUE partially committed"
    );
    assert_rows(
        control,
        &table,
        &expected,
        "duplicate active matches leave every original row",
    )?;
    Ok(())
}

fn unmatched_inserts(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    dop: i32,
    selected: SelectedStatement,
) -> Result<()> {
    let table = format!("{catalog}.{database}.unmatched_target_{dop}");
    let source = format!("{catalog}.{database}.unmatched_source_{dop}");
    control.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT, v BIGINT) TBLPROPERTIES \
         (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\", \
         \"novarocks.update.mode\"=\"merge-on-read\")"
    ))?;
    control.query_drop(format!(
        "CREATE TABLE {source} (id BIGINT, v BIGINT, selected BOOLEAN)"
    ))?;
    // A nonempty scan on six independently sealed files feeds the unkeyed
    // data route. Target identity is absent for every row, including the two
    // equal SQL keys: neither may enter matched-key duplicate detection.
    let mut expected = Vec::new();
    for file in 0..6_i64 {
        let base = file * 64;
        control.query_drop(format!(
            "INSERT INTO {source} SELECT generate_series, generate_series*10, TRUE \
             FROM TABLE(generate_series({},{}))",
            base + 1,
            base + 64
        ))?;
        expected.extend((base + 1..=base + 64).map(|id| (id, id * 10)));
    }
    control.query_drop(format!("INSERT INTO {source} VALUES (500,5000,TRUE),(500,5001,TRUE),(600,6000,FALSE),(601,6010,NULL)"))?;
    expected.extend([(500, 5000), (500, 5001)]);
    let before = all_write_counters(context)?;
    dop_observation::mutate_observed(
        context,
        control,
        &selected,
        &table,
        &unmatched_merge_sql(catalog, database, dop),
        &before,
        dop,
        386,
        None,
        "unmatched inserts retain duplicate SQL keys and omit FALSE/NULL actions",
    )?;
    let after = all_write_counters(context)?;
    // No target row matched. Nonzero finished-writer rows therefore identify
    // the data route on that exact backend; empty delete writers do not count.
    let data_backends = before
        .iter()
        .zip(&after)
        .enumerate()
        .filter(|(_, (before, after))| after.rows > before.rows)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        data_backends.len() >= 2,
        "unmatched data route used only {data_backends:?}; before={before:?} after={after:?}"
    );
    println!(
        "unmatched MERGE requested_dop={dop} actual_nonempty_data_backends={data_backends:?} writer_opens={:?}",
        write_delta(&before, &after).opens
    );
    assert_rows(
        control,
        &table,
        &expected,
        "unmatched distributed insert row bag",
    )?;
    // All TRUE inputs now match, while FALSE/NULL inputs still do not. With
    // no matched action, the same relation has no selected event at all.
    mutate(
        control,
        &table,
        &format!(
            "MERGE INTO {table} t USING {source} s ON t.id=s.id \
         WHEN NOT MATCHED AND s.selected THEN INSERT (id,v) VALUES(s.id,s.v)"
        ),
        0,
        0,
        "inactive unmatched FALSE/NULL inputs",
    )?;
    assert_rows(
        control,
        &table,
        &expected,
        "inactive unmatched inputs preserve the target",
    )?;

    Ok(())
}

fn partition_movement(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    dop: i32,
) -> Result<()> {
    let table = format!("{catalog}.{database}.partition_move_{dop}");
    control.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT, v BIGINT, p BIGINT) PARTITION BY(p) TBLPROPERTIES \
         (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\", \
         \"novarocks.update.mode\"=\"merge-on-read\")"
    ))?;
    control.query_drop(format!("INSERT INTO {table} VALUES(1,10,0),(2,20,0)"))?;
    let original: Vec<(i64, String, i64)> =
        control.query(format!("SELECT id,_file,_row_id FROM {table} ORDER BY id"))?;
    anyhow::ensure!(
        original.len() == 2,
        "cross-partition fixture lost an original row: {original:?}"
    );
    mutate(
        control,
        &table,
        &format!("UPDATE {table} SET p=1,v=11 WHERE id=1"),
        1,
        1,
        "Replace moves one row across table partitions",
    )?;
    let rows: Vec<(i64, i64, i64)> =
        control.query(format!("SELECT id,v,p FROM {table} ORDER BY id"))?;
    anyhow::ensure!(
        rows == [(1, 11, 1), (2, 20, 0)],
        "cross-partition Replace returned {rows:?}"
    );
    let rewritten: Vec<(i64, String, i64)> =
        control.query(format!("SELECT id,_file,_row_id FROM {table} ORDER BY id"))?;
    anyhow::ensure!(
        rewritten.len() == 2,
        "cross-partition Replace lost a physical row: {rewritten:?}"
    );
    anyhow::ensure!(
        rewritten[0].1 != original[0].1,
        "cross-partition Replace kept its old data location"
    );
    anyhow::ensure!(
        rewritten[0].2 == original[0].2 && rewritten[1] == original[1],
        "cross-partition Replace changed logical lineage or its untouched peer: before={original:?} after={rewritten:?}"
    );
    let new_partition: Vec<i64> =
        control.query(format!("SELECT id FROM {table} WHERE p=1 ORDER BY id"))?;
    let old_partition: Vec<i64> =
        control.query(format!("SELECT id FROM {table} WHERE p=0 ORDER BY id"))?;
    anyhow::ensure!(
        new_partition == [1] && old_partition == [2],
        "partition-qualified reads returned new={new_partition:?}, old={old_partition:?}"
    );
    Ok(())
}
