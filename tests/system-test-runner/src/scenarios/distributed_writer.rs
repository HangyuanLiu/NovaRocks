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

//! Native acceptance for the distributed write data plane.
//!
//! These scenarios exist because the interesting claims are all about process
//! boundaries: writers running on several backends, one root aggregating them,
//! and a frontend that is the only thing allowed to commit. None of that is
//! observable in a single process, and an all-in-one run would prove only that
//! the code compiles into one binary.
//!
//! Assertions read published metrics rather than log strings. A backend's
//! tracing output goes to its own log files rather than the stdout the harness
//! captures, so grepping for a log line would assert on a formatting detail;
//! the counters are the contract.

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{CrossProcessChildEnvironment, ServerHandle};

use super::connector::{
    await_resource_convergence, connector_launch_config, connector_reader_environment,
    create_catalog, create_warehouse, mysql_endpoint, require_three_backends, resource_baseline,
};

const WRITER_OPENS: &str = "novarocks_backend_connector_write_writer_opens_total";
const WRITER_TOTALS: &str = "novarocks_backend_connector_write_writer_totals";
const ROOT_PEAK: &str = "novarocks_backend_connector_write_root_prepared_set_peak";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(DistributedWriterDataflow),
        Box::new(DistributedWriterFaults),
    ]
}

/// One backend-count-wide observation of the write data plane.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct WriteCounters {
    opens: f64,
    rows: f64,
    commit_fragments: f64,
    root_peak_entries: f64,
}

fn write_counters(context: &mut ScenarioContext, index: usize) -> Result<WriteCounters> {
    let handle = context.handle();
    Ok(WriteCounters {
        opens: handle.backend_connector_write_metric(index, WRITER_OPENS, "outcome", "opened")?,
        rows: handle.backend_connector_write_metric(index, WRITER_TOTALS, "unit", "rows")?,
        commit_fragments: handle.backend_connector_write_metric(
            index,
            WRITER_TOTALS,
            "unit",
            "commit_fragments",
        )?,
        root_peak_entries: handle.backend_connector_write_metric(
            index,
            ROOT_PEAK,
            "dimension",
            "entries",
        )?,
    })
}

fn all_write_counters(context: &mut ScenarioContext) -> Result<Vec<WriteCounters>> {
    let count = context.handle().be_count();
    (0..count)
        .map(|index| write_counters(context, index))
        .collect()
}

/// Proves the shape the design is actually about: writers on more than one
/// backend, exactly one root aggregating them, and a single snapshot from a
/// frontend that is the only thing that can commit.
struct DistributedWriterDataflow;

impl Scenario for DistributedWriterDataflow {
    fn name(&self) -> &'static str {
        "connector/distributed-writer-dataflow"
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
            context.remaining("connect distributed writer control session")?,
        )?;

        const CATALOG: &str = "distributed_writer";
        const DATABASE: &str = "distributed_writer_db";
        const TABLE: &str = "distributed_writer_data";
        let warehouse = create_warehouse(context, "distributed-writer")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        control
            .query_drop(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))
            .context("create distributed writer database")?;
        control
            .query_drop(format!(
                "CREATE TABLE {CATALOG}.{DATABASE}.{TABLE} (v BIGINT)"
            ))
            .context("create distributed writer table")?;

        // This cluster is this scenario's own, and nothing has written to it
        // yet. The check below relies on that: the root's prepared-set gauge is
        // a per-attempt peak, so comparing it across two writes in one process
        // would compare unrelated attempts. Assert the precondition rather than
        // let a later second write silently turn the root check into a no-op.
        let before = all_write_counters(context)?;
        if before
            .iter()
            .any(|counters| *counters != WriteCounters::default())
        {
            bail!("distributed writer scenario expected a cluster with no prior write: {before:?}");
        }

        context.action("append through the distributed write dataflow");
        control
            .query_drop(format!(
                "INSERT INTO {CATALOG}.{DATABASE}.{TABLE} \
                 SELECT generate_series FROM TABLE(generate_series(1, 3000))"
            ))
            .context("distributed append through the write dataflow")?;

        let after = all_write_counters(context)?;

        // Writers ran on more than one backend. A single-backend write would
        // still produce correct data, so row counts alone cannot prove the
        // plan actually distributed.
        let writing_backends = before
            .iter()
            .zip(&after)
            .filter(|(before, after)| after.opens > before.opens)
            .count();
        if writing_backends < 2 {
            bail!(
                "distributed append opened writers on {writing_backends} backend(s); \
                 before={before:?} after={after:?}"
            );
        }

        // Exactly one backend aggregated. Two roots would each hold a partial
        // set and each believe it complete.
        let roots = after
            .iter()
            .filter(|counters| counters.root_peak_entries > 0.0)
            .count();
        if roots != 1 {
            bail!("distributed append aggregated on {roots} root backends; expected exactly one");
        }

        // Every row a writer accepted is accounted for across the cluster.
        let accepted: f64 = before
            .iter()
            .zip(&after)
            .map(|(before, after)| after.rows - before.rows)
            .sum();
        if (accepted - 3000.0).abs() > f64::EPSILON {
            bail!("writers accepted {accepted} rows; expected 3000");
        }

        // Every writer that finished produced at least one artifact, and the
        // frontend committed them as one snapshot.
        let fragments: f64 = before
            .iter()
            .zip(&after)
            .map(|(before, after)| after.commit_fragments - before.commit_fragments)
            .sum();
        if fragments < writing_backends as f64 {
            bail!(
                "writers produced {fragments} commit fragments across {writing_backends} \
                 writing backends; expected at least one each"
            );
        }

        let total: Option<i64> = control
            .query_first(format!("SELECT SUM(v) FROM {CATALOG}.{DATABASE}.{TABLE}"))
            .context("read back the distributed append")?;
        if total != Some(4_501_500) {
            bail!("distributed append read back {total:?}; expected Some(4501500)");
        }

        await_resource_convergence(context, &baseline, "distributed writer dataflow")?;
        Ok(())
    }
}

/// A failed write must leave nothing behind.
///
/// This is the claim the whole dual barrier exists to make: whatever goes wrong
/// in the data plane, the connector is never asked to commit, so no snapshot
/// appears. Asserting it needs a real cluster because the failure has to happen
/// on a backend while the frontend is deciding.
struct DistributedWriterFaults;

impl Scenario for DistributedWriterFaults {
    fn name(&self) -> &'static str {
        "connector/distributed-writer-faults"
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
            context.remaining("connect distributed writer fault control session")?,
        )?;

        const CATALOG: &str = "distributed_writer_faults";
        const DATABASE: &str = "distributed_writer_fault_db";
        const TABLE: &str = "distributed_writer_fault_data";
        let warehouse = create_warehouse(context, "distributed-writer-faults")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        control
            .query_drop(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))
            .context("create distributed writer fault database")?;
        control
            .query_drop(format!(
                "CREATE TABLE {CATALOG}.{DATABASE}.{TABLE} (v BIGINT)"
            ))
            .context("create distributed writer fault table")?;

        // A committed row would prove the frontend committed despite a failed
        // writer, so seed nothing and require the table to stay empty.
        for (case, kind) in [
            ("a failing writer", "connector-write-writer-failure"),
            ("a failing root aggregation", "connector-write-root-failure"),
        ] {
            context.action(&format!("inject {case} and require no snapshot"));
            for index in 0..context.handle().be_count() {
                context
                    .handle()
                    .arm_query_lifecycle_fault(index, kind)
                    .with_context(|| format!("arm {kind} on BE[{index}]"))?;
            }

            let outcome = control.query_drop(format!(
                "INSERT INTO {CATALOG}.{DATABASE}.{TABLE} \
                 SELECT generate_series FROM TABLE(generate_series(1, 100))"
            ));
            if outcome.is_ok() {
                bail!("{case} did not fail the write");
            }

            let rows: Option<i64> = control
                .query_first(format!("SELECT COUNT(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
                .with_context(|| format!("read back the table after {case}"))?;
            if rows != Some(0) {
                bail!("{case} left {rows:?} committed rows; expected none");
            }
        }

        await_resource_convergence(context, &baseline, "distributed writer faults")?;
        Ok(())
    }
}
