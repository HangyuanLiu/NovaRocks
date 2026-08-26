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

//! Process-boundary backend self-registration acceptance.

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;

const REQUIRED_BACKENDS: usize = 3;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(BackendSelfRegistration)]
}

struct BackendSelfRegistration;

impl Scenario for BackendSelfRegistration {
    fn name(&self) -> &'static str {
        "membership/self-registration"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "membership acceptance requires exactly {REQUIRED_BACKENDS} BEs"
        );
        let initial_process_ids = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            initial_process_ids.iter().all(|id| !id.is_empty()),
            "every self-registered backend must expose a process identity"
        );
        context.action(format!(
            "observed {} eligible self-registered backend process identities",
            initial_process_ids.len()
        ));

        query_one(context, "before FE restart")?;
        let deadline = context.deadline();
        context
            .handle()
            .drain_be_until(REQUIRED_BACKENDS - 1, deadline)
            .context("gracefully drain one BE through SIGTERM")?;
        query_one(context, "while one BE is draining")?;
        context.action(
            "proved SIGTERM removes a BE from future eligibility while the remaining BEs serve queries",
        );
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(REQUIRED_BACKENDS - 1, deadline)
            .context("replace drained BE and wait for a new eligible process identity")?;

        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE and wait for BE renew announce plus exact heartbeat")?;
        let after_fe_restart = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            after_fe_restart == initial_process_ids,
            "FE restart must rebuild membership from the same live BE process identities"
        );
        context.action("proved FE restart rebuilds only from BE renew announce and heartbeat");

        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(0, deadline)
            .context("restart BE[0] and wait for replacement identity eligibility")?;
        let replacement_process_id = context.handle().backend_process_id(0)?;
        ensure!(
            replacement_process_id != initial_process_ids[0],
            "same endpoint replacement must receive a new BackendProcessId"
        );
        query_one(context, "after endpoint replacement")?;
        context.action(
            "proved endpoint replacement cannot inherit the prior process identity and remains queryable only after re-verification",
        );
        Ok(())
    }
}

fn query_one(context: &mut ScenarioContext, phase: &str) -> Result<()> {
    let mut connection = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining(&format!("connect {phase}"))?,
    )?;
    let rows: Vec<i64> = connection
        .query("SELECT 1")
        .with_context(|| format!("run distributed query {phase}"))?;
    ensure!(
        rows == vec![1],
        "distributed query {phase} returned {rows:?}"
    );
    Ok(())
}
