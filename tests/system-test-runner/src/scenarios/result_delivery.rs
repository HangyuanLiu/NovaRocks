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

use super::result_delivery_baseline::await_idle;
use crate::actors::mysql_stream::AsyncMysqlStream;
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Result, ensure};
use novarocks_cluster_harness::ServerHandle;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    topology: String,
    segment_bytes: u64,
    mysql_u24_payload_bytes: u64,
    scope: String,
    cases: Vec<WireCase>,
}

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct WireCase {
    name: String,
    sql: String,
    expected_rows: u64,
    expected_row_payload_bytes: u64,
    expected_packets: u64,
    expected_row_sha256: String,
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(WireBoundary(
            "result-delivery/many-small-rows-cross-segment",
        )),
        Box::new(WireBoundary("result-delivery/large-row-cross-u24")),
    ]
}

struct WireBoundary(&'static str);

impl Scenario for WireBoundary {
    fn name(&self) -> &'static str {
        self.0
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "wire boundary requires native 1FE+3BE"
        );
        let manifest: Manifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/result-delivery-wire-boundary-v1.json"
        ))?;
        ensure!(
            manifest.schema_version == 1
                && manifest.topology == "1FE+3BE"
                && manifest.segment_bytes == 1_048_576
                && manifest.mysql_u24_payload_bytes == 0x00ff_ffff
                && !manifest.scope.is_empty()
                && manifest.cases.len() == 2,
            "unsupported frozen wire boundary manifest"
        );
        let case = manifest
            .cases
            .iter()
            .find(|case| case.name == self.name())
            .ok_or_else(|| anyhow::anyhow!("wire boundary case missing from frozen manifest"))?;
        ensure!(
            case.expected_row_payload_bytes > manifest.segment_bytes,
            "wire boundary must cross a native segment"
        );
        let epoch = Instant::now();
        await_idle(context, "wire-boundary", "before", epoch)?;
        let before: Vec<_> = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<_>>()?;
        let timeout = context
            .remaining("wire boundary query")?
            .min(Duration::from_secs(30));
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let observation = runtime.block_on(async {
            let mut stream = AsyncMysqlStream::connect(&user, port, timeout).await?;
            Ok::<_, anyhow::Error>(stream.observe_text_query(&case.sql, Duration::ZERO).await)
        })?;
        std::fs::write(
            context
                .scenario_root()
                .join("wire-boundary-observation.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version":1,"expected":case,"observation":observation
            }))?,
        )?;
        ensure!(
            observation.error.is_none(),
            "wire query failed: {:?}",
            observation.error
        );
        ensure!(
            observation.columns == 1 && observation.rows == case.expected_rows,
            "wire result schema or row count disagrees with independent oracle"
        );
        ensure!(
            observation.row_payload_bytes == case.expected_row_payload_bytes
                && observation.packets == case.expected_packets,
            "wire payload length or U24 packet count disagrees with independent oracle"
        );
        ensure!(
            observation.row_sha256 == case.expected_row_sha256,
            "wire row bytes or row order disagree with independent oracle"
        );
        let after: Vec<_> = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<_>>()?;
        let mut created = 0_u64;
        for (before, after) in before.iter().zip(&after) {
            ensure!(
                before.is_finite() && after.is_finite() && after >= before,
                "invalid native task creation counter"
            );
            created += (after - before) as u64;
        }
        ensure!(
            created > 0,
            "wire query never executed a native backend task"
        );
        context.record_phase_observation(
            "wire-correctness",
            1,
            1,
            1,
            "public-mysql-native-root",
            1,
            "passed",
            BTreeMap::from([
                ("rows", observation.rows),
                ("row_payload_bytes", observation.row_payload_bytes),
                ("packets", observation.packets),
                ("native_tasks_created", created),
            ]),
        )?;
        context.action("verified native row bytes, order, packet sequence, one schema and terminal success against an independent frozen oracle");
        await_idle(context, "wire-boundary", "after", epoch)?;
        context.action("observed two consecutive idle FE governance/window and BE reservation/ingress snapshots after writer exit");
        Ok(())
    }
}
