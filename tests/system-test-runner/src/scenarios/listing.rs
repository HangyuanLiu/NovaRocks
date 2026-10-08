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

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::CrossProcessConfigOverlay;
use novarocks_cluster_harness::listing_rest::{
    ListingMode, ListingRestFixture, MEMBERS, NAMESPACES,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

pub struct CatalogListing;

impl Scenario for CatalogListing {
    fn name(&self) -> &'static str {
        "catalog/mem-1-m07-sdk-listing"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config(&self, _root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let credential = |purpose: &str| {
            format!(
                "[[connector.credentials]]\npurpose = '{purpose}'\nname = 'cl-fixture'\ngeneration = 'v1'\nkind = 's3'\naccess_key_id = 'cl-fixture'\naccess_key_secret = 'cl-fixture-test-only'\n"
            )
        };
        Ok(ScenarioLaunchConfig {
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(credential("object-store-metadata")),
                be: Some(credential("object-store-data")),
                ..Default::default()
            },
            ..Default::default()
        })
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let fixture = ListingRestFixture::start()?;
        let timeout = context
            .remaining("listing SQL")?
            .min(Duration::from_secs(120));
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let mut control = mysql_actor::connect(&user, port, timeout)?;
        let catalog_sql = |name: &str| {
            format!(
                "CREATE EXTERNAL CATALOG {name} PROPERTIES (\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"warehouse\"=\"s3://cl-fixture/warehouse\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"cl-fixture\",\"credential.object-store-metadata.generation\"=\"v1\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"static\",\"credential.object-store-data.name\"=\"cl-fixture\",\"credential.object-store-data.generation\"=\"v1\")",
                fixture.endpoint(),
                fixture.endpoint()
            )
        };
        control.query_drop(catalog_sql("cl_normal"))?;
        await_fixture(&fixture, timeout, |audit| {
            audit.namespace_pages == 1 && audit.active_listing_requests == 0
        })?;
        let mut phases = Vec::new();
        fixture.set_mode(ListingMode::Normal)?;
        phases.push(measure(context, "lake-discovery", || {
            control.query_drop(catalog_sql("cl_discovery"))?;
            let audit = await_fixture(&fixture, timeout, |audit| {
                audit.table_loads == (NAMESPACES * MEMBERS) as u64
                    && audit.active_listing_requests == 0
            })?;
            Ok(json!({"provider":audit}))
        })?);
        for clients in [1, 8, 16] {
            fixture.set_mode(ListingMode::Normal)?;
            phases.push(measure(context, "information-schema", || {
                let barrier = Arc::new(Barrier::new(clients));
                std::thread::scope(|scope| -> Result<()> {
                    let handles = (0..clients).map(|_| {
                        let barrier = barrier.clone(); let user = &user;
                        scope.spawn(move || -> Result<()> {
                            barrier.wait();
                            let mut connection = mysql_actor::connect(user, port, timeout)?;
                            let count = connection.query_first::<u64, _>("SELECT COUNT(*) FROM cl_normal.information_schema.tables WHERE TABLE_CATALOG='cl_normal' AND TABLE_SCHEMA LIKE 'cl_ns_%'")?;
                            ensure!(count == Some((NAMESPACES * MEMBERS) as u64), "incomplete information_schema table list");
                            Ok(())
                        })
                    }).collect::<Vec<_>>();
                    for handle in handles { handle.join().map_err(|_| anyhow::anyhow!("listing client panicked"))??; }
                    Ok(())
                })?;
                let audit = fixture.snapshot()?;
                ensure!(audit.peak_listing_requests <= 8, "catalog generation exceeded eight active listing requests");
                ensure!(audit.table_pages == (clients * NAMESPACES * 2) as u64, "information_schema skipped or duplicated table pages");
                Ok(json!({"clients":clients,"provider":audit}))
            })?);
        }
        fixture.set_mode(ListingMode::Normal)?;
        control.query_drop("USE cl_normal.cl_ns_0000")?;
        phases.push(measure(context, "show-views", || {
            let views = control.query::<String, _>("SHOW VIEWS")?;
            ensure!(
                views.len() == MEMBERS
                    && views[0] == "cl_view_000000"
                    && views[MEMBERS - 1] == "cl_view_000511",
                "incomplete SHOW VIEWS output"
            );
            Ok(json!({"rows":views.len(),"provider":fixture.snapshot()?}))
        })?);
        for mode in [
            ListingMode::PagedOverflow,
            ListingMode::TerminalOverflow,
            ListingMode::NameOverflow,
            ListingMode::TokenOverflow,
            ListingMode::TokenCycle,
            ListingMode::PageOverflow,
        ] {
            fixture.set_mode(mode)?;
            phases.push(measure(context, "drop-refusal", || {
                let error = control
                    .query_drop("DROP DATABASE cl_normal.cl_ns_0000 FORCE")
                    .err()
                    .context("over-bound discovery must refuse the whole DROP")?;
                let diagnostic = error.to_string();
                let expected_kind = if mode == ListingMode::TokenCycle {
                    "CorruptData"
                } else {
                    "ResourceExhausted"
                };
                ensure!(
                    diagnostic.contains(expected_kind),
                    "DROP failed outside its listing boundary"
                );
                let audit = fixture.snapshot()?;
                ensure!(
                    audit.destructive_mutations == 0,
                    "DROP mutated provider state before complete discovery"
                );
                Ok(json!({"mode":format!("{mode:?}"),"provider":audit}))
            })?);
        }
        fixture.set_mode(ListingMode::ExactEntries)?;
        phases.push(measure(context, "exact-entries", || {
            let tables = control.query::<String, _>("SHOW TABLES")?;
            ensure!(
                tables.len() == 65536,
                "exact-bound table list was truncated or incomplete"
            );
            Ok(json!({"rows":tables.len(),"provider":fixture.snapshot()?}))
        })?);
        fixture.set_mode(ListingMode::Normal)?;
        phases.push(measure(context, "drop-success", || {
            control.query_drop("DROP DATABASE cl_normal.cl_ns_0000 FORCE")?;
            let audit = fixture.snapshot()?;
            ensure!(
                audit.destructive_mutations == (MEMBERS * 2 + 1) as u64,
                "DROP omitted table, view or namespace mutations"
            );
            Ok(json!({"provider":audit}))
        })?);
        await_resource_convergence(context, &baseline, "listing SQL")?;
        std::fs::write(
            context.scenario_root().join("listing-measurements.json"),
            serde_json::to_vec_pretty(&json!({
                "input_freeze":"docs/testing/mem-1-m07/inputs/cl-listing-freeze-v1.json",
                "fixture":"controlled standard REST protocol; no row data",
                "scope":"sampled FE allocator high-water; not a hard SDK byte bound or cross-provider CL completion",
                "phases":phases
            }))?,
        )?;
        Ok(())
    }
}

fn await_fixture(
    fixture: &ListingRestFixture,
    timeout: Duration,
    predicate: impl Fn(&novarocks_cluster_harness::listing_rest::ListingSnapshot) -> bool,
) -> Result<novarocks_cluster_harness::listing_rest::ListingSnapshot> {
    let deadline = std::time::Instant::now() + timeout;
    let mut previous_match = false;
    loop {
        let audit = fixture.snapshot()?;
        let matches = predicate(&audit);
        if matches && previous_match {
            return Ok(audit);
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "controlled listing phase did not converge: {audit:?}"
        );
        previous_match = matches;
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
struct AllocatorReading {
    allocated: u64,
    active: u64,
    resident: u64,
}

struct StopSampler<'a>(&'a AtomicBool);
impl Drop for StopSampler<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn allocator(client: &reqwest::blocking::Client, port: u16) -> Result<AllocatorReading> {
    let body = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()?
        .error_for_status()?
        .text()?;
    let required = |statistic: &str| -> Result<u64> {
        let prefix = format!(
            "novarocks_frontend_process_allocator_memory_bytes{{statistic=\"{statistic}\"}} "
        );
        let values = body
            .lines()
            .filter_map(|line| line.strip_prefix(&prefix))
            .collect::<Vec<_>>();
        ensure!(
            values.len() == 1,
            "missing or duplicate allocator statistic"
        );
        Ok(values[0].parse()?)
    };
    Ok(AllocatorReading {
        allocated: required("allocated")?,
        active: required("active")?,
        resident: required("resident")?,
    })
}

fn measure(
    context: &ScenarioContext,
    phase: &'static str,
    operation: impl FnOnce() -> Result<Value>,
) -> Result<Value> {
    let identities = context.recheck_live_process_launch_identities()?;
    let port = context.fe_http_port();
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(1))
        .build()?;
    let before = allocator(&client, port)?;
    let stopped = AtomicBool::new(false);
    let (outcome, observed) = std::thread::scope(|scope| {
        let sampler = scope.spawn(|| -> Result<(AllocatorReading, u64)> {
            let mut peak = before;
            let mut samples = 0;
            while !stopped.load(Ordering::Acquire) {
                let reading = allocator(&client, port)?;
                peak.allocated = peak.allocated.max(reading.allocated);
                peak.active = peak.active.max(reading.active);
                peak.resident = peak.resident.max(reading.resident);
                samples += 1;
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok((peak, samples))
        });
        let stop = StopSampler(&stopped);
        let outcome = operation();
        drop(stop);
        let observed = sampler
            .join()
            .map_err(|_| anyhow::anyhow!("allocator sampler panicked"))?;
        Ok::<_, anyhow::Error>((outcome, observed))
    })?;
    let result = outcome.with_context(|| format!("listing phase {phase}"))?;
    let (peak, samples) = observed?;
    let after = allocator(&client, port)?;
    let after_identities = context.recheck_live_process_launch_identities()?;
    ensure!(
        serde_json::to_value(&identities)? == serde_json::to_value(&after_identities)?,
        "listing target process identity changed"
    );
    Ok(
        json!({"phase":phase,"before":before,"sampled_peak":peak,"after":after,"samples":samples,"process_launch_identities":identities,"result":result}),
    )
}
