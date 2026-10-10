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

//! Real stock REST metadata preparation is separate from FE allocator samples.
use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use super::listing::measure;
use super::mv_uea7::ManagedMvRestFixture;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, sync_channel};
use std::sync::{Arc, Barrier, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

fn repo() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .context("resolve real REST CL repository")
}

fn hash(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(std::fs::read(path)?)))
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

struct ChildOwner(Option<Child>);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A helper process has no service lifecycle authority. The existing private
/// fixture remains the only owner of its Docker project and data locations.
fn producer(root: &Path, freeze: &Path, phase: &str, previous: Option<&Path>) -> Result<()> {
    let repository = repo()?;
    let output = root.join(phase);
    let log = File::create(root.join(format!("producer-{phase}.log")))?;
    let mut command = Command::new("python3");
    command
        .current_dir(&repository)
        .arg(repository.join("docs/testing/mem-1-m07/scripts/prepare_real_rest_cl.py"))
        .args(["--freeze"])
        .arg(freeze)
        .args(["--phase", phase, "--output"])
        .arg(&output)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    if let Some(previous) = previous {
        command
            .arg(if phase == "bulk" {
                "--preflight-receipt"
            } else {
                "--ready-receipt"
            })
            .arg(previous);
    }
    let mut owner = ChildOwner(Some(
        command
            .spawn()
            .context("start real REST metadata producer")?,
    ));
    let child = owner
        .0
        .as_mut()
        .context("metadata producer owner missing")?;
    // The helper enforces 300s/7200s absolute operation deadlines. This
    // independent exit watchdog permits only five seconds for process exit.
    let deadline =
        Instant::now() + Duration::from_secs(if phase == "preflight" { 305 } else { 7205 });
    loop {
        if let Some(status) = child.try_wait()? {
            ensure!(
                status.success(),
                "real REST external {phase} failed; inspect its safe producer audit"
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("real REST external {phase} process exceeded its exit deadline");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

struct Observer {
    child: Child,
    input: ChildStdin,
    replies: Option<Receiver<Result<Value>>>,
    reader: Option<JoinHandle<()>>,
    uri: String,
    stopped_receipt: PathBuf,
}

impl Observer {
    fn start(root: &Path, producer_freeze: &Path) -> Result<Self> {
        let repository = repo()?;
        let script = repository.join("docs/testing/mem-1-m07/scripts/observe_real_rest_cl.py");
        let producer_script =
            repository.join("docs/testing/mem-1-m07/scripts/prepare_real_rest_cl.py");
        let freeze = root.join("observer-freeze-bound.json");
        write_json(
            &freeze,
            &json!({
                "schema_version":1,"frozen_before_execution":true,"observer_sha256":hash(&script)?,
                "producer_path":producer_script,"producer_sha256":hash(&producer_script)?,
                "producer_freeze_path":producer_freeze,"producer_freeze_sha256":hash(producer_freeze)?,
                "bounds":{"max_observers":256,"max_request_bytes":65536,"max_response_bytes":16777216,
                    "max_decoded_json_bytes":16777216,"max_header_bytes":65536,"max_header_fields":100,
                    "request_absolute_deadline_seconds":30}
            }),
        )?;
        let log = File::create(root.join("observer-process.log"))?;
        let child = Command::new("python3")
            .current_dir(repository)
            .arg(script)
            .arg("--freeze")
            .arg(&freeze)
            .arg("--output")
            .arg(root.join("observer"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(log)
            .spawn()?;
        let mut spawn_owner = ChildOwner(Some(child));
        let child = spawn_owner
            .0
            .as_mut()
            .context("observer process owner missing")?;
        let input = child.stdin.take().context("observer stdin missing")?;
        let output = child.stdout.take().context("observer stdout missing")?;
        let (sent, replies) = sync_channel(1);
        let reader = thread::spawn(move || {
            let mut output = BufReader::new(output);
            loop {
                let mut line = Vec::new();
                let result = (&mut output).take(65537).read_until(b'\n', &mut line);
                let parsed = match result {
                    Ok(0) => break,
                    Ok(_) if line.len() <= 65536 && line.last() == Some(&b'\n') => {
                        serde_json::from_slice(&line).context("invalid observer control JSON")
                    }
                    _ => Err(anyhow::anyhow!(
                        "observer control output exceeds the fixed bound"
                    )),
                };
                if sent.send(parsed).is_err() {
                    break;
                }
            }
        });
        let child = spawn_owner
            .0
            .take()
            .context("observer process owner missing")?;
        let mut owner = Self {
            child,
            input,
            replies: Some(replies),
            reader: Some(reader),
            uri: String::new(),
            stopped_receipt: root.join("observer-stopped.json"),
        };
        let bound = owner.receive(Duration::from_secs(5))?;
        ensure!(
            bound["state"] == "OBSERVER_BOUND",
            "actual REST observer did not bind"
        );
        owner.uri = bound["observer_uri"]
            .as_str()
            .context("observer endpoint missing")?
            .to_owned();
        Ok(owner)
    }

    fn receive(&mut self, timeout: Duration) -> Result<Value> {
        self.replies
            .as_ref()
            .context("observer control was closed")?
            .recv_timeout(timeout)
            .context("observer control response deadline")?
    }

    fn command(&mut self, value: &Value) -> Result<Value> {
        self.input.write_all(&serde_json::to_vec(value)?)?;
        self.input.write_all(b"\n")?;
        self.input.flush()?;
        self.receive(Duration::from_secs(1))
    }

    fn snapshot(&mut self) -> Result<Value> {
        let value = self.command(&json!({"command":"snapshot"}))?;
        ensure!(
            value["command"] == "snapshot" && value["audit"].is_object(),
            "observer snapshot refused"
        );
        Ok(value["audit"].clone())
    }

    fn reset(&mut self, phase: &str) -> Result<()> {
        let value = self.command(&json!({"command":"reset","phase":phase}))?;
        ensure!(
            value["command"] == "reset" && value["current"]["phase"] == phase,
            "observer phase reset refused while requests or connections are live"
        );
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        self.input.write_all(b"{\"command\":\"stop\"}\n")?;
        self.input.flush()?;
        let value = self.receive(Duration::from_secs(35))?;
        write_json(&self.stopped_receipt, &value)?;
        ensure!(
            value["state"] == "OBSERVER_STOPPED" && value["audit"]["valid_observation"] == true,
            "observer shutdown did not retain a valid actual traffic audit"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait()? {
                ensure!(status.success(), "observer process exited with a failure");
                drop(self.replies.take());
                if let Some(reader) = self.reader.take() {
                    reader
                        .join()
                        .map_err(|_| anyhow::anyhow!("observer output owner panicked"))?;
                }
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "observer process did not exit after its final receipt"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        drop(self.replies.take());
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

struct PreparedListing {
    fixture: ManagedMvRestFixture,
    observer: Observer,
    producer_freeze: PathBuf,
    preparation_root: PathBuf,
    catalog_sql: String,
    native_deadline: Option<Instant>,
}

impl PreparedListing {
    fn start(root: &Path) -> Result<(Self, ScenarioLaunchConfig)> {
        let (mut fixture, launch) = ManagedMvRestFixture::start(root, "cl_normal")?;
        let prepared = (|| -> Result<_> {
            let preparation_root = root.join("real-rest-preparation");
            std::fs::create_dir(&preparation_root)?;
            let manifest_path = fixture
                .rest_fixture()
                .runtime_env_file()?
                .parent()
                .context("private fixture publication parent missing")?
                .join("manifest.json");
            let manifest: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
            let mut freeze: Value = serde_json::from_str(include_str!(
                "../../../../docs/testing/mem-1-m07/inputs/real-rest-cl-producer-template-v1.json"
            ))?;
            freeze["frozen_before_execution"] = json!(true);
            freeze["binding"] = json!({
                "owner_mode":"task-private","owner_manifest_path":manifest_path,"owner_manifest_sha256":hash(&manifest_path)?,
                "fixture_id":manifest["env_id"],"compose_project":manifest["compose_project"],
                "rest_uri":manifest["iceberg_rest"]["uri"],"warehouse":manifest["iceberg_rest"]["warehouse"],
                "server_default_warehouse":manifest["iceberg_rest"]["server_default_warehouse"],
                "rest_image_id":manifest["runtime"]["catalog"]["images"]["rest"]["image_id"]
            });
            let producer_freeze = preparation_root.join("producer-freeze-bound.json");
            write_json(&producer_freeze, &freeze)?;
            producer(&preparation_root, &producer_freeze, "preflight", None)?;
            let preflight = preparation_root.join("preflight/PREFLIGHT_PASS.json");
            producer(
                &preparation_root,
                &producer_freeze,
                "bulk",
                Some(&preflight),
            )?;
            ensure!(
                preparation_root.join("bulk/READY.json").is_file(),
                "actual REST metadata did not publish READY"
            );
            let observer = Observer::start(&preparation_root, &producer_freeze)?;
            ensure!(
                fixture
                    .create_catalog_sql()
                    .matches(fixture.rest_uri())
                    .count()
                    == 1,
                "catalog SQL must contain its private REST URI exactly once"
            );
            let catalog_sql = fixture
                .create_catalog_sql()
                .replace(fixture.rest_uri(), &observer.uri);
            std::fs::write(
                preparation_root.join("actual-native-catalog.sql"),
                &catalog_sql,
            )?;
            Ok((observer, producer_freeze, preparation_root, catalog_sql))
        })();
        match prepared {
            Ok((observer, producer_freeze, preparation_root, catalog_sql)) => Ok((
                Self {
                    fixture,
                    observer,
                    producer_freeze,
                    preparation_root,
                    catalog_sql,
                    native_deadline: None,
                },
                launch,
            )),
            Err(error) => {
                // Surface cleanup failures even before the runner owns this
                // fixture. Drop remains the final fallback for exact owners.
                if let Err(cleanup) = fixture.shutdown() {
                    return Err(anyhow::anyhow!(
                        "real REST preparation failed: {error:#}; private fixture cleanup failed: {cleanup:#}"
                    ));
                }
                Err(error)
            }
        }
    }
}

fn idle(audit: &Value) -> bool {
    [
        "active_connections",
        "active_requests",
        "active_upstream_requests",
        "active_upstream_listings",
    ]
    .iter()
    .all(|field| audit[*field].as_u64() == Some(0))
}

fn actual_count(audit: &Value, field: &str) -> Result<u64> {
    audit["counters"][field]
        .as_u64()
        .context("actual REST counter missing")
}

impl PreparedListing {
    fn await_idle(&mut self, timeout: Duration, expected_loads: Option<u64>) -> Result<Value> {
        let deadline = self
            .native_deadline
            .context("native acceptance deadline missing")?
            .min(Instant::now() + timeout);
        let mut previous_match = false;
        loop {
            ensure!(
                Instant::now() < deadline,
                "actual REST native acceptance deadline expired"
            );
            let audit = self.observer.snapshot()?;
            ensure!(
                audit["valid_observation"] == true,
                "actual REST observation was tainted"
            );
            ensure!(
                actual_count(&audit, "listing_active_peak")? <= 8,
                "actual catalog listing exceeded eight concurrent requests"
            );
            let loads = actual_count(&audit, "metadata_loads")?;
            if let Some(expected) = expected_loads {
                ensure!(
                    loads <= expected,
                    "actual REST discovery duplicated metadata loads"
                );
            }
            let matches = idle(&audit) && expected_loads.is_none_or(|expected| loads == expected);
            if matches && previous_match {
                ensure!(
                    Instant::now() < deadline,
                    "actual REST native acceptance deadline expired"
                );
                return Ok(audit);
            }
            ensure!(
                Instant::now() < deadline,
                "actual REST phase did not reach its frozen idle condition"
            );
            previous_match = matches;
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn phase(
        &mut self,
        phase: &str,
        timeout: Duration,
        expected_loads: Option<u64>,
        operation: impl FnOnce() -> Result<Value>,
    ) -> Result<Value> {
        self.await_idle(timeout, None)?;
        self.observer.reset(phase)?;
        let outcome = operation();
        // Retain actual traffic even when SQL refuses or times out. Do not
        // continue to later workloads after such a refusal.
        let immediate = self.observer.snapshot();
        let sql_error = outcome.as_ref().err().map(|error| format!("{error:#}"));
        let observer_error = immediate.as_ref().err().map(|error| format!("{error:#}"));
        let saved = write_json(
            &self
                .preparation_root
                .join(format!("native-{phase}-immediate.json")),
            &json!({"sql_outcome":if outcome.is_ok() {"passed"} else {"failed"},
                "sql":outcome.as_ref().ok(),"sql_error":sql_error,
                "provider":immediate.as_ref().ok(),"observer_error":observer_error}),
        );
        if let Err(error) = outcome {
            return Err(anyhow::anyhow!(
                "native SQL phase failed: {error:#}; observer outcome: {}; receipt outcome: {}",
                observer_error.as_deref().unwrap_or("snapshot retained"),
                saved
                    .as_ref()
                    .err()
                    .map(|error| format!("{error:#}"))
                    .unwrap_or_else(|| "retained".to_owned())
            ));
        }
        saved?;
        immediate?;
        let result = outcome?;
        ensure!(
            Instant::now()
                < self
                    .native_deadline
                    .context("native acceptance deadline missing")?,
            "actual REST native acceptance deadline expired after SQL"
        );
        let audit = self.await_idle(timeout, expected_loads)?;
        write_json(
            &self
                .preparation_root
                .join(format!("native-{phase}-idle.json")),
            &audit,
        )?;
        Ok(json!({"sql":result,"provider":audit}))
    }
}

#[derive(Default)]
pub struct RealRestListing {
    fixture: Mutex<Option<PreparedListing>>,
}

impl Scenario for RealRestListing {
    fn name(&self) -> &'static str {
        "catalog/mem-1-m07-real-rest-listing"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        let (prepared, launch) = PreparedListing::start(root)?;
        let mut owner = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("real REST fixture lock poisoned"))?;
        ensure!(
            owner.is_none(),
            "real REST fixture already owns a private instance"
        );
        *owner = Some(prepared);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let freeze: Value = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/real-rest-cl-native-freeze-v1.json"
        ))?;
        ensure!(
            freeze["frozen_before_execution"] == true,
            "real REST native input is not frozen"
        );
        let timeout = context
            .remaining("real REST CL SQL")?
            .min(Duration::from_secs(120));
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let mut control = mysql_actor::connect(&user, port, timeout)?;
        let mut owner = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("real REST fixture lock poisoned"))?;
        let fixture = owner
            .as_mut()
            .context("real REST fixture was not prepared")?;
        let native_seconds = freeze["deadlines"]["native_scenario_seconds"]
            .as_u64()
            .context("native frozen deadline missing")?;
        ensure!(
            native_seconds == 1800,
            "native frozen acceptance deadline changed"
        );
        fixture.native_deadline = Some(
            context
                .deadline()
                .min(Instant::now() + Duration::from_secs(native_seconds)),
        );
        let mut phases = Vec::new();
        let admission = fixture.catalog_sql.clone();
        phases.push(measure(context, "real-rest-catalog-admission", || {
            let result = fixture.phase("catalog_admission", timeout, Some(16384), || {
                control.query_drop(&admission)?;
                Ok(json!({"catalog":"cl_normal"}))
            })?;
            ensure!(
                actual_count(&result["provider"], "mutations")? == 0,
                "catalog admission mutated provider state"
            );
            Ok(result)
        })?);
        ensure!(
            admission
                .matches("CREATE EXTERNAL CATALOG cl_normal ")
                .count()
                == 1,
            "discovery catalog replacement must have one exact name"
        );
        let discovery = admission.replacen(
            "CREATE EXTERNAL CATALOG cl_normal ",
            "CREATE EXTERNAL CATALOG cl_discovery ",
            1,
        );
        phases.push(measure(context, "real-rest-lake-discovery", || {
            let result = fixture.phase("lake_discovery", timeout, Some(16384), || {
                control.query_drop(&discovery)?;
                Ok(json!({"catalog":"cl_discovery"}))
            })?;
            ensure!(
                actual_count(&result["provider"], "mutations")? == 0,
                "lake discovery mutated provider state"
            );
            Ok(result)
        })?);
        for clients in [1usize, 8, 16] {
            phases.push(measure(context, "real-rest-information-schema", || {
                let result = fixture.phase(&format!("information_schema_{clients}"), timeout, Some(0), || {
                    let barrier = Arc::new(Barrier::new(clients));
                    thread::scope(|scope| -> Result<()> {
                        let handles = (0..clients).map(|_| {
                            let barrier = barrier.clone();
                            let user = &user;
                            scope.spawn(move || -> Result<()> {
                                barrier.wait();
                                let mut connection = mysql_actor::connect(user, port, timeout)?;
                                let count = connection.query_first::<u64, _>("SELECT COUNT(*) FROM cl_normal.information_schema.tables WHERE TABLE_CATALOG='cl_normal' AND TABLE_SCHEMA LIKE 'cl_ns_%'")?;
                                ensure!(count == Some(16384), "real REST information_schema omitted or duplicated tables");
                                Ok(())
                            })
                        }).collect::<Vec<_>>();
                        let mut first_error = None;
                        // Always join every owner, including after an earlier
                        // client refuses. No detached SQL work outlives a phase.
                        for handle in handles {
                            let result = handle.join().map_err(|_| anyhow::anyhow!("real REST client panicked")).and_then(|result| result);
                            if first_error.is_none() { first_error = result.err(); }
                        }
                        if let Some(error) = first_error { return Err(error); }
                        Ok(())
                    })?;
                    Ok(json!({"clients":clients,"rows_per_client":16384}))
                })?;
                ensure!(result["provider"]["routes"]["list_tables"].as_u64() == Some((clients * 64) as u64),
                    "real REST information_schema skipped or duplicated actual table pages");
                ensure!(actual_count(&result["provider"], "mutations")? == 0, "information_schema mutated provider state");
                Ok(result)
            })?);
        }
        for namespace in 0..32 {
            control.query_drop(format!("USE cl_normal.cl_ns_{namespace:04}"))?;
            phases.push(measure(context, "real-rest-show-views", || {
                let result = fixture.phase(
                    &format!("show_views_{namespace:04}"),
                    timeout,
                    Some(0),
                    || {
                        let mut views = control.query::<String, _>("SHOW VIEWS")?;
                        views.sort();
                        let expected = (0..512)
                            .map(|name| format!("cl_view_{name:06}"))
                            .collect::<Vec<_>>();
                        ensure!(
                            views == expected,
                            "real REST SHOW VIEWS omitted or duplicated names"
                        );
                        Ok(json!({"namespace":namespace,"rows":views.len()}))
                    },
                )?;
                ensure!(
                    result["provider"]["routes"]["list_views"].as_u64() == Some(2),
                    "SHOW VIEWS did not read both actual REST pages"
                );
                ensure!(
                    actual_count(&result["provider"], "mutations")? == 0,
                    "SHOW VIEWS mutated provider state"
                );
                Ok(result)
            })?);
        }
        phases.push(measure(context, "real-rest-drop-success", || {
            let result = fixture.phase("drop_success", timeout, None, || {
                control.query_drop("DROP DATABASE cl_normal.cl_ns_0000 FORCE")?;
                Ok(json!({"dropped_namespace":"cl_ns_0000"}))
            })?;
            ensure!(
                actual_count(&result["provider"], "mutations")? == 1025
                    && actual_count(&result["provider"], "successful_mutations")? == 1025,
                "real REST DROP omitted an actual table, view or namespace mutation"
            );
            Ok(result)
        })?);
        await_resource_convergence(context, &baseline, "real REST CL")?;
        ensure!(
            Instant::now()
                < fixture
                    .native_deadline
                    .context("native acceptance deadline missing")?,
            "actual REST native acceptance deadline expired after convergence"
        );
        // This independent provider oracle never uses the observer and runs
        // after FE phase sampling stops. It performs no mutation or retry.
        producer(
            &fixture.preparation_root,
            &fixture.producer_freeze,
            "verify-after-drop",
            Some(&fixture.preparation_root.join("bulk/READY.json")),
        )?;
        ensure!(
            fixture
                .preparation_root
                .join("verify-after-drop/VERIFICATION_PASS.json")
                .is_file(),
            "real REST remaining-object oracle did not publish verification"
        );
        write_json(
            &context
                .scenario_root()
                .join("real-rest-listing-measurements.json"),
            &json!({
                "native_freeze":freeze,"fixture":"stock private Iceberg REST; actual external metadata, no row data",
                "scope":"sampled FE allocator high-water and actual REST calls; observer and producer allocations excluded; not a hard SDK byte bound or cross-provider CL completion",
                "phases":phases,"verification":"real-rest-preparation/verify-after-drop/VERIFICATION_PASS.json"
            }),
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let owner = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("real REST fixture lock poisoned"))?
            .take();
        let Some(mut owner) = owner else {
            return Ok(());
        };
        let observer = owner.observer.stop();
        // On control refusal, Drop still kills/reaps/joins the exact observer
        // before its downstream fixture can be stopped.
        drop(owner.observer);
        let fixture = owner.fixture.shutdown();
        match (observer, fixture) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(observer), Err(fixture)) => Err(anyhow::anyhow!(
                "real REST observer cleanup failed: {observer:#}; private fixture cleanup failed: {fixture:#}"
            )),
            (Err(error), Ok(())) => Err(error).context("stop and join real REST observer"),
            (Ok(()), Err(error)) => Err(error),
        }
    }
}
