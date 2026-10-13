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

//! Finite public-MySQL actors with explicit cancellation and actual joins.

use super::connector::{await_resource_convergence, resource_baseline};
use crate::actors::mysql as mysql_actor;
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

// These are test execution/cleanup guards, never product response-time gates.
const IO_GUARD: Duration = Duration::from_secs(60);
const CONTROL_GUARD: Duration = Duration::from_secs(5);

pub(super) struct MysqlActor<T> {
    ready: mpsc::Receiver<u32>,
    done: mpsc::Receiver<Result<T>>,
    connection_id: Option<u32>,
    thread: Option<JoinHandle<()>>,
    label: String,
}

impl<T: Send + 'static> MysqlActor<T> {
    pub(super) fn spawn(
        context: &ScenarioContext,
        label: &str,
        operation: impl FnOnce(&mut mysql::Conn) -> Result<T> + Send + 'static,
    ) -> Result<Self> {
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let timeout = context
            .remaining("connect finite provider actor")?
            .min(IO_GUARD);
        let (ready_tx, ready) = mpsc::channel();
        let (done_tx, done) = mpsc::channel();
        let thread = thread::spawn(move || {
            let result = (|| {
                let mut connection =
                    mysql_actor::connect_with_io_timeout(&user, port, timeout, IO_GUARD)?;
                ready_tx.send(connection.connection_id())?;
                operation(&mut connection)
            })();
            let _ = done_tx.send(result);
        });
        Ok(Self {
            ready,
            done,
            connection_id: None,
            thread: Some(thread),
            label: label.to_owned(),
        })
    }

    pub(super) fn connection_id(&mut self, timeout: Duration) -> Result<u32> {
        if let Some(id) = self.connection_id {
            return Ok(id);
        }
        match self.ready.recv_timeout(timeout) {
            Ok(id) => {
                self.connection_id = Some(id);
                Ok(id)
            }
            Err(error) => match self.done.try_recv() {
                Ok(Err(failure)) => Err(failure),
                _ => Err(error)
                    .with_context(|| format!("{} did not report its connection ID", self.label)),
            },
        }
    }

    pub(super) fn receive(&mut self, timeout: Duration) -> Result<T> {
        self.done
            .recv_timeout(timeout)
            .with_context(|| format!("{} did not complete", self.label))?
    }

    pub(super) fn try_result(&mut self) -> Result<Option<T>> {
        match self.done.try_recv() {
            Ok(result) => result.map(Some),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                anyhow::bail!("{} exited without a result", self.label)
            }
        }
    }

    pub(super) fn cancel(&mut self, context: &ScenarioContext) -> Result<()> {
        if self.thread.as_ref().is_none_or(JoinHandle::is_finished) {
            return Ok(());
        }
        if self.connection_id.is_none() {
            self.connection_id = self.ready.try_recv().ok();
        }
        if let Some(id) = self.connection_id {
            let mut control =
                mysql_actor::connect(context.mysql_user(), context.mysql_port(), CONTROL_GUARD)?;
            control
                .query_drop(format!("KILL QUERY {id}"))
                .with_context(|| format!("cancel {} connection {id}", self.label))?;
        }
        Ok(())
    }

    pub(super) fn finish(&mut self, context: &mut ScenarioContext) -> Result<()> {
        let Some(actor) = self.thread.as_ref() else {
            return Ok(());
        };
        let deadline = Instant::now() + IO_GUARD + CONTROL_GUARD;
        while !actor.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        let forced_shutdown = !actor.is_finished();
        let mut failures = Vec::new();
        if forced_shutdown {
            // This is failed test cleanup. Record the first available diagnostic
            // before the existing cluster owner closes any remaining sockets.
            failures.push(format!(
                "{} did not physically exit within the test I/O guard",
                self.label
            ));
            if let Err(error) = std::fs::write(
                context
                    .scenario_root()
                    .join(format!("{}-forced-shutdown.json", self.label)),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "actor":self.label,"diagnostics":context.diagnostics(),"failed":true,
                }))?,
            ) {
                failures.push(format!("save pre-shutdown diagnostic: {error}"));
            }
            if let Err(error) = context.shutdown() {
                failures.push(format!("forced cluster cleanup: {error:#}"));
            }
        }
        if self
            .thread
            .take()
            .expect("actor handle retained until actual join")
            .join()
            .is_err()
        {
            failures.push(format!("{} actor panicked", self.label));
        }
        ensure!(failures.is_empty(), "actor exit failed: {failures:?}");
        Ok(())
    }
}

type MysqlCheck = Box<dyn FnOnce(&mut mysql::Conn) -> Result<()> + Send>;

/// Release every prepared connection together. A connection setup failure
/// aborts the gate; an execution failure cancels every remaining actor before
/// joining any of them. All completion/cleanup errors are retained.
pub(super) fn run_checks(
    context: &mut ScenarioContext,
    label: &str,
    checks: Vec<MysqlCheck>,
) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let gate = Arc::new((Mutex::new(None::<bool>), Condvar::new()));
    let mut actors = Vec::new();
    let mut failures = Vec::new();
    for (index, check) in checks.into_iter().enumerate() {
        let actor_gate = Arc::clone(&gate);
        match MysqlActor::spawn(context, &format!("{label}-{index}"), move |connection| {
            let (lock, condition) = &*actor_gate;
            let mut state = lock
                .lock()
                .map_err(|_| anyhow::anyhow!("provider actor gate poisoned"))?;
            while state.is_none() {
                state = condition
                    .wait(state)
                    .map_err(|_| anyhow::anyhow!("provider actor gate poisoned"))?;
            }
            ensure!(
                *state == Some(true),
                "provider actor start aborted after peer setup failed"
            );
            drop(state);
            check(connection)
        }) {
            Ok(actor) => actors.push(actor),
            Err(error) => {
                failures.push(format!("spawn actor {index}: {error:#}"));
                break;
            }
        }
    }
    for actor in &mut actors {
        if let Err(error) = actor.connection_id(CONTROL_GUARD) {
            failures.push(format!("prepare {}: {error:#}", actor.label));
        }
    }
    {
        let (state, condition) = &*gate;
        *state
            .lock()
            .map_err(|_| anyhow::anyhow!("provider actor gate poisoned"))? =
            Some(failures.is_empty());
        condition.notify_all();
    }
    let mut completed = vec![false; actors.len()];
    let execution = (|| -> Result<()> {
        ensure!(
            failures.is_empty(),
            "provider actor setup failed: {failures:?}"
        );
        while completed.iter().any(|done| !done) {
            for (index, actor) in actors.iter_mut().enumerate() {
                if !completed[index] {
                    match actor.try_result() {
                        Ok(Some(())) => completed[index] = true,
                        Ok(None) => {}
                        Err(error) => {
                            completed[index] = true;
                            return Err(error);
                        }
                    }
                }
            }
            context.remaining("await finite provider actors")?;
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    })();
    if let Err(error) = &execution {
        failures.push(format!("execution: {error:#}"));
    }
    if execution.is_err() {
        for actor in &mut actors {
            if let Err(error) = actor.cancel(context) {
                failures.push(format!("cancellation: {error:#}"));
            }
        }
    }
    for actor in &mut actors {
        if let Err(error) = actor.finish(context) {
            failures.push(format!("actual join: {error:#}"));
        }
    }
    for (index, actor) in actors.iter_mut().enumerate() {
        if !completed[index]
            && let Err(error) = actor.try_result()
        {
            failures.push(format!("{} result: {error:#}", actor.label));
        }
    }
    if let Err(error) = await_resource_convergence(context, &baseline, label) {
        failures.push(format!("native resource exit: {error:#}"));
    }
    std::fs::write(
        context.scenario_root().join(format!("{label}-actors.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"actors":actors.len(),"errors":failures,"actual_joins_completed":true}),
        )?,
    )?;
    ensure!(
        failures.is_empty(),
        "finite provider actors failed: {failures:?}"
    );
    Ok(())
}

/// Retain the exact cancellation request/connection facts, without paths,
/// headers or bodies. It is diagnostic evidence, not a replacement for exits.
pub(super) fn record_cancelled_proxy_read(
    context: &ScenarioContext,
    label: &str,
    proxy: &novarocks_cluster_harness::delayed_s3::DelayedS3Proxy,
    hold: &novarocks_cluster_harness::delayed_s3::DelayedS3ReadHold,
) -> Result<()> {
    let observation = hold.read_exit_observation();
    let identity = observation.as_ref().ok();
    let overflow = proxy.snapshot().event_overflow;
    let events = (overflow == 0).then(|| {
        proxy
            .event_log()
            .into_iter()
            .filter(|event| {
                identity.is_some_and(|identity| event.request_id == identity.request_id)
            })
            .map(|event| {
                serde_json::json!({
                    "kind":format!("{:?}",event.kind),"request_id":event.request_id,
                    "connection_id":event.connection_id,"elapsed_millis":event.elapsed_millis,
                    "method":event.method.as_str(),"protocol":event.protocol,
                    "object_id":event.object_id,"range":event.range,"bytes":event.bytes,
                })
            })
            .collect::<Vec<_>>()
    });
    let connections = (overflow == 0).then(|| {
        proxy
            .connection_log()
            .into_iter()
            .filter(|event| {
                identity.is_some_and(|identity| event.connection_id == identity.connection_id)
            })
            .map(|event| {
                serde_json::json!({
                    "kind":format!("{:?}",event.kind),"connection_id":event.connection_id,
                    "elapsed_millis":event.elapsed_millis,
                })
            })
            .collect::<Vec<_>>()
    });
    std::fs::write(
        context
            .scenario_root()
            .join(format!("{label}-proxy-exit.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "observation":identity,"observation_error":observation.as_ref().err().map(ToString::to_string),
            "events":events,"connections":connections,"event_overflow":overflow,
        }))?,
    )?;
    ensure!(
        overflow == 0,
        "cancelled proxy raw evidence overflowed: {overflow}"
    );
    Ok(())
}
