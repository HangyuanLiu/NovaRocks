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

//! Existing scan SQL actors with an explicit idle-connection submission gate.
use super::super::native_sql_controls::SelectedStatement;
use crate::{actors::mysql as mysql_actor, scenario::ScenarioContext};
use anyhow::{Context, Result};
use mysql::prelude::Queryable;
use std::sync::{Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

pub(in crate::scenarios) struct ScanActor<T> {
    pub connection_id: u32,
    pub done: mpsc::Receiver<mysql::Result<T>>,
    actor: Option<JoinHandle<Result<()>>>,
    user: String,
    port: u16,
    deadline: Instant,
}
impl<T: Send + 'static> ScanActor<T> {
    pub fn start(
        context: &mut ScenarioContext,
        sql: String,
        selected: Option<SelectedStatement>,
        query: impl FnOnce(&mut mysql::Conn, &str) -> mysql::Result<T> + Send + 'static,
    ) -> Result<Self> {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (submit_tx, submit_rx) = mpsc::sync_channel(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        context.remaining("connect original scan actor")?;
        let deadline = context.deadline();
        let actor_user = user.clone();
        let actor_sql = sql.clone();
        let actor = thread::Builder::new()
            .name("selected-scan-sql".into())
            .spawn(move || -> Result<()> {
                let connection =
                    mysql_actor::connect_for_cancellation_until(&actor_user, port, deadline)?;
                connection.run(|connection| {
                    // These actors use fully qualified SQL and no SET/USE prelude.
                    // This actual connection is idle until its owner releases the gate.
                    ready_tx.send(connection.connection_id())?;
                    match submit_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    {
                        Ok(()) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            anyhow::bail!("original SQL submission gate deadline expired")
                        }
                    }
                    match done_tx.send(query(connection, &actor_sql)) {
                        Ok(()) => {}
                        Err(mpsc::SendError(Err(actual))) => {
                            return Err(anyhow::Error::from(actual).context(
                                "original scan query failed after its result owner closed",
                            ));
                        }
                        Err(mpsc::SendError(Ok(_))) => {
                            return Err(anyhow::anyhow!(
                                "original scan result owner closed before delivery"
                            ));
                        }
                    }
                    Ok(())
                })
            })?;
        let ready = (|| -> Result<u32> {
            let cid = ready_rx
                .recv_timeout(context.remaining("receive idle scan connection")?)
                .context("original scan actor did not publish its actual connection")?;
            if let Some(selected) = selected {
                selected.arm(context, cid, &sql)?;
            }
            Ok(cid)
        })();
        let cid = match ready {
            Ok(cid) => cid,
            Err(primary) => {
                drop(submit_tx);
                return combine::<Self>(Err(primary), join(actor));
            }
        };
        if let Err(primary) = submit_tx.send(()) {
            return combine::<Self>(Err(primary.into()), join(actor));
        }
        Ok(Self {
            connection_id: cid,
            done,
            actor: Some(actor),
            user,
            port,
            deadline,
        })
    }

    /// Every known success/failure path settles the same original actor. Public
    /// KILL and cluster shutdown are failure-only cleanup, never new admission.
    pub fn settle<U>(mut self, context: &mut ScenarioContext, primary: Result<U>) -> Result<U> {
        let mut cleanup = Ok(());
        if primary.is_err()
            && self
                .actor
                .as_ref()
                .is_some_and(|actor| !actor.is_finished())
        {
            cleanup = cancel(&self.user, self.port, self.connection_id, self.deadline);
            if cleanup.is_err() {
                cleanup = combine(cleanup, context.shutdown());
            }
        }
        if let Some(actor) = self.actor.take() {
            cleanup = combine(cleanup, join(actor));
        }
        if let Ok(Err(query_error)) = self.done.try_recv() {
            cleanup = combine(cleanup, Err(query_error.into()));
        }
        combine(primary, cleanup)
    }
}
impl<T> Drop for ScanActor<T> {
    fn drop(&mut self) {
        // The normal driver calls settle so failures remain reportable. This
        // unwind fallback still joins, instead of detaching live SQL work.
        if let Some(actor) = self.actor.take() {
            if !actor.is_finished() {
                let _ = cancel(&self.user, self.port, self.connection_id, self.deadline);
            }
            let _ = join(actor);
        }
    }
}
fn cancel(user: &str, port: u16, cid: u32, deadline: Instant) -> Result<()> {
    mysql_actor::connect_until(user, port, deadline)?.run(|control| {
        control.query_drop(format!("KILL QUERY {cid}"))?;
        Ok(())
    })
}
pub(super) fn join(actor: JoinHandle<Result<()>>) -> Result<()> {
    actor
        .join()
        .map_err(|payload| ActorPanic(Mutex::new(payload)))?
}
pub(in crate::scenarios) fn combine<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(ActorCleanupFailure { primary, cleanup }.into()),
    }
}
struct ActorCleanupFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Debug for ActorCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ActorCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original scan operation failed: {:#}; actual actor cleanup failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::error::Error for ActorCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
struct ActorPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for ActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original scan actor panicked; source retained")
    }
}
impl std::error::Error for ActorPanic {}
