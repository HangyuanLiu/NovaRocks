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

use anyhow::{Context, Result, bail};
use std::sync::mpsc::{Receiver, RecvTimeoutError, sync_channel};
use std::thread::JoinHandle;
use std::time::Instant;

/// Runs one bounded external effect independently from an FE process lifecycle.
///
/// The operation owns its client and timeout. Dropping this handle detaches the
/// thread instead of pretending the remote effect was cancelled; scenarios
/// must use `finish` and retain their own deadline/observation evidence.
pub struct EffectActor<T: Send + 'static> {
    name: String,
    started: Receiver<Instant>,
    completed: Receiver<(Instant, Result<T>)>,
    thread: Option<JoinHandle<()>>,
}

pub struct EffectReceipt<T> {
    pub started_at: Instant,
    pub completed_at: Instant,
    pub value: T,
}

impl<T: Send + 'static> EffectActor<T> {
    pub fn spawn(
        name: impl Into<String>,
        operation: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<Self> {
        let name = name.into();
        let thread_name = format!("effect-{name}");
        let (started_tx, started) = sync_channel(1);
        let (completed_tx, completed) = sync_channel(1);
        let thread = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let started_at = Instant::now();
                if started_tx.send(started_at).is_err() {
                    return;
                }
                let result = operation();
                let _ = completed_tx.send((Instant::now(), result));
            })
            .context("spawn external effect actor")?;
        Ok(Self {
            name,
            started,
            completed,
            thread: Some(thread),
        })
    }

    pub fn wait_until_started(&self, deadline: Instant) -> Result<Instant> {
        let timeout = deadline.saturating_duration_since(Instant::now());
        self.started
            .recv_timeout(timeout)
            .with_context(|| format!("wait for effect actor {} to start", self.name))
    }

    pub fn finish(mut self, started_at: Instant, deadline: Instant) -> Result<EffectReceipt<T>> {
        let timeout = deadline.saturating_duration_since(Instant::now());
        let (completed_at, result) = match self.completed.recv_timeout(timeout) {
            Ok(receipt) => receipt,
            Err(RecvTimeoutError::Timeout) => {
                bail!(
                    "effect actor {} did not complete before its deadline",
                    self.name
                )
            }
            Err(RecvTimeoutError::Disconnected) => {
                bail!(
                    "effect actor {} exited without a completion receipt",
                    self.name
                )
            }
        };
        let value = result.with_context(|| format!("effect actor {} failed", self.name))?;
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| anyhow::anyhow!("effect actor {} panicked", self.name))?;
        }
        Ok(EffectReceipt {
            started_at,
            completed_at,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn returns_explicit_start_and_completion_receipts() {
        let actor = EffectActor::spawn("receipt", || Ok::<_, anyhow::Error>(42)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        let started = actor.wait_until_started(deadline).unwrap();
        let receipt = actor.finish(started, deadline).unwrap();
        assert_eq!(receipt.value, 42);
        assert!(receipt.completed_at >= receipt.started_at);
    }
}
