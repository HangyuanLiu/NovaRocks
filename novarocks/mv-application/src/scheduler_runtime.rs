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

//! Product-owned process scheduler state for materialized-view refresh.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::scheduler::MvSchedulerConfig;
use crate::maintenance::{MvBackgroundEngineError, MvBackgroundEngineErrorKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvRefreshDisposition {
    Completed,
    NoOp,
    AlreadyActive,
    TargetGone,
    TransientUnavailable(String),
    InvalidDefinition(String),
    TerminalFailure(String),
    Corruption(String),
    InvariantViolation(String),
    ShutdownCancelled,
}

impl MvRefreshDisposition {
    pub fn from_background_error(error: MvBackgroundEngineError) -> Self {
        match error.kind() {
            MvBackgroundEngineErrorKind::TargetGone => Self::TargetGone,
            MvBackgroundEngineErrorKind::TransientUnavailable => {
                Self::TransientUnavailable(error.message().to_owned())
            }
            MvBackgroundEngineErrorKind::InvalidDefinition => {
                Self::InvalidDefinition(error.message().to_owned())
            }
            MvBackgroundEngineErrorKind::TerminalFailure => {
                Self::TerminalFailure(error.message().to_owned())
            }
            MvBackgroundEngineErrorKind::Corruption => Self::Corruption(error.message().to_owned()),
            MvBackgroundEngineErrorKind::InvariantViolation => {
                Self::InvariantViolation(error.message().to_owned())
            }
            MvBackgroundEngineErrorKind::ShutdownCancelled => Self::ShutdownCancelled,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvRefreshRuntimeDecision {
    Success,
    TransientBackoff { error: String, retry_at_ms: i64 },
    Blocked { error: String },
    NoChange,
}

/// Coalesced product runtime.  Hosts interpret durable MV definitions and
/// execute concrete effects, but cannot own a second queue, capacity set, or
/// retry ledger.
#[derive(Debug)]
pub struct MvRefreshSchedulerRuntime<K, R> {
    config: MvSchedulerConfig,
    queue: VecDeque<(K, R)>,
    queued: BTreeSet<K>,
    running: BTreeSet<K>,
    failures: BTreeMap<K, u32>,
    retry_not_before_ms: BTreeMap<K, i64>,
    blocked: BTreeMap<K, String>,
}

impl<K, R> MvRefreshSchedulerRuntime<K, R>
where
    K: Clone + Ord,
{
    pub fn new(config: MvSchedulerConfig) -> Self {
        Self {
            config,
            queue: VecDeque::new(),
            queued: BTreeSet::new(),
            running: BTreeSet::new(),
            failures: BTreeMap::new(),
            retry_not_before_ms: BTreeMap::new(),
            blocked: BTreeMap::new(),
        }
    }

    pub const fn enabled(&self) -> bool {
        self.config.enabled()
    }

    pub fn is_suppressed(&self, key: &K, now_ms: i64) -> bool {
        self.retry_not_before_ms
            .get(key)
            .is_some_and(|retry_at_ms| now_ms < *retry_at_ms)
            || self.blocked.contains_key(key)
            || self.queued.contains(key)
            || self.running.contains(key)
    }

    pub fn enqueue(&mut self, key: K, request: R) {
        if !self.running.contains(&key) && self.queued.insert(key.clone()) {
            self.queue.push_back((key, request));
        }
    }

    pub fn take_ready(&mut self) -> Vec<R> {
        let capacity = self
            .config
            .max_concurrent_refreshes()
            .max(1)
            .saturating_sub(self.running.len());
        let mut ready = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let Some((key, request)) = self.queue.pop_front() else {
                break;
            };
            self.queued.remove(&key);
            if !self.running.contains(&key) {
                ready.push(request);
            }
        }
        ready
    }

    pub fn mark_started(&mut self, key: &K) -> bool {
        if self.running.len() >= self.config.max_concurrent_refreshes().max(1)
            || self.running.contains(key)
        {
            return false;
        }
        self.running.insert(key.clone())
    }

    pub fn requeue(&mut self, key: K, request: R) {
        self.enqueue(key, request);
    }

    pub fn complete(
        &mut self,
        key: &K,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.running.remove(key);
        self.record(key, disposition, now_ms)
    }

    pub fn record(
        &mut self,
        key: &K,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        let decision = match disposition {
            MvRefreshDisposition::Completed | MvRefreshDisposition::NoOp => {
                MvRefreshRuntimeDecision::Success
            }
            MvRefreshDisposition::TransientUnavailable(error) => {
                let attempt = *self
                    .failures
                    .entry(key.clone())
                    .and_modify(|attempt| *attempt = attempt.saturating_add(1))
                    .or_insert(1);
                MvRefreshRuntimeDecision::TransientBackoff {
                    error,
                    retry_at_ms: now_ms.saturating_add(backoff_ms(&self.config, attempt)),
                }
            }
            MvRefreshDisposition::InvalidDefinition(error)
            | MvRefreshDisposition::TerminalFailure(error)
            | MvRefreshDisposition::Corruption(error)
            | MvRefreshDisposition::InvariantViolation(error) => {
                MvRefreshRuntimeDecision::Blocked { error }
            }
            MvRefreshDisposition::AlreadyActive
            | MvRefreshDisposition::TargetGone
            | MvRefreshDisposition::ShutdownCancelled => MvRefreshRuntimeDecision::NoChange,
        };
        match &decision {
            MvRefreshRuntimeDecision::Success => {
                self.failures.remove(key);
                self.retry_not_before_ms.remove(key);
                self.blocked.remove(key);
            }
            MvRefreshRuntimeDecision::TransientBackoff { retry_at_ms, .. } => {
                self.retry_not_before_ms.insert(key.clone(), *retry_at_ms);
                self.blocked.remove(key);
            }
            MvRefreshRuntimeDecision::Blocked { error } => {
                self.failures.remove(key);
                self.retry_not_before_ms.remove(key);
                self.blocked.insert(key.clone(), error.clone());
            }
            MvRefreshRuntimeDecision::NoChange => {}
        }
        decision
    }

    pub fn reset_after_source_change(&mut self, key: &K) {
        self.failures.remove(key);
        self.retry_not_before_ms.remove(key);
        self.blocked.remove(key);
    }

    pub fn pending_len(&self) -> usize {
        self.queue.len()
    }

    pub fn running_len(&self) -> usize {
        self.running.len()
    }
}

fn backoff_ms(config: &MvSchedulerConfig, attempt: u32) -> i64 {
    let base = config.failure_backoff_ms().max(1);
    let maximum = config.max_failure_backoff_ms().max(base);
    let shift = attempt.saturating_sub(1).min(62);
    base.saturating_mul(1_i64.checked_shl(shift).unwrap_or(i64::MAX))
        .min(maximum)
}

#[cfg(test)]
mod tests {
    use super::{MvRefreshDisposition, MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime};
    use crate::scheduler::MvSchedulerConfig;

    #[test]
    fn runtime_coalesces_and_releases_only_typed_terminal_work() {
        let mut runtime =
            MvRefreshSchedulerRuntime::new(MvSchedulerConfig::new(true, 1, 1, 10, 40));
        runtime.enqueue(7_i64, "first");
        runtime.enqueue(7_i64, "duplicate");
        assert_eq!(runtime.take_ready(), ["first"]);
        assert!(runtime.mark_started(&7));
        assert!(runtime.take_ready().is_empty());
        assert_eq!(
            runtime.complete(
                &7,
                MvRefreshDisposition::TransientUnavailable("offline".into()),
                100
            ),
            MvRefreshRuntimeDecision::TransientBackoff {
                error: "offline".into(),
                retry_at_ms: 110,
            }
        );
        assert!(runtime.is_suppressed(&7, 109));
        runtime.reset_after_source_change(&7);
        assert!(!runtime.is_suppressed(&7, 109));
    }
}
