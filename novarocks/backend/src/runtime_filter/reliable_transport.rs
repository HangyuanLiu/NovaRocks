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

//! Opaque, query-scoped reliable transport bookkeeping.
//!
//! The state deliberately does not interpret an envelope's route subtype,
//! sequence, or payload. Domain and native adapters validate those facts
//! before they hand a complete identity, immutable frame, and retained byte
//! count to this module.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub(crate) trait ReliableTransportPolicy: Copy {
    fn retry_interval(self) -> Duration;

    fn max_attempts(self) -> u32;

    fn deadline(self) -> Duration;

    fn max_pending_entries(self) -> usize;

    fn max_pending_bytes(self) -> usize;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReliableTransportResourceLimit {
    PendingEntries,
    PendingBytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReliableTransportFailOpenReason {
    Deadline,
    AttemptsExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReliableTransportSendOutcome {
    Buffered,
    ResourceLimit(ReliableTransportResourceLimit),
    Duplicate,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReliableTransportStateError {
    IdentityConflict,
    RetiredIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReliableTransportTick<K, F> {
    retried: Vec<(K, F)>,
    failed_open: Vec<(K, ReliableTransportFailOpenReason)>,
}

impl<K, F> ReliableTransportTick<K, F> {
    pub(crate) fn retried(&self) -> &[(K, F)] {
        &self.retried
    }

    pub(crate) fn failed_open(&self) -> &[(K, ReliableTransportFailOpenReason)] {
        &self.failed_open
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReliableTransportFailureOutcome<K> {
    RetryScheduled,
    FailedOpen(K, ReliableTransportFailOpenReason),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReliableTransportAckOutcome<F> {
    Released(F),
    Unknown,
}

struct PendingFrame<F> {
    frame: F,
    retained_bytes: usize,
    first_sent_at: Instant,
    last_sent_at: Instant,
    attempts: u32,
    retry_scheduled: bool,
}

/// One bounded retry state for one query participant.
///
/// `max_attempts` includes the initial admission. A retry is eligible only
/// after the caller reports an actual transport failure; no timer may create a
/// second in-flight send for an identity whose first attempt is still pending.
pub(crate) struct ReliableTransportState<K, F, P> {
    policy: P,
    pending: BTreeMap<K, PendingFrame<F>>,
    completed: BTreeMap<K, F>,
    pending_bytes: usize,
    shutdown: bool,
}

impl<K, F, P> ReliableTransportState<K, F, P>
where
    K: Copy + Ord,
    F: Clone + Eq,
    P: ReliableTransportPolicy,
{
    pub(crate) fn new(policy: P) -> Self {
        Self {
            policy,
            pending: BTreeMap::new(),
            completed: BTreeMap::new(),
            pending_bytes: 0,
            shutdown: false,
        }
    }

    pub(crate) fn send(
        &mut self,
        key: K,
        frame: F,
        retained_bytes: usize,
        now: Instant,
    ) -> Result<ReliableTransportSendOutcome, ReliableTransportStateError> {
        if self.shutdown {
            return Ok(ReliableTransportSendOutcome::Shutdown);
        }
        if let Some(existing) = self.pending.get(&key).map(|entry| &entry.frame) {
            return if existing == &frame {
                Ok(ReliableTransportSendOutcome::Duplicate)
            } else {
                Err(ReliableTransportStateError::IdentityConflict)
            };
        }
        if let Some(existing) = self.completed.get(&key) {
            return if existing == &frame {
                Ok(ReliableTransportSendOutcome::Duplicate)
            } else {
                Err(ReliableTransportStateError::RetiredIdentity)
            };
        }
        if self.pending.len() >= self.policy.max_pending_entries() {
            return Ok(ReliableTransportSendOutcome::ResourceLimit(
                ReliableTransportResourceLimit::PendingEntries,
            ));
        }
        let Some(next_pending_bytes) = self.pending_bytes.checked_add(retained_bytes) else {
            return Ok(ReliableTransportSendOutcome::ResourceLimit(
                ReliableTransportResourceLimit::PendingBytes,
            ));
        };
        if next_pending_bytes > self.policy.max_pending_bytes() {
            return Ok(ReliableTransportSendOutcome::ResourceLimit(
                ReliableTransportResourceLimit::PendingBytes,
            ));
        }
        self.pending_bytes = next_pending_bytes;
        self.pending.insert(
            key,
            PendingFrame {
                frame,
                retained_bytes,
                first_sent_at: now,
                last_sent_at: now,
                attempts: 1,
                retry_scheduled: false,
            },
        );
        Ok(ReliableTransportSendOutcome::Buffered)
    }

    pub(crate) fn acknowledge(&mut self, key: K) -> ReliableTransportAckOutcome<F> {
        let Some(entry) = self.pending.remove(&key) else {
            return ReliableTransportAckOutcome::Unknown;
        };
        self.pending_bytes = self.pending_bytes.saturating_sub(entry.retained_bytes);
        self.completed.insert(key, entry.frame.clone());
        ReliableTransportAckOutcome::Released(entry.frame)
    }

    pub(crate) fn transport_failed(
        &mut self,
        key: K,
        now: Instant,
    ) -> ReliableTransportFailureOutcome<K> {
        let Some(entry) = self.pending.get_mut(&key) else {
            return ReliableTransportFailureOutcome::Unknown;
        };
        if now.saturating_duration_since(entry.first_sent_at) >= self.policy.deadline() {
            return self.release_failed_open(key, ReliableTransportFailOpenReason::Deadline);
        }
        if entry.attempts >= self.policy.max_attempts() {
            return self
                .release_failed_open(key, ReliableTransportFailOpenReason::AttemptsExhausted);
        }
        entry.retry_scheduled = true;
        ReliableTransportFailureOutcome::RetryScheduled
    }

    pub(crate) fn schedule_all_pending_retries(&mut self) {
        for entry in self.pending.values_mut() {
            entry.retry_scheduled = true;
        }
    }

    pub(crate) fn drive(&mut self, now: Instant) -> ReliableTransportTick<K, F> {
        let mut retried = Vec::new();
        let mut failed_open = Vec::new();
        let policy = self.policy;
        self.pending.retain(|key, entry| {
            if now.saturating_duration_since(entry.first_sent_at) >= policy.deadline() {
                self.pending_bytes = self.pending_bytes.saturating_sub(entry.retained_bytes);
                failed_open.push((*key, ReliableTransportFailOpenReason::Deadline));
                self.completed.insert(*key, entry.frame.clone());
                return false;
            }
            if entry.retry_scheduled
                && entry.attempts < policy.max_attempts()
                && now.saturating_duration_since(entry.last_sent_at) >= policy.retry_interval()
            {
                entry.attempts += 1;
                entry.last_sent_at = now;
                entry.retry_scheduled = false;
                retried.push((*key, entry.frame.clone()));
            }
            true
        });
        ReliableTransportTick {
            retried,
            failed_open,
        }
    }

    pub(crate) fn shutdown(&mut self) {
        self.shutdown = true;
        self.pending.clear();
        self.pending_bytes = 0;
    }

    #[cfg(test)]
    pub(crate) fn set_pending_bytes_for_test(&mut self, pending_bytes: usize) {
        self.pending_bytes = pending_bytes;
    }

    fn release_failed_open(
        &mut self,
        key: K,
        reason: ReliableTransportFailOpenReason,
    ) -> ReliableTransportFailureOutcome<K> {
        let entry = self
            .pending
            .remove(&key)
            .expect("pending entry was checked before fail-open release");
        self.pending_bytes = self.pending_bytes.saturating_sub(entry.retained_bytes);
        self.completed.insert(key, entry.frame);
        ReliableTransportFailureOutcome::FailedOpen(key, reason)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ReliableTransportFailOpenReason, ReliableTransportFailureOutcome, ReliableTransportPolicy,
        ReliableTransportSendOutcome, ReliableTransportState,
    };
    use std::time::{Duration, Instant};

    #[derive(Clone, Copy)]
    struct Policy;

    impl ReliableTransportPolicy for Policy {
        fn retry_interval(self) -> Duration {
            Duration::from_millis(5)
        }

        fn max_attempts(self) -> u32 {
            2
        }

        fn deadline(self) -> Duration {
            Duration::from_millis(20)
        }

        fn max_pending_entries(self) -> usize {
            2
        }

        fn max_pending_bytes(self) -> usize {
            16
        }
    }

    #[test]
    fn retry_requires_an_observed_transport_failure() {
        let now = Instant::now();
        let mut state = ReliableTransportState::new(Policy);
        assert_eq!(
            state.send(1u8, "frame", 5, now).unwrap(),
            ReliableTransportSendOutcome::Buffered
        );
        assert!(
            state
                .drive(now + Duration::from_millis(6))
                .retried()
                .is_empty()
        );
        assert_eq!(
            state.transport_failed(1, now + Duration::from_millis(1)),
            ReliableTransportFailureOutcome::RetryScheduled
        );
        assert_eq!(
            state.drive(now + Duration::from_millis(6)).retried(),
            &[(1, "frame")]
        );
    }

    #[test]
    fn last_allowed_transport_failure_fails_open_once() {
        let now = Instant::now();
        let mut state = ReliableTransportState::new(Policy);
        state.send(1u8, "frame", 5, now).unwrap();
        assert_eq!(
            state.transport_failed(1, now),
            ReliableTransportFailureOutcome::RetryScheduled
        );
        state.drive(now + Duration::from_millis(6));
        assert_eq!(
            state.transport_failed(1, now + Duration::from_millis(7)),
            ReliableTransportFailureOutcome::FailedOpen(
                1,
                ReliableTransportFailOpenReason::AttemptsExhausted
            )
        );
        assert_eq!(
            state.transport_failed(1, now + Duration::from_millis(8)),
            ReliableTransportFailureOutcome::Unknown
        );
    }
}
