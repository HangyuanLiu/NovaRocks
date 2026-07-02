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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryKey {
    pub hi: i64,
    pub lo: i64,
}

impl QueryKey {
    pub fn from_hi_lo(hi: i64, lo: i64) -> Self {
        Self { hi, lo }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RfDropReason {
    SizeExceeded,
    SendFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RfBuiltInfo {
    pub rows: i64,
    pub bytes: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RfAcquiredInfo {
    pub outcome: String,
    pub latency_ns: i64,
}

#[derive(Default)]
pub struct RuntimeFilterLifecycleRegistry {
    queries: RwLock<HashMap<QueryKey, Arc<QueryRfLifecycle>>>,
}

impl RuntimeFilterLifecycleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn global() -> &'static Self {
        runtime_filter_lifecycle_registry()
    }

    pub fn recorder(&self, query: QueryKey) -> RfLifecycleRecorder {
        RfLifecycleRecorder {
            query,
            lifecycle: self.query_entry(query),
        }
    }

    pub fn snapshot(&self, query: QueryKey) -> Option<QueryRfSnapshot> {
        let lifecycle = {
            let guard = rw_read(&self.queries);
            guard.get(&query).cloned()
        };
        lifecycle.map(|lifecycle| lifecycle.snapshot())
    }

    pub fn remove_query(&self, query: QueryKey) {
        rw_write(&self.queries).remove(&query);
    }

    fn query_entry(&self, query: QueryKey) -> Arc<QueryRfLifecycle> {
        let mut guard = rw_write(&self.queries);
        Arc::clone(
            guard
                .entry(query)
                .or_insert_with(|| Arc::new(QueryRfLifecycle::new())),
        )
    }
}

pub fn runtime_filter_lifecycle_registry() -> &'static RuntimeFilterLifecycleRegistry {
    static REGISTRY: OnceLock<RuntimeFilterLifecycleRegistry> = OnceLock::new();
    REGISTRY.get_or_init(RuntimeFilterLifecycleRegistry::new)
}

#[derive(Default)]
pub struct QueryRfLifecycle {
    filters: RwLock<HashMap<i32, Arc<RfLifecycleRecord>>>,
}

impl QueryRfLifecycle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> QueryRfSnapshot {
        let filters = rw_read(&self.filters)
            .iter()
            .map(|(filter_id, record)| (*filter_id, record.snapshot()))
            .collect();
        QueryRfSnapshot { filters }
    }

    fn record(&self, filter_id: i32) -> Arc<RfLifecycleRecord> {
        let mut guard = rw_write(&self.filters);
        Arc::clone(
            guard
                .entry(filter_id)
                .or_insert_with(|| Arc::new(RfLifecycleRecord::new())),
        )
    }
}

#[derive(Default)]
pub struct RfLifecycleRecord {
    planned: AtomicBool,
    built: Mutex<Option<RfBuiltInfo>>,
    sent_partials: AtomicI64,
    sent_bytes: AtomicI64,
    merged_received: AtomicI64,
    merged_expected: AtomicI64,
    delivered: AtomicBool,
    acquired: Mutex<Option<RfAcquiredInfo>>,
    applied_input_rows: AtomicI64,
    applied_output_rows: AtomicI64,
    applied_evals: AtomicI64,
    dropped: Mutex<Option<RfDropReason>>,
    disabled: AtomicBool,
}

impl RfLifecycleRecord {
    pub fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> RfRecordView {
        RfRecordView {
            planned: self.planned.load(Ordering::Relaxed),
            built: *mutex_lock(&self.built),
            sent_partials: self.sent_partials.load(Ordering::Relaxed),
            sent_bytes: self.sent_bytes.load(Ordering::Relaxed),
            merged_received: self.merged_received.load(Ordering::Relaxed),
            merged_expected: self.merged_expected.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            acquired: mutex_lock(&self.acquired).clone(),
            applied_input_rows: self.applied_input_rows.load(Ordering::Relaxed),
            applied_output_rows: self.applied_output_rows.load(Ordering::Relaxed),
            applied_evals: self.applied_evals.load(Ordering::Relaxed),
            dropped: *mutex_lock(&self.dropped),
            disabled: self.disabled.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct RfLifecycleRecorder {
    query: QueryKey,
    lifecycle: Arc<QueryRfLifecycle>,
}

impl RfLifecycleRecorder {
    pub fn query(&self) -> QueryKey {
        self.query
    }

    pub fn planned(&self, filter_id: i32) {
        let record = self.lifecycle.record(filter_id);
        record.planned.store(true, Ordering::Relaxed);
    }

    pub fn built(&self, filter_id: i32, rows: i64, bytes: i64) {
        let record = self.lifecycle.record(filter_id);
        *mutex_lock(&record.built) = Some(RfBuiltInfo { rows, bytes });
    }

    pub fn sent_partial(&self, filter_id: i32, bytes: i64) {
        let record = self.lifecycle.record(filter_id);
        record.sent_partials.fetch_add(1, Ordering::Relaxed);
        record.sent_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn merge_progress(&self, filter_id: i32, received: i64, expected: i64) {
        let record = self.lifecycle.record(filter_id);
        record.merged_received.store(received, Ordering::Relaxed);
        record.merged_expected.store(expected, Ordering::Relaxed);
    }

    pub fn delivered(&self, filter_id: i32) {
        let record = self.lifecycle.record(filter_id);
        record.delivered.store(true, Ordering::Relaxed);
    }

    pub fn acquired(&self, filter_id: i32, outcome: impl Into<String>, latency_ns: i64) {
        let record = self.lifecycle.record(filter_id);
        *mutex_lock(&record.acquired) = Some(RfAcquiredInfo {
            outcome: outcome.into(),
            latency_ns,
        });
    }

    pub fn applied(&self, filter_id: i32, input_rows: i64, output_rows: i64, evals: i64) {
        let record = self.lifecycle.record(filter_id);
        record
            .applied_input_rows
            .fetch_add(input_rows, Ordering::Relaxed);
        record
            .applied_output_rows
            .fetch_add(output_rows, Ordering::Relaxed);
        record.applied_evals.fetch_add(evals, Ordering::Relaxed);
    }

    pub fn dropped(&self, filter_id: i32, reason: RfDropReason) {
        let record = self.lifecycle.record(filter_id);
        *mutex_lock(&record.dropped) = Some(reason);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryRfSnapshot {
    pub filters: HashMap<i32, RfRecordView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RfRecordView {
    pub planned: bool,
    pub built: Option<RfBuiltInfo>,
    pub sent_partials: i64,
    pub sent_bytes: i64,
    merged_received: i64,
    merged_expected: i64,
    pub delivered: bool,
    pub acquired: Option<RfAcquiredInfo>,
    applied_input_rows: i64,
    applied_output_rows: i64,
    applied_evals: i64,
    pub dropped: Option<RfDropReason>,
    pub disabled: bool,
}

impl RfRecordView {
    pub fn merged_received(&self) -> i64 {
        self.merged_received
    }

    pub fn merged_expected(&self) -> i64 {
        self.merged_expected
    }

    pub fn applied_input_rows(&self) -> i64 {
        self.applied_input_rows
    }

    pub fn applied_output_rows(&self) -> i64 {
        self.applied_output_rows
    }

    pub fn applied_evals(&self) -> i64 {
        self.applied_evals
    }
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn rw_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn rw_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_record_accumulates_and_exports() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let q = QueryKey::from_hi_lo(1, 2);
        let rec = registry.recorder(q);

        rec.planned(7);
        rec.built(7, 3, 128);
        rec.sent_partial(7, 128);
        rec.merge_progress(7, 1, 3);
        rec.merge_progress(7, 3, 3);
        rec.delivered(7);
        rec.applied(7, 1024, 100, 1);
        rec.applied(7, 1024, 50, 1);
        rec.dropped(9, RfDropReason::SizeExceeded);

        let snap = registry.snapshot(q).expect("query snapshot");
        let f7 = snap.filters.get(&7).expect("filter 7");
        assert_eq!(f7.built.as_ref().map(|b| (b.rows, b.bytes)), Some((3, 128)));
        assert_eq!(f7.merged_received(), 3);
        assert_eq!(f7.merged_expected(), 3);
        assert!(f7.delivered);
        assert_eq!(f7.applied_input_rows(), 2048);
        assert_eq!(f7.applied_output_rows(), 150);
        assert_eq!(f7.applied_evals(), 2);
        let f9 = snap.filters.get(&9).expect("filter 9");
        assert_eq!(f9.dropped, Some(RfDropReason::SizeExceeded));

        registry.remove_query(q);
        assert!(registry.snapshot(q).is_none());
    }

    #[test]
    fn recorder_is_noop_safe_for_unknown_query() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let q = QueryKey::from_hi_lo(9, 9);
        registry.recorder(q).applied(1, 10, 10, 1);
        assert!(
            registry.snapshot(q).is_some(),
            "recorder auto-creates the query entry"
        );
    }
}
