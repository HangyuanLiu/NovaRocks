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
//! FE-side query state machine and in-flight fragment table for standalone
//! distributed execution.
//!
//! This module tracks query lifecycle and the fragments currently associated
//! with each backend. It is intentionally small and pure so later tasks can
//! feed it from exec-status reports and backend registry events.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use crate::common::types::UniqueId;
use crate::runtime::query_context::QueryId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryState {
    Running,
    Finished,
    Failed,
}

#[derive(Clone, Debug)]
struct FinstRecord {
    backend_idx: usize,
    done: bool,
}

#[derive(Debug)]
struct QueryEntry {
    state: QueryState,
    failure_reason: Option<String>,
    finsts: HashMap<UniqueId, FinstRecord>,
    pending_finsts: HashSet<UniqueId>,
}

impl QueryEntry {
    fn new() -> Self {
        Self {
            state: QueryState::Running,
            failure_reason: None,
            finsts: HashMap::new(),
            pending_finsts: HashSet::new(),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == QueryState::Running && self.pending_finsts.is_empty()
    }
}

#[derive(Default, Debug)]
struct Inner {
    queries: HashMap<QueryId, QueryEntry>,
    finst_to_query: HashMap<UniqueId, QueryId>,
}

pub(crate) struct InFlightQueryTable {
    inner: Mutex<Inner>,
}

impl InFlightQueryTable {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }

    pub(crate) fn register(&self, query: QueryId, finst: UniqueId, backend_idx: usize) {
        let mut guard = self.inner.lock().expect("query_state lock");
        if let Some(&owner) = guard.finst_to_query.get(&finst) {
            if owner != query {
                return;
            }
            let Some(entry) = guard.queries.get_mut(&query) else {
                return;
            };
            if let std::collections::hash_map::Entry::Vacant(slot) = entry.finsts.entry(finst) {
                slot.insert(FinstRecord {
                    backend_idx,
                    done: false,
                });
                entry.pending_finsts.insert(finst);
            }
            return;
        }

        let entry = guard.queries.entry(query).or_insert_with(QueryEntry::new);
        if let std::collections::hash_map::Entry::Vacant(slot) = entry.finsts.entry(finst) {
            slot.insert(FinstRecord {
                backend_idx,
                done: false,
            });
            entry.pending_finsts.insert(finst);
        }
        guard.finst_to_query.insert(finst, query);
    }

    pub(crate) fn state(&self, query: QueryId) -> Option<QueryState> {
        self.inner
            .lock()
            .expect("query_state lock")
            .queries
            .get(&query)
            .map(|entry| entry.state)
    }

    pub(crate) fn failure_reason(&self, query: QueryId) -> Option<String> {
        self.inner
            .lock()
            .expect("query_state lock")
            .queries
            .get(&query)
            .and_then(|entry| entry.failure_reason.clone())
    }

    pub(crate) fn finsts_on_backend(&self, query: QueryId, backend_idx: usize) -> Vec<UniqueId> {
        let guard = self.inner.lock().expect("query_state lock");
        let Some(entry) = guard.queries.get(&query) else {
            return Vec::new();
        };

        let mut finsts: Vec<UniqueId> = entry
            .finsts
            .iter()
            .filter_map(|(finst, record)| (record.backend_idx == backend_idx).then_some(*finst))
            .collect();
        finsts.sort_by_key(|id| (id.hi, id.lo));
        finsts
    }

    pub(crate) fn on_fragment_done(&self, finst: UniqueId, result: Result<(), String>) {
        let mut guard = self.inner.lock().expect("query_state lock");
        let Some(&query) = guard.finst_to_query.get(&finst) else {
            return;
        };
        let Some(entry) = guard.queries.get_mut(&query) else {
            return;
        };
        if entry.state != QueryState::Running {
            return;
        }

        match result {
            Ok(()) => {
                let Some(record) = entry.finsts.get_mut(&finst) else {
                    return;
                };
                if record.done {
                    return;
                }
                record.done = true;
                entry.pending_finsts.remove(&finst);
                if entry.is_complete() {
                    entry.state = QueryState::Finished;
                }
            }
            Err(reason) => {
                entry.state = QueryState::Failed;
                entry.failure_reason = Some(reason);
            }
        }
    }

    pub(crate) fn on_backend_lost(&self, backend_idx: usize) -> Vec<QueryId> {
        self.on_backend_failed(backend_idx, format!("backend {backend_idx} lost"))
    }

    pub(crate) fn on_backend_failed(&self, backend_idx: usize, reason: String) -> Vec<QueryId> {
        let mut guard = self.inner.lock().expect("query_state lock");
        let mut failed = Vec::new();

        for (query_id, entry) in guard.queries.iter_mut() {
            if entry.state != QueryState::Running {
                continue;
            }
            if entry
                .finsts
                .values()
                .any(|record| record.backend_idx == backend_idx)
            {
                entry.state = QueryState::Failed;
                entry.failure_reason = Some(reason.clone());
                failed.push(*query_id);
            }
        }

        failed.sort_by_key(|id| (id.hi, id.lo));
        failed
    }

    pub(crate) fn forget(&self, query: QueryId) {
        let mut guard = self.inner.lock().expect("query_state lock");
        if let Some(entry) = guard.queries.remove(&query) {
            for finst in entry.finsts.keys() {
                guard.finst_to_query.remove(finst);
            }
        }
    }
}

static TABLE: OnceLock<InFlightQueryTable> = OnceLock::new();

pub(crate) fn in_flight_table() -> &'static InFlightQueryTable {
    TABLE.get_or_init(InFlightQueryTable::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qid(n: i64) -> QueryId {
        QueryId { hi: n, lo: n }
    }

    fn fid(n: i64) -> UniqueId {
        UniqueId { hi: n, lo: n }
    }

    #[test]
    fn tracks_finsts_per_backend_and_completes() {
        let t = InFlightQueryTable::new();
        t.register(qid(1), fid(11), 1);
        t.register(qid(1), fid(10), 1);
        assert_eq!(t.state(qid(1)), Some(QueryState::Running));
        assert_eq!(t.finsts_on_backend(qid(1), 1), vec![fid(10), fid(11)]);
        t.on_fragment_done(fid(10), Ok(()));
        assert_eq!(t.state(qid(1)), Some(QueryState::Running));
        t.on_fragment_done(fid(11), Ok(()));
        assert_eq!(t.state(qid(1)), Some(QueryState::Finished));
    }

    #[test]
    fn fragment_failure_fails_query_with_reason() {
        let t = InFlightQueryTable::new();
        t.register(qid(2), fid(20), 0);
        t.register(qid(2), fid(21), 1);
        t.on_fragment_done(fid(20), Err("be#0 crashed".into()));
        assert_eq!(t.state(qid(2)), Some(QueryState::Failed));
        assert_eq!(t.failure_reason(qid(2)).as_deref(), Some("be#0 crashed"));
    }

    #[test]
    fn backend_lost_fails_queries_touching_that_backend() {
        let t = InFlightQueryTable::new();
        t.register(qid(3), fid(30), 0);
        t.register(qid(2), fid(20), 0);
        let failed = t.on_backend_lost(0);
        assert_eq!(failed, vec![qid(2), qid(3)]);
        assert_eq!(t.state(qid(3)), Some(QueryState::Failed));
        assert_eq!(t.state(qid(2)), Some(QueryState::Failed));
    }

    #[test]
    fn backend_failed_records_custom_reason_atomically() {
        let t = InFlightQueryTable::new();
        t.register(qid(12), fid(120), 2);
        t.register(qid(13), fid(130), 3);

        let failed = t.on_backend_failed(2, "backend 2 restarted (epoch 7 -> 8)".into());

        assert_eq!(failed, vec![qid(12)]);
        assert_eq!(t.state(qid(12)), Some(QueryState::Failed));
        assert_eq!(
            t.failure_reason(qid(12)).as_deref(),
            Some("backend 2 restarted (epoch 7 -> 8)")
        );
        assert_eq!(t.state(qid(13)), Some(QueryState::Running));
        assert_eq!(t.failure_reason(qid(13)), None);
    }

    #[test]
    fn finished_ignores_late_error_and_failed_ignores_late_ok() {
        let t = InFlightQueryTable::new();

        t.register(qid(5), fid(50), 0);
        t.on_fragment_done(fid(50), Ok(()));
        assert_eq!(t.state(qid(5)), Some(QueryState::Finished));
        assert_eq!(t.failure_reason(qid(5)), None);
        t.on_fragment_done(fid(50), Err("late failure".into()));
        assert_eq!(t.state(qid(5)), Some(QueryState::Finished));
        assert_eq!(t.failure_reason(qid(5)), None);

        t.register(qid(6), fid(60), 0);
        t.on_fragment_done(fid(60), Err("be#1 crashed".into()));
        assert_eq!(t.state(qid(6)), Some(QueryState::Failed));
        t.on_fragment_done(fid(60), Ok(()));
        assert_eq!(t.state(qid(6)), Some(QueryState::Failed));
        assert_eq!(t.failure_reason(qid(6)).as_deref(), Some("be#1 crashed"));
    }

    #[test]
    fn duplicate_finst_on_another_query_does_not_corrupt_owner() {
        let t = InFlightQueryTable::new();
        t.register(qid(7), fid(70), 0);
        t.register(qid(8), fid(70), 1);

        assert_eq!(t.state(qid(8)), None);
        assert_eq!(t.finsts_on_backend(qid(8), 1), Vec::<UniqueId>::new());

        t.on_fragment_done(fid(70), Ok(()));
        assert_eq!(t.state(qid(7)), Some(QueryState::Finished));
        assert_eq!(t.state(qid(8)), None);
    }

    #[test]
    fn forget_removes_reverse_mapping_and_allows_reuse() {
        let t = InFlightQueryTable::new();
        t.register(qid(9), fid(90), 0);
        t.forget(qid(9));

        t.on_fragment_done(fid(90), Ok(()));
        assert_eq!(t.state(qid(9)), None);

        t.register(qid(10), fid(90), 1);
        assert_eq!(t.state(qid(10)), Some(QueryState::Running));
        t.on_fragment_done(fid(90), Ok(()));
        assert_eq!(t.state(qid(10)), Some(QueryState::Finished));
    }
}
