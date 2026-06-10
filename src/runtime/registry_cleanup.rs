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

use crate::runtime::backend_registry::{BeId, RegistryEvent};
use crate::runtime::heartbeat_mgr::RegistryEventSink;
use crate::runtime::query_state::in_flight_table;

pub struct QueryCleanupSink;

impl QueryCleanupSink {
    pub fn new() -> Self {
        Self
    }

    fn fail_backend_queries(be_id: BeId, reason: String) {
        let affected = in_flight_table().on_backend_failed(be_id as usize, reason.clone());
        for query_id in affected {
            crate::cancel_query_by_id(query_id, reason.clone());
        }
    }
}

impl RegistryEventSink for QueryCleanupSink {
    fn on_event(&self, event: RegistryEvent) {
        match event {
            RegistryEvent::BackendLost { be_id } => {
                Self::fail_backend_queries(be_id, format!("backend {be_id} lost"));
            }
            RegistryEvent::BackendRestarted {
                be_id,
                old_epoch,
                new_epoch,
            } => {
                Self::fail_backend_queries(
                    be_id,
                    format!("backend {be_id} restarted (epoch {old_epoch} -> {new_epoch})"),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::UniqueId;
    use crate::runtime::backend_registry::RegistryEvent;
    use crate::runtime::exchange::{ExchangeKey, set_expected_senders, snapshot_receiver_state};
    use crate::runtime::heartbeat_mgr::RegistryEventSink;
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use crate::runtime::query_state::{QueryState, in_flight_table};

    struct Cleanup {
        queries: Vec<QueryId>,
        finsts: Vec<UniqueId>,
    }

    impl Cleanup {
        fn new(queries: Vec<QueryId>, finsts: Vec<UniqueId>) -> Self {
            Self { queries, finsts }
        }
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            for query in &self.queries {
                in_flight_table().forget(*query);
            }
            let manager = query_context_manager();
            for finst in &self.finsts {
                manager.unregister_finst(*finst);
                crate::runtime::exchange::cancel_fragment(finst.hi, finst.lo);
            }
        }
    }

    fn qid(n: i64) -> QueryId {
        QueryId {
            hi: 6_100_000 + n,
            lo: 6_200_000 + n,
        }
    }

    fn fid(n: i64) -> UniqueId {
        UniqueId {
            hi: 6_300_000 + n,
            lo: 6_400_000 + n,
        }
    }

    fn key(finst: UniqueId, node_id: i32) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: finst.hi,
            finst_id_lo: finst.lo,
            node_id,
        }
    }

    #[test]
    fn backend_lost_fails_only_queries_on_that_backend() {
        let affected_query = qid(1);
        let unaffected_query = qid(2);
        let affected_finst = fid(1);
        let unaffected_finst = fid(2);
        let _cleanup = Cleanup::new(
            vec![affected_query, unaffected_query],
            vec![affected_finst, unaffected_finst],
        );

        let table = in_flight_table();
        table.register(affected_query, affected_finst, 61);
        table.register(unaffected_query, unaffected_finst, 62);

        QueryCleanupSink::new().on_event(RegistryEvent::BackendLost { be_id: 61 });

        assert_eq!(table.state(affected_query), Some(QueryState::Failed));
        assert_eq!(
            table.failure_reason(affected_query).as_deref(),
            Some("backend 61 lost")
        );
        assert_eq!(table.state(unaffected_query), Some(QueryState::Running));
        assert_eq!(table.failure_reason(unaffected_query), None);
    }

    #[test]
    fn backend_lost_cleans_mapped_exchange_receivers() {
        let query = qid(11);
        let finst = fid(11);
        let receiver_key = key(finst, 611);
        let _cleanup = Cleanup::new(vec![query], vec![finst]);

        in_flight_table().register(query, finst, 63);
        query_context_manager().register_finst(finst, query);
        set_expected_senders(receiver_key, 1);
        assert!(snapshot_receiver_state(receiver_key).is_some());

        QueryCleanupSink::new().on_event(RegistryEvent::BackendLost { be_id: 63 });

        assert_eq!(in_flight_table().state(query), Some(QueryState::Failed));
        assert!(snapshot_receiver_state(receiver_key).is_none());
    }

    #[test]
    fn backend_restarted_fails_query_with_epoch_reason() {
        let query = qid(21);
        let finst = fid(21);
        let _cleanup = Cleanup::new(vec![query], vec![finst]);

        in_flight_table().register(query, finst, 64);

        QueryCleanupSink::new().on_event(RegistryEvent::BackendRestarted {
            be_id: 64,
            old_epoch: 7,
            new_epoch: 8,
        });

        assert_eq!(in_flight_table().state(query), Some(QueryState::Failed));
        let reason = in_flight_table()
            .failure_reason(query)
            .expect("failure reason");
        assert!(reason.contains("restarted"), "reason={reason}");
        assert!(reason.contains("7 -> 8"), "reason={reason}");
    }
}
