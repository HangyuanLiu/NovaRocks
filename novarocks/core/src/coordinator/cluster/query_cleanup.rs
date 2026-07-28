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

use crate::common::types::UniqueId;
use crate::runtime::query_state::in_flight_table;

use super::{BeId, RegistryEvent, RegistryEventSink};

pub(crate) struct QueryCleanupSink;

impl QueryCleanupSink {
    pub(crate) fn new() -> Self {
        Self
    }

    fn fail_backend_queries(be_id: BeId, reason: String) {
        let affected = in_flight_table().on_backend_failed(be_id as usize, reason.clone());
        for query_id in affected {
            let write_query_id = UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            };
            crate::coordinator::write::mark_query_failed(&write_query_id, reason.clone());
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
            } => Self::fail_backend_queries(
                be_id,
                format!("backend {be_id} restarted (epoch {old_epoch} -> {new_epoch})"),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::write::write_registry_test_guard;
    use crate::query_execution::write::WriterKey;
    use crate::runtime::exchange::{ExchangeKey, set_expected_senders, snapshot_receiver_state};
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use crate::runtime::query_state::{QueryState, in_flight_table};

    struct Cleanup {
        queries: Vec<QueryId>,
        fragment_instances: Vec<UniqueId>,
    }

    impl Cleanup {
        fn new(queries: Vec<QueryId>, fragment_instances: Vec<UniqueId>) -> Self {
            Self {
                queries,
                fragment_instances,
            }
        }
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            for query in &self.queries {
                in_flight_table().forget(*query);
            }
            let manager = query_context_manager();
            for fragment_instance in &self.fragment_instances {
                manager.unregister_finst(fragment_instance.clone());
                crate::runtime::exchange::cancel_fragment(
                    fragment_instance.hi,
                    fragment_instance.lo,
                );
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

    fn key(fragment_instance: &UniqueId, node_id: i32) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: fragment_instance.hi,
            finst_id_lo: fragment_instance.lo,
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
            vec![affected_finst.clone(), unaffected_finst.clone()],
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
        let fragment_instance = fid(11);
        let receiver_key = key(&fragment_instance, 611);
        let _cleanup = Cleanup::new(vec![query], vec![fragment_instance.clone()]);

        in_flight_table().register(query, fragment_instance.clone(), 63);
        query_context_manager().register_finst(fragment_instance, query);
        set_expected_senders(receiver_key, 1);
        assert!(snapshot_receiver_state(receiver_key).is_some());

        QueryCleanupSink::new().on_event(RegistryEvent::BackendLost { be_id: 63 });

        assert_eq!(in_flight_table().state(query), Some(QueryState::Failed));
        assert!(snapshot_receiver_state(receiver_key).is_none());
    }

    #[test]
    fn backend_restarted_fails_query_with_epoch_reason() {
        let query = qid(21);
        let fragment_instance = fid(21);
        let _cleanup = Cleanup::new(vec![query], vec![fragment_instance.clone()]);
        in_flight_table().register(query, fragment_instance, 64);

        QueryCleanupSink::new().on_event(RegistryEvent::BackendRestarted {
            be_id: 64,
            old_epoch: 7,
            new_epoch: 8,
        });

        let reason = in_flight_table()
            .failure_reason(query)
            .expect("failure reason");
        assert!(reason.contains("restarted"), "reason={reason}");
        assert!(reason.contains("7 -> 8"), "reason={reason}");
    }

    #[test]
    fn backend_lost_fails_query_wide_write_coordinator() {
        let query = qid(31);
        let query_uid = UniqueId {
            hi: query.hi,
            lo: query.lo,
        };
        let fragment_instance = fid(31);
        let _cleanup = Cleanup::new(vec![query], vec![fragment_instance.clone()]);
        let mut write_registry = write_registry_test_guard();
        let writer = WriterKey {
            query_id: query_uid.clone(),
            fragment_instance_id: fragment_instance.clone(),
            backend_num: 65,
        };
        let coordinator = write_registry
            .register_query(query_uid, vec![writer])
            .expect("register write coordinator");
        in_flight_table().register(query, fragment_instance, 65);

        QueryCleanupSink::new().on_event(RegistryEvent::BackendLost { be_id: 65 });

        let coordinator = coordinator.lock().expect("write coordinator lock");
        assert!(coordinator.has_failed());
        assert_eq!(
            coordinator.failed_reason().as_deref(),
            Some("backend 65 lost")
        );
        let abort = coordinator
            .abort_input()
            .expect("backend loss must make write abort-ready");
        assert_eq!(abort.reason, "backend 65 lost");
    }

    #[test]
    fn backend_restart_fails_query_wide_write_coordinator_with_epoch_reason() {
        let query = qid(41);
        let query_uid = UniqueId {
            hi: query.hi,
            lo: query.lo,
        };
        let fragment_instance = fid(41);
        let _cleanup = Cleanup::new(vec![query], vec![fragment_instance.clone()]);
        let mut write_registry = write_registry_test_guard();
        let writer = WriterKey {
            query_id: query_uid.clone(),
            fragment_instance_id: fragment_instance.clone(),
            backend_num: 66,
        };
        let coordinator = write_registry
            .register_query(query_uid, vec![writer])
            .expect("register write coordinator");
        in_flight_table().register(query, fragment_instance, 66);

        QueryCleanupSink::new().on_event(RegistryEvent::BackendRestarted {
            be_id: 66,
            old_epoch: 10,
            new_epoch: 11,
        });

        let coordinator = coordinator.lock().expect("write coordinator lock");
        assert_eq!(
            coordinator.failed_reason().as_deref(),
            Some("backend 66 restarted (epoch 10 -> 11)")
        );
        let abort = coordinator
            .abort_input()
            .expect("backend restart must make write abort-ready");
        assert_eq!(abort.reason, "backend 66 restarted (epoch 10 -> 11)");
    }
}
