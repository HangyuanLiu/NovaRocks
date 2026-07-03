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
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;

use crate::common::ids::SlotId;
use crate::exec::runtime_filter::{
    RuntimeInFilter, RuntimeMembershipFilter, StarrocksRuntimeFilterType,
    decode_starrocks_in_filter, decode_starrocks_membership_filter, encode_starrocks_bitset_filter,
    encode_starrocks_bloom_filter, encode_starrocks_empty_filter, encode_starrocks_in_filter,
    peek_starrocks_filter_type,
};
use crate::novarocks_logging::{debug, warn};
use crate::runtime::query_context::QueryId;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
use crate::runtime::runtime_filter_observability::{
    QueryKey, RfDropReason, RfLifecycleRecorder, RuntimeFilterLifecycleRegistry,
};
use crate::service::exchange_sender;

pub(crate) struct RuntimeFilterWorker {
    query_id: QueryId,
    params: RuntimeFilterWorkerParams,
    hub: Arc<RuntimeFilterHub>,
    merge_states: Mutex<HashMap<i32, MergeState>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeFilterWorkerParams {
    id_to_prober_targets: HashMap<i32, Vec<RuntimeFilterProberTarget>>,
    runtime_filter_builder_number: HashMap<i32, i32>,
    runtime_filter_max_size: Option<i64>,
}

impl RuntimeFilterWorkerParams {
    pub(crate) fn new(
        id_to_prober_targets: HashMap<i32, Vec<RuntimeFilterProberTarget>>,
        runtime_filter_builder_number: HashMap<i32, i32>,
        runtime_filter_max_size: Option<i64>,
    ) -> Self {
        Self {
            id_to_prober_targets,
            runtime_filter_builder_number,
            runtime_filter_max_size,
        }
    }

    fn runtime_filter_max_size(&self) -> Option<i64> {
        self.runtime_filter_max_size.filter(|v| *v > 0)
    }

    fn expected_builders(&self, filter_id: i32) -> usize {
        self.runtime_filter_builder_number
            .get(&filter_id)
            .copied()
            .unwrap_or(1)
            .max(1) as usize
    }

    fn prober_targets(&self, filter_id: i32) -> Option<&[RuntimeFilterProberTarget]> {
        self.id_to_prober_targets
            .get(&filter_id)
            .map(|targets| targets.as_slice())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterProberTarget {
    hostname: String,
    port: i32,
}

impl RuntimeFilterProberTarget {
    pub(crate) fn new(hostname: impl Into<String>, port: i32) -> Self {
        Self {
            hostname: hostname.into(),
            port,
        }
    }

    fn hostname(&self) -> &str {
        &self.hostname
    }

    fn port(&self) -> i32 {
        self.port
    }
}

struct MergeState {
    expected: usize,
    received: HashMap<i32, RuntimeFilterPayload>,
    done: bool,
}

impl MergeState {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            received: HashMap::new(),
            done: false,
        }
    }
}

impl RuntimeFilterWorker {
    pub(crate) fn new(
        query_id: QueryId,
        params: RuntimeFilterWorkerParams,
        hub: Arc<RuntimeFilterHub>,
    ) -> Self {
        Self {
            query_id,
            params,
            hub,
            merge_states: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn receive_partial(
        &self,
        filter_id: i32,
        data: &[u8],
        build_be_number: i32,
        build_data_type: Option<DataType>,
    ) -> Result<(), String> {
        let slot_id = match self.hub.filter_spec_slot_id(filter_id) {
            Some(slot_id) => slot_id,
            None => {
                warn!(
                    "runtime filter spec missing on merge node: filter_id={}",
                    filter_id
                );
                SlotId::new(0)
            }
        };
        let rf_type = peek_starrocks_filter_type(data)?;
        debug!(
            "runtime filter receive partial: filter_id={} type={:?} build_be={} bytes={}",
            filter_id,
            rf_type,
            build_be_number,
            data.len()
        );
        let filter = match rf_type {
            StarrocksRuntimeFilterType::In => {
                let hub_data_type;
                let decode_data_type = if let Some(data_type) = build_data_type.as_ref() {
                    Some(data_type)
                } else {
                    hub_data_type = self.hub.filter_spec_data_type(filter_id);
                    hub_data_type.as_ref()
                };
                RuntimeFilterPayload::In(decode_starrocks_in_filter(
                    filter_id,
                    slot_id,
                    decode_data_type,
                    data,
                )?)
            }
            _ => RuntimeFilterPayload::Membership(decode_starrocks_membership_filter(
                filter_id, slot_id, data,
            )?),
        };
        let expected = self.expected_builders(filter_id);
        debug!(
            "runtime filter merge state: filter_id={} expected_builders={}",
            filter_id, expected
        );
        let (ready, merge_progress) = {
            let mut guard = self.merge_states.lock().expect("runtime filter merge lock");
            let state = guard
                .entry(filter_id)
                .or_insert_with(|| MergeState::new(expected));
            if state.done {
                return Ok(());
            }
            if state.expected != expected {
                state.expected = expected;
            }
            if state.received.contains_key(&build_be_number) {
                return Ok(());
            }
            state.received.insert(build_be_number, filter);
            let merge_progress = Some((state.received.len(), state.expected));
            if state.received.len() < state.expected {
                (None, merge_progress)
            } else {
                let mut parts = Vec::with_capacity(state.received.len());
                for value in state.received.values() {
                    parts.push(value.clone());
                }
                state.received.clear();
                state.done = true;
                (Some(parts), merge_progress)
            }
        };

        if let Some((received, expected)) = merge_progress {
            self.recorder()
                .merge_progress(filter_id, received as i64, expected as i64);
        }

        if let Some(parts) = ready {
            let max_size = self.params.runtime_filter_max_size();
            let payload = merge_and_encode_filters(parts, max_size)?;
            if payload.size_exceeded {
                if let Some(limit) = max_size {
                    warn!(
                        "runtime filter merge size exceeded: filter_id={} limit={} final_bytes={}",
                        filter_id,
                        limit,
                        payload.data.len()
                    );
                }
                self.recorder()
                    .dropped(filter_id, RfDropReason::SizeExceeded);
            }
            debug!(
                "runtime filter merged final: filter_id={} bytes={}",
                filter_id,
                payload.data.len()
            );
            self.broadcast_final_filter(filter_id, payload.data);
        }
        Ok(())
    }

    fn expected_builders(&self, filter_id: i32) -> usize {
        self.params.expected_builders(filter_id)
    }

    fn recorder(&self) -> RfLifecycleRecorder {
        RuntimeFilterLifecycleRegistry::global()
            .recorder(QueryKey::from_hi_lo(self.query_id.hi, self.query_id.lo))
    }

    fn broadcast_final_filter(&self, filter_id: i32, data: Vec<u8>) {
        let Some(probers) = self.params.prober_targets(filter_id) else {
            return;
        };
        debug!(
            "runtime filter broadcast final: filter_id={} bytes={} targets={}",
            filter_id,
            data.len(),
            probers.len()
        );

        let mut seen_hosts = HashSet::new();
        for prober in probers {
            if prober.hostname().is_empty() {
                continue;
            }
            if !seen_hosts.insert(prober.hostname().to_string()) {
                continue;
            }
            let req = crate::proto::filter::TransmitRuntimeFilterRequest {
                is_partial: false,
                query_id: Some(crate::proto::common::UniqueId {
                    hi: self.query_id.hi,
                    lo: self.query_id.lo,
                }),
                filter_id,
                data: data.clone(),
                build_be_number: 0,
                column_type: None,
            };
            let dest_port = prober.port() as u16;
            if let Err(e) = send_final_runtime_filter(prober.hostname(), dest_port, req) {
                warn!(
                    "send runtime filter failed: dest={} filter_id={} err={}",
                    prober.hostname(),
                    filter_id,
                    e
                );
                self.recorder().dropped(filter_id, RfDropReason::SendFailed);
            }
        }
    }
}

#[cfg(not(test))]
fn send_final_runtime_filter(
    hostname: &str,
    port: u16,
    req: crate::proto::filter::TransmitRuntimeFilterRequest,
) -> Result<(), String> {
    exchange_sender::send_runtime_filter(hostname, port, req)
}

#[cfg(all(test, not(feature = "compat")))]
fn send_final_runtime_filter(
    hostname: &str,
    port: u16,
    req: crate::proto::filter::TransmitRuntimeFilterRequest,
) -> Result<(), String> {
    tests::send_final_runtime_filter_for_test(hostname, port, req)
}

#[cfg(all(test, feature = "compat"))]
fn send_final_runtime_filter(
    hostname: &str,
    port: u16,
    req: crate::proto::filter::TransmitRuntimeFilterRequest,
) -> Result<(), String> {
    exchange_sender::send_runtime_filter(hostname, port, req)
}

#[derive(Clone)]
enum RuntimeFilterPayload {
    In(RuntimeInFilter),
    Membership(RuntimeMembershipFilter),
}

struct MergedRuntimeFilterPayload {
    data: Vec<u8>,
    size_exceeded: bool,
}

fn merge_and_encode_filters(
    parts: Vec<RuntimeFilterPayload>,
    max_size: Option<i64>,
) -> Result<MergedRuntimeFilterPayload, String> {
    let mut iter = parts.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| "runtime filter merge requires at least one part".to_string())?;
    match first {
        RuntimeFilterPayload::In(mut merged) => {
            for part in iter {
                match part {
                    RuntimeFilterPayload::In(filter) => merged.merge_from(&filter)?,
                    _ => return Err("runtime filter merge type mismatch".to_string()),
                }
            }
            let mut data = encode_starrocks_in_filter(&merged)?;
            let mut size_exceeded = false;
            if let Some(limit) = max_size
                && data.len() as i64 > limit
            {
                merged = merged.empty_like();
                data = encode_starrocks_in_filter(&merged)?;
                size_exceeded = true;
            }
            Ok(MergedRuntimeFilterPayload {
                data,
                size_exceeded,
            })
        }
        RuntimeFilterPayload::Membership(merged) => {
            let mut total_size = merged.size();
            let mut force_empty = !merged.can_use_for_merge();
            let mut size_exceeded = false;
            let mut min_max = merged.min_max().clone();
            let mut merged_membership = if matches!(merged, RuntimeMembershipFilter::Empty(_)) {
                None
            } else {
                Some(merged.clone())
            };
            for part in iter {
                match part {
                    RuntimeFilterPayload::Membership(filter) => {
                        total_size = total_size.saturating_add(filter.size());
                        min_max.merge_from(filter.min_max())?;
                        if !filter.can_use_for_merge() {
                            force_empty = true;
                        }
                        match (&mut merged_membership, &filter) {
                            (Some(_current), RuntimeMembershipFilter::Empty(_)) => {}
                            (Some(current), _) => {
                                current.merge_membership_from(&filter)?;
                            }
                            (None, RuntimeMembershipFilter::Empty(_)) => {}
                            (None, _) => {
                                merged_membership = Some(filter.clone());
                            }
                        }
                    }
                    _ => return Err("runtime filter merge type mismatch".to_string()),
                }
            }
            if let Some(limit) = max_size
                && total_size as i64 > limit
            {
                force_empty = true;
                size_exceeded = true;
            }
            let mut result = if force_empty {
                let mut base = merged_membership.unwrap_or(merged);
                base.set_min_max(min_max.clone());
                base.to_empty()
            } else {
                let mut base = merged_membership.unwrap_or_else(|| merged.to_empty());
                base.set_min_max(min_max.clone());
                base
            };
            let mut data = encode_membership_filter(&result)?;
            if let Some(limit) = max_size
                && data.len() as i64 > limit
            {
                result = result.to_empty();
                result.set_min_max(min_max);
                data = encode_membership_filter(&result)?;
                size_exceeded = true;
            }
            Ok(MergedRuntimeFilterPayload {
                data,
                size_exceeded,
            })
        }
    }
}

fn encode_membership_filter(filter: &RuntimeMembershipFilter) -> Result<Vec<u8>, String> {
    match filter {
        RuntimeMembershipFilter::Bloom(bloom) => encode_starrocks_bloom_filter(bloom),
        RuntimeMembershipFilter::Empty(empty) => encode_starrocks_empty_filter(empty),
        RuntimeMembershipFilter::Bitset(bitset) => encode_starrocks_bitset_filter(bitset),
    }
}

#[cfg(all(test, not(feature = "compat")))]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::node::join::JoinRuntimeFilterSpec;
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::runtime_filter::{
        RUNTIME_FILTER_JOIN_MODE_BROADCAST, RuntimeEmptyFilter, RuntimeFilterType,
        RuntimeMinMaxFilter,
    };
    use crate::runtime::runtime_filter_observability::{
        QueryKey, RfDropReason, RuntimeFilterLifecycleRegistry,
    };

    type SendHook = Box<
        dyn Fn(&str, u16, crate::proto::filter::TransmitRuntimeFilterRequest) -> Result<(), String>
            + 'static,
    >;

    thread_local! {
        static FINAL_SEND_HOOK: RefCell<Option<SendHook>> = const { RefCell::new(None) };
    }

    pub(super) fn send_final_runtime_filter_for_test(
        hostname: &str,
        port: u16,
        params: crate::proto::filter::TransmitRuntimeFilterRequest,
    ) -> Result<(), String> {
        FINAL_SEND_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow().as_ref() {
                return hook(hostname, port, params);
            }
            exchange_sender::send_runtime_filter(hostname, port, params)
        })
    }

    struct FinalSendHookGuard {
        previous: Option<SendHook>,
    }

    impl Drop for FinalSendHookGuard {
        fn drop(&mut self) {
            let previous = self.previous.take();
            FINAL_SEND_HOOK.with(|hook| {
                *hook.borrow_mut() = previous;
            });
        }
    }

    fn install_final_send_hook(hook: SendHook) -> FinalSendHookGuard {
        let previous = FINAL_SEND_HOOK.with(|slot| slot.borrow_mut().replace(hook));
        FinalSendHookGuard { previous }
    }

    #[derive(Clone, Debug)]
    struct CapturedFinalRuntimeFilterSend {
        hostname: String,
        port: u16,
        params: crate::proto::filter::TransmitRuntimeFilterRequest,
    }

    struct LifecycleQueryGuard {
        query_key: QueryKey,
    }

    impl LifecycleQueryGuard {
        fn new(query_id: QueryId) -> Self {
            let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
            RuntimeFilterLifecycleRegistry::global().remove_query(query_key);
            Self { query_key }
        }

        fn query_key(&self) -> QueryKey {
            self.query_key
        }
    }

    impl Drop for LifecycleQueryGuard {
        fn drop(&mut self) {
            RuntimeFilterLifecycleRegistry::global().remove_query(self.query_key);
        }
    }

    #[test]
    fn receive_partial_does_not_broadcast_until_all_builders_arrive() {
        let sends = Arc::new(Mutex::new(Vec::new()));
        let sends_capture = Arc::clone(&sends);
        let _hook_guard = install_final_send_hook(Box::new(move |hostname, port, params| {
            sends_capture
                .lock()
                .expect("captured runtime filter sends lock")
                .push(CapturedFinalRuntimeFilterSend {
                    hostname: hostname.to_string(),
                    port,
                    params,
                });
            Ok(())
        }));

        let filter_id = 1;
        let slot_id = SlotId::new(7);
        let query_id = QueryId { hi: 123, lo: 456 };
        let lifecycle_guard = LifecycleQueryGuard::new(query_id);
        let hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        hub.register_filter_specs(
            100,
            &[JoinRuntimeFilterSpec {
                filter_id,
                expr_order: 0,
                probe_slot_id: slot_id,
                build_data_type: DataType::Int32,
                merge_nodes: Vec::new(),
                has_remote_targets: true,
            }],
        );

        let params = RuntimeFilterWorkerParams::new(
            HashMap::from([(
                filter_id,
                vec![RuntimeFilterProberTarget::new("127.0.0.1", 18030)],
            )]),
            HashMap::from([(filter_id, 2)]),
            None,
        );
        let worker = RuntimeFilterWorker::new(query_id, params, hub);
        let first = encoded_empty_membership_partial(filter_id, slot_id);
        let second = encoded_empty_membership_partial(filter_id, slot_id);

        worker
            .receive_partial(filter_id, &first, 10, Some(DataType::Int32))
            .expect("first partial");
        assert!(
            sends
                .lock()
                .expect("captured runtime filter sends lock")
                .is_empty(),
            "first of two build BE partials must not broadcast a final filter"
        );
        let snapshot = RuntimeFilterLifecycleRegistry::global()
            .snapshot(lifecycle_guard.query_key())
            .expect("query snapshot after first partial");
        let filter = snapshot
            .filters
            .get(&filter_id)
            .expect("filter lifecycle after first partial");
        assert_eq!(filter.merged_received(), 1);
        assert_eq!(filter.merged_expected(), 2);

        worker
            .receive_partial(filter_id, &second, 11, Some(DataType::Int32))
            .expect("second partial");
        let snapshot = RuntimeFilterLifecycleRegistry::global()
            .snapshot(lifecycle_guard.query_key())
            .expect("query snapshot after second partial");
        let filter = snapshot
            .filters
            .get(&filter_id)
            .expect("filter lifecycle after second partial");
        assert_eq!(filter.merged_received(), 2);
        assert_eq!(filter.merged_expected(), 2);

        let sends = sends.lock().expect("captured runtime filter sends lock");
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].hostname, "127.0.0.1");
        assert_eq!(sends[0].port, 18030);
        assert!(!sends[0].params.is_partial);
        assert_eq!(sends[0].params.filter_id, filter_id);
        assert_eq!(
            sends[0].params.query_id.as_ref().map(|id| (id.hi, id.lo)),
            Some((123, 456))
        );
        assert!(!sends[0].params.data.is_empty());
        assert_eq!(sends[0].params.build_be_number, 0);
        assert!(sends[0].params.column_type.is_none());
    }

    #[test]
    fn size_capped_merge_records_dropped_and_broadcasts_empty() {
        let sends = Arc::new(Mutex::new(Vec::new()));
        let sends_capture = Arc::clone(&sends);
        let _hook_guard = install_final_send_hook(Box::new(move |hostname, port, params| {
            sends_capture
                .lock()
                .expect("captured runtime filter sends lock")
                .push(CapturedFinalRuntimeFilterSend {
                    hostname: hostname.to_string(),
                    port,
                    params,
                });
            Ok(())
        }));

        let filter_id = 2;
        let slot_id = SlotId::new(8);
        let query_id = QueryId { hi: 223, lo: 456 };
        let lifecycle_guard = LifecycleQueryGuard::new(query_id);
        let hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        hub.register_filter_specs(
            100,
            &[JoinRuntimeFilterSpec {
                filter_id,
                expr_order: 0,
                probe_slot_id: slot_id,
                build_data_type: DataType::Int32,
                merge_nodes: Vec::new(),
                has_remote_targets: true,
            }],
        );

        let params = RuntimeFilterWorkerParams::new(
            HashMap::from([(
                filter_id,
                vec![RuntimeFilterProberTarget::new("127.0.0.1", 18031)],
            )]),
            HashMap::from([(filter_id, 2)]),
            Some(1),
        );
        let worker = RuntimeFilterWorker::new(query_id, params, hub);
        let first = encoded_empty_membership_partial(filter_id, slot_id);
        let second = encoded_empty_membership_partial(filter_id, slot_id);

        worker
            .receive_partial(filter_id, &first, 20, Some(DataType::Int32))
            .expect("first partial");
        worker
            .receive_partial(filter_id, &second, 21, Some(DataType::Int32))
            .expect("second partial");

        let snapshot = RuntimeFilterLifecycleRegistry::global()
            .snapshot(lifecycle_guard.query_key())
            .expect("query snapshot");
        let filter = snapshot.filters.get(&filter_id).expect("filter lifecycle");
        assert_eq!(filter.merged_received(), 2);
        assert_eq!(filter.merged_expected(), 2);
        assert!(filter.has_drop_reason(RfDropReason::SizeExceeded));

        let sends = sends.lock().expect("captured runtime filter sends lock");
        assert_eq!(sends.len(), 1);
        assert!(!sends[0].params.is_partial);
        assert_eq!(sends[0].params.filter_id, filter_id);
        assert!(!sends[0].params.data.is_empty());
        assert_eq!(sends[0].params.build_be_number, 0);
        assert!(sends[0].params.column_type.is_none());
    }

    #[test]
    fn send_failure_records_dropped() {
        let _hook_guard =
            install_final_send_hook(Box::new(
                |_hostname, _port, _params| Err("boom".to_string()),
            ));

        let filter_id = 3;
        let slot_id = SlotId::new(9);
        let query_id = QueryId { hi: 323, lo: 456 };
        let lifecycle_guard = LifecycleQueryGuard::new(query_id);
        let hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        hub.register_filter_specs(
            100,
            &[JoinRuntimeFilterSpec {
                filter_id,
                expr_order: 0,
                probe_slot_id: slot_id,
                build_data_type: DataType::Int32,
                merge_nodes: Vec::new(),
                has_remote_targets: true,
            }],
        );

        let params = RuntimeFilterWorkerParams::new(
            HashMap::from([(
                filter_id,
                vec![RuntimeFilterProberTarget::new("127.0.0.1", 18032)],
            )]),
            HashMap::from([(filter_id, 1)]),
            None,
        );
        let worker = RuntimeFilterWorker::new(query_id, params, hub);
        let partial = encoded_empty_membership_partial(filter_id, slot_id);

        worker
            .receive_partial(filter_id, &partial, 30, Some(DataType::Int32))
            .expect("partial");

        let snapshot = RuntimeFilterLifecycleRegistry::global()
            .snapshot(lifecycle_guard.query_key())
            .expect("query snapshot");
        let filter = snapshot.filters.get(&filter_id).expect("filter lifecycle");
        assert_eq!(filter.merged_received(), 1);
        assert_eq!(filter.merged_expected(), 1);
        assert!(filter.has_drop_reason(RfDropReason::SendFailed));
    }

    #[test]
    fn size_capped_merge_and_send_failure_preserve_both_dropped_reasons() {
        let _hook_guard =
            install_final_send_hook(Box::new(
                |_hostname, _port, _params| Err("boom".to_string()),
            ));

        let filter_id = 4;
        let slot_id = SlotId::new(10);
        let query_id = QueryId { hi: 423, lo: 456 };
        let lifecycle_guard = LifecycleQueryGuard::new(query_id);
        let hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        hub.register_filter_specs(
            100,
            &[JoinRuntimeFilterSpec {
                filter_id,
                expr_order: 0,
                probe_slot_id: slot_id,
                build_data_type: DataType::Int32,
                merge_nodes: Vec::new(),
                has_remote_targets: true,
            }],
        );

        let params = RuntimeFilterWorkerParams::new(
            HashMap::from([(
                filter_id,
                vec![RuntimeFilterProberTarget::new("127.0.0.1", 18033)],
            )]),
            HashMap::from([(filter_id, 2)]),
            Some(1),
        );
        let worker = RuntimeFilterWorker::new(query_id, params, hub);
        let first = encoded_empty_membership_partial(filter_id, slot_id);
        let second = encoded_empty_membership_partial(filter_id, slot_id);

        worker
            .receive_partial(filter_id, &first, 40, Some(DataType::Int32))
            .expect("first partial");
        worker
            .receive_partial(filter_id, &second, 41, Some(DataType::Int32))
            .expect("second partial");

        let snapshot = RuntimeFilterLifecycleRegistry::global()
            .snapshot(lifecycle_guard.query_key())
            .expect("query snapshot");
        let filter = snapshot.filters.get(&filter_id).expect("filter lifecycle");
        assert_eq!(
            filter.drop_reasons(),
            &[RfDropReason::SizeExceeded, RfDropReason::SendFailed]
        );
        assert!(filter.has_drop_reason(RfDropReason::SizeExceeded));
        assert!(filter.has_drop_reason(RfDropReason::SendFailed));
    }

    fn encoded_empty_membership_partial(filter_id: i32, slot_id: SlotId) -> Vec<u8> {
        let min_max =
            RuntimeMinMaxFilter::full_range(RuntimeFilterType::Int32).expect("min/max range");
        let filter = RuntimeEmptyFilter::new(
            filter_id,
            slot_id,
            RuntimeFilterType::Int32,
            false,
            RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            0,
            min_max,
        );
        encode_starrocks_empty_filter(&filter).expect("encode empty filter")
    }
}
