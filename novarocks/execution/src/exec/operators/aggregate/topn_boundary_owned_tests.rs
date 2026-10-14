// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Owner-local ranking/lifetime protection. The independent oracle sorts ALL
//! declared new-group projections with std byte/NULL order; it does not call
//! the boundary/comparator under test. Group ids are explicit input facts here,
//! not a claim of actual KeyTable or SQL producer installation. Tests own
//! isolated trackers/controls and join the only thread before returning.

use super::*;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime_filter::contribution::{
    OrderedScalarRef, OrderedTupleRef, RuntimeOrderKey, RuntimeOrderNullOrder,
    RuntimeOrderSortDirection,
};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

struct Pass;
impl KernelEvaluationControl for Pass {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("boundary never waits")
    }
}
fn key_value(key: &OwnedUtf8OrderKey) -> Option<&str> {
    match key.borrowed() {
        OrderedTupleRef::Single(None) => None,
        OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(s))) => Some(s),
        _ => panic!("owned UTF8 key lost its exact type"),
    }
}
fn contract(
    direction: RuntimeOrderSortDirection,
    nulls: RuntimeOrderNullOrder,
) -> Arc<RuntimeOrderContract> {
    Arc::new(RuntimeOrderContract::from_frozen(
        [RuntimeOrderKey::with_order(
            DataType::Utf8,
            direction,
            nulls,
        )],
        [2; 32],
        [3; 32],
    ))
}
fn oracle_cmp(
    a: Option<&str>,
    b: Option<&str>,
    direction: RuntimeOrderSortDirection,
    nulls: RuntimeOrderNullOrder,
) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls == RuntimeOrderNullOrder::First {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls == RuntimeOrderNullOrder::First {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(a), Some(b)) => {
            let result = a.as_bytes().cmp(b.as_bytes());
            if direction == RuntimeOrderSortDirection::Ascending {
                result
            } else {
                result.reverse()
            }
        }
    }
}

#[test]
fn ordered_topn_owned_boundary_matches_independent_full_sort_and_legacy_projection() {
    let input = [
        Some("z"),
        None,
        Some(""),
        Some("\u{6d77}\u{6e2f}"),
        Some("a\0b"),
        Some("a"),
        Some("a"),
        Some("b"),
        Some("\u{00e9}"),
        Some("\0"),
    ];
    for direction in [
        RuntimeOrderSortDirection::Ascending,
        RuntimeOrderSortDirection::Descending,
    ] {
        for nulls in [RuntimeOrderNullOrder::First, RuntimeOrderNullOrder::Last] {
            let tracker = MemTracker::new_root("ordered-owned-ranking-oracle");
            let host = OrderedTopNAllocationHost::new(tracker.clone());
            let contract = contract(direction, nulls);
            let mut state = OwnedAggregateTopNBoundaryState::try_new_owned(
                NonZeroU32::new(3).unwrap(),
                contract.clone(),
                &host,
            )
            .unwrap();
            let mut legacy =
                AggregateTopNBoundaryState::try_new(NonZeroU32::new(3).unwrap(), contract.clone())
                    .unwrap();
            let mut all = Vec::new();
            let mut previous: Option<Option<&str>> = None;
            for (id, value) in input.into_iter().enumerate() {
                state
                    .observe_owned_group(id, host.copy_utf8(value, &Pass).unwrap(), &Pass)
                    .unwrap();
                legacy
                    .observe_new_group(
                        id,
                        OrderedTuple::new([value.map(|s| OrderedScalar::Utf8(s.into()))]),
                    )
                    .unwrap();
                all.push(value);
                all.sort_by(|a, b| oracle_cmp(*a, *b, direction, nulls));
                let snapshot = state.snapshot_owned(&Pass).unwrap();
                if all.len() < 3 {
                    assert!(snapshot.is_none());
                    assert_eq!(state.take_pending_controlled(&Pass).unwrap(), None);
                    assert_eq!(legacy.take_pending_tightening().unwrap(), None);
                    continue;
                }
                let expected = all[2];
                assert_eq!(key_value(snapshot.as_ref().unwrap()), expected);
                let pending = state.take_pending_controlled(&Pass).unwrap();
                let expected_emit = previous.is_none_or(|old| {
                    oracle_cmp(expected, old, direction, nulls) == Ordering::Less
                });
                assert_eq!(pending.is_some(), expected_emit);
                let legacy_pending = legacy.take_pending_tightening().unwrap();
                assert_eq!(pending.is_some(), legacy_pending.is_some());
                if let Some(pending) = pending {
                    assert_eq!(key_value(&pending), expected);
                    previous = Some(expected);
                }
            }
            assert!(state.finish_controlled(&Pass).unwrap().is_none());
            assert!(state.finish_controlled(&Pass).unwrap().is_none());
            drop(state);
            drop(host);
            assert_eq!(tracker.current(), 0);
        }
    }
}

#[test]
fn ordered_topn_owned_pending_key_outlives_eviction_and_state_on_another_thread() {
    let tracker = MemTracker::new_root("ordered-pending-key-eviction");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let mut state = OwnedAggregateTopNBoundaryState::try_new_owned(
        NonZeroU32::new(2).unwrap(),
        contract(
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        ),
        &host,
    )
    .unwrap();
    for (id, value) in ["z", "b"].into_iter().enumerate() {
        state
            .observe_owned_group(id, host.copy_utf8(Some(value), &Pass).unwrap(), &Pass)
            .unwrap();
    }
    let pending = state.take_pending_controlled(&Pass).unwrap().unwrap();
    assert_eq!(key_value(&pending), Some("z"));
    state
        .observe_owned_group(2, host.copy_utf8(Some("a"), &Pass).unwrap(), &Pass)
        .unwrap();
    assert_eq!(
        key_value(&state.snapshot_owned(&Pass).unwrap().unwrap()),
        Some("b")
    );
    drop(state);
    drop(host);
    assert!(
        tracker.current() > 0,
        "evicted pending key still owns its real backing"
    );
    std::thread::spawn(move || {
        assert_eq!(key_value(&pending), Some("z"));
        drop(pending);
    })
    .join()
    .unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_topn_owned_candidate_slot_refusal_keeps_new_group_uncommitted() {
    let tracker = MemTracker::new_root("ordered-candidate-layout-refusal");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    // Prepare an immutable NULL value without retained payload/metadata, then
    // freeze the real limit before the first candidate container request.
    let null = host.copy_utf8(None, &Pass).unwrap();
    assert_eq!(tracker.current(), 0);
    tracker.install_limit_once(1).unwrap();
    let mut state = OwnedAggregateTopNBoundaryState::try_new_owned(
        NonZeroU32::new(1).unwrap(),
        contract(
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        ),
        &host,
    )
    .unwrap();
    assert_eq!(
        state.observe_owned_group(0, null, &Pass),
        Err(OwnedTopNBoundaryError::Kernel(
            KernelFailure::ResourceExhausted
        ))
    );
    assert_eq!(state.observed_group_count, 0);
    assert!(state.snapshot_owned(&Pass).unwrap().is_none());
    drop(state);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_topn_owned_long_comparison_preserves_control_and_original_sequence_errors() {
    struct Refuse {
        units: AtomicUsize,
        cause: KernelFailure,
    }
    impl KernelEvaluationControl for Refuse {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            if self.units.fetch_add(units as usize, AtomicOrdering::SeqCst) + units as usize >= 512
            {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("boundary never waits")
        }
    }
    let tracker = MemTracker::new_root("ordered-boundary-control-cause");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let prefix = "x".repeat(1 << 16);
    let values = [prefix.clone() + "a", prefix + "b"];
    let mut state = OwnedAggregateTopNBoundaryState::try_new_owned(
        NonZeroU32::new(2).unwrap(),
        contract(
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        ),
        &host,
    )
    .unwrap();
    for (id, value) in values.iter().enumerate() {
        state
            .observe_owned_group(id, host.copy_utf8(Some(value), &Pass).unwrap(), &Pass)
            .unwrap();
    }
    let held = tracker.current();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let control = Refuse {
            units: AtomicUsize::new(0),
            cause: cause.clone(),
        };
        assert_eq!(
            state.snapshot_owned(&control),
            Err(OwnedTopNBoundaryError::Kernel(cause))
        );
        // Metadata work is observed too. Refusal may cross the threshold by
        // one public quantum, but must never consume the whole long value.
        assert!(
            (512..512 + novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize)
                .contains(&control.units.load(AtomicOrdering::SeqCst))
        );
        assert_eq!(tracker.current(), held);
    }
    assert_eq!(
        state.observe_owned_group(0, host.copy_utf8(None, &Pass).unwrap(), &Pass),
        Err(OwnedTopNBoundaryError::Boundary(
            AggregateTopNBoundaryError::GroupIdentityOutOfSequence {
                expected: 2,
                actual: 0
            }
        ))
    );
    assert_eq!(state.observed_group_count, 2);
    drop(state);
    drop(host);
    assert_eq!(tracker.current(), 0);
}
