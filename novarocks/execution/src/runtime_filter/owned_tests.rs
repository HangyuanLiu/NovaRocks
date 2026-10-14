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

//! Short owner-local hard-limit/last-alias tests, not performance acceptance.
//! Oracles are independent bytes/NULL values, the original canonical decoder
//! and actual isolated production MemTracker balances after real Drop. Public
//! control verdicts exercise unpublished-prefix rollback without a product hook.

use super::*;
use crate::runtime_filter::contribution::{
    ContributionCodecError, ContributionCodecExpectation, OrderedBoundUpdate, OrderedScalar,
    OrderedTuple, RuntimeFilterContribution as CodecContribution, RuntimeOrderKey,
    decode_contribution, encode_contribution,
};
use arrow::datatypes::DataType;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct Control {
    calls: AtomicUsize,
    units: AtomicUsize,
    max_units: AtomicUsize,
    refuse_at: Option<usize>,
    cause: KernelFailure,
}
impl Control {
    fn pass() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            units: AtomicUsize::new(0),
            max_units: AtomicUsize::new(0),
            refuse_at: None,
            cause: KernelFailure::Cancelled,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        self.units.fetch_add(units as usize, Ordering::SeqCst);
        self.max_units.fetch_max(units as usize, Ordering::SeqCst);
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.refuse_at == Some(ordinal) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("ordered storage never waits")
    }
}

#[test]
fn ordered_topn_owned_utf8_preserves_null_empty_and_last_shared_key() {
    let tracker = MemTracker::new_root("ordered-key-last-alias");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let control = Control::pass();
    let null = host.copy_utf8(None, &control).unwrap();
    assert!(matches!(null.borrowed(), OrderedTupleRef::Single(None)));
    assert_eq!(tracker.current(), 0);
    let empty = host.copy_utf8(Some(""), &control).unwrap();
    assert!(matches!(
        empty.borrowed(),
        OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8("")))
    ));
    assert!(
        tracker.current() > 0,
        "empty payload still has paid typed metadata"
    );
    drop(empty);
    assert_eq!(tracker.current(), 0);
    let text = "\u{6d77}\0\u{6e2f}".repeat(1024);
    let key = host.copy_utf8(Some(&text), &control).unwrap();
    let held = tracker.current();
    assert!(held >= text.len() as i64);
    let last = key.clone();
    assert_eq!(
        tracker.current(),
        held,
        "whole sharing requests no payload or holder"
    );
    drop(key);
    drop(host);
    assert_eq!(tracker.current(), held);
    std::thread::spawn(move || {
        let OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(value))) = last.borrowed() else {
            panic!("UTF8 key lost")
        };
        assert_eq!(value, text);
        drop(last);
    })
    .join()
    .unwrap();
    assert_eq!(tracker.current(), 0);
    assert!(control.max_units.load(Ordering::SeqCst) <= MAX_UNOBSERVED_KERNEL_WORK as usize);
}

#[test]
fn ordered_topn_owned_canonical_bytes_use_original_codec_and_whole_aliases() {
    let tracker = MemTracker::new_root("ordered-canonical-last-alias");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [9; 32]);
    for value in [None, Some(""), Some("\0"), Some("\u{6d77}\u{6e2f}")] {
        let tuple = OrderedTuple::new([value.map(|s| OrderedScalar::Utf8(s.into()))]);
        let expected =
            CodecContribution::ordered_bound(OrderedBoundUpdate::new(contract.digest(), tuple));
        let original = encode_contribution(
            &expected,
            ContributionCodecExpectation::OrderedBound(&contract),
            1024,
        )
        .unwrap();
        let bytes = host
            .encode_ordered_bound(
                OrderedTupleRef::Single(value.map(OrderedScalarRef::Utf8)),
                &contract,
                1024,
                &Control::pass(),
            )
            .unwrap();
        assert_eq!(bytes.as_slice(), original.payload());
        assert_eq!(
            decode_contribution(
                &bytes,
                &contract.digest(),
                ContributionCodecExpectation::OrderedBound(&contract),
                1024
            )
            .unwrap(),
            expected
        );
        let held = tracker.current();
        let ingress = bytes.clone_for_local_ingress();
        assert_eq!(bytes.as_ptr(), ingress.as_ptr());
        assert_eq!(tracker.current(), held);
        drop(bytes);
        assert_eq!(tracker.current(), held);
        std::thread::spawn(move || drop(ingress)).join().unwrap();
        assert_eq!(tracker.current(), 0);
    }
    let legacy = CanonicalContributionBytes::from(vec![1, 2, 3]);
    let ingress = legacy.clone_for_local_ingress();
    assert_eq!(legacy, ingress);
    assert_ne!(
        legacy.as_ptr(),
        ingress.as_ptr(),
        "legacy ingress retains its original copy"
    );
}

#[test]
fn ordered_topn_owned_real_limits_refuse_prefix_vector_and_late_holder_without_stock() {
    let text = "x".repeat(4096);
    let prefix = source_custody_prefix_facts().requests().bytes();
    for limit in [1, prefix, prefix + text.len()] {
        let tracker = MemTracker::new_root("ordered-storage-hard-refusal");
        tracker.install_limit_once(limit as i64).unwrap();
        let host = OrderedTopNAllocationHost::new(tracker.clone());
        let control = Control::pass();
        assert!(matches!(
            host.copy_utf8(Some(&text), &control),
            Err(KernelFailure::ResourceExhausted)
        ));
        assert_eq!(tracker.current(), 0);
        if limit == 1 {
            assert_eq!(control.calls.load(Ordering::SeqCst), 0);
        }
        if limit == prefix + text.len() {
            assert!(
                control.units.load(Ordering::SeqCst) >= text.len(),
                "holder refusal follows the actual payload copy"
            );
        }
    }
}

#[test]
fn ordered_topn_owned_encoder_and_null_refuse_prefix_before_the_first_callback() {
    let tracker = MemTracker::new_root("ordered-prefix-first-callback");
    tracker.install_limit_once(1).unwrap();
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [5; 32]);
    let control = Control::pass();
    assert!(matches!(
        host.encode_ordered_bound(
            OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8("value"))),
            &contract,
            1024,
            &control
        ),
        Err(ControlledContributionCodecError::Kernel(
            KernelFailure::ResourceExhausted
        ))
    ));
    assert_eq!(control.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tracker.current(), 0);
    assert!(matches!(
        host.copy_utf8(None, &control),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(control.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_topn_owned_control_refusal_frees_unpublished_key_and_canonical_prefix() {
    let text = "x".repeat(4096);
    let tracker = MemTracker::new_root("ordered-storage-control-refusal");
    let host = OrderedTopNAllocationHost::new(tracker.clone());
    let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [7; 32]);
    let tuple = OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(&text)));
    let successful = Control::pass();
    drop(
        host.encode_ordered_bound(tuple, &contract, usize::MAX, &successful)
            .unwrap(),
    );
    let last = successful.calls.load(Ordering::SeqCst) - 1;
    assert_eq!(tracker.current(), 0);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let control = Control {
            refuse_at: Some(last),
            cause: cause.clone(),
            ..Control::pass()
        };
        assert!(
            matches!(host.encode_ordered_bound(tuple, &contract, usize::MAX, &control), Err(ControlledContributionCodecError::Kernel(e)) if e == cause)
        );
        assert_eq!(
            tracker.current(),
            0,
            "late refusal destroys the complete unpublished owner"
        );
        let control = Control {
            refuse_at: Some(4),
            cause: cause.clone(),
            ..Control::pass()
        };
        assert!(matches!(host.copy_utf8(Some(&text), &control), Err(e) if e == cause));
        assert_eq!(tracker.current(), 0);
    }
    assert!(matches!(
        host.encode_ordered_bound(tuple, &contract, 1, &Control::pass()),
        Err(ControlledContributionCodecError::Codec(
            ContributionCodecError::EncodedSizeExceeded
        ))
    ));
    assert_eq!(tracker.current(), 0);
}
