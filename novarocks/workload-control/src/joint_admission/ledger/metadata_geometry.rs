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

//! Source-derived requested backing for the fixed joint metadata service.
//! All original fixed claims, including Free claims, keep this one envelope
//! prepaid. Role recycling never refunds the process service's old snapshots.
//! Caller-built vectors, other WC admission/control scratch, real task/Waker
//! graphs and complete roles remain with their separate original authors.

use super::*;
use crate::control_storage::{
    arc_bytes, arc_slice_bytes, array_bytes, btree_peak_bytes, sum_bytes,
};
use std::mem::size_of;

pub(super) fn backing_bytes(
    config: &JointCapacityConfig,
    proposals: usize,
    empty_classes: usize,
) -> Result<usize, WorkError> {
    let n = config.max_claims;
    let k = config.max_dependencies_per_claim;
    let c = config.max_changes_per_request;
    let m = config.max_registered_dependencies;
    let qc = proposals
        .checked_mul(c)
        .ok_or(WorkError::ArithmeticOverflow)?;
    // Charged + active claims, historical map-owned aggregate facts, old driver
    // snapshots, and all original/proposed Q bodies. The transaction may retain
    // 2C historical temporary profile values; one fetched snapshot candidate
    // can precede its capacity rejection. No alias credit is assumed.
    let facts_count = n
        .checked_mul(4)
        .and_then(|n| n.checked_add(qc.checked_mul(2)?))
        .and_then(|n| n.checked_add(c.checked_mul(2)?))
        .and_then(|n| n.checked_add(1))
        .ok_or(WorkError::ArithmeticOverflow)?;
    let facts = sum_bytes([
        arc_bytes::<ProfileFacts>()?,
        arc_bytes::<ProfileKey>()?,
        array_bytes::<u64>(k)?,
        arc_slice_bytes::<Arc<DependencyRecord>>(k)?,
    ])?;
    let empty_facts = sum_bytes([
        arc_bytes::<ProfileFacts>()?,
        arc_bytes::<ProfileKey>()?,
        arc_slice_bytes::<Arc<DependencyRecord>>(0)?,
    ])?
    .checked_mul(empty_classes)
    .ok_or(WorkError::ArithmeticOverflow)?;
    let facts = facts_count
        .checked_mul(facts)
        .and_then(|n| n.checked_add(empty_facts))
        .ok_or(WorkError::ArithmeticOverflow)?;
    // The old cursor can retain a key after its original ProfileFacts exits.
    let cursor = sum_bytes([arc_bytes::<ProfileKey>()?, array_bytes::<u64>(k)?])?;
    let origin = novarocks_resource_contract::NetworkServiceOrigin::MAX_BYTES;
    let row = sum_bytes([
        arc_bytes::<DependencyRecord>()?,
        arc_slice_bytes::<u8>(origin)?,
    ])?;
    let registry = sum_bytes([
        btree_peak_bytes::<SharedDependency, Weak<DependencyRecord>>(m)?,
        m.checked_mul(row).ok_or(WorkError::ArithmeticOverflow)?,
    ])?;
    // Current append/dedup authors can temporarily retain old+grown selected
    // buffers. Do not use the smaller future bounded-builder geometry yet.
    let selected = array_bytes::<Arc<DependencyRecord>>(
        k.checked_mul(6)
            .ok_or(WorkError::ArithmeticOverflow)?
            .max(4),
    )?;
    let origin_scratch = sum_bytes([array_bytes::<u8>(origin)?, arc_slice_bytes::<u8>(origin)?])?;
    let construction = sum_bytes([selected, origin_scratch])?;
    let reply = sum_bytes([array_bytes::<u64>(c)?, array_bytes::<JointClaim>(c)?])?;
    let body = sum_bytes([array_bytes::<ChangeRecord>(c)?, construction.max(reply)])?;
    // Immutable graphs are counted above once, not again for every body pin.
    // Independent finite maxima reserve an envelope, not measured live bytes.
    sum_bytes([
        size_of::<JointLedger>(),
        arc_bytes::<ValidatedJointCapacityConfig>()?,
        facts,
        cursor,
        registry,
        proposals
            .checked_mul(body)
            .ok_or(WorkError::ArithmeticOverflow)?,
        super::super::driver::metadata_backing_bytes(config, proposals)?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkClass, WorkRequest};

    fn drive(driver: &mut JointProgressDriver) {
        for _ in 0..10000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("the original proof driver must reach finite idle");
    }

    fn receive(request: &mut JointAdmission) -> JointClaim {
        let Poll::Ready(Ok(mut batch)) =
            Pin::new(request).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("physical claim reply");
        };
        assert!(batch.adjustment.is_none());
        batch.new_claims.pop().unwrap()
    }

    #[test]
    fn aggregated_profile_keeps_its_original_facts_after_the_creator_role_is_recycled() {
        let (control, handle, mut driver) =
            crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 1);
        let first = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let first_id = first.owner.scope().id();
        let second = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let dependency = handle
            .register_dependency(SharedDependency::LocalHost)
            .unwrap();
        let changes = || {
            vec![JointClaimChange::New {
                dependencies: vec![dependency.clone()],
                lane: JointLane::Custody,
                amounts: JointResources::ZERO.with(JointResource::ClaimRecords, 1),
            }]
        };
        let mut a = handle.request(&first.owner.scope(), changes()).unwrap();
        drive(&mut driver);
        let a = receive(&mut a);
        drive(&mut driver);
        let original = {
            let state = control.inner.state.lock().unwrap();
            Arc::downgrade(&state.joint.as_ref().unwrap().claims[&a.id].facts)
        };
        let mut b = handle.request(&second.owner.scope(), changes()).unwrap();
        drive(&mut driver);
        let b = receive(&mut b);
        drive(&mut driver);
        drop(a);
        first.business.release();
        first.owner.complete();
        drive(&mut driver);
        {
            let state = control.inner.state.lock().unwrap();
            assert!(!state.nodes.contains_key(&first_id));
            let ledger = state.joint.as_ref().unwrap();
            let survivor = &ledger.claims[&b.id].facts;
            let historical = original
                .upgrade()
                .expect("aggregate owns the original graph");
            assert!(!Arc::ptr_eq(&historical, survivor));
            let (key, aggregate) = ledger.profiles.get_key_value(&survivor.key).unwrap();
            assert!(Arc::ptr_eq(&aggregate.facts, &historical));
            assert!(Arc::ptr_eq(key, &aggregate.facts.key));
        }
        drop(b);
        second.business.release();
        second.owner.complete();
        drive(&mut driver);
        assert!(original.upgrade().is_none());
    }
}
