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

//! Real original fixed ledger/proof; no physical worker/profile qualification.
use super::*;
use crate::{ControlStorageConfig, WorkloadConfig};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let count = 8;
    let storage = ControlStorageConfig::new([count, 0, 0, 0, 0], 2, 2, &[]);
    let mut config = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: 4,
        max_registered_dependencies: (4 * 16 + 2 * 8 * 2 * 4 + 2 * 4 + 1) * 4,
        max_claims: 16,
        max_requests: 16,
        max_changes_per_request: 4,
        max_snapshot_bytes: 16 * 1024 * 1024,
        max_scratch_bytes: 16 * 1024 * 1024,
        max_residual_arcs: 10000,
        max_total_proof_work: 10000000,
        work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
    };
    let base = storage.base_charge(&config).unwrap();
    let m = base.get(JointResource::MetadataBytes);
    let custody = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ScopeRecords, 8)
            .with(JointResource::ClaimRecords, 12)
            .with(JointResource::MetadataBytes, m * 16)
            .with(JointResource::PayloadBytes, 64)
            .with(JointResource::UnreceivedProductions, 1),
        per_dependency: JointResources::ZERO
            .with(JointResource::ScopeRecords, 7)
            .with(JointResource::ClaimRecords, 12)
            .with(JointResource::MetadataBytes, m * 8)
            .with(JointResource::PayloadBytes, 64)
            .with(JointResource::UnreceivedProductions, 1),
        protected_progress: base,
    };
    let foreground = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ClaimRecords, 4)
            .with(JointResource::MetadataBytes, m * 16)
            .with(JointResource::BlockingJobs, 4)
            .with(JointResource::PhysicalQueueRecords, 4)
            .with(JointResource::PhysicalQueueBytes, 4096),
        per_dependency: JointResources::ZERO
            .with(JointResource::ClaimRecords, 4)
            .with(JointResource::MetadataBytes, m * 8)
            .with(JointResource::BlockingJobs, 4)
            .with(JointResource::PhysicalQueueRecords, 4)
            .with(JointResource::PhysicalQueueBytes, 4096),
        protected_progress: JointResources::ZERO,
    };
    let settlement = foreground;
    let global = JointResourceLimits {
        global: custody
            .global
            .checked_add(foreground.global)
            .unwrap()
            .checked_add(settlement.global)
            .unwrap(),
        per_dependency: custody
            .per_dependency
            .checked_add(foreground.per_dependency)
            .unwrap()
            .checked_add(settlement.per_dependency)
            .unwrap(),
        protected_progress: base,
    };
    config.global = global;
    config.classes[0] = global;
    config.lanes[0] = custody;
    config.lanes[1] = foreground;
    config.class_lanes[0][0] = custody;
    config.class_lanes[0][1] = foreground;
    config.lanes[2] = settlement;
    config.class_lanes[0][2] = settlement;
    // Only the original fixed substrate requires progress in this fixture.
    // CPU/complete Native action protection is deliberately not asserted.
    let mut required = RequiredJointProgress {
        global: base,
        classes: [JointResources::ZERO; 5],
        lanes: [JointResources::ZERO; 3],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    required.classes[0] = base;
    required.lanes[0] = base;
    required.class_lanes[0][0] = base;
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (handle, driver) = control
        .configure_joint_capacity(config.validate(&required).unwrap())
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
}
fn drive(driver: &mut JointProgressDriver) {
    crate::joint_admission::tests::drive(driver);
}
fn receive(admission: &mut JointAdmission) -> JointGrantedBatch {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(batch)) => batch,
        _ => panic!("expected original paid reply"),
    }
}
fn fact(name: &str) -> SharedDependency {
    SharedDependency::Network(
        novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
            &url::Url::parse(&format!("https://{name}.example")).unwrap(),
        )
        .unwrap(),
    )
}
fn root(control: &WorkloadControl) -> crate::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}
fn bind(scope: &WorkScope, facts: &[SharedDependency], driver: &mut JointProgressDriver) {
    let mut proposal = scope.begin_control_binding().unwrap();
    for fact in facts {
        proposal.push_dependency(fact).unwrap();
    }
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(driver);
    let mut reply = receive(&mut pending);
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(scope)
        .unwrap()
        .notify();
    drop(reply);
    drive(driver);
}

#[test]
fn original_owner_source_preserves_full_d_and_rejects_stale_or_cancelled_business() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let a = fact("actual-catalog");
    let b = fact("actual-storage");
    bind(&scope, &[a], &mut driver);
    let stale = work
        .owner
        .blocking_action_source(JointLane::Foreground)
        .unwrap();
    bind(&scope, &[b], &mut driver);
    assert!(matches!(
        stale.begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap()),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));

    let mut action = work
        .owner
        .blocking_action_source(JointLane::Foreground)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    drive(&mut driver);
    {
        let state = handle.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        let record = &ledger.requests[&action.admission.id.unwrap()];
        assert_eq!(record.owner, scope.id);
        let [change] = &*record.changes else {
            panic!("one original source action")
        };
        assert_eq!(change.claim.facts.dependencies.len(), 2);
        assert!(change.claim.facts.key.causal.is_empty());
        assert_eq!(change.claim.facts.class, WorkClass::Query);
        assert_eq!(change.claim.facts.lane, JointLane::Foreground);
        assert_eq!(change.claim.amounts.get(JointResource::BlockingJobs), 1);
        assert_eq!(
            change
                .claim
                .amounts
                .get(JointResource::PhysicalQueueRecords),
            1
        );
        assert_eq!(change.claim.amounts.get(JointResource::Computations), 0);
        assert_eq!(change.claim.amounts.get(JointResource::InternalCpuJobs), 0);
    }
    {
        let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("expected the original foreground offer"),
        };
        // No physical call has been attempted. Explicitly report that fact
        // before withdrawing the same ticket; Drop alone proves no such exit.
        let mut physical = None::<std::convert::Infallible>;
        offer.reobserve_after_drop(&mut physical).unwrap().notify();
    }
    drop(action);
    drive(&mut driver);
    let cancelled = work
        .owner
        .blocking_action_source(JointLane::Foreground)
        .unwrap();
    let settlement = work
        .owner
        .blocking_action_source(JointLane::Settlement)
        .unwrap();
    work.owner.cancel(crate::CancellationReason::ServerShutdown);
    assert!(
        cancelled
            .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
            .is_err()
    );
    let mut settlement = settlement
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    drive(&mut driver);
    {
        let mut offer = match settlement.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("expected the original settlement offer"),
        };
        let mut physical = None::<std::convert::Infallible>;
        offer.reobserve_after_drop(&mut physical).unwrap().notify();
    }
    // No physical capability was manufactured or accepted. The explicit
    // no-attempt report permits this original paid ticket to be withdrawn.
    drop(settlement);
    drive(&mut driver);
    work.business.release();
    work.owner.complete_after_terminal_cancel_settled();
    drive(&mut driver);
    assert!(control.snapshot().scopes.is_empty());
}
