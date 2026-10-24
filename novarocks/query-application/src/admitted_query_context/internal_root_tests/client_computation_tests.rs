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

//! Actual P=1 original claims, weak manifests and the original activity cell.
use super::*;

fn assert_finite_stage_refused(
    payload: &RetainedPayloadGrant,
    root: &WorkScope,
    stage: &WorkOwner,
    driver: &mut JointProgressDriver,
    phase: &str,
) {
    let mut batch = payload
        .root_stage_source(root, stage)
        .unwrap()
        .begin(4, JointLane::Foreground)
        .unwrap();
    batch.end_payload().unwrap();
    batch.end_stage().unwrap();
    let mut probe = batch.submit().unwrap();
    drive(driver);
    match Pin::new(&mut probe).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(failure)) => {
            eprintln!("Client finite P=1 probe {phase}: {failure:?}");
            assert_eq!(
                failure.error,
                WorkError::JointCapacity(novarocks_workload_control::joint_admission::JointCapacityFailure {
                    resource: JointResource::Computations,
                    projection: novarocks_workload_control::joint_admission::JointCapacityProjection::Global,
                    constraint: novarocks_workload_control::joint_admission::JointCapacityConstraint::Global,
                }),
                "only the actual saturated resource proof is the expected refusal",
            );
        }
        Poll::Ready(Ok(_)) => panic!("Client finite P=1 probe {phase} acquired P too early"),
        Poll::Pending => panic!("Client finite P=1 probe {phase} did not finish its finite proof"),
    }
    drop(probe);
    drive(driver);
}

#[test]
fn client_seal_with_zero_activity_still_requires_actual_producer_exit_and_cannot_reopen_p() {
    let (control, handle, mut driver) = fixture_with_client(true);
    let root = root(&control);
    let root_scope = root.owner.scope();
    let facts = [SharedDependency::LocalHost];
    bind(&root_scope, &facts, &mut driver);
    let mut raw = Some(claim(
        &handle,
        &root.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&root_scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let payload = payload.unwrap();
    let stage = joint_child(&root_scope, &mut driver);
    let mut batch = payload
        .root_stage_source(&root_scope, &stage)
        .unwrap()
        .begin(4, JointLane::Foreground)
        .unwrap();
    batch.end_payload().unwrap();
    batch.end_stage().unwrap();
    let mut pending = batch.submit().unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(value)) => value,
        _ => panic!("actual first P=1 stage"),
    };
    let mut compute = None;
    ready
        .install_in(&root_scope, &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    drop(ready);
    drop(pending);
    drive(&mut driver);
    let window = root_scope
        .result_capacity()
        .unwrap()
        .try_acquire(
            &root_scope,
            novarocks_workload_control::ResultWindowClass::Client,
        )
        .unwrap();
    let mut binding = None;
    QueryResultCapacityBinding::from_owned_client_computation_in(
        &root_scope,
        &stage,
        &window.retain_alias(),
        &payload,
        &mut compute,
        &mut binding,
    )
    .unwrap();
    assert!(
        compute.is_none(),
        "the original unique owner moves, no extra pin"
    );
    let binding = binding.unwrap();
    let tail = binding.try_clone().unwrap();
    let old = binding.try_window_alias().unwrap();
    let old_manifest = old.internal_manifest_binding().unwrap().unwrap().clone();
    let authority = old.native_action_authority().unwrap();
    let quote = NativeAsyncActionQuote::for_original_native_transport(
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
    );
    let producer = binding.client_computation_producer().unwrap();
    let cut = binding.client_computation_cut_handle().unwrap();
    let activity = binding.begin_result_activity().unwrap();
    let next = joint_child(&root_scope, &mut driver);
    assert_finite_stage_refused(&payload, &root_scope, &next, &mut driver, "producer active");
    cut.seal_producer().unwrap();
    drive(&mut driver);
    assert_finite_stage_refused(
        &payload,
        &root_scope,
        &next,
        &mut driver,
        "sealed producer with live activity and local body",
    );
    assert!(
        binding.begin_result_activity().is_err(),
        "closed producer cannot start more activity"
    );
    drop(activity);
    drive(&mut driver);
    assert_finite_stage_refused(
        &payload,
        &root_scope,
        &next,
        &mut driver,
        "sealed producer with zero activity but live local body",
    );
    producer.native_body_exited().unwrap();
    drive(&mut driver);
    // A completed finite refusal cannot later become a grant. Submit one
    // fresh original request after the actual producer exit returned P.
    let mut batch = payload
        .root_stage_source(&root_scope, &next)
        .unwrap()
        .begin(4, JointLane::Foreground)
        .unwrap();
    batch.end_payload().unwrap();
    batch.end_stage().unwrap();
    let mut second = batch.submit().unwrap();
    drive(&mut driver);
    let mut second_ready = match Pin::new(&mut second).poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(value)) => value,
        Poll::Ready(Err(failure)) => {
            panic!("actual second P=1 stage refused after producer exit: {failure:?}")
        }
        Poll::Pending => panic!("actual second P=1 stage must finish its finite proof"),
    };
    let mut second_compute = None;
    second_ready
        .install_in(&root_scope, &payload, &next, &mut second_compute)
        .unwrap()
        .notify();
    drop(second_ready);
    drop(second);
    drive(&mut driver);
    assert!(old_manifest.try_begin().is_err());
    assert!(authority
        .action_source(JointLane::Foreground, quote)
        .is_err());
    assert!(old
        .native_action_source(JointLane::Foreground, quote)
        .is_err());
    assert!(authority
        .action_source(JointLane::Settlement, quote)
        .is_ok());
    assert!(QueryResultCapacityBinding::try_new(&stage.scope(), old.try_clone().unwrap()).is_err());
    let descendant = joint_child(&stage.scope(), &mut driver);
    let projected = old.for_child(&descendant.scope()).unwrap();
    assert!(projected.internal_manifest_binding().unwrap().is_none());
    assert!(
        projected.native_action_authority().is_err(),
        "erasing manifest cannot erase the compute stamp"
    );
    assert!(QueryResultCapacityBinding::try_new(&descendant.scope(), projected).is_err());
    assert_eq!(
        tail.try_window_alias()
            .unwrap()
            .client_all_objects_bytes()
            .unwrap(),
        8
    );
    assert!(
        binding
            .activity
            .state
            .lock()
            .unwrap()
            .client
            .as_ref()
            .unwrap()
            .retired
    );
    drop((
        second_compute,
        producer,
        old_manifest,
        authority,
        old,
        binding,
        tail,
        window,
        payload,
    ));
    descendant.complete();
    next.complete();
    stage.complete();
    finish(root, &mut driver);
    control.close_admission();
    assert!(control.shutdown().is_ok());
}

#[test]
fn client_custody_uses_the_original_invoiced_activity_layout() {
    let actual =
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            ResultCapacityActivity,
        >())
        .unwrap();
    assert_eq!(
        QueryResultCapacityBinding::fixed_backing_layout().unwrap(),
        actual
    );
    // The reference moved into OriginalNativePreparation is inline; it creates
    // no second activity allocation or holder of a positive computation pin.
    let producer = std::alloc::Layout::new::<std::sync::Arc<ResultCapacityActivity>>()
        .extend(std::alloc::Layout::new::<WorkScope>())
        .unwrap()
        .0
        .pad_to_align();
    assert_eq!(
        std::mem::size_of::<ClientComputationProducer>(),
        producer.size()
    );
    println!(
        "Client activity source bytes={}, binding header={}, producer header={}, cut handle header={}",
        actual.size(),
        std::mem::size_of::<QueryResultCapacityBinding>(),
        std::mem::size_of::<ClientComputationProducer>(),
        std::mem::size_of::<ClientComputationCutHandle>()
    );
}
