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

//! Original computation custody, weak manifests and the actual activity cell.
use super::*;

fn assert_original_compute_held(
    binding: &QueryResultCapacityBinding,
    stage: &WorkOwner,
    activities: usize,
    sealed: bool,
    phase: &str,
) {
    let state = binding.activity.state.lock().unwrap();
    assert_eq!(state.count, activities, "{phase}");
    let client = state.client.as_ref().unwrap();
    assert_eq!(client.sealed, sealed, "{phase}");
    assert!(
        !client.producer_exited && !client.retiring && !client.retired,
        "{phase}"
    );
    assert!(
        client.grant.as_ref().unwrap().is_for_scope(&stage.scope()),
        "{phase}"
    );
}

#[test]
fn client_seal_with_zero_activity_requires_actual_producer_exit_and_cannot_reopen_original_compute()
{
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
    let window = root_scope
        .result_capacity()
        .unwrap()
        .try_acquire(
            &root_scope,
            novarocks_workload_control::ResultWindowClass::Client,
        )
        .unwrap();
    let original_window = window.retain_alias();
    let stage = joint_child(&root_scope, &mut driver);
    let mut batch = payload
        .root_stage_source(&root_scope, &stage)
        .unwrap()
        .for_client_window(&original_window)
        .unwrap()
        .begin(4, JointLane::Foreground)
        .unwrap();
    batch.end_payload().unwrap();
    batch.end_stage().unwrap();
    let mut pending = batch.submit().unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(value)) => value,
        _ => panic!("actual original first stage"),
    };
    let mut compute = None;
    ready
        .install_in(&root_scope, &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    drop(ready);
    drop(pending);
    drive(&mut driver);
    let mut binding = None;
    QueryResultCapacityBinding::from_owned_client_computation_in(
        &root_scope,
        &stage,
        &original_window,
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
    // Both requests borrow the original Client window's one computation
    // position. This same pending request must observe its actual last exit.
    let mut batch = payload
        .root_stage_source(&root_scope, &next)
        .unwrap()
        .for_client_window(&original_window)
        .unwrap()
        .begin(4, JointLane::Foreground)
        .unwrap();
    batch.end_payload().unwrap();
    batch.end_stage().unwrap();
    let mut second = batch.submit().unwrap();
    drive(&mut driver);
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "the same Client window retains its active original computation"
    );
    assert_original_compute_held(&binding, &stage, 1, false, "producer active");
    cut.seal_producer().unwrap();
    drive(&mut driver);
    assert_original_compute_held(
        &binding,
        &stage,
        1,
        true,
        "sealed producer with live activity and local body",
    );
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "producer seal and activity zero do not substitute for actual body exit"
    );
    assert!(
        binding.begin_result_activity().is_err(),
        "closed producer cannot start more activity"
    );
    drop(activity);
    drive(&mut driver);
    assert_original_compute_held(
        &binding,
        &stage,
        0,
        true,
        "sealed producer with zero activity but live local body",
    );
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "producer seal and activity zero do not substitute for actual body exit"
    );
    producer.native_body_exited().unwrap();
    drive(&mut driver);
    {
        let state = binding.activity.state.lock().unwrap();
        let client = state.client.as_ref().unwrap();
        assert!(client.producer_exited && client.retired && client.grant.is_none());
    }
    // The original request now progresses on the same window after its
    // previous computation's true exit; no replacement request is minted.
    let mut second_ready = match Pin::new(&mut second).poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(value)) => value,
        Poll::Ready(Err(failure)) => {
            panic!("actual second stage refused after producer exit: {failure:?}")
        }
        Poll::Pending => panic!("actual second stage must finish its responsibility transaction"),
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
    assert!(
        authority
            .action_source(JointLane::Foreground, quote)
            .is_err()
    );
    assert!(
        old.native_action_source(JointLane::Foreground, quote)
            .is_err()
    );
    assert!(
        authority
            .action_source(JointLane::Settlement, quote)
            .is_ok()
    );
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
        original_window,
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
