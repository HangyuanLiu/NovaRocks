// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this
// file except in compliance with the License. You may obtain a copy at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct lifecycle-kernel phases and actual original carrier retirement.
//! These tests deliberately bind no socket and claim no wire/IO acceptance.

use super::*;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use std::num::NonZeroUsize;

fn fixture() -> (
    NativeTransportCapacityFactory,
    Arc<ResultRetainedBudget>,
    usize,
) {
    let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let factory = NativeTransportCapacityFactory::try_new(budget.clone()).unwrap();
    (factory, budget, bytes)
}

fn key(method: NativeRpcMethod) -> InlineNativeChannelIdentity {
    InlineNativeChannelIdentity::from_parts(
        Some(BackendProcessId::new_v7()),
        &NativeEndpoint::from_host_port("key-owner.example.com", 9000).unwrap(),
        method,
    )
    .unwrap()
}

fn initial(config: &Http2ConnectionConfig) -> h2::BoundConnectionLifecycle {
    let lease = config
        .connection_lifecycle
        .as_ref()
        .unwrap()
        .bind()
        .unwrap();
    lease.on_initial_settings_complete().unwrap();
    lease
}

fn installed(config: &Http2ConnectionConfig) -> h2::BoundConnectionLifecycle {
    let lease = initial(config);
    config
        .connection_lifecycle
        .as_ref()
        .unwrap()
        .on_acquisition_complete()
        .unwrap();
    lease
}

fn blocked(factory: &NativeTransportCapacityFactory, key: InlineNativeChannelIdentity) {
    assert_eq!(
        factory
            .try_config_for_key(TransportClass::Data, key)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
}

fn returned(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("the original carrier and key kernel must physically exit");
    };
    drop(credit);
}

#[test]
fn unbound_key_position_is_held_until_last_actual_original_pool_alias() {
    let (factory, budget, bytes) = fixture();
    let key = key(NativeRpcMethod::ExchangeUnary);
    let config = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    let alias = config.receive_frame_buffer.as_ref().unwrap().clone();
    drop(config); // Observer retires Connecting into the one Closing position.
    let second = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    let second_alias = second.receive_frame_buffer.as_ref().unwrap().clone();
    drop(second); // Closing is full: RetiringConnecting keeps Connecting=1.
    blocked(&factory, key);
    drop(alias); // Closing is now free; the other physical charge still exists.
    blocked(&factory, key);
    assert_eq!(factory.available_positions(TransportClass::Data), 517);
    drop(second_alias);
    assert_eq!(factory.available_positions(TransportClass::Data), 518);
    let replacement = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    drop(replacement);
    drop(factory);
    returned(&budget, bytes);
}

#[test]
fn full_closing_preserves_live_and_connecting_original_charges_until_real_alias_exit() {
    let (factory, budget, bytes) = fixture();
    let key = key(NativeRpcMethod::TransmitRuntimeFilterEnvelope);
    let first = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    let first_lease = installed(&first);
    let first_alias = first.receive_frame_buffer.as_ref().unwrap().clone();
    drop(first_lease);
    drop(first); // Closing=1 remains through the first original alias.
    let second = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    let second_lease = installed(&second);
    let second_alias = second.receive_frame_buffer.as_ref().unwrap().clone();
    drop(second_lease);
    drop(second); // RetiringLive continues to occupy Live=1.
    let third = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    let third_lease = initial(&third);
    let third_alias = third.receive_frame_buffer.as_ref().unwrap().clone();
    assert_eq!(
        third
            .connection_lifecycle
            .as_ref()
            .unwrap()
            .on_acquisition_complete()
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(third_lease);
    drop(third); // Failed install cannot counterfeit either free position.
    blocked(&factory, key);
    drop(first_alias);
    blocked(&factory, key);
    drop(third_alias);
    let fourth = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    let fourth_lease = initial(&fourth);
    assert_eq!(
        fourth
            .connection_lifecycle
            .as_ref()
            .unwrap()
            .on_acquisition_complete()
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(fourth_lease);
    drop(fourth);
    drop(second_alias);
    let replacement = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    let replacement_lease = installed(&replacement);
    drop(replacement_lease);
    drop(replacement);
    assert_eq!(factory.available_positions(TransportClass::Data), 518);
    drop(factory);
    returned(&budget, bytes);
}

#[test]
fn stock_refusal_rolls_back_unpublished_key_claim_before_next_attempt() {
    let (factory, budget, bytes) = fixture();
    let key = key(NativeRpcMethod::ExchangeUnary);
    let slots: Vec<_> = (0..factory.positions(TransportClass::Data))
        .map(|_| factory.claim(TransportClass::Data, None).unwrap())
        .collect();
    blocked(&factory, key);
    assert_eq!(factory.available_acquisitions(TransportClass::Data), 32);
    drop(slots);
    let replacement = factory
        .try_config_for_key(TransportClass::Data, key)
        .unwrap();
    let lease = installed(&replacement);
    drop(lease);
    drop(replacement);
    drop(factory);
    returned(&budget, bytes);
}
