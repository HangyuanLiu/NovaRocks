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

use super::*;
use crate::exec::chunk::typed_fixture;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::DataType,
};
use novarocks_type_contract::FunctionValueType;
fn with_step<T>(body: impl FnOnce(&mut StepContext<'_>) -> T) -> T {
    let state = RuntimeState::default();
    let control = state.error_state();
    let mut work =
        crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut ctx = StepContext::new(&control, &mut work);
    body(&mut ctx)
}
fn chunk(values: &[i32]) -> Chunk {
    let owned = values.to_vec();
    let bytes = owned.capacity() * std::mem::size_of::<i32>();
    let mut chunk = typed_fixture::columns(
        &["target"],
        &[SlotId(1)],
        &[FunctionValueType::new(DataType::Int32, false)],
        vec![Arc::new(Int32Array::from(owned)) as ArrayRef],
        values.len(),
    );
    chunk
        .attach_source_backing_receipt(crate::exec::chunk::ChunkSourceBackingReceipt::from_source(
            bytes,
            ValueTypeStorageLifetime::pure(),
        ))
        .unwrap();
    chunk
}
fn fixture(
    producers: usize,
    partitions: usize,
) -> (
    Arc<LocalExchanger>,
    Arc<MemTracker>,
    Arc<dyn InputConstructionFactory>,
) {
    let tracker = MemTracker::new_root("strict local fixture");
    let exchange = LocalExchanger::new_bound(
        partitions,
        producers,
        LocalExchangePartitionSpec::HierarchicalInputSlotIds(vec![SlotId(1)]),
        8 << 20,
        tracker.clone(),
        OperatorStorageLimits {
            bytes: 8 << 20,
            nodes: 256,
            depth: 16,
            columns: 1,
        },
        1 << 20,
        4096,
    )
    .unwrap();
    let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),Arc::new(RuntimeErrorState::default())).unwrap();
    (exchange, tracker, factory)
}
fn admit(
    exchange: &LocalExchanger,
    producer: i32,
    dop: i32,
    chunk: Chunk,
) -> (Chunk, LocalInputPermit) {
    let raw = RawInput::for_test(chunk);
    let LocalAdmission::Granted(permit) = exchange.admit_bounded(producer, dop, &raw).unwrap()
    else {
        panic!("available parent input")
    };
    (raw.original_for_test().clone(), permit)
}
fn publish(
    exchange: &LocalExchanger,
    producer: i32,
    dop: i32,
    input: Chunk,
    factory: &dyn InputConstructionFactory,
) {
    let (chunk, permit) = admit(exchange, producer, dop, input);
    with_step(|ctx| {
        let mut continuation = Some(
            exchange
                .prepare_bounded(chunk, permit, &mut 0, factory, ctx)
                .unwrap(),
        );
        let step = exchange
            .poll_bounded(&mut continuation, factory, ctx)
            .unwrap();
        assert!(matches!(step.next, NextStep::NeedInput));
        assert!(continuation.is_none());
    });
}
fn next(exchange: &LocalExchanger, partition: usize) -> Chunk {
    let SourceRead::Chunk(chunk) = exchange.next_chunk_checked(partition).unwrap() else {
        panic!("ready local row")
    };
    chunk
}
#[test]
fn raw_parent_waits_without_consumption_until_actual_downstream_holder_exits() {
    let (exchange, tracker, factory) = fixture(1, 1);
    publish(&exchange, 0, 1, chunk(&[7]), factory.as_ref());
    let held = next(&exchange, 0);
    let raw = RawInput::for_test(chunk(&[8]));
    let address = Arc::as_ptr(&raw.original_for_test().columns()[0]);
    let LocalAdmission::Pending(wait) = exchange.admit_bounded(0, 1, &raw).unwrap() else {
        panic!("parent holder still owns its position")
    };
    assert_eq!(Arc::as_ptr(&raw.original_for_test().columns()[0]), address);
    drop(held);
    assert!(wait.is_ready());
    assert!(matches!(
        exchange.admit_bounded(0, 1, &raw).unwrap(),
        LocalAdmission::Granted(_)
    ));
    drop(raw);
    exchange.close_consumer(0);
    drop(factory);
    drop(exchange);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn a_blocked_destination_does_not_hide_a_ready_sibling_output() {
    let (exchange, tracker, factory) = fixture(1, 2);
    let bounded = exchange.bounded.as_ref().unwrap();
    let port = bounded.credit.port(0).unwrap();
    let CreditAcquire::Granted(blocker) = port.try_queue(port.max_frame_bytes()).unwrap() else {
        panic!("reserve one destination")
    };
    // Native goldens: target 0 routes to driver0, target4 routes to driver1.
    let (input, permit) = admit(&exchange, 0, 1, chunk(&[0, 4]));
    with_step(|ctx| {
        let mut pending = Some(
            exchange
                .prepare_bounded(input, permit, &mut 0, factory.as_ref(), ctx)
                .unwrap(),
        );
        let waiting = exchange
            .poll_bounded(&mut pending, factory.as_ref(), ctx)
            .unwrap();
        let NextStep::Wait(wait) = waiting.next else {
            panic!("one necessary destination is blocked")
        };
        assert!(matches!(
            exchange.next_chunk_checked(0).unwrap(),
            SourceRead::Pending
        ));
        let sibling = next(&exchange, 1);
        assert_eq!(
            sibling.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            4
        );
        drop(sibling);
        drop(blocker);
        assert!(wait.is_ready());
        assert!(matches!(
            exchange
                .poll_bounded(&mut pending, factory.as_ref(), ctx)
                .unwrap()
                .next,
            NextStep::NeedInput
        ));
        assert_eq!(next(&exchange, 0).len(), 1);
    });
    exchange.close_consumer(0);
    exchange.close_consumer(1);
    drop(port);
    drop(factory);
    drop(exchange);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn exact_endpoint_close_is_idempotent_and_cannot_end_a_sibling() {
    let (exchange, tracker, factory) = fixture(2, 1);
    exchange.finish_producer_at(0).unwrap();
    exchange.finish_producer_at(0).unwrap();
    assert_eq!(exchange.remaining_producers(), 1);
    assert!(exchange.finish_producer_at(2).is_err());
    publish(&exchange, 1, 2, chunk(&[9]), factory.as_ref());
    exchange.finish_producer_at(1).unwrap();
    let held = next(&exchange, 0);
    assert!(matches!(
        exchange.next_chunk_checked(0).unwrap(),
        SourceRead::Finished
    ));
    exchange.close_consumer(0);
    drop(factory);
    drop(exchange);
    assert!(tracker.current() > 0);
    drop(held);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn cancelled_continuation_returns_parent_and_unpublished_scratch() {
    let (exchange, tracker, factory) = fixture(1, 1);
    let bounded = exchange.bounded.as_ref().unwrap();
    let port = bounded.credit.port(0).unwrap();
    let CreditAcquire::Granted(blocker) = port.try_queue(port.max_frame_bytes()).unwrap() else {
        panic!("blocked output")
    };
    let (input, permit) = admit(&exchange, 0, 1, chunk(&[7]));
    with_step(|ctx| {
        let mut pending = Some(
            exchange
                .prepare_bounded(input, permit, &mut 0, factory.as_ref(), ctx)
                .unwrap(),
        );
        assert!(matches!(
            exchange
                .poll_bounded(&mut pending, factory.as_ref(), ctx)
                .unwrap()
                .next,
            NextStep::Wait(_)
        ));
        let raw = RawInput::for_test(chunk(&[8]));
        let LocalAdmission::Pending(wait) = exchange.admit_bounded(0, 1, &raw).unwrap() else {
            panic!("parent occupied")
        };
        pending.take();
        assert!(wait.is_ready());
        assert!(matches!(
            exchange.admit_bounded(0, 1, &raw).unwrap(),
            LocalAdmission::Granted(_)
        ));
    });
    drop(blocker);
    exchange.close_consumer(0);
    drop(port);
    drop(factory);
    drop(exchange);
    assert_eq!(tracker.current(), 0);
}
