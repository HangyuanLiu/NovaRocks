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
use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_types::SlotId;
fn chunk(value: i32) -> Chunk {
    let field = Field::new("value", DataType::Int32, false);
    let ty = FunctionValueType::try_from_field(&field).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(Int32Array::from(vec![value])) as ArrayRef],
    )
    .unwrap();
    let mut chunk = crate::exec::chunk::typed_fixture::chunk(batch, &[SlotId(1)], &[ty]);
    // Original fixture author moved an exact one-element owned Vec into Arrow.
    chunk
        .attach_source_backing_receipt(crate::exec::chunk::ChunkSourceBackingReceipt::from_source(
            4,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        ))
        .unwrap();
    chunk
}
fn shared(inputs: &[usize]) -> (UnionAllSharedState, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("bounded UnionAll fixture");
    let shared = UnionAllSharedState::new_bound(
        inputs,
        7,
        64 * 1024,
        tracker.clone(),
        OperatorStorageLimits {
            bytes: 64 * 1024,
            nodes: 64,
            depth: 8,
            columns: 1,
        },
        1024,
    )
    .unwrap();
    (shared, tracker)
}
fn enqueue(shared: &UnionAllSharedState, producer: UnionProducer, value: i32) {
    let chunk = chunk(value);
    let size = chunk
        .retained_operator_geometry(shared.input_limits(producer).unwrap())
        .unwrap();
    let CreditAcquire::Granted(permit) = shared.acquire(producer, size).unwrap() else {
        panic!("available queue")
    };
    shared.push_chunk(producer, chunk, permit).unwrap();
}
fn next(shared: &UnionAllSharedState) -> Chunk {
    let SourceRead::Chunk(chunk) = shared.next_chunk().unwrap() else {
        panic!("ready data")
    };
    chunk
}
fn value(chunk: &Chunk) -> i32 {
    chunk.columns()[0]
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0)
}
#[test]
fn round_robin_skips_a_held_input_and_preserves_each_inputs_order() {
    let (shared, tracker) = shared(&[1, 1]);
    let a = shared.producer(0, 0, 1).unwrap();
    let b = shared.producer(1, 0, 1).unwrap();
    enqueue(&shared, a, 10);
    enqueue(&shared, b, 20);
    let held_a = next(&shared);
    assert_eq!(value(&held_a), 10);
    drop(next(&shared));
    enqueue(&shared, a, 11);
    enqueue(&shared, b, 21);
    let progressed = next(&shared);
    assert_eq!(value(&progressed), 21);
    assert!(matches!(shared.next_chunk().unwrap(), SourceRead::Pending));
    drop(progressed);
    drop(held_a);
    assert_eq!(value(&next(&shared)), 11);
    shared.close();
    drop(shared);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn temporary_full_returns_pending_before_input_is_consumed_and_dequeue_wakes() {
    let (shared, tracker) = shared(&[1]);
    let endpoint = shared.producer(0, 0, 1).unwrap();
    enqueue(&shared, endpoint, 1);
    let deferred = chunk(2);
    let original = Arc::as_ptr(&deferred.columns()[0]);
    let bytes = deferred
        .retained_operator_geometry(shared.input_limits(endpoint).unwrap())
        .unwrap();
    let CreditAcquire::Pending(wait) = shared.acquire(endpoint, bytes).unwrap() else {
        panic!("full slot")
    };
    assert_eq!(Arc::as_ptr(&deferred.columns()[0]), original);
    let held = next(&shared);
    assert!(wait.is_ready());
    let CreditAcquire::Granted(permit) = shared.acquire(endpoint, bytes).unwrap() else {
        panic!("dequeue returns Q")
    };
    shared.push_chunk(endpoint, deferred, permit).unwrap();
    assert!(matches!(shared.next_chunk().unwrap(), SourceRead::Pending));
    drop(held);
    assert_eq!(value(&next(&shared)), 2);
    shared.close();
    drop(shared);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn last_data_precedes_eos_and_duplicate_close_cannot_finish_another_endpoint() {
    let (shared, tracker) = shared(&[2]);
    let first = shared.producer(0, 0, 2).unwrap();
    let last = shared.producer(0, 1, 2).unwrap();
    shared.producer_finished(first).unwrap();
    shared.producer_finished(first).unwrap();
    assert!(matches!(shared.next_chunk().unwrap(), SourceRead::Pending));
    enqueue(&shared, last, 77);
    shared.producer_finished(last).unwrap();
    let data = next(&shared);
    assert_eq!(value(&data), 77);
    assert!(matches!(shared.next_chunk().unwrap(), SourceRead::Finished));
    drop(shared);
    assert!(
        tracker.current() > 0,
        "actual downstream holder still retains its finite metadata responsibility"
    );
    drop(data);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn cancel_drops_queued_inputs_wakes_waiters_and_keeps_live_downstream_custody() {
    let (shared, tracker) = shared(&[1]);
    let endpoint = shared.producer(0, 0, 1).unwrap();
    enqueue(&shared, endpoint, 1);
    let held = next(&shared);
    enqueue(&shared, endpoint, 2);
    let CreditAcquire::Pending(wait) = shared.acquire(endpoint, 1).unwrap() else {
        panic!("queue full")
    };
    shared.close();
    assert!(wait.is_ready());
    assert!(matches!(shared.next_chunk().unwrap(), SourceRead::Finished));
    drop(shared);
    assert!(tracker.current() > 0);
    drop(held);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn unknown_endpoint_and_oversized_backing_are_explicit_failures() {
    let (shared, _) = shared(&[1]);
    assert!(shared.producer(0, 1, 1).is_err());
    assert!(shared.producer(0, 0, 2).is_err());
    let endpoint = shared.producer(0, 0, 1).unwrap();
    let error = match shared.acquire(endpoint, usize::MAX) {
        Err(e) => e,
        _ => panic!("oversized backing"),
    };
    assert_eq!(
        error.failure_fact().class(),
        novarocks_failure_contract::FailureClass::Resource
    );
}
#[test]
fn nonzero_source_driver_cannot_drain_and_driver_zero_emits_last_data() {
    use crate::exec::operators::setop::union_all_source::UnionAllSourceFactory;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::exec::pipeline::step::NextStep;
    use crate::runtime::runtime_state::RuntimeState;
    let (shared, _) = shared(&[1]);
    let endpoint = shared.producer(0, 0, 1).unwrap();
    enqueue(&shared, endpoint, 3);
    shared.producer_finished(endpoint).unwrap();
    let factory = UnionAllSourceFactory::new(shared.clone(), 7);
    let state = RuntimeState::default();
    let mut inactive = factory.create(2, 1);
    assert!(matches!(
        inactive.drive_for_test(&state).unwrap().next,
        NextStep::Finished
    ));
    let mut active = factory.create(2, 0);
    let step = active.drive_for_test(&state).unwrap();
    assert_eq!(value(step.output.as_ref().unwrap()), 3);
    drop(step);
    assert!(matches!(
        active.drive_for_test(&state).unwrap().next,
        NextStep::Finished
    ));
}
