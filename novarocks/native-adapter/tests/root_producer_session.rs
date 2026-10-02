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

//! Functional coverage of the real bounded root session and CPU pool. This
//! module does not establish Native host installation or distributed support.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal256Array, DictionaryArray, Int32Array, RecordBatch,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow_buffer::i256;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use novarocks_execution::runtime::fragment::io::{
    ResultAbort, ResultWriteAdmission, RootInputAdmission, RootInputAuthority, RootInputPermit,
    RootProducerState, RootResultSession, RootResultWriteSpec,
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_execution_contract::root_lifetime::RootRetentionClose;
use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultRead};
use novarocks_native_adapter::root_result_session::{NativeRootResultSession, RootProducerPool};
use novarocks_result_contract::{
    ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
    RenderPresentation, RootOutputContract, RootProfileId,
};
use novarocks_types::arrow_metadata_owner::{
    ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
};
use novarocks_types::{
    AttemptId, BackendProcessId, QueryExecutionId, QueryId, SlotId, StageId, TaskId,
};
use novarocks_worker::WorkerResultRetainedLimits;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use novarocks_worker::root_result_channel::{RootResultChannel, RootResultDelivery};

const DEADLINE: Duration = Duration::from_secs(5);

fn task() -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}

fn client(columns: usize) -> FrozenRootOutput {
    let columns = (0..columns)
        .map(|ordinal| RenderColumn {
            // Repeated output occurrences deliberately share one source slot.
            source_ordinal: 0,
            source_slot: Some(7),
            name: format!("value_{ordinal}"),
            field: RenderField {
                native_type: NativeRenderType::String,
                presentation: RenderPresentation::ScalarText,
                nullable: true,
            },
        })
        .collect();
    FrozenRootOutput::ClientRows(ClientRenderSchema::try_new(columns, 1).unwrap())
}

struct Fixture {
    pool: Arc<RootProducerPool>,
    budget: Arc<ResultRetainedBudget>,
    channel: Arc<RootResultChannel>,
    session: Arc<NativeRootResultSession>,
}

impl Fixture {
    fn new(output: FrozenRootOutput) -> Self {
        let limits = WorkerResultRetainedLimits::try_new(256 << 20, 512 << 20).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let pool = RootProducerPool::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            1 << 20,
            Arc::clone(&budget),
        )
        .unwrap();
        let channel = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, output)),
            },
            Arc::clone(&budget),
            limits,
        )
        .unwrap();
        channel.mark_context_owned().unwrap();
        let session = NativeRootResultSession::try_open(Arc::clone(&channel), &pool).unwrap();
        Self {
            pool,
            budget,
            channel,
            session,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // A failed assertion must not perform an unbounded join on the test
        // thread. Normal tests explicitly await shutdown under DEADLINE.
        let pool = Arc::clone(&self.pool);
        std::thread::spawn(move || {
            let _ = pool.shutdown();
        });
    }
}

async fn shutdown(pool: &Arc<RootProducerPool>) {
    let pool = Arc::clone(pool);
    let (sent, received) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = sent.send(pool.shutdown());
    });
    tokio::time::timeout(DEADLINE, received)
        .await
        .expect("root pool shutdown exceeded deadline")
        .expect("shutdown owner exited without its result")
        .expect("root pool shutdown failed");
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("root condition exceeded deadline");
}

async fn input(session: &NativeRootResultSession) -> RootInputPermit {
    tokio::time::timeout(DEADLINE, async {
        loop {
            match session.try_acquire_input().unwrap() {
                RootInputAdmission::Granted(permit) => return permit,
                RootInputAdmission::Blocked => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    })
    .await
    .expect("input admission exceeded deadline")
}

async fn read(
    channel: &Arc<RootResultChannel>,
    wanted: Option<u64>,
    consumed: u64,
) -> RootResultDelivery {
    let spec = channel.spec();
    let request = RootResultRead::try_new(
        spec.task,
        spec.contract.profile(),
        spec.contract.kind(),
        wanted.map(|sequence| NonZeroU64::new(sequence).unwrap()),
        consumed,
        Duration::from_millis(1),
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, channel.read(&request))
        .await
        .expect("root read exceeded deadline")
        .expect("root read failed")
}

fn owned_chunk(array: ArrayRef) -> Chunk {
    let field = ArrowMetadataOwner::try_new(
        Vec::new(),
        MetadataOwnerLimits {
            entries: 0,
            construction_bytes: 0,
        },
    )
    .unwrap()
    .into_field("value".into(), array.data_type().clone(), true);
    let origins = FieldMetadataOrigins::try_new(vec![field.clone()], 1).unwrap();
    let slot = ChunkSlotSchema::try_new_with_metadata_origins(
        SlotId::new(7),
        field.field().clone(),
        origins,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(ChunkSchema::try_new(vec![slot]).unwrap());
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}

fn strings(values: Vec<Option<&str>>) -> Chunk {
    owned_chunk(Arc::new(StringArray::from(values)))
}

fn body(delivery: &RootResultDelivery) -> &[u8] {
    let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
        panic!("expected retained Data");
    };
    data.body().as_ref()
}

fn assert_end(delivery: &RootResultDelivery, sequence: u64, rows: u64) {
    let RootReadOutcome::End(end) = &delivery.reply().outcome else {
        panic!("expected retained End");
    };
    assert_eq!(end.sequence.get(), sequence);
    assert_eq!(end.output_rows, rows);
}

#[tokio::test]
async fn client_dictionary_repeated_occurrences_and_null_have_independent_exact_bytes() {
    let fixture = Fixture::new(client(2));
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(1), None, Some(0)]),
        Arc::new(StringArray::from(vec!["x", "é"])),
    )
    .unwrap();
    assert!(matches!(dictionary.data_type(), DataType::Dictionary(_, _)));
    fixture
        .session
        .submit_input(
            owned_chunk(Arc::new(dictionary)),
            input(&fixture.session).await,
        )
        .unwrap();
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    assert!(fixture.session.producer_exited());
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    assert_eq!(fixture.channel.snapshot().offered_through, 0);
    let first = read(&fixture.channel, Some(1), 0).await;
    // Each row begins with u32LE rowTotal; cells use MySQL length-encoded
    // UTF-8 bytes (or 0xfb NULL). Expected bytes use no production encoder.
    let expected = [
        6, 0, 0, 0, 2, 0xc3, 0xa9, 2, 0xc3, 0xa9, 2, 0, 0, 0, 0xfb, 0xfb, 4, 0, 0, 0, 1, b'x', 1,
        b'x',
    ];
    assert_eq!(body(&first), expected);
    drop(first);
    let replay = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(body(&replay), expected);
    drop(replay);
    assert_end(&read(&fixture.channel, Some(2), 0).await, 2, 3);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn count_only_accepts_decimal256_without_client_cell_rendering() {
    let fixture = Fixture::new(FrozenRootOutput::CountOnly);
    // Decimal256 is deliberately unavailable for ClientRows presentation;
    // CountOnly must inspect its standard backing and count rows directly.
    let array = Decimal256Array::from(vec![
        Some(i256::from_i128(7)),
        None,
        Some(i256::from_i128(-9)),
    ])
    .with_precision_and_scale(76, 0)
    .unwrap();
    fixture
        .session
        .submit_input(owned_chunk(Arc::new(array)), input(&fixture.session).await)
        .unwrap();
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    assert!(fixture.session.producer_exited());
    assert_eq!(fixture.channel.snapshot().data_positions, 0);
    assert_eq!(fixture.channel.snapshot().produced_through, 1);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 3);
    assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 3);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn one_original_input_position_and_same_task_foreign_issuer_are_distinct() {
    // Two CountOnly grants fit the same 256 MiB root budget. ClientRows would
    // require two 192 MiB grants and correctly block at the budget first.
    let fixture = Fixture::new(FrozenRootOutput::CountOnly);
    let permit = input(&fixture.session).await;
    let generation = permit.generation();
    assert!(matches!(
        fixture.session.try_acquire_input().unwrap(),
        RootInputAdmission::Blocked
    ));
    let foreign = RootInputAuthority::new(fixture.session.spec());
    let ResultWriteAdmission::Granted(credit) = fixture
        .channel
        .try_reserve(foreign.required_bytes())
        .unwrap()
    else {
        panic!("foreign issuer uses the same existing retained budget");
    };
    let RootInputAdmission::Granted(foreign_permit) = foreign.try_acquire(credit).unwrap() else {
        panic!("foreign issuer has a free position");
    };
    let error = fixture
        .session
        .submit_input(strings(vec![Some("foreign")]), foreign_permit)
        .unwrap_err();
    assert!(error.to_string().contains("different issuer or generation"));
    assert!(foreign.is_available());
    assert!(matches!(
        fixture.session.try_acquire_input().unwrap(),
        RootInputAdmission::Blocked
    ));
    drop(permit);
    let next = input(&fixture.session).await;
    assert!(next.generation() > generation);
    drop(next);
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn tiny_complete_batches_flush_before_finish_and_before_next_input() {
    let fixture = Fixture::new(client(1));
    fixture
        .session
        .submit_input(strings(vec![Some("a")]), input(&fixture.session).await)
        .unwrap();
    wait_until(|| fixture.channel.snapshot().produced_through == 1).await;
    assert_eq!(
        fixture.session.producer_state(),
        RootProducerState::Accepting
    );
    let first = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(body(&first), [2, 0, 0, 0, 1, b'a']);
    drop(first);
    fixture
        .session
        .submit_input(strings(vec![Some("b")]), input(&fixture.session).await)
        .unwrap();
    wait_until(|| fixture.channel.snapshot().produced_through == 2).await;
    assert_eq!(fixture.channel.snapshot().data_positions, 2);
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    assert!(fixture.session.producer_exited());
    let second = read(&fixture.channel, Some(2), 0).await;
    assert_eq!(body(&second), [2, 0, 0, 0, 1, b'b']);
    drop(second);
    assert_end(&read(&fixture.channel, Some(3), 0).await, 3, 2);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn empty_finish_has_end_without_a_prior_input_or_data_item() {
    for output in [client(1), FrozenRootOutput::CountOnly] {
        let fixture = Fixture::new(output);
        fixture.session.finish_input().unwrap();
        wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
        assert!(fixture.session.producer_exited());
        assert!(fixture.session.try_acquire_input().is_err());
        assert_eq!(fixture.channel.snapshot().data_positions, 0);
        assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 0);
        shutdown(&fixture.pool).await;
    }
}

#[tokio::test]
async fn unknown_actual_schema_refuses_before_producer_and_drops_chunk_before_credit_wake() {
    let fixture = Fixture::new(client(1));
    let array: ArrayRef = Arc::new(StringArray::from(vec!["unproven"]));
    let weak = Arc::downgrade(&array);
    let original = owned_chunk(array);
    // Same field/schema contents, independently allocated top-level schema.
    // Generic Chunk construction preserves semantics but cannot mint an origin.
    let unknown_schema = Arc::new(original.batch.schema().as_ref().clone());
    let unknown_batch =
        RecordBatch::try_new(unknown_schema.clone(), original.batch.columns().to_vec()).unwrap();
    let unknown =
        Chunk::try_new_with_chunk_schema(unknown_batch, original.chunk_schema_ref()).unwrap();
    assert!(Arc::ptr_eq(&unknown.batch.schema(), &unknown_schema));
    drop(original);
    let permit = input(&fixture.session).await;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let dropped_before_wake = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&callbacks);
    let actual_dropped = Arc::clone(&dropped_before_wake);
    let _subscription = fixture
        .session
        .writable_observable()
        .subscribe(Arc::new(move || {
            actual_dropped.store(weak.upgrade().is_none(), Ordering::SeqCst);
            observed.fetch_add(1, Ordering::SeqCst);
        }));
    let error = fixture.session.submit_input(unknown, permit).unwrap_err();
    assert!(error.to_string().contains("backing is unproven"));
    assert!(callbacks.load(Ordering::SeqCst) > 0);
    assert!(dropped_before_wake.load(Ordering::SeqCst));
    assert!(fixture.session.producer_exited());
    assert_eq!(fixture.channel.snapshot().produced_through, 0);
    assert_eq!(
        fixture.session.producer_state(),
        RootProducerState::Accepting
    );
    drop(input(&fixture.session).await);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn full_window_suspends_original_input_and_abort_exits_with_response_alias_held() {
    let fixture = Fixture::new(client(1));
    let value = "q".repeat(3 << 20);
    let array: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
    let weak = Arc::downgrade(&array);
    fixture
        .session
        .submit_input(owned_chunk(array), input(&fixture.session).await)
        .unwrap();
    wait_until(|| fixture.channel.snapshot().data_positions == 2).await;
    assert_eq!(fixture.channel.snapshot().produced_through, 2);
    assert!(weak.upgrade().is_some());
    assert!(!fixture.session.producer_exited());
    assert!(matches!(
        fixture.session.try_acquire_input().unwrap(),
        RootInputAdmission::Blocked
    ));
    let delivery = read(&fixture.channel, Some(1), 0).await;
    let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
        panic!("expected Data")
    };
    let alias = data.body().clone();
    drop(delivery);
    fixture
        .session
        .abort(ResultAbort::Cancelled("test cancellation".into()));
    wait_until(|| fixture.session.producer_exited()).await;
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert!(fixture.session.try_acquire_input().is_err());
    assert!(!fixture.channel.physical_idle());
    assert!(!alias.is_empty());
    // Drop the session's fixed metadata holder too: only the response alias
    // remains as the deliberately live physical result owner.
    let channel = Arc::clone(&fixture.channel);
    let pool = Arc::clone(&fixture.pool);
    drop(fixture);
    assert!(!channel.physical_idle());
    drop(alias);
    wait_until(|| channel.physical_idle()).await;
    shutdown(&pool).await;
}

#[tokio::test]
async fn pool_shutdown_drains_an_active_blocked_session_without_consumer_ack() {
    let fixture = Fixture::new(client(1));
    let value = "x".repeat(3 << 20);
    let array: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
    let weak = Arc::downgrade(&array);
    fixture
        .session
        .submit_input(owned_chunk(array), input(&fixture.session).await)
        .unwrap();
    wait_until(|| fixture.channel.snapshot().data_positions == 2).await;
    assert!(!fixture.session.producer_exited());
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    shutdown(&fixture.pool).await;
    assert!(fixture.session.producer_exited());
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert!(fixture.session.try_acquire_input().is_err());
    assert!(
        NativeRootResultSession::try_open(Arc::clone(&fixture.channel), &fixture.pool).is_err()
    );
    fixture.channel.close(RootRetentionClose::ContextReleased);
}

#[tokio::test]
async fn repeated_cancel_observer_panics_cannot_defer_original_input_exit() {
    let fixture = Fixture::new(client(1));
    let value = "x".repeat(3 << 20);
    let array: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
    let weak = Arc::downgrade(&array);
    fixture
        .session
        .submit_input(owned_chunk(array), input(&fixture.session).await)
        .unwrap();
    wait_until(|| fixture.channel.snapshot().data_positions == 2).await;
    let observed = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&observed);
    let original = weak.clone();
    let subscription = fixture
        .session
        .writable_observable()
        .subscribe(Arc::new(move || {
            if original.upgrade().is_some() {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                // The cap keeps a broken implementation's failure finite.
                // The oracle requires actual cleanup before it can exhaust
                // these repeated panics, rather than passing after retries.
                if call < 64 {
                    panic!("injected repeated cancellation observer panic");
                }
            }
        }));
    shutdown(&fixture.pool).await;
    assert!(fixture.session.producer_exited());
    assert!(weak.upgrade().is_none());
    assert!(
        observed.load(Ordering::SeqCst) < 64,
        "original input exit depended on exhausting the observer's panic budget"
    );
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    drop(subscription);
    let available = (512 << 20) - fixture.pool.reserved_bytes() - (1 << 20);
    let ResultWriteAdmission::Granted(credit) =
        fixture.budget.try_reserve_process(available).unwrap()
    else {
        panic!("original input grant remained retained after its physical exit");
    };
    drop(credit);
}

#[tokio::test]
async fn count_only_panicking_input_credit_observer_cannot_strand_shutdown_or_grant() {
    let fixture = Fixture::new(FrozenRootOutput::CountOnly);
    let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let weak = Arc::downgrade(&array);
    let permit = input(&fixture.session).await;
    let fired = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&fired);
    let _subscription = fixture
        .session
        .writable_observable()
        .subscribe(Arc::new(move || {
            if !observed.swap(true, Ordering::SeqCst) {
                panic!("injected one-time writable observer failure");
            }
        }));
    // CountOnly drops Input inside the producer turn. The first release
    // notification unwinds through that turn's state lock and poisons it.
    fixture
        .session
        .submit_input(owned_chunk(array), permit)
        .unwrap();
    wait_until(|| fired.load(Ordering::SeqCst)).await;
    shutdown(&fixture.pool).await;
    assert!(fixture.session.producer_exited());
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert!(fixture.session.try_acquire_input().is_err());
    // A reservation of every byte apart from the still-live fixed pool and
    // channel proves the original 96 MiB grant has returned. No body allocation
    // accompanies this process credit, and no private testing counter is used.
    let available = (512 << 20) - fixture.pool.reserved_bytes() - (1 << 20);
    let ResultWriteAdmission::Granted(credit) =
        fixture.budget.try_reserve_process(available).unwrap()
    else {
        panic!("original input grant remained retained after its physical exit");
    };
    drop(credit);
}

#[tokio::test]
async fn exit_hook_notification_panic_cannot_hide_completed_producer_exit() {
    let fixture = Fixture::new(client(1));
    let fired = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&fired);
    let channel = Arc::downgrade(&fixture.channel);
    let _subscription = fixture
        .channel
        .writable_observable()
        .subscribe(Arc::new(move || {
            if let Some(channel) = channel.upgrade()
                && channel.producer_state() == RootProducerState::ContextHeld
                && !observed.swap(true, Ordering::SeqCst)
            {
                // End has been published and the real producer guard has
                // handed off to its context. Its exit notification can unwind
                // before the ordinary final exited.store would execute.
                panic!("injected one-time completed producer exit notification failure");
            }
        }));
    fixture.session.finish_input().unwrap();
    wait_until(|| fired.load(Ordering::SeqCst)).await;
    wait_until(|| fixture.session.producer_exited()).await;
    shutdown(&fixture.pool).await;
    assert!(fixture.session.producer_exited());
    assert_eq!(
        fixture.session.producer_state(),
        RootProducerState::ContextHeld
    );
    assert_eq!(fixture.channel.snapshot().data_positions, 0);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 0);
}

#[tokio::test]
async fn writable_snapshot_retains_fixed_metadata_after_last_session_arc_exits() {
    // Construct these owners directly: no Fixture session alias or automatic
    // pool shutdown may hide which owner keeps the metadata grant alive.
    let limits = WorkerResultRetainedLimits::try_new(256 << 20, 512 << 20).unwrap();
    let budget = ResultRetainedBudget::new(limits.per_process());
    let pool = RootProducerPool::try_new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(8).unwrap(),
        1 << 20,
        Arc::clone(&budget),
    )
    .unwrap();
    let channel = RootResultChannel::try_open(
        RootResultWriteSpec {
            task: task(),
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            )),
        },
        budget,
        limits,
    )
    .unwrap();
    channel.mark_context_owned().unwrap();
    let session = NativeRootResultSession::try_open(Arc::clone(&channel), &pool).unwrap();
    assert!(session.producer_exited());
    let weak_session = Arc::downgrade(&session);
    let first = Arc::new(AtomicBool::new(true));
    let first_notification = Arc::clone(&first);
    let released_in_time = Arc::new(AtomicBool::new(false));
    let gate_result = Arc::clone(&released_in_time);
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (release, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let _subscription = session.writable_observable().subscribe(Arc::new(move || {
        if first_notification.swap(false, Ordering::SeqCst) {
            if let Ok(mut entered) = entered.lock()
                && let Some(entered) = entered.take()
            {
                let _ = entered.send(());
            }
            // The gate never panics and never blocks indefinitely. A
            // second notification from channel.close must pass through.
            let released = release_rx
                .lock()
                .ok()
                .is_some_and(|receiver| receiver.recv_timeout(DEADLINE).is_ok());
            gate_result.store(released, Ordering::SeqCst);
        }
    }));
    let writable = channel.writable_observable();
    let notification = std::thread::spawn(move || writable.notify_observers());
    let (joined, joined_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = joined.send(notification.join());
    });
    tokio::time::timeout(DEADLINE, entered_rx)
        .await
        .expect("notification did not reach the observer gate")
        .expect("observer gate owner disappeared");
    assert_eq!(Arc::strong_count(&session), 1);
    drop(session);
    // The exported readiness token now also retains the fixed backing grant.
    // Drop it while the already captured notification is still blocked, so
    // the remaining holder below is the real callback/snapshot itself.
    drop(_subscription);
    assert!(
        weak_session.upgrade().is_none(),
        "the notification snapshot must not pin the Session itself"
    );
    channel.close(RootRetentionClose::ContextAborted);
    let owners = wait_until(|| channel.physical_idle());
    tokio::pin!(owners);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut owners)
            .await
            .is_err(),
        "the blocked callback snapshot still retains fixed metadata"
    );
    release.send(()).unwrap();
    tokio::time::timeout(DEADLINE, joined_rx)
        .await
        .expect("notification join exceeded deadline")
        .expect("notification join owner disappeared")
        .expect("notification thread panicked");
    assert!(released_in_time.load(Ordering::SeqCst));
    tokio::time::timeout(DEADLINE, &mut owners)
        .await
        .expect("fixed metadata did not exit with the callback snapshot");
    assert!(channel.physical_idle());
    shutdown(&pool).await;
}

// Construct from the real Arrow physical DTO decoder and retain its exact
// nested Field owners in each List/Map carrier. A borrowed equal schema cannot
// stand in for this construction provenance.
fn statistics_chunk(ids: &[i32], blob: &str, body: &[u8]) -> (Chunk, std::sync::Weak<BinaryArray>) {
    use arrow::array::{ListArray, MapArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use novarocks_proto_codec::{FieldPath, arrow_physical};
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Utf8, false)),
            ]
            .into(),
        ),
        false,
    ));
    let schema = Schema::new(vec![
        Field::new(
            "input_fields",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        ),
        Field::new("blob_type", DataType::Utf8, false),
        Field::new("body", DataType::Binary, true),
        Field::new("properties", DataType::Map(entries, false), false),
    ]);
    let (wire, metadata) =
        arrow_physical::encode_schema(&schema, &[1, 2, 3, 4], false, FieldPath::root("schema"))
            .unwrap();
    let decoded =
        arrow_physical::decode_schema(&wire, &metadata, FieldPath::root("schema")).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_owned_schema_and_slot_ids(
        decoded.schema_metadata_origin(),
        decoded.field_metadata_origins(),
        &[
            SlotId::new(1),
            SlotId::new(2),
            SlotId::new(3),
            SlotId::new(4),
        ],
    )
    .unwrap();
    let DataType::List(item) = decoded.schema().field(0).data_type() else {
        panic!("list");
    };
    let DataType::Map(entries, false) = decoded.schema().field(3).data_type() else {
        panic!("map");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("entries");
    };
    let empty = || Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let body_array = Arc::new(BinaryArray::from_iter_values([body]));
    let weak = Arc::downgrade(&body_array);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::new(vec![0, ids.len() as i32].into()),
            Arc::new(Int32Array::from(ids.to_vec())),
            None,
        )),
        Arc::new(StringArray::from(vec![blob])),
        body_array,
        Arc::new(MapArray::new(
            entries.clone(),
            OffsetBuffer::new(vec![0, 0].into()),
            StructArray::new(fields.clone(), vec![empty(), empty()], None),
            None,
            false,
        )),
    ];
    let batch = RecordBatch::try_new(chunk_schema.arrow_schema_ref(), columns).unwrap();
    (
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap(),
        weak,
    )
}

#[tokio::test]
async fn statistics_domain_emits_exact_opaque_records_across_two_original_inputs() {
    use novarocks_result_contract::InternalResultDomain;
    let fixture = Fixture::new(FrozenRootOutput::InternalFacts(
        InternalResultDomain::StatisticsArtifactV1,
    ));
    let (chunk, weak) = statistics_chunk(&[9, 7], "é", &[0, 255, 65]);
    fixture
        .session
        .submit_input(chunk, input(&fixture.session).await)
        .unwrap();
    let second_permit = input(&fixture.session).await;
    assert!(weak.upgrade().is_none());
    let first = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(
        body(&first),
        &[
            83, 84, 65, 49, 37, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0,
            0, 7, 0, 0, 0, 195, 169, 0, 255, 65
        ]
    );
    drop(first);
    let (chunk, _) = statistics_chunk(&[3], "x", &[]);
    fixture.session.submit_input(chunk, second_permit).unwrap();
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_exited()).await;
    let second = read(&fixture.channel, Some(2), 1).await;
    assert_eq!(
        body(&second),
        &[
            83, 84, 65, 49, 29, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0,
            0, 120
        ]
    );
    drop(second);
    assert_end(&read(&fixture.channel, Some(3), 2).await, 3, 2);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn statistics_record_can_cross_segments_and_cancel_releases_its_original_backing() {
    use novarocks_result_contract::InternalResultDomain;
    let fixture = Fixture::new(FrozenRootOutput::InternalFacts(
        InternalResultDomain::StatisticsArtifactV1,
    ));
    let body_bytes = vec![0xf3; 3 << 20];
    let (chunk, weak) = statistics_chunk(&[1], "theta", &body_bytes);
    fixture
        .session
        .submit_input(chunk, input(&fixture.session).await)
        .unwrap();
    wait_until(|| fixture.channel.snapshot().data_positions == 2).await;
    assert!(!fixture.session.producer_exited());
    assert!(weak.upgrade().is_some());
    let first = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(&body(&first)[..4], b"STA1");
    assert_eq!(body(&first).len(), 1 << 20);
    drop(first);
    fixture.session.abort(ResultAbort::Cancelled(
        "cancel statistics continuation".into(),
    ));
    wait_until(|| fixture.session.producer_exited()).await;
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    shutdown(&fixture.pool).await;
}
