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

//! A finite BE root producer; publication and consumption have separate owners.

use std::alloc::Layout;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use novarocks_execution::exec::chunk::{
    Chunk, RootArrayStorageLimits, borrowed_root_chunk_storage,
};
use novarocks_execution::runtime::fragment::io::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, ResultAbort, ResultWriteAdmission,
    ResultWriteCredit, RootInputAdmission, RootInputAuthority, RootInputPermit, RootProducerState,
    RootResultSession, RootResultWriteSpec,
};
use novarocks_execution::runtime::observable::{Observable, ObserverSubscription};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_execution_contract::root_lifetime::RootRetentionClose;
use novarocks_result_contract::{RootOutputKind, RootProfileV1};
use novarocks_result_render::{ArrowMysqlTextEncoder, BoundedMysqlTextEncoder, RenderTurnStatus};
use novarocks_worker::root_result_channel::{
    RootMetadataReservation, RootProducerExit, RootResultChannel, RootSegmentBuilder,
};

pub use crate::root_producer_pool::RootProducerPool;
use crate::root_producer_pool::{
    RootProducerJob, RootProducerRegistration, RootProducerTurn, RootProducerWake,
    weak_core_backing_bytes,
};

// Destruction order is deliberate: original/cursor Arrow owners are gone
// before input overlap credit can wake another driver. Scratch follows cursor.
struct Input {
    encoder: Option<Box<ArrowMysqlTextEncoder>>,
    chunk: Chunk,
    permit: RootInputPermit,
    scratch: Option<ResultWriteCredit>,
}
struct ProducerState {
    input: Option<Input>,
    builder: Option<RootSegmentBuilder>,
    used: usize,
    sealed: bool,
    failed: Option<&'static str>,
    producer: Option<RootProducerExit>,
}
// A notification snapshot can outlive the session subscription. Its guard
// follows the real callback and its Weak control tails through that exit.
struct WakeBridge {
    wake: RootProducerWake,
    driver: Weak<Observable>,
    _metadata: Arc<RootMetadataReservation>,
}
impl WakeBridge {
    fn notify(&self) {
        let _ = self.wake.wake_active();
        if let Some(observable) = self.driver.upgrade() {
            observable.notify_observers();
        }
    }
}

/// Driver calls transfer one original input. Rendering runs only on the shared
/// fixed CPU pool; neither a full queue nor a pending ACK occupies that pool.
/// The composition retains the pool and explicitly shuts it down before its
/// driver executor. Runtime Task owners retain the channel, not this session.
pub struct NativeRootResultSession {
    channel: Arc<RootResultChannel>,
    authority: RootInputAuthority,
    state: Mutex<ProducerState>,
    registration: OnceLock<RootProducerRegistration>,
    wake_subscription: OnceLock<ObserverSubscription>,
    exited: AtomicBool,
    _metadata: Arc<RootMetadataReservation>,
}
impl NativeRootResultSession {
    pub fn try_open(
        channel: Arc<RootResultChannel>,
        pool: &Arc<RootProducerPool>,
    ) -> Result<Arc<Self>, FragmentIoError> {
        if matches!(
            channel.spec().contract.kind(),
            RootOutputKind::InternalFacts(_)
        ) {
            return Err(io_error("explicit internal root codec is not installed"));
        }
        // All fixed session/issuer/callback scaffolds are covered before
        // creating them. Cursor/batch-clone heaps use separate scratch credit.
        #[repr(C, align(2))]
        struct Header {
            strong: AtomicUsize,
            weak: AtomicUsize,
        }
        fn arc<T>() -> usize {
            Layout::new::<Header>()
                .extend(Layout::new::<T>())
                .expect("fixed Arc layout fits usize")
                .0
                .pad_to_align()
                .size()
        }
        let metadata_bytes = arc::<Self>()
            + RootInputAuthority::initial_backing_bytes()
            + arc::<WakeBridge>()
            + arc::<Arc<dyn Fn() + Send + Sync>>()
            + 2 * std::mem::size_of::<std::sync::Weak<Arc<dyn Fn() + Send + Sync>>>()
            + arc::<RootMetadataReservation>()
            + weak_core_backing_bytes();
        #[cfg(target_vendor = "apple")]
        let metadata_bytes = metadata_bytes + Layout::new::<(isize, [u8; 56])>().size();
        let metadata = channel
            .try_reserve_metadata(metadata_bytes)
            .map_err(|_| io_error("root session fixed metadata exceeds its envelope"))?;
        let authority = RootInputAuthority::try_new(channel.spec())?;
        let session = Arc::new(Self {
            channel,
            authority,
            state: Mutex::new(ProducerState {
                input: None,
                builder: None,
                used: 0,
                sealed: false,
                failed: None,
                producer: None,
            }),
            registration: OnceLock::new(),
            wake_subscription: OnceLock::new(),
            exited: AtomicBool::new(true),
            _metadata: Arc::new(metadata),
        });
        drop(
            session
                .state
                .lock()
                .expect("root session initial state lock"),
        );
        let job = Arc::clone(&session) as Arc<dyn RootProducerJob>;
        let registration = pool
            .register(Arc::downgrade(&job))
            .map_err(|_| io_error("root CPU pool has no installation position"))?;
        drop(job);
        let wake = registration.wake_handle();
        session
            .registration
            .set(registration)
            .map_err(|_| io_error("root producer registered twice"))?;
        let bridge = WakeBridge {
            wake,
            driver: Arc::downgrade(&session.authority.observable()),
            _metadata: Arc::clone(&session._metadata),
        };
        let subscription = session
            .channel
            .writable_observable()
            .try_subscribe_retained(
                Arc::new(move || {
                    // No session/job mutex is taken here. Readiness callbacks can run
                    // synchronously while a producer destroys input or returns credit.
                    bridge.notify();
                }),
                Arc::clone(&session._metadata) as Arc<dyn std::any::Any + Send + Sync>,
            )
            .map_err(|_| io_error("root readiness has no producer bridge position"))?;
        session
            .wake_subscription
            .set(subscription)
            .map_err(|_| io_error("root readiness registered twice"))?;
        Ok(session)
    }
    pub fn channel(&self) -> &Arc<RootResultChannel> {
        &self.channel
    }

    fn state(&self) -> MutexGuard<'_, ProducerState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.failed.get_or_insert("root producer turn panicked");
                state.sealed = true;
                // The following real pool turn still destroys input/cursor and
                // calls exited. Recovery never equates a panic with reclamation.
                self.state.clear_poison();
                state
            }
        }
    }

    fn wake(&self) -> Result<(), FragmentIoError> {
        self.registration
            .get()
            .expect("root is registered")
            .wake()
            .map_err(|_| io_error("root CPU pool is closed"))
    }
    fn start(&self, state: &mut ProducerState) -> Result<(), FragmentIoError> {
        if state.producer.is_none() && self.exited.load(Ordering::Acquire) {
            state.producer = Some(
                self.channel
                    .start_producer()
                    .map_err(|_| io_error("root producer cannot start"))?,
            );
            self.exited.store(false, Ordering::Release);
        }
        Ok(())
    }
    fn fail(&self, state: &mut ProducerState, message: &'static str) -> RootProducerTurn {
        state.failed.get_or_insert(message);
        state.sealed = true;
        self.authority.close();
        self.channel.close(RootRetentionClose::ContextAborted);
        // Original owners, cursor and scratch are destroyed before exited().
        drop(state.input.take());
        drop(state.builder.take());
        state.used = 0;
        RootProducerTurn::Complete
    }
    fn advance(&self, state: &mut ProducerState) -> RootProducerTurn {
        if state.failed.is_some() || self.channel.is_closed() {
            return self.fail(state, "root producer was cancelled");
        }
        if let Some(input) = state.input.as_mut() {
            if self.spec().contract.kind() == RootOutputKind::CountOnly {
                let Ok(rows) = u64::try_from(input.chunk.len()) else {
                    return self.fail(state, "root row count exceeds u64");
                };
                if self.channel.note_rows(rows).is_err() {
                    return self.fail(state, "root row count overflow");
                }
                drop(state.input.take());
                return RootProducerTurn::Yielded;
            }
            if input.encoder.is_none() {
                let capacity = NativeResultSupportGeometry::V1.root_scratch_capacity_bytes as usize;
                let credit = match self.channel.try_reserve(capacity) {
                    Ok(ResultWriteAdmission::Granted(credit)) => credit,
                    Ok(ResultWriteAdmission::Blocked) => return RootProducerTurn::Blocked,
                    Err(_) => return self.fail(state, "root scratch reservation was rejected"),
                };
                input.scratch = Some(credit);
                // This clone only allocates the finite columns Vec. Original
                // Chunk accounting/source capabilities remain live in input.
                let encoder = match ArrowMysqlTextEncoder::try_new_root(
                    Arc::clone(&self.spec().contract),
                    input.chunk.batch.clone(),
                ) {
                    Ok(encoder) => encoder,
                    Err(_) => {
                        return self.fail(
                            state,
                            "root input cannot be rendered under its frozen schema",
                        );
                    }
                };
                let cloning =
                    input.chunk.batch.num_columns() * std::mem::size_of::<arrow::array::ArrayRef>();
                if encoder
                    .scratch_capacity_bytes()
                    .checked_add(cloning)
                    .and_then(|bytes| {
                        bytes.checked_add(std::mem::size_of::<ArrowMysqlTextEncoder>())
                    })
                    .is_none_or(|b| b > capacity)
                {
                    return self.fail(state, "root renderer scratch exceeds its pregrant");
                }
                input.encoder = Some(Box::new(encoder));
                return RootProducerTurn::Yielded;
            }
            if state.builder.is_none() {
                match self.channel.try_segment() {
                    Ok(Some(builder)) => {
                        state.builder = Some(builder);
                        state.used = 0;
                    }
                    Ok(None) => return RootProducerTurn::Blocked,
                    Err(_) => return self.fail(state, "root segment reservation was rejected"),
                }
            }
            let turn = match input
                .encoder
                .as_mut()
                .unwrap()
                .step(state.builder.as_mut().unwrap().output_at(state.used))
            {
                Ok(turn) => turn,
                Err(_) => return self.fail(state, "root text encoding failed"),
            };
            state.used += turn.emitted_bytes;
            if self.channel.note_rows(turn.completed_rows).is_err() {
                return self.fail(state, "root row count overflow");
            }
            let input_complete = turn.status == RenderTurnStatus::InputComplete;
            if input_complete {
                drop(state.input.take());
            }
            let finishing = input_complete && state.sealed;
            if finishing && self.channel.request_finish().is_err() {
                return self.fail(state, "root finish request was rejected");
            }
            let flush = input_complete
                || turn.completed_rows != 0
                || turn.status == RenderTurnStatus::NeedsOutput
                || state.used == RootProfileV1::SEGMENT_BYTES;
            if flush {
                let builder = state.builder.take().unwrap();
                let bytes = std::mem::take(&mut state.used);
                if bytes == 0 {
                    drop(builder);
                } else if self
                    .channel
                    .publish_segment(builder, bytes, finishing)
                    .is_err()
                {
                    return self.fail(state, "root data publication failed");
                }
                if finishing {
                    if bytes == 0 && self.channel.publish_end().is_err() {
                        return self.fail(state, "root End publication failed");
                    }
                    return RootProducerTurn::Complete;
                }
            }
            return RootProducerTurn::Yielded;
        }
        if state.sealed {
            if self
                .channel
                .request_finish()
                .and_then(|_| self.channel.publish_end())
                .is_err()
            {
                return self.fail(state, "root End publication failed");
            }
            return RootProducerTurn::Complete;
        }
        RootProducerTurn::Idle
    }
}
impl RootResultSession for NativeRootResultSession {
    fn spec(&self) -> &RootResultWriteSpec {
        self.channel.spec()
    }
    fn writable_observable(&self) -> Arc<Observable> {
        self.authority.observable()
    }
    fn try_acquire_input(&self) -> Result<RootInputAdmission, FragmentIoError> {
        if self.channel.is_closed() || self.state().sealed {
            return Err(io_error("root input is closed"));
        }
        if !self.authority.is_available() {
            return Ok(RootInputAdmission::Blocked);
        }
        match self.channel.try_reserve(self.authority.required_bytes()) {
            Ok(ResultWriteAdmission::Granted(credit)) => self.authority.try_acquire(credit),
            Ok(ResultWriteAdmission::Blocked) => Ok(RootInputAdmission::Blocked),
            Err(_) => Err(io_error("root input reservation was rejected")),
        }
    }
    fn submit_input(&self, chunk: Chunk, permit: RootInputPermit) -> Result<(), FragmentIoError> {
        let input = Input {
            encoder: None,
            chunk,
            permit,
            scratch: None,
        };
        if !self.authority.owns(&input.permit) {
            drop(input);
            return Err(io_error(
                "root input permit belongs to a different issuer or generation",
            ));
        }
        // Bounded structural inspection only: no render/hydrate/row counting
        // in the driver. An oversized/unknown original cannot enter the queue.
        let cap =
            NativeResultSupportGeometry::V1.root_original_input_backing_capacity_bytes as usize;
        if borrowed_root_chunk_storage(
            &input.chunk,
            RootArrayStorageLimits {
                bytes: cap,
                nodes: 2 * RootProfileV1::SCHEMA_TYPE_NODES,
                depth: RootProfileV1::MAX_DEPTH,
            },
        )
        .is_err()
        {
            drop(input);
            return Err(io_error(
                "root original input backing is unproven or exceeds its profile",
            ));
        }
        let mut state = self.state();
        if state.sealed
            || state.failed.is_some()
            || state.input.is_some()
            || self.channel.is_closed()
        {
            drop(state);
            drop(input);
            return Err(io_error("root input position is not accepting"));
        }
        if let Err(error) = self.start(&mut state) {
            drop(state);
            drop(input);
            return Err(error);
        }
        state.input = Some(input);
        drop(state);
        self.wake()
    }
    fn finish_input(&self) -> Result<(), FragmentIoError> {
        let mut state = self.state();
        if state.failed.is_some() || self.channel.is_closed() {
            return Err(io_error("root producer is closed"));
        }
        self.start(&mut state)?;
        state.sealed = true;
        self.authority.close();
        drop(state);
        self.wake()
    }
    fn producer_state(&self) -> RootProducerState {
        let state = self.state();
        if let Some(message) = state.failed {
            return RootProducerState::Failed(message.into());
        }
        if self.channel.is_closed() {
            return RootProducerState::Failed("root channel is closed".into());
        }
        let published = self.channel.producer_state();
        if state.sealed && published == RootProducerState::Accepting {
            RootProducerState::Finishing
        } else {
            published
        }
    }
    fn producer_exited(&self) -> bool {
        self.exited.load(Ordering::Acquire)
    }
    fn abort(&self, _reason: ResultAbort) {
        let mut state = self.state();
        state.failed.get_or_insert("root producer was cancelled");
        state.sealed = true;
        self.authority.close();
        self.channel.close(RootRetentionClose::ContextAborted);
        drop(state);
        let _ = self.wake();
    }
}
impl RootProducerJob for NativeRootResultSession {
    fn turn(&self) -> RootProducerTurn {
        self.advance(&mut self.state())
    }
    fn cancel(&self) {
        self.abort(ResultAbort::Cancelled(String::new()));
    }
    fn exited(&self) {
        let producer = {
            let mut state = self.state();
            assert!(
                state.input.is_none() && state.builder.is_none(),
                "producer's real backing exits before its guard"
            );
            state.producer.take()
        };
        // Input/cursor/builder destruction and the actual pool turn have
        // completed. Publish this physical fact before guard notification:
        // observers of ContextHeld must already see the producer's exit, and
        // a synchronous callback unwind cannot skip it. Fixed control backing
        // remains covered by the metadata lease until its actual owner drops.
        self.exited.store(true, Ordering::Release);
        drop(producer);
        self.authority.observable().notify_observers();
    }
}
fn io_error(message: &'static str) -> FragmentIoError {
    FragmentIoError::new(
        FragmentIoOperation::ResultWrite,
        FragmentIoErrorKind::Unavailable,
        message,
    )
}
