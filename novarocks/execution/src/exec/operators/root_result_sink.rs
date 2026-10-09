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

//! The last upstream pull owns its complete input overlap before allocation.
//! Drivers transfer an opaque grant and Chunk; a host's finite producer owns
//! carrier validation/hydration/rendering. No driver does those operations.

use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::datatypes::DataType;
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_types::SlotId;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{FinishWatch, Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::io::{
    ResultAbort, RootInputAdmission, RootInputPermit, RootProducerState, RootResultSession,
};
use crate::runtime::observable::Observable;
use crate::runtime::runtime_state::RuntimeState;

const NAME: &str = "BOUNDED_ROOT_RESULT_SINK";

pub struct RootResultSinkFactory {
    session: Arc<dyn RootResultSession>,
    remaining: Arc<AtomicI32>,
    maximum_dop: i32,
    actual_dop: OnceLock<i32>,
    created: AtomicU64,
}
impl RootResultSinkFactory {
    pub fn try_new(session: Arc<dyn RootResultSession>, dop: i32) -> Result<Self, String> {
        if dop <= 0 || dop as u64 > NativeResultSupportGeometry::V1.root_maximum_root_drivers {
            return Err("bounded root sink DOP is outside its frozen profile".to_string());
        }
        Ok(Self {
            session,
            remaining: Arc::new(AtomicI32::new(0)),
            maximum_dop: dop,
            actual_dop: OnceLock::new(),
            created: AtomicU64::new(0),
        })
    }
    /// The host covers the finite operators, callback registrations, shared
    /// counter, and simultaneous readiness snapshot capacity before creating
    /// the factory. Static names do not allocate per-driver strings.
    pub fn metadata_capacity_bytes(dop: usize) -> Result<usize, String> {
        if dop == 0 || dop as u64 > NativeResultSupportGeometry::V1.root_maximum_root_drivers {
            return Err("bounded root metadata DOP is outside its frozen profile".to_string());
        }
        let per_driver = std::mem::size_of::<RootResultSink>()
            + std::mem::size_of::<Option<(usize, RootInputPermit)>>()
            // Driver sink/finish callbacks and their Arc registrations,
            // driver sink/finish forwarding, live slots and notify snapshots.
            + 32 * std::mem::size_of::<usize>();
        dop.checked_mul(per_driver)
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<Self>() + 4 * std::mem::size_of::<usize>())
            })
            .ok_or_else(|| "bounded root metadata capacity overflow".to_string())
    }
}
impl OperatorFactory for RootResultSinkFactory {
    fn name(&self) -> &str {
        NAME
    }
    fn is_sink(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> Result<(), String> {
        if dop <= 0 || dop > self.maximum_dop {
            return Err("built bounded root DOP exceeds its precovered profile".to_string());
        }
        let actual = *self.actual_dop.get_or_init(|| {
            self.remaining.store(dop, Ordering::Release);
            dop
        });
        if actual != dop {
            return Err("bounded root factory was rebound to a different pipeline DOP".to_string());
        }
        Ok(())
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let valid = self.actual_dop.get() == Some(&dop) && driver_id >= 0 && driver_id < dop;
        let valid = valid && {
            let bit = 1u64 << driver_id;
            self.created.fetch_or(bit, Ordering::AcqRel) & bit == 0
        };
        Box::new(RootResultSink {
            session: Arc::clone(&self.session),
            remaining: Arc::clone(&self.remaining),
            input: Mutex::new(None),
            finished: false,
            cancelled: false,
            valid_dop: valid,
            blocked_at: Mutex::new(None),
        })
    }
}

// Establish physical Drop order before invoking any fallible host code.
struct PreparedRootInput {
    chunk: Chunk,
    permit: RootInputPermit,
}

struct RootResultSink {
    session: Arc<dyn RootResultSession>,
    remaining: Arc<AtomicI32>,
    input: Mutex<Option<RootInputPermit>>,
    blocked_at: Mutex<Option<u64>>,
    finished: bool,
    cancelled: bool,
    valid_dop: bool,
}
impl RootResultSink {
    fn release_unused_input(&self) {
        let input = self.input.lock().unwrap().take();
        *self.blocked_at.lock().unwrap() = None;
        // Returning a permit can synchronously notify readiness observers.
        drop(input);
    }
    fn check_state(&self) -> Result<(), String> {
        if !self.valid_dop {
            return Err("bounded root factory and prepared driver DOP disagree".to_string());
        }
        if let RootProducerState::Failed(error) = self.session.producer_state() {
            return Err(error);
        }
        Ok(())
    }
}
impl Drop for RootResultSink {
    fn drop(&mut self) {
        self.release_unused_input();
        if !self.finished && !self.cancelled {
            self.session.abort(ResultAbort::NeverStarted);
        }
    }
}
impl Operator for RootResultSink {
    fn name(&self) -> &str {
        NAME
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn activate(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.check_state()
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn cancel(&mut self) {
        if !self.cancelled {
            self.cancelled = true;
            self.release_unused_input();
            self.session
                .abort(ResultAbort::Cancelled("root driver cancelled".to_string()));
        }
    }
    fn on_driver_failure(&mut self) {
        self.cancel();
    }
    fn pending_finish(&self) -> Option<FinishWatch> {
        ((self.finished || self.cancelled)
            && (!self.session.producer_exited()
                || (!self.cancelled
                    && !matches!(
                        self.session.producer_state(),
                        RootProducerState::ContextHeld | RootProducerState::Failed(_)
                    ))))
        .then(|| FinishWatch::Notify(self.session.writable_observable()))
    }
}
impl ProcessorOperator for RootResultSink {
    fn need_input(&self) -> bool {
        !self.finished
            && !self.cancelled
            && matches!(self.session.producer_state(), RootProducerState::Accepting)
            && self.blocked_at.lock().unwrap().is_none_or(|generation| {
                self.session.writable_observable().generation() != generation
            })
    }
    fn prepare_upstream_pull(&self) -> Result<bool, String> {
        self.check_state()?;
        if !self.need_input() {
            return Ok(false);
        }
        let mut input = self.input.lock().unwrap();
        if input.is_some() {
            return Ok(true);
        }
        let generation = self.session.writable_observable().generation();
        match self
            .session
            .try_acquire_input()
            .map_err(|error| error.to_string())?
        {
            RootInputAdmission::Granted(permit) => {
                *self.blocked_at.lock().unwrap() = None;
                *input = Some(permit);
                Ok(true)
            }
            RootInputAdmission::Blocked => {
                *self.blocked_at.lock().unwrap() = Some(generation);
                Ok(false)
            }
        }
    }
    fn finish_upstream_pull(&self, produced_chunk: bool) {
        if !produced_chunk {
            self.release_unused_input();
        }
    }
    fn take_prepared_root_input(&mut self) -> Option<RootInputPermit> {
        self.input.lock().unwrap().take()
    }
    fn can_accept_root_input(
        &self,
        _chunk: &Chunk,
        input: &RootInputPermit,
    ) -> Result<bool, String> {
        self.check_state()?;
        if input.task() != self.session.spec().task {
            return Err("root edge input grant belongs to a different task".to_string());
        }
        Ok(!self.finished && !self.cancelled)
    }
    fn push_chunk_with_root_input(
        &mut self,
        _state: &RuntimeState,
        chunk: Chunk,
        input: RootInputPermit,
    ) -> Result<(), String> {
        let input = PreparedRootInput {
            chunk,
            permit: input,
        };
        self.check_state()?;
        if self.finished || self.cancelled {
            return Err("bounded root input reached a closed driver".to_string());
        }
        self.session
            .submit_input(input.chunk, input.permit)
            .map_err(|error| error.to_string())
    }
    fn can_accept_input(&self, _chunk: &Chunk) -> Result<bool, String> {
        self.check_state()?;
        if self.finished || self.cancelled {
            return Ok(false);
        }
        if self.input.lock().unwrap().is_none() {
            return Err(
                "bounded root input reached its edge without pre-pull coverage".to_string(),
            );
        }
        Ok(true)
    }
    fn takes_original_input(&self) -> bool {
        true
    }
    fn has_output(&self) -> bool {
        false
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        self.check_state()?;
        if self.finished || self.cancelled {
            return Err("bounded root input reached a closed driver".to_string());
        }
        let permit = self
            .input
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| "bounded root push has no original pre-pull grant".to_string())?;
        self.session
            .submit_input(chunk, permit)
            .map_err(|error| error.to_string())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.check_state()?;
        if self.finished {
            return Ok(());
        }
        self.release_unused_input();
        self.finished = true;
        let old = self.remaining.fetch_sub(1, Ordering::AcqRel);
        if old <= 0 {
            return Err("bounded root driver finish count underflow".to_string());
        }
        if old == 1 {
            self.session
                .finish_input()
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    fn accepts_encoded_column(&self, _slot: SlotId, _data_type: &DataType) -> bool {
        // The host receives the original carrier under its exact overlap
        // grant. Any accepted hydration is host-owned bounded work; the
        // driver must never expand it synchronously before submission.
        true
    }
    fn sink_observable(&self) -> Option<Arc<Observable>> {
        Some(self.session.writable_observable())
    }
    fn early_finish_observable(&self) -> Option<Arc<Observable>> {
        Some(self.session.writable_observable())
    }
    fn execution_error(&self) -> Option<String> {
        self.check_state().err()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::runtime::fragment::io::{
        FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, ResultWriteCredit,
        RootInputAuthority, RootResultWriteSpec,
    };
    use arrow::array::Int32Array;
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Duration;

    struct Session {
        spec: RootResultWriteSpec,
        authority: RootInputAuthority,
        input: Mutex<Option<(Chunk, RootInputPermit)>>,
        state: Mutex<RootProducerState>,
        exited: AtomicBool,
        finishes: AtomicUsize,
        held_bytes: Arc<AtomicUsize>,
        input_backing: Arc<Mutex<Option<std::sync::Weak<Int32Array>>>>,
        released_live_backing: Arc<AtomicBool>,
        panic_in_state: AtomicBool,
    }
    impl Session {
        fn new() -> Arc<Self> {
            let spec = RootResultWriteSpec {
                task: TaskIdentity::new(
                    QueryExecutionId::new(QueryId::new(51, 52), AttemptId::new(1).unwrap())
                        .unwrap(),
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                contract: Arc::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::CountOnly,
                )),
            };
            Arc::new(Self {
                authority: RootInputAuthority::new(&spec),
                spec,
                input: Mutex::new(None),
                state: Mutex::new(RootProducerState::Accepting),
                exited: AtomicBool::new(false),
                finishes: AtomicUsize::new(0),
                held_bytes: Arc::new(AtomicUsize::new(0)),
                input_backing: Arc::new(Mutex::new(None)),
                released_live_backing: Arc::new(AtomicBool::new(false)),
                panic_in_state: AtomicBool::new(false),
            })
        }
        fn complete_input(&self) {
            let input = self.input.lock().unwrap().take();
            drop(input);
        }
        fn fail(&self) {
            *self.state.lock().unwrap() =
                RootProducerState::Failed("root producer failed".to_string());
            self.writable_observable().notify_observers();
        }
        fn actual_exit(&self) {
            self.complete_input();
            let mut state = self.state.lock().unwrap();
            if !matches!(*state, RootProducerState::Failed(_)) {
                *state = RootProducerState::ContextHeld;
            }
            drop(state);
            self.exited.store(true, Ordering::Release);
            self.writable_observable().notify_observers();
        }
    }
    impl RootResultSession for Session {
        fn spec(&self) -> &RootResultWriteSpec {
            &self.spec
        }
        fn try_acquire_input(&self) -> Result<RootInputAdmission, FragmentIoError> {
            let bytes = self.authority.required_bytes();
            self.held_bytes.fetch_add(bytes, Ordering::SeqCst);
            let held = Arc::clone(&self.held_bytes);
            let backing = Arc::clone(&self.input_backing);
            let released_live = Arc::clone(&self.released_live_backing);
            self.authority
                .try_acquire(ResultWriteCredit::new(bytes, move |released| {
                    if backing
                        .lock()
                        .unwrap()
                        .as_ref()
                        .is_some_and(|weak| weak.upgrade().is_some())
                    {
                        released_live.store(true, Ordering::SeqCst);
                    }
                    held.fetch_sub(released, Ordering::SeqCst);
                }))
        }
        fn writable_observable(&self) -> Arc<Observable> {
            self.authority.observable()
        }
        fn submit_input(
            &self,
            chunk: Chunk,
            permit: RootInputPermit,
        ) -> Result<(), FragmentIoError> {
            if !self.authority.owns(&permit) {
                return Err(FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::InvalidResponse,
                    "foreign root input grant",
                ));
            }
            let mut input = self.input.lock().unwrap();
            assert!(input.is_none());
            *input = Some((chunk, permit));
            Ok(())
        }
        fn finish_input(&self) -> Result<(), FragmentIoError> {
            self.finishes.fetch_add(1, Ordering::SeqCst);
            *self.state.lock().unwrap() = RootProducerState::Finishing;
            Ok(())
        }
        fn producer_state(&self) -> RootProducerState {
            assert!(
                !self.panic_in_state.load(Ordering::SeqCst),
                "host state panic"
            );
            self.state.lock().unwrap().clone()
        }
        fn producer_exited(&self) -> bool {
            self.exited.load(Ordering::Acquire)
        }
        fn abort(&self, _reason: ResultAbort) {
            self.authority.close();
        }
    }
    fn factory(session: &Arc<Session>, dop: i32) -> RootResultSinkFactory {
        let factory =
            RootResultSinkFactory::try_new(Arc::clone(session) as Arc<dyn RootResultSession>, dop)
                .unwrap();
        factory.bind_pipeline_dop(dop).unwrap();
        factory
    }
    #[test]
    fn drivers_share_one_pre_pull_position_through_actual_input_exit() {
        let session = Session::new();
        let factory = factory(&session, 2);
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        assert!(
            first
                .as_processor_ref()
                .unwrap()
                .prepare_upstream_pull()
                .unwrap()
        );
        assert!(
            !second
                .as_processor_ref()
                .unwrap()
                .prepare_upstream_pull()
                .unwrap()
        );
        assert_eq!(
            session.held_bytes.load(Ordering::SeqCst),
            session.authority.required_bytes()
        );
        first
            .as_processor_ref()
            .unwrap()
            .finish_upstream_pull(false);
        assert!(
            second
                .as_processor_ref()
                .unwrap()
                .prepare_upstream_pull()
                .unwrap()
        );
        second
            .as_processor_mut()
            .unwrap()
            .push_chunk(&RuntimeState::default(), Chunk::default())
            .unwrap();
        assert!(
            !first
                .as_processor_ref()
                .unwrap()
                .prepare_upstream_pull()
                .unwrap(),
            "submitted input still owns its unique position"
        );
        session.complete_input();
        assert!(
            first
                .as_processor_ref()
                .unwrap()
                .prepare_upstream_pull()
                .unwrap()
        );
        first
            .as_processor_ref()
            .unwrap()
            .finish_upstream_pull(false);
        first
            .as_processor_mut()
            .unwrap()
            .set_finishing(&RuntimeState::default())
            .unwrap();
        assert_eq!(session.finishes.load(Ordering::SeqCst), 0);
        second
            .as_processor_mut()
            .unwrap()
            .set_finishing(&RuntimeState::default())
            .unwrap();
        assert_eq!(session.finishes.load(Ordering::SeqCst), 1);
        assert!(first.pending_finish().is_some());
        session.actual_exit();
        assert!(first.pending_finish().is_none());
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn root_push_cannot_acquire_a_grant_after_materialization() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut sink = factory.create(1, 0);
        assert!(
            sink.as_processor_ref()
                .unwrap()
                .can_accept_input(&Chunk::default())
                .is_err()
        );
        assert!(
            sink.as_processor_mut()
                .unwrap()
                .push_chunk(&RuntimeState::default(), Chunk::default())
                .is_err()
        );
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn actual_sixty_four_drivers_finish_once_without_input_multiplication() {
        let session = Session::new();
        let factory = factory(&session, 64);
        let mut sinks: Vec<_> = (0..64).map(|id| factory.create(64, id)).collect();
        for sink in &sinks {
            assert!(
                sink.pending_finish().is_none(),
                "an idle producer must not park a live driver"
            );
        }
        assert!(
            sinks[0]
                .as_processor_ref()
                .unwrap()
                .prepare_upstream_pull()
                .unwrap()
        );
        for sink in &sinks[1..] {
            assert!(
                !sink
                    .as_processor_ref()
                    .unwrap()
                    .prepare_upstream_pull()
                    .unwrap()
            );
        }
        for (index, sink) in sinks.iter_mut().enumerate() {
            sink.as_processor_mut()
                .unwrap()
                .set_finishing(&RuntimeState::default())
                .unwrap();
            assert_eq!(
                session.finishes.load(Ordering::SeqCst),
                usize::from(index == 63)
            );
        }
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
        session.actual_exit();
        assert!(sinks.iter().all(|sink| sink.pending_finish().is_none()));
    }
    #[test]
    fn cancellation_and_logical_failure_keep_pending_finish_until_actual_exit() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut sink = factory.create(1, 0);
        assert!(
            sink.as_processor_ref()
                .unwrap()
                .prepare_upstream_pull()
                .unwrap()
        );
        sink.as_processor_mut()
            .unwrap()
            .push_chunk(&RuntimeState::default(), Chunk::default())
            .unwrap();
        session.fail();
        assert_eq!(
            sink.as_processor_ref()
                .unwrap()
                .execution_error()
                .as_deref(),
            Some("root producer failed")
        );
        sink.cancel();
        assert!(sink.pending_finish().is_some());
        assert!(
            session.held_bytes.load(Ordering::SeqCst) > 0,
            "logical failure did not drop the host input"
        );
        session.actual_exit();
        assert!(sink.pending_finish().is_none());
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn driver_identity_and_frozen_dop_have_no_fallback() {
        let session = Session::new();
        assert!(
            RootResultSinkFactory::try_new(Arc::clone(&session) as Arc<dyn RootResultSession>, 0)
                .is_err()
        );
        assert!(RootResultSinkFactory::metadata_capacity_bytes(65).is_err());
        let factory = factory(&session, 1);
        let first = factory.create(1, 0);
        assert!(
            first
                .as_processor_ref()
                .unwrap()
                .execution_error()
                .is_none()
        );
        let repeated = factory.create(1, 0);
        assert!(
            repeated
                .as_processor_ref()
                .unwrap()
                .execution_error()
                .is_some()
        );
        let mismatch = factory.create(2, 1);
        assert!(
            mismatch
                .as_processor_ref()
                .unwrap()
                .execution_error()
                .is_some()
        );
    }
    #[test]
    fn built_root_dop_can_collapse_within_its_precovered_upper_bound() {
        let session = Session::new();
        let factory =
            RootResultSinkFactory::try_new(Arc::clone(&session) as Arc<dyn RootResultSession>, 64)
                .unwrap();
        let unbound = factory.create(1, 0);
        assert!(
            unbound
                .as_processor_ref()
                .unwrap()
                .execution_error()
                .is_some()
        );
        factory.bind_pipeline_dop(1).unwrap();
        assert!(factory.bind_pipeline_dop(2).is_err());
        let mut sink = factory.create(1, 0);
        sink.as_processor_mut()
            .unwrap()
            .set_finishing(&RuntimeState::default())
            .unwrap();
        assert_eq!(session.finishes.load(Ordering::SeqCst), 1);
        session.actual_exit();
    }
    #[test]
    fn producer_exit_alone_cannot_replace_end_and_context_handoff() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut sink = factory.create(1, 0);
        sink.as_processor_mut()
            .unwrap()
            .set_finishing(&RuntimeState::default())
            .unwrap();
        session.exited.store(true, Ordering::Release);
        assert!(
            sink.pending_finish().is_some(),
            "End and protected context handoff are still missing"
        );
        *session.state.lock().unwrap() = RootProducerState::ContextHeld;
        assert!(sink.pending_finish().is_none());
    }

    struct Source {
        session: Arc<Session>,
        pulls: Arc<AtomicUsize>,
        produced: bool,
        empty: bool,
        fail_in_pull: bool,
    }
    impl Operator for Source {
        fn name(&self) -> &str {
            "PRE_PULL_PROOF_SOURCE"
        }
        fn is_finished(&self) -> bool {
            self.produced
        }
        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }
    }
    impl ProcessorOperator for Source {
        fn need_input(&self) -> bool {
            false
        }
        fn has_output(&self) -> bool {
            !self.produced
        }
        fn push_chunk(&mut self, _: &RuntimeState, _: Chunk) -> Result<(), String> {
            Err("source cannot accept input".to_string())
        }
        fn pull_chunk(&mut self, _: &RuntimeState) -> Result<Option<Chunk>, String> {
            assert_eq!(
                self.session.held_bytes.load(Ordering::SeqCst),
                self.session.authority.required_bytes(),
                "coverage must exist before this owner can allocate its final output"
            );
            self.pulls.fetch_add(1, Ordering::SeqCst);
            self.produced = true;
            if self.fail_in_pull {
                let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
                *self.session.input_backing.lock().unwrap() = Some(Arc::downgrade(&array));
                let schema = Arc::new(
                    ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                        SlotId::new(1),
                        arrow::datatypes::Field::new("input", DataType::Int32, false),
                        None,
                        None,
                    )])
                    .unwrap(),
                );
                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.arrow_schema_ref(),
                    vec![array],
                )
                .unwrap();
                let chunk = Chunk::new_with_chunk_schema(batch, schema);
                self.session.fail();
                return Ok(Some(chunk));
            }
            Ok((!self.empty).then(Chunk::default))
        }
        fn set_finishing(&mut self, _: &RuntimeState) -> Result<(), String> {
            Ok(())
        }
    }
    #[test]
    fn actual_driver_last_pull_is_precovered_and_empty_pull_returns_the_grant() {
        for empty in [false, true] {
            let session = Session::new();
            let factory = factory(&session, 1);
            let pulls = Arc::new(AtomicUsize::new(0));
            let mut driver = PipelineDriver::new(
                1,
                vec![
                    Box::new(Source {
                        session: Arc::clone(&session),
                        pulls: Arc::clone(&pulls),
                        produced: false,
                        empty,
                        fail_in_pull: false,
                    }),
                    factory.create(1, 0),
                ],
                None,
                Vec::new(),
                Arc::new(RuntimeState::default()),
                None,
            );
            let mut state = driver.process(Duration::from_millis(100));
            // A None pull can make the source terminal without moving an
            // edge. The ordinary driver yields before closing that edge.
            if state == DriverState::Ready {
                state = driver.process(Duration::from_millis(100));
            }
            assert_eq!(state, DriverState::PendingFinish);
            assert_eq!(pulls.load(Ordering::SeqCst), 1);
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                if empty {
                    0
                } else {
                    session.authority.required_bytes()
                }
            );
            session.actual_exit();
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Finished
            );
        }
    }
    #[test]
    fn failed_last_pull_drops_real_edge_backing_before_returning_credit() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(Source {
                    session: Arc::clone(&session),
                    pulls: Arc::new(AtomicUsize::new(0)),
                    produced: false,
                    empty: false,
                    fail_in_pull: true,
                }),
                factory.create(1, 0),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::PendingFinish
        );
        assert!(
            !session.released_live_backing.load(Ordering::SeqCst),
            "input credit returned before the real Arrow edge owner exited"
        );
        assert!(
            session
                .input_backing
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_none()
        );
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
        session.actual_exit();
        assert!(matches!(
            driver.process(Duration::from_millis(100)),
            DriverState::Failed(_)
        ));
    }
    #[test]
    fn late_producer_failure_cannot_turn_pending_success_into_finished() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(Source {
                    session: Arc::clone(&session),
                    pulls: Arc::new(AtomicUsize::new(0)),
                    produced: false,
                    empty: false,
                    fail_in_pull: false,
                }),
                factory.create(1, 0),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::PendingFinish
        );
        session.fail();
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::PendingFinish
        );
        session.actual_exit();
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Failed("root producer failed".to_string())
        );
    }

    #[derive(Clone, Copy)]
    enum QuantumExit {
        Chunk,
        Empty,
        Error,
        Panic,
    }
    struct QuantumSource {
        session: Arc<Session>,
        pulls: Arc<AtomicUsize>,
        generation: Arc<AtomicU64>,
        workspace: Option<Arc<Int32Array>>,
        exit: QuantumExit,
        pulled_this_turn: bool,
        done: bool,
    }
    impl Operator for QuantumSource {
        fn name(&self) -> &str {
            "ROOT_QUANTUM_PROOF_SOURCE"
        }
        fn is_finished(&self) -> bool {
            self.done
        }
        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }
    }
    impl ProcessorOperator for QuantumSource {
        fn begin_turn(&mut self) {
            self.pulled_this_turn = false;
        }
        fn need_input(&self) -> bool {
            false
        }
        fn has_output(&self) -> bool {
            !self.done && !self.pulled_this_turn && self.workspace.is_none()
        }
        fn push_chunk(&mut self, _: &RuntimeState, _: Chunk) -> Result<(), String> {
            Err("source cannot accept input".into())
        }
        fn pull_chunk(&mut self, _: &RuntimeState) -> Result<Option<Chunk>, String> {
            panic!("protected source must borrow the original root grant")
        }
        fn pull_chunk_with_root_input(
            &mut self,
            _: &RuntimeState,
            input: &RootInputPermit,
        ) -> Result<crate::exec::pipeline::operator::RootPreparedPull, String> {
            use crate::exec::pipeline::operator::RootPreparedPull;
            assert_eq!(input.task(), self.session.spec.task);
            assert_eq!(
                input.retained_bytes(),
                self.session.authority.required_bytes()
            );
            assert_eq!(
                self.session.held_bytes.load(Ordering::SeqCst),
                input.retained_bytes()
            );
            let turn = self.pulls.fetch_add(1, Ordering::SeqCst);
            if turn == 0 {
                self.generation.store(input.generation(), Ordering::SeqCst);
                let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
                *self.session.input_backing.lock().unwrap() = Some(Arc::downgrade(&array));
                self.workspace = Some(array);
            }
            assert_eq!(input.generation(), self.generation.load(Ordering::SeqCst));
            self.pulled_this_turn = true;
            if turn < 2 {
                return Ok(RootPreparedPull::Yielded);
            }
            self.done = true;
            match self.exit {
                QuantumExit::Empty => Ok(RootPreparedPull::Empty),
                QuantumExit::Error => Err("protected materializer failed".into()),
                QuantumExit::Panic => panic!("protected materializer panic"),
                QuantumExit::Chunk => {
                    let schema = Arc::new(
                        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                            SlotId::new(1),
                            arrow::datatypes::Field::new("input", DataType::Int32, false),
                            None,
                            None,
                        )])
                        .unwrap(),
                    );
                    let batch = arrow::record_batch::RecordBatch::try_new(
                        schema.arrow_schema_ref(),
                        vec![self.workspace.as_ref().unwrap().clone()],
                    )
                    .unwrap();
                    // Keep construction scratch until the driver synchronously
                    // clears it before transferring the grant into the host.
                    Ok(RootPreparedPull::Chunk(Chunk::new_with_chunk_schema(
                        batch, schema,
                    )))
                }
            }
        }
        fn release_root_pull_workspace(&mut self) {
            if self.workspace.is_some() {
                assert_eq!(
                    self.session.held_bytes.load(Ordering::SeqCst),
                    self.session.authority.required_bytes()
                );
                drop(self.workspace.take());
            }
        }
        fn set_finishing(&mut self, _: &RuntimeState) -> Result<(), String> {
            Ok(())
        }
    }
    fn quantum_driver(
        session: &Arc<Session>,
        exit: QuantumExit,
    ) -> (PipelineDriver, Arc<AtomicUsize>, Arc<AtomicU64>) {
        let factory = factory(session, 1);
        let pulls = Arc::new(AtomicUsize::new(0));
        let generation = Arc::new(AtomicU64::new(0));
        let driver = PipelineDriver::new(
            1,
            vec![
                Box::new(QuantumSource {
                    session: session.clone(),
                    pulls: pulls.clone(),
                    generation: generation.clone(),
                    workspace: None,
                    exit,
                    pulled_this_turn: false,
                    done: false,
                }),
                factory.create(1, 0),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        (driver, pulls, generation)
    }
    fn assert_workspace_exited_before_credit(session: &Session) {
        assert!(
            session
                .input_backing
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_none(),
            "the actual Arrow workspace must have exited"
        );
        assert!(
            !session.released_live_backing.load(Ordering::SeqCst),
            "returning the original credit cannot precede actual backing exit"
        );
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn yielded_root_pull_keeps_the_same_original_grant_until_host_input_exit() {
        let session = Session::new();
        let (mut driver, pulls, generation) = quantum_driver(&session, QuantumExit::Chunk);
        for turn in 1..=2 {
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Ready
            );
            assert_eq!(pulls.load(Ordering::SeqCst), turn);
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                session.authority.required_bytes()
            );
            assert!(
                session
                    .input_backing
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .is_some()
            );
            assert!(session.input.lock().unwrap().is_none());
        }
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::PendingFinish
        );
        assert_eq!(pulls.load(Ordering::SeqCst), 3);
        assert_eq!(
            session
                .input
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .1
                .generation(),
            generation.load(Ordering::SeqCst)
        );
        session.complete_input();
        assert_workspace_exited_before_credit(&session);
        session.actual_exit();
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Finished
        );
    }
    #[test]
    fn empty_and_failed_quantum_drop_workspace_before_returning_original_grant() {
        for exit in [QuantumExit::Empty, QuantumExit::Error] {
            let session = Session::new();
            let (mut driver, pulls, _) = quantum_driver(&session, exit);
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Ready
            );
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Ready
            );
            let mut state = driver.process(Duration::from_millis(100));
            if state == DriverState::Ready {
                state = driver.process(Duration::from_millis(100));
            }
            assert_eq!(state, DriverState::PendingFinish);
            assert_eq!(pulls.load(Ordering::SeqCst), 3);
            assert_workspace_exited_before_credit(&session);
            session.actual_exit();
            let state = driver.process(Duration::from_millis(100));
            match exit {
                QuantumExit::Empty => assert_eq!(state, DriverState::Finished),
                QuantumExit::Error => assert!(matches!(state,
                    DriverState::Failed(error) if error.ends_with("protected materializer failed"))),
                QuantumExit::Chunk | QuantumExit::Panic => unreachable!(),
            }
        }
    }
    #[test]
    fn cancelled_and_dropped_driver_clear_yielded_workspace_before_original_grant() {
        for cancel in [true, false] {
            let session = Session::new();
            let (mut driver, _, _) = quantum_driver(&session, QuantumExit::Chunk);
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Ready
            );
            if cancel {
                assert_eq!(
                    driver.cancel_for_fragment_abort(),
                    DriverState::PendingFinish
                );
                assert_workspace_exited_before_credit(&session);
                session.actual_exit();
                assert_eq!(
                    driver.process(Duration::from_millis(100)),
                    DriverState::Canceled
                );
            } else {
                drop(driver);
                assert_workspace_exited_before_credit(&session);
            }
        }
    }

    #[test]
    fn host_state_panic_drops_original_chunk_before_edge_input_grant() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut sink = factory.create(1, 0);
        let processor = sink.as_processor_mut().unwrap();
        assert!(processor.prepare_upstream_pull().unwrap());
        let input = processor.take_prepared_root_input().unwrap();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        *session.input_backing.lock().unwrap() = Some(Arc::downgrade(&array));
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                arrow::datatypes::Field::new("input", DataType::Int32, false),
                None,
                None,
            )])
            .unwrap(),
        );
        let batch =
            arrow::record_batch::RecordBatch::try_new(schema.arrow_schema_ref(), vec![array])
                .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        session.panic_in_state.store(true, Ordering::SeqCst);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            processor.push_chunk_with_root_input(&RuntimeState::default(), chunk, input)
        }));
        assert!(result.is_err());
        assert_workspace_exited_before_credit(&session);
        session.panic_in_state.store(false, Ordering::SeqCst);
    }

    #[test]
    fn pull_panic_retains_original_grant_until_executor_failure_cleanup() {
        let session = Session::new();
        let (mut driver, _, _) = quantum_driver(&session, QuantumExit::Panic);
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Ready
        );
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Ready
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            driver.process(Duration::from_millis(100))
        }));
        assert!(result.is_err());
        assert_eq!(
            session.held_bytes.load(Ordering::SeqCst),
            session.authority.required_bytes()
        );
        assert!(
            session
                .input_backing
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_some()
        );
        assert_eq!(
            driver.fail_after_panic("protected materializer panic".into()),
            DriverState::PendingFinish
        );
        assert_workspace_exited_before_credit(&session);
        session.actual_exit();
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Failed("protected materializer panic".into())
        );
    }
}
