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
//! Core operator traits and blocking semantics.
//!
//! Responsibilities:
//! - Defines source/processor/sink execution contracts and blocked-reason signaling.
//! - Used by drivers to orchestrate cooperative operator execution steps.
//!
//! Key exported interfaces:
//! - Types: `BlockedReason`, `FinishWatch`, `Operator`, `ProcessorOperator`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::fragment::ExecutionResult;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::{FragmentEventSink, RootInputPermit};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;
use arrow::datatypes::DataType;
use novarocks_types::SlotId;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[path = "input_construction.rs"]
pub mod input_construction;
pub use input_construction::{
    ConstructionFailure, EncodedValueOccurrence, InputConstructionFactory, InputConstructionOwner,
};

#[derive(Clone, Debug, PartialEq, Eq)]
/// The execution engine uses cooperative scheduling.
///
/// Operators are driven by a [`PipelineDriver`](crate::exec::pipeline::driver::PipelineDriver)
/// which repeatedly tries to move data from upstream to downstream.
/// When a driver cannot make progress without blocking, it records a [`BlockedReason`]
/// and yields.
pub enum BlockedReason {
    /// Upstream currently has no data available.
    InputEmpty,
    /// Downstream cannot accept more output at the moment.
    OutputFull,
    /// Blocked on a dependency object (e.g. build-side ready).
    Dependency(DependencyHandle),
}

/// Worker-frozen deadline that may invalidate one event-blocked decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverBlockDeadline {
    at: Instant,
    token: u64,
}

impl DriverBlockDeadline {
    pub(crate) fn new(at: Instant, token: u64) -> Self {
        Self { at, token }
    }

    pub(crate) fn at(self) -> Instant {
        self.at
    }

    pub(crate) fn token(self) -> u64 {
        self.token
    }
}

/// How a driver learns that an operator's pending finish work may have ended.
///
/// An operator can only report pending finish work together with a watch, so
/// a driver never has to guess when to look again.
#[derive(Clone)]
pub enum FinishWatch {
    /// Notified whenever the pending work may have ended. The observable keeps
    /// its identity for the operator's lifetime.
    Notify(Arc<Observable>),
    /// Nothing announces the end of the pending work; the driver checks again
    /// after this interval. Only for owners without a completion event: every
    /// recheck costs a scheduling turn.
    RecheckAfter(Duration),
}

impl std::fmt::Debug for FinishWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Notify(observable) => f
                .debug_tuple("Notify")
                .field(&observable.generation())
                .finish(),
            Self::RecheckAfter(interval) => f.debug_tuple("RecheckAfter").field(interval).finish(),
        }
    }
}

/// Base operator contract implemented by source/processor/sink operator implementations.
pub trait Operator: Send {
    fn name(&self) -> &str;

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let _ = tracker;
    }

    /// The preparation owner installs construction observations before any
    /// source or processor can create independently retained typed storage.
    fn set_construction_factory(&mut self, _factory: Arc<dyn InputConstructionFactory>) {}

    fn creates_typed_storage(&self) -> bool {
        false
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        let _ = profiles;
    }

    /// An exact frozen schema construction site, independent of funding.
    /// The preparation caller may qualify its real body with original scope.
    fn compiled_schema_preparation_site(
        &self,
    ) -> Option<(
        &Arc<novarocks_local_program::LocalProgram>,
        crate::runtime::preparation_metadata::CompiledSchemaSite,
    )> {
        None
    }

    #[cfg(any(test, novarocks_dev_controls))]
    fn prepared_schema_for_test(&self) -> Option<crate::exec::chunk::ChunkSchemaRef> {
        None
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        Ok(())
    }

    fn bind_runtime_state(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }

    /// Start task-owned activity after the driver has been admitted for execution.
    /// Preparation and binding must leave providers and subscriptions dormant.
    fn activate(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }

    fn set_fragment_event_sink(&mut self, _sink: Arc<dyn FragmentEventSink>) {}

    fn close(&mut self) -> ExecutionResult<()> {
        Ok(())
    }

    fn cancel(&mut self) {
        // Default: nothing to cancel.
    }

    /// Handles a driver execution failure before the operator is closed.
    ///
    /// The default preserves the existing cleanup behavior for operators that do not distinguish
    /// execution failure from cancellation.
    fn on_driver_failure(&mut self) {
        self.cancel();
    }

    fn is_finished(&self) -> bool {
        false
    }

    /// Asynchronous work this operator still owns after its driver reached a
    /// terminal state, as the way to learn that the work may have ended.
    ///
    /// While this is `Some`, the driver cannot complete: it parks on the watch
    /// and asks again when the watch fires.
    fn pending_finish(&self) -> Option<FinishWatch> {
        None
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        None
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        None
    }
}

/// What an operator's unfinished `set_finishing` is waiting for.
///
/// The driver asks this one question for two different decisions, and the two
/// need different answers, which is why this is not a boolean:
///
/// * whether the operator may be latched as finished with `set_finishing` —
///   only [`Self::Complete`] may;
/// * whether giving it another turn right now would accomplish anything —
///   only [`Self::OwedOutput`] would.
///
/// Collapsing the two deadlocks the driver in one direction or spins it in the
/// other. Reporting owed output as "cannot progress" parks a driver forever
/// with the payload still in hand — measured on a real 1FE+3BE cluster as a
/// distributed `SELECT` whose one row-carrying producer never sent its end of
/// stream. Reporting a wait for an external event as "can progress" turns the
/// park into a hot spin over an obstacle the operator cannot clear.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum FinishingWait {
    /// Finishing is done for this operator; the driver may latch it.
    #[default]
    Complete,
    /// Output the operator still owes and can push on its next turn: a payload
    /// it has buffered, or an end-of-stream marker it has not sent. The driver
    /// must give it that turn.
    OwedOutput,
    /// An event only something else can produce — an exchange sink's outbound
    /// edge has not been granted send permission, for one. Another turn now
    /// would do nothing, so the driver parks on the operator's sink observable
    /// and asks again when it fires.
    ExternalEvent,
}

impl FinishingWait {
    /// Whether finishing is still unfinished, whatever it is waiting for.
    pub const fn is_pending(self) -> bool {
        !matches!(self, Self::Complete)
    }

    /// Whether another turn right now would move this operator forward.
    pub const fn can_progress(self) -> bool {
        matches!(self, Self::OwedOutput)
    }
}

/// Extended operator contract for processor stages with push/pull semantics.
pub enum RootPreparedPull {
    Chunk(Chunk),
    /// A bounded CPU quantum completed. Workspace and its original grant
    /// remain on the same driver edge while the driver yields Ready for its
    /// next turn. No external readiness event is required to resume.
    Yielded,
    /// No root-covered workspace remains after this pull.
    Empty,
}

pub trait ProcessorOperator: Operator {
    fn need_input(&self) -> bool;

    /// Admission before the upstream owner can materialize its next output.
    /// Ordinary operators use readiness; bounded root sinks retain one exact
    /// input/hydration overlap position through the final edge and producer.
    fn prepare_upstream_pull(&self) -> ExecutionResult<bool> {
        Ok(self.need_input())
    }

    /// An empty or failed pull must return an unused root position. A
    /// successful pull keeps it while the chunk is retained on this edge.
    fn finish_upstream_pull(&self, _produced_chunk: bool) {}

    /// Move the original terminal admission onto its exact driver edge.
    /// No upstream allocation may run while a sink's permit lock is held.
    fn take_prepared_root_input(&mut self) -> Option<RootInputPermit> {
        None
    }

    /// Borrow the unique original grant while producing the final batch.
    /// Only a root-owned materializer constructs its output under this grant
    /// and can yield without returning the coverage. An ordinary upstream
    /// operator's output belongs to that operator's own admitted memory owner;
    /// the root permit is never extended to cover it. The root boundary
    /// instead reserves one input allowance before this pull and checks the
    /// actual backing against it after the pull, refusing the input explicitly
    /// when it does not fit.
    fn pull_chunk_with_root_input(
        &mut self,
        state: &RuntimeState,
        _input: &RootInputPermit,
    ) -> ExecutionResult<RootPreparedPull> {
        self.pull_chunk(state).map(|chunk| match chunk {
            Some(chunk) => RootPreparedPull::Chunk(chunk),
            None => RootPreparedPull::Empty,
        })
    }

    /// Synchronously destroy only root-covered unpublished workspace before
    /// its driver edge can return the permit. This API starts no async work.
    fn release_root_pull_workspace(&mut self) {}

    fn can_accept_root_input(
        &self,
        chunk: &Chunk,
        _input: &RootInputPermit,
    ) -> ExecutionResult<bool> {
        self.can_accept_input(chunk)
    }

    fn push_chunk_with_root_input(
        &mut self,
        _state: &RuntimeState,
        chunk: Chunk,
        input: RootInputPermit,
    ) -> ExecutionResult<()> {
        drop(chunk);
        drop(input);
        Err("processor cannot consume an original root input grant"
            .to_string()
            .into())
    }

    /// A host-owned asynchronous terminal processor can fail while its source is
    /// parked. The driver observes that failure before deciding completion;
    /// the processor also supplies its stable early-finish wake observable.
    fn execution_error(&self) -> Option<crate::runtime::fragment::ExecutionFailure> {
        None
    }

    /// Non-blocking admission for the exact chunk currently retained on the
    /// upstream edge.
    ///
    /// Most operators use their ordinary `need_input` decision. A bounded
    /// terminal sink may reserve byte credit here before the driver transfers
    /// ownership to `push_chunk`.
    fn can_accept_input(&self, _chunk: &Chunk) -> ExecutionResult<bool> {
        Ok(self.need_input())
    }

    /// Transfers the exact original Chunk to a precovered host input owner.
    /// Such a processor owns finite carrier validation, hydration and memory
    /// accounting itself; the driver must not inspect or copy its schema,
    /// expand dictionaries, or create another accounting charge on this edge.
    fn takes_original_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool;

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()>;

    fn pull_chunk(&mut self, state: &RuntimeState) -> ExecutionResult<Option<Chunk>>;

    fn set_finishing(&mut self, state: &RuntimeState) -> ExecutionResult<()>;

    /// What `set_finishing` could not complete, and whether another turn
    /// would complete it.
    ///
    /// Default [`FinishingWait::Complete`]: for almost every operator,
    /// finishing completes in the one call the driver makes. An operator that
    /// can be legitimately unable to finish yet reports which of the two
    /// reasons it is, because the driver acts on them differently — see
    /// [`FinishingWait`].
    fn finishing_wait(&self) -> FinishingWait {
        FinishingWait::Complete
    }

    /// Whether this operator can consume the given column in its current
    /// physical encoding WITHOUT hydration. Default: false - the driver
    /// hydrates every encoded column before `push_chunk`. C1-C4 override
    /// this per slot to keep specific dictionary columns encoded on their
    /// fast path.
    fn accepts_encoded_column(&self, _slot_id: SlotId, _data_type: &DataType) -> bool {
        false
    }

    /// Actual installed consumer capability for this structural occurrence.
    /// The existing root capability covers only the empty path. A consumer
    /// that handles nested encodings overrides this method explicitly.
    fn accepts_encoded_path(&self, value: EncodedValueOccurrence<'_>) -> bool {
        value.path.is_empty() && self.accepts_encoded_column(value.slot, value.carrier)
    }

    /// Actual preparation-created input construction owner, independent of
    /// encoded-kernel capability. Typed construction refuses a missing owner.
    fn input_construction_factory(&self) -> Option<&dyn InputConstructionFactory> {
        None
    }

    /// Dependency that must be ready before the operator can make progress.
    /// This is used for build-side readiness (join, runtime filters, etc.).
    fn precondition_dependency(&self) -> Option<DependencyHandle> {
        None
    }

    /// Observable for source-side readiness (has_output becomes true).
    ///
    /// The observable identity is part of the operator's scheduling contract:
    /// while the operator is not finished, every `Some` returned for this
    /// readiness direction must be the same `Arc` identity. Composite operators
    /// must therefore expose an operator-owned observable and forward every
    /// underlying readiness transition to it. A callback used for forwarding
    /// must retain the composite observable weakly so an underlying observable
    /// cannot extend the composite operator's lifetime.
    fn source_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    /// A new scheduling turn of this operator's driver begins. Work bounded
    /// per turn, such as a stream's CPU budget, is reset here, once per turn
    /// however often the operator is polled within it.
    fn begin_turn(&mut self) {}

    /// Called on a pipeline source when its latest output stays on its
    /// output edge because the downstream cannot accept it (`paused`), and
    /// again when the downstream took it. The driver stops pulling the
    /// source meanwhile.
    fn on_downstream_backpressure(&mut self, _paused: bool) {}

    /// Arm or return the deadline for the current confirmed source-idle block.
    /// Called only by the driver worker after `has_output` returned false.
    fn source_block_deadline(&self) -> Option<DriverBlockDeadline> {
        None
    }

    /// Observable for sink-side readiness (need_input becomes true).
    ///
    /// The same lifetime-stable identity and weak-forwarding requirements as
    /// [`ProcessorOperator::source_observable`] apply to this direction.
    fn sink_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    /// Deadline at which a confirmed sink-side block must be rechecked even
    /// without a notification, e.g. a bounded wait for runtime filters.
    /// Called only by the driver worker after `need_input` returned false.
    fn sink_block_deadline(&self) -> Option<DriverBlockDeadline> {
        None
    }

    /// Observable notified when this operator becomes finished without a turn
    /// of its own driver, e.g. a sink whose consumers all left.
    ///
    /// The driver watches the terminal operator's observable while it is
    /// parked for any other reason, so an early finish ends the pipeline
    /// instead of waiting for its source to produce again.
    fn early_finish_observable(&self) -> Option<Arc<Observable>> {
        None
    }
}

/// Forwards readiness transitions without retaining the target observable.
///
/// Composite operators use this once during construction to keep their public
/// observable identity stable while covering every child readiness source.
pub(crate) fn forward_observable(source: &Arc<Observable>, target: &Arc<Observable>) {
    if Arc::ptr_eq(source, target) {
        return;
    }
    let target = Arc::downgrade(target);
    source.add_observer(Arc::new(move || {
        if let Some(target) = target.upgrade() {
            target.notify_observers();
        }
    }));
}

/// Hydrate a chunk for delivery to `downstream`, keeping only the columns the
/// operator declares it can consume encoded. Default operators accept nothing
/// encoded, so this hydrates everything (C0 behavior).
pub(crate) fn hydrate_for_downstream(
    chunk: &Chunk,
    downstream: &dyn ProcessorOperator,
) -> ExecutionResult<Chunk> {
    hydrate_for_downstream_with_factory(chunk, downstream, downstream.input_construction_factory())
}

pub(crate) fn hydrate_for_downstream_with_factory(
    chunk: &Chunk,
    downstream: &dyn ProcessorOperator,
    factory: Option<&dyn InputConstructionFactory>,
) -> ExecutionResult<Chunk> {
    if chunk.chunk_schema().is_typed() {
        let mut accepts =
            |slot, occurrence, path: &[crate::exec::chunk::ArrayPathStep], carrier: &DataType| {
                downstream.accepts_encoded_path(EncodedValueOccurrence {
                    slot,
                    occurrence,
                    path,
                    carrier,
                })
            };
        if !crate::exec::chunk::dictionary_hydration_required_observed::<ConstructionFailure, _, _>(
            chunk,
            &mut accepts,
            |_, path, _| match factory {
                Some(factory) => factory.observe_input_work(1 + path.len()),
                None => Ok(()),
            },
        )? {
            return Ok(chunk.clone());
        }
        let factory = factory.ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_input_construction(chunk)?;
        return crate::exec::chunk::hydrate_dictionary_columns_owned_observed(
            chunk,
            owner.storage_lifetime(),
            accepts,
            |occurrence, path, step| owner.observe(occurrence, path, step),
            |layout| owner.request(layout),
        )
        .map_err(Into::into);
    }
    // Explicit expansion adapter for producers not yet emitting typed Chunks.
    crate::exec::chunk::hydrate_dictionary_columns_except(chunk, |slot_id, dt| {
        downstream.accepts_encoded_column(slot_id, dt)
    })
    .map_err(Into::into)
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct DictionaryCarrierStats {
    pub input_rows: i64,
    pub input_columns: i64,
    pub kept_rows: i64,
    pub kept_columns: i64,
    pub hydrated_rows: i64,
    pub hydrated_columns: i64,
    pub unsupported_columns: i64,
}

impl DictionaryCarrierStats {
    pub fn has_input(self) -> bool {
        self.input_columns > 0
    }
}

pub(crate) fn dictionary_carrier_stats(
    chunk: &Chunk,
    downstream: &dyn ProcessorOperator,
) -> DictionaryCarrierStats {
    let rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
    let mut stats = DictionaryCarrierStats::default();

    for (occurrence, slot) in chunk.chunk_schema().slots().iter().enumerate() {
        let data_type = slot.data_type();
        if !matches!(data_type, arrow::datatypes::DataType::Dictionary(_, _)) {
            continue;
        }

        stats.input_columns = stats.input_columns.saturating_add(1);
        stats.input_rows = stats.input_rows.saturating_add(rows);

        if downstream.accepts_encoded_path(EncodedValueOccurrence {
            slot: slot.slot_id(),
            occurrence,
            path: &[],
            carrier: data_type,
        }) {
            stats.kept_columns = stats.kept_columns.saturating_add(1);
            stats.kept_rows = stats.kept_rows.saturating_add(rows);
        } else {
            stats.hydrated_columns = stats.hydrated_columns.saturating_add(1);
            stats.hydrated_rows = stats.hydrated_rows.saturating_add(rows);
            stats.unsupported_columns = stats.unsupported_columns.saturating_add(1);
        }
    }

    stats
}

#[cfg(test)]
mod typed_input_construction_tests {
    use super::*;
    use crate::exec::chunk::{ArrayNormalizationStep, ArrayPathStep, ChunkSchema, ChunkSlotSchema};
    use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Owner {
        storage: ValueTypeStorageLifetime,
        requests: Arc<AtomicUsize>,
        inspected: Arc<AtomicUsize>,
        reject_requests: bool,
    }
    impl InputConstructionOwner for Owner {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            self.storage.clone()
        }
        fn request(&self, _: std::alloc::Layout) -> Result<(), ConstructionFailure> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            if self.reject_requests {
                Err(ConstructionFailure::Caller(Box::new(
                    novarocks_functions::KernelFailure::ResourceExhausted.into(),
                )))
            } else {
                Ok(())
            }
        }
        fn observe_opaque_upper_bound(&self, _: usize) -> Result<(), ConstructionFailure> {
            self.inspected.fetch_add(1, Ordering::SeqCst);
            if self.reject_requests {
                Err(ConstructionFailure::Caller(Box::new(
                    novarocks_functions::KernelFailure::ResourceExhausted.into(),
                )))
            } else {
                Ok(())
            }
        }
        fn observe(
            &self,
            _: usize,
            _: &[ArrayPathStep],
            _: ArrayNormalizationStep<'_>,
        ) -> Result<(), ConstructionFailure> {
            self.inspected.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct Factory {
        drops: Arc<AtomicUsize>,
        begins: AtomicUsize,
        factory_requests: AtomicUsize,
        requests: Arc<AtomicUsize>,
        inspected: Arc<AtomicUsize>,
        reject_requests: bool,
        control_error: Option<novarocks_type_contract::CompileControlError>,
    }
    impl InputConstructionFactory for Factory {
        fn observe_input_work(&self, _: usize) -> Result<(), ConstructionFailure> {
            match self.control_error {
                Some(error) => Err(error.into()),
                None => Ok(()),
            }
        }
        fn begin_compiled_layout_construction(
            &self,
            layout: &novarocks_local_program::StaticLayout,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            if !layout.is_typed() {
                return Err(ConstructionFailure::InvalidSource(
                    "fixture requires a typed layout".into(),
                ));
            }
            self.begin_fixture_scope()
        }
        fn begin_typed_construction(
            &self,
            _: &ChunkSchema,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            self.begin_fixture_scope()
        }
    }
    impl Factory {
        fn begin_fixture_scope(
            &self,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            self.begins.fetch_add(1, Ordering::SeqCst);
            let _hold_request = novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<Hold>(),
            )
            .map_err(|error| {
                ConstructionFailure::InvalidSource(format!("test factory hold request: {error:?}"))
            })?;
            self.factory_requests.fetch_add(1, Ordering::SeqCst);
            let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(self.drops.clone())));
            let _scope_request = std::alloc::Layout::new::<Owner>();
            self.factory_requests.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Owner {
                storage,
                requests: self.requests.clone(),
                inspected: self.inspected.clone(),
                reject_requests: self.reject_requests,
            }))
        }
    }
    struct Consumer {
        factory: Option<Factory>,
        nested: bool,
    }
    impl Operator for Consumer {
        fn name(&self) -> &str {
            "typed-consumer"
        }
    }
    impl ProcessorOperator for Consumer {
        fn need_input(&self) -> bool {
            true
        }
        fn has_output(&self) -> bool {
            false
        }
        fn push_chunk(&mut self, _: &RuntimeState, _: Chunk) -> ExecutionResult<()> {
            Ok(())
        }
        fn pull_chunk(&mut self, _: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(None)
        }
        fn set_finishing(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            Ok(())
        }
        fn accepts_encoded_column(&self, slot: SlotId, _: &DataType) -> bool {
            slot == SlotId::new(1)
        }
        fn accepts_encoded_path(&self, value: EncodedValueOccurrence<'_>) -> bool {
            if value.path.is_empty() {
                self.accepts_encoded_column(value.slot, value.carrier)
            } else {
                self.nested
                    && value.occurrence == 0
                    && value.path == [ArrayPathStep::StructField(0)]
            }
        }
        fn input_construction_factory(&self) -> Option<&dyn InputConstructionFactory> {
            self.factory
                .as_ref()
                .map(|factory| factory as &dyn InputConstructionFactory)
        }
    }
    fn source() -> Chunk {
        let pure = ValueTypeStorageLifetime::pure();
        let dictionary: ArrayRef = Arc::new(
            vec![Some("{}"), None]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let sibling: ArrayRef = Arc::new(Int32Array::from(vec![7, 8]));
        let fields = vec![
            Arc::new(Field::new("payload", dictionary.data_type().clone(), true)),
            Arc::new(Field::new("row", DataType::Int32, false)),
        ];
        let array: ArrayRef =
            Arc::new(StructArray::try_new(fields.into(), vec![dictionary, sibling], None).unwrap());
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            pure.clone(),
        )
        .unwrap();
        let row =
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, pure.clone()).unwrap();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![
                    ValueField::new("payload", json, pure.clone()),
                    ValueField::new("row", row, pure.clone()),
                ]
                .into_boxed_slice(),
            ),
            false,
            pure.clone(),
        )
        .unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(array.data_type(), pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("output", array.data_type().clone(), false)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            Arc::new(ChunkSchema::try_new_typed(vec![slot], pure.clone()).unwrap()),
            vec![array],
            2,
            pure,
            |_| Ok(()),
        )
        .unwrap()
    }
    fn consumer(drops: &Arc<AtomicUsize>, nested: bool, reject_requests: bool) -> Consumer {
        Consumer {
            factory: Some(Factory {
                drops: drops.clone(),
                begins: AtomicUsize::new(0),
                factory_requests: AtomicUsize::new(0),
                requests: Arc::new(AtomicUsize::new(0)),
                inspected: Arc::new(AtomicUsize::new(0)),
                reject_requests,
                control_error: None,
            }),
            nested,
        }
    }
    #[test]
    fn typed_consumer_requires_an_installed_construction_owner() {
        let source = source();
        let op = Consumer {
            factory: None,
            nested: false,
        };
        assert!(
            matches!(hydrate_for_downstream(&source, &op), Err(error) if matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)))
        );
    }
    #[test]
    fn typed_delivery_uses_nested_capability_and_keeps_actual_owner_until_drop() {
        let source = source();
        let drops = Arc::new(AtomicUsize::new(0));
        let op = consumer(&drops, false, false);
        let result = hydrate_for_downstream(&source, &op).unwrap();
        let array = result.columns()[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(array.column(0).data_type(), &DataType::Utf8);
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "{}"
        );
        let original = source.columns()[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(Arc::ptr_eq(array.column(1), original.column(1)));
        assert!(
            result.chunk_schema().slots()[0]
                .typed_binding()
                .same_semantics(source.chunk_schema().slots()[0].typed_binding())
        );
        assert!(op.factory.as_ref().unwrap().requests.load(Ordering::SeqCst) > 0);
        assert!(
            op.factory
                .as_ref()
                .unwrap()
                .inspected
                .load(Ordering::SeqCst)
                > 0
        );
        drop(op);
        drop(source);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(result);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn typed_delivery_preserves_only_the_exact_nested_capability() {
        let source = source();
        let drops = Arc::new(AtomicUsize::new(0));
        let op = consumer(&drops, true, false);
        let result = hydrate_for_downstream(&source, &op).unwrap();
        assert!(Arc::ptr_eq(&source.columns()[0], &result.columns()[0]));
        assert_eq!(
            op.factory.as_ref().unwrap().begins.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            op.factory
                .as_ref()
                .unwrap()
                .factory_requests
                .load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            op.factory.as_ref().unwrap().requests.load(Ordering::SeqCst),
            0
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn typed_delivery_refuses_the_first_scaffold_request_without_replacing_source() {
        let source = source();
        let original = source.columns()[0].clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let op = consumer(&drops, false, true);
        assert!(
            matches!(hydrate_for_downstream(&source, &op), Err(error) if matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Kernel(novarocks_functions::KernelFailure::ResourceExhausted)))
        );
        assert_eq!(
            op.factory.as_ref().unwrap().requests.load(Ordering::SeqCst),
            1
        );
        assert!(Arc::ptr_eq(&original, &source.columns()[0]));
    }
    #[test]
    fn each_actual_input_construction_has_an_independent_retained_scope() {
        let source = source();
        let drops = Arc::new(AtomicUsize::new(0));
        let op = consumer(&drops, false, false);
        let first = hydrate_for_downstream(&source, &op).unwrap();
        let second = hydrate_for_downstream(&source, &op).unwrap();
        assert_eq!(
            op.factory.as_ref().unwrap().begins.load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            op.factory
                .as_ref()
                .unwrap()
                .factory_requests
                .load(Ordering::SeqCst),
            4
        );
        drop(op);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(first);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(second.len(), 2);
        drop(second);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn demand_probe_preserves_control_refusal_before_creating_a_scope() {
        use novarocks_type_contract::CompileControlError;
        let source = source();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let drops = Arc::new(AtomicUsize::new(0));
            let mut op = consumer(&drops, false, false);
            op.factory.as_mut().unwrap().control_error = Some(cause);
            assert!(
                matches!(hydrate_for_downstream(&source, &op), Err(error) if matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Construction(ConstructionFailure::Control(actual)) if *actual == cause))
            );
            assert_eq!(
                op.factory.as_ref().unwrap().begins.load(Ordering::SeqCst),
                0
            );
            assert_eq!(drops.load(Ordering::SeqCst), 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::fragment::ExecutionResult;
    use std::sync::Arc;

    use super::{
        Chunk, Operator, ProcessorOperator, dictionary_carrier_stats, forward_observable,
        hydrate_for_downstream,
    };
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::pipeline::schedule::observer::Observable;
    use crate::runtime::runtime_state::RuntimeState;
    use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    struct StubOp;

    impl Operator for StubOp {
        fn name(&self) -> &str {
            "stub"
        }
    }

    impl ProcessorOperator for StubOp {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            Ok(())
        }
    }

    struct KeepSlot1Op;

    impl Operator for KeepSlot1Op {
        fn name(&self) -> &str {
            "keep-slot-1"
        }
    }

    impl ProcessorOperator for KeepSlot1Op {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            Ok(())
        }

        fn accepts_encoded_column(&self, slot_id: SlotId, data_type: &DataType) -> bool {
            slot_id == SlotId::new(1)
                && matches!(
                    data_type,
                    DataType::Dictionary(key_type, value_type)
                        if key_type.as_ref() == &DataType::Int32
                            && value_type.as_ref() == &DataType::Utf8
                )
        }
    }

    fn dict_utf8_with_nulls_and_empty() -> ArrayRef {
        Arc::new(
            vec![Some("PAID"), None, Some(""), Some("NEW")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn two_dictionary_column_chunk() -> Chunk {
        let slot1 = SlotId::new(1);
        let slot2 = SlotId::new(2);
        let column1 = dict_utf8_with_nulls_and_empty();
        let column2 = dict_utf8_with_nulls_and_empty();
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    slot1,
                    Field::new("status_1", column1.data_type().clone(), true),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    slot2,
                    Field::new("status_2", column2.data_type().clone(), true),
                    None,
                    None,
                ),
            ])
            .expect("chunk schema"),
        );
        let batch = RecordBatch::try_new(chunk_schema.arrow_schema_ref(), vec![column1, column2])
            .expect("record batch");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    fn assert_utf8_values(column: &ArrayRef) {
        let values = column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
    }

    #[test]
    fn processor_operator_rejects_physical_encodings_by_default() {
        let op = StubOp;
        let slot_id = SlotId::new(1);
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        assert!(!op.accepts_encoded_column(slot_id, &dictionary_type));
        assert!(!op.accepts_encoded_column(slot_id, &DataType::Utf8));
    }

    #[test]
    fn hydrate_for_downstream_hydrates_all_dictionary_columns_by_default() {
        let chunk = two_dictionary_column_chunk();
        let op = StubOp;

        let hydrated = hydrate_for_downstream(&chunk, &op).expect("hydrate for downstream");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        assert_eq!(hydrated.columns()[1].data_type(), &DataType::Utf8);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(1))
                .expect("slot 1")
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(2))
                .expect("slot 2")
                .data_type(),
            &DataType::Utf8
        );
        assert_utf8_values(&hydrated.columns()[0]);
        assert_utf8_values(&hydrated.columns()[1]);
    }

    #[test]
    fn hydrate_for_downstream_keeps_only_declared_encoded_slots() {
        let chunk = two_dictionary_column_chunk();
        let op = KeepSlot1Op;

        let hydrated = hydrate_for_downstream(&chunk, &op).expect("hydrate for downstream");

        assert!(matches!(
            hydrated.columns()[0].data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(1))
                .expect("slot 1")
                .data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        assert_eq!(hydrated.columns()[1].data_type(), &DataType::Utf8);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(2))
                .expect("slot 2")
                .data_type(),
            &DataType::Utf8
        );
        assert_utf8_values(&hydrated.columns()[1]);
    }

    #[test]
    fn root_encoded_capability_never_authorizes_a_nested_occurrence() {
        use super::EncodedValueOccurrence;
        use crate::exec::chunk::ArrayPathStep;
        let operator = KeepSlot1Op;
        let carrier = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert!(operator.accepts_encoded_path(EncodedValueOccurrence {
            slot: SlotId::new(1),
            occurrence: 0,
            path: &[],
            carrier: &carrier,
        }));
        assert!(!operator.accepts_encoded_path(EncodedValueOccurrence {
            slot: SlotId::new(1),
            occurrence: 0,
            path: &[ArrayPathStep::StructField(0)],
            carrier: &carrier,
        }));
    }

    #[test]
    fn dictionary_carrier_stats_hydrates_all_dictionary_columns_by_default() {
        let chunk = two_dictionary_column_chunk();
        let op = StubOp;

        let stats = dictionary_carrier_stats(&chunk, &op);

        assert_eq!(stats.input_rows, 8);
        assert_eq!(stats.input_columns, 2);
        assert_eq!(stats.kept_rows, 0);
        assert_eq!(stats.kept_columns, 0);
        assert_eq!(stats.hydrated_rows, 8);
        assert_eq!(stats.hydrated_columns, 2);
        assert_eq!(stats.unsupported_columns, 2);
    }

    #[test]
    fn dictionary_carrier_stats_keeps_declared_encoded_slot() {
        let chunk = two_dictionary_column_chunk();
        let op = KeepSlot1Op;

        let stats = dictionary_carrier_stats(&chunk, &op);

        assert_eq!(stats.input_rows, 8);
        assert_eq!(stats.input_columns, 2);
        assert_eq!(stats.kept_rows, 4);
        assert_eq!(stats.kept_columns, 1);
        assert_eq!(stats.hydrated_rows, 4);
        assert_eq!(stats.hydrated_columns, 1);
        assert_eq!(stats.unsupported_columns, 1);
    }

    #[test]
    fn observable_forwarding_does_not_retain_the_target() {
        let source = Arc::new(Observable::new());
        let target = Arc::new(Observable::new());
        let target_weak = Arc::downgrade(&target);
        forward_observable(&source, &target);

        drop(target);
        source.notify_observers();

        assert!(target_weak.upgrade().is_none());
    }
}
