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

//! The node-shared RHS owner of one exact Task's membership node.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use novarocks_execution_contract::{SafeDetail, TaskFailure, TaskFailureCategory};
use novarocks_types::SlotId;

use crate::exec::chunk::{Chunk, ChunkSchemaRef, arc_allocation_bytes};
use crate::exec::expr::agg::{AggregateRetainedCharge, AggregateVec};
use crate::exec::expr::json_in_pair::{JsonPairError, JsonPairTask};
use crate::exec::pipeline::dependency::{DependencyHandle, DependencyManager};
use crate::runtime::observable::Observable;
use crate::runtime::runtime_state::RuntimeState;

/// One retained RHS chunk, with its accounting owner, and the ordinal of its
/// build value column.
pub(super) struct RhsBatch {
    chunk: Chunk,
    column: usize,
}

impl RhsBatch {
    pub(super) fn values(&self) -> Result<&StringArray, String> {
        self.chunk
            .columns()
            .get(self.column)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| "membership RHS batch lost its Utf8 JSON carrier".to_string())
    }
}

/// The complete RHS of one exact Task's membership node: every frozen sender
/// and the local build reached EOS. Immutable once published; the last
/// `Arc` owner releases every retained chunk and its charge.
pub(super) struct CompleteRhs {
    batches: AggregateVec<RhsBatch>,
    rows: usize,
    /// Admits this value's own `Arc` allocation before it is made.
    _allocation: AggregateRetainedCharge,
}

impl CompleteRhs {
    pub(super) fn batches(&self) -> &[RhsBatch] {
        &self.batches
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows == 0
    }
}

enum Phase {
    Building,
    Complete(Arc<CompleteRhs>),
    /// The build failed; the typed cause when the task already published one.
    Failed(Option<TaskFailure>),
    Stopped,
    /// Every probe left; nothing retains the RHS any longer.
    Released,
}

struct Inner {
    phase: Phase,
    task: Option<JsonPairTask>,
    batches: Option<AggregateVec<RhsBatch>>,
    rows: usize,
    closed_probes: usize,
}

/// Why a probe cannot read the RHS.
pub(super) enum RhsUnavailable {
    Failed(Option<TaskFailure>),
    Stopped,
    Contract(&'static str),
}

/// Per exact Task and node, the one owner of the RHS that a single local
/// build driver fills and every local probe driver reads.
///
/// The phase leaves `Building` exactly once. `Complete` is published only by
/// the build driver's normal finish, which runs after every frozen sender and
/// the local build input reached EOS; failure and cancellation publish their
/// own terminal phase and never become `Complete`. Completion, cancellation
/// and a failure with the task's typed cause ready the build dependency and
/// wake every waiting probe, which then reports that same cause. A failure
/// whose cause the task has not yet published leaves waiting probes parked:
/// the fragment publishes the original error and wakes every blocked driver,
/// so no probe can race a derived message ahead of it.
pub(crate) struct MembershipShared {
    node_id: i32,
    build_slot: SlotId,
    probe_drivers: usize,
    dependency: DependencyHandle,
    consumers_left: Arc<Observable>,
    consumers_gone: AtomicBool,
    inner: Mutex<Inner>,
}

impl MembershipShared {
    pub(crate) fn try_new(
        node_id: i32,
        build_slot: SlotId,
        build_schema: &ChunkSchemaRef,
        probe_drivers: usize,
        manager: &DependencyManager,
    ) -> Result<Arc<Self>, String> {
        match build_schema.slot(build_slot).map(|slot| slot.data_type()) {
            Some(DataType::Utf8) => {}
            other => {
                return Err(format!(
                    "membership node {node_id} build slot {build_slot} is not the Utf8 JSON carrier: {other:?}"
                ));
            }
        }
        if probe_drivers == 0 {
            return Err(format!(
                "membership node {node_id} has no local probe driver"
            ));
        }
        Ok(Arc::new(Self {
            node_id,
            build_slot,
            probe_drivers,
            dependency: manager.get_or_create(format!("membership_rhs:{node_id}")),
            consumers_left: Arc::new(Observable::new()),
            consumers_gone: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                phase: Phase::Building,
                task: None,
                batches: None,
                rows: 0,
                closed_probes: 0,
            }),
        }))
    }

    pub(super) fn node_id(&self) -> i32 {
        self.node_id
    }

    pub(super) fn dependency(&self) -> &DependencyHandle {
        &self.dependency
    }

    pub(super) fn consumers_left(&self) -> &Arc<Observable> {
        &self.consumers_left
    }

    /// Whether every local probe already left, so the RHS has no reader.
    pub(super) fn consumers_gone(&self) -> bool {
        self.consumers_gone.load(Ordering::Acquire)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Binds one driver's operator to the exact Task. Every driver of this
    /// node must bind the same task memory owner.
    pub(super) fn bind(&self, state: &RuntimeState) -> Result<JsonPairTask, String> {
        let task = JsonPairTask::try_bind(state).map_err(|error| pair_error_text(&error))?;
        let mut inner = self.lock();
        match &inner.task {
            Some(bound) if !bound.same_owner(&task) => Err(format!(
                "membership node {} crossed task memory owners",
                self.node_id
            )),
            Some(_) => Ok(task),
            None => {
                inner.task = Some(task.clone());
                Ok(task)
            }
        }
    }

    /// Moves one whole RHS chunk into the fallible index and transfers its
    /// existing lease to the exact Task tracker, both checked before use.
    pub(super) fn ingest(&self, task: &JsonPairTask, mut chunk: Chunk) -> Result<(), String> {
        let mut inner = self.lock();
        match inner.phase {
            Phase::Building => {}
            Phase::Released => return Ok(()),
            Phase::Complete(_) | Phase::Failed(_) | Phase::Stopped => {
                return Err(format!(
                    "membership node {} received RHS input after its build was adjudicated",
                    self.node_id
                ));
            }
        }
        if self.consumers_gone() || chunk.is_empty() {
            return Ok(());
        }
        let column = chunk
            .chunk_schema()
            .index_of(self.build_slot)
            .filter(|column| {
                chunk.columns()[*column]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .is_some()
            })
            .ok_or_else(|| {
                format!(
                    "membership node {} RHS chunk lacks its Utf8 JSON slot {}",
                    self.node_id, self.build_slot
                )
            })?;
        let rows = chunk.len();
        let batches = inner
            .batches
            .get_or_insert_with(|| AggregateVec::new_in(task.allocator().clone()));
        batches
            .try_reserve(1)
            .map_err(|_| pair_error_text(&task.allocation_error()))?;
        if chunk.try_transfer_to(task.tracker()).is_err() {
            return Err(pair_error_text(&task.allocation_error()));
        }
        batches.push(RhsBatch { chunk, column });
        inner.rows = inner.rows.saturating_add(rows);
        Ok(())
    }

    /// Publishes the complete RHS after the local build's normal EOS. A
    /// failed or stopped build keeps its terminal phase.
    pub(super) fn complete(&self, task: &JsonPairTask) -> Result<(), String> {
        let mut inner = self.lock();
        if !matches!(inner.phase, Phase::Building) {
            return Ok(());
        }
        let batches = inner
            .batches
            .take()
            .unwrap_or_else(|| AggregateVec::new_in(task.allocator().clone()));
        let allocation = match admit(
            task,
            arc_allocation_bytes::<CompleteRhs>(),
            "membership RHS",
        ) {
            Ok(allocation) => allocation,
            Err(_) => {
                let failure = task.task_failure().unwrap_or_else(|| {
                    TaskFailure::new(
                        TaskFailureCategory::ResourceExhausted,
                        SafeDetail::truncating("membership RHS publication was refused"),
                    )
                });
                let message = failure.to_string();
                inner.phase = Phase::Failed(Some(failure));
                drop(batches);
                drop(inner);
                self.dependency.set_ready();
                return Err(message);
            }
        };
        let complete = Arc::new(CompleteRhs {
            batches,
            rows: inner.rows,
            _allocation: allocation,
        });
        inner.phase = if inner.closed_probes >= self.probe_drivers {
            Phase::Released
        } else {
            Phase::Complete(complete)
        };
        drop(inner);
        self.dependency.set_ready();
        Ok(())
    }

    /// Publishes the build driver's failure. With the task's typed cause,
    /// waiting probes wake and report it; without one they stay parked until
    /// the fragment publishes the original error.
    pub(super) fn fail(&self, failure: Option<TaskFailure>) {
        let mut inner = self.lock();
        if matches!(inner.phase, Phase::Building) {
            let wake = failure.is_some();
            inner.phase = Phase::Failed(failure);
            let batches = inner.batches.take();
            drop(inner);
            drop(batches);
            if wake {
                self.dependency.set_ready();
            }
        }
    }

    /// Publishes cancellation of a build that never completed.
    pub(super) fn stop(&self) {
        let mut inner = self.lock();
        if matches!(inner.phase, Phase::Building) {
            inner.phase = Phase::Stopped;
            let batches = inner.batches.take();
            drop(inner);
            drop(batches);
            self.dependency.set_ready();
        }
    }

    /// The complete RHS for a probe whose build dependency is ready.
    pub(super) fn rhs(&self) -> Result<Arc<CompleteRhs>, RhsUnavailable> {
        match &self.lock().phase {
            Phase::Complete(rhs) => Ok(Arc::clone(rhs)),
            Phase::Failed(failure) => Err(RhsUnavailable::Failed(failure.clone())),
            Phase::Stopped => Err(RhsUnavailable::Stopped),
            Phase::Building => Err(RhsUnavailable::Contract(
                "membership probe read its RHS before the build was adjudicated",
            )),
            Phase::Released => Err(RhsUnavailable::Contract(
                "membership probe read its RHS after every probe left",
            )),
        }
    }

    /// One local probe left for good. When the last one leaves, the shared
    /// reference to the RHS is released and the build may finish early.
    pub(super) fn close_probe(&self) {
        let mut inner = self.lock();
        inner.closed_probes = inner.closed_probes.saturating_add(1);
        if inner.closed_probes < self.probe_drivers {
            return;
        }
        let released = match std::mem::replace(&mut inner.phase, Phase::Released) {
            Phase::Complete(rhs) => Some(rhs),
            Phase::Building => None,
            other => {
                inner.phase = other;
                None
            }
        };
        let batches = inner.batches.take();
        drop(inner);
        drop(released);
        drop(batches);
        self.consumers_gone.store(true, Ordering::Release);
        self.consumers_left.notify_observers();
    }
}

/// Admits `bytes` on the exact Task tracker, before the allocations they
/// cover, as a charge held until the returned owner drops.
pub(super) fn admit(
    task: &JsonPairTask,
    bytes: usize,
    what: &str,
) -> Result<AggregateRetainedCharge, String> {
    let mut charge = AggregateRetainedCharge::new(task.allocator().clone());
    let mut reservation = charge
        .reserve_operation(bytes, what)
        .map_err(|_| pair_error_text(&task.allocation_error()))?;
    charge.reconcile_under_reservation(bytes, &mut reservation)?;
    Ok(charge)
}

/// Text of a pair-owner error, keeping a typed cause's own rendering.
pub(super) fn pair_error_text(error: &JsonPairError) -> String {
    match error {
        JsonPairError::Failed(failure) => failure.to_string(),
        JsonPairError::Stopped => "JSON membership stopped with its task".to_string(),
        JsonPairError::Contract(message) => (*message).to_string(),
    }
}
