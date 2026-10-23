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

//! The coordinator's per-round split-assignment pump.
//!
//! The pump advances as part of `TaskRound`. Each synchronous Connector call
//! moves one source into supervised blocking-I/O work; TaskUpdate
//! submission, acknowledgement, retry delay, and source-owner bookkeeping stay
//! on the serial round without occupying a blocking worker. The guard only signals
//! stop, so cancellation and drop never join or block an OS thread.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::SplitSourceProfile;

use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::artifact::FragmentId;
use crate::query_execution::artifact::{PreparedDistributedQuery, ValidatedFragmentSchedule};
use crate::query_execution::split_assignment::{
    AssignmentTarget, RoundSplitAssignment, RoundSplitAssignmentStop, RoundSplitEnumeration,
    RoundSplitEnumerationResult, RoundSplitSource, ScanNodeKey, SplitAssignmentDriverError,
    SupervisedSplitSource, TaskUpdateTransport,
};
use crate::task_execution::blocking_io::{
    ConnectorBlockingIoBacking, ConnectorBlockingIoResponsibility, ConnectorBlockingIoSupervisor,
};
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::execution::QueryTaskExecution;
use crate::task_execution::round::{PumpCloseUnresolved, TaskRound, TurnPump};
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_query_application::coordination::TaskUpdateRetryPolicy;

/// How many splits one task may hold before the driver stops pulling for it.
const DEFAULT_MAX_QUEUED_SPLITS_PER_TASK: u64 = 4096;

/// The admitted tasks that will read each typed scan node.
///
/// Derived from the schedule rather than from placed splits: with runtime
/// assignment every instance of a fragment that owns the scan node is a
/// legitimate destination, and deriving from placement would silently exclude
/// an instance that happened to receive nothing at planning time.
pub(crate) fn assignment_targets(
    schedule: &ValidatedFragmentSchedule,
    scan_nodes: &[(FragmentId, i32)],
) -> BTreeMap<ScanNodeKey, Vec<AssignmentTarget>> {
    let placements_by_fragment = schedule.fragment_placements();
    let mut targets: BTreeMap<ScanNodeKey, Vec<AssignmentTarget>> = BTreeMap::new();
    for &(fragment_id, plan_node_id) in scan_nodes {
        let Some(placements) = placements_by_fragment.get(&fragment_id) else {
            continue;
        };
        let entry = targets
            .entry(ScanNodeKey::new(fragment_id, plan_node_id))
            .or_default();
        for placement in placements {
            entry.push(AssignmentTarget {
                backend_idx: placement.backend_idx,
                fragment_instance_id: placement.finst_id,
            });
        }
    }
    targets
}

/// Immutable, single-scan input for one attempt source-open call.
///
/// It shares only this scan's process-local access entry. The actor keeps the
/// complete prepared artifact and all previously opened sources.
pub(crate) struct RoundSplitSourceRecipe {
    fragment_id: FragmentId,
    plan_node_id: i32,
    /// The provider columns this scan reads, in the order it produces them.
    assignments: Vec<novarocks_spi::connector::read_stack::runtime::ConnectorReadAssignment>,
    /// Which runtime filter constrains which provider column.
    ///
    /// Held as resolved pairs rather than as named bindings to be matched
    /// against the assignments again: the match was already made where the
    /// names were still meaningful, and repeating it here is a second place
    /// for a rename to go wrong.
    dynamic_filters: Vec<(
        u32,
        novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
    )>,
    constraint: novarocks_spi::connector::read_stack::ConnectorReadConstraint,
    access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessEntry>,
}

impl RoundSplitSourceRecipe {
    pub(crate) fn from_artifacts(
        artifacts: &PreparedDistributedQuery,
        fragment_id: FragmentId,
        plan_node_id: i32,
    ) -> Result<Self, String> {
        let scan = artifacts
            .scan_facts(fragment_id, plan_node_id)
            .ok_or_else(|| {
                format!(
                    "typed connector scan fragment_id={fragment_id} node_id={plan_node_id} is absent from its frozen artifact"
                )
            })?;
        let access = artifacts
            .share_connector_attempt_access(fragment_id, plan_node_id)
            .ok_or_else(|| {
                format!(
                    "typed connector scan fragment_id={fragment_id} node_id={plan_node_id} has no attempt access"
                )
            })?;
        Ok(Self {
            fragment_id,
            plan_node_id,
            assignments: scan.assignments.clone(),
            dynamic_filters: scan.dynamic_filters.clone(),
            constraint: scan.constraint.clone(),
            access,
        })
    }

    pub(crate) const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) fn generation(&self) -> &novarocks_spi::connector::read_stack::ConnectorReadBinding {
        self.access.access().frozen().binding()
    }
}

/// One source-open result before the actor attaches attempt-wide feedback.
pub(crate) struct OpenedRoundSplitSource {
    scan: ScanNodeKey,
    source: Option<SupervisedSplitSource>,
    encoder: Option<Arc<dyn novarocks_spi::connector::ConnectorReadWireEncoder>>,
    feedback_bindings: Vec<(
        u32,
        novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
    )>,
    backing: Option<ConnectorBlockingIoBacking>,
}

impl OpenedRoundSplitSource {
    pub(crate) fn into_round_source(
        mut self,
        feedback: Arc<crate::runtime_filter::feedback::RuntimeFilterFeedbackState>,
    ) -> RoundSplitSource {
        RoundSplitSource {
            scan: self.scan,
            source: self
                .source
                .take()
                .expect("an opened split source is consumed exactly once"),
            encoder: self
                .encoder
                .take()
                .expect("an opened split source encoder is consumed exactly once"),
            feedback,
            feedback_bindings: std::mem::take(&mut self.feedback_bindings),
            initial_wait_initialized: false,
            initial_wait_deadline: None,
            _backing: self.backing.take(),
        }
    }
}

impl Drop for OpenedRoundSplitSource {
    fn drop(&mut self) {
        // The source's fixed original join pin is minted by its Drop before
        // encoder and feedback fields retire. No new admission is needed.
        drop(self.source.take());
    }
}

/// Open one exact scan source from frozen logical semantics and one attempt's
/// request capability.
///
/// The attempt initializer is the only source-open owner. Keeping provider
/// reacquisition and `get_splits` in this narrow operation makes the exact
/// generation, pushed constraint, and runtime-filter columns travel through
/// one managed blocking-I/O call.
pub(crate) fn open_round_split_source(
    recipe: RoundSplitSourceRecipe,
    session: &novarocks_spi::connector::read_stack::ConnectorSession,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    blocking_io: ConnectorBlockingIoSupervisor,
    responsibility: ConnectorBlockingIoResponsibility,
) -> Result<OpenedRoundSplitSource, ConnectorError> {
    let access = &recipe.access;
    let connector_context = crate::connector::context_for_planning_lease_typed(
        access.planning_lease(),
        connector_context.clone(),
    )?;
    let attempt_context =
        novarocks_spi::connector::ConnectorAttemptContext::from_admitted_request(connector_context);
    let capabilities = access
        .access()
        .for_attempt(&attempt_context, access.planning_lease())?;
    let source = capabilities
        .splits()
        .get_splits(
            session,
            capabilities.frozen(),
            &recipe.assignments,
            &recipe
                .dynamic_filters
                .iter()
                .map(|(_, column)| column.clone())
                .collect(),
            &recipe.constraint,
        )
        .map_err(|error| {
            error.map_message(|message| {
                format!(
                    "typed connector scan node_id={} cannot open its split source: {message}",
                    recipe.plan_node_id
                )
            })
        })?;
    let backing = responsibility.retain_backing();
    let source =
        SupervisedSplitSource::new(source, recipe.plan_node_id, blocking_io, responsibility);
    Ok(OpenedRoundSplitSource {
        scan: ScanNodeKey::new(recipe.fragment_id, recipe.plan_node_id),
        source: Some(source),
        encoder: Some(capabilities.encoder()),
        feedback_bindings: recipe.dynamic_filters,
        backing: Some(backing),
    })
}

/// Everything one round needs to start assigning splits, built before the
/// prepared artifacts are consumed by staging.
///
/// It already owns open connector split sources, so it closes them if the
/// round never starts — staging or Start can still fail between here and
/// there, and a source dropped without closing leaves the connector holding
/// whatever the enumeration opened.
pub(crate) struct RoundSplitAssignmentPlan {
    targets: BTreeMap<ScanNodeKey, Vec<AssignmentTarget>>,
    sources: Vec<RoundSplitSource>,
    retry_policy: TaskUpdateRetryPolicy,
    initial_dynamic_filter_wait_cap: std::time::Duration,
    blocking_io: ConnectorBlockingIoSupervisor,
}

/// Sources opened while one round is still being assembled.
///
/// Any later scan or target-validation error drops this owner before a plan
/// exists. Its drop therefore schedules exact close work through protected
/// lifecycle work instead of relying on `Box` destruction to release a
/// Connector's enumeration state.
pub(crate) struct OpenRoundSplitSources {
    sources: Vec<RoundSplitSource>,
    blocking_io: ConnectorBlockingIoSupervisor,
}

impl OpenRoundSplitSources {
    pub(crate) fn with_capacity(
        capacity: usize,
        blocking_io: ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            sources: Vec::with_capacity(capacity),
            blocking_io,
        }
    }

    pub(crate) fn push(&mut self, source: RoundSplitSource) {
        self.sources.push(source);
    }

    pub(crate) fn scans(&self) -> impl Iterator<Item = ScanNodeKey> + '_ {
        self.sources.iter().map(|source| source.scan)
    }

    pub(crate) fn into_sources(mut self) -> Vec<RoundSplitSource> {
        std::mem::take(&mut self.sources)
    }
}

impl Drop for OpenRoundSplitSources {
    fn drop(&mut self) {
        schedule_source_close(&self.blocking_io, std::mem::take(&mut self.sources));
    }
}

fn schedule_source_close(
    blocking_io: &ConnectorBlockingIoSupervisor,
    sources: Vec<RoundSplitSource>,
) {
    if sources.is_empty() {
        return;
    }
    let pins = sources
        .iter()
        .filter_map(|source| source.source.join_pin())
        .collect();
    let _ = blocking_io.spawn_pinned(pins, move || {
        for source in sources {
            RoundSplitAssignment::close_source(source);
        }
    });
}

impl RoundSplitAssignmentPlan {
    /// Everything but the delivery transport.
    ///
    /// The transport arrives at [`SplitAssignmentRoundGuard::install`] because
    /// the two are frozen at different moments: sources must be open before
    /// the attempt's credential leases are sealed, while the task substrate's
    /// delivery bridge cannot exist until the task graph does -- and the graph
    /// is built from the encoder output, which comes later.
    pub(crate) fn new(
        targets: BTreeMap<ScanNodeKey, Vec<AssignmentTarget>>,
        sources: Vec<RoundSplitSource>,
        retry_policy: TaskUpdateRetryPolicy,
        initial_dynamic_filter_wait_cap: std::time::Duration,
        blocking_io: ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            targets,
            sources,
            retry_policy,
            initial_dynamic_filter_wait_cap,
            blocking_io,
        }
    }

    /// The scans this plan opened a source for.
    #[cfg(test)]
    pub(crate) fn scans(&self) -> impl Iterator<Item = ScanNodeKey> + '_ {
        self.sources.iter().map(|source| source.scan)
    }
}

impl Drop for RoundSplitAssignmentPlan {
    fn drop(&mut self) {
        schedule_source_close(&self.blocking_io, std::mem::take(&mut self.sources));
    }
}

struct OwnerSlot<T> {
    owner_live: bool,
    result: Option<T>,
}

impl<T> OwnerSlot<T> {
    fn publish(&mut self, result: T) -> Result<(), T> {
        if self.owner_live {
            self.result = Some(result);
            Ok(())
        } else {
            Err(result)
        }
    }

    fn abandon(&mut self) -> Option<T> {
        self.owner_live = false;
        self.result.take()
    }
}

#[derive(Default)]
struct SplitUnresolved {
    enumeration: Option<Arc<crate::task_execution::blocking_io::ConnectorBlockingIoError>>,
    close: Option<Arc<crate::task_execution::blocking_io::ConnectorBlockingIoError>>,
}

struct SplitAssignmentPump {
    assignment: Option<RoundSplitAssignment>,
    stop: RoundSplitAssignmentStop,
    data_runtime: FrontendDataRuntime,
    wake: Arc<dyn StatusIntakeWake>,
    enumeration: Arc<
        Mutex<
            OwnerSlot<
                Result<
                    RoundSplitEnumerationResult,
                    crate::task_execution::blocking_io::ConnectorBlockingIoError,
                >,
            >,
        >,
    >,
    enumeration_in_flight: bool,
    retry_at: Option<Instant>,
    timer_at: Option<Instant>,
    failure: Arc<Mutex<Option<SplitAssignmentDriverError>>>,
    outcome: Arc<Mutex<Option<Result<SplitSourceProfile, SplitAssignmentDriverError>>>>,
    closing: bool,
    close_joined: Arc<AtomicBool>,
    cleanup_failure: Arc<Mutex<Option<SplitAssignmentDriverError>>>,
    cleanup_failure_reported: bool,
    unresolved: Arc<Mutex<SplitUnresolved>>,
    unresolved_reported: bool,
}

impl SplitAssignmentPump {
    const PUMP_NAME: &'static str = "split_assignment";

    fn arm_wake(&mut self, deadline: Instant) {
        if self.timer_at.is_some_and(|armed| armed <= deadline) {
            return;
        }
        self.timer_at = Some(deadline);
        let wake = Arc::clone(&self.wake);
        self.data_runtime.spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            wake.wake();
        });
    }

    fn close_enumeration_after_owner_exit(
        data_runtime: &FrontendDataRuntime,
        enumeration: RoundSplitEnumerationResult,
    ) {
        let supervisor = data_runtime.connector_blocking_io().clone();
        let pins = enumeration.source.source.join_pin().into_iter().collect();
        let _ = supervisor.spawn_pinned(pins, move || {
            // Both arbitrary batch payload destruction and original source
            // close occur outside the owner-slot mutex, under the same join.
            let source = enumeration.discard_batch();
            RoundSplitAssignment::close_source(source);
        });
    }

    fn start_close(&mut self, error: Option<SplitAssignmentDriverError>) {
        self.start_close_with_enumeration(error, None);
    }

    fn start_close_with_enumeration(
        &mut self,
        error: Option<SplitAssignmentDriverError>,
        enumeration: Option<RoundSplitEnumerationResult>,
    ) {
        let Some(mut assignment) = self.assignment.take() else {
            return;
        };
        self.closing = true;
        if let Some(error) = &error {
            let mut failure = self.failure.lock().unwrap_or_else(|lock| lock.into_inner());
            if failure.is_none() {
                *failure = Some(error.clone());
            }
        }
        let failure = Arc::clone(&self.failure);
        let outcome = Arc::clone(&self.outcome);
        let wake = Arc::clone(&self.wake);
        let mut pins = assignment.join_pins();
        if let Some(pin) = enumeration
            .as_ref()
            .and_then(|result| result.source.source.join_pin())
        {
            pins.push(pin);
        }
        let blocking_io = self.data_runtime.connector_blocking_io().clone();
        let close_joined = Arc::clone(&self.close_joined);
        let cleanup_failure = Arc::clone(&self.cleanup_failure);
        let unresolved = Arc::clone(&self.unresolved);
        let job = blocking_io.spawn_pinned(pins, move || {
            // The in-flight source and its unused provider batch stay on this
            // original job. No opaque split destructor runs on the async lane.
            let unused_batch = enumeration.map(|result| assignment.adopt_enumeration(result));
            let closed = assignment.close_with_profile();
            drop(unused_batch);
            closed
        });
        self.data_runtime.spawn(async move {
            let cleanup = match job.finish().await {
                Ok(result) => {
                    // A normal domain Err is still an actual returned job:
                    // the consumed assignment and raw payloads already dropped.
                    close_joined.store(true, Ordering::Release);
                    result
                }
                Err(worker_error) => {
                    // This exact original error/payload and its existing
                    // backing remain in the pump's one bounded health slot.
                    // It is not a Task originating cause or a normal exit.
                    let worker_error = Arc::new(worker_error);
                    let detail = worker_error.to_string();
                    unresolved
                        .lock()
                        .unwrap_or_else(|lock| lock.into_inner())
                        .close = Some(worker_error);
                    Err(SplitAssignmentDriverError::SplitSource { scan: None, detail })
                }
            };
            if let Err(error) = &cleanup {
                *cleanup_failure
                    .lock()
                    .unwrap_or_else(|lock| lock.into_inner()) = Some(error.clone());
            }
            // Cleanup may add diagnostics but cannot replace the original
            // failure that caused this exact assignment to stop.
            let result = match error {
                Some(error) => Err(error),
                None => cleanup,
            };
            if let Err(error) = &result {
                let mut failure = failure.lock().unwrap_or_else(|lock| lock.into_inner());
                if failure.is_none() {
                    *failure = Some(error.clone());
                }
            }
            *outcome.lock().unwrap_or_else(|lock| lock.into_inner()) = Some(result);
            wake.wake();
        });
    }

    fn start_enumeration(
        &mut self,
        request: crate::query_execution::split_assignment::RoundSplitEnumerationRequest,
    ) {
        self.enumeration_in_flight = true;
        let pins = request.join_pin().into_iter().collect();
        let job = self
            .data_runtime
            .connector_blocking_io()
            .spawn_pinned(pins, move || {
                RoundSplitAssignment::enumerate_source(request)
            });
        let slot = Arc::clone(&self.enumeration);
        let runtime = self.data_runtime.clone();
        let wake = Arc::clone(&self.wake);
        self.data_runtime.spawn(async move {
            let result = job.finish().await;
            let reap = {
                let mut slot = slot.lock().unwrap_or_else(|lock| lock.into_inner());
                slot.publish(result).err()
            };
            match reap {
                Some(Ok(enumeration)) => {
                    Self::close_enumeration_after_owner_exit(&runtime, enumeration)
                }
                Some(Err(error)) => runtime
                    .connector_blocking_io()
                    .retire_original_failure(error),
                None => {}
            }
            wake.wake();
        });
    }
}

impl SplitAssignmentPump {
    fn advance(&mut self) -> Result<usize, TaskExecutionError> {
        if self.closing {
            return Ok(0);
        }
        if self
            .timer_at
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            self.timer_at = None;
        }
        if self.enumeration_in_flight {
            let result = self
                .enumeration
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .result
                .take();
            let Some(result) = result else {
                return Ok(0);
            };
            self.enumeration_in_flight = false;
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    let error = Arc::new(error);
                    let detail = error.to_string();
                    self.unresolved
                        .lock()
                        .unwrap_or_else(|lock| lock.into_inner())
                        .enumeration = Some(error);
                    self.start_close(Some(SplitAssignmentDriverError::SplitSource {
                        scan: None,
                        detail,
                    }));
                    return Ok(1);
                }
            };
            if self.stop.is_stopped() {
                self.start_close_with_enumeration(None, Some(result));
                return Ok(1);
            }
            let assignment = self
                .assignment
                .as_mut()
                .expect("an enumeration result retains its assignment owner");
            let enumerated = assignment.adopt_enumeration(result);
            let enumerated = match enumerated {
                Ok(batch) => batch,
                Err(error) => {
                    self.start_close(Some(error));
                    return Ok(1);
                }
            };
            let Some((scan, batch)) = enumerated else {
                return Ok(1);
            };
            if let Err(error) = assignment.deliver(scan, batch) {
                self.start_close(Some(error));
                return Ok(1);
            }
            return Ok(1);
        }

        if self.stop.is_stopped() {
            self.start_close(None);
            return Ok(1);
        }

        let assignment = self
            .assignment
            .as_mut()
            .expect("a live split pump owns its assignment");
        if assignment.delivery_in_progress() {
            return match assignment.drive_delivery() {
                Ok(true) => Ok(1),
                Ok(false) => {
                    if let Some(deadline) = assignment.next_delivery_wake_at() {
                        self.arm_wake(deadline);
                    }
                    Ok(0)
                }
                Err(error) => {
                    self.start_close(Some(error));
                    Ok(1)
                }
            };
        }
        if self
            .retry_at
            .is_some_and(|deadline| Instant::now() < deadline)
        {
            return Ok(0);
        }
        self.retry_at = None;
        match assignment.enumerate_once() {
            Ok(RoundSplitEnumeration::Ready(request)) => {
                self.start_enumeration(*request);
                Ok(1)
            }
            Ok(RoundSplitEnumeration::Idle(wait)) => {
                let retry_at = Instant::now() + wait;
                self.retry_at = Some(retry_at);
                self.arm_wake(retry_at);
                Ok(0)
            }
            Ok(RoundSplitEnumeration::Finished) => {
                self.start_close(None);
                Ok(1)
            }
            Err(error) => {
                self.start_close(Some(error));
                Ok(1)
            }
        }
    }
}

impl TurnPump for SplitAssignmentPump {
    fn name(&self) -> &'static str {
        Self::PUMP_NAME
    }

    fn drive(&mut self, _execution: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError> {
        self.advance()
    }
    fn begin_close(&mut self) {
        self.stop.stop();
        self.wake.wake();
    }
    fn close_turn(&mut self) -> Result<usize, TaskExecutionError> {
        self.stop.stop();
        let moved = self.advance()?;
        if !self.cleanup_failure_reported {
            let failure = self
                .cleanup_failure
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .clone();
            if let Some(input) = failure
                .as_ref()
                .and_then(SplitAssignmentDriverError::execution_failure)
            {
                self.cleanup_failure_reported = true;
                return Err(TaskExecutionError::Execution(input));
            }
        }
        Ok(moved)
    }
    fn is_closed(&self) -> bool {
        self.close_joined.load(Ordering::Acquire)
            && self
                .outcome
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .is_some()
            && {
                let fault = self
                    .unresolved
                    .lock()
                    .unwrap_or_else(|lock| lock.into_inner());
                fault.enumeration.is_none() && fault.close.is_none()
            }
    }
    fn take_close_unresolved(&mut self) -> Option<PumpCloseUnresolved> {
        if self.unresolved_reported {
            return None;
        }
        let cause = {
            let fault = self
                .unresolved
                .lock()
                .unwrap_or_else(|lock| lock.into_inner());
            fault
                .enumeration
                .as_ref()
                .or(fault.close.as_ref())
                .cloned()?
        };
        self.unresolved_reported = true;
        Some(PumpCloseUnresolved {
            owner: Self::PUMP_NAME,
            cause,
        })
    }
}

impl Drop for SplitAssignmentPump {
    fn drop(&mut self) {
        self.stop.stop();
        let pending_enumeration = {
            let mut slot = self
                .enumeration
                .lock()
                .unwrap_or_else(|lock| lock.into_inner());
            slot.abandon()
        };
        match pending_enumeration {
            Some(Ok(enumeration)) => {
                Self::close_enumeration_after_owner_exit(&self.data_runtime, enumeration)
            }
            Some(Err(error)) => self
                .data_runtime
                .connector_blocking_io()
                .retire_original_failure(error),
            None => {}
        }
        if let Some(mut assignment) = self.assignment.take() {
            let supervisor = self.data_runtime.connector_blocking_io().clone();
            let pins = assignment.join_pins();
            let _ = supervisor.spawn_pinned(pins, move || assignment.close());
        }
    }
}

/// A nonblocking observation handle for the round-owned split pump.
pub(crate) struct SplitAssignmentRoundGuard {
    stop: RoundSplitAssignmentStop,
    failure: Arc<Mutex<Option<SplitAssignmentDriverError>>>,
    outcome: Arc<Mutex<Option<Result<SplitSourceProfile, SplitAssignmentDriverError>>>>,
    wake: Arc<dyn StatusIntakeWake>,
}

impl SplitAssignmentRoundGuard {
    /// Installs the pump. Returns `None` when this round has no typed scan.
    pub(crate) fn install(
        round: &mut TaskRound,
        execution_id: QueryExecutionId,
        mut plan: RoundSplitAssignmentPlan,
        transport: Arc<dyn TaskUpdateTransport>,
        data_runtime: FrontendDataRuntime,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Option<Self> {
        // Taken, not borrowed: the sources move into the round, so the plan's
        // own drop must not close what the round now owns.
        let sources = std::mem::take(&mut plan.sources);
        let tasks = std::mem::take(&mut plan.targets);
        if sources.is_empty() {
            return None;
        }
        let assignment = RoundSplitAssignment::new(
            execution_id,
            transport,
            tasks,
            DEFAULT_MAX_QUEUED_SPLITS_PER_TASK,
            sources,
            plan.retry_policy,
            plan.initial_dynamic_filter_wait_cap,
        );
        let stop = assignment.stop_handle();
        let failure = Arc::new(Mutex::new(None));
        let outcome = Arc::new(Mutex::new(None));
        round.add_pump(Box::new(SplitAssignmentPump {
            assignment: Some(assignment),
            stop: stop.clone(),
            data_runtime,
            wake: Arc::clone(&wake),
            enumeration: Arc::new(Mutex::new(OwnerSlot {
                owner_live: true,
                result: None,
            })),
            enumeration_in_flight: false,
            retry_at: None,
            timer_at: None,
            failure: Arc::clone(&failure),
            outcome: Arc::clone(&outcome),
            closing: false,
            close_joined: Arc::new(AtomicBool::new(false)),
            cleanup_failure: Arc::new(Mutex::new(None)),
            cleanup_failure_reported: false,
            unresolved: Arc::new(Mutex::new(SplitUnresolved::default())),
            unresolved_reported: false,
        }));
        Some(Self {
            stop,
            failure,
            outcome,
            wake,
        })
    }

    /// Request the original round-owned close; never run provider code here.
    pub(crate) fn request_stop(&self) {
        self.stop.stop();
        self.wake.wake();
    }

    /// Whether the pump has published its final source profile or failure.
    /// This is a business result, not the round's independent local exit proof.
    pub(crate) fn is_finished(&self) -> bool {
        self.outcome
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .is_some()
    }

    /// Why delivery stopped, if it stopped by failing.
    ///
    /// A caller may ask on every turn. A pump that finished normally answers
    /// `None`: a round
    /// whose sources are all terminal legitimately stops delivering long
    /// before its scans finish, and that is not a failure.
    ///
    /// This exists because a failed deliverer is otherwise silent. Every
    /// remaining scan then waits for splits nobody will send, the round keeps
    /// turning with nothing to fold, and the query dies on the statement
    /// deadline reporting the wait instead of the cause -- including the case
    /// ADR-0123 froze a bounded budget for, whose exhaustion is longer than a
    /// typical statement timeout and so could never be seen.
    pub(crate) fn failure(&self) -> Option<SplitAssignmentDriverError> {
        self.failure
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .clone()
    }

    /// Takes the final profile without waiting for the pump.
    ///
    /// An absent outcome is accepted only after the attempt owner explicitly
    /// abandoned residual delivery; an ordinary successful finish may never
    /// turn an unfinished assignment into a successful empty profile.
    pub(crate) fn finish_after_attempt(
        self,
        abandoned: bool,
    ) -> Result<SplitSourceProfile, SplitAssignmentDriverError> {
        let outcome = self
            .outcome
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .clone();
        match outcome {
            Some(outcome) => outcome,
            None if abandoned => Ok(SplitSourceProfile::default()),
            None => Err(SplitAssignmentDriverError::DeliveryInProgress),
        }
    }
}

impl Drop for SplitAssignmentRoundGuard {
    fn drop(&mut self) {
        self.request_stop();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use novarocks_spi::connector::ConnectorError;
    use novarocks_spi::connector::read_stack::{
        ConnectorReadDynamicFilterSnapshot, ConnectorReadSplit, ConnectorReadSplitSource,
        ConnectorSplitBatch,
    };
    use novarocks_types::{AttemptId, QueryId};

    use super::*;

    struct CloseSignalSource {
        closed: Option<mpsc::Sender<()>>,
    }

    impl ConnectorReadSplitSource for CloseSignalSource {
        fn next_batch(
            &mut self,
            _max_size: usize,
            _dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
            panic!("source-owner cleanup test must not enumerate")
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            if let Some(closed) = self.closed.take() {
                closed.send(()).expect("publish source close");
            }
            Ok(())
        }
    }

    struct CleanupTestEncoder;

    impl novarocks_spi::connector::ConnectorReadWireEncoder for CleanupTestEncoder {
        fn owner(&self) -> &str {
            "split-cleanup-test"
        }

        fn encode_relation_payload(
            &self,
            _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
        ) -> Result<
            novarocks_spi::connector::ConnectorReadRelationPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("cleanup test must not encode a relation")
        }

        fn encode_column_payload(
            &self,
            _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("cleanup test must not encode a column")
        }

        fn encode_transaction_payload(
            &self,
            _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("cleanup test must not encode a transaction")
        }

        fn encode_split_payload(
            &self,
            _split: &ConnectorReadSplit,
        ) -> Result<
            novarocks_spi::connector::ConnectorReadSplitPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("cleanup test must not encode a split")
        }
    }

    fn cleanup_test_source(closed: mpsc::Sender<()>) -> RoundSplitSource {
        let execution_id = QueryExecutionId::new(
            QueryId::new(17, 19),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id");
        RoundSplitSource {
            scan: ScanNodeKey::new(FragmentId::from(1u32), 7),
            source: SupervisedSplitSource::fixture(Box::new(CloseSignalSource {
                closed: Some(closed),
            })),
            encoder: Arc::new(CleanupTestEncoder),
            feedback: Arc::new(
                crate::runtime_filter::feedback::RuntimeFilterFeedbackState::new(
                    execution_id,
                    Default::default(),
                )
                .expect("empty feedback declaration"),
            ),
            feedback_bindings: Vec::new(),
            initial_wait_initialized: false,
            initial_wait_deadline: None,
            _backing: None,
        }
    }

    #[test]
    fn one_provider_unit_is_delivered_once_and_all_three_tasks_receive_eos() {
        use crate::query_execution::connector_domain::TaskUpdateRequest;
        use crate::query_execution::split_assignment::{
            AcceptedPlanNode, TaskUpdateOutcome, TaskUpdateTicket, TaskUpdateTransportError,
        };
        use novarocks_spi::connector as spi;
        use spi::ConnectorReadWireEncoder;
        use spi::read_stack::adapter::{ProviderReadRuntime, ReadRuntimeAdapter};
        use spi::read_stack::{ColumnHandle, ConnectorSplit};
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
        struct Column;
        impl ColumnHandle for Column {}
        #[derive(Debug)]
        struct Unit;
        impl ConnectorSplit for Unit {
            fn retained_size_in_bytes(&self) -> u64 {
                0
            }
        }
        struct Provider {
            descriptor: spi::ConnectorInstanceDescriptor,
            catalog: spi::CatalogHandle,
        }
        impl ProviderReadRuntime for Provider {
            type Table = ();
            type Column = Column;
            type Transaction = ();
            type Split = Unit;
            fn descriptor(&self) -> &spi::ConnectorInstanceDescriptor {
                &self.descriptor
            }
            fn catalog_handle(&self) -> &spi::CatalogHandle {
                &self.catalog
            }
            fn transaction(&self) -> Self::Transaction {}
        }
        struct UnitSource {
            split: Option<ConnectorReadSplit>,
            calls: Arc<AtomicUsize>,
            closes: Arc<AtomicUsize>,
        }
        impl ConnectorReadSplitSource for UnitSource {
            fn next_batch(
                &mut self,
                max_size: usize,
                _: &ConnectorReadDynamicFilterSnapshot,
            ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
                assert!(max_size > 0);
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(ConnectorSplitBatch::new(
                    self.split.take().into_iter().collect(),
                    true,
                ))
            }
            fn is_finished(&self) -> bool {
                self.split.is_none()
            }
            fn close(&mut self) -> Result<(), ConnectorError> {
                self.closes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        struct Encoder {
            payload: spi::ConnectorEncodedPayload,
        }
        impl ConnectorReadWireEncoder for Encoder {
            fn owner(&self) -> &str {
                "single-unit-delivery-test"
            }
            fn encode_relation_payload(
                &self,
                value: &spi::read_stack::ConnectorReadRelation,
            ) -> Result<spi::ConnectorReadRelationPayload, spi::ConnectorCodecError> {
                CleanupTestEncoder.encode_relation_payload(value)
            }
            fn encode_column_payload(
                &self,
                value: &spi::read_stack::ConnectorReadColumnHandle,
            ) -> Result<spi::ConnectorEncodedPayload, spi::ConnectorCodecError> {
                CleanupTestEncoder.encode_column_payload(value)
            }
            fn encode_transaction_payload(
                &self,
                value: &spi::read_stack::ConnectorReadTransactionHandle,
            ) -> Result<spi::ConnectorEncodedPayload, spi::ConnectorCodecError> {
                CleanupTestEncoder.encode_transaction_payload(value)
            }
            fn encode_split_payload(
                &self,
                _: &ConnectorReadSplit,
            ) -> Result<spi::ConnectorReadSplitPayload, spi::ConnectorCodecError> {
                Ok(spi::ConnectorReadSplitPayload::new(
                    spi::ConnectorReadSplitCategory::SystemRelation,
                    self.payload.clone(),
                ))
            }
        }
        type RecordedTaskUpdate = (usize, usize, bool, u64, i32);

        #[derive(Default)]
        struct Transport {
            requests: Mutex<Vec<RecordedTaskUpdate>>,
        }
        impl TaskUpdateTransport for Transport {
            fn begin(
                &self,
                _: QueryExecutionId,
                target: &AssignmentTarget,
                request: &TaskUpdateRequest,
            ) -> Result<TaskUpdateTicket, TaskUpdateTransportError> {
                assert_eq!(request.assignments().len(), 1);
                let a = &request.assignments()[0];
                let mut requests = self.requests.lock().unwrap();
                requests.push((
                    target.backend_idx,
                    a.splits().len(),
                    a.no_more_splits(),
                    a.splits().last().map_or(0, |split| split.sequence_id()),
                    a.plan_node_id(),
                ));
                Ok(TaskUpdateTicket((requests.len() - 1) as u64))
            }
            fn poll(
                &self,
                ticket: TaskUpdateTicket,
                _: &RoundSplitAssignmentStop,
            ) -> Option<Result<TaskUpdateOutcome, TaskUpdateTransportError>> {
                let (_, _, eos, sequence, node) = self.requests.lock().unwrap()[ticket.0 as usize];
                Some(Ok(TaskUpdateOutcome::Accepted(vec![AcceptedPlanNode {
                    plan_node_id: node,
                    accepted_through_sequence: sequence,
                    no_more_splits: eos,
                    queued_splits: 0,
                }])))
            }
        }
        let instance = spi::ConnectorInstanceId::parse("single.unit").unwrap();
        let descriptor = spi::ConnectorInstanceDescriptor {
            provider_id: spi::ConnectorProviderId::parse("single-unit-test").unwrap(),
            instance_id: instance.clone(),
        };
        let catalog = spi::CatalogHandle::new(instance, spi::CatalogVersion::from_bytes([11; 32]));
        let payload = spi::ConnectorEncodedPayload::new(
            spi::ConnectorEnvelopeHeader::new(
                descriptor.provider_id.clone(),
                catalog.clone(),
                spi::ConnectorCodecCategory::ReadSplit,
                spi::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![42u8].into(),
        );
        let adapter = ReadRuntimeAdapter::new(Arc::new(Provider {
            descriptor,
            catalog,
        }));
        let calls = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let (closed_tx, _closed_rx) = mpsc::channel();
        let mut source = cleanup_test_source(closed_tx);
        let scan = source.scan;
        source.source = SupervisedSplitSource::fixture(Box::new(UnitSource {
            split: Some(adapter.wrap_split(Unit)),
            calls: calls.clone(),
            closes: closes.clone(),
        }));
        source.encoder = Arc::new(Encoder { payload });
        let execution =
            QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(1).unwrap()).unwrap();
        let transport = Arc::new(Transport::default());
        let targets = (0..3)
            .map(|backend_idx| AssignmentTarget {
                backend_idx,
                fragment_instance_id: novarocks_types::UniqueId::new(17, 20 + backend_idx as i64),
            })
            .collect();
        let mut round = RoundSplitAssignment::new(
            execution,
            transport.clone(),
            BTreeMap::from([(scan, targets)]),
            4,
            vec![source],
            TaskUpdateRetryPolicy::default(),
            Duration::ZERO,
        );
        for _ in 0..4 {
            match round.enumerate_once().unwrap() {
                RoundSplitEnumeration::Ready(request) => {
                    let result = RoundSplitAssignment::enumerate_source(*request);
                    if let Some((key, batch)) = round.adopt_enumeration(result).unwrap() {
                        round.deliver(key, batch).unwrap();
                        break;
                    }
                }
                _ => panic!("fixed single unit must be ready"),
            }
        }
        for _ in 0..32 {
            if !round.delivery_in_progress() {
                break;
            }
            round.drive_delivery().unwrap();
        }
        assert!(!round.delivery_in_progress());
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests
                .iter()
                .map(|r| r.0)
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from([0, 1, 2])
        );
        assert_eq!(requests.iter().map(|r| r.1).sum::<usize>(), 1);
        assert_eq!(requests.iter().filter(|r| r.1 == 1).count(), 1);
        assert!(requests.iter().all(|r| r.2));
        drop(requests);
        assert!(matches!(
            round.enumerate_once().unwrap(),
            RoundSplitEnumeration::Finished
        ));
        round.close_with_profile().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    struct ObservedSource {
        closes: Arc<std::sync::atomic::AtomicUsize>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        started: Option<mpsc::Sender<()>>,
        release: Option<mpsc::Receiver<()>>,
        panic_close: bool,
        panic_profile: bool,
    }

    impl ConnectorReadSplitSource for ObservedSource {
        fn profile_snapshot(&self) -> SplitSourceProfile {
            assert!(!self.panic_profile, "injected provider profile panic");
            SplitSourceProfile::default()
        }
        fn next_batch(
            &mut self,
            _: usize,
            _: &ConnectorReadDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
            unreachable!("cleanup test does not enumerate")
        }
        fn is_finished(&self) -> bool {
            false
        }
        fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(started) = self.started.take() {
                started.send(()).expect("close entered");
            }
            if let Some(release) = self.release.take() {
                release.recv().expect("release original close");
            }
            assert!(!self.panic_close, "injected original close panic");
            Ok(())
        }
    }
    impl Drop for ObservedSource {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn owned_round_source<S: ConnectorReadSplitSource + 'static>(
        source: S,
        responsibility: ConnectorBlockingIoResponsibility,
        supervisor: ConnectorBlockingIoSupervisor,
    ) -> RoundSplitSource {
        let execution =
            QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(1).unwrap()).unwrap();
        let backing = responsibility.retain_backing();
        RoundSplitSource {
            scan: ScanNodeKey::new(FragmentId::from(1u32), 7),
            source: SupervisedSplitSource::new(Box::new(source), 7, supervisor, responsibility),
            encoder: Arc::new(CleanupTestEncoder),
            feedback: Arc::new(
                crate::runtime_filter::feedback::RuntimeFilterFeedbackState::new(
                    execution,
                    Default::default(),
                )
                .unwrap(),
            ),
            feedback_bindings: Vec::new(),
            initial_wait_initialized: false,
            initial_wait_deadline: None,
            _backing: Some(backing),
        }
    }

    #[test]
    fn unclaimed_open_preserves_work_and_window_while_original_close_is_held() {
        use crate::task_execution::blocking_io::tests::{admitted, until};
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) = admitted();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .unwrap();
        let pin = responsibility.join_pin();
        let backing = responsibility.retain_backing();
        let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (started, entered) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let source = ObservedSource {
            closes: Arc::clone(&closes),
            drops: Arc::clone(&drops),
            started: Some(started),
            release: Some(released),
            panic_close: false,
            panic_profile: false,
        };
        let close_supervisor = supervisor.clone();
        let (opened, open) = mpsc::channel();
        let job = supervisor.spawn_pinned(vec![pin], move || {
            let outcome = OpenedRoundSplitSource {
                scan: ScanNodeKey::new(FragmentId::from(1u32), 7),
                source: Some(SupervisedSplitSource::new(
                    Box::new(source),
                    7,
                    close_supervisor,
                    responsibility,
                )),
                encoder: Some(Arc::new(CleanupTestEncoder)),
                feedback_bindings: Vec::new(),
                backing: Some(backing),
            };
            opened.send(()).expect("original open returned");
            outcome
        });
        open.recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        root.owner.complete();
        root.business.release();
        drop(window);
        drop(job);
        entered
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let held = control.snapshot();
        let still_live = drops.load(std::sync::atomic::Ordering::SeqCst) == 0;
        release.send(()).unwrap();
        until(|| control.snapshot().scopes.is_empty());
        assert_eq!(held.root_responsibilities, 1);
        assert_eq!(held.result_windows.held_positions, [0, 0, 1, 0]);
        assert!(still_live);
        assert_eq!(closes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    fn source_batch_cleanup(panic_close: bool, panic_profile: bool) {
        use crate::task_execution::blocking_io::tests::{admitted, until};
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) = admitted();
        let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut opened = OpenRoundSplitSources::with_capacity(2, supervisor.clone());
        for first in [true, false] {
            let responsibility = ConnectorBlockingIoResponsibility::admit(
                &root.owner.scope(),
                &window.retain_alias(),
            )
            .unwrap();
            opened.push(owned_round_source(
                ObservedSource {
                    closes: Arc::clone(&closes),
                    drops: Arc::clone(&drops),
                    started: None,
                    release: None,
                    panic_close: first && panic_close,
                    panic_profile,
                },
                responsibility,
                supervisor.clone(),
            ));
        }
        root.owner.complete();
        root.business.release();
        drop(window);
        drop(opened);
        until(|| drops.load(std::sync::atomic::Ordering::SeqCst) == 2);
        if panic_close {
            let mut original = None;
            until(|| {
                original = supervisor.take_original_failure();
                original.is_some()
            });
            assert!(original.as_ref().unwrap().to_string().contains("panicked"));
            drop(original);
        }
        until(|| control.snapshot().scopes.is_empty());
        assert_eq!(closes.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(supervisor.take_original_failure().is_none());
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn a_panicking_batch_close_still_closes_the_remaining_original_sources() {
        source_batch_cleanup(true, false);
    }

    #[test]
    fn protected_close_does_not_depend_on_provider_profile() {
        source_batch_cleanup(false, true);
    }

    #[tokio::test]
    async fn a_round_with_no_typed_scan_starts_no_worker() {
        let plan = RoundSplitAssignmentPlan::new(
            BTreeMap::new(),
            Vec::new(),
            TaskUpdateRetryPolicy::default(),
            std::time::Duration::ZERO,
            ConnectorBlockingIoSupervisor::new(tokio::runtime::Handle::current()),
        );
        assert_eq!(plan.scans().count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partially_opened_sources_close_while_another_call_is_held() {
        let supervisor = ConnectorBlockingIoSupervisor::new(tokio::runtime::Handle::current());
        let (release, released) = mpsc::channel();
        let (started, ordinary_started) = mpsc::channel();
        let ordinary = supervisor.spawn_ordinary(move || {
            started.send(()).expect("publish ordinary start");
            released.recv().expect("release ordinary call");
        });
        ordinary_started
            .recv_timeout(Duration::from_secs(2))
            .expect("ordinary call must start");

        let (closed, observe_close) = mpsc::channel();
        let mut opened = OpenRoundSplitSources::with_capacity(1, supervisor);
        opened.push(cleanup_test_source(closed));
        drop(opened);

        observe_close
            .recv_timeout(Duration::from_secs(2))
            .expect("cleanup must progress while another call is held");
        release.send(()).expect("release ordinary call");
        ordinary.finish().await.expect("ordinary call finishes");
    }

    #[test]
    fn ordinary_finish_refuses_an_unfinished_pump() {
        let stop = RoundSplitAssignmentStop::default();
        let guard = SplitAssignmentRoundGuard {
            stop: stop.clone(),
            failure: Arc::new(Mutex::new(None)),
            outcome: Arc::new(Mutex::new(None)),
            wake: Arc::new(crate::task_execution::status_intake::CountingWake::default()),
        };

        assert!(matches!(
            guard.finish_after_attempt(false),
            Err(SplitAssignmentDriverError::DeliveryInProgress)
        ));
        assert!(
            stop.is_stopped(),
            "consuming the guard signals residual stop"
        );
    }

    #[test]
    fn a_failed_deliverer_is_readable_while_the_round_is_still_turning() {
        // What this catches: a delivery failure that is only visible to
        // `finish`, which runs after the round loop. A deliverer that stopped
        // will never feed the tasks it had left, so that loop never reaches
        // its own exit -- the query runs out its statement deadline and
        // reports what it was waiting on instead of why nobody was sending.
        // A normal stop must stay silent, because a round whose sources all
        // went terminal legitimately stops delivering long before its scans
        // finish.
        let failed = SplitAssignmentRoundGuard {
            stop: RoundSplitAssignmentStop::default(),
            failure: Arc::new(Mutex::new(Some(
                SplitAssignmentDriverError::NoAdmittedTask {
                    scan: ScanNodeKey::new(FragmentId::from(1u32), 4),
                },
            ))),
            outcome: Arc::new(Mutex::new(Some(Err(
                SplitAssignmentDriverError::NoAdmittedTask {
                    scan: ScanNodeKey::new(FragmentId::from(1u32), 4),
                },
            )))),
            wake: Arc::new(crate::task_execution::status_intake::CountingWake::default()),
        };
        let detail = failed
            .failure()
            .expect("a failed deliverer reports its own cause")
            .to_string();
        assert!(detail.contains("no admitted task"), "{detail}");
        // Reaped, not consumed: the same verdict is still the round's own
        // result afterwards.
        assert!(failed.failure().is_some());
        failed
            .finish_after_attempt(false)
            .expect_err("the joined verdict is still the round's result");

        let healthy = SplitAssignmentRoundGuard {
            stop: RoundSplitAssignmentStop::default(),
            failure: Arc::new(Mutex::new(None)),
            outcome: Arc::new(Mutex::new(Some(Ok(SplitSourceProfile::default())))),
            wake: Arc::new(crate::task_execution::status_intake::CountingWake::default()),
        };
        assert!(
            healthy.failure().is_none(),
            "a deliverer that ran out of sources has not failed"
        );
        healthy
            .finish_after_attempt(false)
            .expect("a clean stop is still a clean stop");
    }

    struct NoCloseTaskUpdates;
    impl TaskUpdateTransport for NoCloseTaskUpdates {
        fn begin(
            &self,
            _: QueryExecutionId,
            _: &AssignmentTarget,
            _: &crate::query_execution::connector_domain::TaskUpdateRequest,
        ) -> Result<
            crate::query_execution::split_assignment::TaskUpdateTicket,
            crate::query_execution::split_assignment::TaskUpdateTransportError,
        > {
            unreachable!("a closing pump must never send a normal TaskUpdate")
        }
        fn poll(
            &self,
            _: crate::query_execution::split_assignment::TaskUpdateTicket,
            _: &RoundSplitAssignmentStop,
        ) -> Option<
            Result<
                crate::query_execution::split_assignment::TaskUpdateOutcome,
                crate::query_execution::split_assignment::TaskUpdateTransportError,
            >,
        > {
            unreachable!("a closing pump must never poll normal delivery")
        }
    }
    struct CloseDomainErrorSource {
        inner: ObservedSource,
        fail: bool,
    }
    impl ConnectorReadSplitSource for CloseDomainErrorSource {
        fn profile_snapshot(&self) -> SplitSourceProfile {
            self.inner.profile_snapshot()
        }
        fn next_batch(
            &mut self,
            max: usize,
            filter: &ConnectorReadDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
            self.inner.next_batch(max, filter)
        }
        fn is_finished(&self) -> bool {
            false
        }
        fn close(&mut self) -> Result<(), ConnectorError> {
            self.inner.close()?;
            if self.fail {
                Err(ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
                    "private provider diagnostics are not safe detail",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn closing_round_joins_its_original_sources(
        terminal_cleanup: bool,
        panic_profile: bool,
        panic_close: bool,
        provider_error: bool,
    ) {
        use crate::task_execution::blocking_io::tests::{admitted, until};
        use crate::task_execution::status_intake::{CountingWake, StatusEvent};
        use novarocks_execution::task_execution::status::{
            TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion,
        };
        use novarocks_execution_contract::failure::FailureProjection;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let data_runtime = FrontendDataRuntime::new(runtime.handle().clone());
        let supervisor = data_runtime.connector_blocking_io().clone();
        let (control, root, window) = admitted();
        let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (started, entered) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let mut started = Some(started);
        let mut released = Some(released);
        let mut sources = Vec::new();
        for first in [true, false] {
            let responsibility = ConnectorBlockingIoResponsibility::admit(
                &root.owner.scope(),
                &window.retain_alias(),
            )
            .unwrap();
            sources.push(owned_round_source(
                CloseDomainErrorSource {
                    inner: ObservedSource {
                        closes: Arc::clone(&closes),
                        drops: Arc::clone(&drops),
                        started: if first { started.take() } else { None },
                        release: if first { released.take() } else { None },
                        panic_close: first && panic_close,
                        panic_profile: first && panic_profile,
                    },
                    fail: first && provider_error,
                },
                responsibility,
                supervisor.clone(),
            ));
        }
        let (mut round, statuses, _clock) =
            crate::task_execution::tests::synchronous_root_control_round();
        let identity = round.root_task();
        let plan = RoundSplitAssignmentPlan::new(
            BTreeMap::new(),
            sources,
            TaskUpdateRetryPolicy::default(),
            Duration::ZERO,
            supervisor.clone(),
        );
        let wake = Arc::new(CountingWake::default());
        let guard = SplitAssignmentRoundGuard::install(
            &mut round,
            identity.query_execution_id(),
            plan,
            Arc::new(NoCloseTaskUpdates),
            data_runtime.clone(),
            wake.clone(),
        )
        .unwrap();
        let wake_count_before_stop = wake.count();
        guard.request_stop();
        assert!(
            wake.count() > wake_count_before_stop,
            "requesting source close must wake the round owner"
        );
        if terminal_cleanup {
            round.begin_terminal_cleanup();
        } else {
            round.begin_normal_drain();
        }
        let initial_turn = round.turn();
        entered
            .recv_timeout(Duration::from_secs(2))
            .expect("the original source close entered");
        root.owner.complete();
        root.business.release();
        drop(window);
        let before = control.snapshot();
        let before_finished = guard.is_finished();
        let before_closed = round.pumps_closed();
        let before_drops = drops.load(Ordering::SeqCst);
        // Always release the real foreign close before any negative assertion.
        let released = release.send(());
        until(|| guard.is_finished());
        if !terminal_cleanup {
            statuses.publish(StatusEvent::Published(Box::new(
                TaskStatus::try_new(
                    identity,
                    TaskStatusVersion::new(3).unwrap(),
                    TaskState::Running,
                    None,
                    TaskOutputFacts::new(false),
                )
                .unwrap(),
            )));
        }
        let joined_turn = round.turn();
        let local_closed = round.pumps_closed();
        let close_error = round.take_pump_close_error();
        let second_turn = round.turn();
        let repeated_error = round.take_pump_close_error();
        let selected_cause = round.failure_cause().cloned();
        let result = guard.finish_after_attempt(false);
        let final_closes = closes.load(Ordering::SeqCst);
        let final_drops = drops.load(Ordering::SeqCst);
        drop(round);
        drop(data_runtime);
        until(|| control.snapshot().scopes.is_empty());
        control.close_admission();
        let shutdown = control.shutdown();
        assert!(released.is_ok());
        assert!(initial_turn.is_ok(), "{initial_turn:?}");
        assert!(!before_finished && !before_closed);
        assert_eq!(before_drops, 0);
        assert_eq!(before.root_responsibilities, 1);
        assert_eq!(before.result_windows.held_positions, [0, 0, 1, 0]);
        assert!(
            local_closed,
            "only the original returned close job proves local exit"
        );
        assert_eq!(
            (final_closes, final_drops),
            (2, 2),
            "both actual sources must close and drop once"
        );
        let report = joined_turn.expect("typed close errors cannot starve the serial turn");
        if !terminal_cleanup {
            assert_eq!(report.status_events, 1);
        }
        assert!(second_turn.is_ok());
        assert!(repeated_error.is_none());
        assert!(
            selected_cause.is_none(),
            "a local cleanup must not mint a Task conclusion"
        );
        if panic_profile || panic_close || provider_error {
            let error = result.expect_err("the original close error must not be hidden");
            let input = error
                .execution_failure()
                .expect("the source owner projected its own fact");
            let observed = close_error
                .expect("the serial round reports the new close error once")
                .preparation_input();
            assert_eq!(observed, input);
            assert_eq!(
                input.site(),
                novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator
            );
            if provider_error {
                let original = ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
                    "diagnostic",
                );
                assert_eq!(input.fact(), original.failure_fact());
                assert_eq!(
                    input.detail().as_str(),
                    "closing the connector split source failed"
                );
            } else {
                assert_eq!(
                    input.fact().reason().code(),
                    if panic_profile { 180 } else { 181 }
                );
            }
        } else {
            assert!(result.is_ok());
            assert!(close_error.is_none());
        }
        assert!(shutdown.is_ok());
    }

    #[test]
    fn normal_drain_closes_and_joins_the_original_gated_split_sources() {
        closing_round_joins_its_original_sources(false, false, false, false);
    }
    #[test]
    fn terminal_cleanup_closes_and_joins_the_original_gated_split_sources() {
        closing_round_joins_its_original_sources(true, false, false, false);
    }
    #[test]
    fn a_provider_close_error_keeps_its_fact_without_stopping_status_progress() {
        closing_round_joins_its_original_sources(false, false, false, true);
    }
    #[test]
    fn a_profile_panic_still_closes_and_drops_all_original_sources() {
        closing_round_joins_its_original_sources(false, true, false, false);
    }
    #[test]
    fn a_close_panic_still_closes_and_drops_all_original_sources_once() {
        closing_round_joins_its_original_sources(false, false, true, false);
    }

    struct DropPanicSource {
        inner: ObservedSource,
    }
    impl ConnectorReadSplitSource for DropPanicSource {
        fn profile_snapshot(&self) -> SplitSourceProfile {
            self.inner.profile_snapshot()
        }
        fn next_batch(
            &mut self,
            max: usize,
            filter: &ConnectorReadDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
            self.inner.next_batch(max, filter)
        }
        fn is_finished(&self) -> bool {
            false
        }
        fn close(&mut self) -> Result<(), ConnectorError> {
            self.inner.close()
        }
    }
    impl Drop for DropPanicSource {
        fn drop(&mut self) {
            panic!("original split source destruction did not exit normally");
        }
    }

    #[test]
    fn a_source_drop_panic_retains_original_backing_and_reports_health_once() {
        use crate::task_execution::blocking_io::ConnectorBlockingIoError;
        use crate::task_execution::blocking_io::tests::{admitted, until};
        use crate::task_execution::status_intake::CountingWake;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let data_runtime = FrontendDataRuntime::new(runtime.handle().clone());
        let supervisor = data_runtime.connector_blocking_io().clone();
        let (control, root, window) = admitted();
        let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .unwrap();
        let source = owned_round_source(
            DropPanicSource {
                inner: ObservedSource {
                    closes: Arc::clone(&closes),
                    drops: Arc::clone(&drops),
                    started: None,
                    release: None,
                    panic_close: false,
                    panic_profile: false,
                },
            },
            responsibility,
            supervisor.clone(),
        );
        let (mut round, _, _clock) = crate::task_execution::tests::synchronous_root_control_round();
        let plan = RoundSplitAssignmentPlan::new(
            BTreeMap::new(),
            vec![source],
            TaskUpdateRetryPolicy::default(),
            Duration::ZERO,
            supervisor.clone(),
        );
        let identity = round.root_task();
        let guard = SplitAssignmentRoundGuard::install(
            &mut round,
            identity.query_execution_id(),
            plan,
            Arc::new(NoCloseTaskUpdates),
            data_runtime.clone(),
            Arc::new(CountingWake::default()),
        )
        .unwrap();
        round.begin_normal_drain();
        let started = round.turn();
        until(|| guard.is_finished());
        let advanced = round.turn();
        let local_closed = round.pumps_closed();
        let health = round
            .take_pump_close_unresolved()
            .expect("the genuine original job panic has a health observation");
        let repeated = round.take_pump_close_unresolved();
        root.owner.complete();
        root.business.release();
        drop(window);
        let retained = control.snapshot();
        let originating_cause = round.failure_cause().cloned();
        let captured = health
            .cause
            .downcast_ref::<ConnectorBlockingIoError>()
            .expect("the health cause is the original protected job error")
            .clone();
        let owner_name = health.owner;
        let result = guard.finish_after_attempt(false);
        drop(health);
        drop(round);
        drop(data_runtime);
        // Fixture teardown is explicit protected retirement of its known
        // static payload, not production permission to finish an unknown T.
        supervisor.retire_original_failure(captured);
        until(|| control.snapshot().scopes.is_empty());
        control.close_admission();
        let shutdown = control.shutdown();
        assert!(started.is_ok() && advanced.is_ok());
        assert!(!local_closed);
        assert_eq!(owner_name, "split_assignment");
        assert!(repeated.is_none());
        assert!(
            originating_cause.is_none(),
            "health is not a new Task failure"
        );
        assert!(result.is_err());
        assert_eq!(retained.root_responsibilities, 1);
        assert_eq!(retained.result_windows.held_positions, [0, 0, 1, 0]);
        assert_eq!(
            (closes.load(Ordering::SeqCst), drops.load(Ordering::SeqCst)),
            (1, 1)
        );
        assert!(shutdown.is_ok());
    }

    struct EnumeratingGateSource {
        inner: ObservedSource,
        started: Option<mpsc::Sender<()>>,
        release: mpsc::Receiver<()>,
    }
    impl ConnectorReadSplitSource for EnumeratingGateSource {
        fn profile_snapshot(&self) -> SplitSourceProfile {
            self.inner.profile_snapshot()
        }
        fn next_batch(
            &mut self,
            _: usize,
            _: &ConnectorReadDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
            self.started
                .take()
                .unwrap()
                .send(())
                .expect("enumeration entered");
            self.release.recv().expect("release actual enumeration");
            Ok(ConnectorSplitBatch::empty())
        }
        fn is_finished(&self) -> bool {
            false
        }
        fn close(&mut self) -> Result<(), ConnectorError> {
            self.inner.close()
        }
    }

    #[test]
    fn closing_waits_for_the_original_enumeration_before_its_gated_source_close() {
        use crate::task_execution::blocking_io::tests::{admitted, until};
        use crate::task_execution::status_intake::CountingWake;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let data_runtime = FrontendDataRuntime::new(runtime.handle().clone());
        let supervisor = data_runtime.connector_blocking_io().clone();
        let (control, root, window) = admitted();
        let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (enum_started, enum_entered) = mpsc::channel();
        let (enum_release, enum_released) = mpsc::channel();
        let (close_started, close_entered) = mpsc::channel();
        let (close_release, close_released) = mpsc::channel();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .unwrap();
        let mut source = owned_round_source(
            EnumeratingGateSource {
                inner: ObservedSource {
                    closes: Arc::clone(&closes),
                    drops: Arc::clone(&drops),
                    started: Some(close_started),
                    release: Some(close_released),
                    panic_close: false,
                    panic_profile: false,
                },
                started: Some(enum_started),
                release: enum_released,
            },
            responsibility,
            supervisor.clone(),
        );
        source.initial_wait_initialized = true;
        let scan = source.scan;
        let (mut round, _, _clock) = crate::task_execution::tests::synchronous_root_control_round();
        let tasks = BTreeMap::from([(
            scan,
            vec![AssignmentTarget {
                backend_idx: 0,
                fragment_instance_id: novarocks_types::UniqueId::new(17, 19),
            }],
        )]);
        let plan = RoundSplitAssignmentPlan::new(
            tasks,
            vec![source],
            TaskUpdateRetryPolicy::default(),
            Duration::ZERO,
            supervisor.clone(),
        );
        let identity = round.root_task();
        let guard = SplitAssignmentRoundGuard::install(
            &mut round,
            identity.query_execution_id(),
            plan,
            Arc::new(NoCloseTaskUpdates),
            data_runtime.clone(),
            Arc::new(CountingWake::default()),
        )
        .unwrap();
        let first_turn = round.turn();
        enum_entered
            .recv_timeout(Duration::from_secs(2))
            .expect("the original enumeration entered");
        guard.request_stop();
        round.begin_normal_drain();
        let close_turn = round.turn();
        root.owner.complete();
        root.business.release();
        drop(window);
        let waiting_enum = control.snapshot();
        let falsely_closed = round.pumps_closed() || guard.is_finished();
        let prematurely_closed_source = closes.load(Ordering::SeqCst);
        let released_enum = enum_release.send(());
        // This loop advances only the closing turn; it cannot enumerate again.
        let stop = Instant::now() + Duration::from_secs(2);
        let mut saw_close = false;
        while Instant::now() < stop {
            let _ = round.turn();
            if close_entered.try_recv().is_ok() {
                saw_close = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let waiting_close = control.snapshot();
        let closed_before_release = round.pumps_closed() || guard.is_finished();
        let released_close = close_release.send(());
        until(|| guard.is_finished());
        let joined = round.turn();
        let closed = round.pumps_closed();
        let outcome = guard.finish_after_attempt(false);
        drop(round);
        drop(data_runtime);
        until(|| control.snapshot().scopes.is_empty());
        control.close_admission();
        let shutdown = control.shutdown();
        assert!(first_turn.is_ok() && close_turn.is_ok() && joined.is_ok());
        assert!(released_enum.is_ok() && released_close.is_ok());
        assert!(!falsely_closed && !closed_before_release);
        assert_eq!(prematurely_closed_source, 0);
        assert!(saw_close && closed && outcome.is_ok());
        assert_eq!(waiting_enum.root_responsibilities, 1);
        assert_eq!(waiting_close.root_responsibilities, 1);
        assert_eq!(waiting_close.result_windows.held_positions, [0, 0, 1, 0]);
        assert_eq!(
            (closes.load(Ordering::SeqCst), drops.load(Ordering::SeqCst)),
            (1, 1)
        );
        assert!(shutdown.is_ok());
    }
}
