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

//! Local data dependencies and exact task custody. There is no executor here:
//! a caller spends its original turn allowance on `advance`, and owns wakeups
//! and actual driver exit. A completed data cursor is not a phase barrier.

use super::{
    IndexPurpose, KeyInput, KeyMatch,
    bulk::CapturedRuns,
    bulk::CompleteInput,
    capture::{BulkDomain, CapturedPageId},
    direct_int::{self, DirectWork},
    invariant, shared,
    strategy::{StrategyKind, choose_layout},
    vector,
};
use crate::{
    exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::ValueViolation;
use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{Arc, Mutex, MutexGuard},
};

// A claim/work segmentation, never an input or distinct-count limit.
const INPUT_RANGE_ROWS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Init,
    Mark,
    Count,
    Prefix,
    Fill,
    Null,
    Assign,
    Ready,
    Published,
}

#[derive(Clone)]
struct InputRanges {
    run: usize,
    page: usize,
    occurrence: usize,
}
struct InputRange {
    page: CapturedPageId,
    occurrences: Range<usize>,
}
impl InputRanges {
    fn new() -> Self {
        Self {
            run: 0,
            page: 0,
            occurrence: 0,
        }
    }
    /// Skip only E empty run headers. Even an empty captured page is a task;
    /// one claim never walks all B page descriptors before doing useful work.
    fn next(
        &mut self,
        runs: &CapturedRuns,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Option<InputRange>> {
        while self.run < runs.len() {
            super::checkpoint_at(self.run, checkpoint)?;
            let run = runs.run(self.run)?;
            if self.page == run.len() {
                self.run += 1;
                self.page = 0;
                self.occurrence = 0;
                continue;
            }
            let page = run.page(self.page)?;
            let start = self.occurrence;
            let end = start.saturating_add(INPUT_RANGE_ROWS).min(page.len());
            let result = InputRange {
                page: page.id(),
                occurrences: start..end,
            };
            if end == page.len() {
                self.page += 1;
                self.occurrence = 0;
            } else {
                self.occurrence = end;
            }
            return Ok(Some(result));
        }
        Ok(None)
    }
}

struct ShardFacts {
    counted: Option<direct_int::CountedShard>,
    prefixed: Option<direct_int::PrefixedShard>,
    base: usize,
}
struct State {
    phase: Phase,
    next_shard: usize,
    ranges: InputRanges,
    exhausted: bool,
    issued: usize,
    completed: usize,
    inflight: usize,
    shards: Vec<ShardFacts>,
    nonnull_classes: usize,
    assignments: Option<Arc<AssignmentNode>>,
}
struct Shared {
    domain: BulkDomain,
    runs: CapturedRuns,
    store: Arc<direct_int::Storage>,
    workers: usize,
    state: Mutex<State>,
    _storage: ValueTypeStorageLifetime,
}
impl Shared {
    fn state(&self) -> ExecutionResult<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| invariant(ValueViolation::RowTransaction))
    }
    fn check(&self, work: &DirectWork<'_>) -> ExecutionResult<()> {
        self.domain.check_failure()?;
        self.store.check_failure()?;
        work.control.checkpoint(0).map_err(ExecutionFailure::from)
    }
    fn latch<T>(&self, result: ExecutionResult<T>) -> ExecutionResult<T> {
        if let Err(cause) = &result {
            self.domain.latch(cause.clone());
        }
        result
    }
}

/// One graph owns the identities and barriers for the selected local layout.
/// Direct and Hash workers must use this author, not mint their own completion
/// registry. The Hash phases are integrated separately before consumer freeze.
pub(super) struct BulkTaskGraph(Arc<Shared>);
pub(super) enum Claim {
    Task(Task),
    Pending,
    Finished,
}

impl BulkTaskGraph {
    /// Enter only a strategy-selected Direct path. This is an internal staging
    /// entry, not a fallback from a failed Hash or resource operation.
    pub(super) fn begin_direct(
        input: CompleteInput,
        workers: NonZeroUsize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Self> {
        let domain = input.stats().domain().clone();
        let result = (|| {
            let decision =
                choose_layout(input.stats(), Some(input.stats().hint()), workers, &|| {
                    work.control.checkpoint(0).map_err(ExecutionFailure::from)
                })?;
            let StrategyKind::Direct(geometry) = decision.kind else {
                return Err(invariant(ValueViolation::Layout));
            };
            let store = direct_int::Storage::new(
                geometry,
                domain.spec(),
                input.stats().exact().integer_root_nullable_union(),
                work,
            )?;
            let mut shards = vector(work.owner, geometry.shard_count())?;
            for _ in 0..geometry.shard_count() {
                work.control.checkpoint(0)?;
                shards.push(ShardFacts {
                    counted: None,
                    prefixed: None,
                    base: 0,
                });
            }
            let (runs, _) = input.into_parts();
            let shared = shared(
                work.owner,
                Shared {
                    domain: domain.clone(),
                    runs,
                    store,
                    workers: workers.get(),
                    state: Mutex::new(State {
                        phase: Phase::Init,
                        next_shard: 0,
                        ranges: InputRanges::new(),
                        exhausted: false,
                        issued: 0,
                        completed: 0,
                        inflight: 0,
                        shards,
                        nonnull_classes: 0,
                        assignments: None,
                    }),
                    _storage: work.owner.storage_lifetime(),
                },
            )?;
            shared.check(work)?;
            Ok(Self(shared))
        })();
        if let Err(cause) = &result {
            domain.latch(cause.clone());
        }
        result
    }

    pub(super) fn phase(&self) -> ExecutionResult<Phase> {
        self.0.domain.check_failure()?;
        Ok(self.0.state()?.phase)
    }

    pub(super) fn claim(&self, work: &DirectWork<'_>) -> ExecutionResult<Claim> {
        self.0.latch(self.claim_inner(work))
    }
    fn claim_inner(&self, work: &DirectWork<'_>) -> ExecutionResult<Claim> {
        self.0.check(work)?;
        let (lease, seed) = {
            let mut state = self.0.state()?;
            loop {
                self.cross_barrier(&mut state, work)?;
                if matches!(state.phase, Phase::Ready | Phase::Published) {
                    return Ok(Claim::Finished);
                }
                if state.inflight == self.0.workers || state.exhausted {
                    return Ok(Claim::Pending);
                }
                let seed = match state.phase {
                    Phase::Init | Phase::Count | Phase::Prefix | Phase::Fill => {
                        if state.next_shard == state.shards.len() {
                            state.exhausted = true;
                            continue;
                        }
                        let shard = state.next_shard;
                        state.next_shard += 1;
                        match state.phase {
                            Phase::Init => Seed::Init(shard),
                            Phase::Count => Seed::Count(shard),
                            Phase::Prefix => {
                                let facts = &mut state.shards[shard];
                                Seed::Prefix(
                                    facts
                                        .counted
                                        .take()
                                        .ok_or_else(|| invariant(ValueViolation::RowTransaction))?,
                                    facts.base,
                                )
                            }
                            Phase::Fill => Seed::Fill(
                                state.shards[shard]
                                    .prefixed
                                    .take()
                                    .ok_or_else(|| invariant(ValueViolation::RowTransaction))?,
                            ),
                            _ => unreachable!(),
                        }
                    }
                    Phase::Mark | Phase::Assign => {
                        let Some(range) =
                            state.ranges.next(&self.0.runs, &|| self.0.check(work))?
                        else {
                            state.exhausted = true;
                            continue;
                        };
                        if state.phase == Phase::Mark {
                            Seed::Mark(range)
                        } else {
                            Seed::Assign(range)
                        }
                    }
                    Phase::Null => {
                        state.exhausted = true;
                        Seed::Null(state.nonnull_classes)
                    }
                    Phase::Ready | Phase::Published => unreachable!(),
                };
                let ordinal = state.issued;
                state.issued = state.issued.checked_add(1).ok_or_else(arithmetic)?;
                state.inflight = state.inflight.checked_add(1).ok_or_else(arithmetic)?;
                break (
                    Lease {
                        shared: Arc::clone(&self.0),
                        phase: state.phase,
                        ordinal,
                        settled: false,
                    },
                    seed,
                );
            }
        };
        // Allocation, source access and actual cursor work run outside the
        // metadata mutex. A failed constructor drops its still-open lease.
        let body: ExecutionResult<_> = (|| {
            Ok(match seed {
                Seed::Init(shard) => Body::Init(direct_int::InitCursor::new(
                    Arc::clone(&self.0.store),
                    shard,
                    work,
                )?),
                Seed::Mark(range) => Body::Mark(direct_int::MarkCursor::new(
                    Arc::clone(&self.0.store),
                    self.0.runs.page(range.page)?.input(),
                    range.occurrences,
                )?),
                Seed::Count(shard) => Body::Count(direct_int::CountCursor::new(
                    Arc::clone(&self.0.store),
                    shard,
                    work,
                )?),
                Seed::Prefix(counted, base) => {
                    Body::Prefix(direct_int::PrefixCursor::new(counted, base)?)
                }
                Seed::Fill(prefixed) => Body::Fill(direct_int::FillCursor::new(prefixed, work)?),
                Seed::Null(count) => Body::Null(count),
                Seed::Assign(range) => {
                    let input = self.0.runs.page(range.page)?.input().clone();
                    let cursor = direct_int::AssignCursor::new(
                        Arc::clone(&self.0.store),
                        &input,
                        range.occurrences,
                        work,
                    )?;
                    Body::Assign {
                        cursor,
                        input,
                        page: range.page,
                    }
                }
            })
        })();
        let body = match body {
            Ok(body) => body,
            Err(cause) => {
                // Preserve the actual refusal before the unfinished lease's
                // Drop records abandonment of this exact task.
                self.0.domain.latch(cause.clone());
                return Err(cause);
            }
        };
        Ok(Claim::Task(Task {
            body: Some(body),
            output: None,
            lease: Some(lease),
        }))
    }

    /// Taking a completion is exact once even when a caller repeats its
    /// `complete` invocation. Output and lease are not separately constructible.
    pub(super) fn complete(
        &self,
        completion: &mut Completion,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<()> {
        self.0.latch(self.complete_inner(completion, work))
    }
    fn complete_inner(
        &self,
        completion: &mut Completion,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<()> {
        self.0.check(work)?;
        let (output, mut lease) = completion
            .value
            .take()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        let result = (|| {
            if !Arc::ptr_eq(&self.0, &lease.shared) {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            let mut state = self.0.state()?;
            if lease.phase != state.phase
                || lease.ordinal >= state.issued
                || lease.settled
                || state.inflight == 0
                || state.completed >= state.issued
            {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            match (state.phase, output) {
                (Phase::Init | Phase::Mark | Phase::Fill | Phase::Null, Output::Unit) => {}
                (Phase::Count, Output::Counted(counted)) => {
                    let shard = counted.shard();
                    let facts = state
                        .shards
                        .get_mut(shard)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    if facts.counted.is_some() {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    facts.counted = Some(counted);
                }
                (Phase::Prefix, Output::Prefixed { shard, prefix }) => {
                    let facts = state
                        .shards
                        .get_mut(shard)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    if facts.prefixed.is_some() {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    facts.prefixed = Some(prefix);
                }
                (Phase::Assign, Output::Assigned(mut node)) => {
                    let entry = Arc::get_mut(&mut node)
                        .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
                    entry.next = state.assignments.take();
                    state.assignments = Some(node);
                }
                _ => return Err(invariant(ValueViolation::RowTransaction)),
            }
            state.inflight -= 1;
            state.completed += 1;
            lease.settled = true;
            self.cross_barrier(&mut state, work)
        })();
        // In particular, drop any unconsumed cursor/output before the failed
        // token publishes its abandonment fact; never refund an active worker.
        if let Err(cause) = &result {
            lease.shared.domain.latch(cause.clone());
        }
        result
    }

    fn cross_barrier(&self, state: &mut State, work: &DirectWork<'_>) -> ExecutionResult<()> {
        if !state.exhausted || state.inflight != 0 {
            return Ok(());
        }
        if state.completed != state.issued {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        self.0.check(work)?;
        let next = match state.phase {
            Phase::Init => Phase::Mark,
            Phase::Mark if self.0.store.geometry().purpose() == IndexPurpose::PresenceOnly => {
                Phase::Ready
            }
            Phase::Mark => Phase::Count,
            Phase::Count => {
                // Only W small totals here; each worker prefixes its own word
                // counts later. There is no coordinator scan of bitmap/slots.
                let mut base = 0usize;
                for (index, facts) in state.shards.iter_mut().enumerate() {
                    work.control.checkpoint(0)?;
                    let counted = facts
                        .counted
                        .as_ref()
                        .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
                    if counted.shard() != index {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    facts.base = base;
                    base = base.checked_add(counted.total()).ok_or_else(arithmetic)?;
                }
                state.nonnull_classes = base;
                Phase::Prefix
            }
            Phase::Prefix => Phase::Fill,
            Phase::Fill => Phase::Null,
            Phase::Null => Phase::Assign,
            Phase::Assign => Phase::Ready,
            Phase::Ready | Phase::Published => return Ok(()),
        };
        state.phase = next;
        state.next_shard = 0;
        state.ranges = InputRanges::new();
        state.exhausted = false;
        state.issued = 0;
        state.completed = 0;
        Ok(())
    }

    pub(super) fn take_ready(&self, work: &DirectWork<'_>) -> ExecutionResult<Option<DirectReady>> {
        self.0.latch((|| {
            self.0.check(work)?;
            let mut state = self.0.state()?;
            self.cross_barrier(&mut state, work)?;
            if state.phase != Phase::Ready {
                return Ok(None);
            }
            self.0.check(work)?;
            state.phase = Phase::Published;
            Ok(Some(DirectReady {
                assignments: state.assignments.take(),
                store: Arc::clone(&self.0.store),
            }))
        })())
    }
}

enum Seed {
    Init(usize),
    Mark(InputRange),
    Count(usize),
    Prefix(direct_int::CountedShard, usize),
    Fill(direct_int::PrefixedShard),
    Null(usize),
    Assign(InputRange),
}
struct Lease {
    shared: Arc<Shared>,
    phase: Phase,
    ordinal: usize,
    settled: bool,
}
impl Drop for Lease {
    fn drop(&mut self) {
        if !self.settled {
            self.shared
                .domain
                .latch(invariant(ValueViolation::RowTransaction));
        }
    }
}
enum Body {
    Init(direct_int::InitCursor),
    Mark(direct_int::MarkCursor),
    Count(direct_int::CountCursor),
    Prefix(direct_int::PrefixCursor),
    Fill(direct_int::FillCursor),
    Null(usize),
    Assign {
        cursor: direct_int::AssignCursor,
        input: KeyInput,
        page: CapturedPageId,
    },
}
enum Output {
    Unit,
    Counted(direct_int::CountedShard),
    Prefixed {
        shard: usize,
        prefix: direct_int::PrefixedShard,
    },
    Assigned(Arc<AssignmentNode>),
}
pub(super) struct Task {
    body: Option<Body>,
    output: Option<Output>,
    // Actual cursor/output fields drop before an abandoned lease.
    lease: Option<Lease>,
}
pub(super) struct Completion {
    value: Option<(Output, Lease)>,
}
impl Task {
    pub(super) fn phase(&self) -> Phase {
        self.lease.as_ref().expect("unconsumed task").phase
    }
    /// Quota is the caller's existing synchronous turn allowance, not a new
    /// resource authorization. Zero cannot run even the small NULL task.
    pub(super) fn advance(&mut self, quota: usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        let origin = Arc::clone(&self.lease.as_ref().expect("unconsumed task").shared);
        let result = self.advance_inner(quota, work);
        if let Err(cause) = &result {
            self.body.take();
            self.output.take();
            origin.domain.latch(cause.clone());
        }
        result
    }
    fn advance_inner(&mut self, quota: usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        let lease = self.lease.as_ref().expect("unconsumed task");
        lease.shared.check(work)?;
        let body = self
            .body
            .as_mut()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        if quota == 0 {
            return Ok(false);
        }
        let done = match body {
            Body::Init(c) => c.advance(quota, work)?,
            Body::Mark(c) => c.advance(quota, work)?,
            Body::Count(c) => c.advance(quota, work)?,
            Body::Prefix(c) => c.advance(quota, work)?,
            Body::Fill(c) => c.advance(quota, work)?,
            Body::Null(classes) => {
                lease.shared.store.finish_null(*classes, work)?;
                true
            }
            Body::Assign { cursor, .. } => cursor.advance(quota, work)?,
        };
        if done {
            let output = match self.body.take().expect("advanced body") {
                Body::Count(c) => Output::Counted(c.finish()?),
                Body::Prefix(c) => {
                    // Claim order is the exact shard ordinal in these phases.
                    Output::Prefixed {
                        shard: lease.ordinal,
                        prefix: c.finish()?,
                    }
                }
                Body::Assign {
                    cursor,
                    input,
                    page,
                } => Output::Assigned(shared(
                    work.owner,
                    AssignmentNode {
                        page,
                        input,
                        assignment: cursor.finish()?,
                        next: None,
                        _storage: work.owner.storage_lifetime(),
                    },
                )?),
                Body::Init(_) | Body::Mark(_) | Body::Fill(_) | Body::Null(_) => Output::Unit,
            };
            lease.shared.check(work)?;
            self.output = Some(output);
        }
        Ok(done)
    }
    pub(super) fn finish(mut self) -> ExecutionResult<Completion> {
        let lease = self.lease.as_ref().expect("unconsumed task");
        lease.shared.domain.check_failure()?;
        let Some(output) = self.output.take() else {
            let cause = invariant(ValueViolation::RowTransaction);
            lease.shared.domain.latch(cause.clone());
            return Err(cause);
        };
        Ok(Completion {
            value: Some((output, self.lease.take().expect("unconsumed task"))),
        })
    }
}

struct AssignmentNode {
    page: CapturedPageId,
    input: KeyInput,
    assignment: direct_int::AssignmentPage,
    next: Option<Arc<AssignmentNode>>,
    _storage: ValueTypeStorageLifetime,
}
impl Drop for AssignmentNode {
    fn drop(&mut self) {
        // A large input may have arbitrarily many output chunks. Release a
        // unique chain iteratively instead of recursively consuming the stack.
        let mut next = self.next.take();
        while let Some(node) = next {
            match Arc::try_unwrap(node) {
                Ok(mut node) => next = node.next.take(),
                Err(_) => break,
            }
        }
    }
}

/// Only the all-task barrier constructs this readonly holder. It cannot expose
/// mark/intern or be manufactured from one successful low-level cursor.
pub(super) struct DirectReady {
    assignments: Option<Arc<AssignmentNode>>,
    store: Arc<direct_int::Storage>,
}
impl DirectReady {
    pub(super) fn read_keys(
        &self,
        ids: &[super::KeyId],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::OwnedKeyReadBatch<'_>> {
        self.store.read_keys(ids, owner, control)
    }
    pub(super) fn export_keys(
        &self,
        ids: &[super::KeyId],
        targets: &[FunctionValueType],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::ExportedKeys> {
        self.store.export_keys(ids, targets, owner, host, control)
    }
    pub(super) fn contains_integer(&self, value: Option<i64>) -> ExecutionResult<bool> {
        self.store.contains_value_after_mark(value)
    }
    pub(super) fn lookup_integer(&self, value: Option<i64>) -> ExecutionResult<KeyMatch> {
        self.store.lookup_value_after_fill(value)
    }
    pub(super) fn assignments(&self) -> Assignments<'_> {
        Assignments(self.assignments.as_deref())
    }
}
pub(super) struct Assignments<'a>(Option<&'a AssignmentNode>);
pub(super) struct AssignmentChunk<'a>(&'a AssignmentNode);
impl<'a> Iterator for Assignments<'a> {
    type Item = AssignmentChunk<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.0.take()?;
        self.0 = node.next.as_deref();
        Some(AssignmentChunk(node))
    }
}
impl AssignmentChunk<'_> {
    pub(super) fn page(&self) -> CapturedPageId {
        self.0.page
    }
    pub(super) fn start(&self) -> usize {
        self.0.assignment.start
    }
    pub(super) fn matches(&self) -> &[KeyMatch] {
        self.0.assignment.matches()
    }
    pub(super) fn row(&self, ordinal: usize) -> ExecutionResult<usize> {
        if ordinal >= self.matches().len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        self.0.input.row(self.start() + ordinal)
    }
}
fn arithmetic() -> ExecutionFailure {
    ConstructionFailure::from(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
    .into()
}

#[cfg(test)]
#[path = "bulk_tasks_tests.rs"]
mod tests;
