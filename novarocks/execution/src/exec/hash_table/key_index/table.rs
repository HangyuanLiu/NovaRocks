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

//! Instance-local equality classes. Full-hash directory callbacks inspect only
//! cached integers; fallible equality runs in bounded candidate batches outside
//! hashbrown. This is not a join row chain or a distributed hash identity.

use super::owned_batch::{CompareScratch, OwnedKeyBatch};
use super::{
    IndexPurpose, KeyId, KeySpec, LocalHashBatch, LocalHashPolicy, PreparedKeyBatch, checkpoint_at,
    invariant, shared, vector,
};
use crate::{
    exec::{
        hash_table::allocation::{AggregateAllocator, AggregateVec},
        pipeline::operator::InputConstructionOwner,
    },
    runtime::{
        fragment::{ExecutionFailure, ExecutionResult},
        mem_tracker::MemTracker,
        tracked_state_allocator::expression_allocation_host,
    },
};
use hashbrown::HashTable;
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_value_arrays::ValueViolation;
use std::sync::Arc;

// Work segmentation only. All further occurrences and collision candidates
// continue in subsequent turns; no input or collision limit follows from it.
const WORK_ROWS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyMatch {
    Unmatchable,
    Miss,
    Found(KeyId),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NewKey {
    pub(crate) occurrence: usize,
    pub(crate) row: usize,
    pub(crate) id: KeyId,
}
pub(crate) struct InternBatch {
    matches: Vec<KeyMatch>,
    new_keys: Vec<NewKey>,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) struct PresenceBatch {
    present: Vec<u8>,
    participates: Vec<u8>,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) struct ProbeBatch {
    matches: Vec<KeyMatch>,
    _storage: ValueTypeStorageLifetime,
}
impl InternBatch {
    pub(crate) fn matches(&self) -> &[KeyMatch] {
        &self.matches
    }
    pub(crate) fn new_keys(&self) -> &[NewKey] {
        &self.new_keys
    }
}
impl PresenceBatch {
    pub(crate) fn present(&self) -> &[u8] {
        &self.present
    }
    pub(crate) fn participates(&self) -> &[u8] {
        &self.participates
    }
}
impl ProbeBatch {
    pub(crate) fn matches(&self) -> &[KeyMatch] {
        &self.matches
    }
}
#[derive(Clone, Copy)]
struct Directory {
    full: u64,
    table: u64,
    head: usize,
}
#[derive(Clone, Copy)]
struct StoredKey {
    batch: usize,
    row: usize,
}
struct Class {
    key: StoredKey,
    next: Option<usize>,
}
struct Pending {
    row: usize,
    occurrence: usize,
    hash: u64,
    next: Option<usize>,
}
#[derive(Clone, Copy, PartialEq)]
enum Match {
    Unmatchable,
    Miss,
    Hit(usize),
    Temporary(usize),
}
struct Chunk {
    start: usize,
    rows: Vec<usize>,
    hashes: Vec<u64>,
    valid: Vec<u8>,
}
#[derive(Clone, Copy)]
struct Node {
    handle: usize,
    batch: usize,
    row: usize,
    next: Option<usize>,
}
#[derive(Clone, Copy)]
struct Candidate {
    probe: usize,
    node: Node,
}
struct PairBuffers {
    left: Vec<usize>,
    right: Vec<usize>,
    owned: CompareScratch,
}
impl PairBuffers {
    fn new(count: usize, owner: &dyn InputConstructionOwner) -> ExecutionResult<Self> {
        Ok(Self {
            left: vector(owner, count)?,
            right: vector(owner, count)?,
            owned: CompareScratch::new(),
        })
    }
    fn rows(
        &mut self,
        chunk: &Chunk,
        candidates: &[Candidate],
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        self.left.clear();
        self.right.clear();
        for (i, candidate) in candidates.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            self.left.push(chunk.rows[candidate.probe]);
            self.right.push(candidate.node.row);
        }
        Ok(())
    }
}

pub(crate) struct KeyIndex {
    spec: KeySpec,
    purpose: IndexPurpose,
    policy: LocalHashPolicy,
    directory: HashTable<Directory, AggregateAllocator>,
    classes: AggregateVec<Class>,
    batches: AggregateVec<Arc<OwnedKeyBatch>>,
    allocator: AggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
    failed: Option<ExecutionFailure>,
    _storage: ValueTypeStorageLifetime,
}
impl KeyIndex {
    pub(crate) fn new(
        spec: KeySpec,
        purpose: IndexPurpose,
        tracker: Arc<MemTracker>,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        let allocator = AggregateAllocator::new(tracker.clone());
        Ok(Self {
            policy: LocalHashPolicy::new(&spec, owner)?,
            spec,
            purpose,
            directory: HashTable::new_in(allocator.clone()),
            classes: AggregateVec::new_in(allocator.clone()),
            batches: AggregateVec::new_in(allocator.clone()),
            allocator,
            host: expression_allocation_host(tracker),
            failed: None,
            _storage: owner.storage_lifetime(),
        })
    }
    pub(crate) fn spec(&self) -> &KeySpec {
        &self.spec
    }
    pub(crate) fn policy(&self) -> &LocalHashPolicy {
        &self.policy
    }
    pub(crate) fn len(&self) -> usize {
        self.classes.len()
    }
    fn check(&self) -> ExecutionResult<()> {
        match &self.failed {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
    /// Borrow actual published pages, grouping each column at page boundaries.
    /// The metadata preserves every requested occurrence in caller order.
    pub(crate) fn read_keys(
        &self,
        ids: &[KeyId],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::read::OwnedKeyReadBatch<'_>> {
        self.check()?;
        if self.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        self.read_representatives(ids, owner, control)
    }
    // Private class coordinates also serve hash-presence compaction. They are
    // never exported as public IDs or caller row assignments for that purpose.
    fn read_representatives(
        &self,
        ids: &[KeyId],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::read::OwnedKeyReadBatch<'_>> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let mut requests = vector(owner, ids.len())?;
        for (ordinal, id) in ids.iter().enumerate() {
            checkpoint_at(ordinal, &checkpoint)?;
            let class = self
                .classes
                .get(id.index())
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            requests.push(super::read::ReadRequest {
                ordinal,
                batch: class.key.batch,
                row: class.key.row,
            });
        }
        super::prepared::sort_by_key(&mut requests, owner, &checkpoint, |r| (r.batch, r.ordinal))?;
        let mut columns = vector(owner, self.spec.types().len())?;
        for column in 0..self.spec.types().len() {
            checkpoint()?;
            let mut pages = Vec::new();
            let mut begin = 0;
            while begin < requests.len() {
                checkpoint()?;
                let mut end = begin + 1;
                while end < requests.len() && requests[end].batch == requests[begin].batch {
                    checkpoint_at(end - begin, &checkpoint)?;
                    end += 1;
                }
                self.batches
                    .get(requests[begin].batch)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?
                    .append_reads(
                        column,
                        &requests[begin..end],
                        &mut pages,
                        owner,
                        &checkpoint,
                    )?;
                begin = end;
            }
            columns.push(super::read::KeyReadColumn { pages });
        }
        checkpoint()?;
        Ok(super::read::OwnedKeyReadBatch {
            columns,
            len: ids.len(),
            _storage: owner.storage_lifetime(),
        })
    }
    fn validate(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
    ) -> ExecutionResult<()> {
        self.check()?;
        if !self.spec.same_instance(&input.spec) || hashes.len() != input.len() {
            return Err(invariant(ValueViolation::Layout));
        }
        hashes.validate(input.input, &self.policy)
    }
    /// Construct complete typed output in the caller's requested ID order.
    /// Refusal does not poison this immutable index.
    pub(crate) fn export_keys(
        &self,
        ids: &[KeyId],
        targets: &[novarocks_type_contract::FunctionValueType],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::ExportedKeys> {
        self.check()?;
        super::export::validate_targets(&self.spec, targets, control)?;
        let read = self.read_keys(ids, owner, control)?;
        super::export::construct(
            &read,
            &self.spec,
            targets,
            owner,
            Arc::clone(&self.host),
            control,
        )
    }
    /// Rebuild immutable representative pages without rehashing or resigning
    /// classes. Escaped Arrow aliases continue to own their previous creators.
    pub(crate) fn compact(
        &mut self,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        self.check()?;
        let result = self.compact_pages(owner, control);
        if let Err(cause) = &result {
            if self.failed.is_none() {
                self.failed = Some(cause.clone());
            }
        }
        result
    }
    fn compact_pages(
        &mut self,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        if self.classes.is_empty() {
            return checkpoint();
        }
        let mut ids = vector(owner, self.classes.len())?;
        for id in 0..self.classes.len() {
            checkpoint_at(id, &checkpoint)?;
            ids.push(KeyId(id));
        }
        let read = self.read_representatives(&ids, owner, control)?;
        let batch =
            super::compact::gather(&read, &self.spec, owner, Arc::clone(&self.host), control)?;
        if batch.len() != self.classes.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        let batch = shared(owner, batch)?;
        let mut replacement = AggregateVec::new_in(self.allocator.clone());
        reserve_state(&mut replacement, 1, &checkpoint)?;
        replacement.push(batch);
        drop(read);
        // Only locations change. The immutable full/table hashes, collision
        // links and class IDs remain exact. Failure during this commit latches
        // the entire index; its partially changed prefix is never readable.
        checkpoint()?;
        for (row, class) in self.classes.iter_mut().enumerate() {
            checkpoint_at(row, &checkpoint)?;
            class.key = StoredKey { batch: 0, row };
        }
        self.batches = replacement;
        checkpoint()
    }
    /// Mutation publishes a complete result or permanently latches its first
    /// failure. Partially committed internals can never be retried as success.
    pub(crate) fn intern_batch(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<InternBatch> {
        let result = self.intern(input, hashes, true, owner, control);
        match result {
            Ok(Some(output)) => Ok(output),
            Ok(None) => unreachable!("KeyIds intern returns its assignment"),
            Err(cause) => {
                if self.failed.is_none() {
                    self.failed = Some(cause.clone());
                }
                Err(cause)
            }
        }
    }
    /// Presence construction returns only cardinality change. No public KeyId,
    /// per-input build assignment or duplicate physical row list is produced.
    pub(crate) fn include_presence(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<usize> {
        let before = self.len();
        match self.intern(input, hashes, false, owner, control) {
            Ok(_) => Ok(self.len() - before),
            Err(cause) => {
                if self.failed.is_none() {
                    self.failed = Some(cause.clone());
                }
                Err(cause)
            }
        }
    }
    pub(crate) fn probe_batch(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<ProbeBatch> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        self.validate(input, hashes)?;
        if self.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        let mut output = vector(owner, input.len())?;
        for start in (0..input.len()).step_by(WORK_ROWS) {
            checkpoint()?;
            let chunk = Chunk::load(input, hashes, start, owner, &checkpoint)?;
            let mut found = chunk.matches(owner, &checkpoint)?;
            self.probe_existing(input, &chunk, &mut found, owner, &checkpoint)?;
            for (i, found) in found.into_iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                output.push(public_match(found)?);
            }
        }
        checkpoint()?;
        Ok(ProbeBatch {
            matches: output,
            _storage: owner.storage_lifetime(),
        })
    }
    pub(crate) fn contains_batch(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<PresenceBatch> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        self.validate(input, hashes)?;
        let mut present = vector(owner, input.len())?;
        let mut participates = vector(owner, input.len())?;
        for start in (0..input.len()).step_by(WORK_ROWS) {
            checkpoint()?;
            let chunk = Chunk::load(input, hashes, start, owner, &checkpoint)?;
            let mut found = chunk.matches(owner, &checkpoint)?;
            self.probe_existing(input, &chunk, &mut found, owner, &checkpoint)?;
            for (i, found) in found.into_iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                present.push(u8::from(matches!(found, Match::Hit(_))));
                participates.push(u8::from(found != Match::Unmatchable));
            }
        }
        checkpoint()?;
        Ok(PresenceBatch {
            present,
            participates,
            _storage: owner.storage_lifetime(),
        })
    }
    fn probe_existing(
        &self,
        input: &PreparedKeyBatch<'_>,
        chunk: &Chunk,
        output: &mut [Match],
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        let mut buffers = PairBuffers::new(chunk.rows.len(), owner)?;
        probe_chains(
            chunk,
            output,
            &self.directory,
            |handle| {
                let class = self
                    .classes
                    .get(handle)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                Ok(Node {
                    handle,
                    batch: class.key.batch,
                    row: class.key.row,
                    next: class.next,
                })
            },
            |candidates, truth| {
                buffers.rows(chunk, candidates, checkpoint)?;
                let batch = candidates[0].node.batch;
                self.batches
                    .get(batch)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?
                    .compare_pairs(
                        input,
                        &buffers.left,
                        &buffers.right,
                        truth,
                        &mut buffers.owned,
                        owner,
                        checkpoint,
                    )
            },
            owner,
            checkpoint,
        )
    }
    fn intern(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        ids: bool,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Option<InternBatch>> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        self.validate(input, hashes)?;
        if ids != (self.purpose == IndexPurpose::KeyIds) {
            return Err(invariant(ValueViolation::Layout));
        }
        checkpoint()?;
        let base = self.classes.len();
        let mut pending = AggregateVec::<Pending>::new_in(self.allocator.clone());
        let mut directory =
            HashTable::<Directory, AggregateAllocator>::new_in(self.allocator.clone());
        let mut output = if ids {
            Some(vector(owner, input.len())?)
        } else {
            None
        };
        for start in (0..input.len()).step_by(WORK_ROWS) {
            checkpoint()?;
            let chunk = Chunk::load(input, hashes, start, owner, &checkpoint)?;
            let mut found = chunk.matches(owner, &checkpoint)?;
            self.probe_existing(input, &chunk, &mut found, owner, &checkpoint)?;
            let mut buffers = PairBuffers::new(chunk.rows.len(), owner)?;
            probe_chains(
                &chunk,
                &mut found,
                &directory,
                |handle| {
                    let value = pending
                        .get(handle)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    Ok(Node {
                        handle: base
                            .checked_add(handle)
                            .ok_or_else(|| invariant(ValueViolation::Bounds))?,
                        batch: 0,
                        row: value.row,
                        next: value.next,
                    })
                },
                |candidates, truth| {
                    buffers.rows(&chunk, candidates, &checkpoint)?;
                    super::pairs::compare_prepared_pairs(
                        input,
                        &buffers.left,
                        input,
                        &buffers.right,
                        truth,
                        owner,
                        &checkpoint,
                    )
                },
                owner,
                &checkpoint,
            )?;
            deduplicate_chunk(
                input,
                &chunk,
                &mut found,
                &mut pending,
                &mut directory,
                base,
                owner,
                &checkpoint,
            )?;
            if let Some(output) = &mut output {
                for (i, found) in found.into_iter().enumerate() {
                    checkpoint_at(i, &checkpoint)?;
                    output.push(public_match(found)?);
                }
            }
        }
        let mut new_keys = if ids {
            Some(vector(owner, pending.len())?)
        } else {
            None
        };
        if !pending.is_empty() {
            let mut rows = vector(owner, pending.len())?;
            for (i, value) in pending.iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                rows.push(value.row);
            }
            let batch = shared(
                owner,
                OwnedKeyBatch::copy_selected(input, &rows, owner, Arc::clone(&self.host), control)?,
            )?;
            if batch.len() != pending.len() {
                return Err(invariant(ValueViolation::Bounds));
            }
            let mut additional = 0;
            for (i, entry) in directory.iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                if self
                    .directory
                    .find(entry.table, |old| old.full == entry.full)
                    .is_none()
                {
                    additional += 1;
                }
            }
            reserve_directory(&mut self.directory, additional, &checkpoint)?;
            reserve_state(&mut self.classes, pending.len(), &checkpoint)?;
            reserve_state(&mut self.batches, 1, &checkpoint)?;
            let batch_id = self.batches.len();
            self.batches.push(batch);
            for (row, value) in pending.iter().enumerate() {
                checkpoint_at(row, &checkpoint)?;
                let handle = self.classes.len();
                let next = self
                    .directory
                    .find(value.hash, |old| old.full == value.hash)
                    .map(|old| old.head);
                self.classes.push(Class {
                    key: StoredKey {
                        batch: batch_id,
                        row,
                    },
                    next,
                });
                if let Some(entry) = self
                    .directory
                    .find_mut(value.hash, |old| old.full == value.hash)
                {
                    entry.head = handle;
                } else {
                    self.directory.insert_unique(
                        value.hash,
                        Directory {
                            full: value.hash,
                            table: value.hash,
                            head: handle,
                        },
                        |entry| entry.table,
                    );
                }
                if let Some(new_keys) = &mut new_keys {
                    new_keys.push(NewKey {
                        occurrence: value.occurrence,
                        row: value.row,
                        id: KeyId(handle),
                    });
                }
            }
        }
        checkpoint()?;
        Ok(output.map(|matches| InternBatch {
            matches,
            new_keys: new_keys.expect("KeyIds new selection"),
            _storage: owner.storage_lifetime(),
        }))
    }
}
impl Chunk {
    fn load(
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        start: usize,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        let end = start.saturating_add(WORK_ROWS).min(input.len());
        let mut rows = vector(owner, end - start)?;
        let mut full = vector(owner, end - start)?;
        let mut valid = vector(owner, end - start)?;
        for occurrence in start..end {
            checkpoint_at(occurrence, checkpoint)?;
            rows.push(input.input.row(occurrence)?);
            full.push(hashes.hash(occurrence)?);
            valid.push(u8::from(hashes.participates(occurrence)?));
        }
        Ok(Self {
            start,
            rows,
            hashes: full,
            valid,
        })
    }
    fn matches(
        &self,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Vec<Match>> {
        let mut output = vector(owner, self.rows.len())?;
        for (i, &valid) in self.valid.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            output.push(if valid != 0 {
                Match::Miss
            } else {
                Match::Unmatchable
            });
        }
        Ok(output)
    }
}
fn public_match(value: Match) -> ExecutionResult<KeyMatch> {
    match value {
        Match::Unmatchable => Ok(KeyMatch::Unmatchable),
        Match::Miss => Ok(KeyMatch::Miss),
        Match::Hit(id) => Ok(KeyMatch::Found(KeyId(id))),
        Match::Temporary(_) => Err(invariant(ValueViolation::Layout)),
    }
}
#[allow(clippy::too_many_arguments)]
fn probe_chains(
    chunk: &Chunk,
    output: &mut [Match],
    directory: &HashTable<Directory, AggregateAllocator>,
    resolve: impl Fn(usize) -> ExecutionResult<Node>,
    mut compare: impl FnMut(&[Candidate], &mut [u8]) -> ExecutionResult<()>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let mut active = vector(owner, chunk.rows.len())?;
    for (probe, value) in output.iter().enumerate() {
        checkpoint_at(probe, checkpoint)?;
        if *value == Match::Miss {
            if let Some(entry) = directory.find(chunk.hashes[probe], |entry| {
                entry.full == chunk.hashes[probe]
            }) {
                active.push(Candidate {
                    probe,
                    node: resolve(entry.head)?,
                });
            }
        }
    }
    let mut truth = vector(owner, active.len())?;
    while !active.is_empty() {
        checkpoint()?;
        super::prepared::sort_by_key(&mut active, owner, checkpoint, |candidate| {
            (candidate.node.batch, candidate.probe)
        })?;
        truth.clear();
        for i in 0..active.len() {
            checkpoint_at(i, checkpoint)?;
            truth.push(0);
        }
        let mut start = 0;
        while start < active.len() {
            checkpoint()?;
            let mut end = start + 1;
            while end < active.len() && active[end].node.batch == active[start].node.batch {
                checkpoint_at(end, checkpoint)?;
                end += 1;
            }
            compare(&active[start..end], &mut truth[start..end])?;
            start = end;
        }
        let mut kept = 0;
        for i in 0..active.len() {
            checkpoint_at(i, checkpoint)?;
            let candidate = active[i];
            if truth[i] != 0 {
                output[candidate.probe] = Match::Hit(candidate.node.handle);
            } else if let Some(next) = candidate.node.next {
                active[kept] = Candidate {
                    probe: candidate.probe,
                    node: resolve(next)?,
                };
                kept += 1;
            }
        }
        active.truncate(kept);
    }
    checkpoint()
}
#[allow(clippy::too_many_arguments)]
fn deduplicate_chunk(
    input: &PreparedKeyBatch<'_>,
    chunk: &Chunk,
    output: &mut [Match],
    pending: &mut AggregateVec<Pending>,
    directory: &mut HashTable<Directory, AggregateAllocator>,
    base: usize,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let mut remaining = vector(owner, output.len())?;
    for (i, value) in output.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        if *value == Match::Miss {
            remaining.push(i);
        }
    }
    super::prepared::sort_by_key(&mut remaining, owner, checkpoint, |i| (chunk.hashes[i], i))?;
    let mut representatives = vector(owner, remaining.len())?;
    let mut next = vector(owner, remaining.len())?;
    let mut probes = vector(owner, remaining.len())?;
    let mut rep_ids = vector(owner, remaining.len())?;
    let mut left = vector(owner, remaining.len())?;
    let mut right = vector(owner, remaining.len())?;
    let mut truth = vector(owner, remaining.len())?;
    while !remaining.is_empty() {
        checkpoint()?;
        next.clear();
        probes.clear();
        rep_ids.clear();
        left.clear();
        right.clear();
        truth.clear();
        let mut start = 0;
        while start < remaining.len() {
            checkpoint()?;
            let first = remaining[start];
            let temp = representatives.len();
            representatives.push(first);
            output[first] = Match::Temporary(temp);
            let mut end = start + 1;
            while end < remaining.len() && chunk.hashes[remaining[end]] == chunk.hashes[first] {
                checkpoint_at(end, checkpoint)?;
                probes.push(remaining[end]);
                rep_ids.push(temp);
                left.push(chunk.rows[remaining[end]]);
                right.push(chunk.rows[first]);
                truth.push(0);
                end += 1;
            }
            start = end;
        }
        super::pairs::compare_prepared_pairs(
            input, &left, input, &right, &mut truth, owner, checkpoint,
        )?;
        for i in 0..probes.len() {
            checkpoint_at(i, checkpoint)?;
            if truth[i] != 0 {
                output[probes[i]] = Match::Temporary(rep_ids[i]);
            } else {
                next.push(probes[i]);
            }
        }
        std::mem::swap(&mut remaining, &mut next);
    }
    let mut order = vector(owner, representatives.len())?;
    let mut assigned = vector(owner, representatives.len())?;
    for i in 0..representatives.len() {
        checkpoint_at(i, checkpoint)?;
        order.push(i);
        assigned.push(0usize);
    }
    super::prepared::sort_by_key(&mut order, owner, checkpoint, |i| representatives[i])?;
    for (i, &temp) in order.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        let probe = representatives[temp];
        let hash = chunk.hashes[probe];
        let handle = pending.len();
        reserve_state(pending, 1, checkpoint)?;
        if directory.find(hash, |entry| entry.full == hash).is_none() {
            reserve_directory(directory, 1, checkpoint)?;
        }
        let previous = directory
            .find(hash, |entry| entry.full == hash)
            .map(|entry| entry.head);
        pending.push(Pending {
            row: chunk.rows[probe],
            occurrence: chunk.start + probe,
            hash,
            next: previous,
        });
        if let Some(entry) = directory.find_mut(hash, |entry| entry.full == hash) {
            entry.head = handle;
        } else {
            directory.insert_unique(
                hash,
                Directory {
                    full: hash,
                    table: hash,
                    head: handle,
                },
                |entry| entry.table,
            );
        }
        assigned[temp] = base
            .checked_add(handle)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
    }
    for (i, value) in output.iter_mut().enumerate() {
        checkpoint_at(i, checkpoint)?;
        if let Match::Temporary(temp) = *value {
            *value = Match::Hit(assigned[temp]);
        }
    }
    checkpoint()
}
fn reserve_directory(
    directory: &mut HashTable<Directory, AggregateAllocator>,
    additional: usize,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let allocator = directory.allocator().clone();
    directory
        .try_reserve(additional, |entry| entry.table)
        .map_err(|source| {
            ExecutionFailure::from(
                allocator.allocation_error("reserve local key directory", source),
            )
        })?;
    checkpoint()
}
fn reserve_state<T>(
    values: &mut AggregateVec<T>,
    additional: usize,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let allocator = values.allocator().clone();
    values.try_reserve(additional).map_err(|source| {
        ExecutionFailure::from(allocator.allocation_error("reserve local key state", source))
    })?;
    checkpoint()
}
