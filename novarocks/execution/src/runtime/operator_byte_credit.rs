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
//! Finite per-occurrence queue and consumer windows. These permits grant no
//! allocation authority: actual Arrow/source funding continues with Chunk.

use super::{
    fragment::{ExecutionFailure, ExecutionReason, ExecutionResult},
    mem_tracker::{MemTracker, TrackedBytes},
    observable::Observable,
};
use crate::exec::pipeline::step::{WaitFact, WaitReason};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, ResourceDimension, ResourceFacts,
};
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub(crate) enum CreditFailure {
    Contract(&'static str),
    Allocation(std::collections::TryReserveError),
    Observers {
        requested: usize,
        source: super::observable::ObservableCapacityError,
    },
    Capacity {
        requested: usize,
        limit: usize,
    },
}
impl std::fmt::Display for CreditFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => write!(f, "operator byte-credit contract: {detail}"),
            Self::Allocation(source) => {
                write!(f, "operator retained-state allocation failed: {source}")
            }
            Self::Observers { requested, source } => write!(
                f,
                "operator requires {requested} observable positions: {source}"
            ),
            Self::Capacity { requested, limit } => write!(
                f,
                "operator byte-credit request {requested} exceeds capacity {limit}"
            ),
        }
    }
}
impl std::error::Error for CreditFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Allocation(source) => Some(source),
            Self::Observers { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for CreditFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Allocation(_) => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::OperatorByteCreditCapacity.reason(),
            ),
            Self::Observers { requested, .. } => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::OperatorByteCreditCapacity.reason(),
            )
            .with_resource(ResourceFacts {
                request_scope: None,
                constraint_scope: None,
                dimension: ResourceDimension::Items,
                requested: u64::try_from(*requested).ok(),
                required_growth: None,
                limit: Some(129),
                live: None,
                committed: None,
            }),
            Self::Contract(_) => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::OperatorByteCreditContract.reason(),
            ),
            Self::Capacity { requested, limit } => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::OperatorByteCreditCapacity.reason(),
            )
            .with_resource(ResourceFacts {
                request_scope: None,
                constraint_scope: None,
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*requested).ok(),
                required_growth: None,
                limit: u64::try_from(*limit).ok(),
                live: None,
                committed: None,
            }),
        }
    }
}
pub(crate) fn credit_contract(detail: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(CreditFailure::Contract(detail))
}
pub(crate) fn credit_allocation(error: std::collections::TryReserveError) -> ExecutionFailure {
    ExecutionFailure::from_domain(CreditFailure::Allocation(error))
}
pub(crate) fn credit_observer_backing(waiters: usize) -> ExecutionResult<usize> {
    Observable::bounded_backing_bytes(waiters).map_err(|source| {
        if matches!(
            source,
            super::observable::ObservableCapacityError::InvalidCapacity
        ) {
            ExecutionFailure::from_domain(CreditFailure::Observers {
                requested: waiters,
                source,
            })
        } else {
            ExecutionFailure::from_domain(source)
        }
    })
}
pub(crate) fn credit_layout(error: LayoutResourceError) -> ExecutionFailure {
    crate::exec::pipeline::operator::ConstructionFailure::Resource(error).into()
}
fn capacity(requested: usize, limit: usize) -> ExecutionFailure {
    ExecutionFailure::from_domain(CreditFailure::Capacity { requested, limit })
}
fn add(a: usize, b: usize) -> ExecutionResult<usize> {
    a.checked_add(b).ok_or_else(|| capacity(usize::MAX, a))
}
fn multiply(a: usize, b: usize) -> ExecutionResult<usize> {
    a.checked_mul(b).ok_or_else(|| capacity(usize::MAX, a))
}
fn arc_bytes<T>() -> ExecutionResult<usize> {
    arc_layout(Layout::new::<T>())
        .map(|v| v.size())
        .map_err(credit_layout)
}
fn mutex_backing() -> usize {
    // Same pinned std Darwin pthread mutex allocation as Observable's author.
    if cfg!(target_vendor = "apple") {
        std::mem::size_of::<(isize, [u8; 56])>()
    } else {
        0
    }
}

/// Exact occurrence metadata bounds, obtained during preparation. Queue and
/// holder positions are independent, so rows with zero payload cannot create
/// an unbounded descriptor queue. A frame must fit the smaller byte window.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PortCreditSpec {
    pub queue_positions: usize,
    pub holder_positions: usize,
    pub minimum_frame_bytes: usize,
    pub waiters: usize,
}

#[derive(Clone)]
pub(crate) struct OperatorByteCredit {
    ports: Arc<[PortCredit]>,
    envelope_bytes: usize,
    metadata_bytes: usize,
    workspace_bytes: usize,
    _metadata: Arc<TrackedBytes>,
}

#[derive(Clone)]
pub(crate) struct PortCredit(Arc<Port>);
struct Port {
    queue_limit: usize,
    holder_limit: usize,
    queue_positions: usize,
    holder_positions: usize,
    live: Mutex<Live>,
    observable: Arc<Observable>,
    consumer: Option<Arc<Observable>>,
    // The same finite metadata loan covers this port and every detached permit.
    _metadata: Arc<TrackedBytes>,
}
#[derive(Default)]
struct Live {
    queued_bytes: usize,
    holder_bytes: usize,
    queued_positions: usize,
    held_positions: usize,
    closed: bool,
}

pub(crate) enum CreditAcquire {
    Granted(QueuePermit),
    Pending(WaitFact),
    Closed,
}
pub(crate) enum HolderAcquire {
    Granted(HolderPermit),
    Pending { queue: QueuePermit, wait: WaitFact },
    Closed(QueuePermit),
}
/// Linear custody; these owners deliberately implement neither Clone nor any
/// capacity-renewal API. Dropping an owner is the sole way to return its window.
pub(crate) struct QueuePermit {
    port: PortCredit,
    bytes: usize,
    live: bool,
}
pub(crate) struct HolderPermit {
    port: PortCredit,
    bytes: usize,
}
impl ValueTypeStorageHold for HolderPermit {}

/// A BE staging window has no consumer-holder transition. The distinct
/// release-only ticket prevents a second logical staging window from arising.
#[derive(Clone)]
pub(crate) struct StagingCredit(PortCredit);
pub(crate) struct StagingPermit(QueuePermit);
pub(crate) enum StagingAcquire {
    Granted(StagingPermit),
    Pending(WaitFact),
    Closed,
}
impl StagingCredit {
    pub(crate) fn bind(
        limit: usize,
        waiters: usize,
        parent: Arc<MemTracker>,
    ) -> ExecutionResult<Self> {
        // At most one nonzero staging byte per position. Exact flow positions
        // remain additionally bounded by the receive pool's prepared table.
        OperatorByteCredit::bind_staging(limit, limit, waiters, parent)
    }
    pub(crate) fn try_acquire(&self, bytes: usize) -> ExecutionResult<StagingAcquire> {
        if bytes == 0 {
            return Err(credit_contract(
                "zero-byte frames require no staging reservation",
            ));
        }
        Ok(match self.0.try_queue(bytes)? {
            CreditAcquire::Granted(permit) => StagingAcquire::Granted(StagingPermit(permit)),
            CreditAcquire::Pending(wait) => StagingAcquire::Pending(wait),
            CreditAcquire::Closed => StagingAcquire::Closed,
        })
    }
    pub(crate) fn close(&self) {
        self.0.close();
    }
    pub(crate) fn observable(&self) -> Arc<Observable> {
        self.0.observable()
    }
}
impl StagingPermit {
    pub(crate) fn bytes(&self) -> usize {
        self.0.bytes()
    }
}

fn binding_geometry(
    per_contributor_bytes: usize,
    contributors: usize,
    workspace_bytes: usize,
    specs: &[PortCreditSpec],
) -> ExecutionResult<(usize, usize, usize, usize)> {
    if contributors == 0 || specs.is_empty() {
        return Err(credit_contract(
            "an operator needs exact contributors and occurrences",
        ));
    }
    let envelope = multiply(per_contributor_bytes, contributors)?;
    let mut metadata = add(
        arc_bytes::<TrackedBytes>()?,
        add(
            std::mem::size_of::<[usize; 2]>(),
            multiply(multiply(specs.len(), std::mem::size_of::<PortCredit>())?, 2)?,
        )?,
    )?;
    for spec in specs {
        if spec.queue_positions == 0 || spec.holder_positions == 0 {
            return Err(credit_contract(
                "queue and holder positions must be nonzero",
            ));
        }
        let registry = credit_observer_backing(spec.waiters)?;
        metadata = add(
            metadata,
            add(
                arc_bytes::<Port>()?,
                add(mutex_backing(), add(arc_bytes::<Observable>()?, registry)?)?,
            )?,
        )?;
        // A holder can become an Arc-backed release-only lifetime, plus a
        // composite hold joining it to existing source custody. Reserve
        // every possible live holder shell before publication.
        let holder = add(
            arc_bytes::<HolderPermit>()?,
            ValueTypeStorageLifetime::retaining_allocation_layout()
                .map_err(credit_layout)?
                .size(),
        )?;
        metadata = add(metadata, multiply(spec.holder_positions, holder)?)?;
    }
    let available = envelope
        .checked_sub(add(metadata, workspace_bytes)?)
        .ok_or_else(|| {
            capacity(
                add(metadata, workspace_bytes).unwrap_or(usize::MAX),
                envelope,
            )
        })?;
    let share = available / specs.len();
    let queue_limit = share / 2;
    let holder_limit = share - queue_limit;
    for spec in specs {
        if spec.minimum_frame_bytes > queue_limit {
            return Err(capacity(spec.minimum_frame_bytes, queue_limit));
        }
    }
    Ok((envelope, metadata, queue_limit, holder_limit))
}

impl OperatorByteCredit {
    /// Side-effect-free preparation sizing, shared with the actual binder.
    pub(crate) fn preflight(
        per_contributor_bytes: usize,
        contributors: usize,
        workspace_bytes: usize,
        specs: &[PortCreditSpec],
    ) -> ExecutionResult<usize> {
        binding_geometry(per_contributor_bytes, contributors, workspace_bytes, specs)
            .map(|(_, _, q, h)| q.min(h))
    }
    pub(crate) fn check_fixed_reservation(
        per_contributor_bytes: usize,
        contributors: usize,
        reserved: usize,
    ) -> ExecutionResult<()> {
        let envelope = multiply(per_contributor_bytes, contributors)?;
        if reserved > envelope {
            Err(capacity(reserved, envelope))
        } else {
            Ok(())
        }
    }
    pub(crate) fn bind_staging(
        limit: usize,
        positions: usize,
        waiters: usize,
        parent: Arc<MemTracker>,
    ) -> ExecutionResult<StagingCredit> {
        if positions == 0 || limit == 0 {
            return Err(credit_contract(
                "staging bytes and positions must be nonzero",
            ));
        }
        let backing = credit_observer_backing(waiters)?;
        let metadata = add(
            arc_bytes::<TrackedBytes>()?,
            add(
                arc_bytes::<Port>()?,
                add(mutex_backing(), add(arc_bytes::<Observable>()?, backing)?)?,
            )?,
        )?;
        let owner = Arc::new(TrackedBytes::try_new(metadata, parent)?);
        let observable = Arc::new(
            Observable::try_bounded_retained(waiters, owner.clone())
                .map_err(ExecutionFailure::from_domain)?,
        );
        let port = PortCredit(Arc::new(Port {
            queue_limit: limit,
            holder_limit: 0,
            queue_positions: positions,
            holder_positions: 0,
            live: Mutex::new(Live::default()),
            observable,
            consumer: None,
            _metadata: owner,
        }));
        drop(port.0.live.lock().expect("operator staging credit lock"));
        Ok(StagingCredit(port))
    }

    pub(crate) fn bind(
        per_contributor_bytes: usize,
        contributors: usize,
        workspace_bytes: usize,
        specs: &[PortCreditSpec],
        parent: Arc<MemTracker>,
        consumer: Option<Arc<Observable>>,
    ) -> ExecutionResult<Self> {
        let (envelope, metadata, queue_limit, holder_limit) =
            binding_geometry(per_contributor_bytes, contributors, workspace_bytes, specs)?;
        // Metadata alone consumes query funding. The envelope is a logical
        // progress bound and is never charged again as duplicate Arrow data.
        let owner = Arc::new(TrackedBytes::try_new(metadata, parent)?);
        let mut ports = Vec::new();
        ports
            .try_reserve_exact(specs.len())
            .map_err(credit_allocation)?;
        if ports.capacity() != specs.len() {
            return Err(credit_contract(
                "port backing differs from pre-admitted Layout",
            ));
        }
        for spec in specs {
            let observable = Arc::new(
                Observable::try_bounded_retained(spec.waiters, owner.clone())
                    .map_err(ExecutionFailure::from_domain)?,
            );
            let port = PortCredit(Arc::new(Port {
                queue_limit,
                holder_limit,
                queue_positions: spec.queue_positions,
                holder_positions: spec.holder_positions,
                live: Mutex::new(Live::default()),
                observable,
                consumer: consumer.clone(),
                _metadata: owner.clone(),
            }));
            // Materialize any platform mutex backing before concurrent use.
            drop(port.0.live.lock().expect("operator credit lock"));
            ports.push(port);
        }
        Ok(Self {
            ports: ports.into(),
            envelope_bytes: envelope,
            metadata_bytes: metadata,
            workspace_bytes,
            _metadata: owner,
        })
    }
    pub(crate) fn port(&self, index: usize) -> ExecutionResult<PortCredit> {
        self.ports
            .get(index)
            .cloned()
            .ok_or_else(|| credit_contract("unknown input occurrence"))
    }
    pub(crate) fn envelope_bytes(&self) -> usize {
        self.envelope_bytes
    }
    pub(crate) fn metadata_bytes(&self) -> usize {
        self.metadata_bytes
    }
    pub(crate) fn workspace_bytes(&self) -> usize {
        self.workspace_bytes
    }
    pub(crate) fn close(&self) {
        for port in self.ports.iter() {
            port.close();
        }
    }
}
impl PortCredit {
    pub(crate) fn max_frame_bytes(&self) -> usize {
        if self.0.holder_positions == 0 {
            self.0.queue_limit
        } else {
            self.0.queue_limit.min(self.0.holder_limit)
        }
    }
    pub(crate) fn queue_positions(&self) -> usize {
        self.0.queue_positions
    }
    pub(crate) fn observable(&self) -> Arc<Observable> {
        self.0.observable.clone()
    }
    pub(crate) fn try_queue(&self, bytes: usize) -> ExecutionResult<CreditAcquire> {
        if bytes > self.max_frame_bytes() {
            return Err(capacity(bytes, self.max_frame_bytes()));
        }
        let generation = self.0.observable.generation();
        let mut live = self.0.live.lock().expect("operator credit lock");
        if live.closed {
            return Ok(CreditAcquire::Closed);
        }
        if bytes > self.0.queue_limit - live.queued_bytes
            || live.queued_positions == self.0.queue_positions
        {
            return Ok(CreditAcquire::Pending(self.wait(generation)));
        }
        live.queued_bytes += bytes;
        live.queued_positions += 1;
        Ok(CreditAcquire::Granted(QueuePermit {
            port: self.clone(),
            bytes,
            live: true,
        }))
    }
    fn wait(&self, generation: u64) -> WaitFact {
        WaitFact {
            reason: WaitReason::Output,
            observable: self.observable(),
            observed_generation: generation,
            deadline: None,
        }
    }
    fn notify(&self) {
        self.0.observable.notify_observers();
        if let Some(consumer) = &self.0.consumer {
            consumer.notify_observers();
        }
    }
    pub(crate) fn close(&self) {
        self.0.live.lock().expect("operator credit lock").closed = true;
        self.notify();
    }
    #[cfg(test)]
    fn snapshot(&self) -> (usize, usize, usize, usize) {
        let live = self.0.live.lock().unwrap();
        (
            live.queued_bytes,
            live.holder_bytes,
            live.queued_positions,
            live.held_positions,
        )
    }
}
impl QueuePermit {
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
    /// Complete a conservative unpublished output reservation using its actual
    /// observed backing. This only releases unused Q headroom; it cannot grow
    /// a ticket or release any live backing/consumer-holder responsibility.
    pub(crate) fn retain_actual(&mut self, bytes: usize) -> ExecutionResult<()> {
        if bytes > self.bytes || !self.live {
            return Err(credit_contract(
                "actual queue backing exceeds its original reservation",
            ));
        }
        let unused = self.bytes - bytes;
        self.port
            .0
            .live
            .lock()
            .expect("operator credit lock")
            .queued_bytes -= unused;
        self.bytes = bytes;
        if unused != 0 {
            self.port.notify();
        }
        Ok(())
    }

    pub(crate) fn try_into_holder(mut self) -> HolderAcquire {
        let generation = self.port.0.observable.generation();
        let mut live = self.port.0.live.lock().expect("operator credit lock");
        if live.closed {
            drop(live);
            return HolderAcquire::Closed(self);
        }
        if self.bytes > self.port.0.holder_limit - live.holder_bytes
            || live.held_positions == self.port.0.holder_positions
        {
            drop(live);
            let wait = self.port.wait(generation);
            return HolderAcquire::Pending { queue: self, wait };
        }
        live.queued_bytes -= self.bytes;
        live.queued_positions -= 1;
        live.holder_bytes += self.bytes;
        live.held_positions += 1;
        self.live = false;
        drop(live);
        self.port.notify();
        HolderAcquire::Granted(HolderPermit {
            port: self.port.clone(),
            bytes: self.bytes,
        })
    }
}
impl Drop for QueuePermit {
    fn drop(&mut self) {
        if !self.live {
            return;
        }
        let mut live = self.port.0.live.lock().expect("operator credit lock");
        live.queued_bytes -= self.bytes;
        live.queued_positions -= 1;
        drop(live);
        self.port.notify();
    }
}
impl HolderPermit {
    /// The shell and possible composite holder were admitted at binding for
    /// every live holder position. No funding or runtime capability is exposed.
    pub(crate) fn into_storage_lifetime(self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(Arc::new(self))
    }
}
impl Drop for HolderPermit {
    fn drop(&mut self) {
        let mut live = self.port.0.live.lock().expect("operator credit lock");
        live.holder_bytes -= self.bytes;
        live.held_positions -= 1;
        drop(live);
        self.port.notify();
    }
}

#[cfg(test)]
#[path = "operator_byte_credit_tests.rs"]
mod tests;
