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

//! Same metadata Source and dynamically retained original request cells.
//! Provider I/O never owns the WorkOwner. Every foreign poll/Drop/wake occurs
//! after the metadata mutex is released, with original custody restored on unwind.
use futures::task::AtomicWaker;
use novarocks_resource_contract::SharedDependency;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorIoAdmissionPort, ConnectorIoCleanupFuture,
    ConnectorIoProgressSource, ConnectorIoRequest, ConnectorIoRequestSource,
    ConnectorMetadataMaintenanceIoProfile, ConnectorPayloadRetentionGuard,
};
use novarocks_workload_control::joint_admission::{
    AsyncActionAdmission, AsyncActionGrant, AsyncActionSource, JointLane, JointProposalSlotWait,
    JointTryBegin, RetainedPayloadGrant, RootDomainBinding, RootDomainInitialAdmission,
    RootDomainInitialSource, RootPayloadAdmission, RootPayloadReady,
};
use novarocks_workload_control::{WorkError, WorkOwner, WorkScope};
use std::{
    any::Any,
    future::Future,
    num::NonZeroU64,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// One fixed, non-cyclic wake header shared by the original request cells.
/// It contains no body/Cell/Source reference and grants no capacity.
struct MetadataWake {
    ready: AtomicWaker,
}
impl MetadataWake {
    fn new() -> Self {
        Self {
            ready: AtomicWaker::new(),
        }
    }
    fn register(&self, waker: &std::task::Waker) {
        self.ready.register(waker);
    }
    fn wake(&self) {
        self.ready.wake();
    }
}
impl std::task::Wake for MetadataWake {
    fn wake(self: Arc<Self>) {
        self.ready.wake();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.wake();
    }
}

pub(crate) struct MetadataIoAdmission {
    wake: Arc<MetadataWake>,
    original: Mutex<Option<Body>>,
    cleanup: Mutex<CleanupReceipt>,
}
/// One fixed first-wins domain-failure receipt in the same Source. The
/// original result/raw cause moves here before its request cell can be reused.
#[derive(Default)]
struct CleanupReceipt {
    first: Option<ConnectorError>,
    original: Option<Box<dyn Any + Send>>,
    rejected_drop_panic: Option<Box<dyn Any + Send>>,
}
impl CleanupReceipt {
    fn accept_in(
        slot: &Mutex<Self>,
        completion: (Result<(), ConnectorError>, Option<Box<dyn Any + Send>>),
    ) {
        let (result, original) = completion;
        let failure = match result {
            Err(error) => error,
            Ok(()) if original.is_some() => ConnectorError::new(
                ConnectorErrorKind::Internal,
                "cleanup cause contradicts its successful result",
            ),
            Ok(()) => return,
        };
        let rejected = {
            let mut receipt = slot.lock().unwrap_or_else(|p| p.into_inner());
            if receipt.first.is_none() {
                receipt.first = Some(failure);
                receipt.original = original;
                None
            } else {
                Some((failure, original))
            }
        };
        // Original first cause is stable before a later domain error exits.
        // No foreign destructor or wake runs while the receipt Mutex is held.
        if let Err(cause) = catch_unwind(AssertUnwindSafe(|| drop(rejected))) {
            let mut receipt = slot.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                receipt.rejected_drop_panic.is_none(),
                "poisoned cleanup source cannot accept another result"
            );
            receipt.rejected_drop_panic = Some(cause);
        }
    }
}
struct Body {
    root: WorkScope,
    profile: ConnectorMetadataMaintenanceIoProfile,
    source_metadata_bytes: u64,
    domains: RootDomainBinding,
    bound: Vec<SharedDependency>,
    full: Option<Arc<[SharedDependency]>>,
    initial_source: Option<RootDomainInitialSource>,
    payload_slot_wait: Option<JointProposalSlotWait>,
    initial: Option<RootDomainInitialAdmission>,
    payload: Option<RetainedPayloadGrant>,
    // An actually admitted Provider peak remains a conservative retained
    // floor even after its request cell exits. Formation may not consume it.
    provider_owned_floor: u64,
    growth: Option<RootPayloadAdmission>,
    growth_ready: Option<RootPayloadReady>,
    growth_cell: Option<usize>,
    growth_route: Option<SharedDependency>,
    growth_bytes: u64,
    cells: Vec<Arc<RequestCell>>,
    cursor: usize,
    started: bool,
    adopted_initial: bool,
    first_ready: bool,
    settlement: bool,
    failure: Option<WorkError>,
    changed: bool,
    reobserve: bool,
}
struct RequestCell {
    original: Mutex<Option<CellBody>>,
    stream: Mutex<Option<StreamBody>>,
    progress: Mutex<Option<Arc<dyn ConnectorIoProgressSource>>>,
    cleanup_error: Mutex<Option<Box<dyn Any + Send>>>,
    observer: AtomicWaker,
    root: Arc<MetadataWake>,
}
#[derive(Default)]
struct CellBody {
    requested: Option<SharedDependency>,
    owned: Option<NonZeroU64>,
    owned_admitted: bool,
    claimed: bool,
    full: Option<Arc<[SharedDependency]>>,
    extension: Option<SharedDependency>,
    source: Option<AsyncActionSource>,
    slot_wait: Option<JointProposalSlotWait>,
    admission: Option<AsyncActionAdmission>,
    grant: Option<AsyncActionGrant>,
    failure: Option<WorkError>,
}
#[derive(Default)]
struct StreamBody {
    reserved: bool,
    custody_lost: bool,
    future: Option<ConnectorIoCleanupFuture>,
    result: Option<Result<(), ConnectorError>>,
    poll_failure: Option<Box<dyn Any + Send>>,
    drop_failure: Option<Box<dyn Any + Send>>,
}
struct Loan<'a, T> {
    source: &'a Mutex<Option<T>>,
    body: Option<T>,
}
impl<T> Drop for Loan<'_, T> {
    fn drop(&mut self) {
        let mut slot = self.source.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "original metadata loan restored once");
        *slot = self.body.take();
    }
}
fn loan<T>(source: &Mutex<Option<T>>) -> Option<Loan<'_, T>> {
    let body = source.lock().unwrap_or_else(|p| p.into_inner()).take()?;
    Some(Loan {
        source,
        body: Some(body),
    })
}
fn error(error: &WorkError) -> ConnectorError {
    ConnectorError::new(
        match error {
            WorkError::Cancelled(_) => ConnectorErrorKind::Cancelled,
            WorkError::Capacity(_) => ConnectorErrorKind::ResourceExhausted,
            _ => ConnectorErrorKind::Internal,
        },
        format!("original metadata I/O admission refused: {error:?}"),
    )
}
fn busy() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "original metadata cell is being progressed",
    )
}
impl RequestCell {
    fn new(root: Arc<MetadataWake>) -> Self {
        Self {
            original: Mutex::new(Some(CellBody::default())),
            stream: Mutex::new(Some(StreamBody::default())),
            progress: Mutex::new(None),
            cleanup_error: Mutex::new(None),
            observer: AtomicWaker::new(),
            root,
        }
    }
    fn take_cleanup_completion(
        &self,
    ) -> Option<(Result<(), ConnectorError>, Option<Box<dyn Any + Send>>)> {
        let mut slot = self.stream.lock().unwrap_or_else(|p| p.into_inner());
        let stream = slot.as_mut()?;
        if stream.reserved
            || stream.future.is_some()
            || stream.custody_lost
            || stream.poll_failure.is_some()
            || stream.drop_failure.is_some()
        {
            return None;
        }
        let result = stream.result.take()?;
        let cause = self
            .cleanup_error
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        Some((result, cause))
    }
    fn stream_clear(&self) -> bool {
        let clear = self
            .stream
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|s| !s.reserved && s.result.is_none());
        let progress = self
            .progress
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        clear
            && self
                .cleanup_error
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none()
            && progress
                .as_ref()
                .is_none_or(|source| source.actual_exit_observed())
    }
    fn wake(&self) {
        self.observer.wake();
    }
    fn poll_original(&self, cx: &mut Context<'_>) -> bool {
        let mut changed = false;
        let progress = self
            .progress
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(progress) = progress {
            if matches!(progress.poll_original_progress(cx), Poll::Ready(Ok(())))
                && progress.actual_exit_observed()
            {
                let retired = {
                    let mut field = self.progress.lock().unwrap_or_else(|p| p.into_inner());
                    if field
                        .as_ref()
                        .is_some_and(|old| Arc::ptr_eq(old, &progress))
                    {
                        field.take()
                    } else {
                        None
                    }
                };
                drop(retired);
                changed = true;
            }
        }
        let Some(mut original) = loan(&self.stream) else {
            return changed;
        };
        let stream = original.body.as_mut().unwrap();
        if stream.poll_failure.is_some() || stream.drop_failure.is_some() || stream.custody_lost {
            return changed;
        }
        if let Some(future) = stream.future.as_mut() {
            match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(Poll::Pending) => return changed,
                Ok(Poll::Ready(result)) => stream.result = Some(result),
                Err(cause) => stream.poll_failure = Some(cause),
            }
            let future = stream.future.take();
            if let Err(cause) = catch_unwind(AssertUnwindSafe(|| drop(future))) {
                stream.drop_failure = Some(cause);
            }
            if stream.poll_failure.is_none() && stream.drop_failure.is_none() {
                stream.reserved = false;
            }
            changed = true;
        }
        changed
    }
    fn has_health_failure(&self) -> bool {
        self.stream
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|s| s.custody_lost || s.poll_failure.is_some() || s.drop_failure.is_some())
    }
}
impl std::fmt::Debug for RequestCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalMetadataRequestCell")
    }
}
impl std::fmt::Display for RequestCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original metadata stream exit remains unproven")
    }
}
impl std::error::Error for RequestCell {}
impl ConnectorIoRequestSource for RequestCell {
    fn exit_waker(&self) -> std::task::Waker {
        std::task::Waker::from(Arc::clone(&self.root))
    }
    fn reserve_stream(&self) -> Result<(), ConnectorError> {
        let mut original = self.stream.lock().unwrap_or_else(|p| p.into_inner());
        let stream = original.as_mut().ok_or_else(busy)?;
        if stream.reserved {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "original stream disposition is already reserved",
            ));
        }
        stream.reserved = true;
        Ok(())
    }
    fn stream_closed(&self) {
        {
            let mut original = self.stream.lock().unwrap_or_else(|p| p.into_inner());
            let stream = original
                .as_mut()
                .expect("stream close outside original poll");
            assert!(
                stream.future.is_none(),
                "normal close cannot replace cleanup F"
            );
            stream.reserved = false;
        }
        self.root.wake();
    }
    fn install_stream_cleanup_in(
        &self,
        source: &mut Option<ConnectorIoCleanupFuture>,
    ) -> Result<(), ConnectorError> {
        let mut original = self.stream.lock().unwrap_or_else(|p| p.into_inner());
        let stream = original.as_mut().ok_or_else(busy)?;
        if !stream.reserved || stream.future.is_some() || source.is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "original cleanup destination is occupied",
            ));
        }
        stream.future = source.take();
        Ok(())
    }
    fn record_cleanup_failure(
        &self,
        cause: Box<dyn Any + Send>,
    ) -> Result<(), Box<dyn Any + Send>> {
        let mut slot = self.cleanup_error.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(cause);
        }
        *slot = Some(cause);
        Ok(())
    }
    fn stream_custody_lost(&self) {
        // A lost unique disposition is failclosed. Do not callback while unwinding.
        if let Some(stream) = self
            .stream
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
        {
            stream.custody_lost = true;
        }
    }
    fn attach_original_progress(
        &self,
        source: Arc<dyn ConnectorIoProgressSource>,
    ) -> Result<(), ConnectorError> {
        let old = self
            .progress
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if old
            .as_ref()
            .is_some_and(|old| !old.actual_exit_observed() && !Arc::ptr_eq(old, &source))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "one original request already owns an acquisition source",
            ));
        }
        let retired = self
            .progress
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replace(source);
        drop(retired);
        drop(old);
        Ok(())
    }
    fn poll_admitted(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.observer.register(cx.waker());
        let original = self.original.lock().unwrap_or_else(|p| p.into_inner());
        match original.as_ref() {
            Some(body) if body.failure.is_some() => {
                Poll::Ready(Err(error(body.failure.as_ref().unwrap())))
            }
            Some(body)
                if body.extension.is_none()
                    && body.requested.as_ref().is_none_or(|route| {
                        body.full.as_ref().is_some_and(|full| full.contains(route))
                    })
                    && (body.grant.is_some() || body.owned_admitted) =>
            {
                Poll::Ready(Ok(()))
            }
            _ => Poll::Pending,
        }
    }
    fn poll_include_dependency(
        &self,
        dependency: &SharedDependency,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ConnectorError>> {
        self.observer.register(cx.waker());
        let (result, changed) = {
            let mut original = self.original.lock().unwrap_or_else(|p| p.into_inner());
            match original.as_mut() {
                Some(body)
                    if body
                        .full
                        .as_ref()
                        .is_some_and(|facts| facts.contains(dependency)) =>
                {
                    (Poll::Ready(Ok(())), false)
                }
                Some(body) if body.failure.is_some() => (
                    Poll::Ready(Err(error(body.failure.as_ref().unwrap()))),
                    false,
                ),
                Some(body) => {
                    if body.extension.as_ref().is_some_and(|old| old != dependency) {
                        return Poll::Ready(Err(ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "one original request has an unfinished atomic route extension",
                        )));
                    }
                    let changed = body.extension.is_none();
                    body.extension = Some(dependency.clone());
                    (Poll::Pending, changed)
                }
                None => (Poll::Pending, false),
            }
        };
        if changed {
            self.root.wake();
        }
        result
    }
}
/// The same original Source has observed every request/frame and last alias
/// exit. An ordinary admission refusal remains a product fact, not evidence
/// that a physical source is still running. Only this owner constructs it.
#[derive(Debug)]
pub(crate) struct MetadataIoQuiescence {
    ordinary_failure: Option<WorkError>,
}
impl MetadataIoQuiescence {
    pub(crate) fn into_ordinary_failure(self) -> Option<WorkError> {
        self.ordinary_failure
    }
}

impl MetadataIoAdmission {
    pub(crate) const fn request_metadata_bytes() -> u64 {
        std::mem::size_of::<CellBody>() as u64
    }
    pub(crate) fn new(
        root: WorkScope,
        profile: ConnectorMetadataMaintenanceIoProfile,
        source_metadata_bytes: u64,
    ) -> Result<Arc<Self>, WorkError> {
        // Mark the original root before preparation creates any long-lived
        // claims. This observer alone grants neither D nor physical capacity.
        let domains = root.prepare_root_domains()?;
        let wake = Arc::new(MetadataWake::new());
        // One real bootstrap/local-preparation request is required by the
        // original source protocol. Later actual requests grow/reuse this Vec.
        let cells = vec![Arc::new(RequestCell::new(Arc::clone(&wake)))];
        let bound = Vec::new();
        let provider_owned_floor = profile.initial_owned_bytes().get();
        Ok(Arc::new(Self {
            wake,
            cleanup: Mutex::new(CleanupReceipt::default()),
            original: Mutex::new(Some(Body {
                root,
                profile,
                source_metadata_bytes,
                domains,
                bound,
                full: None,
                initial_source: None,
                payload_slot_wait: None,
                initial: None,
                payload: None,
                provider_owned_floor,
                growth: None,
                growth_ready: None,
                growth_cell: None,
                growth_route: None,
                growth_bytes: 0,
                cells,
                cursor: 0,
                started: false,
                adopted_initial: false,
                first_ready: false,
                settlement: false,
                failure: None,
                changed: false,
                reobserve: false,
            })),
        }))
    }
    /// Move the same queued Root payload into its original I/O author. The
    /// caller paid this source's fixed/variable envelope before job publication.
    /// No initial grant is recreated; the original LocalHost D grows through
    /// the same paid Root proposal before the provider can observe Ready.
    pub(crate) fn adopt_original_payload_in(
        &self,
        source: &mut Option<RetainedPayloadGrant>,
    ) -> Result<(), WorkError> {
        let mut original = loan(&self.original).ok_or(WorkError::Conflict)?;
        let body = original.body.as_mut().ok_or(WorkError::Released)?;
        let payload = source.as_ref().ok_or(WorkError::NotReady)?;
        if body.started
            || body.initial.is_some()
            || body.payload.is_some()
            || !payload.is_for_scope(&body.root)
        {
            return Err(WorkError::Conflict);
        }
        // Preserve the exact positive incarnation. Profile/layout numbers
        // are responsibility facts, never a qualification of actual capacity.
        payload.root_domain_source(&body.root, JointLane::Foreground)?;
        body.provider_owned_floor = payload.capacity_bytes().map_err(|_| WorkError::Released)?;
        body.bound.push(SharedDependency::LocalHost);
        body.full = Some(body.bound.clone().into());
        body.payload = source.take();
        body.started = true;
        body.adopted_initial = true;
        body.changed = true;
        Ok(())
    }
    /// Legacy responsibility-only declaration used by callers not yet migrated.
    /// This is not a fixed request inventory or a capacity qualification.
    pub(crate) fn fixed_backing_bytes(n: usize, k: usize) -> Result<u64, WorkError> {
        let arc = std::mem::size_of::<[usize; 2]>();
        let mutex_external = if cfg!(target_os = "macos") {
            128usize
        } else {
            0
        };
        // Inline wait/source fields are already in the actual CellBody. Only
        // the one possible original wait factory F pointee is additional.
        let (_, wait_future) = JointProposalSlotWait::metadata_layouts();
        let per = std::mem::size_of::<RequestCell>()
            .checked_add(wait_future.size())
            .and_then(|v| v.checked_add(arc))
            .and_then(|v| v.checked_add(4 * mutex_external))
            .and_then(|v| v.checked_add(std::mem::size_of::<Arc<RequestCell>>()))
            .and_then(|v| v.checked_add(std::mem::size_of::<Loan<'_, CellBody>>()))
            .and_then(|v| v.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or(WorkError::ArithmeticOverflow)?;
        let facts = k
            .checked_mul(3)
            .and_then(|v| v.checked_mul(std::mem::size_of::<SharedDependency>() + 2 * 1024 + arc))
            .ok_or(WorkError::ArithmeticOverflow)?;
        let bytes = per
            .checked_mul(n)
            // Initial and growth are mutually exclusive original Root phases;
            // they share one fixed wait slot and at most one actual wait F.
            .and_then(|v| v.checked_add(wait_future.size()))
            .and_then(|v| {
                v.checked_add(
                    std::mem::size_of::<Self>()
                        + std::mem::size_of::<Body>()
                        + std::mem::size_of::<MetadataWake>()
                        + 2 * arc
                        + 2 * mutex_external,
                )
            })
            .and_then(|v| v.checked_add(facts))
            .ok_or(WorkError::ArithmeticOverflow)?;
        u64::try_from(bytes).map_err(|_| WorkError::ArithmeticOverflow)
    }
    fn receive_cleanup_completion(&self, cell: &RequestCell) -> bool {
        if self
            .cleanup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .rejected_drop_panic
            .is_some()
        {
            return false;
        }
        let Some(completion) = cell.take_cleanup_completion() else {
            return false;
        };
        CleanupReceipt::accept_in(&self.cleanup, completion);
        true
    }
    pub(crate) fn cleanup_failure(&self) -> Option<ConnectorError> {
        self.cleanup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .first
            .clone()
    }
    pub(crate) fn failure(&self) -> Option<WorkError> {
        self.original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|b| b.failure.clone())
    }
    pub(crate) fn health_failure(
        self: &Arc<Self>,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        if self
            .cleanup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .rejected_drop_panic
            .is_some()
        {
            return Some(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("metadata cleanup cause disposition panicked",Arc::clone(self) as Arc<dyn std::error::Error+Send+Sync>));
        }
        let (direct, progress) = {
            let original = self.original.lock().unwrap_or_else(|p| p.into_inner());
            let body = original.as_ref()?;
            let mut progress = Vec::with_capacity(body.cells.len());
            let mut direct = None;
            for cell in &body.cells {
                if cell.has_health_failure() {
                    direct = Some(Arc::clone(cell) as Arc<dyn std::error::Error + Send + Sync>);
                    break;
                }
                if let Some(source) = cell
                    .progress
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .as_ref()
                {
                    progress.push(Arc::clone(source));
                }
            }
            (direct, progress)
        };
        if let Some(cause) = direct {
            return Some(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("metadata original stream exit unproven",cause));
        }
        for source in progress {
            if let Some(cause) = source.health_cause() {
                return Some(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("metadata original acquisition exit unproven",cause));
            }
        }
        None
    }
    pub(crate) fn poll_progress(
        &self,
        owner: &WorkOwner,
        cx: &mut Context<'_>,
    ) -> Result<bool, WorkError> {
        self.poll_with_original_owner(Some(owner), cx)
    }
    /// Only the same already-positive Root Source may continue after its owner
    /// moved into a stable statement/protocol receiver. Initial construction
    /// still requires that actual owner; aliases cannot enable this method.
    pub(crate) fn poll_continuing(&self, cx: &mut Context<'_>) -> Result<bool, WorkError> {
        self.poll_with_original_owner(None, cx)
    }
    fn poll_with_original_owner(
        &self,
        owner: Option<&WorkOwner>,
        cx: &mut Context<'_>,
    ) -> Result<bool, WorkError> {
        self.wake.register(cx.waker());
        let (result, cells, changed, reobserve) = {
            let Some(mut original) = loan(&self.original) else {
                return Err(WorkError::Conflict);
            };
            let body = original.body.as_mut().unwrap();
            body.changed = false;
            body.reobserve = false;
            let mut result = Self::progress(body, owner, cx);
            if let Err(error) = &result {
                body.failure.get_or_insert_with(|| error.clone());
                // Once physical requests exist, refusal stops new admission;
                // it never turns surviving F/body/cleanup into actual exit.
                if body.first_ready {
                    result = Ok(true);
                }
            }
            let cells = body.cells.iter().cloned().collect::<Vec<_>>();
            (result, cells, body.changed, body.reobserve)
        };
        // Restore original fields before nested credential/abort I/O can ask
        // this same paid source to extend its complete dependency union.
        let mut completed = false;
        for cell in &cells {
            let original_changed = cell.poll_original(cx);
            completed |= self.receive_cleanup_completion(cell);
            if changed || original_changed {
                cell.wake();
            }
        }
        // This is one actual completion publication, not periodic polling.
        if completed {
            self.wake.wake();
        }
        if reobserve {
            self.wake.wake();
        }
        result
    }
    fn progress(
        body: &mut Body,
        owner: Option<&WorkOwner>,
        cx: &mut Context<'_>,
    ) -> Result<bool, WorkError> {
        if !body.started {
            let owner = owner.ok_or(WorkError::NotReady)?;
            if !owner.is_for_scope(&body.root) {
                return Err(WorkError::ForeignAuthority);
            }
            if let Some(error) = &body.failure {
                return Err(error.clone());
            }
            if body.initial.is_none() {
                if let Some(wait) = body.payload_slot_wait.as_mut() {
                    match Pin::new(wait).poll(cx) {
                        Poll::Pending => return Ok(false),
                        Poll::Ready(Ok(())) => drop(body.payload_slot_wait.take()),
                        Poll::Ready(Err(error)) => return Err(error),
                    }
                }
                if body.initial_source.is_none() {
                    body.initial_source = Some(
                        body.domains
                            .initial_payload_source(&body.root, JointLane::Custody)?,
                    );
                }
                body.bound
                    .try_reserve(body.profile.initial_dependencies().len())
                    .map_err(|_| {
                        WorkError::Capacity("metadata dependency record allocation was refused")
                    })?;
                let bytes = body.source_metadata_bytes;
                match body
                    .initial_source
                    .as_ref()
                    .unwrap()
                    .try_begin(
                        NonZeroU64::new(bytes).ok_or(WorkError::ArithmeticOverflow)?,
                        body.profile.initial_owned_bytes(),
                    )
                    .map_err(|failure| failure.error)?
                {
                    JointTryBegin::Started(mut proposal) => {
                        for fact in body.profile.initial_dependencies() {
                            proposal.push_dependency(fact)?;
                        }
                        body.initial = Some(proposal.submit().map_err(|failure| failure.error)?);
                    }
                    JointTryBegin::Full(mut wait) => {
                        match Pin::new(&mut wait).poll(cx) {
                            Poll::Pending => body.payload_slot_wait = Some(wait),
                            Poll::Ready(Ok(())) => body.reobserve = true,
                            Poll::Ready(Err(error)) => return Err(error),
                        }
                        return Ok(false);
                    }
                }
            }
            let notice = match body.initial.as_mut().unwrap().poll_install_in(
                cx,
                &body.root,
                &mut body.payload,
            ) {
                Poll::Pending => return Ok(false),
                Poll::Ready(r) => r.map_err(|e| e.error)?,
            };
            // The positive payload is already stable in the original field.
            // Retire the completed admission's empty batch, not that payload.
            drop(body.initial.take());
            body.bound
                .extend_from_slice(body.profile.initial_dependencies());
            body.full = Some(body.bound.clone().into());
            body.started = true;
            loan(&body.cells[0].original)
                .ok_or(WorkError::Conflict)?
                .body
                .as_mut()
                .unwrap()
                .requested = body.bound.first().cloned();
            body.changed = true;
            notice.notify();
        }
        if body.adopted_initial
            && !body.first_ready
            && body.growth.is_none()
            && body.growth_ready.is_none()
            && body.growth_cell.is_none()
        {
            let route = body
                .profile
                .initial_dependencies()
                .iter()
                .find(|route| !body.bound.contains(route))
                .cloned()
                .unwrap_or(SharedDependency::LocalHost);
            let mut first = loan(&body.cells[0].original).ok_or(WorkError::Conflict)?;
            let first = first.body.as_mut().unwrap();
            first.extension = (!body.bound.contains(&route)).then(|| route.clone());
            first.requested = Some(route);
        }
        if body.growth.is_none() && body.growth_ready.is_none() && body.growth_cell.is_none() {
            for original in &mut body.cells {
                if !original.stream_clear() || Arc::get_mut(original).is_none() {
                    continue;
                }
                let mut cell = loan(&original.original).ok_or(WorkError::Conflict)?;
                if cell.body.as_ref().unwrap().claimed {
                    let retired = std::mem::take(cell.body.as_mut().unwrap());
                    drop(retired);
                }
            }
        }
        let mut cells = Vec::with_capacity(body.cells.len());
        for cell in &body.cells {
            cells.push(loan(&cell.original).ok_or(WorkError::Conflict)?);
        }
        if let Some(wait) = body.payload_slot_wait.as_mut() {
            match Pin::new(wait).poll(cx) {
                Poll::Pending => return Ok(body.first_ready),
                Poll::Ready(Ok(())) => {
                    drop(body.payload_slot_wait.take());
                    // The same pending route/peak and actual holder set were
                    // frozen while no paid constructor had a Q cell.
                    body.cursor = body.growth_cell.ok_or(WorkError::Conflict)?;
                }
                Poll::Ready(Err(error)) => {
                    let i = body.growth_cell.take().ok_or(WorkError::Conflict)?;
                    cells[i].body.as_mut().unwrap().failure = Some(error.clone());
                    body.failure.get_or_insert(error);
                    body.changed = true;
                    drop(body.payload_slot_wait.take());
                    body.growth_route.take();
                    body.growth_bytes = 0;
                    return Ok(body.first_ready);
                }
            }
        }
        // First finish the ONE original atomic replacement. Its active grant set
        // cannot change or be reclaimed while this exact receipt is outstanding.
        if let Some(admission) = body.growth.as_mut() {
            if let Poll::Ready(result) = Pin::new(admission).poll(cx) {
                match result {
                    Ok(ready) => body.growth_ready = Some(ready),
                    Err(failure) => {
                        let i = body.growth_cell.take().unwrap();
                        cells[i].body.as_mut().unwrap().failure = Some(failure.error.clone());
                        body.failure.get_or_insert(failure.error);
                        body.changed = true;
                        body.growth.take();
                        body.growth_route.take();
                        body.growth_bytes = 0;
                    }
                }
            }
        }
        if let Some(ready) = body.growth_ready.as_mut() {
            let result = ready.install_domains_in(&body.root, body.payload.as_ref().unwrap());
            let notice = match result {
                Ok(notice) => notice,
                Err(error) => {
                    let i = body.growth_cell.take().unwrap();
                    cells[i].body.as_mut().unwrap().failure = Some(error.clone());
                    body.failure.get_or_insert(error);
                    body.changed = true;
                    let ready = body.growth_ready.take();
                    let admission = body.growth.take();
                    body.growth_route.take();
                    body.growth_bytes = 0;
                    drop(ready);
                    drop(admission);
                    return Ok(body.first_ready);
                }
            };
            if let Some(route) = body.growth_route.take() {
                body.bound.push(route);
            }
            body.full = Some(body.bound.clone().into());
            let i = body.growth_cell.take().unwrap();
            if body.growth_bytes != 0 {
                let cell = cells[i].body.as_mut().unwrap();
                cell.owned_admitted = true;
                if let Some(required) = cell.owned {
                    body.provider_owned_floor = body.provider_owned_floor.max(required.get());
                }
            }
            for cell in &mut cells {
                let cell = cell.body.as_mut().unwrap();
                if cell.grant.is_some() || cell.owned_admitted {
                    cell.full = body.full.clone();
                    if cell.extension.as_ref().is_some_and(|route| {
                        body.full.as_ref().is_some_and(|full| full.contains(route))
                    }) {
                        cell.extension = None;
                    }
                }
            }
            body.growth.take();
            body.growth_ready.take();
            body.growth_bytes = 0;
            body.changed = true;
            notice.notify();
        }
        if body.growth.is_some() {
            return Ok(body.first_ready);
        }
        if let Some(i) = body.growth_cell {
            // A wait may have observed Free during its very first poll. Keep
            // that exact original pending intent first on the next turn too.
            body.cursor = i;
        }
        for offset in 0..cells.len() {
            let i = (body.cursor + offset) % cells.len();
            let cell = cells[i].body.as_ref().unwrap();
            if cell.failure.is_some() {
                continue;
            }
            let requested = cell.extension.as_ref().or(cell.requested.as_ref()).cloned();
            let route = requested
                .as_ref()
                .filter(|r| !body.bound.contains(r))
                .cloned();
            // A required peak reuses the same Root envelope; it is not a
            // cumulative debit for each temporary plan that has already exited.
            let required = cell
                .owned
                .filter(|_| !cell.owned_admitted)
                .map_or(0, NonZeroU64::get);
            let capacity = body
                .payload
                .as_ref()
                .unwrap()
                .capacity_bytes()
                .map_err(|_| WorkError::Conflict)?;
            let bytes = required.saturating_sub(capacity);
            if required != 0 {
                tracing::debug!(
                    requested_total = required,
                    paid_payload = capacity,
                    positive_growth = bytes,
                    "Original metadata payload invoice observed"
                );
            }
            if required != 0 && bytes == 0 {
                body.provider_owned_floor = body.provider_owned_floor.max(required);
                let cell = cells[i].body.as_mut().unwrap();
                cell.owned_admitted = true;
                cell.full = body.full.clone();
                if cell.extension.as_ref().is_some_and(|route| {
                    body.full.as_ref().is_some_and(|full| full.contains(route))
                }) {
                    cell.extension = None;
                }
                body.changed = true;
            }
            if route.is_some() || bytes != 0 {
                if route.is_some() {
                    body.bound.try_reserve(1).map_err(|_| {
                        WorkError::Capacity("metadata dependency record allocation was refused")
                    })?;
                }
                // Undelivered all-New actions have not started physical I/O.
                // Withdraw them before rekeying the same control source; after
                // install they seek a fresh finite intent from its new fence.
                for source in &mut cells {
                    let source = source.body.as_mut().unwrap();
                    if source.grant.is_none() {
                        drop(source.admission.take());
                        drop(source.slot_wait.take());
                        drop(source.source.take());
                    }
                }
                let lane = if body.settlement {
                    JointLane::Settlement
                } else {
                    JointLane::Foreground
                };
                let construction = (|| {
                    let source = body
                        .payload
                        .as_ref()
                        .unwrap()
                        .root_domain_source(&body.root, lane)?;
                    match source.try_begin(bytes)? {
                        JointTryBegin::Started(mut proposal) => {
                            if let Some(route) = route.as_ref() {
                                proposal.push_dependency(route)?;
                            }
                            proposal.end_change()?;
                            proposal
                                .submit()
                                .map(JointTryBegin::Started)
                                .map_err(|failure| failure.error)
                        }
                        JointTryBegin::Full(wait) => Ok(JointTryBegin::Full(wait)),
                    }
                })();
                match construction {
                    Ok(JointTryBegin::Started(proposal)) => {
                        body.growth = Some(proposal);
                        body.growth_cell = Some(i);
                        body.growth_route = route;
                        body.growth_bytes = bytes;
                        body.reobserve = true;
                    }
                    Ok(JointTryBegin::Full(mut wait)) => {
                        // Full has no paid Q intent. Keep the same original
                        // positive holders and route/peak until real reobserve.
                        body.growth_cell = Some(i);
                        body.growth_route = route;
                        body.growth_bytes = bytes;
                        match Pin::new(&mut wait).poll(cx) {
                            Poll::Pending => body.payload_slot_wait = Some(wait),
                            Poll::Ready(Ok(())) => body.reobserve = true,
                            Poll::Ready(Err(error)) => {
                                cells[i].body.as_mut().unwrap().failure = Some(error.clone());
                                body.failure.get_or_insert(error);
                                body.changed = true;
                                body.growth_cell.take();
                                body.growth_route.take();
                                body.growth_bytes = 0;
                            }
                        }
                    }
                    Err(error) => {
                        cells[i].body.as_mut().unwrap().failure = Some(error.clone());
                        body.failure.get_or_insert(error);
                        body.changed = true;
                        body.growth_cell.take();
                        body.growth_route.take();
                        body.growth_bytes = 0;
                    }
                }
                break;
            }
            let cell = cells[i].body.as_mut().unwrap();
            if requested.is_none() || cell.owned.is_some() || cell.grant.is_some() {
                continue;
            }
            if body.failure.is_some() && !body.settlement {
                cell.failure = body.failure.clone();
                body.changed = true;
                continue;
            }
            if cell.admission.is_none() {
                let lane = if body.settlement {
                    JointLane::Settlement
                } else {
                    JointLane::Foreground
                };
                if let Some(wait) = cell.slot_wait.as_mut() {
                    match Pin::new(wait).poll(cx) {
                        Poll::Pending => continue,
                        Poll::Ready(Ok(())) => {
                            drop(cell.slot_wait.take());
                        }
                        Poll::Ready(Err(error)) => {
                            cell.failure = Some(error.clone());
                            body.failure.get_or_insert(error);
                            body.changed = true;
                            drop(cell.slot_wait.take());
                            continue;
                        }
                    }
                }
                if cell.source.is_none() {
                    // Capture from the original positive Root payload. The
                    // exact original Root/epoch/full D/lane are authenticated
                    // both here and by the original joint proof at dispatch.
                    match body
                        .payload
                        .as_ref()
                        .ok_or(WorkError::NotReady)?
                        .async_continuation_source(&body.root, lane)
                    {
                        Ok(source) => cell.source = Some(source),
                        Err(error) => {
                            cell.failure = Some(error.clone());
                            body.failure.get_or_insert(error);
                            body.changed = true;
                            continue;
                        }
                    }
                }
                // Full means only the original fixed proposal inventory is
                // temporarily occupied. Keep this exact captured source and
                // request, and keep receiving other cells' original replies.
                match cell.source.as_ref().unwrap().try_begin(
                    NonZeroU64::new(Self::request_metadata_bytes()).unwrap(),
                    NonZeroU64::new(1).unwrap(),
                ) {
                    Ok(JointTryBegin::Started(admission)) => cell.admission = Some(admission),
                    Ok(JointTryBegin::Full(mut wait)) => {
                        // Register the actual release observer before leaving
                        // this turn; an unpolled wait would have no wake source.
                        match Pin::new(&mut wait).poll(cx) {
                            Poll::Pending => cell.slot_wait = Some(wait),
                            Poll::Ready(Ok(())) => {
                                // A real Free cut raced the initial Full fact.
                                // Reobserve once, never repoll a completed wait.
                                body.reobserve = true;
                            }
                            Poll::Ready(Err(error)) => {
                                cell.failure = Some(error.clone());
                                body.failure.get_or_insert(error);
                                body.changed = true;
                            }
                        }
                        continue;
                    }
                    Err(failure) => {
                        cell.failure = Some(failure.error.clone());
                        body.failure.get_or_insert(failure.error);
                        body.changed = true;
                        continue;
                    }
                }
            }
            match cell
                .admission
                .as_mut()
                .unwrap()
                .poll_install_in(cx, &mut cell.grant)
            {
                Poll::Pending => {}
                Poll::Ready(Ok(())) => {
                    cell.full = body.full.clone();
                    body.changed = true;
                }
                Poll::Ready(Err(failure)) => {
                    cell.failure = Some(failure.error.clone());
                    body.failure.get_or_insert(failure.error);
                    body.changed = true;
                }
            }
        }
        body.cursor = (body.cursor + 1) % body.cells.len();
        body.first_ready |= cells[0].body.as_ref().unwrap().grant.is_some()
            && (!body.adopted_initial
                || body
                    .profile
                    .initial_dependencies()
                    .iter()
                    .all(|route| body.bound.contains(route)));
        if !body.first_ready {
            if let Some(error) = &body.failure {
                return Err(error.clone());
            }
        }
        Ok(body.first_ready)
    }
    /// Borrow the same positive Root payload; no computation position is retained.
    pub(crate) fn root_capacity(
        &self,
    ) -> Result<
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        WorkError,
    > {
        let loan = loan(&self.original).ok_or(WorkError::Conflict)?;
        let body = loan.body.as_ref().ok_or(WorkError::NotReady)?;
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::from_internal_payload(
            &body.root, &body.root, body.payload.as_ref().ok_or(WorkError::NotReady)?)
    }

    /// The caller has retired the failed computation's original producer,
    /// output and aliases. Keep every actually admitted Provider peak, and
    /// subtract at most that computation's formation increment. No domain or
    /// unreceived production responsibility is released here.
    pub(crate) fn retire_unaccepted_formation_in(
        &self,
        formation: &mut Option<u64>,
    ) -> Result<Option<novarocks_workload_control::WorkloadProgressNotification>, WorkError> {
        let Some(bytes) = *formation else {
            return Ok(None);
        };
        let mut original = loan(&self.original).ok_or(WorkError::Conflict)?;
        let body = original.body.as_mut().ok_or(WorkError::Released)?;
        if body.growth.is_some()
            || body.growth_ready.is_some()
            || body.growth_cell.is_some()
            || body.payload_slot_wait.is_some()
            || body
                .cells
                .iter()
                .any(|cell| !cell.stream_clear() || Arc::strong_count(cell) != 1)
        {
            return Err(WorkError::NotReady);
        }
        let payload = body.payload.as_ref().ok_or(WorkError::NotReady)?;
        let current = payload.capacity_bytes().map_err(|_| WorkError::Released)?;
        let floor = current
            .checked_sub(bytes)
            .ok_or(WorkError::Conflict)?
            .max(body.provider_owned_floor);
        let floor = NonZeroU64::new(floor).ok_or(WorkError::Conflict)?;
        let notice = payload.retire_unused_to(floor)?;
        // Stable disposition precedes any foreign notification. Re-poll and
        // notification panic cannot subtract the original increment twice.
        *formation = None;
        Ok(Some(notice))
    }

    /// Borrow the original Root payload without exposing its raw claim or
    /// creating another budget. The caller owns the original direct stage.
    pub(crate) fn root_stage_source(
        &self,
        stage: &WorkOwner,
    ) -> Result<novarocks_workload_control::joint_admission::RootStageSource, WorkError> {
        let mut original = loan(&self.original).ok_or(WorkError::Conflict)?;
        let body = original.body.as_mut().ok_or(WorkError::Released)?;
        body.payload
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .root_stage_source(&body.root, stage)
    }

    pub(crate) fn install_stage_in(
        &self,
        ready: &mut novarocks_workload_control::joint_admission::RootStageReady,
        stage: &WorkOwner,
        target: &mut Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    ) -> Result<novarocks_workload_control::WorkloadProgressNotification, WorkError> {
        let mut original = loan(&self.original).ok_or(WorkError::Conflict)?;
        let body = original.body.as_mut().ok_or(WorkError::Released)?;
        ready.install_in(
            &body.root,
            body.payload.as_ref().ok_or(WorkError::NotReady)?,
            stage,
            target,
        )
    }

    pub(crate) fn stage_capacity(
        &self,
        stage: &WorkOwner,
        compute: &novarocks_workload_control::joint_admission::InternalComputationGrant,
    ) -> Result<
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        WorkError,
    > {
        let mut original = loan(&self.original).ok_or(WorkError::Conflict)?;
        let body = original.body.as_mut().ok_or(WorkError::Released)?;
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::from_internal_computation(
            &body.root, stage, body.payload.as_ref().ok_or(WorkError::NotReady)?, compute)
    }

    pub(crate) fn install_client_stage_capacity_in(
        &self,
        window: &novarocks_workload_control::ResultWindowAlias,
        stage: &WorkOwner,
        compute: &mut Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
        target: &mut Option<
            novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        >,
    ) -> Result<(), WorkError> {
        let mut original = loan(&self.original).ok_or(WorkError::Conflict)?;
        let body = original.body.as_mut().ok_or(WorkError::Released)?;
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::from_owned_client_computation_in(
            &body.root, stage, window, body.payload.as_ref().ok_or(WorkError::NotReady)?, compute, target,
        )
    }
    pub(crate) fn poll_quiescent(
        &self,
        owner: &WorkOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        self.poll_quiescent_with_owner(Some(owner), cx)
    }
    pub(crate) fn poll_quiescent_continuing(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        self.poll_quiescent_with_owner(None, cx)
    }
    /// A migrated residual consumes actual exit separately from the original
    /// business refusal. Raw health and structural custody failures still
    /// refuse retirement; absence of a health flag is never an exit receipt.
    pub(crate) fn poll_actual_quiescent_continuing(
        self: &Arc<Self>,
        cx: &mut Context<'_>,
    ) -> Poll<
        Result<
            MetadataIoQuiescence,
            novarocks_query_application::api::StatementNativeHealthFailure,
        >,
    > {
        if let Some(failure) = self.health_failure() {
            return Poll::Ready(Err(failure));
        }
        let observed = self.poll_quiescence_with_owner(None, cx);
        // Progress may just have observed the original stream poll/Drop panic.
        // Preserve that same Source/cause even when the normal exit is Pending.
        if let Some(failure) = self.health_failure() {
            return Poll::Ready(Err(failure));
        }
        observed.map(|result| {
            result.map_err(|error| {
                novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "metadata original quiescence could not be established",
                    Arc::new(error),
                )
            })
        })
    }
    fn poll_quiescent_with_owner(
        &self,
        owner: Option<&WorkOwner>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        // Keep the original business-error contract for unmigrated callers.
        self.poll_quiescence_with_owner(owner, cx)
            .map(|result| result.and_then(|observed| observed.ordinary_failure.map_or(Ok(()), Err)))
    }
    fn poll_quiescence_with_owner(
        &self,
        owner: Option<&WorkOwner>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<MetadataIoQuiescence, WorkError>> {
        if let Err(error) = self.poll_with_original_owner(owner, cx) {
            return Poll::Ready(Err(error));
        }
        let Some(mut original) = loan(&self.original) else {
            return Poll::Ready(Err(WorkError::Conflict));
        };
        let body = original.body.as_mut().unwrap();
        if body.growth.is_some()
            || body.growth_ready.is_some()
            || body.growth_cell.is_some()
            || body.payload_slot_wait.is_some()
        {
            return Poll::Pending;
        }
        for original in &mut body.cells {
            if !original.stream_clear() || Arc::get_mut(original).is_none() {
                return Poll::Pending;
            }
            let mut cell = loan(&original.original).ok_or(WorkError::Conflict)?;
            let retired = std::mem::take(cell.body.as_mut().unwrap());
            drop(retired);
        }
        Poll::Ready(Ok(MetadataIoQuiescence {
            ordinary_failure: body.failure.clone(),
        }))
    }
    fn request_in(
        &self,
        dependency: Option<SharedDependency>,
        owned: Option<NonZeroU64>,
    ) -> Result<ConnectorIoRequest, ConnectorError> {
        if self
            .cleanup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .rejected_drop_panic
            .is_some()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "metadata cleanup source disposition is unproven",
            ));
        }
        let Some(mut source) = loan(&self.original) else {
            return Err(busy());
        };
        let body = source.body.as_mut().unwrap();
        if !body.settlement {
            if let Some(failure) = &body.failure {
                return Err(error(failure));
            }
        }
        for (index, cell) in body.cells.iter_mut().enumerate() {
            // Keep the original bootstrap request intact until its own first
            // readiness cut; an early new route must not overwrite that fact.
            if index == 0 && !body.first_ready {
                continue;
            }
            // The original route/peak request cannot be overwritten while its
            // constructor awaits Q, even if its borrowing caller has exited.
            if body.growth_cell == Some(index) {
                continue;
            }
            if !cell.stream_clear() || Arc::get_mut(cell).is_none() {
                continue;
            }
            let mut original = cell.original.lock().unwrap_or_else(|p| p.into_inner());
            let state = original.as_mut().ok_or_else(busy)?;
            if owned.is_some() && !state.claimed && state.grant.is_some() {
                continue;
            }
            // An outstanding batch freezes its exact original physical rows.
            // Their completed request cell becomes reusable after that batch.
            if state.claimed
                && state.grant.is_some()
                && (body.growth.is_some()
                    || body.growth_ready.is_some()
                    || body.growth_cell.is_some())
            {
                continue;
            }
            let retired = if state.claimed {
                Some(std::mem::take(state))
            } else {
                None
            };
            state.requested = dependency.clone();
            state.owned = owned;
            state.claimed = true;
            if let Some(dependency) = dependency.as_ref() {
                if state.grant.is_some()
                    && !state.full.as_ref().is_some_and(|f| f.contains(dependency))
                {
                    state.extension = Some(dependency.clone());
                }
            }
            let request = ConnectorIoRequest::from_original(
                Arc::clone(cell) as Arc<dyn ConnectorIoRequestSource>
            );
            drop(original);
            drop(source);
            drop(retired);
            self.wake.wake();
            return Ok(request);
        }
        body.cells.try_reserve(1).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "metadata request record allocation was refused",
            )
        })?;
        let cell = Arc::new(RequestCell::new(Arc::clone(&self.wake)));
        {
            let mut original = cell.original.lock().unwrap_or_else(|p| p.into_inner());
            let state = original.as_mut().expect("new original request cell");
            state.requested = dependency;
            state.owned = owned;
            state.claimed = true;
        }
        let request = ConnectorIoRequest::from_original(
            Arc::clone(&cell) as Arc<dyn ConnectorIoRequestSource>
        );
        body.cells.push(cell);
        drop(source);
        self.wake.wake();
        Ok(request)
    }
}
impl ConnectorIoAdmissionPort for MetadataIoAdmission {
    fn configure_read_source(
        &self,
        profile: &ConnectorMetadataMaintenanceIoProfile,
    ) -> Result<(), ConnectorError> {
        let mut original = loan(&self.original).ok_or_else(busy)?;
        let body = original.body.as_mut().expect("same original source loan");
        // Configuration describes the real Provider source. It grants no I/O:
        // request/poll_admitted still install each exact selected dependency
        // into this same Root before a Provider can start that request.
        let _ = profile;
        if !body.settlement {
            body.root.check().map_err(|failure| error(&failure))?;
            if let Some(failure) = &body.failure {
                return Err(error(failure));
            }
        }
        Ok(())
    }
    fn poll_read_source_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.wake.register(cx.waker());
        let original = self.original.lock().unwrap_or_else(|p| p.into_inner());
        let Some(body) = original.as_ref() else {
            return Poll::Pending;
        };
        if let Some(failure) = &body.failure {
            return Poll::Ready(Err(error(failure)));
        }
        if body.first_ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn request(&self, dependency: SharedDependency) -> Result<ConnectorIoRequest, ConnectorError> {
        self.request_in(Some(dependency), None)
    }
    fn request_owned(&self, additional: NonZeroU64) -> Result<ConnectorIoRequest, ConnectorError> {
        self.request_in(None, Some(additional))
    }
    fn publication_obligation_started(&self) {
        if let Some(body) = self
            .original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
        {
            body.settlement = true;
        }
    }
    fn retain_payload(&self) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        let original = self.original.lock().unwrap_or_else(|p| p.into_inner());
        let body = original.as_ref().ok_or_else(busy)?;
        let alias = body
            .payload
            .as_ref()
            .ok_or_else(busy)?
            .retain_alias()
            .map_err(|_| busy())?;
        Ok(ConnectorPayloadRetentionGuard::from_existing_lease(
            alias.into_retention(),
        ))
    }
}

impl std::fmt::Debug for MetadataIoAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalMetadataIoSource")
    }
}
impl std::fmt::Display for MetadataIoAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original metadata cleanup cause disposition failed")
    }
}
impl std::error::Error for MetadataIoAdmission {}

#[cfg(test)]
#[path = "io_admission/cleanup_tests.rs"]
pub(super) mod cleanup_tests;

#[cfg(test)]
mod original_governor_tests {
    use super::*;
    use novarocks_spi::connector::ConnectorIoPermit;
    use novarocks_workload_control::{
        ControlStorageConfig, ResponsibilityConfig, ResultCapacityConfig, RootWork, WorkClass,
        WorkRequest, WorkloadConfig, WorkloadControl,
        joint_admission::{JointProgress, JointProgressDriver},
    };
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Wake, Waker},
    };

    #[derive(Default)]
    struct OriginalWake(AtomicUsize);
    impl Wake for OriginalWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    // This creates the real original substrate and actual result configuration;
    // neither the dependency count nor profile geometry is a quota.
    fn original_governor() -> (WorkloadControl, JointProgressDriver, RootWork) {
        let count = 8;
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        let (_, driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(count, &[]))
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        (control, driver, root)
    }

    fn drive_original(driver: &mut JointProgressDriver) {
        for _ in 0..100000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("same finite metadata component driver failed to become idle");
    }

    fn assert_original_healthy(source: &MetadataIoAdmission) {
        let original = source.original.lock().unwrap();
        let body = original.as_ref().unwrap();
        assert!(body.failure.is_none());
        for cell in &body.cells {
            assert!(
                cell.original
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .failure
                    .is_none()
            );
        }
    }

    #[test]
    fn original_root_responsibility_growth_does_not_change_the_actual_result_budget() {
        let (control, mut driver, root) = original_governor();
        let source_bytes = MetadataIoAdmission::fixed_backing_bytes(2, 1).unwrap();
        let profile = ConnectorMetadataMaintenanceIoProfile::try_new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(source_bytes).unwrap(),
            NonZeroU64::new(128).unwrap(),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        let source = MetadataIoAdmission::new(root.owner.scope(), profile, source_bytes).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = false;
        for _ in 0..256 {
            ready = source.poll_progress(&root.owner, &mut cx).unwrap();
            if ready {
                break;
            }
            drive_original(&mut driver);
        }
        assert!(ready);
        // Borrow exactly the same API as Statistics ReadOriginalCheck BEFORE
        // either prospective request; no fresh alias can conceal a stale cap.
        let original = source.root_capacity().unwrap();
        original.window_alias().check_backing_total(128).unwrap();
        let actual_budget = ResultCapacityConfig::V1.all_objects_bytes[2];
        let held = original.window_alias().reserve_payload_bytes(64).unwrap();
        let recorded = || {
            source
                .original
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .payload
                .as_ref()
                .unwrap()
                .capacity_bytes()
                .unwrap()
        };
        for required in [768, 1536] {
            let before = recorded();
            assert!(before < required);
            original
                .window_alias()
                .check_backing_total(required)
                .unwrap();
            let mut request = Box::pin(
                source
                    .request_owned(NonZeroU64::new(required).unwrap())
                    .unwrap()
                    .admitted(),
            );
            assert!(request.as_mut().poll(&mut cx).is_pending());
            assert_eq!(
                recorded(),
                before,
                "waiting cannot install responsibility growth"
            );
            assert_eq!(original.window_alias().retained_payload_bytes(), 64);
            let mut permit = None;
            for _ in 0..256 {
                source.poll_progress(&root.owner, &mut cx).unwrap();
                drive_original(&mut driver);
                if let Poll::Ready(result) = request.as_mut().poll(&mut cx) {
                    permit = Some(result.unwrap());
                    break;
                }
            }
            let permit = permit.expect("same original Root positive growth installed");
            original
                .window_alias()
                .check_backing_total(required)
                .unwrap();
            assert!(
                original
                    .window_alias()
                    .check_backing_total(actual_budget + 1)
                    .is_err()
            );
            assert!(recorded() >= required);
            assert_eq!(original.window_alias().retained_payload_bytes(), 64);
            drop((request, permit));
            assert!(source.poll_quiescent(&root.owner, &mut cx).is_ready());
            drive_original(&mut driver);
        }
        assert_original_healthy(&source);
        drop(held);
        assert_eq!(original.window_alias().retained_payload_bytes(), 0);
        drop((original, source));
        root.business.release();
        root.owner.complete();
        control.close_admission();
        drive_original(&mut driver);
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn retired_formation_preserves_admitted_provider_peak_and_actual_request_exit() {
        let (control, mut driver, root) = original_governor();
        let source_bytes = MetadataIoAdmission::fixed_backing_bytes(2, 1).unwrap();
        let profile = ConnectorMetadataMaintenanceIoProfile::try_new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(source_bytes).unwrap(),
            NonZeroU64::new(128).unwrap(),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        let source = MetadataIoAdmission::new(root.owner.scope(), profile, source_bytes).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = false;
        for _ in 0..256 {
            ready = source.poll_progress(&root.owner, &mut cx).unwrap();
            if ready {
                break;
            }
            drive_original(&mut driver);
        }
        assert!(ready);
        // Same original Root author as Stage's positive payload growth.
        let mut growth = {
            let original = source.original.lock().unwrap();
            let body = original.as_ref().unwrap();
            let author = body
                .payload
                .as_ref()
                .unwrap()
                .root_domain_source(&body.root, JointLane::Foreground)
                .unwrap();
            let JointTryBegin::Started(mut proposal) = author.try_begin(1024).unwrap() else {
                panic!("same original Root Q available");
            };
            proposal.end_change().unwrap();
            proposal.submit().unwrap()
        };
        drive_original(&mut driver);
        let Poll::Ready(Ok(mut result)) = Pin::new(&mut growth).poll(&mut cx) else {
            panic!("original Root growth proof");
        };
        {
            let original = source.original.lock().unwrap();
            let body = original.as_ref().unwrap();
            result
                .install_domains_in(&body.root, body.payload.as_ref().unwrap())
                .unwrap()
                .notify();
        }
        drop((result, growth));
        drive_original(&mut driver);
        let alias = {
            let original = source.original.lock().unwrap();
            original
                .as_ref()
                .unwrap()
                .payload
                .as_ref()
                .unwrap()
                .retain_alias()
                .unwrap()
        };
        assert_eq!(alias.capacity_bytes().unwrap(), 1152);
        // This actual Provider request fits inside formation, so causes no
        // growth. Its original admitted peak must nevertheless survive release.
        let mut request = Box::pin(
            source
                .request_owned(NonZeroU64::new(768).unwrap())
                .unwrap()
                .admitted(),
        );
        let mut permit = None;
        for _ in 0..256 {
            source.poll_progress(&root.owner, &mut cx).unwrap();
            drive_original(&mut driver);
            if let Poll::Ready(result) = request.as_mut().poll(&mut cx) {
                permit = Some(result.unwrap());
                break;
            }
        }
        let permit = permit.expect("actual original Provider peak admitted");
        let mut formation = Some(1024);
        assert!(
            matches!(
                source.retire_unaccepted_formation_in(&mut formation),
                Err(WorkError::NotReady)
            ),
            "a live original request refuses formation retirement"
        );
        assert_eq!(formation, Some(1024));
        assert_eq!(alias.capacity_bytes().unwrap(), 1152);
        drop((request, permit));
        assert!(source.poll_quiescent(&root.owner, &mut cx).is_ready());
        let notice = source
            .retire_unaccepted_formation_in(&mut formation)
            .unwrap()
            .unwrap();
        assert!(formation.is_none());
        notice.notify();
        assert_eq!(
            alias.capacity_bytes().unwrap(),
            768,
            "keep the admitted Provider peak, not only the pre-stage 128 bytes"
        );
        alias.check_backing_total(768).unwrap();
        assert!(
            source
                .retire_unaccepted_formation_in(&mut formation)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            alias.capacity_bytes().unwrap(),
            768,
            "no repeated subtraction"
        );
        {
            let original = source.original.lock().unwrap();
            let body = original.as_ref().unwrap();
            assert_eq!(body.bound, vec![SharedDependency::LocalHost]);
            assert_eq!(body.provider_owned_floor, 768);
        }
        drop((alias, source));
        root.business.release();
        root.owner.complete();
        control.close_admission();
        drive_original(&mut driver);
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn queued_root_payload_is_adopted_and_provider_domains_are_proved_before_ready() {
        let (control, mut driver, root) = original_governor();
        let scope = root.owner.scope();
        let source_bytes = MetadataIoAdmission::fixed_backing_bytes(2, 2).unwrap();
        let route = SharedDependency::Network(
            novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
                &url::Url::parse("https://statistics-rebind.invalid").unwrap(),
            )
            .unwrap(),
        );
        let profile = ConnectorMetadataMaintenanceIoProfile::try_new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(source_bytes).unwrap(),
            NonZeroU64::new(128).unwrap(),
            vec![SharedDependency::LocalHost, route.clone()],
        )
        .unwrap();
        let domains = scope.prepare_root_domains().unwrap();
        let initial = domains
            .initial_payload_source(&scope, JointLane::Custody)
            .unwrap();
        let JointTryBegin::Started(mut proposal) = initial
            .try_begin(
                NonZeroU64::new(source_bytes).unwrap(),
                NonZeroU64::new(128).unwrap(),
            )
            .unwrap()
        else {
            panic!("original queued Root Q");
        };
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        let mut admission = proposal.submit().unwrap();
        let mut payload = None;
        let mut cx = Context::from_waker(Waker::noop());
        drive_original(&mut driver);
        let Poll::Ready(Ok(notice)) = admission.poll_install_in(&mut cx, &scope, &mut payload)
        else {
            panic!("original queued payload proof");
        };
        notice.notify();
        drop(admission);
        drive_original(&mut driver);
        let source = MetadataIoAdmission::new(scope.clone(), profile, source_bytes).unwrap();
        source.adopt_original_payload_in(&mut payload).unwrap();
        assert!(
            payload.is_none(),
            "the exact original was moved, not copied"
        );
        assert_eq!(source.root_capacity().unwrap().scope().id(), scope.id());
        assert!(
            !source.poll_progress(&root.owner, &mut cx).unwrap(),
            "queued LocalHost capacity is not the newly selected Provider route"
        );
        {
            let original = source.original.lock().unwrap();
            assert!(!original.as_ref().unwrap().bound.contains(&route));
        }
        let mut ready = false;
        for _ in 0..256 {
            drive_original(&mut driver);
            if source.poll_progress(&root.owner, &mut cx).unwrap() {
                ready = true;
                break;
            }
        }
        assert!(ready);
        assert_original_healthy(&source);
        {
            let original = source.original.lock().unwrap();
            let body = original.as_ref().unwrap();
            assert!(body.bound.contains(&route));
            assert!(
                body.initial.is_none(),
                "adoption never creates a second initial payload"
            );
            assert_eq!(
                body.payload.as_ref().unwrap().capacity_bytes().unwrap(),
                128
            );
        }
        let mut exited = false;
        for _ in 0..256 {
            match source.poll_quiescent(&root.owner, &mut cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    exited = true;
                    break;
                }
                Poll::Pending => drive_original(&mut driver),
            }
        }
        assert!(exited);
        drop(source);
        root.business.release();
        root.owner.complete_after_terminal_cancel_settled();
        control.close_admission();
        drive_original(&mut driver);
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn reused_bootstrap_grant_cannot_dispatch_a_new_route_before_full_domain_install() {
        let (control, mut driver, root) = original_governor();
        let source_bytes = MetadataIoAdmission::fixed_backing_bytes(2, 2).unwrap();
        let profile = ConnectorMetadataMaintenanceIoProfile::try_new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(source_bytes).unwrap(),
            NonZeroU64::new(128).unwrap(),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        let source = MetadataIoAdmission::new(root.owner.scope(), profile, source_bytes).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = false;
        for _ in 0..256 {
            ready = source.poll_progress(&root.owner, &mut cx).unwrap();
            if ready {
                break;
            }
            drive_original(&mut driver);
        }
        assert!(ready);
        let route = SharedDependency::Network(
            novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
                &url::Url::parse("https://new-vended-fe-fs.invalid").unwrap(),
            )
            .unwrap(),
        );
        let mut request = Box::pin(source.request(route.clone()).unwrap().admitted());
        assert!(
            request.as_mut().poll(&mut cx).is_pending(),
            "a real existing bootstrap grant does not authorize the newly requested service"
        );
        source.poll_progress(&root.owner, &mut cx).unwrap();
        assert!(
            request.as_mut().poll(&mut cx).is_pending(),
            "queued proof is not dispatch permission"
        );
        let mut permit = None;
        for _ in 0..256 {
            drive_original(&mut driver);
            source.poll_progress(&root.owner, &mut cx).unwrap();
            match request.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(actual)) => {
                    permit = Some(actual);
                    break;
                }
                Poll::Ready(Err(e)) => panic!("original new route refused: {e}"),
            }
        }
        assert!(permit.is_some());
        assert_original_healthy(&source);
        {
            let original = source.original.lock().unwrap();
            assert!(original.as_ref().unwrap().bound.contains(&route));
        }
        drop((permit, request));
        let mut exited = false;
        for _ in 0..256 {
            match source.poll_quiescent(&root.owner, &mut cx) {
                Poll::Ready(r) => {
                    r.unwrap();
                    exited = true;
                    break;
                }
                Poll::Pending => drive_original(&mut driver),
            }
        }
        assert!(exited);
        drop(source);
        root.business.release();
        root.owner.complete_after_terminal_cancel_settled();
        control.close_admission();
        drive_original(&mut driver);
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn covered_owned_peak_retains_positive_domains_and_proves_each_new_route() {
        let (control, mut driver, root) = original_governor();
        let source_bytes = MetadataIoAdmission::fixed_backing_bytes(2, 2).unwrap();
        let profile = ConnectorMetadataMaintenanceIoProfile::try_new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(source_bytes).unwrap(),
            NonZeroU64::new(128).unwrap(),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        let source = MetadataIoAdmission::new(root.owner.scope(), profile, source_bytes).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = false;
        for _ in 0..256 {
            ready = source.poll_progress(&root.owner, &mut cx).unwrap();
            if ready {
                break;
            }
            drive_original(&mut driver);
        }
        assert!(ready);
        let mut request = Box::pin(
            source
                .request_owned(NonZeroU64::new(64).unwrap())
                .unwrap()
                .admitted(),
        );
        assert!(request.as_mut().poll(&mut cx).is_pending());
        source.poll_progress(&root.owner, &mut cx).unwrap();
        let permit = match request.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("the already positive original payload covers this peak"),
        };
        {
            let known_route = SharedDependency::LocalHost;
            let mut known = Box::pin(permit.include_dependency(&known_route));
            assert!(
                matches!(known.as_mut().poll(&mut cx), Poll::Ready(Ok(()))),
                "the peak receipt retains its actual positive full domain set"
            );
        }
        let route = SharedDependency::Network(
            novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
                &url::Url::parse("https://owned-peak-selected-service.invalid").unwrap(),
            )
            .unwrap(),
        );
        {
            let mut extension = Box::pin(permit.include_dependency(&route));
            assert!(extension.as_mut().poll(&mut cx).is_pending());
            source.poll_progress(&root.owner, &mut cx).unwrap();
            assert!(
                extension.as_mut().poll(&mut cx).is_pending(),
                "queued proof is not permission"
            );
            let mut positive = false;
            for _ in 0..256 {
                drive_original(&mut driver);
                source.poll_progress(&root.owner, &mut cx).unwrap();
                match extension.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(())) => {
                        positive = true;
                        break;
                    }
                    Poll::Ready(Err(error)) => panic!("original peak route refused: {error}"),
                }
            }
            assert!(positive);
        }
        assert_original_healthy(&source);
        drop((permit, request));
        let mut exited = false;
        for _ in 0..256 {
            match source.poll_quiescent(&root.owner, &mut cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    exited = true;
                    break;
                }
                Poll::Pending => drive_original(&mut driver),
            }
        }
        assert!(exited);
        drop(source);
        root.business.release();
        root.owner.complete_after_terminal_cancel_settled();
        control.close_admission();
        drive_original(&mut driver);
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn original_cancellation_retains_its_fact_after_actual_io_and_alias_exit() {
        use std::sync::atomic::AtomicBool;

        struct ActualIo {
            released: Arc<AtomicBool>,
            exits: Arc<AtomicUsize>,
        }
        impl Future for ActualIo {
            type Output = Result<(), ConnectorError>;
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                if self.released.load(Ordering::SeqCst) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
        }
        impl Drop for ActualIo {
            fn drop(&mut self) {
                self.exits.fetch_add(1, Ordering::SeqCst);
            }
        }

        let (control, mut driver, root) = original_governor();
        let source_bytes = MetadataIoAdmission::fixed_backing_bytes(2, 1).unwrap();
        let profile = ConnectorMetadataMaintenanceIoProfile::try_new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(source_bytes).unwrap(),
            NonZeroU64::new(128).unwrap(),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        let source = MetadataIoAdmission::new(root.owner.scope(), profile, source_bytes).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = false;
        for _ in 0..256 {
            ready = source.poll_progress(&root.owner, &mut cx).unwrap();
            if ready {
                break;
            }
            drive_original(&mut driver);
        }
        assert!(ready);
        let mut request = Box::pin(
            source
                .request(SharedDependency::LocalHost)
                .unwrap()
                .admitted(),
        );
        let mut permit = None;
        for _ in 0..256 {
            drive_original(&mut driver);
            source.poll_progress(&root.owner, &mut cx).unwrap();
            if let Poll::Ready(result) = request.as_mut().poll(&mut cx) {
                permit = Some(result.unwrap());
                break;
            }
        }
        let permit = permit.expect("one actual positive AsyncIo grant");
        drop(request);
        let released = Arc::new(AtomicBool::new(false));
        let exits = Arc::new(AtomicUsize::new(0));
        let mut original: Option<ConnectorIoCleanupFuture> = Some(Box::pin(ActualIo {
            released: Arc::clone(&released),
            exits: Arc::clone(&exits),
        }));
        permit
            .begin_stream()
            .unwrap()
            .install_cleanup_in(&mut original)
            .unwrap_or_else(|_| panic!("same original stream accepts its actual F"));
        assert!(original.is_none());

        let mut refused = Box::pin(
            source
                .request(SharedDependency::LocalHost)
                .unwrap()
                .admitted(),
        );
        root.owner
            .cancel(novarocks_workload_control::CancellationReason::Requested);
        let mut refusal = None;
        for _ in 0..256 {
            source.poll_progress(&root.owner, &mut cx).unwrap();
            drive_original(&mut driver);
            match refused.as_mut().poll(&mut cx) {
                Poll::Ready(Err(error)) => {
                    refusal = Some(error);
                    break;
                }
                Poll::Ready(Ok(_)) => panic!("cancelled original request cannot start"),
                Poll::Pending => {}
            }
        }
        assert!(matches!(
            refusal.unwrap().kind(),
            ConnectorErrorKind::Cancelled
        ));
        drop(refused);
        assert!(matches!(source.failure(), Some(WorkError::Cancelled(_))));
        assert!(source.health_failure().is_none());
        assert!(
            source
                .poll_actual_quiescent_continuing(&mut cx)
                .is_pending()
        );
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        released.store(true, Ordering::SeqCst);
        source.poll_continuing(&mut cx).unwrap();
        assert_eq!(
            exits.load(Ordering::SeqCst),
            1,
            "actual original F Drop occurred"
        );
        assert!(
            source
                .poll_actual_quiescent_continuing(&mut cx)
                .is_pending(),
            "the actual permit alias still holds its source"
        );
        drop(permit);
        let observed = match source.poll_actual_quiescent_continuing(&mut cx) {
            Poll::Ready(Ok(observed)) => observed,
            _ => panic!("all actual requests and aliases exited with their ordinary refusal"),
        };
        assert!(matches!(
            observed.into_ordinary_failure(),
            Some(WorkError::Cancelled(_))
        ));
        assert!(
            source.failure().is_some(),
            "the original failure was observed, not erased"
        );
        assert!(source.health_failure().is_none());
        assert!(
            matches!(
                source.poll_quiescent(&root.owner, &mut cx),
                Poll::Ready(Err(_))
            ),
            "legacy callers keep their original business-error contract"
        );

        // An absent original loan is still a custody refusal, not success.
        let borrowed = loan(&source.original).unwrap();
        assert!(matches!(
            source.poll_actual_quiescent_continuing(&mut cx),
            Poll::Ready(Err(_))
        ));
        drop(borrowed);
        drop(source);
        root.business.release();
        root.owner.complete_after_terminal_cancel_settled();
        control.close_admission();
        drive_original(&mut driver);
        assert!(
            control.shutdown().is_ok(),
            "same WC really drains; ordinary refusal is not fake running"
        );
    }

    #[test]
    fn actual_quiescence_does_not_dispose_or_reclassify_raw_cleanup_panic() {
        let (source, primary_drops, _) = super::cleanup_tests::two_failures(true);
        let mut cx = Context::from_waker(Waker::noop());
        let failure = match source.poll_actual_quiescent_continuing(&mut cx) {
            Poll::Ready(Err(failure)) => failure,
            _ => panic!("the same raw cleanup panic must still refuse actual retirement"),
        };
        assert!(std::ptr::eq(
            failure
                .cause()
                .downcast_ref::<MetadataIoAdmission>()
                .unwrap(),
            source.as_ref(),
        ));
        assert_eq!(primary_drops.load(Ordering::SeqCst), 0);
        drop(source);
        assert_eq!(
            primary_drops.load(Ordering::SeqCst),
            0,
            "the exact health source still owns its primary cause"
        );
        drop(failure);
        assert_eq!(primary_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn eight_original_requests_exceed_a_one_request_profile_and_all_exit() {
        let (control, mut driver, root) = original_governor();
        let source_bytes = MetadataIoAdmission::fixed_backing_bytes(8, 1).unwrap();
        let profile = ConnectorMetadataMaintenanceIoProfile::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroU64::new(source_bytes).unwrap(),
            NonZeroU64::new(128).unwrap(),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        let source = MetadataIoAdmission::new(root.owner.scope(), profile, source_bytes).unwrap();
        let wake = Arc::new(OriginalWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut cx = Context::from_waker(&waker);
        let mut first_ready = false;
        // No prebound control or injected payload/grant: progress drives the
        // same Source's actual bootstrap, initial payload and first action.
        for _ in 0..256 {
            first_ready = source.poll_progress(&root.owner, &mut cx).unwrap();
            assert_original_healthy(&source);
            if first_ready {
                break;
            }
            drive_original(&mut driver);
        }
        assert!(
            first_ready,
            "real bootstrap and initial admission must complete"
        );
        drive_original(&mut driver);

        let mut waiting: Vec<_> = (0..8)
            .map(|_| {
                Some(Box::pin(
                    source
                        .request(SharedDependency::LocalHost)
                        .unwrap()
                        .admitted(),
                ))
            })
            .collect();
        let mut permits: Vec<Option<ConnectorIoPermit>> = (0..8).map(|_| None).collect();
        source.poll_progress(&root.owner, &mut cx).unwrap();
        assert_original_healthy(&source);
        {
            let original = source.original.lock().unwrap();
            let body = original.as_ref().unwrap();
            assert_eq!(body.cells.len(), 8);
        }

        // No driver step or external event occurs here. After registration
        // settles, observing old Waiting replies/Full slots must not wake the
        // same Source forever. In particular Waiting reply polls are pure.
        source.poll_progress(&root.owner, &mut cx).unwrap();
        source.poll_progress(&root.owner, &mut cx).unwrap();
        let quiet = wake.0.load(Ordering::SeqCst);
        source.poll_progress(&root.owner, &mut cx).unwrap();
        assert_eq!(
            wake.0.load(Ordering::SeqCst),
            quiet,
            "unchanged waiting cannot create an endless self-wake"
        );
        assert_original_healthy(&source);

        for _ in 0..256 {
            // Only this one original driver proves/grants/reaps. The Source
            // keeps receiving every cell while original requests remain Pending.
            drive_original(&mut driver);
            source.poll_progress(&root.owner, &mut cx).unwrap();
            assert_original_healthy(&source);
            for index in 0..waiting.len() {
                let Some(future) = waiting[index].as_mut() else {
                    continue;
                };
                match future.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(permit)) => {
                        permits[index] = Some(permit);
                        // The original receipt is stable before its completed
                        // borrowing request future actually drops.
                        drop(waiting[index].take());
                    }
                    Poll::Ready(Err(error)) => panic!("real request refused: {error}"),
                }
            }
            if permits.iter().all(Option::is_some) {
                break;
            }
        }
        assert!(
            permits.iter().all(Option::is_some),
            "all eight real Async grants must be simultaneously held without a profile-concurrency quota"
        );
        {
            let original = source.original.lock().unwrap();
            let body = original.as_ref().unwrap();
            assert_eq!(
                body.cells
                    .iter()
                    .filter(|cell| cell
                        .original
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .grant
                        .is_some())
                    .count(),
                8
            );
        }
        assert!(matches!(
            source.poll_quiescent(&root.owner, &mut cx),
            Poll::Pending
        ));
        let retained = permits[0].as_ref().unwrap().retain();
        drop(permits);
        drop(waiting);
        assert!(
            matches!(source.poll_quiescent(&root.owner, &mut cx), Poll::Pending),
            "the original retained request alias still prevents true exit"
        );
        drop(retained);
        let mut exited = false;
        for _ in 0..256 {
            match source.poll_quiescent(&root.owner, &mut cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    exited = true;
                    break;
                }
                Poll::Pending => drive_original(&mut driver),
            }
        }
        assert!(exited, "actual original request aliases must all exit");
        drop(source);
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
        control.close_admission();
        drive_original(&mut driver);
        assert!(
            control.shutdown().is_ok(),
            "same unique WC owner must really drain; no ledger reset or fake exit"
        );
    }
}
