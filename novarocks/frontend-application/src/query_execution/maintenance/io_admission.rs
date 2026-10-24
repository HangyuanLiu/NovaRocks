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

//! Same metadata Source, finite original request cells, and same Root paidQ.
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
    AsyncActionAdmission, AsyncActionGrant, JointAdjustmentReceipt, JointAdmission, JointLane,
    RetainedPayloadGrant, RootPayloadAdmission, RootPayloadInitialAdmission, RootPayloadReady,
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

pub(super) struct MetadataIoAdmission {
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
    k: usize,
    bootstrap: Option<JointAdmission>,
    bootstrap_receipt: Option<JointAdjustmentReceipt>,
    bound: Vec<SharedDependency>,
    full: Option<Arc<[SharedDependency]>>,
    initial: Option<RootPayloadInitialAdmission>,
    payload: Option<RetainedPayloadGrant>,
    growth: Option<RootPayloadAdmission>,
    growth_ready: Option<RootPayloadReady>,
    growth_cell: Option<usize>,
    growth_route: Option<SharedDependency>,
    growth_bytes: u64,
    cells: Box<[Arc<RequestCell>]>,
    cursor: usize,
    started: bool,
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
            Some(body) if body.grant.is_some() || body.owned_admitted => Poll::Ready(Ok(())),
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
impl MetadataIoAdmission {
    pub(super) const fn request_metadata_bytes() -> u64 {
        std::mem::size_of::<CellBody>() as u64
    }
    pub(super) fn new(
        root: WorkScope,
        profile: ConnectorMetadataMaintenanceIoProfile,
        source_metadata_bytes: u64,
    ) -> Result<Arc<Self>, WorkError> {
        let k = root.joint_dependency_limit()?;
        let wake = Arc::new(MetadataWake::new());
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(profile.max_concurrent_io().get())
            .map_err(|_| WorkError::Capacity("metadata fixed request cells"))?;
        for _ in 0..profile.max_concurrent_io().get() {
            cells.push(Arc::new(RequestCell {
                original: Mutex::new(Some(CellBody::default())),
                stream: Mutex::new(Some(StreamBody::default())),
                progress: Mutex::new(None),
                cleanup_error: Mutex::new(None),
                observer: AtomicWaker::new(),
                root: Arc::clone(&wake),
            }));
        }
        let mut bound = Vec::new();
        bound
            .try_reserve_exact(k)
            .map_err(|_| WorkError::Capacity("metadata original complete union"))?;
        Ok(Arc::new(Self {
            wake,
            cleanup: Mutex::new(CleanupReceipt::default()),
            original: Mutex::new(Some(Body {
                root,
                profile,
                source_metadata_bytes,
                k,
                bootstrap: None,
                bootstrap_receipt: None,
                bound,
                full: None,
                initial: None,
                payload: None,
                growth: None,
                growth_ready: None,
                growth_cell: None,
                growth_route: None,
                growth_bytes: 0,
                cells: cells.into_boxed_slice(),
                cursor: 0,
                started: false,
                first_ready: false,
                settlement: false,
                failure: None,
                changed: false,
                reobserve: false,
            })),
        }))
    }
    /// Actual Source containers plus all finite loan/fence/fact replacement scratch.
    /// This is owned first-party backing, not a recursive SDK/allocator/RSS quote.
    pub(super) fn fixed_backing_bytes(n: usize, k: usize) -> Result<u64, WorkError> {
        let arc = std::mem::size_of::<[usize; 2]>();
        let mutex_external = if cfg!(target_os = "macos") {
            128usize
        } else {
            0
        };
        let per = std::mem::size_of::<RequestCell>()
            .checked_add(arc)
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
    pub(super) fn cleanup_failure(&self) -> Option<ConnectorError> {
        self.cleanup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .first
            .clone()
    }
    pub(super) fn failure(&self) -> Option<WorkError> {
        self.original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|b| b.failure.clone())
    }
    pub(super) fn health_failure(
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
    pub(super) fn poll_progress(
        &self,
        owner: &WorkOwner,
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
        owner: &WorkOwner,
        cx: &mut Context<'_>,
    ) -> Result<bool, WorkError> {
        if !body.started {
            if let Some(error) = &body.failure {
                return Err(error.clone());
            }
            if body.bootstrap.is_none() && body.bootstrap_receipt.is_none() {
                let mut p = body.root.begin_control_binding()?;
                for fact in body.profile.initial_dependencies() {
                    p.push_dependency(fact)?;
                }
                p.end_change()?;
                body.bootstrap = Some(p.submit().map_err(|e| e.error)?);
            }
            if body.bootstrap_receipt.is_none() {
                let batch = match Pin::new(body.bootstrap.as_mut().unwrap()).poll(cx) {
                    Poll::Pending => return Ok(false),
                    Poll::Ready(r) => r.map_err(|e| e.error)?,
                };
                if !batch.new_claims.is_empty() {
                    return Err(WorkError::Conflict);
                }
                body.bootstrap_receipt = batch.adjustment;
                let notice = body
                    .bootstrap_receipt
                    .as_mut()
                    .ok_or(WorkError::Conflict)?
                    .install_control_scope_in(&body.root)?;
                body.changed = true;
                notice.notify();
            }
            if body.initial.is_none() {
                let bytes = body.source_metadata_bytes;
                body.initial = Some(
                    owner
                        .begin_root_payload(
                            NonZeroU64::new(bytes).ok_or(WorkError::ArithmeticOverflow)?,
                            body.profile.initial_owned_bytes(),
                        )
                        .map_err(|e| e.error)?,
                );
            }
            let notice = match body.initial.as_mut().unwrap().poll_install_in(
                cx,
                &body.root,
                &mut body.payload,
            ) {
                Poll::Pending => return Ok(false),
                Poll::Ready(r) => r.map_err(|e| e.error)?,
            };
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
        if body.growth.is_none() && body.growth_ready.is_none() {
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
            let active: Vec<_> = cells
                .iter()
                .filter_map(|c| c.body.as_ref().unwrap().grant.as_ref())
                .collect();
            let result = ready.install_in(&body.root, body.payload.as_ref().unwrap(), &active);
            drop(active);
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
                cells[i].body.as_mut().unwrap().owned_admitted = true;
            }
            for cell in &mut cells {
                let cell = cell.body.as_mut().unwrap();
                if cell.grant.is_some() {
                    cell.full = body.full.clone();
                    cell.extension = None;
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
            if required != 0 && bytes == 0 {
                cells[i].body.as_mut().unwrap().owned_admitted = true;
                body.changed = true;
            }
            if route.is_some() || bytes != 0 {
                // Undelivered all-New actions have not started physical I/O.
                // Withdraw them before rekeying the same control source; after
                // install they seek a fresh finite intent from its new fence.
                for source in &mut cells {
                    let source = source.body.as_mut().unwrap();
                    if source.grant.is_none() {
                        drop(source.admission.take());
                    }
                }
                let active: Vec<_> = cells
                    .iter()
                    .filter_map(|c| c.body.as_ref().unwrap().grant.as_ref())
                    .collect();
                let lane = if body.settlement {
                    JointLane::Settlement
                } else {
                    JointLane::Foreground
                };
                let construction = (|| {
                    let mut p = body
                        .payload
                        .as_ref()
                        .unwrap()
                        .root_payload_source(&body.root, lane, &active)?
                        .begin(bytes)?;
                    if let Some(route) = route.as_ref() {
                        p.push_dependency(route)?;
                    }
                    p.end_change()?;
                    p.submit().map_err(|e| e.error)
                })();
                match construction {
                    Ok(p) => {
                        body.growth = Some(p);
                        body.growth_cell = Some(i);
                        body.growth_route = route;
                        body.growth_bytes = bytes;
                        body.reobserve = true;
                    }
                    Err(error) => {
                        cells[i].body.as_mut().unwrap().failure = Some(error.clone());
                        body.failure.get_or_insert(error);
                        body.changed = true;
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
                match owner.async_action_source(lane).and_then(|s| {
                    s.begin(
                        NonZeroU64::new(Self::request_metadata_bytes()).unwrap(),
                        NonZeroU64::new(1).unwrap(),
                    )
                    .map_err(|e| e.error)
                }) {
                    Ok(a) => cell.admission = Some(a),
                    Err(error) => {
                        cell.failure = Some(error.clone());
                        body.failure.get_or_insert(error);
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
        body.first_ready |= cells[0].body.as_ref().unwrap().grant.is_some();
        if !body.first_ready {
            if let Some(error) = &body.failure {
                return Err(error.clone());
            }
        }
        Ok(body.first_ready)
    }
    pub(super) fn poll_quiescent(
        &self,
        owner: &WorkOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        if let Err(error) = self.poll_progress(owner, cx) {
            return Poll::Ready(Err(error));
        }
        let Some(mut original) = loan(&self.original) else {
            return Poll::Ready(Err(WorkError::Conflict));
        };
        let body = original.body.as_mut().unwrap();
        if body.growth.is_some() || body.growth_ready.is_some() {
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
        Poll::Ready(body.failure.clone().map_or(Ok(()), Err))
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
        for cell in &mut body.cells {
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
                && (body.growth.is_some() || body.growth_ready.is_some())
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
        Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "metadata exhausted its original fixed request cells",
        ))
    }
}
impl ConnectorIoAdmissionPort for MetadataIoAdmission {
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
