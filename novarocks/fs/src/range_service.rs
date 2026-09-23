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

//! One BE-owned range dispatcher. Queue entries represent requests, while
//! window slots represent physical segments and survive until task exit.

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::runtime::Handle;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use crate::{
    BoundFile, FileCancellation, FileError, FileErrorKind, FileReadRange, FileResult,
    FileTaskSpawner, PreparedFileInput,
};

const SEGMENT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileRangeScope {
    query: (i64, i64, u64),
    source: (i64, i64, i32),
}

impl FileRangeScope {
    pub fn try_new(
        query_high: i64,
        query_low: i64,
        attempt: u64,
        fragment_high: i64,
        fragment_low: i64,
        node_id: i32,
    ) -> FileResult<Self> {
        if query_high == 0 && query_low == 0 {
            return Err(FileError::invalid("range query identity must be nonzero"));
        }
        if attempt == 0 {
            return Err(FileError::invalid("range query attempt must be nonzero"));
        }
        if fragment_high == 0 && fragment_low == 0 {
            return Err(FileError::invalid(
                "range fragment identity must be nonzero",
            ));
        }
        if node_id < 0 {
            return Err(FileError::invalid(
                "range source node id must be nonnegative",
            ));
        }
        Ok(Self {
            query: (query_high, query_low, attempt),
            source: (fragment_high, fragment_low, node_id),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileRangeClass {
    Demand,
    Prefetch,
}

pub enum FileRangeStart {
    Started(FileRangeRequest),
    Deferred,
}

pub struct FileRangeRequest {
    service: Arc<FileRangeService>,
    id: u64,
    cancellation: FileCancellation,
    file: BoundFile,
    offset: u64,
    partial_copy_bytes: usize,
    missing_bytes: usize,
    result: Option<oneshot::Receiver<FileResult<RangeOutput>>>,
    exit: Option<oneshot::Receiver<FileResult<()>>>,
}

/// A non-owning dispatch gate for one prefetch request. Dropping this handle
/// does not stop the request; only its owning `FileRangeRequest` can do that.
#[derive(Clone)]
pub struct FileRangeControl {
    service: std::sync::Weak<FileRangeService>,
    id: u64,
}

impl FileRangeControl {
    pub fn request_pause(&self) {
        if let Some(service) = self.service.upgrade() {
            service.set_prefetch_paused(self.id, true);
        }
    }

    pub fn request_resume(&self) {
        if let Some(service) = self.service.upgrade() {
            service.set_prefetch_paused(self.id, false);
        }
    }
}

impl Drop for FileRangeRequest {
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl FileRangeRequest {
    pub fn control(&self) -> FileRangeControl {
        FileRangeControl {
            service: Arc::downgrade(&self.service),
            id: self.id,
        }
    }

    pub async fn result_ready(&mut self) -> FileResult<Bytes> {
        self.take_result().await.map(|output| output.bytes)
    }

    pub async fn prepared_input_ready(&mut self) -> FileResult<PreparedFileInput> {
        let output = self.take_result().await?;
        PreparedFileInput::from_completed(
            &self.file,
            self.offset,
            output.bytes,
            output.retained_backing_capacity,
        )
    }

    pub fn partial_copy_bytes(&self) -> usize {
        self.partial_copy_bytes
    }

    pub fn missing_bytes(&self) -> usize {
        self.missing_bytes
    }

    async fn take_result(&mut self) -> FileResult<RangeOutput> {
        self.result
            .take()
            .ok_or_else(|| FileError::invalid("range result already consumed"))?
            .await
            .map_err(|_| FileError::new(FileErrorKind::Internal, "range request lost its result"))?
    }

    pub async fn drained(mut self) -> FileResult<()> {
        self.exit
            .take()
            .ok_or_else(|| FileError::invalid("range exit already consumed"))?
            .await
            .map_err(|_| {
                FileError::new(
                    FileErrorKind::Internal,
                    "range request lost its exit receipt",
                )
            })?
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
        self.service.stop_request(self.id);
    }
}

struct RangeOutput {
    bytes: Bytes,
    retained_backing_capacity: usize,
}

fn prepare_segments(
    offset: u64,
    length: usize,
    present: Option<&PreparedFileInput>,
) -> FileResult<(VecDeque<Segment>, Vec<Option<BytesMut>>, usize)> {
    let end = offset + length as u64;
    let overlap = present.and_then(|input| {
        let range = input.range();
        let start = offset.max(range.start);
        let stop = end.min(range.end);
        (start < stop).then_some(start..stop)
    });
    let mut backing = BytesMut::zeroed(length);
    if backing.capacity() != length {
        return Err(FileError::new(
            FileErrorKind::ResourceExhausted,
            "range target capacity exceeds its reserved length",
        ));
    }
    let mut copy_bytes = 0;
    if let (Some(input), Some(overlap)) = (present, overlap.as_ref()) {
        let target_start = (overlap.start - offset) as usize;
        let source_start = (overlap.start - input.range().start) as usize;
        copy_bytes = (overlap.end - overlap.start) as usize;
        backing[target_start..target_start + copy_bytes]
            .copy_from_slice(&input.bytes()[source_start..source_start + copy_bytes]);
    }
    let mut pending = VecDeque::new();
    let mut completed = Vec::new();
    let mut cursor = offset;
    let mut parts: Vec<(Range<u64>, bool)> = Vec::new();
    if let Some(overlap) = overlap {
        if cursor < overlap.start {
            parts.push((cursor..overlap.start, false));
        }
        parts.push((overlap.clone(), true));
        cursor = overlap.end;
    }
    if cursor < end {
        parts.push((cursor..end, false));
    }
    for (part, ready) in parts {
        let mut part_cursor = part.start;
        while part_cursor < part.end {
            let take = ((part.end - part_cursor) as usize).min(SEGMENT_BYTES);
            let bytes = backing.split_to(take);
            let index = completed.len();
            if ready {
                completed.push(Some(bytes));
            } else {
                completed.push(None);
                pending.push_back(Segment {
                    offset: part_cursor,
                    bytes,
                    index,
                });
            }
            part_cursor += take as u64;
        }
    }
    Ok((pending, completed, copy_bytes))
}

struct Segment {
    offset: u64,
    bytes: BytesMut,
    index: usize,
}

struct RequestState {
    scope: FileRangeScope,
    class: FileRangeClass,
    paused: bool,
    file: BoundFile,
    cancellation: FileCancellation,
    pending: VecDeque<Segment>,
    completed: Vec<Option<BytesMut>>,
    expected_length: usize,
    active: usize,
    failed: bool,
    exit_error: Option<FileError>,
    result: Option<oneshot::Sender<FileResult<RangeOutput>>>,
    exit: Option<oneshot::Sender<FileResult<()>>>,
}

struct State {
    closed: bool,
    next_id: u64,
    active: usize,
    source_active: HashMap<FileRangeScope, usize>,
    requests: HashMap<u64, RequestState>,
    demand: VecDeque<u64>,
    prefetch: VecDeque<u64>,
    last_query: Option<(i64, i64, u64)>,
    last_source: HashMap<(i64, i64, u64), (i64, i64, i32)>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            closed: false,
            next_id: 0,
            active: 0,
            source_active: HashMap::new(),
            requests: HashMap::new(),
            demand: VecDeque::new(),
            prefetch: VecDeque::new(),
            last_query: None,
            last_source: HashMap::new(),
        }
    }
}

struct Dispatch {
    id: u64,
    scope: FileRangeScope,
    file: BoundFile,
    cancellation: FileCancellation,
    segment: Segment,
}

pub struct FileRangeService {
    process_window: usize,
    source_window: usize,
    queue_capacity: usize,
    demand_waiters: AtomicUsize,
    task_spawner: Arc<dyn FileTaskSpawner>,
    scan_handle: Handle,
    state: Mutex<State>,
    supervisors: Mutex<Vec<JoinHandle<()>>>,
    changed: Notify,
}

struct DemandWaitGuard(Arc<FileRangeService>);

impl DemandWaitGuard {
    fn new(service: Arc<FileRangeService>) -> Self {
        service.demand_waiters.fetch_add(1, Ordering::SeqCst);
        Self(service)
    }
}

impl Drop for DemandWaitGuard {
    fn drop(&mut self) {
        self.0.demand_waiters.fetch_sub(1, Ordering::SeqCst);
        self.0.dispatch();
    }
}

impl FileRangeService {
    pub fn new(
        process_window: NonZeroUsize,
        source_window: NonZeroUsize,
        queue_capacity: NonZeroUsize,
        task_spawner: Arc<dyn FileTaskSpawner>,
        scan_handle: Handle,
    ) -> Arc<Self> {
        Arc::new(Self {
            process_window: process_window.get(),
            source_window: source_window.get(),
            queue_capacity: queue_capacity.get(),
            demand_waiters: AtomicUsize::new(0),
            task_spawner,
            scan_handle,
            state: Mutex::new(State::default()),
            supervisors: Mutex::new(Vec::new()),
            changed: Notify::new(),
        })
    }

    pub fn start(
        self: &Arc<Self>,
        scope: FileRangeScope,
        class: FileRangeClass,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
    ) -> FileResult<FileRangeRequest> {
        match self.try_start(scope, class, file, range, cancellation)? {
            FileRangeStart::Started(request) => Ok(request),
            FileRangeStart::Deferred => Err(FileError::new(
                FileErrorKind::ResourceExhausted,
                "range request queue is full",
            )),
        }
    }

    pub async fn start_wait(
        self: &Arc<Self>,
        scope: FileRangeScope,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
    ) -> FileResult<FileRangeRequest> {
        self.start_wait_with_present(scope, file, range, cancellation, None)
            .await
    }

    pub async fn start_wait_with_present(
        self: &Arc<Self>,
        scope: FileRangeScope,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
        present: Option<PreparedFileInput>,
    ) -> FileResult<FileRangeRequest> {
        let _waiter = DemandWaitGuard::new(Arc::clone(self));
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            cancellation.check()?;
            match self.try_start_with_present(
                scope,
                FileRangeClass::Demand,
                file.clone(),
                range,
                cancellation.clone(),
                present.clone(),
            )? {
                FileRangeStart::Started(request) => return Ok(request),
                FileRangeStart::Deferred => {
                    tokio::select! { _ = &mut changed => {}, error = cancellation.ended() => return Err(error) }
                }
            }
        }
    }

    pub fn try_start(
        self: &Arc<Self>,
        scope: FileRangeScope,
        class: FileRangeClass,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
    ) -> FileResult<FileRangeStart> {
        self.try_start_with_present(scope, class, file, range, cancellation, None)
    }

    pub fn try_start_with_present(
        self: &Arc<Self>,
        scope: FileRangeScope,
        class: FileRangeClass,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
        present: Option<PreparedFileInput>,
    ) -> FileResult<FileRangeStart> {
        cancellation.check()?;
        if let Some(input) = &present {
            input.validate_for(&file)?;
        }
        let (offset, length) = match range {
            FileReadRange::WholeFile => (0, file.identity().file_size()),
            FileReadRange::Bounded { offset, length } => (offset, length),
        };
        if length == 0 {
            return Err(FileError::invalid("range request must be nonempty"));
        }
        if offset
            .checked_add(length)
            .is_none_or(|end| end > file.identity().file_size())
        {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                "range exceeds bound file length",
            ));
        }
        let length = usize::try_from(length).map_err(|_| {
            FileError::new(
                FileErrorKind::ResourceExhausted,
                "range target length exceeds address space",
            )
        })?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| FileError::new(FileErrorKind::Internal, "range service state poisoned"))?;
        if state.closed {
            return Err(FileError::cancelled("range service admission is closed"));
        }
        if class == FileRangeClass::Prefetch && self.demand_waiters.load(Ordering::SeqCst) != 0 {
            return Ok(FileRangeStart::Deferred);
        }
        if state.demand.len() + state.prefetch.len() >= self.queue_capacity {
            if class == FileRangeClass::Prefetch {
                return Ok(FileRangeStart::Deferred);
            }
            // Queued speculation must not occupy the last demand slot.
            while state.demand.len() + state.prefetch.len() >= self.queue_capacity {
                let Some(evicted) = state.prefetch.pop_back() else {
                    return Ok(FileRangeStart::Deferred);
                };
                self.fail_request_locked(
                    &mut state,
                    evicted,
                    FileError::cancelled("queued prefetch yielded to demand"),
                );
            }
        }
        let cancellation = cancellation.child();
        let (pending, completed, partial_copy_bytes) =
            prepare_segments(offset, length, present.as_ref())?;
        let (result_sender, result) = oneshot::channel();
        let (exit_sender, exit) = oneshot::channel();
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| FileError::new(FileErrorKind::Internal, "range request id exhausted"))?;
        state.requests.insert(
            id,
            RequestState {
                scope,
                class,
                paused: false,
                file: file.clone(),
                cancellation: cancellation.clone(),
                pending,
                completed,
                expected_length: length,
                active: 0,
                failed: false,
                exit_error: None,
                result: Some(result_sender),
                exit: Some(exit_sender),
            },
        );
        if state.requests[&id].pending.is_empty() {
            self.finish_locked(&mut state, id);
        } else {
            match class {
                FileRangeClass::Demand => state.demand.push_back(id),
                FileRangeClass::Prefetch => state.prefetch.push_back(id),
            };
        }
        drop(state);
        self.dispatch();
        Ok(FileRangeStart::Started(FileRangeRequest {
            service: Arc::clone(self),
            id,
            cancellation,
            file,
            offset,
            partial_copy_bytes,
            missing_bytes: length - partial_copy_bytes,
            result: Some(result),
            exit: Some(exit),
        }))
    }

    pub fn close_admission(self: &Arc<Self>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.closed = true;
        let ids: Vec<_> = state.requests.keys().copied().collect();
        for id in ids {
            self.fail_request_locked(
                &mut state,
                id,
                FileError::cancelled("range service is closing"),
            );
        }
        state.demand.clear();
        state.prefetch.clear();
        drop(state);
        self.changed.notify_waiters();
    }

    fn stop_request(self: &Arc<Self>, id: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        self.fail_request_locked(
            &mut state,
            id,
            FileError::cancelled("range request stopped"),
        );
        drop(state);
        self.dispatch();
    }

    fn set_prefetch_paused(self: &Arc<Self>, id: u64, paused: bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(request) = state.requests.get_mut(&id) else {
            return;
        };
        if request.class != FileRangeClass::Prefetch || request.failed || request.paused == paused {
            return;
        }
        // Segment admission is decided under this same lock. A segment that
        // already holds a window slot keeps running; queued segments wait.
        request.paused = paused;
        drop(state);
        if !paused {
            self.dispatch();
        }
    }

    pub async fn drain(&self) -> FileResult<()> {
        self.wait_requests_empty(|| {}).await?;
        let supervisors =
            std::mem::take(&mut *self.supervisors.lock().map_err(|_| {
                FileError::new(FileErrorKind::Internal, "range supervisors poisoned")
            })?);
        for supervisor in supervisors {
            supervisor.await.map_err(|error| {
                FileError::with_source(FileErrorKind::Internal, "range supervisor failed", error)
            })?;
        }
        Ok(())
    }

    async fn wait_requests_empty(&self, mut before_wait: impl FnMut()) -> FileResult<()> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .state
                .lock()
                .map_err(|_| {
                    FileError::new(FileErrorKind::Internal, "range service state poisoned")
                })?
                .requests
                .is_empty()
            {
                break;
            }
            before_wait();
            notified.await;
        }
        Ok(())
    }

    fn fail_request_locked(&self, state: &mut State, id: u64, error: FileError) {
        state.demand.retain(|queued| *queued != id);
        state.prefetch.retain(|queued| *queued != id);
        self.changed.notify_waiters();
        let Some(request) = state.requests.get_mut(&id) else {
            return;
        };
        if !request.failed {
            request.cancellation.cancel();
            request.pending.clear();
            request.failed = true;
            if let Some(sender) = request.result.take() {
                let _ = sender.send(Err(error));
            }
        }
        if request.active == 0 {
            self.finish_locked(state, id);
        }
    }

    fn finish_locked(&self, state: &mut State, id: u64) {
        let Some(mut request) = state.requests.remove(&id) else {
            return;
        };
        if let Some(sender) = request.result.take() {
            let mut segments = request.completed.into_iter();
            let mut backing = segments
                .next()
                .expect("nonempty range")
                .expect("completed segment");
            for segment in segments {
                backing.unsplit(segment.expect("completed segment"));
            }
            let result = if backing.len() == request.expected_length
                && backing.capacity() == request.expected_length
            {
                Ok(RangeOutput {
                    bytes: backing.freeze(),
                    retained_backing_capacity: request.expected_length,
                })
            } else {
                Err(FileError::new(
                    FileErrorKind::Internal,
                    "range target changed its reserved length or capacity",
                ))
            };
            let _ = sender.send(result);
        }
        if let Some(sender) = request.exit.take() {
            let _ = sender.send(request.exit_error.map_or(Ok(()), Err));
        }
        self.changed.notify_waiters();
    }

    fn dispatch(self: &Arc<Self>) {
        loop {
            let work = {
                let Ok(mut state) = self.state.lock() else {
                    return;
                };
                if state.closed || state.active >= self.process_window {
                    return;
                }
                let id = self.choose_locked(&mut state);
                let Some(id) = id else {
                    return;
                };
                let request = state.requests.get_mut(&id).expect("queued range request");
                let segment = request.pending.pop_front().expect("queued segment");
                let work = Dispatch {
                    id,
                    scope: request.scope,
                    file: request.file.clone(),
                    cancellation: request.cancellation.child(),
                    segment,
                };
                let has_more = !request.pending.is_empty();
                let class = request.class;
                request.active += 1;
                state.active += 1;
                *state.source_active.entry(work.scope).or_default() += 1;
                if has_more {
                    match class {
                        FileRangeClass::Demand => state.demand.push_back(id),
                        FileRangeClass::Prefetch => state.prefetch.push_back(id),
                    }
                }
                work
            };
            self.changed.notify_waiters();
            self.spawn_segment(work);
        }
    }

    fn choose_locked(&self, state: &mut State) -> Option<u64> {
        for class in [FileRangeClass::Demand, FileRangeClass::Prefetch] {
            if class == FileRangeClass::Prefetch && self.demand_waiters.load(Ordering::SeqCst) != 0
            {
                return None;
            }
            let queue = match class {
                FileRangeClass::Demand => &state.demand,
                FileRangeClass::Prefetch => &state.prefetch,
            };
            let mut queries = Vec::new();
            for id in queue {
                let request = &state.requests[id];
                if !request.paused
                    && state
                        .source_active
                        .get(&request.scope)
                        .copied()
                        .unwrap_or(0)
                        < self.source_window
                    && !queries.contains(&request.scope.query)
                {
                    queries.push(request.scope.query);
                }
            }
            if queries.is_empty() {
                continue;
            }
            let query_index = state
                .last_query
                .and_then(|last| {
                    queries
                        .iter()
                        .position(|query| *query == last)
                        .map(|index| (index + 1) % queries.len())
                })
                .unwrap_or(0);
            let query = queries[query_index];
            let mut sources = Vec::new();
            for id in queue {
                let request = &state.requests[id];
                if !request.paused
                    && request.scope.query == query
                    && state
                        .source_active
                        .get(&request.scope)
                        .copied()
                        .unwrap_or(0)
                        < self.source_window
                    && !sources.contains(&request.scope.source)
                {
                    sources.push(request.scope.source);
                }
            }
            let source_index = state
                .last_source
                .get(&query)
                .and_then(|last| {
                    sources
                        .iter()
                        .position(|source| source == last)
                        .map(|index| (index + 1) % sources.len())
                })
                .unwrap_or(0);
            let source = sources[source_index];
            let queue = match class {
                FileRangeClass::Demand => &mut state.demand,
                FileRangeClass::Prefetch => &mut state.prefetch,
            };
            let index = queue
                .iter()
                .position(|id| {
                    let request = &state.requests[id];
                    !request.paused
                        && request.scope.query == query
                        && request.scope.source == source
                })
                .expect("eligible request");
            state.last_query = Some(query);
            state.last_source.insert(query, source);
            return queue.remove(index);
        }
        None
    }

    fn spawn_segment(self: &Arc<Self>, work: Dispatch) {
        let Dispatch {
            id,
            scope,
            file,
            cancellation,
            segment,
        } = work;
        let Segment {
            offset,
            mut bytes,
            index,
        } = segment;
        let len = bytes.len() as u64;
        let (sender, receiver) = oneshot::channel();
        let task = self.task_spawner.spawn(Box::pin(async move {
            let result = file
                .read_into(
                    FileReadRange::Bounded {
                        offset,
                        length: len,
                    },
                    &mut bytes,
                    &cancellation,
                )
                .await
                .map(|()| bytes);
            let _ = sender.send(result);
        }));
        match task {
            Ok(task) => {
                let service = Arc::clone(self);
                let supervisor = self.scan_handle.spawn(async move {
                    let result = receiver.await.unwrap_or_else(|_| {
                        Err(FileError::new(
                            FileErrorKind::Internal,
                            "range segment exited without result",
                        ))
                    });
                    service.segment_result(id, index, result);
                    let exit = task.drain().await;
                    service.segment_exit(id, scope, exit);
                });
                if let Ok(mut supervisors) = self.supervisors.lock() {
                    supervisors.retain(|supervisor| !supervisor.is_finished());
                    supervisors.push(supervisor);
                }
            }
            Err(error) => {
                self.segment_result(id, index, Err(error));
                self.segment_exit(id, scope, Ok(()));
            }
        }
    }

    fn segment_result(&self, id: u64, index: usize, result: FileResult<BytesMut>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        match result {
            Ok(bytes) => {
                if let Some(request) = state.requests.get_mut(&id) {
                    request.completed[index] = Some(bytes);
                }
            }
            Err(error) => self.fail_request_locked(&mut state, id, error),
        }
    }

    fn segment_exit(self: &Arc<Self>, id: u64, scope: FileRangeScope, exit: FileResult<()>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.active -= 1;
        let source_active = state
            .source_active
            .get_mut(&scope)
            .expect("active range source");
        *source_active -= 1;
        if *source_active == 0 {
            state.source_active.remove(&scope);
        }
        if let Some(request) = state.requests.get_mut(&id) {
            request.active -= 1;
            if let Err(error) = exit {
                if let Some(request) = state.requests.get_mut(&id) {
                    request
                        .exit_error
                        .get_or_insert_with(|| FileError::new(error.kind(), error.to_string()));
                }
                self.fail_request_locked(&mut state, id, error);
            }
            if state
                .requests
                .get(&id)
                .is_some_and(|request| request.active == 0 && request.pending.is_empty())
            {
                self.finish_locked(&mut state, id);
            }
        }
        drop(state);
        self.changed.notify_waiters();
        self.dispatch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileIdentity, FileTask, FileTaskFuture, FsAccessResolver};
    use novarocks_spi::connector::StorageAccessDomainId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    struct GateSpawner {
        permits: Arc<Semaphore>,
        started: AtomicUsize,
    }

    impl GateSpawner {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                permits: Arc::new(Semaphore::new(0)),
                started: AtomicUsize::new(0),
            })
        }
        fn release(&self, count: usize) {
            self.permits.add_permits(count);
        }
        fn started(&self) -> usize {
            self.started.load(Ordering::SeqCst)
        }
    }

    impl FileTaskSpawner for GateSpawner {
        fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
            self.started.fetch_add(1, Ordering::SeqCst);
            let permits = Arc::clone(&self.permits);
            Ok(FileTask::new(tokio::spawn(async move {
                let permit = permits.acquire_owned().await.expect("gate open");
                permit.forget();
                task.await;
            })))
        }
        fn spawn_detached_blocking(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
            unreachable!()
        }
    }

    fn fixture() -> (tempfile::TempDir, BoundFile) {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("range.parquet");
        std::fs::write(&path, b"abcdefghijklmnop").expect("fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([7; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(0, FileIdentity::new(path.to_string_lossy(), 16, None))
            .expect("bound file");
        (dir, file)
    }

    fn scope(query: i64, source: i64) -> FileRangeScope {
        FileRangeScope::try_new(query, 0, 1, source, 0, 1).expect("scope")
    }

    fn range(offset: u64, length: u64) -> FileReadRange {
        FileReadRange::bounded(offset, length).expect("range")
    }

    #[tokio::test]
    async fn drain_registers_wakeup_before_last_request_exits() {
        let (_dir, file) = fixture();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            GateSpawner::new(),
            Handle::current(),
        );
        service.state.lock().expect("state").requests.insert(
            7,
            RequestState {
                scope: scope(1, 1),
                class: FileRangeClass::Demand,
                paused: false,
                file,
                cancellation: FileCancellation::new(),
                pending: VecDeque::new(),
                completed: Vec::new(),
                expected_length: 0,
                active: 0,
                failed: false,
                exit_error: None,
                result: None,
                exit: None,
            },
        );
        let mut removed = false;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.wait_requests_empty(|| {
                if !removed {
                    removed = true;
                    service.state.lock().expect("state").requests.remove(&7);
                    service.changed.notify_waiters();
                }
            }),
        )
        .await
        .expect("last-exit notification cannot be lost")
        .expect("drain wait");
        assert!(removed);
    }

    #[tokio::test]
    async fn prepared_middle_span_dispatches_only_two_missing_segments() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let present = PreparedFileInput::new(&file, 4, BytesMut::from(&b"efgh"[..]))
            .expect("prepared middle span");
        let mut request = match service
            .try_start_with_present(
                scope(1, 1),
                FileRangeClass::Demand,
                file,
                range(0, 12),
                FileCancellation::new(),
                Some(present),
            )
            .expect("range request")
        {
            FileRangeStart::Started(request) => request,
            FileRangeStart::Deferred => panic!("unexpected defer"),
        };
        assert_eq!(request.partial_copy_bytes(), 4);
        assert_eq!(request.missing_bytes(), 8);
        assert_eq!(spawner.started(), 2);
        spawner.release(2);
        assert_eq!(
            request.result_ready().await.expect("filled input"),
            b"abcdefghijkl"[..]
        );
        request.drained().await.expect("actual exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn prefetch_result_carries_owned_backing_capacity_and_exact_identity() {
        let (_dir, file) = fixture();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(crate::TokioFileTaskSpawner::new(Handle::current())),
            Handle::current(),
        );
        let mut request = service
            .start(
                scope(1, 1),
                FileRangeClass::Prefetch,
                file.clone(),
                range(2, 7),
                FileCancellation::new(),
            )
            .expect("prefetch");
        let input = request.prepared_input_ready().await.expect("owned input");
        assert_eq!(input.range(), 2..9);
        assert_eq!(input.retained_backing_capacity(), 7);
        assert_eq!(input.access_domain(), file.access_domain());
        assert_eq!(input.identity(), file.identity());
        request.drained().await.expect("physical exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn prepared_input_rejects_other_domain_or_file_identity() {
        let (_dir, file) = fixture();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            GateSpawner::new(),
            Handle::current(),
        );
        let same_path = file.identity().path().to_owned();
        let other_domain = FsAccessResolver::new()
            .resolve_location(StorageAccessDomainId::from_bytes([8; 32]), &same_path, None)
            .expect("other access")
            .bind(0, file.identity().clone())
            .expect("other bound file");
        let other_identity = file
            .access()
            .bind(0, FileIdentity::new(&same_path, 15, None))
            .expect("other identity");
        for wrong in [other_domain, other_identity] {
            let present = PreparedFileInput::new(&wrong, 0, BytesMut::from(&b"abcd"[..]))
                .expect("prepared wrong file");
            assert!(
                service
                    .try_start_with_present(
                        scope(1, 1),
                        FileRangeClass::Demand,
                        file.clone(),
                        range(0, 8),
                        FileCancellation::new(),
                        Some(present),
                    )
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn failed_missing_span_never_publishes_partial_input() {
        let (dir, file) = fixture();
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            Arc::new(crate::TokioFileTaskSpawner::new(Handle::current())),
            Handle::current(),
        );
        let present = PreparedFileInput::new(&file, 4, BytesMut::from(&b"efgh"[..]))
            .expect("prepared middle span");
        std::fs::remove_file(dir.path().join("range.parquet")).expect("remove source");
        let mut request = service
            .start_wait_with_present(
                scope(1, 1),
                file,
                range(0, 12),
                FileCancellation::new(),
                Some(present),
            )
            .await
            .expect("accepted request");
        assert!(request.result_ready().await.is_err());
        request.drained().await.expect("actual exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn cancelled_partial_fill_keeps_its_slot_until_physical_exit() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let present = PreparedFileInput::new(&file, 4, BytesMut::from(&b"efgh"[..]))
            .expect("prepared middle span");
        let mut partial = match service
            .try_start_with_present(
                scope(1, 1),
                FileRangeClass::Demand,
                file,
                range(0, 12),
                FileCancellation::new(),
                Some(present),
            )
            .expect("partial request")
        {
            FileRangeStart::Started(request) => request,
            FileRangeStart::Deferred => panic!("unexpected defer"),
        };
        assert_eq!(spawner.started(), 1);
        partial.request_stop();
        assert!(partial.result_ready().await.is_err());
        let drain = tokio::spawn(partial.drained());
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        assert_eq!(spawner.started(), 1);
        spawner.release(1);
        drain.await.expect("join drain").expect("physical exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn demand_overtakes_queued_prefetch_and_source_slot_waits_for_exit() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut first = service
            .start(
                scope(1, 1),
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("first");
        assert_eq!(spawner.started(), 1);
        let mut speculative = match service
            .try_start(
                scope(1, 2),
                FileRangeClass::Prefetch,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("prefetch")
        {
            FileRangeStart::Started(request) => request,
            FileRangeStart::Deferred => panic!("unexpected defer"),
        };
        let mut later_demand = service
            .start(
                scope(2, 1),
                FileRangeClass::Demand,
                file,
                range(8, 4),
                FileCancellation::new(),
            )
            .expect("demand");
        spawner.release(1);
        assert_eq!(
            first.result_ready().await.expect("first bytes").as_ref(),
            b"abcd"
        );
        first.drained().await.expect("first exit");
        for _ in 0..100 {
            if spawner.started() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(spawner.started(), 2);
        spawner.release(1);
        assert_eq!(
            later_demand
                .result_ready()
                .await
                .expect("demand bytes")
                .as_ref(),
            b"ijkl"
        );
        later_demand.drained().await.expect("demand exit");
        spawner.release(1);
        assert_eq!(
            speculative
                .result_ready()
                .await
                .expect("prefetch bytes")
                .as_ref(),
            b"efgh"
        );
        speculative.drained().await.expect("prefetch exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn queued_prefetch_defers_and_cancelled_active_request_keeps_slot_until_exit() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut active = service
            .start(
                scope(1, 1),
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("active");
        let mut waiting = service
            .start(
                scope(2, 1),
                FileRangeClass::Demand,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("waiting");
        assert!(matches!(
            service
                .try_start(
                    scope(3, 1),
                    FileRangeClass::Prefetch,
                    file,
                    range(8, 4),
                    FileCancellation::new()
                )
                .expect("prefetch attempt"),
            FileRangeStart::Deferred
        ));
        active.request_stop();
        assert_eq!(
            spawner.started(),
            1,
            "cancellation has not released the physical slot"
        );
        spawner.release(1);
        assert_eq!(
            active
                .result_ready()
                .await
                .expect_err("cancelled read")
                .kind(),
            FileErrorKind::Cancelled
        );
        active.drained().await.expect("actual exit");
        for _ in 0..100 {
            if spawner.started() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(spawner.started(), 2);
        spawner.release(1);
        assert_eq!(
            waiting
                .result_ready()
                .await
                .expect("waiting bytes")
                .as_ref(),
            b"efgh"
        );
        waiting.drained().await.expect("waiting exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn demand_rotates_queries_then_sources() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(3).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut q1s1 = service
            .start(
                scope(1, 1),
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("first");
        let mut q1s1_next = service
            .start(
                scope(1, 1),
                FileRangeClass::Demand,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("same source");
        let mut q1s2 = service
            .start(
                scope(1, 2),
                FileRangeClass::Demand,
                file.clone(),
                range(8, 4),
                FileCancellation::new(),
            )
            .expect("other source");
        let mut q2s1 = service
            .start(
                scope(2, 1),
                FileRangeClass::Demand,
                file,
                range(12, 4),
                FileCancellation::new(),
            )
            .expect("other query");
        spawner.release(1);
        assert_eq!(
            q1s1.result_ready().await.expect("first bytes").as_ref(),
            b"abcd"
        );
        q1s1.drained().await.expect("first exit");
        spawner.release(1);
        assert_eq!(
            q2s1.result_ready().await.expect("query rotation").as_ref(),
            b"mnop"
        );
        q2s1.drained().await.expect("query exit");
        spawner.release(1);
        assert_eq!(
            q1s2.result_ready().await.expect("source rotation").as_ref(),
            b"ijkl"
        );
        q1s2.drained().await.expect("source exit");
        spawner.release(1);
        assert_eq!(
            q1s1_next
                .result_ready()
                .await
                .expect("remaining bytes")
                .as_ref(),
            b"efgh"
        );
        q1s1_next.drained().await.expect("remaining exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn waiting_demand_blocks_new_prefetch_until_it_enters_queue() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut first = service
            .start(
                scope(1, 1),
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("active demand");
        let mut queued = service
            .start(
                scope(2, 1),
                FileRangeClass::Demand,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("queued demand");
        let waiting_service = Arc::clone(&service);
        let waiting_file = file.clone();
        let waiting = tokio::spawn(async move {
            waiting_service
                .start_wait(
                    scope(3, 1),
                    waiting_file,
                    range(8, 4),
                    FileCancellation::new(),
                )
                .await
        });
        for _ in 0..100 {
            if service.demand_waiters.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(service.demand_waiters.load(Ordering::SeqCst), 1);
        assert!(matches!(
            service
                .try_start(
                    scope(4, 1),
                    FileRangeClass::Prefetch,
                    file,
                    range(12, 4),
                    FileCancellation::new()
                )
                .expect("prefetch attempt"),
            FileRangeStart::Deferred
        ));
        spawner.release(1);
        first.result_ready().await.expect("active result");
        first.drained().await.expect("active exit");
        spawner.release(1);
        queued.result_ready().await.expect("queued result");
        queued.drained().await.expect("queued exit");
        let mut admitted = waiting
            .await
            .expect("waiter task")
            .expect("waiting demand admitted");
        spawner.release(1);
        assert_eq!(
            admitted
                .result_ready()
                .await
                .expect("waiting result")
                .as_ref(),
            b"ijkl"
        );
        admitted.drained().await.expect("waiting exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[test]
    fn prefetch_submission_from_scan_cpu_does_not_need_a_caller_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("scan runtime");
        let (_dir, file) = fixture();
        let spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(crate::TokioFileTaskSpawner::new(runtime.handle().clone()));
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            spawner,
            runtime.handle().clone(),
        );
        let mut request = match service
            .try_start(
                scope(1, 1),
                FileRangeClass::Prefetch,
                file,
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("CPU submit")
        {
            FileRangeStart::Started(request) => request,
            FileRangeStart::Deferred => panic!("free slot should accept prefetch"),
        };
        runtime.block_on(async {
            assert_eq!(
                request.result_ready().await.expect("bytes").as_ref(),
                b"abcd"
            );
            request.drained().await.expect("actual exit");
            service.close_admission();
            service.drain().await.expect("service drain");
        });
    }

    #[tokio::test]
    async fn adjacent_segments_rejoin_into_the_exact_backing() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("large.parquet");
        let mut output = std::fs::File::create(&path).expect("large fixture");
        output
            .set_len((SEGMENT_BYTES + 4) as u64)
            .expect("sparse fixture");
        output.write_all(b"head").expect("first bytes");
        output
            .seek(SeekFrom::Start(SEGMENT_BYTES as u64))
            .expect("tail position");
        output.write_all(b"tail").expect("last bytes");
        drop(output);
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([8; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(
                0,
                FileIdentity::new(path.to_string_lossy(), (SEGMENT_BYTES + 4) as u64, None),
            )
            .expect("bound file");
        let spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(crate::TokioFileTaskSpawner::new(Handle::current()));
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner,
            Handle::current(),
        );
        let mut request = service
            .start(
                scope(1, 1),
                FileRangeClass::Demand,
                file,
                FileReadRange::WholeFile,
                FileCancellation::new(),
            )
            .expect("large range");
        let bytes = request.result_ready().await.expect("exact result");
        assert_eq!(bytes.len(), SEGMENT_BYTES + 4);
        assert_eq!(&bytes[..4], b"head");
        assert_eq!(&bytes[SEGMENT_BYTES..], b"tail");
        request.drained().await.expect("segment exits");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn paused_prefetch_keeps_ready_segment_and_blocks_queued_segment() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("paused-large.parquet");
        std::fs::File::create(&path)
            .expect("fixture")
            .set_len((SEGMENT_BYTES + 4) as u64)
            .expect("sparse fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([18; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(
                0,
                FileIdentity::new(path.to_string_lossy(), (SEGMENT_BYTES + 4) as u64, None),
            )
            .expect("bound file");
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut prefetch = service
            .start(
                scope(1, 1),
                FileRangeClass::Prefetch,
                file.clone(),
                FileReadRange::WholeFile,
                FileCancellation::new(),
            )
            .expect("prefetch");
        assert_eq!(spawner.started(), 1);
        let control = prefetch.control();
        control.request_pause();
        spawner.release(1);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let changed = service.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if service.state.lock().expect("state").active == 0 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("first segment exits");
        assert_eq!(spawner.started(), 1, "queued segment remains paused");
        let mut demand = service
            .start(
                scope(2, 1),
                FileRangeClass::Demand,
                file,
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("demand passes paused prefetch");
        assert_eq!(spawner.started(), 2);
        spawner.release(1);
        assert_eq!(demand.result_ready().await.expect("demand bytes").len(), 4);
        demand.drained().await.expect("demand exit");
        assert_eq!(spawner.started(), 2);
        control.request_resume();
        assert_eq!(spawner.started(), 3);
        spawner.release(1);
        assert_eq!(
            prefetch
                .result_ready()
                .await
                .expect("completed prefetch")
                .len(),
            SEGMENT_BYTES + 4
        );
        prefetch.drained().await.expect("prefetch exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    struct FailFirstSpawner {
        gate: Arc<Semaphore>,
        count: AtomicUsize,
    }

    impl FileTaskSpawner for FailFirstSpawner {
        fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
            let first = self.count.fetch_add(1, Ordering::SeqCst) == 0;
            let gate = Arc::clone(&self.gate);
            Ok(FileTask::new(tokio::spawn(async move {
                if first {
                    drop(task);
                    return;
                }
                gate.acquire_owned().await.expect("gate").forget();
                task.await;
            })))
        }
        fn spawn_detached_blocking(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn first_segment_error_is_reported_before_held_sibling_exits() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("large.parquet");
        std::fs::File::create(&path)
            .expect("fixture")
            .set_len((SEGMENT_BYTES + 4) as u64)
            .expect("sparse fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([8; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(
                0,
                FileIdentity::new(path.to_string_lossy(), (SEGMENT_BYTES + 4) as u64, None),
            )
            .expect("bound file");
        let spawner = Arc::new(FailFirstSpawner {
            gate: Arc::new(Semaphore::new(0)),
            count: AtomicUsize::new(0),
        });
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut request = service
            .start(
                scope(1, 1),
                FileRangeClass::Demand,
                file,
                FileReadRange::WholeFile,
                FileCancellation::new(),
            )
            .expect("large range");
        assert_eq!(
            request
                .result_ready()
                .await
                .expect_err("first error")
                .kind(),
            FileErrorKind::Internal
        );
        assert_eq!(spawner.count.load(Ordering::SeqCst), 2);
        assert_eq!(
            service.state.lock().unwrap().active,
            1,
            "held sibling still owns its physical slot"
        );
        spawner.gate.add_permits(1);
        request.drained().await.expect("sibling exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }
}
