// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Original, finite positions for actual derived Native HTTP/2 tasks.
//! The caller must pregrant the complete capacity before constructing this
//! executor. This is a holder of that capability, never a capacity authority.

use std::alloc::Layout;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use crate::native_incoming_key_capacity::NativeIncomingKey;
use crate::native_transport_capacity::NativeIncomingConnectionBinding;
use bytes::Bytes;
use hyper::http;
use hyper::rt::Executor;
use novarocks_native_trust::NativeServerAdmission;
use novarocks_proto_codec::native_rpc::{NativeEndpointDomain, NativeRpcMethod};

#[derive(Clone)]
struct NativeIncomingHeadAdmission {
    binding: NativeIncomingConnectionBinding,
    admission: NativeServerAdmission,
    domain: NativeEndpointDomain,
}

impl NativeIncomingHeadAdmission {
    fn admit(&self, uri: &http::Uri, headers: &http::HeaderMap) -> io::Result<()> {
        // Preserve authentication errors and unknown/wrong-endpoint fallback.
        // Only a legal authenticated method can establish a lane authority.
        let Some(method) = NativeRpcMethod::from_path(uri.path()) else {
            return Ok(());
        };
        if !method.is_allowed_at(self.domain) {
            return Ok(());
        }
        let Ok(caller) = self.admission.admit_headers(headers) else {
            return Ok(());
        };
        let peer = caller
            .process_identity()
            .ok_or(io::ErrorKind::PermissionDenied)?;
        let key = NativeIncomingKey::new(peer, self.domain, method.contract().traffic)?;
        self.binding.seal(key)
    }
}

struct TaskPoolCore {
    // After the strong-only Arc allocation exits, these backings exit before
    // the original connection capability. No task or Weak is stored here.
    positions: Vec<AtomicU8>,
    task_bound: usize,
    _original: Bytes,
}

struct TaskPool {
    core: Option<Arc<TaskPoolCore>>,
}

impl Clone for TaskPool {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(self.core())),
        }
    }
}

impl Drop for TaskPool {
    fn drop(&mut self) {
        if let Some(core) = self.core.take() {
            // Arc deallocation precedes the moved Core's Vec and capability.
            drop(Arc::into_inner(core));
        }
    }
}

impl TaskPool {
    fn core(&self) -> &Arc<TaskPoolCore> {
        self.core.as_ref().expect("original task pool is live")
    }

    fn claim(&self, actual_bound: usize) -> io::Result<PreparedTask> {
        let core = self.core();
        if actual_bound > core.task_bound {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        for (index, position) in core.positions.iter().enumerate() {
            if position
                .compare_exchange(FREE, PREPARED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // The wrapper is also prepaid. Its exit guard runs after its
                // backing is freed, and retains this same original pool.
                return Ok(PreparedTask {
                    owner: Bytes::from_owner_with_exit_guard(
                        Bytes::new(),
                        TaskExit {
                            pool: self.clone(),
                            index,
                        },
                    ),
                    index,
                });
            }
        }
        Err(io::ErrorKind::WouldBlock.into())
    }
}

struct TaskExit {
    pool: TaskPool,
    index: usize,
}

impl Drop for TaskExit {
    fn drop(&mut self) {
        // All Cell/automatic-FutureBox and this carrier's allocations have
        // exited. A retained Waker prevents reaching this transition.
        let previous = self.pool.core().positions[self.index].swap(FREE, Ordering::Release);
        assert!(
            previous != FREE,
            "original task position exits exactly once"
        );
    }
}

/// A connection-specific executor and, after preparation, one original task
/// position. Clone does not obtain a new position or create a new grant.
#[derive(Clone)]
pub struct NativeTaskExecutor {
    head: Option<NativeIncomingHeadAdmission>,
    pool: Option<TaskPool>,
    prepared: Option<PreparedTask>,
}

const FREE: u8 = 0;
const PREPARED: u8 = 1;
const EXECUTED: u8 = 2;

#[derive(Clone)]
struct PreparedTask {
    owner: Bytes,
    index: usize,
}

impl NativeTaskExecutor {
    /// Keep the ordinary Tokio execution path for callers without an original
    /// transport capability. Production funded listeners use `with_original`.
    pub fn ordinary() -> Self {
        Self {
            head: None,
            pool: None,
            prepared: None,
        }
    }

    pub(crate) fn with_incoming_head(
        mut self,
        binding: NativeIncomingConnectionBinding,
        admission: NativeServerAdmission,
        domain: NativeEndpointDomain,
    ) -> Self {
        self.head = Some(NativeIncomingHeadAdmission {
            binding,
            admission,
            domain,
        });
        self
    }

    /// Total requested backing: strong-only pool, its fixed positions, every
    /// original carrier, and every actual Cell/automatic-FutureBox position.
    pub fn allocation_capacity_bound(positions: usize, task_bound: usize) -> io::Result<usize> {
        if positions == 0 || task_bound == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let arc = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<TaskPoolCore>())
            .map_err(|_| io::ErrorKind::InvalidInput)?
            .0
            .pad_to_align()
            .size();
        let table = Layout::array::<AtomicU8>(positions)
            .map_err(|_| io::ErrorKind::InvalidInput)?
            .size();
        let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, TaskExit>();
        task_bound
            .checked_add(carrier)
            .and_then(|v| v.checked_mul(positions))
            .and_then(|v| v.checked_add(table))
            .and_then(|v| v.checked_add(arc))
            .filter(|v| *v <= isize::MAX as usize)
            .ok_or_else(|| io::ErrorKind::InvalidInput.into())
    }

    /// Construct only after obtaining `allocation_capacity_bound` from the
    /// caller's existing original stock. The original survives all construction
    /// unwind paths as well as the actual pool Arc/position Vec deallocation.
    pub fn with_original(positions: usize, task_bound: usize, owner: Bytes) -> io::Result<Self> {
        let original = owner;
        Self::allocation_capacity_bound(positions, task_bound)?;
        let mut records = Vec::with_capacity(positions);
        for _ in 0..positions {
            records.push(AtomicU8::new(FREE));
        }
        let core = Arc::new(TaskPoolCore {
            positions: records,
            task_bound,
            _original: original.clone(),
        });
        drop(original);
        Ok(Self {
            head: None,
            pool: Some(TaskPool { core: Some(core) }),
            prepared: None,
        })
    }
}

impl<F> Executor<F> for NativeTaskExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn task_allocation_capacity_bound() -> io::Result<usize> {
        tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
    }

    fn try_prepare_task(&self) -> io::Result<Option<Self>> {
        let Some(pool) = &self.pool else {
            return Ok(None);
        };
        if self.prepared.is_some() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let bound = tokio::runtime::Handle::task_allocation_capacity_bound::<F>()?;
        let prepared = pool.claim(bound)?;
        Ok(Some(Self {
            head: self.head.clone(),
            pool: Some(pool.clone()),
            prepared: Some(prepared),
        }))
    }

    fn admit_request_head(&self, uri: &http::Uri, headers: &http::HeaderMap) -> io::Result<()> {
        match &self.head {
            Some(head) => head.admit(uri, headers),
            None => Ok(()),
        }
    }

    fn execute(&self, future: F) {
        match (&self.pool, &self.prepared) {
            (None, None) => {
                tokio::spawn(future);
            }
            (Some(pool), Some(prepared)) => {
                // A clone is an alias, not permission to submit another Cell.
                // Check the actual current F as well: this public executor can
                // otherwise be prepared using a smaller Future family.
                let Ok(actual) = tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
                else {
                    drop(future);
                    return;
                };
                if actual > pool.core().task_bound
                    || pool.core().positions[prepared.index]
                        .compare_exchange(PREPARED, EXECUTED, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                {
                    drop(future);
                    return;
                }
                // Preparation already checked this exact F/configuration. The
                // same original position now lives in the actual TaskCell.
                // Tokio destroys a refused future before returning its owner;
                // never route a refused funded task to ordinary spawn.
                let _ = tokio::runtime::Handle::current()
                    .spawn_with_task_owner(future, prepared.owner.clone());
            }
            _ => {
                // Direct execution without the original preparation is refused.
                // Dropping the unsubmitted HTTP/2 task also drops its responder.
                drop(future);
            }
        }
    }
}

#[cfg(test)]
#[path = "native_task_executor/tests_incoming.rs"]
mod tests_incoming;
