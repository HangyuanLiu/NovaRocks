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

//! Shared private original-payload retirement. No independent admission or registry.
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::{
    runtime::Handle,
    task::{JoinError, JoinHandle},
};
type RawValue = Box<dyn Any + Send>;

// The caller-owned backing remains in the original record and private carrier.
// This shared kernel has no admission authority or slot storage of its own.
struct BackedValue<B: Send + Sync + 'static> {
    value: Option<RawValue>,
    backing: Option<Arc<B>>,
}
impl<B: Send + Sync + 'static> Drop for BackedValue<B> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value)))
            {
                resume_backed_panic(payload, self.backing.take());
            }
        }
    }
}
fn resume_backed_panic<B: Send + Sync + 'static>(payload: RawValue, backing: Option<Arc<B>>) -> ! {
    // A successor already emitted by this private carrier must not acquire a
    // growing tower of wrappers. Preserve its raw object, flatten only the
    // SAME backing, and never interpret a foreign/user panic payload.
    let payload: RawValue = match payload.downcast::<BackedValue<B>>() {
        Ok(mut previous)
            if previous
                .backing
                .as_ref()
                .zip(backing.as_ref())
                .is_some_and(|(a, b)| Arc::ptr_eq(a, b)) =>
        {
            let value = previous
                .value
                .take()
                .expect("one original retirement payload");
            previous.backing.take();
            value
        }
        Ok(previous) => previous,
        Err(payload) => payload,
    };
    std::panic::resume_unwind(Box::new(BackedValue {
        value: Some(payload),
        backing,
    }));
}
struct RetirementInput<B: Send + Sync + 'static> {
    value: Mutex<Option<BackedValue<B>>>,
}
pub(super) enum CleanupState {
    NotStarted,
    Running(JoinHandle<()>),
    // If a queued callback never took its original input, retain BOTH raw
    // objects. Runtime teardown cannot be repaired by fabricating cleanup.
    #[allow(
        dead_code,
        reason = "retain the actual unretired raw error; no fabricated source read"
    )]
    Stalled(JoinError),
    // The API unwound without returning an original cleanup handle. Retain
    // its actual raw cause AND original input custody; never respawn.
    #[allow(
        dead_code,
        reason = "retain the actual raw spawn cause without another spawn or false join"
    )]
    SpawnUnjoinable(RawValue),
    Complete,
}
pub(super) struct OriginalRetirement<B: Send + Sync + 'static> {
    input: Arc<RetirementInput<B>>,
    backing: Arc<B>,
    pub(super) state: CleanupState,
    pub(super) cleanup_failures: u64,
}
impl<B: Send + Sync + 'static> OriginalRetirement<B> {
    pub(super) fn new<T: Any + Send>(original: T, backing: Arc<B>) -> Self {
        let input = Arc::new(RetirementInput {
            value: Mutex::new(Some(BackedValue {
                value: Some(Box::new(original)),
                backing: Some(Arc::clone(&backing)),
            })),
        });
        Self {
            input,
            backing,
            state: CleanupState::NotStarted,
            cleanup_failures: 0,
        }
    }
    pub(super) fn poll(&mut self, handle: &Handle, cx: &mut std::task::Context<'_>) -> Poll<()> {
        if matches!(self.state, CleanupState::NotStarted) {
            let input = Arc::clone(&self.input);
            // The input already belongs to this record. A spawn panic/drop of
            // the closure releases only this Arc, never the retained raw value.
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handle.spawn_blocking(move || {
                    let value = input
                        .value
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .take()
                        .expect("one original retirement callback");
                    drop(value);
                })
            })) {
                Ok(original) => self.state = CleanupState::Running(original),
                Err(actual) => {
                    self.cleanup_failures = self.cleanup_failures.saturating_add(1);
                    self.state = CleanupState::SpawnUnjoinable(actual);
                    return Poll::Pending;
                }
            }
        }
        let joined = match &mut self.state {
            CleanupState::Running(original) => Pin::new(original).poll(cx),
            CleanupState::Complete => return Poll::Ready(()),
            CleanupState::Stalled(_) | CleanupState::SpawnUnjoinable(_) => return Poll::Pending,
            CleanupState::NotStarted => unreachable!(),
        };
        match joined {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.state = CleanupState::Complete;
                Poll::Ready(())
            }
            Poll::Ready(Err(original)) => {
                self.cleanup_failures = self.cleanup_failures.saturating_add(1);
                if self
                    .input
                    .value
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_some()
                {
                    // Actual cleanup join returned, but actual destruction did
                    // not happen. Do not overwrite/drop its original input.
                    self.state = CleanupState::Stalled(original);
                } else {
                    // A destructor panic's actual JoinError owns the successor
                    // raw object. Reuse this SAME record and SAME reservations.
                    self.input = Arc::new(RetirementInput {
                        value: Mutex::new(Some(BackedValue {
                            value: Some(Box::new(original)),
                            backing: Some(Arc::clone(&self.backing)),
                        })),
                    });
                    self.state = CleanupState::NotStarted;
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
        }
    }
}
