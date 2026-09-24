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

//! Page streams the host polls, and what closing one means.
//!
//! A connector read is a [`ConnectorPageStream`]: the host polls it with its
//! own task context, so a read waiting for I/O parks the host driver instead
//! of a thread. Three small, runtime-neutral pieces make that safe:
//!
//! - [`ConnectorPollBudget`] bounds the CPU work one host scheduling turn
//!   spends inside a stream. A provider consumes it at its natural work
//!   units; when it runs out, the stream yields and the host ends its turn.
//! - [`ConnectorSourceOperations`] records every operation a source started
//!   before any work is submitted for it, so that closing the source can
//!   stop them all and observe their actual exit.
//! - [`ConnectorPageStream::close`] consumes the stream, seals its
//!   operations and returns a future that only observes their exit.
//!   Dropping that future, or never polling it, changes no responsibility.
//!
//! No runtime type crosses this interface.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures::Stream;
use futures::future::BoxFuture;

use super::page_source::{PageSourceMetrics, SourcePage};
use crate::connector::{ConnectorError, ConnectorErrorKind};

/// A connector read the host polls for pages.
///
/// `Poll::Pending` means the stream registered the context's waker and will
/// wake it when it can make progress: it is never end of stream, and neither
/// is an empty page. `Ready(None)` means no more pages; background work may
/// still be ending, which [`Self::close`] observes. After an error the host
/// polls the stream no more.
pub trait ConnectorPageStream: Stream<Item = Result<SourcePage, ConnectorError>> + Send {
    fn metrics(&self) -> PageSourceMetrics;

    fn memory_usage_bytes(&self) -> u64;

    /// Ends delivery, seals the source's operations and asks each one to
    /// stop, all before returning. The returned future resolves once every
    /// operation the source started has exited, with the first real error;
    /// it observes only, so dropping it does not stop or leak anything.
    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>>;
}

/// A page stream owned by the one host driver that polls it.
pub type OwnedConnectorPageStream = Pin<Box<dyn ConnectorPageStream>>;

/// Cooperative CPU budget for the work one host scheduling turn does inside
/// its streams.
///
/// The host refills the budget once per turn and hands the same budget to
/// every stream and nested stream it polls in that turn. A provider calls
/// [`Self::consume`] at its natural units of CPU work (a decoded batch, a
/// merged key run, a skipped empty batch). While the turn has budget the
/// call resolves at once. When the budget runs out it wakes the task and
/// returns `Pending` once, which ends the host's turn; the provider resumes
/// after the host polls it again in a later, refilled turn.
///
/// This is a cooperation point, not preemption: work between two
/// consumptions runs to completion.
#[derive(Clone, Default)]
pub struct ConnectorPollBudget {
    inner: Arc<PollBudgetInner>,
}

#[derive(Default)]
struct PollBudgetInner {
    remaining: AtomicU64,
    exhaustions: AtomicU64,
}

impl ConnectorPollBudget {
    pub fn new() -> Self {
        Self::default()
    }

    /// Host: sets the budget of a new scheduling turn.
    pub fn refill(&self, units: u64) {
        self.inner.remaining.store(units, Ordering::Release);
    }

    /// Units left in the current turn.
    pub fn remaining(&self) -> u64 {
        self.inner.remaining.load(Ordering::Acquire)
    }

    /// How many times a stream ran out of budget and yielded, so a host can
    /// tell a CPU yield from waiting for I/O.
    pub fn exhaustions(&self) -> u64 {
        self.inner.exhaustions.load(Ordering::Acquire)
    }

    /// Provider: spends `units` of CPU work; see [`ConnectorPollBudget`].
    pub fn consume(&self, units: u64) -> BudgetConsume {
        BudgetConsume {
            budget: self.clone(),
            units,
            yielded: false,
        }
    }

    fn try_take(&self, units: u64) -> bool {
        self.inner
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(units)
            })
            .is_ok()
    }
}

impl fmt::Debug for ConnectorPollBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorPollBudget")
            .field("remaining", &self.remaining())
            .field("exhaustions", &self.exhaustions())
            .finish()
    }
}

/// Future returned by [`ConnectorPollBudget::consume`].
#[must_use = "budget is only spent when the consumption is awaited"]
pub struct BudgetConsume {
    budget: ConnectorPollBudget,
    units: u64,
    yielded: bool,
}

impl Future for BudgetConsume {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            // Resumed in a later turn: charge that turn for the work that
            // is about to run, without yielding a second time.
            let units = self.units;
            let _ = self.budget.inner.remaining.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |remaining| Some(remaining.saturating_sub(units)),
            );
            return Poll::Ready(());
        }
        if self.budget.try_take(self.units) {
            return Poll::Ready(());
        }
        self.budget.inner.remaining.store(0, Ordering::Release);
        self.budget.inner.exhaustions.fetch_add(1, Ordering::AcqRel);
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

type StopRequest = Arc<dyn Fn() + Send + Sync>;

/// The operations one connector source started, so that closing the source
/// stops them and can observe their exit.
///
/// A provider admits an operation before it submits any work for it and ends
/// the returned ticket once the work has actually exited. Sealing the source
/// refuses every later admission and asks every admitted operation to stop,
/// so an operation either is admitted before the seal, and is then stopped
/// by it, or is refused. The source has exited once it is sealed and every
/// admitted operation ended; the first real error is kept.
#[derive(Clone, Default)]
pub struct ConnectorSourceOperations {
    inner: Arc<Mutex<SourceOperationsState>>,
}

#[derive(Default)]
struct SourceOperationsState {
    sealed: bool,
    next_id: u64,
    live: BTreeMap<u64, StopRequest>,
    first_error: Option<ConnectorError>,
    waiters: Vec<Waker>,
}

impl ConnectorSourceOperations {
    pub fn new() -> Self {
        Self::default()
    }

    /// Admits one operation together with the request that stops it.
    /// Refused once the source is sealed.
    pub fn admit(&self, stop: StopRequest) -> Result<ConnectorOperationTicket, ConnectorError> {
        let mut state = self.lock();
        if state.sealed {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector source is closed and admits no new operation",
            ));
        }
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.live.insert(id, stop);
        Ok(ConnectorOperationTicket {
            operations: self.clone(),
            id,
            ended: false,
        })
    }

    /// Seals the source: refuses every later admission and asks every
    /// admitted operation to stop. Idempotent.
    pub fn seal(&self) {
        let (stops, waiters) = {
            let mut state = self.lock();
            if state.sealed {
                return;
            }
            state.sealed = true;
            let stops = state.live.values().cloned().collect::<Vec<_>>();
            let waiters = if state.live.is_empty() {
                std::mem::take(&mut state.waiters)
            } else {
                Vec::new()
            };
            (stops, waiters)
        };
        for stop in stops {
            stop();
        }
        for waiter in waiters {
            waiter.wake();
        }
    }

    pub fn is_sealed(&self) -> bool {
        self.lock().sealed
    }

    /// Admitted operations that have not ended yet.
    pub fn live_operations(&self) -> usize {
        self.lock().live.len()
    }

    /// Whether the source is sealed and every admitted operation ended.
    pub fn is_exited(&self) -> bool {
        let state = self.lock();
        state.sealed && state.live.is_empty()
    }

    /// Resolves once the source is sealed and every admitted operation has
    /// ended, with the first real error. Any number of observers may wait.
    pub fn exited(&self) -> ConnectorSourceExit {
        ConnectorSourceExit {
            operations: self.clone(),
        }
    }

    fn end(&self, id: u64, result: Result<(), ConnectorError>) {
        let waiters = {
            let mut state = self.lock();
            if state.live.remove(&id).is_none() {
                return;
            }
            if let Err(error) = result
                && state.first_error.is_none()
            {
                state.first_error = Some(error);
            }
            if state.sealed && state.live.is_empty() {
                std::mem::take(&mut state.waiters)
            } else {
                Vec::new()
            }
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SourceOperationsState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl fmt::Debug for ConnectorSourceOperations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        formatter
            .debug_struct("ConnectorSourceOperations")
            .field("sealed", &state.sealed)
            .field("live", &state.live.len())
            .field("first_error", &state.first_error)
            .finish()
    }
}

/// One admitted operation of a connector source.
///
/// End it once the operation has actually exited. A ticket dropped without
/// being ended counts as an operation that exited without an error, which
/// is what an admitted operation that never started is.
#[must_use = "an admitted operation must end its ticket when it exits"]
pub struct ConnectorOperationTicket {
    operations: ConnectorSourceOperations,
    id: u64,
    ended: bool,
}

impl ConnectorOperationTicket {
    /// Ends the operation with its real outcome.
    pub fn end(mut self, result: Result<(), ConnectorError>) {
        self.ended = true;
        self.operations.end(self.id, result);
    }
}

impl Drop for ConnectorOperationTicket {
    fn drop(&mut self) {
        if !self.ended {
            self.operations.end(self.id, Ok(()));
        }
    }
}

/// Future returned by [`ConnectorSourceOperations::exited`].
pub struct ConnectorSourceExit {
    operations: ConnectorSourceOperations,
}

impl Future for ConnectorSourceExit {
    type Output = Result<(), ConnectorError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.operations.lock();
        if state.sealed && state.live.is_empty() {
            return Poll::Ready(state.first_error.clone().map_or(Ok(()), Err));
        }
        if !state
            .waiters
            .iter()
            .any(|waiter| waiter.will_wake(cx.waker()))
        {
            state.waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use super::*;

    #[derive(Default)]
    struct CountingWaker(AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn poll_once<F: Future + Unpin>(future: &mut F, wakes: &Arc<CountingWaker>) -> Poll<F::Output> {
        let waker = Waker::from(Arc::clone(wakes));
        let mut context = Context::from_waker(&waker);
        Pin::new(future).poll(&mut context)
    }

    #[test]
    fn budget_yields_once_when_the_turn_runs_out_and_resumes_next_turn() {
        let budget = ConnectorPollBudget::new();
        budget.refill(3);
        let wakes = Arc::new(CountingWaker::default());

        let mut first = budget.consume(2);
        assert!(poll_once(&mut first, &wakes).is_ready());
        assert_eq!(budget.remaining(), 1);

        let mut second = budget.consume(2);
        assert!(
            poll_once(&mut second, &wakes).is_pending(),
            "a turn that ran out ends"
        );
        assert_eq!(
            wakes.0.load(Ordering::Acquire),
            1,
            "the yield wakes its own task"
        );
        assert_eq!(budget.remaining(), 0);
        assert_eq!(budget.exhaustions(), 1);
        assert!(
            poll_once(&mut budget.consume(1), &wakes).is_pending(),
            "nothing else runs in the exhausted turn"
        );

        budget.refill(10);
        assert!(
            poll_once(&mut second, &wakes).is_ready(),
            "the next turn resumes the yielded work"
        );
        assert_eq!(budget.remaining(), 8, "and pays for it");
    }

    #[test]
    fn a_sealed_source_stops_its_operations_refuses_new_ones_and_exits_when_they_end() {
        let operations = ConnectorSourceOperations::new();
        let stopped = Arc::new(AtomicUsize::new(0));
        let stop = {
            let stopped = Arc::clone(&stopped);
            Arc::new(move || {
                stopped.fetch_add(1, Ordering::AcqRel);
            }) as StopRequest
        };
        let first = operations.admit(Arc::clone(&stop)).expect("admitted");
        let second = operations.admit(stop).expect("admitted");
        let wakes = Arc::new(CountingWaker::default());
        let mut exit = operations.exited();
        assert!(
            poll_once(&mut exit, &wakes).is_pending(),
            "an open source has not exited"
        );

        operations.seal();
        operations.seal();
        assert_eq!(
            stopped.load(Ordering::Acquire),
            2,
            "sealing stops each operation once"
        );
        assert!(operations.admit(Arc::new(|| {})).is_err());
        assert!(poll_once(&mut exit, &wakes).is_pending());

        first.end(Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "object store went away",
        )));
        assert!(poll_once(&mut exit, &wakes).is_pending());
        drop(second);
        assert_eq!(
            wakes.0.load(Ordering::Acquire),
            1,
            "the last exit wakes the observer"
        );
        let Poll::Ready(result) = poll_once(&mut exit, &wakes) else {
            panic!("every operation ended");
        };
        assert_eq!(
            result.expect_err("first error").kind(),
            ConnectorErrorKind::Unavailable
        );
        let Poll::Ready(again) = poll_once(&mut operations.exited(), &wakes) else {
            panic!("exit is observable again");
        };
        assert!(again.is_err());
    }

    #[test]
    fn admission_racing_the_seal_is_either_stopped_or_refused() {
        for _ in 0..200 {
            let operations = ConnectorSourceOperations::new();
            let stopped = Arc::new(AtomicUsize::new(0));
            let admitted = std::thread::scope(|scope| {
                let admitting = scope.spawn(|| {
                    let stopped = Arc::clone(&stopped);
                    operations.admit(Arc::new(move || {
                        stopped.fetch_add(1, Ordering::AcqRel);
                    }) as StopRequest)
                });
                let sealing = scope.spawn(|| operations.seal());
                sealing.join().expect("sealing thread");
                admitting.join().expect("admitting thread")
            });
            assert_eq!(
                stopped.load(Ordering::Acquire),
                usize::from(admitted.is_ok()),
                "an admitted operation is stopped by the seal; a refused one never started"
            );
            drop(admitted);
            assert!(operations.is_exited());
        }
    }
}
