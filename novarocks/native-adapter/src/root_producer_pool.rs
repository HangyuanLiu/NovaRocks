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
//! A process-owned finite CPU pool. Jobs own no pool Arc; registrations hold
//! only Weak<Core>. One activation pins a job until its real Complete turn and
//! exit hook. The process credit covers requested stacks and the fixed Rust
//! construction shapes below, not libc/TLS/guard pages or whole-process RSS.
use std::alloc::Layout;
use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::ffi::CString;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle, Thread, ThreadId};

use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;

const MAX_THREADS: usize = 64;
const MAX_POSITIONS: usize = 4096;
const MIN_STACK: usize = 1024 * 1024;
const MAX_STACK: usize = 8 * MIN_STACK;
thread_local! {
    static CURRENT_POOL: Cell<usize> = const { Cell::new(0) };
}

pub(crate) trait RootProducerJob: Send + Sync {
    fn turn(&self) -> RootProducerTurn;
    /// Must be nonblocking and idempotent. A cancelled job must eventually
    /// return Complete from turn; neither cancel nor Drop proves physical exit.
    fn cancel(&self);
    /// Called outside the pool lock after Complete returned, before unpinning.
    fn exited(&self);
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootProducerTurn {
    Yielded,
    Blocked,
    Idle,
    Complete,
}
#[derive(Clone, Copy)]
struct Ticket {
    index: usize,
    generation: u64,
}
#[derive(Default)]
struct Slot {
    generation: u64,
    registered: Option<Weak<dyn RootProducerJob>>,
    active: Option<Arc<dyn RootProducerJob>>,
    queued: bool,
    running: bool,
    woken: bool,
}
struct State {
    slots: Vec<Slot>,
    ready: VecDeque<Ticket>,
    stopping: bool,
}
struct Core {
    state: Mutex<State>,
    changed: Condvar,
}
struct Resources {
    handles: Vec<JoinHandle<()>>,
    // Declaration order releases the handle Vec before its construction credit.
    _credit: ResultWriteCredit,
    _budget: Arc<ResultRetainedBudget>,
}
pub struct RootProducerPool {
    core: Arc<Core>,
    resources: Mutex<Option<Resources>>,
    reserved_bytes: usize,
}
/// This value is inline and finite. Every registration caller must pregrant
/// weak_core_backing_bytes() beside its root metadata and keep that guard until
/// this registration and its wake callbacks actually drop. Shutdown/Pool Drop
/// release the process credit but do not free a Weak-held Core allocation.
pub(crate) struct RootProducerRegistration {
    core: Weak<Core>,
    ticket: Ticket,
}

/// Callback token without registration ownership. Clones keep only a Weak Core
/// control tail. Each callback owner covers this inline value and retains its
/// root's weak_core_backing_bytes() grant through the callback's actual Drop.
#[derive(Clone)]
pub(crate) struct RootProducerWake {
    core: Weak<Core>,
    ticket: Ticket,
}

fn add(total: &mut usize, bytes: usize) -> Result<(), String> {
    *total = total
        .checked_add(bytes)
        .ok_or("root producer pool layout overflow")?;
    Ok(())
}
fn array<T>(count: usize) -> Result<usize, String> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| "root producer pool array layout overflow".into())
}
fn arc_layout<T>() -> Result<usize, String> {
    Layout::new::<[usize; 2]>()
        .extend(Layout::new::<T>())
        .map(|(layout, _)| layout.pad_to_align().size())
        .map_err(|_| "root producer pool Arc layout overflow".into())
}
/// Exact requested Arc<Core> allocation under pinned Rust 1.92: repr(C)
/// ArcInner's two AtomicUsize counters followed by the aligned Core value.
/// Weak handles retain this entire allocation after Core's fields have dropped;
/// they do not retain the slots/queue heap backings, jobs or worker stacks.
/// Every registration/callback owner independently pregrants this upper tail
/// before registration and retains its guard until its last actual token Drop.
/// It is allocation provenance only, not a second budget or whole-source proof.
pub(crate) fn weak_core_backing_bytes() -> usize {
    arc_layout::<Core>().expect("fixed root producer Core Arc layout fits usize")
}

fn construction_bytes(threads: usize, positions: usize, stack: usize) -> Result<usize, String> {
    // Source-audited Rust 1.92 std::thread::spawn_unchecked_ + Unix Thread::new:
    // ThreadInner Arc (unnamed CString option, id, Darwin's pointer+i8 Parker
    // upper bound also covers Linux's u32 Parker), Packet<()> Arc, main Box
    // (Core Arc, Thread, Packet Arc, empty ChildSpawnHooks), Unix ThreadData Box.
    // Stable 1.92 cannot install unstable spawn hooks. No thread name is set.
    // These are Layout upper bounds for requested Rust allocations. Darwin's
    // dispatch semaphore and libc TLS/stack cache are external platform facts.
    // Unix may raise stack to TLS minimum/round pages: only requested stack is
    // proved here. Host acceptance must separately verify its platform envelope.
    let mut total = arc_layout::<RootProducerPool>()?;
    // Darwin std 1.92 lazily Box-pins pthread primitives: two mutexes
    // (signature + 56 bytes each) and one condvar (signature + 40 bytes).
    // Initialize them serially before workers/publication to exclude duplicate
    // OnceBox candidates. Linux uses inline futex primitives instead.
    #[cfg(target_vendor = "apple")]
    {
        add(&mut total, array::<(isize, [u8; 56])>(2)?)?;
        add(&mut total, Layout::new::<(isize, [u8; 40])>().size())?;
    }
    add(&mut total, arc_layout::<Core>()?)?;
    add(&mut total, array::<Slot>(positions)?)?;
    add(&mut total, array::<Ticket>(positions)?)?;
    add(&mut total, array::<JoinHandle<()>>(threads)?)?;
    add(&mut total, array::<RootProducerRegistration>(positions)?)?;
    add(
        &mut total,
        Layout::new::<Weak<ResultRetainedBudget>>().size(),
    )?;
    let mut per_thread = stack;
    add(
        &mut per_thread,
        arc_layout::<(Option<CString>, ThreadId, (usize, i8))>()?,
    )?;
    add(
        &mut per_thread,
        arc_layout::<(Option<Arc<()>>, UnsafeCell<Option<thread::Result<()>>>)>()?,
    )?;
    add(
        &mut per_thread,
        Layout::new::<(
            Arc<Core>,
            Thread,
            Arc<()>,
            Option<Arc<()>>,
            Vec<Box<dyn FnOnce() + Send>>,
        )>()
        .size(),
    )?;
    add(
        &mut per_thread,
        Layout::new::<(Option<Box<str>>, Box<dyn FnOnce()>)>().size(),
    )?;
    // Each worker can concurrently hold one popped ticket and one Arc clone.
    add(
        &mut per_thread,
        Layout::new::<(Ticket, Arc<dyn RootProducerJob>)>().size(),
    )?;
    add(
        &mut total,
        per_thread
            .checked_mul(threads)
            .ok_or("root producer thread layout overflow")?,
    )?;
    Ok(total)
}
/// Validated local CPU resources, independent of task transport capacity.
/// This does not advertise result transport or product support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootProducerLimits {
    threads: NonZeroUsize,
    positions: NonZeroUsize,
    stack_bytes: usize,
}
impl RootProducerLimits {
    pub fn try_new(threads: usize, positions: usize, stack_bytes: usize) -> Result<Self, String> {
        let threads = NonZeroUsize::new(threads).ok_or("root producer threads must be positive")?;
        let positions =
            NonZeroUsize::new(positions).ok_or("root producer positions must be positive")?;
        if threads.get() > MAX_THREADS
            || positions.get() > MAX_POSITIONS
            || !(MIN_STACK..=MAX_STACK).contains(&stack_bytes)
        {
            return Err(
                "root producer pool requires threads<=64, positions<=4096 and stack in 1..=8 MiB"
                    .into(),
            );
        }
        Ok(Self {
            threads,
            positions,
            stack_bytes,
        })
    }
    pub fn threads(self) -> NonZeroUsize {
        self.threads
    }
    pub fn positions(self) -> NonZeroUsize {
        self.positions
    }
    pub fn stack_bytes(self) -> usize {
        self.stack_bytes
    }
}
impl RootProducerPool {
    pub fn try_new(
        threads: NonZeroUsize,
        positions: NonZeroUsize,
        stack_bytes: usize,
        budget: Arc<ResultRetainedBudget>,
    ) -> Result<Arc<Self>, String> {
        let limits = RootProducerLimits::try_new(threads.get(), positions.get(), stack_bytes)?;
        let threads = limits.threads().get();
        let positions = limits.positions().get();
        if !cfg!(all(
            target_pointer_width = "64",
            any(target_vendor = "apple", target_os = "linux")
        )) {
            return Err("root producer pool layout audit requires 64-bit Darwin or Linux".into());
        }
        // Supply a 64 KiB-aligned stack request, covering the audited Darwin
        // 16 KiB and Linux <=64 KiB page rounding without post-grant growth.
        let stack_bytes = stack_bytes
            .checked_add(65535)
            .ok_or("root producer stack layout overflow")?
            & !65535;
        let reserved_bytes = construction_bytes(threads, positions, stack_bytes)?;
        let credit = match budget.try_reserve_process(reserved_bytes)? {
            ResultWriteAdmission::Granted(credit) => credit,
            ResultWriteAdmission::Blocked => {
                return Err("root producer pool process capacity is exhausted".into());
            }
        };
        // try_reserve_exact on these Rust 1.92 RawVec owners allocates the
        // requested Layout; there is no growth after accepting registrations.
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(positions)
            .map_err(|_| "root producer slot allocation failed")?;
        slots.resize_with(positions, Slot::default);
        let mut ready = VecDeque::new();
        ready
            .try_reserve_exact(positions)
            .map_err(|_| "root producer queue allocation failed")?;
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(threads)
            .map_err(|_| "root producer handle allocation failed")?;
        let core = Arc::new(Core {
            state: Mutex::new(State {
                slots,
                ready,
                stopping: false,
            }),
            changed: Condvar::new(),
        });
        drop(core.state.lock().expect("root producer initial state lock"));
        core.changed.notify_all();
        for _ in 0..threads {
            let worker = Arc::clone(&core);
            match thread::Builder::new()
                .stack_size(stack_bytes)
                .spawn(move || run(worker))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    stop(&core);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    // No registrations existed. All fixed owners die before
                    // the credit; failed spawn's internal allocations unwind.
                    drop(core);
                    drop(credit);
                    return Err(format!("root producer thread spawn failed: {error}"));
                }
            }
        }
        let pool = Arc::new(Self {
            core,
            resources: Mutex::new(Some(Resources {
                handles,
                _credit: credit,
                _budget: budget,
            })),
            reserved_bytes,
        });
        drop(
            pool.resources
                .lock()
                .expect("root producer initial resources lock"),
        );
        Ok(pool)
    }
    /// Caller must pregrant weak_core_backing_bytes() in its root metadata
    /// owner before registering and retain it through all wake-token exits.
    pub(crate) fn register(
        &self,
        job: Weak<dyn RootProducerJob>,
    ) -> Result<RootProducerRegistration, String> {
        let mut state = self.core.state.lock().expect("root producer pool lock");
        if state.stopping {
            return Err("root producer pool is shut down".into());
        }
        let (index, slot) = state
            .slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| {
                slot.registered.is_none()
                    && slot.active.is_none()
                    && !slot.running
                    && slot.generation != u64::MAX
            })
            .ok_or("root producer pool positions are exhausted")?;
        slot.generation = slot
            .generation
            .checked_add(1)
            .ok_or("root producer generation overflow")?;
        slot.registered = Some(job);
        Ok(RootProducerRegistration {
            core: Arc::downgrade(&self.core),
            ticket: Ticket {
                index,
                generation: slot.generation,
            },
        })
    }
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }
    /// Must run on the external process owner, never a producer worker. The
    /// fixed backing/credit stays owned until this Pool is actually dropped.
    pub fn shutdown(&self) -> Result<(), String> {
        if on_worker(&self.core) {
            return Err("root producer worker cannot join its own pool".into());
        }
        stop(&self.core);
        let mut resources = self.resources.lock().expect("root producer resources lock");
        let mut failed = false;
        if let Some(resources) = resources.as_mut() {
            // Only this owner mutex is held; workers never acquire it.
            for handle in resources.handles.drain(..) {
                failed |= handle.join().is_err();
            }
        }
        if failed {
            Err("root producer worker panicked during shutdown".into())
        } else {
            Ok(())
        }
    }
}
fn on_worker(core: &Arc<Core>) -> bool {
    CURRENT_POOL.with(|current| current.get() == Arc::as_ptr(core) as usize)
}
impl Drop for RootProducerPool {
    fn drop(&mut self) {
        if on_worker(&self.core) {
            stop(&self.core);
            // Misuse: a job retained the process owner and dropped its last Arc
            // on a worker. Self-join cannot prove stack exit. Fail closed: retain
            // this fixed owner's credit/backing instead of claiming reclaimed
            // bytes or creating an unbounded reaper thread. Composition must
            // call shutdown and drop the owner externally.
            if let Some(resources) = self
                .resources
                .get_mut()
                .expect("root producer resources lock")
                .take()
            {
                std::mem::forget(resources);
            }
            std::mem::forget(Arc::clone(&self.core));
        } else {
            let _ = self.shutdown();
        }
    }
}
impl RootProducerRegistration {
    pub(crate) fn wake_handle(&self) -> RootProducerWake {
        RootProducerWake {
            core: self.core.clone(),
            ticket: self.ticket,
        }
    }
    pub(crate) fn wake(&self) -> Result<(), String> {
        self.wake_handle().wake()
    }
}
impl RootProducerWake {
    /// Explicit input/terminal activation. Dormant jobs acquire their first pin.
    pub(crate) fn wake(&self) -> Result<(), String> {
        self.wake_impl(true)
    }
    /// Readiness callback only: dormant jobs remain Weak and unqueued. A shared
    /// budget event must not turn an unopened Session into a live producer.
    pub(crate) fn wake_active(&self) -> Result<(), String> {
        self.wake_impl(false)
    }
    fn wake_impl(&self, activate: bool) -> Result<(), String> {
        let core = self.core.upgrade().ok_or("root producer pool has exited")?;
        let mut state = core.state.lock().expect("root producer pool lock");
        if state.stopping {
            return Err("root producer pool is shut down".into());
        }
        let ticket = self.ticket;
        let slot = state
            .slots
            .get_mut(ticket.index)
            .filter(|slot| slot.generation == ticket.generation && slot.registered.is_some())
            .ok_or("root producer registration has expired")?;
        if slot.active.is_none() {
            if !activate {
                return Ok(());
            }
            slot.active = Some(
                slot.registered
                    .as_ref()
                    .and_then(Weak::upgrade)
                    .ok_or("root producer job has exited")?,
            );
        }
        if slot.running {
            slot.woken = true;
        } else if !slot.queued {
            slot.queued = true;
            state.ready.push_back(ticket);
        }
        drop(state);
        core.changed.notify_one();
        Ok(())
    }
}
impl Drop for RootProducerRegistration {
    fn drop(&mut self) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        let (cancel, removed) = {
            let mut state = core.state.lock().expect("root producer pool lock");
            let Some(slot) = state
                .slots
                .get_mut(self.ticket.index)
                .filter(|slot| slot.generation == self.ticket.generation)
            else {
                return;
            };
            let removed = slot.registered.take();
            let cancel = slot.active.clone();
            if cancel.is_some() {
                if slot.running {
                    slot.woken = true;
                } else if !slot.queued {
                    slot.queued = true;
                    state.ready.push_back(self.ticket);
                }
            }
            (cancel, removed)
        };
        if let Some(job) = cancel {
            cancel_job(&job);
        }
        drop(removed);
        core.changed.notify_all();
    }
}
fn cancel_job(job: &Arc<dyn RootProducerJob>) {
    let _ = catch_unwind(AssertUnwindSafe(|| job.cancel()));
}
fn stop(core: &Arc<Core>) {
    {
        let mut state = core.state.lock().expect("root producer pool lock");
        state.stopping = true;
        // Activate all live dormant jobs in the same locked transition. No
        // worker may conclude shutdown is drained before this walk finishes.
        let State { slots, ready, .. } = &mut *state;
        for (index, slot) in slots.iter_mut().enumerate() {
            if slot.active.is_none() {
                slot.active = slot.registered.as_ref().and_then(Weak::upgrade);
            }
            if slot.active.is_some() {
                if slot.running {
                    slot.woken = true;
                } else if !slot.queued {
                    slot.queued = true;
                    ready.push_back(Ticket {
                        index,
                        generation: slot.generation,
                    });
                }
            }
        }
    }
    // Fixed slots make cancellation finite without allocating a snapshot Vec.
    // Invoking cancel under the queue mutex could deadlock a Session callback.
    let count = core
        .state
        .lock()
        .expect("root producer pool lock")
        .slots
        .len();
    for index in 0..count {
        let job = core.state.lock().expect("root producer pool lock").slots[index]
            .active
            .clone();
        if let Some(job) = job {
            cancel_job(&job);
        }
    }
    core.changed.notify_all();
}

fn run(core: Arc<Core>) {
    CURRENT_POOL.with(|current| current.set(Arc::as_ptr(&core) as usize));
    loop {
        let (ticket, job) = {
            let mut state = core.state.lock().expect("root producer pool lock");
            loop {
                if let Some(ticket) = state.ready.pop_front() {
                    let slot = &mut state.slots[ticket.index];
                    if slot.generation != ticket.generation || !slot.queued {
                        continue;
                    }
                    slot.queued = false;
                    slot.running = true;
                    slot.woken = false;
                    let job = slot
                        .active
                        .as_ref()
                        .expect("queued root producer is pinned")
                        .clone();
                    break (ticket, job);
                }
                if state.stopping && state.slots.iter().all(|slot| slot.active.is_none()) {
                    CURRENT_POOL.with(|current| current.set(0));
                    return;
                }
                state = core.changed.wait(state).expect("root producer wait lock");
            }
        };
        let turn = match catch_unwind(AssertUnwindSafe(|| job.turn())) {
            Ok(turn) => turn,
            Err(_) => {
                cancel_job(&job);
                RootProducerTurn::Yielded
            }
        };
        if turn == RootProducerTurn::Complete {
            // Even a failing hook must not strand all other finite positions.
            let _ = catch_unwind(AssertUnwindSafe(|| job.exited()));
        }
        let removed = {
            let mut state = core.state.lock().expect("root producer pool lock");
            let stopping = state.stopping;
            let slot = &mut state.slots[ticket.index];
            slot.running = false;
            if turn == RootProducerTurn::Complete {
                slot.woken = false;
                (slot.active.take(), slot.registered.take())
            } else {
                if turn == RootProducerTurn::Yielded
                    || slot.woken
                    || stopping
                    || slot.registered.is_none()
                {
                    slot.queued = true;
                    state.ready.push_back(ticket);
                }
                (None, None)
            }
        };
        // Job/Weak destructors can lock a Session or return a funding guard.
        // Neither destructor is allowed to execute under the pool queue mutex.
        drop(removed);
        drop(job);
        core.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    const WAIT: Duration = Duration::from_secs(5);
    struct FakeJob {
        turn: Box<dyn Fn() -> RootProducerTurn + Send + Sync>,
        cancel: Box<dyn Fn() + Send + Sync>,
        exit: Box<dyn Fn() + Send + Sync>,
    }
    impl RootProducerJob for FakeJob {
        fn turn(&self) -> RootProducerTurn {
            (self.turn)()
        }
        fn cancel(&self) {
            (self.cancel)()
        }
        fn exited(&self) {
            (self.exit)()
        }
    }
    struct ReleaseGate(Arc<Gate>);
    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    #[derive(Default)]
    struct Gate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }
    impl Gate {
        fn enter(&self) {
            let mut state = self.state.lock().unwrap();
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self.changed.wait(state).unwrap();
            }
        }
        fn wait_entered(&self) {
            let state = self.state.lock().unwrap();
            let (state, _) = self
                .changed
                .wait_timeout_while(state, WAIT, |state| !state.0)
                .unwrap();
            assert!(state.0, "worker must reach its controlled turn");
        }
        fn release(&self) {
            self.state.lock().unwrap().1 = true;
            self.changed.notify_all();
        }
    }
    fn pool(positions: usize) -> (Arc<RootProducerPool>, Arc<ResultRetainedBudget>) {
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(8 * MIN_STACK).unwrap());
        let pool = RootProducerPool::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(positions).unwrap(),
            MIN_STACK,
            Arc::clone(&budget),
        )
        .unwrap();
        (pool, budget)
    }
    fn register(
        pool: &RootProducerPool,
        job: &Arc<dyn RootProducerJob>,
    ) -> RootProducerRegistration {
        pool.register(Arc::downgrade(job)).unwrap()
    }
    fn wait_drained(pool: &RootProducerPool) {
        let state = pool.core.state.lock().unwrap();
        let (state, _) = pool
            .core
            .changed
            .wait_timeout_while(state, WAIT, |state| {
                state.slots.iter().any(|slot| slot.active.is_some())
            })
            .unwrap();
        assert!(
            state.slots.iter().all(|slot| slot.active.is_none()),
            "all activated jobs must physically finish"
        );
    }

    #[test]
    fn dormant_weak_registration_capacity_and_generation_reject_stale_callbacks() {
        let (pool, _) = pool(1);
        let turns = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&turns);
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                RootProducerTurn::Complete
            }),
            cancel: Box::new(|| {}),
            exit: Box::new(|| {}),
        });
        let weak = Arc::downgrade(&job);
        let first = register(&pool, &job);
        let stale = first.wake_handle();
        assert!(pool.register(Arc::downgrade(&job)).is_err());
        drop(first);
        let second = register(&pool, &job);
        assert!(
            stale.wake().is_err(),
            "old generation must not activate a reused position"
        );
        assert!(
            stale.wake_active().is_err(),
            "inactive wake must check generation too"
        );
        assert_eq!(turns.load(Ordering::SeqCst), 0);
        drop(job);
        assert!(weak.upgrade().is_none(), "dormant slot may not pin its job");
        assert!(second.wake().is_err());
        drop(second);
        pool.shutdown().unwrap();
    }

    #[test]
    fn budget_wake_does_not_activate_or_pin_dormant_job_or_prevent_unregister() {
        let (pool, _) = pool(1);
        let turns = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&turns);
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                RootProducerTurn::Idle
            }),
            cancel: Box::new(|| {}),
            exit: Box::new(|| panic!("never-activated registration has no producer exit")),
        });
        let weak_job = Arc::downgrade(&job);
        let registration = register(&pool, &job);
        let callback = registration.wake_handle();
        for _ in 0..100 {
            callback.wake_active().unwrap();
        }
        {
            let state = pool.core.state.lock().unwrap();
            assert!(state.ready.is_empty());
            assert!(state.slots[0].active.is_none());
            assert!(!state.slots[0].running && !state.slots[0].woken);
        }
        assert_eq!(turns.load(Ordering::SeqCst), 0);
        drop(job);
        assert!(
            weak_job.upgrade().is_none(),
            "budget callback must not retain an unopened Session"
        );
        // Even a dead dormant job needs no Weak::upgrade for a budget event.
        callback.wake_active().unwrap();
        drop(registration);
        let replacement: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(|| RootProducerTurn::Complete),
            cancel: Box::new(|| {}),
            exit: Box::new(|| {}),
        });
        let replacement_registration = register(&pool, &replacement);
        assert!(
            callback.wake_active().is_err(),
            "stale callback must not touch a replacement generation"
        );
        assert!(pool.core.state.lock().unwrap().ready.is_empty());
        drop(replacement_registration);
        pool.shutdown().unwrap();
    }

    #[test]
    fn yield_round_robin_is_fair_and_repeated_queued_wakes_coalesce() {
        let (pool, _) = pool(3);
        let gate = Arc::new(Gate::default());
        let _release_on_exit = ReleaseGate(Arc::clone(&gate));
        let log = Arc::new(Mutex::new(Vec::new()));
        let (exited, receiver) = mpsc::channel();
        let mut jobs = Vec::new();
        let mut registrations = Vec::new();
        for id in 0..3 {
            let turns = AtomicUsize::new(0);
            let log = Arc::clone(&log);
            let gate = Arc::clone(&gate);
            let exited = exited.clone();
            let weak_core = Arc::downgrade(&pool.core);
            let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
                turn: Box::new(move || {
                    let ordinal = turns.fetch_add(1, Ordering::SeqCst);
                    log.lock().unwrap().push(id);
                    if id == 0 && ordinal == 0 {
                        gate.enter();
                    }
                    if ordinal == 0 {
                        RootProducerTurn::Yielded
                    } else {
                        RootProducerTurn::Complete
                    }
                }),
                cancel: Box::new(|| {}),
                exit: Box::new(move || {
                    let core = weak_core.upgrade().unwrap();
                    assert!(
                        core.state.try_lock().is_ok(),
                        "exit hook must run outside the pool lock"
                    );
                    exited.send(id).unwrap();
                }),
            });
            registrations.push(register(&pool, &job));
            jobs.push(job);
        }
        registrations[0].wake().unwrap();
        gate.wait_entered();
        registrations[1].wake().unwrap();
        registrations[2].wake().unwrap();
        for _ in 0..100 {
            registrations[1].wake().unwrap();
            registrations[2].wake().unwrap();
        }
        assert_eq!(pool.core.state.lock().unwrap().ready.len(), 2);
        gate.release();
        for _ in 0..3 {
            receiver.recv_timeout(WAIT).unwrap();
        }
        wait_drained(&pool);
        assert_eq!(log.lock().unwrap().as_slice(), &[0, 1, 2, 0, 1, 2]);
        assert!(pool.core.state.lock().unwrap().ready.is_empty());
        pool.shutdown().unwrap();
    }

    #[test]
    fn wake_during_running_blocked_turn_is_not_lost() {
        let (pool, _) = pool(1);
        let gate = Arc::new(Gate::default());
        let _release_on_exit = ReleaseGate(Arc::clone(&gate));
        let inside = Arc::clone(&gate);
        let turns = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&turns);
        let (exited, receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    inside.enter();
                    RootProducerTurn::Blocked
                } else {
                    RootProducerTurn::Complete
                }
            }),
            cancel: Box::new(|| {}),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let registration = register(&pool, &job);
        registration.wake().unwrap();
        gate.wait_entered();
        let callback = registration.wake_handle();
        for _ in 0..100 {
            callback.wake_active().unwrap();
        }
        {
            let state = pool.core.state.lock().unwrap();
            assert!(state.ready.is_empty());
            assert!(state.slots[0].running && state.slots[0].woken);
        }
        gate.release();
        receiver.recv_timeout(WAIT).unwrap();
        wait_drained(&pool);
        assert_eq!(turns.load(Ordering::SeqCst), 2);
        pool.shutdown().unwrap();
    }

    #[test]
    fn active_budget_wake_resumes_a_parked_blocked_job() {
        let (pool, _) = pool(1);
        let turns = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&turns);
        let (exited, receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    RootProducerTurn::Blocked
                } else {
                    RootProducerTurn::Complete
                }
            }),
            cancel: Box::new(|| {}),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let registration = register(&pool, &job);
        registration.wake().unwrap();
        {
            let state = pool.core.state.lock().unwrap();
            let (state, _) = pool
                .core
                .changed
                .wait_timeout_while(state, WAIT, |state| {
                    let slot = &state.slots[0];
                    slot.running || slot.queued
                })
                .unwrap();
            assert!(state.slots[0].active.is_some());
            assert!(!state.slots[0].running && !state.slots[0].queued);
        }
        registration.wake_handle().wake_active().unwrap();
        receiver.recv_timeout(WAIT).unwrap();
        wait_drained(&pool);
        assert_eq!(turns.load(Ordering::SeqCst), 2);
        pool.shutdown().unwrap();
    }

    #[test]
    fn dropping_active_registration_cancels_but_keeps_pin_and_position_until_exit() {
        let (pool, _) = pool(1);
        let gate = Arc::new(Gate::default());
        let _release_on_exit = ReleaseGate(Arc::clone(&gate));
        let inside = Arc::clone(&gate);
        let cancelled = Arc::new(AtomicBool::new(false));
        let turn_cancelled = Arc::clone(&cancelled);
        let cancel_flag = Arc::clone(&cancelled);
        let (exited, receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if turn_cancelled.load(Ordering::SeqCst) {
                    RootProducerTurn::Complete
                } else {
                    inside.enter();
                    RootProducerTurn::Blocked
                }
            }),
            cancel: Box::new(move || {
                cancel_flag.store(true, Ordering::SeqCst);
            }),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let weak_job = Arc::downgrade(&job);
        let registration = register(&pool, &job);
        registration.wake().unwrap();
        gate.wait_entered();
        drop(registration);
        assert!(cancelled.load(Ordering::SeqCst));
        assert!(
            pool.register(Arc::downgrade(&job)).is_err(),
            "an executing position is not free"
        );
        drop(job);
        assert!(
            weak_job.upgrade().is_some(),
            "registration Drop is not producer exit"
        );
        assert!(receiver.try_recv().is_err());
        gate.release();
        receiver.recv_timeout(WAIT).unwrap();
        wait_drained(&pool);
        pool.shutdown().unwrap();
        assert!(weak_job.upgrade().is_none());
    }

    #[test]
    fn shutdown_cancels_live_turn_then_joins_before_owner_credit_release() {
        let (pool, budget) = pool(2);
        let gate = Arc::new(Gate::default());
        let _release_on_exit = ReleaseGate(Arc::clone(&gate));
        let inside = Arc::clone(&gate);
        let cancelled = Arc::new(AtomicBool::new(false));
        let turn_flag = Arc::clone(&cancelled);
        let cancel_flag = Arc::clone(&cancelled);
        let (cancelled_sender, cancelled_receiver) = mpsc::channel();
        let (exited, exit_receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if turn_flag.load(Ordering::SeqCst) {
                    RootProducerTurn::Complete
                } else {
                    inside.enter();
                    RootProducerTurn::Blocked
                }
            }),
            cancel: Box::new(move || {
                cancel_flag.store(true, Ordering::SeqCst);
                let _ = cancelled_sender.send(());
            }),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let registration = register(&pool, &job);
        registration.wake().unwrap();
        gate.wait_entered();
        let (done, done_receiver) = mpsc::channel();
        let owner = Arc::clone(&pool);
        let shutdown = thread::spawn(move || {
            owner.shutdown().unwrap();
            done.send(()).unwrap();
        });
        cancelled_receiver.recv_timeout(WAIT).unwrap();
        assert!(
            done_receiver.try_recv().is_err(),
            "join must await the still-running turn"
        );
        assert!(matches!(
            budget.try_reserve_process(8 * MIN_STACK).unwrap(),
            ResultWriteAdmission::Blocked
        ));
        gate.release();
        exit_receiver.recv_timeout(WAIT).unwrap();
        done_receiver.recv_timeout(WAIT).unwrap();
        shutdown.join().unwrap();
        assert!(registration.wake().is_err());
        assert!(pool.register(Arc::downgrade(&job)).is_err());
        assert!(
            matches!(
                budget.try_reserve_process(8 * MIN_STACK).unwrap(),
                ResultWriteAdmission::Blocked
            ),
            "shutdown leaves actual fixed pool backings alive"
        );
        let weak_core = registration.core.clone();
        drop(pool);
        assert!(weak_core.upgrade().is_none());
        assert!(matches!(
            budget.try_reserve_process(8 * MIN_STACK).unwrap(),
            ResultWriteAdmission::Granted(_)
        ));
    }

    #[test]
    fn panic_requests_cancel_then_cleanup_turn_and_actual_exit_hook() {
        let (pool, _) = pool(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let turn_flag = Arc::clone(&cancelled);
        let cancel_flag = Arc::clone(&cancelled);
        let (exited, receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if turn_flag.load(Ordering::SeqCst) {
                    RootProducerTurn::Complete
                } else {
                    panic!("injected producer turn failure");
                }
            }),
            cancel: Box::new(move || {
                cancel_flag.store(true, Ordering::SeqCst);
            }),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let registration = register(&pool, &job);
        registration.wake().unwrap();
        receiver.recv_timeout(WAIT).unwrap();
        wait_drained(&pool);
        assert!(cancelled.load(Ordering::SeqCst));
        pool.shutdown().unwrap();
    }

    #[test]
    fn local_configuration_and_process_capacity_reject_before_pool_allocations() {
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(1).unwrap());
        for (threads, positions, stack) in [
            (65, 1, MIN_STACK),
            (1, 4097, MIN_STACK),
            (1, 1, MIN_STACK - 1),
            (1, 1, MAX_STACK + 1),
            (1, 1, MIN_STACK),
        ] {
            assert!(
                RootProducerPool::try_new(
                    NonZeroUsize::new(threads).unwrap(),
                    NonZeroUsize::new(positions).unwrap(),
                    stack,
                    Arc::clone(&budget)
                )
                .is_err()
            );
        }
        assert!(matches!(
            budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Granted(_)
        ));
        let (pool, _) = pool(3);
        assert!(pool.reserved_bytes() > MIN_STACK);
        {
            let state = pool.core.state.lock().unwrap();
            assert_eq!(state.slots.capacity(), 3);
            assert_eq!(state.ready.capacity(), 3);
        }
        pool.shutdown().unwrap();
    }

    #[test]
    fn wake_keeps_exact_core_arc_tail_after_shutdown_and_pool_owner_drop() {
        let (pool, budget) = pool(1);
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(|| RootProducerTurn::Complete),
            cancel: Box::new(|| {}),
            exit: Box::new(|| {}),
        });
        let registration = register(&pool, &job);
        let wake = registration.wake_handle();
        let pointer = wake.core.as_ptr();
        let header = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>();
        let (inner, _) = header.extend(Layout::new::<Core>()).unwrap();
        assert_eq!(weak_core_backing_bytes(), inner.pad_to_align().size());
        assert!(weak_core_backing_bytes() >= std::mem::size_of::<Core>() + header.size());
        pool.shutdown().unwrap();
        drop(registration);
        drop(job);
        drop(pool);
        assert_eq!(wake.core.strong_count(), 0);
        assert!(wake.core.upgrade().is_none());
        assert_eq!(
            wake.core.as_ptr(),
            pointer,
            "Weak retains the Arc allocation without retaining Core's live value"
        );
        assert!(wake.wake_active().is_err());
        // The pool credit has returned, so the separately pregranted root tail
        // (not the process pool wallet) is what covers this remaining Weak.
        assert!(matches!(
            budget.try_reserve_process(8 * MIN_STACK).unwrap(),
            ResultWriteAdmission::Granted(_)
        ));
        let callback_alias = wake.clone();
        drop(wake);
        assert_eq!(callback_alias.core.as_ptr(), pointer);
        assert!(callback_alias.wake_active().is_err());
        drop(callback_alias);
    }

    #[test]
    fn shutdown_also_activates_dormant_jobs_for_cancel_cleanup() {
        let (pool, _) = pool(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let turn_flag = Arc::clone(&cancelled);
        let cancel_flag = Arc::clone(&cancelled);
        let (exited, receiver) = mpsc::channel();
        let job: Arc<dyn RootProducerJob> = Arc::new(FakeJob {
            turn: Box::new(move || {
                if turn_flag.load(Ordering::SeqCst) {
                    RootProducerTurn::Complete
                } else {
                    RootProducerTurn::Idle
                }
            }),
            cancel: Box::new(move || {
                cancel_flag.store(true, Ordering::SeqCst);
            }),
            exit: Box::new(move || exited.send(()).unwrap()),
        });
        let _registration = register(&pool, &job);
        let before = Instant::now();
        pool.shutdown().unwrap();
        assert!(before.elapsed() < WAIT);
        receiver.recv_timeout(WAIT).unwrap();
        assert!(cancelled.load(Ordering::SeqCst));
    }
}
