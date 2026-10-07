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

//! Process-level owner of the system local time zone offset.
//!
//! The ungoverned Variant to JSON conversion reads `chrono::Local::now()` at
//! the start of every conversion. On unix, chrono keeps a per-thread zone
//! cache and re-checks `TZ` and `/etc/localtime` at most once per second,
//! reloading the TZif rules when the source changed. Those allocations and
//! that file I/O would land on driver threads, outside the task allocator, so
//! governed conversion must not call chrono `Local` at all.
//!
//! [`LocalOffsetOwner`] keeps that responsibility outside every task. It
//! evaluates the rules through chrono `Local` itself on its own named
//! refresher thread and publishes an immutable [`LocalOffsetSnapshot`] that
//! answers, for every whole second of its validity window, exactly the offset
//! chrono `Local` gives, including a transition inside the window. Readers
//! copy the snapshot out of a `RwLock` and evaluate it without allocating.
//! An instant outside the window is a typed [`LocalOffsetExpired`] failure:
//! there is no fallback to chrono `Local`, UTC or a session time zone.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, FixedOffset, Local, TimeZone};
use novarocks_execution_contract::{SafeDetail, TaskFailure, TaskFailureCategory};

/// Interval between two refreshes of the published snapshot.
///
/// The contract is at most one second, the same class as chrono's per-thread
/// one-second source re-check; half a second leaves scheduling headroom. A
/// change of the zone source becomes visible within roughly this period plus
/// chrono's own one-second re-check on the refresher thread.
pub const LOCAL_OFFSET_REFRESH_PERIOD: Duration = Duration::from_millis(500);

/// Seconds after the refresh instant that a snapshot covers (the horizon H).
///
/// H is far below the spacing of any two real offset transitions of one zone
/// (tzdata transitions are days apart), so a window holds at most one real
/// transition. It also lets a stalled refresher miss many periods before
/// readers observe expiry.
pub const LOCAL_OFFSET_HORIZON_SECS: i64 = 60;

/// Seconds before the refresh instant that a snapshot still covers.
///
/// A reader that sampled the clock just before a publish, or a small backward
/// wall-clock step, still resolves inside the newest snapshot.
pub const LOCAL_OFFSET_LOOKBEHIND_SECS: i64 = 10;

const REFRESHER_THREAD_NAME: &str = "local_offset_refresher";

/// An evaluation that straddles chrono's own source reload fails confirmation;
/// chrono reloads at most once per second, so an immediate retry evaluates one
/// consistent zone state.
const FIRST_SNAPSHOT_ATTEMPTS: usize = 3;

/// Source of the system local time zone rules evaluated by the owner.
///
/// Production uses [`ChronoLocalRules`]; tests inject synthetic rules.
pub trait LocalRulesSource {
    /// Offset in effect at `utc_secs` whole seconds since the Unix epoch, or
    /// `None` when the instant cannot be evaluated.
    fn offset_at(&self, utc_secs: i64) -> Option<FixedOffset>;
}

/// The production rules source: chrono `Local` itself.
///
/// Evaluating through `Local.offset_from_utc_datetime` keeps every chrono
/// behavior of the ungoverned path, including the UTC fallback when no local
/// zone can be loaded and the per-thread source re-check.
#[derive(Clone, Copy, Debug, Default)]
pub struct ChronoLocalRules;

impl LocalRulesSource for ChronoLocalRules {
    fn offset_at(&self, utc_secs: i64) -> Option<FixedOffset> {
        let utc = DateTime::from_timestamp(utc_secs, 0)?;
        Some(Local.offset_from_utc_datetime(&utc.naive_utc()))
    }
}

/// Immutable view of the local offset over `[valid_from, valid_until)`.
///
/// The window holds at most one transition. Every whole second of the window
/// was evaluated against the rules source when the snapshot was built.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalOffsetSnapshot {
    valid_from_utc: i64,
    valid_until_utc: i64,
    offset_before: FixedOffset,
    transition_at_utc: Option<i64>,
    offset_after: FixedOffset,
}

impl LocalOffsetSnapshot {
    /// First covered second, inclusive.
    pub const fn valid_from_utc(&self) -> i64 {
        self.valid_from_utc
    }

    /// First second no longer covered.
    pub const fn valid_until_utc(&self) -> i64 {
        self.valid_until_utc
    }

    /// The one transition inside the window, if any.
    pub const fn transition_at_utc(&self) -> Option<i64> {
        self.transition_at_utc
    }

    /// Offset in effect at `utc_secs`; an instant outside the window is an
    /// [`LocalOffsetExpired`] failure. Allocation-free.
    pub fn offset_at(&self, utc_secs: i64) -> Result<FixedOffset, LocalOffsetExpired> {
        if !(self.valid_from_utc..self.valid_until_utc).contains(&utc_secs) {
            return Err(LocalOffsetExpired {
                at_utc: utc_secs,
                valid_from_utc: self.valid_from_utc,
                valid_until_utc: self.valid_until_utc,
            });
        }
        Ok(match self.transition_at_utc {
            Some(transition) if utc_secs >= transition => self.offset_after,
            _ => self.offset_before,
        })
    }
}

/// The requested instant lies outside the newest published snapshot.
///
/// This happens when the refresher stalled or stopped, or when the wall clock
/// moved outside the published window. Callers fail the conversion instead
/// of guessing an offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalOffsetExpired {
    at_utc: i64,
    valid_from_utc: i64,
    valid_until_utc: i64,
}

impl LocalOffsetExpired {
    pub const fn at_utc(&self) -> i64 {
        self.at_utc
    }

    pub const fn valid_from_utc(&self) -> i64 {
        self.valid_from_utc
    }

    pub const fn valid_until_utc(&self) -> i64 {
        self.valid_until_utc
    }

    /// The task's own failure for an expired snapshot.
    ///
    /// The snapshot is a process-local resource the backend maintains outside
    /// the task; when it is stale the task cannot obtain it. That is
    /// [`TaskFailureCategory::ResourceExhausted`], not a query semantic error
    /// (`Execution`) nor a malformed request or broken invariant of the task
    /// (`Protocol`/`Internal`). Its frontend classification as recoverable
    /// infrastructure matches the cause: a stalled refresher or a wall-clock
    /// step is a backend-local condition, and only an effect-free attempt
    /// whose output is not yet visible is ever replaced.
    pub fn into_task_failure(self) -> TaskFailure {
        TaskFailure::new(
            TaskFailureCategory::ResourceExhausted,
            SafeDetail::truncating(&self.to_string()),
        )
    }
}

impl fmt::Display for LocalOffsetExpired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "local time zone offset snapshot does not cover unix second {}: valid window is [{}, {})",
            self.at_utc, self.valid_from_utc, self.valid_until_utc
        )
    }
}

impl std::error::Error for LocalOffsetExpired {}

/// The owner could not be started.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalOffsetOwnerError {
    message: String,
}

impl fmt::Display for LocalOffsetOwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "local offset owner: {}", self.message)
    }
}

impl std::error::Error for LocalOffsetOwnerError {}

/// Process-level owner of the local offset snapshot.
///
/// The first snapshot is published synchronously by [`Self::start`], so a
/// snapshot exists before the process accepts any task. A named refresher
/// thread then replaces it every [`LOCAL_OFFSET_REFRESH_PERIOD`]. That thread
/// holds only a `Weak` reference and exits once the owner is dropped. A panic
/// of the refresher is not caught: the last snapshot then expires on its own
/// bound and readers receive [`LocalOffsetExpired`].
pub struct LocalOffsetOwner {
    current: RwLock<LocalOffsetSnapshot>,
    refresher: Arc<RefresherSignal>,
}

impl LocalOffsetOwner {
    /// Publishes the first snapshot and starts the refresher.
    ///
    /// The first evaluation runs on the calling thread, which is application
    /// composition, never a task.
    pub fn start<S>(source: S) -> Result<Arc<Self>, LocalOffsetOwnerError>
    where
        S: LocalRulesSource + Send + 'static,
    {
        let first = first_snapshot(&source)?;
        let owner = Arc::new(Self {
            current: RwLock::new(first),
            refresher: Arc::new(RefresherSignal::default()),
        });
        let weak = Arc::downgrade(&owner);
        let signal = Arc::clone(&owner.refresher);
        thread::Builder::new()
            .name(REFRESHER_THREAD_NAME.to_owned())
            .spawn(move || refresh_loop(weak, source, signal))
            .map_err(|error| LocalOffsetOwnerError {
                message: format!("spawn {REFRESHER_THREAD_NAME}: {error}"),
            })?;
        Ok(owner)
    }

    /// Copies the newest published snapshot. Allocation-free.
    pub fn snapshot(&self) -> LocalOffsetSnapshot {
        // A writer only assigns a `Copy` value, so a poisoned lock still
        // holds one whole published snapshot.
        *self.current.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Offset in effect at `utc_secs` according to the newest snapshot.
    /// Allocation-free.
    pub fn offset_at(&self, utc_secs: i64) -> Result<FixedOffset, LocalOffsetExpired> {
        self.snapshot().offset_at(utc_secs)
    }

    /// Offset in effect at the current wall-clock second, the instant the
    /// ungoverned path reads through `Local::now()`. Allocation-free.
    pub fn offset_now(&self) -> Result<FixedOffset, LocalOffsetExpired> {
        self.offset_at(unix_seconds_floor(SystemTime::now()))
    }

    fn publish(&self, snapshot: LocalOffsetSnapshot) {
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = snapshot;
    }

    #[cfg(test)]
    pub(crate) fn refresher_exit_probe(&self) -> LocalOffsetRefresherExitProbe {
        LocalOffsetRefresherExitProbe(Arc::clone(&self.refresher))
    }
}

impl Drop for LocalOffsetOwner {
    fn drop(&mut self) {
        // Wake the refresher so it observes the release promptly. Never join:
        // the last owner may be released on the refresher thread itself.
        self.refresher.stop();
    }
}

impl fmt::Debug for LocalOffsetOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalOffsetOwner")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

/// Whole seconds since the Unix epoch, rounded toward negative infinity.
///
/// Rules resolve offsets per whole second, so the floor of an instant selects
/// the same offset as the instant itself. Values beyond the `i64` range
/// saturate, which no snapshot window covers.
pub fn unix_seconds_floor(instant: SystemTime) -> i64 {
    match instant.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_secs()).unwrap_or(i64::MAX),
        Err(before) => {
            let before = before.duration();
            let whole = i64::try_from(before.as_secs()).unwrap_or(i64::MAX);
            if before.subsec_nanos() == 0 {
                -whole
            } else {
                -whole - 1
            }
        }
    }
}

#[derive(Default)]
struct RefresherSignal {
    stopped: Mutex<bool>,
    wake: Condvar,
    exited: AtomicBool,
}

impl RefresherSignal {
    fn stop(&self) {
        *self.stopped.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.wake.notify_all();
    }

    /// Waits one refresh period; returns `false` once the owner was released.
    fn wait_period(&self) -> bool {
        let stopped = self.stopped.lock().unwrap_or_else(PoisonError::into_inner);
        let (stopped, _) = self
            .wake
            .wait_timeout_while(stopped, LOCAL_OFFSET_REFRESH_PERIOD, |stopped| !*stopped)
            .unwrap_or_else(PoisonError::into_inner);
        !*stopped
    }
}

/// Marks the refresher as exited on every path out of its loop, unwinding
/// included.
struct RefresherExitMark(Arc<RefresherSignal>);

impl Drop for RefresherExitMark {
    fn drop(&mut self) {
        self.0.exited.store(true, Ordering::Release);
    }
}

fn refresh_loop<S: LocalRulesSource>(
    weak_owner: Weak<LocalOffsetOwner>,
    source: S,
    signal: Arc<RefresherSignal>,
) {
    let _exit = RefresherExitMark(Arc::clone(&signal));
    while signal.wait_period() {
        // Evaluate before taking a strong reference so the owner is held only
        // for the publish itself.
        let candidate = build_snapshot(&source, unix_seconds_floor(SystemTime::now()));
        let Some(owner) = weak_owner.upgrade() else {
            break;
        };
        // A window without an established unique transition is not
        // published; the previous snapshot keeps its own expiry.
        if let Some(snapshot) = candidate {
            owner.publish(snapshot);
        }
    }
}

fn first_snapshot<S: LocalRulesSource>(
    source: &S,
) -> Result<LocalOffsetSnapshot, LocalOffsetOwnerError> {
    for _ in 0..FIRST_SNAPSHOT_ATTEMPTS {
        if let Some(snapshot) = build_snapshot(source, unix_seconds_floor(SystemTime::now())) {
            return Ok(snapshot);
        }
    }
    Err(LocalOffsetOwnerError {
        message: format!(
            "the first snapshot could not be established in {FIRST_SNAPSHOT_ATTEMPTS} attempts"
        ),
    })
}

/// Builds the snapshot for `[anchor - lookbehind, anchor + H)`.
///
/// Every whole second of the window is evaluated: equal offsets at both ends
/// do not prove the absence of an even number of transitions, so endpoint
/// bisection alone cannot establish a unique transition. A second transition
/// shortens the window to end just before it. The candidate is accepted only
/// when a second full evaluation agrees on every covered second, which
/// rejects a window that straddles a reload of the rules source. A window
/// that does not cover the anchor, or any instant the source cannot evaluate,
/// yields `None`.
fn build_snapshot<S: LocalRulesSource>(source: &S, anchor_utc: i64) -> Option<LocalOffsetSnapshot> {
    let valid_from = anchor_utc.checked_sub(LOCAL_OFFSET_LOOKBEHIND_SECS)?;
    let horizon_end = anchor_utc.checked_add(LOCAL_OFFSET_HORIZON_SECS)?;
    let candidate = scan_window(source, valid_from, horizon_end)?;
    if candidate.valid_until_utc <= anchor_utc {
        return None;
    }
    confirm_window(source, &candidate).then_some(candidate)
}

fn scan_window<S: LocalRulesSource>(
    source: &S,
    valid_from: i64,
    horizon_end: i64,
) -> Option<LocalOffsetSnapshot> {
    let offset_before = source.offset_at(valid_from)?;
    let mut transition: Option<(i64, FixedOffset)> = None;
    let mut valid_until = horizon_end;
    for instant in valid_from + 1..horizon_end {
        let offset = source.offset_at(instant)?;
        let in_effect = transition.map_or(offset_before, |(_, after)| after);
        if offset == in_effect {
            continue;
        }
        if transition.is_some() {
            valid_until = instant;
            break;
        }
        transition = Some((instant, offset));
    }
    Some(LocalOffsetSnapshot {
        valid_from_utc: valid_from,
        valid_until_utc: valid_until,
        offset_before,
        transition_at_utc: transition.map(|(at, _)| at),
        offset_after: transition.map_or(offset_before, |(_, after)| after),
    })
}

fn confirm_window<S: LocalRulesSource>(source: &S, candidate: &LocalOffsetSnapshot) -> bool {
    (candidate.valid_from_utc..candidate.valid_until_utc).all(|instant| {
        matches!(
            (source.offset_at(instant), candidate.offset_at(instant)),
            (Some(evaluated), Ok(published)) if evaluated == published
        )
    })
}

#[cfg(test)]
pub(crate) struct LocalOffsetRefresherExitProbe(Arc<RefresherSignal>);

#[cfg(test)]
impl LocalOffsetRefresherExitProbe {
    pub(crate) fn exited(&self) -> bool {
        self.0.exited.load(Ordering::Acquire)
    }
}

/// Fixed rules with one transition, for tests outside this module.
#[cfg(test)]
pub(crate) struct LocalOffsetStepRules {
    pub(crate) transition_at_utc: i64,
    pub(crate) before: FixedOffset,
    pub(crate) after: FixedOffset,
}

#[cfg(test)]
impl LocalRulesSource for LocalOffsetStepRules {
    fn offset_at(&self, utc_secs: i64) -> Option<FixedOffset> {
        Some(if utc_secs < self.transition_at_utc {
            self.before
        } else {
            self.after
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, AtomicU64};
    use std::time::Instant;

    use chrono::{Offset, Utc};

    use super::*;

    const HOUR: i32 = 3600;

    fn east(secs: i32) -> FixedOffset {
        FixedOffset::east_opt(secs).expect("valid test offset")
    }

    fn now_secs() -> i64 {
        unix_seconds_floor(SystemTime::now())
    }

    fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        predicate()
    }

    fn utc_secs(year: i32, month: u32, day: u32, hour: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0)
            .single()
            .expect("valid test instant")
            .timestamp()
    }

    fn chrono_local_at(utc_secs: i64) -> FixedOffset {
        *Local
            .timestamp_opt(utc_secs, 0)
            .single()
            .expect("a UTC instant maps to one local time")
            .offset()
    }

    struct TzRules(chrono_tz::Tz);

    impl LocalRulesSource for TzRules {
        fn offset_at(&self, utc_secs: i64) -> Option<FixedOffset> {
            let utc = DateTime::from_timestamp(utc_secs, 0)?;
            Some(self.0.offset_from_utc_datetime(&utc.naive_utc()).fix())
        }
    }

    /// Rules shared with the test thread: the offset can change, and the
    /// source can stop being evaluable.
    #[derive(Clone)]
    struct SharedRules {
        offset_secs: Arc<AtomicI32>,
        unevaluable: Arc<AtomicBool>,
        seen_on_refresher: Arc<AtomicBool>,
    }

    impl SharedRules {
        fn new(offset_secs: i32) -> Self {
            Self {
                offset_secs: Arc::new(AtomicI32::new(offset_secs)),
                unevaluable: Arc::new(AtomicBool::new(false)),
                seen_on_refresher: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl LocalRulesSource for SharedRules {
        fn offset_at(&self, _utc_secs: i64) -> Option<FixedOffset> {
            if thread::current().name() == Some(REFRESHER_THREAD_NAME) {
                self.seen_on_refresher.store(true, Ordering::Release);
            }
            if self.unevaluable.load(Ordering::Acquire) {
                return None;
            }
            FixedOffset::east_opt(self.offset_secs.load(Ordering::Acquire))
        }
    }

    #[test]
    fn local_offset_snapshot_without_transition_covers_the_whole_window() {
        let anchor = 1_000_000;
        // The transition sits exactly at the exclusive window end.
        let snapshot = build_snapshot(
            &LocalOffsetStepRules {
                transition_at_utc: anchor + LOCAL_OFFSET_HORIZON_SECS,
                before: east(8 * HOUR),
                after: east(9 * HOUR),
            },
            anchor,
        )
        .expect("rules without a transition in the window give a snapshot");

        assert_eq!(
            snapshot.valid_from_utc(),
            anchor - LOCAL_OFFSET_LOOKBEHIND_SECS
        );
        assert_eq!(
            snapshot.valid_until_utc(),
            anchor + LOCAL_OFFSET_HORIZON_SECS
        );
        assert_eq!(snapshot.transition_at_utc(), None);
        for instant in snapshot.valid_from_utc()..snapshot.valid_until_utc() {
            assert_eq!(snapshot.offset_at(instant), Ok(east(8 * HOUR)));
        }
    }

    #[test]
    fn local_offset_snapshot_resolves_real_dst_transitions_at_the_second() {
        let rules = TzRules(chrono_tz::America::New_York);
        // 2026-03-08 02:00 EST -> 03:00 EDT and 2026-11-01 02:00 EDT -> 01:00 EST.
        for (transition, before, after) in [
            (utc_secs(2026, 3, 8, 7), -5 * HOUR, -4 * HOUR),
            (utc_secs(2026, 11, 1, 6), -4 * HOUR, -5 * HOUR),
        ] {
            let snapshot =
                build_snapshot(&rules, transition - 20).expect("one transition is established");

            assert_eq!(snapshot.transition_at_utc(), Some(transition));
            assert_eq!(snapshot.offset_at(transition - 1), Ok(east(before)));
            assert_eq!(snapshot.offset_at(transition), Ok(east(after)));
            assert_eq!(snapshot.offset_at(transition + 1), Ok(east(after)));
            for instant in snapshot.valid_from_utc()..snapshot.valid_until_utc() {
                assert_eq!(snapshot.offset_at(instant).ok(), rules.offset_at(instant));
            }
        }
    }

    #[test]
    fn local_offset_snapshot_keeps_transitions_at_window_edges() {
        let anchor = 2_000_000;
        let first = anchor - LOCAL_OFFSET_LOOKBEHIND_SECS + 1;
        let last = anchor + LOCAL_OFFSET_HORIZON_SECS - 1;
        for transition in [first, last] {
            let rules = LocalOffsetStepRules {
                transition_at_utc: transition,
                before: east(HOUR),
                after: east(2 * HOUR),
            };
            let snapshot = build_snapshot(&rules, anchor).expect("one transition");

            assert_eq!(snapshot.transition_at_utc(), Some(transition));
            assert_eq!(snapshot.offset_at(transition - 1), Ok(east(HOUR)));
            assert_eq!(snapshot.offset_at(transition), Ok(east(2 * HOUR)));
        }

        // A transition exactly at the window start is the offset in effect.
        let snapshot = build_snapshot(
            &LocalOffsetStepRules {
                transition_at_utc: anchor - LOCAL_OFFSET_LOOKBEHIND_SECS,
                before: east(HOUR),
                after: east(2 * HOUR),
            },
            anchor,
        )
        .expect("no transition inside the window");
        assert_eq!(snapshot.transition_at_utc(), None);
        assert_eq!(
            snapshot.offset_at(snapshot.valid_from_utc()),
            Ok(east(2 * HOUR))
        );
    }

    #[test]
    fn local_offset_second_transition_shortens_the_window() {
        /// One more hour east after each listed transition.
        struct TwoSteps(i64, i64);
        impl LocalRulesSource for TwoSteps {
            fn offset_at(&self, utc_secs: i64) -> Option<FixedOffset> {
                Some(if utc_secs < self.0 {
                    east(0)
                } else if utc_secs < self.1 {
                    east(HOUR)
                } else {
                    east(2 * HOUR)
                })
            }
        }
        let anchor = 3_000_000;
        let snapshot =
            build_snapshot(&TwoSteps(anchor + 5, anchor + 15), anchor).expect("first transition");

        assert_eq!(snapshot.transition_at_utc(), Some(anchor + 5));
        assert_eq!(snapshot.valid_until_utc(), anchor + 15);
        assert_eq!(snapshot.offset_at(anchor + 14), Ok(east(HOUR)));
        assert_eq!(
            snapshot.offset_at(anchor + 15),
            Err(LocalOffsetExpired {
                at_utc: anchor + 15,
                valid_from_utc: anchor - LOCAL_OFFSET_LOOKBEHIND_SECS,
                valid_until_utc: anchor + 15,
            })
        );

        // Two transitions up to the anchor leave no window covering it.
        assert_eq!(
            build_snapshot(&TwoSteps(anchor - 8, anchor - 3), anchor),
            None
        );
        assert_eq!(build_snapshot(&TwoSteps(anchor - 8, anchor), anchor), None);
        let covering = build_snapshot(&TwoSteps(anchor - 8, anchor + 1), anchor)
            .expect("the window still covers the anchor");
        assert_eq!(covering.valid_until_utc(), anchor + 1);
        assert_eq!(covering.offset_at(anchor), Ok(east(HOUR)));
    }

    #[test]
    fn local_offset_unestablished_window_is_not_published() {
        struct UnevaluableAt(i64);
        impl LocalRulesSource for UnevaluableAt {
            fn offset_at(&self, utc_secs: i64) -> Option<FixedOffset> {
                (utc_secs != self.0).then(|| east(HOUR))
            }
        }
        let anchor = 4_000_000;
        assert_eq!(build_snapshot(&UnevaluableAt(anchor + 30), anchor), None);

        // Rules that change between the two evaluations are not trusted.
        struct ChangesAfter {
            calls: AtomicU64,
            limit: u64,
        }
        impl LocalRulesSource for ChangesAfter {
            fn offset_at(&self, _utc_secs: i64) -> Option<FixedOffset> {
                let call = self.calls.fetch_add(1, Ordering::Relaxed);
                Some(if call < self.limit {
                    east(HOUR)
                } else {
                    east(2 * HOUR)
                })
            }
        }
        let window = (LOCAL_OFFSET_LOOKBEHIND_SECS + LOCAL_OFFSET_HORIZON_SECS) as u64;
        let rules = ChangesAfter {
            calls: AtomicU64::new(0),
            limit: window,
        };
        assert_eq!(build_snapshot(&rules, anchor), None);
    }

    #[test]
    fn local_offset_owner_publishes_the_first_snapshot_before_returning() {
        let before = now_secs();
        let owner = LocalOffsetOwner::start(LocalOffsetStepRules {
            transition_at_utc: before + 20,
            before: east(HOUR),
            after: east(2 * HOUR),
        })
        .expect("start owner");
        let after = now_secs();

        let snapshot = owner.snapshot();
        assert!(snapshot.valid_from_utc() <= before);
        assert!(snapshot.valid_until_utc() > after);
        assert_eq!(snapshot.transition_at_utc(), Some(before + 20));
        assert_eq!(owner.offset_at(before + 19), Ok(east(HOUR)));
        assert_eq!(owner.offset_at(before + 20), Ok(east(2 * HOUR)));
        assert_eq!(owner.offset_at(before + 21), Ok(east(2 * HOUR)));
        assert_eq!(owner.offset_now(), Ok(east(HOUR)));
    }

    #[test]
    fn local_offset_owner_rejects_instants_outside_the_snapshot() {
        let owner = LocalOffsetOwner::start(SharedRules::new(HOUR)).expect("start owner");
        let snapshot = owner.snapshot();

        let expired = snapshot
            .offset_at(snapshot.valid_until_utc())
            .expect_err("the window end is exclusive");
        assert_eq!(expired.at_utc(), snapshot.valid_until_utc());
        assert_eq!(expired.valid_from_utc(), snapshot.valid_from_utc());
        assert_eq!(expired.valid_until_utc(), snapshot.valid_until_utc());
        assert!(snapshot.offset_at(snapshot.valid_from_utc() - 1).is_err());
        assert!(owner.offset_at(i64::MAX).is_err());
        assert!(owner.offset_at(i64::MIN).is_err());

        let failure = expired.into_task_failure();
        assert_eq!(failure.category(), TaskFailureCategory::ResourceExhausted);
        assert!(
            failure
                .detail()
                .as_str()
                .contains("local time zone offset snapshot does not cover")
        );
    }

    #[test]
    fn local_offset_owner_start_fails_without_an_established_snapshot() {
        let rules = SharedRules::new(HOUR);
        rules.unevaluable.store(true, Ordering::Release);

        let error = LocalOffsetOwner::start(rules).expect_err("no first snapshot");
        assert!(error.to_string().contains("first snapshot"));
    }

    #[test]
    fn local_offset_refresher_publishes_source_changes_on_its_own_thread() {
        let rules = SharedRules::new(HOUR);
        let owner = LocalOffsetOwner::start(rules.clone()).expect("start owner");
        assert_eq!(owner.offset_now(), Ok(east(HOUR)));

        rules.offset_secs.store(-3 * HOUR, Ordering::Release);

        assert!(wait_until(Duration::from_secs(5), || {
            owner.offset_now() == Ok(east(-3 * HOUR))
        }));
        assert!(rules.seen_on_refresher.load(Ordering::Acquire));
    }

    #[test]
    fn local_offset_failed_refresh_keeps_the_previous_snapshot() {
        let rules = SharedRules::new(HOUR);
        let owner = LocalOffsetOwner::start(rules.clone()).expect("start owner");

        rules.unevaluable.store(true, Ordering::Release);
        // Let any refresh that evaluated before the change finish publishing.
        thread::sleep(LOCAL_OFFSET_REFRESH_PERIOD + Duration::from_millis(100));
        let kept = owner.snapshot();
        thread::sleep(2 * LOCAL_OFFSET_REFRESH_PERIOD + Duration::from_millis(100));

        assert_eq!(owner.snapshot(), kept);
        assert_eq!(owner.offset_at(kept.valid_from_utc()), Ok(east(HOUR)));
        assert!(owner.offset_at(kept.valid_until_utc()).is_err());
    }

    #[test]
    fn local_offset_refresher_exits_after_owner_drop() {
        let owner = LocalOffsetOwner::start(SharedRules::new(0)).expect("start owner");
        let probe = owner.refresher_exit_probe();
        assert!(!probe.exited());

        drop(owner);

        assert!(wait_until(Duration::from_secs(2), || probe.exited()));
    }

    #[test]
    fn local_offset_production_rules_match_chrono_local_in_machine_zone() {
        let owner = LocalOffsetOwner::start(ChronoLocalRules).expect("start owner");
        let snapshot = owner.snapshot();
        for instant in snapshot.valid_from_utc()..snapshot.valid_until_utc() {
            assert_eq!(snapshot.offset_at(instant), Ok(chrono_local_at(instant)));
        }

        // Sampled instants from forty years back to two years ahead.
        let now = now_secs();
        let start = now - 40 * 366 * 86_400;
        let end = now + 2 * 366 * 86_400;
        let mut transitions = Vec::new();
        let mut previous = chrono_local_at(start);
        let mut hour = start + 3600;
        while hour < end {
            let offset = chrono_local_at(hour);
            assert_eq!(ChronoLocalRules.offset_at(hour), Some(offset));
            if offset != previous {
                // Locate the exact transition second inside (hour - 3600, hour].
                let (mut low, mut high) = (hour - 3600, hour);
                while high - low > 1 {
                    let middle = low + (high - low) / 2;
                    if chrono_local_at(middle) == previous {
                        low = middle;
                    } else {
                        high = middle;
                    }
                }
                transitions.push(high);
                previous = offset;
            }
            hour += 3600;
        }
        for transition in transitions {
            let snapshot = build_snapshot(&ChronoLocalRules, transition - 20)
                .expect("a real transition is established");
            assert_eq!(snapshot.transition_at_utc(), Some(transition));
            for instant in snapshot.valid_from_utc()..snapshot.valid_until_utc() {
                assert_eq!(snapshot.offset_at(instant), Ok(chrono_local_at(instant)));
            }
        }
    }

    #[test]
    fn local_offset_unix_seconds_round_toward_negative_infinity() {
        assert_eq!(unix_seconds_floor(UNIX_EPOCH), 0);
        assert_eq!(
            unix_seconds_floor(UNIX_EPOCH + Duration::from_millis(1_500)),
            1
        );
        assert_eq!(
            unix_seconds_floor(UNIX_EPOCH - Duration::from_millis(500)),
            -1
        );
        assert_eq!(unix_seconds_floor(UNIX_EPOCH - Duration::from_secs(2)), -2);
    }
}
