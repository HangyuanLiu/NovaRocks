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

//! Worker-owned monotonic time sources for local task lifecycle decisions.

use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::MonotonicInstant;

/// The process-local timeline used to decide Worker lifecycle deadlines.
pub trait WorkerMonotonicClock: fmt::Debug + Send + Sync {
    fn now(&self) -> MonotonicInstant;
}

/// Production clock whose readings are elapsed time since process startup.
#[derive(Debug)]
pub struct ProcessMonotonicClock {
    origin: Instant,
}

impl ProcessMonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for ProcessMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerMonotonicClock for ProcessMonotonicClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(self.origin.elapsed())
    }
}

/// A manually advanced timeline for deterministic Worker lifecycle tests.
#[derive(Debug, Default)]
pub struct ManualClock {
    elapsed: Mutex<Duration>,
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            elapsed: Mutex::new(Duration::ZERO),
        }
    }

    /// Moves the timeline forward without waiting on wall time.
    pub fn advance(&self, delta: Duration) {
        let mut elapsed = self.elapsed.lock().expect("manual clock lock");
        *elapsed = elapsed.saturating_add(delta);
    }
}

impl WorkerMonotonicClock for ManualClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(*self.elapsed.lock().expect("manual clock lock"))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ManualClock, WorkerMonotonicClock};
    use crate::MonotonicInstant;

    #[test]
    fn manual_clock_advances_only_when_its_owner_moves_it() {
        let clock = ManualClock::new();
        assert_eq!(clock.now(), MonotonicInstant::ORIGIN);

        clock.advance(Duration::from_millis(17));
        assert_eq!(
            clock.now(),
            MonotonicInstant::from_origin(Duration::from_millis(17))
        );
    }
}
