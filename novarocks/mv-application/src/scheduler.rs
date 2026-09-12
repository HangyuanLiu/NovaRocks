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

//! Product policy configuration for asynchronous MV refresh scheduling.

/// Frozen process-local policy for asynchronous materialized-view refresh.
///
/// It does not own a queue, thread, provider handle, or persisted record. The
/// role-local scheduler owns those resources and consumes these product bounds
/// after configuration has been resolved once at startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvSchedulerConfig {
    enabled: bool,
    tick_interval_ms: u64,
    max_concurrent_refreshes: usize,
    failure_backoff_ms: i64,
    max_failure_backoff_ms: i64,
}

impl MvSchedulerConfig {
    pub const fn new(
        enabled: bool,
        tick_interval_ms: u64,
        max_concurrent_refreshes: usize,
        failure_backoff_ms: i64,
        max_failure_backoff_ms: i64,
    ) -> Self {
        Self {
            enabled,
            tick_interval_ms,
            max_concurrent_refreshes,
            failure_backoff_ms,
            max_failure_backoff_ms,
        }
    }

    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    pub const fn tick_interval_ms(&self) -> u64 {
        self.tick_interval_ms
    }

    pub const fn max_concurrent_refreshes(&self) -> usize {
        self.max_concurrent_refreshes
    }

    pub const fn failure_backoff_ms(&self) -> i64 {
        self.failure_backoff_ms
    }

    pub const fn max_failure_backoff_ms(&self) -> i64 {
        self.max_failure_backoff_ms
    }
}

impl Default for MvSchedulerConfig {
    fn default() -> Self {
        Self::new(false, 30_000, 1, 60_000, 1_800_000)
    }
}

#[cfg(test)]
mod tests {
    use super::MvSchedulerConfig;

    #[test]
    fn default_policy_preserves_the_deployed_scheduler_bounds() {
        assert_eq!(
            MvSchedulerConfig::default(),
            MvSchedulerConfig::new(false, 30_000, 1, 60_000, 1_800_000)
        );
    }
}
