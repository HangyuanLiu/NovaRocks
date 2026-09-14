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

//! One-way process drain state for a Worker role.

use std::sync::atomic::{AtomicBool, Ordering};

/// The single drain fact for one Worker process.
#[derive(Debug, Default)]
pub struct WorkerDrainState {
    draining: AtomicBool,
}

impl WorkerDrainState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Draining is irreversible for one process generation.
    pub fn begin_drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerDrainState;

    #[test]
    fn a_fresh_process_is_not_draining() {
        assert!(!WorkerDrainState::new().is_draining());
    }

    #[test]
    fn draining_is_one_way_and_visible_to_every_reader() {
        let state = WorkerDrainState::new();
        state.begin_drain();
        assert!(state.is_draining());
        state.begin_drain();
        assert!(state.is_draining());
    }
}
