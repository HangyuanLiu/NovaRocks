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

//! Process-lifetime epoch marker reported in heartbeats to help the FE identify
//! likely BE process restarts (same endpoint, new marker) and stale in-flight state.

use std::sync::OnceLock;

static START_EPOCH: OnceLock<u64> = OnceLock::new();

/// A stable, nonzero value for the lifetime of this process. Computed once on
/// first call from wall-clock millis (units match nothing else; only equality
/// across heartbeats matters).
pub fn start_epoch() -> u64 {
    *START_EPOCH.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1)
            .max(1)
    })
}

#[cfg(test)]
mod tests {
    use super::start_epoch;

    #[test]
    fn start_epoch_is_stable_and_nonzero() {
        let a = start_epoch();
        let b = start_epoch();
        assert_eq!(a, b, "start_epoch must be stable within a process");
        assert!(a > 0, "start_epoch must be nonzero");
    }
}
