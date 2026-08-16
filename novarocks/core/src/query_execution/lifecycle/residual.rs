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

//! Core-owned lifecycle orchestration residuals.
//!
//! These values are not FE/BE wire contracts. They remain in Core until the
//! CLS-R2 orchestration cutover, while wire values are owned by Protocol.

use std::net::SocketAddr;

/// Frozen target selected from one live backend snapshot.
///
/// This is coordinator orchestration state, not a native lifecycle message.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryLifecycleTarget {
    backend_idx: usize,
    endpoint: SocketAddr,
    start_epoch: u64,
}

impl QueryLifecycleTarget {
    pub const fn new(backend_idx: usize, endpoint: SocketAddr, start_epoch: u64) -> Self {
        Self {
            backend_idx,
            endpoint,
            start_epoch,
        }
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(self) -> SocketAddr {
        self.endpoint
    }

    pub const fn start_epoch(self) -> u64 {
        self.start_epoch
    }
}
