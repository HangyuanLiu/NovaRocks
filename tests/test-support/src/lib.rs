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

//! Shared test-only process and network resource lifecycle helpers.
//!
//! This crate intentionally owns only mechanical testing concerns. Domain
//! fixtures, provider conformance, and NovaRocks runtime semantics stay with
//! their respective assertion owners.

// Design: ADR-0069 (docs/adr/ADR-0069-shared-test-mechanics-owner.md)
mod managed_process;
#[path = "monitor/client.rs"]
mod monitor_client;
mod reserved_tcp_port;
mod run_retention;
#[cfg(unix)]
mod socket_deadline;
mod unix;
#[cfg(unix)]
pub use socket_deadline::SocketDeadlineOwner;

pub use managed_process::{
    ManagedProcess, ManagedProcessLogSource, ManagedProcessLogTail, ReadyMarker,
};
pub use monitor_client::ManagedCommand;
pub use reserved_tcp_port::ReservedTcpPort;

/// Establishes the ordinary nextest run lease before a nested runner is spawned.
/// This is lazy: pure tests and test discovery never call it implicitly.
pub fn ensure_run_retention(deadline: std::time::Instant) -> std::io::Result<()> {
    run_retention::ensure_run_retention(deadline).map(|_| ())
}

#[cfg(unix)]
pub use managed_process::{CapturedCommandOutcome, capture_command_until};
