// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

mod actor;
mod actor_state;
mod completion;
mod conclusion;
mod context;
mod delivery;
mod dispatch;
mod domain_tracker;
mod establish;
mod lease;
mod operation;
mod plan_activation;
mod recovery;
mod replacement;
mod result;
mod result_decode;
mod result_pump;
mod runtime_registry;
mod schedule;
mod stand_down;
mod status;
mod supervisor;
mod task_update_retry;

pub use actor::*;
pub use actor_state::*;
pub use completion::*;
pub use conclusion::*;
pub use context::*;
pub use delivery::*;
pub use dispatch::*;
pub use domain_tracker::*;
pub use establish::*;
pub use lease::*;
pub use operation::*;
pub use plan_activation::*;
pub use recovery::*;
pub use replacement::*;
pub use result::*;
pub use result_pump::*;
pub use runtime_registry::*;
pub use schedule::*;
pub use stand_down::*;
pub use status::*;
pub use supervisor::*;
pub use task_update_retry::*;

// The fixed-worker queue is a process-runtime primitive. Product runtimes
// retain their own typed owners around it; they do not share admission or
// lifecycle authority merely because they share this implementation.
pub(crate) use result_decode::{
    BoundedResultDecodeHandle, BoundedResultDecodeOwner, ResultDecodeExecutorConfig,
    ResultDecodeJob,
};

/// Enforces a process owner's explicit-shutdown invariant from its `Drop`.
///
/// A process owner that reaches its destructor without an explicit shutdown
/// has leaked the runtime it owns, and that is a defect worth failing on. The
/// invariant is deliberately not enforced while the thread is already
/// unwinding. A panic raised from a destructor during unwinding is not a
/// second reported failure: Rust turns it into a non-unwinding panic and
/// aborts the process, which in a test binary destroys every other test's
/// result and buries the original panic that is the real diagnosis. An owner
/// abandoned by an unwind was never given the chance to shut down, so the leak
/// this would report is a consequence of that panic rather than independent
/// evidence of a leak.
pub(crate) fn assert_shutdown_complete(shutdown_complete: bool, owner: &str) {
    assert!(
        shutdown_complete || std::thread::panicking(),
        "{owner} dropped without explicit shutdown"
    );
}
