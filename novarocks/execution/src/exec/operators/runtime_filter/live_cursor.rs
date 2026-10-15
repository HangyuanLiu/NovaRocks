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

//! The original ordered consumer's version/terminal transition, independent
//! of its legacy lock/arena shell and the compiled driver's storage owner.

use crate::runtime_filter::{LivePollOutcome, LiveTerminal, LogicalVersion, RuntimeFilterSnapshot};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LiveCursorDisposition {
    Keep,
    PassThrough,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LiveCursorViolation {
    NewerVersionWithoutArtifact,
    Regressed,
}

pub(super) fn apply_poll_outcome(
    observed: &mut Option<LogicalVersion>,
    latest_snapshot: &mut Option<Arc<RuntimeFilterSnapshot>>,
    terminal: &mut Option<LiveTerminal>,
    outcome: LivePollOutcome,
) -> Result<LiveCursorDisposition, LiveCursorViolation> {
    let update_terminal = match outcome {
        LivePollOutcome::Updated { snapshot, terminal } => {
            if observed.is_none_or(|seen| snapshot.logical_version() > seen) {
                *observed = Some(snapshot.logical_version());
                *latest_snapshot = Some(snapshot);
            }
            terminal
        }
        LivePollOutcome::Idle {
            latest_version,
            terminal,
        } => {
            if latest_version.is_some_and(|latest| observed.is_none_or(|seen| latest > seen)) {
                return Err(LiveCursorViolation::NewerVersionWithoutArtifact);
            }
            if latest_version.is_some_and(|latest| observed.is_some_and(|seen| latest < seen)) {
                return Err(LiveCursorViolation::Regressed);
            }
            terminal
        }
    };
    // Equal/stale updates still settle their terminal. A valid final artifact
    // remains active; a terminal with no artifact becomes pass-through.
    if let Some(update_terminal) = update_terminal {
        if latest_snapshot.is_none() {
            return Ok(LiveCursorDisposition::PassThrough);
        }
        *terminal = Some(update_terminal);
    }
    Ok(LiveCursorDisposition::Keep)
}
