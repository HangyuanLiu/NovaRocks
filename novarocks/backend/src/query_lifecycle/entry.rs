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

use std::collections::BTreeSet;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use novarocks::UniqueId;
use novarocks::query_execution::lifecycle::{
    ParticipantManifest, ParticipantManifestDigest, QueryControlEvent, QueryInitOutcome,
    QueryTerminationReason,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryLifecyclePhase {
    Initializing,
    Initialized,
    ControlAttached,
    Terminating,
    Tombstone,
}

pub(crate) struct QueryLifecycleEntry {
    pub(crate) digest: ParticipantManifestDigest,
    pub(crate) manifest: ParticipantManifest,
    pub(crate) state: Mutex<QueryLifecycleEntryState>,
    pub(crate) init_completed: Condvar,
}

pub(crate) struct QueryLifecycleEntryState {
    pub(crate) phase: QueryLifecyclePhase,
    pub(crate) init_outcome: Option<QueryInitOutcome>,
    pub(crate) termination_reason: Option<QueryTerminationReason>,
    pub(crate) runtime_filter_installed: bool,
    pub(crate) runtime_filter_cleanup_required: bool,
    pub(crate) runtime_filter_cleanup_in_flight: bool,
    pub(crate) ever_initialized: bool,
    pub(crate) terminated_at: Option<Instant>,
    pub(crate) in_flight_fragments: BTreeSet<UniqueId>,
    pub(crate) accepted_fragments: BTreeSet<UniqueId>,
    pub(crate) pre_start_deadline: Option<Instant>,
    pub(crate) last_heartbeat: Option<Instant>,
    pub(crate) frontend_owner_epoch: Option<u64>,
    pub(crate) events: Option<tokio::sync::mpsc::Sender<QueryControlEvent>>,
    pub(crate) terminal_event_permit: Option<tokio::sync::mpsc::OwnedPermit<QueryControlEvent>>,
}

impl QueryLifecycleEntry {
    pub(crate) fn initializing(
        manifest: ParticipantManifest,
        digest: ParticipantManifestDigest,
    ) -> Self {
        Self {
            digest,
            manifest,
            state: Mutex::new(QueryLifecycleEntryState {
                phase: QueryLifecyclePhase::Initializing,
                init_outcome: None,
                termination_reason: None,
                runtime_filter_installed: false,
                runtime_filter_cleanup_required: false,
                runtime_filter_cleanup_in_flight: false,
                ever_initialized: false,
                terminated_at: None,
                in_flight_fragments: BTreeSet::new(),
                accepted_fragments: BTreeSet::new(),
                pre_start_deadline: None,
                last_heartbeat: None,
                frontend_owner_epoch: None,
                events: None,
                terminal_event_permit: None,
            }),
            init_completed: Condvar::new(),
        }
    }
}
