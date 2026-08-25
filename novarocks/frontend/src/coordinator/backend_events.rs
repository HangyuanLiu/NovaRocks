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

use std::sync::Arc;

use crate::common::backend_topology::{
    BackendQueryEvent, BackendQueryEventSink, LiveBackendTarget,
};
use novarocks_types::{BackendProcessId, QueryId};

use super::query_registry::FrontendQueryRegistry;

/// Frontend-owned view used to translate backend lifecycle events into
/// query-wide failure and dispatcher cancellation.
#[derive(Clone)]
pub struct BackendQueryActivity {
    registry: Arc<FrontendQueryRegistry>,
}

impl BackendQueryActivity {
    pub(crate) fn new(registry: Arc<FrontendQueryRegistry>) -> Self {
        Self { registry }
    }

    pub fn backend_lost(&self, process_id: BackendProcessId) -> Vec<QueryId> {
        self.registry
            .backend_failed(process_id, format!("backend process {process_id} lost"))
    }

    pub fn backend_restarted(
        &self,
        old_process_id: BackendProcessId,
        new_process_id: BackendProcessId,
    ) -> Vec<QueryId> {
        self.registry.backend_restarted(
            old_process_id,
            format!("backend process restarted ({old_process_id} -> {new_process_id})"),
        )
    }
}

impl BackendQueryEventSink for BackendQueryActivity {
    fn on_backend_event(&self, event: BackendQueryEvent) {
        match event {
            BackendQueryEvent::Unavailable {
                process_id, reason, ..
            } => {
                self.registry.backend_failed(process_id, reason);
            }
            BackendQueryEvent::Restarted {
                old_process_id,
                new_process_id,
                ..
            } => {
                self.backend_restarted(old_process_id, new_process_id);
            }
        }
    }

    fn backend_has_active_queries(&self, process_id: BackendProcessId) -> bool {
        self.registry.backend_has_active_queries(process_id)
    }

    fn replace_live_backends(&self, revision: u64, backends: Vec<LiveBackendTarget>) {
        self.registry.replace_live_backends(revision, &backends);
    }
}
