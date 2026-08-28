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

//! Backend-local ingress contracts for native fragment control.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_proto_codec::connector::AdmittedConnectorExecutionDeclaration;
use novarocks_proto_codec::provider::{
    EnsureConnectorExecutionBindingResult, RetireConnectorExecutionBindingResult,
};
use novarocks_spi::connector::ConnectorExecutionBindingKey;
use novarocks_types::{QueryExecutionId, QueryId, UniqueId};

/// A split received by this backend after the exact binding codec recovered
/// its provider-private SPI payload.  Replay compares the separately retained
/// received canonical bytes; it never re-encodes an internal split.
#[derive(Clone, Debug)]
pub(crate) struct ReceivedReadSplit {
    evidence: novarocks_proto_codec::connector_read::ReceivedScheduledSplitEvidence,
    split: novarocks_spi::connector::read_stack::ConnectorReadSplit,
}

impl ReceivedReadSplit {
    pub(crate) const fn new(
        evidence: novarocks_proto_codec::connector_read::ReceivedScheduledSplitEvidence,
        split: novarocks_spi::connector::read_stack::ConnectorReadSplit,
    ) -> Self {
        Self { evidence, split }
    }

    pub(crate) const fn split(&self) -> &novarocks_spi::connector::read_stack::ConnectorReadSplit {
        &self.split
    }
}

impl novarocks_execution::connector::ScheduledSplitFacts for ReceivedReadSplit {
    fn sequence_id(&self) -> u64 {
        self.evidence.sequence_id()
    }

    fn plan_node_id(&self) -> i32 {
        self.evidence.plan_node_id()
    }

    fn canonical_bytes(&self) -> &[u8] {
        self.evidence.canonical_bytes()
    }

    fn retained_size_in_bytes(&self) -> u64 {
        self.split
            .facts()
            .retained_size_in_bytes()
            .saturating_add(self.evidence.canonical_bytes().len() as u64)
    }
}

/// Provisional per-attempt read contexts collected while the fragment plan is
/// decoded. They become visible to TaskUpdate only after the existing fragment
/// admission/registration path succeeds.
pub(crate) struct TypedReadAttemptContext {
    entries: Mutex<BTreeMap<i32, crate::connector::typed_registry::InstalledReadExecution>>,
    published: AtomicBool,
}

impl TypedReadAttemptContext {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            published: AtomicBool::new(false),
        }
    }

    pub(crate) fn register(
        &self,
        plan_node_id: i32,
        execution: crate::connector::typed_registry::InstalledReadExecution,
    ) -> Result<(), String> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "typed read context lock poisoned")?;
        if entries.insert(plan_node_id, execution).is_some() {
            return Err(format!(
                "duplicate typed read execution for plan node {plan_node_id}"
            ));
        }
        Ok(())
    }

    pub(crate) fn publish(&self) {
        self.published.store(true, Ordering::Release);
    }

    pub(crate) fn resolve(
        &self,
        plan_node_id: i32,
    ) -> Option<crate::connector::typed_registry::InstalledReadExecution> {
        if !self.published.load(Ordering::Acquire) {
            return None;
        }
        self.entries.lock().ok()?.get(&plan_node_id).cloned()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
pub(crate) struct NativeFragmentCancelRequest {
    query_id: QueryId,
    fragment_instance_ids: Vec<UniqueId>,
    reason: String,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl NativeFragmentCancelRequest {
    pub(crate) fn new(
        query_id: QueryId,
        fragment_instance_ids: Vec<UniqueId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            query_id,
            fragment_instance_ids,
            reason: reason.into(),
        }
    }
    pub(crate) const fn query_id(&self) -> QueryId {
        self.query_id
    }
    pub(crate) fn fragment_instance_ids(&self) -> &[UniqueId] {
        &self.fragment_instance_ids
    }
    pub(crate) fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeFragmentIngressError {
    message: String,
}

impl NativeFragmentIngressError {
    pub(crate) fn new(error: impl fmt::Display) -> Self {
        Self {
            message: error.to_string(),
        }
    }
}
impl fmt::Display for NativeFragmentIngressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for NativeFragmentIngressError {}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
pub(crate) trait NativeFragmentIngress: Send + Sync + 'static {
    fn ensure_connector_execution_binding(
        &self,
        execution_id: QueryExecutionId,
        declaration: AdmittedConnectorExecutionDeclaration,
    ) -> EnsureConnectorExecutionBindingResult;
    fn retire_connector_execution_binding(
        &self,
        key: ConnectorExecutionBindingKey,
    ) -> RetireConnectorExecutionBindingResult;
    fn cancel(
        &self,
        request: NativeFragmentCancelRequest,
    ) -> Result<(), NativeFragmentIngressError>;
}
