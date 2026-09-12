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

//! The execution-side ports the task protocol owner drives.
//!
//! The owner in [`super::registry`] owns linearization, transactions, status,
//! and retention; it owns no plan decoding, no receiver allocation, and no
//! pipeline. Those are exactly the steps that can be slow or can fail, so they
//! are named here as narrow ports with an explicit undo for every install.
//!
//! Each install has a matching remove because the creation transaction's
//! rollback calls them in reverse: a port whose effect cannot be undone could
//! not take part in an atomic creation.

use novarocks_execution_contract::task_execution::identity::QueryContextRef;
use novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate;
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_worker::{HostRejection, SharedFactsRequest};

/// The query-context side of execution.
///
/// `materialize` runs while the context is `Establishing`, outside the owner's
/// lock and already racing the sequence-zero lease. Whatever it installs must
/// be undone by `release`, because an establish that loses that race rolls
/// back rather than reaching `Active` late.
/// What a query context's tear-down sealed, for the release that reports it.
///
/// A release that installed no participant carries `None`. That is a
/// different statement from an empty contribution, and the two must not be
/// collapsed: the frontend distinguishes "this query had no runtime filter on
/// this backend" from "this backend observed no runtime-filter activity".
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReleasedContextEvidence {
    runtime_filter: Option<QueryTerminalProfileContributionTelemetry>,
}

impl ReleasedContextEvidence {
    /// Evidence from a tear-down that found no participant to seal.
    pub const fn none() -> Self {
        Self {
            runtime_filter: None,
        }
    }

    pub const fn with_runtime_filter(
        runtime_filter: QueryTerminalProfileContributionTelemetry,
    ) -> Self {
        Self {
            runtime_filter: Some(runtime_filter),
        }
    }

    pub const fn runtime_filter(&self) -> Option<&QueryTerminalProfileContributionTelemetry> {
        self.runtime_filter.as_ref()
    }
}

pub trait QueryContextHost: Send + Sync {
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection>;

    /// Undoes everything `materialize` installed. Called for a rollback and
    /// for a normal release, so it must be idempotent.
    ///
    /// Returns the terminal evidence the tear-down sealed. A release is the
    /// only point at which the runtime-filter observation is a complete fact
    /// -- every local task is a terminal record and nothing further will be
    /// observed -- so the seal happens here and its result is handed back
    /// rather than left inside the host for a later reader to go looking for.
    fn release(&self, context: QueryContextRef) -> ReleasedContextEvidence;

    /// Applies a shared-domain advance the owner already classified as
    /// applicable.
    fn advance_shared_domain(
        &self,
        context: QueryContextRef,
        domain: &QueryContextDomainUpdate,
    ) -> Result<(), HostRejection>;
}
