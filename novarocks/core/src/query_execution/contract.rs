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

//! Core-owned distributed-query request contract.

use std::fmt;

use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::artifact::PreparedDistributedQuery;
use crate::query_execution::cancellation::QueryCancellationView;
pub use crate::query_execution::outcome::DistributedQueryOutcome;
pub use crate::query_execution::outcome::FragmentProfileSet;
pub use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::preparation::PreparedFragmentSet;
pub use crate::query_execution::profile::ProfileReportBuilder;
use crate::runtime::query_options::QueryOptions;

/// Coordinator-neutral query identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryId {
    hi: i64,
    lo: i64,
}

impl QueryId {
    pub const fn new(hi: i64, lo: i64) -> Self {
        Self { hi, lo }
    }

    pub const fn high(self) -> i64 {
        self.hi
    }

    pub const fn low(self) -> i64 {
        self.lo
    }

    pub(crate) const fn into_unique_id(self) -> crate::common::types::UniqueId {
        crate::common::types::UniqueId {
            hi: self.hi,
            lo: self.lo,
        }
    }
}

/// Query options resolved by core before ownership crosses into frontend.
///
/// The runtime representation stays private; frontend only receives stable
/// scalar views needed to schedule, submit, and time out native work.
pub struct ResolvedQueryOptions {
    runtime: QueryOptions,
}

impl ResolvedQueryOptions {
    pub(crate) fn from_upstream(options: Option<QueryOptions>) -> Self {
        let mut runtime = options.unwrap_or_default();
        let pipeline_dop =
            crate::runtime::exec_env::calc_pipeline_dop(runtime.pipeline_dop.unwrap_or_default());
        debug_assert!(pipeline_dop > 0, "resolved pipeline DOP must be positive");
        runtime.pipeline_dop = Some(pipeline_dop);
        Self { runtime }
    }

    pub fn timeout_ms(&self) -> i64 {
        self.runtime
            .query_timeout
            .map(|seconds| i64::from(seconds) * 1_000)
            .unwrap_or(300_000)
    }

    pub fn native_submission_options(&self) -> NativeSubmissionOptionsView {
        NativeSubmissionOptionsView {
            pipeline_dop: self
                .runtime
                .pipeline_dop
                .expect("core resolves pipeline DOP before request handoff"),
            enable_profile: self.runtime.enable_profile,
        }
    }

    pub fn runtime_filter_lifecycle(&self) -> RuntimeFilterLifecycleView {
        let (delivery_expire, query_expire) =
            crate::runtime::query_options::query_expire_durations(Some(&self.runtime));
        RuntimeFilterLifecycleView {
            delivery_expire,
            query_expire,
        }
    }

    pub(crate) fn runtime_options(&self) -> &QueryOptions {
        &self.runtime
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeSubmissionOptionsView {
    pipeline_dop: i32,
    enable_profile: bool,
}

impl NativeSubmissionOptionsView {
    pub const fn pipeline_dop(self) -> i32 {
        self.pipeline_dop
    }

    pub const fn enable_profile(self) -> bool {
        self.enable_profile
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterLifecycleView {
    delivery_expire: std::time::Duration,
    query_expire: std::time::Duration,
}

impl RuntimeFilterLifecycleView {
    pub const fn delivery_expire(self) -> std::time::Duration {
        self.delivery_expire
    }

    pub const fn query_expire(self) -> std::time::Duration {
        self.query_expire
    }
}

/// The engine-visible purpose of a distributed execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryIntent {
    Result,
    Write,
    Profile,
}

/// An owned request passed from core to the injected execution coordinator.
///
/// Every field is private so role crates cannot assemble a request from
/// unrelated prepared/native artifacts or replace its cancellation/completion
/// capabilities.
pub struct DistributedQueryRequest {
    artifacts: PreparedDistributedQuery,
    options: ResolvedQueryOptions,
    cancellation: QueryCancellationView,
    completion: QueryOutcomeFactory,
}

impl DistributedQueryRequest {
    pub fn intent(&self) -> DistributedQueryIntent {
        self.completion.intent()
    }

    pub fn artifacts(&self) -> &PreparedDistributedQuery {
        &self.artifacts
    }

    pub fn options(&self) -> &ResolvedQueryOptions {
        &self.options
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }

    pub fn into_parts(self) -> DistributedQueryRequestParts {
        DistributedQueryRequestParts {
            artifacts: self.artifacts,
            options: self.options,
            cancellation: self.cancellation,
            completion: self.completion,
        }
    }
}

/// Consuming frontend handoff. There is deliberately no constructor,
/// `Clone`, or inverse recombination API.
pub struct DistributedQueryRequestParts {
    pub artifacts: PreparedDistributedQuery,
    pub options: ResolvedQueryOptions,
    pub cancellation: QueryCancellationView,
    pub completion: QueryOutcomeFactory,
}

/// The only production constructor for a core-owned distributed request.
#[allow(dead_code)]
pub(crate) fn build_distributed_query_request(
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    options: Option<QueryOptions>,
    intent: DistributedQueryIntent,
    cancellation: QueryCancellationView,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    Ok(DistributedQueryRequest {
        artifacts: PreparedDistributedQuery::new(prepared, native_bundle),
        options: ResolvedQueryOptions::from_upstream(options),
        cancellation,
        completion: QueryOutcomeFactory::new(intent),
    })
}

/// Stable error categories exposed by the coordinator boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryErrorKind {
    ContractViolation,
    Rejected,
    Failed,
}

/// A coordinator failure that core can surface without naming a coordinator
/// implementation or frontend state type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryError {
    kind: DistributedQueryErrorKind,
    message: String,
}

impl DistributedQueryError {
    pub fn new(kind: DistributedQueryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> DistributedQueryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for DistributedQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for DistributedQueryError {}

/// Frontend-owned distributed query execution port.
pub trait DistributedQueryCoordinator: Send + Sync + 'static {
    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError>;
}
