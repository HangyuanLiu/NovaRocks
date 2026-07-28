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
use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::preparation::PreparedFragmentSet;
use crate::runtime::query_options::QueryOptions;

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
    options: Option<QueryOptions>,
    cancellation: QueryCancellationView,
    completion: QueryOutcomeFactory,
}

impl DistributedQueryRequest {
    pub(crate) fn into_internal_parts(
        self,
    ) -> (
        PreparedDistributedQuery,
        Option<QueryOptions>,
        QueryCancellationView,
        QueryOutcomeFactory,
    ) {
        (
            self.artifacts,
            self.options,
            self.cancellation,
            self.completion,
        )
    }
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
        options,
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
