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

//! The engine-facing distributed-query boundary.
//!
//! This module deliberately contains only immutable request/outcome values and
//! the object-safe port. Query-wide state, timers, callbacks, and role choice
//! belong to the frontend coordinator, not to core.

use std::fmt;

use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::preparation::PreparedFragmentSet;
use crate::query_execution::write::{WriteAbortInput, WriteCommitInput};
use crate::runtime::profile::RuntimeProfileTree;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::query_result::QueryResult;

/// The engine-visible purpose of a distributed execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryIntent {
    Result,
    Write,
    Profile,
}

/// Immutable execution inputs assembled by core before coordinator ownership
/// is selected. Its fields remain private so callers cannot combine artifacts
/// from different sealed plans.
pub(crate) struct DistributedQueryArtifacts {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    query_options: Option<QueryOptions>,
}

impl DistributedQueryArtifacts {
    pub(crate) fn new(
        prepared: PreparedFragmentSet,
        native_bundle: NativeFragmentBundle,
        query_options: Option<QueryOptions>,
    ) -> Self {
        Self {
            prepared,
            native_bundle,
            query_options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        PreparedFragmentSet,
        NativeFragmentBundle,
        Option<QueryOptions>,
    ) {
        (self.prepared, self.native_bundle, self.query_options)
    }
}

/// A sealed request passed from the engine to its injected coordinator.
pub struct DistributedQueryRequest {
    artifacts: DistributedQueryArtifacts,
    intent: DistributedQueryIntent,
}

impl DistributedQueryRequest {
    pub(crate) fn new(
        artifacts: DistributedQueryArtifacts,
        intent: DistributedQueryIntent,
    ) -> Self {
        Self { artifacts, intent }
    }

    pub fn intent(&self) -> DistributedQueryIntent {
        self.intent
    }

    /// Consumes the sealed request and creates a successful result outcome.
    /// The request intent is preserved, while the immutable execution
    /// artifacts remain opaque to the frontend implementation.
    pub fn into_success(self, query_result: QueryResult) -> DistributedQueryOutcome {
        DistributedQueryOutcome::new(self.intent, query_result, None, None, Vec::new())
    }

    pub(crate) fn into_parts(self) -> (DistributedQueryArtifacts, DistributedQueryIntent) {
        (self.artifacts, self.intent)
    }

    /// Creates an opaque request for cross-crate contract conformance tests.
    ///
    /// Production request construction remains internal to core's sealed-plan
    /// path. This fixture carries no executable fragments and exists solely
    /// so a role crate can prove it implements the public execution port.
    pub fn for_contract_test(intent: DistributedQueryIntent) -> Self {
        Self {
            artifacts: DistributedQueryArtifacts {
                prepared:
                    crate::query_execution::preparation::empty_prepared_fragment_set_for_contract_test(),
                native_bundle:
                    crate::protocol::native::encode::empty_native_fragment_bundle_for_contract_test(),
                query_options: None,
            },
            intent,
        }
    }
}

/// Engine-visible completion value. Fields are sealed so coordinator-only
/// state can never leak into the engine contract.
pub struct DistributedQueryOutcome {
    intent: DistributedQueryIntent,
    query_result: QueryResult,
    write_commit: Option<WriteCommitInput>,
    write_abort: Option<WriteAbortInput>,
    fragment_profiles: Vec<RuntimeProfileTree>,
}

impl DistributedQueryOutcome {
    pub(crate) fn new(
        intent: DistributedQueryIntent,
        query_result: QueryResult,
        write_commit: Option<WriteCommitInput>,
        write_abort: Option<WriteAbortInput>,
        fragment_profiles: Vec<RuntimeProfileTree>,
    ) -> Self {
        Self {
            intent,
            query_result,
            write_commit,
            write_abort,
            fragment_profiles,
        }
    }

    pub fn intent(&self) -> DistributedQueryIntent {
        self.intent
    }

    /// Returns the result value owned by this outcome without exposing its
    /// sealed execution metadata.
    pub fn into_query_result(self) -> QueryResult {
        self.query_result
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        QueryResult,
        Option<WriteCommitInput>,
        Option<WriteAbortInput>,
        Vec<RuntimeProfileTree>,
    ) {
        (
            self.query_result,
            self.write_commit,
            self.write_abort,
            self.fragment_profiles,
        )
    }
}

/// Stable error categories exposed by the coordinator boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryErrorKind {
    Rejected,
    Failed,
}

/// A coordinator failure that the engine can surface without naming a
/// coordinator implementation or frontend state type.
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

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeCoordinator;

    impl DistributedQueryCoordinator for FakeCoordinator {
        fn execute(
            &self,
            request: DistributedQueryRequest,
        ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
            Ok(request.into_success(QueryResult {
                columns: Vec::new(),
                chunks: Vec::new(),
            }))
        }
    }

    #[test]
    fn injected_coordinator_consumes_sealed_request_and_returns_engine_outcome() {
        let coordinator: &dyn DistributedQueryCoordinator = &FakeCoordinator;
        let outcome = coordinator
            .execute(DistributedQueryRequest::for_contract_test(
                DistributedQueryIntent::Profile,
            ))
            .expect("fake coordinator accepts the engine request");

        assert_eq!(outcome.intent(), DistributedQueryIntent::Profile);
    }
}
