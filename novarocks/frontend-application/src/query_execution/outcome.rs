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

//! Retained write completion and fragment profile values.

use crate::query_execution::profile::FragmentProfileTree;

/// A write whose data plane closed and whose execution succeeded, carried to
/// the statement owner so it can perform the one external commit.
///
/// The coordinator has already checked both halves of the gate; what it has not
/// done, and must not do, is commit. Affected rows stay inside here until the
/// commit is known to have succeeded, because reporting them earlier would tell
/// a client about rows that may never become visible.
pub struct ConnectorWriteSessionCompletion {
    prepared: crate::query_execution::write_result::DecodedPreparedWriteSet,
    // Last: the original COW holder outlives the completion payload.
    session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl ConnectorWriteSessionCompletion {
    pub(crate) const fn session(
        &self,
    ) -> &std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession> {
        &self.session
    }

    /// The rows every writer accepted. Report them to a client only after the
    /// external commit is known to have succeeded.
    pub(crate) fn row_count(&self) -> u64 {
        self.prepared.row_count()
    }

    pub(crate) fn logical_rows_after_write(
        &self,
        previous: Option<u64>,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<u64>, novarocks_spi::connector::ConnectorError> {
        self.session
            .logical_rows_after_write(&self.prepared, previous, context)
    }

    /// Whether the closed data plane produced no commit fragment at all.
    ///
    /// This is not "zero rows": a writer that accepted no row still emits its
    /// fragment. An empty set means no writer produced anything, which is the
    /// one case where committing would publish a snapshot describing nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.prepared.fragments().is_empty()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        crate::query_execution::write_result::DecodedPreparedWriteSet,
    ) {
        (self.session, self.prepared)
    }

    /// Build a completion directly, for statement-flow tests that need one
    /// without driving a whole distributed round. Production code has no such
    /// constructor: only the coordinator's dual barrier produces one.
    #[cfg(test)]
    pub(crate) const fn for_test(
        session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        prepared: crate::query_execution::write_result::DecodedPreparedWriteSet,
    ) -> Self {
        Self { session, prepared }
    }
}

pub struct FragmentProfileSet {
    profiles: Vec<FragmentProfileTree>,
}

impl FragmentProfileSet {
    pub(crate) fn new(profiles: Vec<FragmentProfileTree>) -> Self {
        Self { profiles }
    }

    pub(crate) fn into_profiles(self) -> Vec<FragmentProfileTree> {
        self.profiles
    }
}
