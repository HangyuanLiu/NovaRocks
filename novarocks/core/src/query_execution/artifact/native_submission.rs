// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Sealed schedule-time native submission handoff.
//!
//! The view exposes only a stable identity/key projection.  The attachment is
//! consuming and verifies that a mapper returns exactly one native submission
//! for every sealed placement before Core creates `StageBatch` values.

use std::collections::BTreeSet;

use super::{
    ExpectedOutputSchema, FragmentId, RootFetchMetadata, ValidatedNativeSubmission,
    WriterRegistrationSet,
};
use crate::common::types::UniqueId;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle::QueryExecutionId;

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// One frozen native-submission identity.  It intentionally contains no
/// mutable schedule, connector lease, or payload-construction capability.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NativeSubmissionKey {
    backend_idx: usize,
    fragment_id: FragmentId,
    fragment_instance_id: UniqueId,
}

impl NativeSubmissionKey {
    pub(crate) const fn new(
        backend_idx: usize,
        fragment_id: FragmentId,
        fragment_instance_id: UniqueId,
    ) -> Self {
        Self {
            backend_idx,
            fragment_id,
            fragment_instance_id,
        }
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }

    pub const fn fragment_id(self) -> FragmentId {
        self.fragment_id
    }

    pub const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }
}

/// Borrow-only facts for encoding one fully prepared, control-ready native
/// submission.  It has no public constructor.
#[derive(Clone)]
pub struct NativeSubmissionEncodingView<'a> {
    handoff_id: u64,
    execution_id: QueryExecutionId,
    keys: Vec<NativeSubmissionKey>,
    root: NativeSubmissionKey,
    _borrowed: std::marker::PhantomData<&'a ()>,
}

impl<'a> NativeSubmissionEncodingView<'a> {
    pub(crate) fn new(
        handoff_id: u64,
        execution_id: QueryExecutionId,
        keys: Vec<NativeSubmissionKey>,
        root: NativeSubmissionKey,
    ) -> Result<Self, DistributedQueryError> {
        let actual = keys.iter().copied().collect::<BTreeSet<_>>();
        if actual.len() != keys.len() {
            return Err(contract_error(
                "native submission encoding view repeats a sealed placement key",
            ));
        }
        if !actual.contains(&root) {
            return Err(contract_error(
                "native submission encoding view root is absent from sealed placement keys",
            ));
        }
        Ok(Self {
            handoff_id,
            execution_id,
            keys,
            root,
            _borrowed: std::marker::PhantomData,
        })
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn placement_keys(&self) -> impl ExactSizeIterator<Item = NativeSubmissionKey> + '_ {
        self.keys.iter().copied()
    }

    pub fn root_key(&self) -> NativeSubmissionKey {
        self.root
    }

    pub(crate) fn seal(
        &self,
        submissions: Vec<ValidatedNativeSubmission>,
        root_fetch: RootFetchMetadata,
        writer_registrations: WriterRegistrationSet,
        expected_output: ExpectedOutputSchema,
    ) -> Result<NativeSubmissionAttachment, DistributedQueryError> {
        let expected = self.keys.iter().copied().collect::<BTreeSet<_>>();
        let mut actual = BTreeSet::new();
        for submission in &submissions {
            if submission.execution_id() != self.execution_id {
                return Err(contract_error(
                    "native submission attachment execution id differs from sealed view",
                ));
            }
            let key = NativeSubmissionKey::new(
                submission.backend_idx(),
                submission.fragment_id(),
                submission.fragment_instance_id(),
            );
            if !actual.insert(key) {
                return Err(contract_error(format!(
                    "native submission attachment repeats placement key {key:?}"
                )));
            }
        }
        if actual != expected {
            let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
            let unknown = actual.difference(&expected).copied().collect::<Vec<_>>();
            return Err(contract_error(format!(
                "native submission attachment placement set mismatch: missing={missing:?} unknown={unknown:?}"
            )));
        }
        let root = NativeSubmissionKey::new(
            root_fetch.backend_idx(),
            root_fetch.fragment_id(),
            root_fetch.fragment_instance_id(),
        );
        if root != self.root {
            return Err(contract_error(
                "native submission attachment root metadata differs from sealed view",
            ));
        }
        Ok(NativeSubmissionAttachment {
            handoff_id: self.handoff_id,
            execution_id: self.execution_id,
            submissions,
            root_fetch,
            writer_registrations,
            expected_output,
        })
    }
}

/// Consuming, artifact-bound native submission payload.  Core validates this
/// attachment before it constructs lifecycle `StageBatch` values.
pub struct NativeSubmissionAttachment {
    handoff_id: u64,
    execution_id: QueryExecutionId,
    submissions: Vec<ValidatedNativeSubmission>,
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
}

impl NativeSubmissionAttachment {
    pub(crate) fn matches(&self, handoff_id: u64, execution_id: QueryExecutionId) -> bool {
        self.handoff_id == handoff_id && self.execution_id == execution_id
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<ValidatedNativeSubmission>,
        RootFetchMetadata,
        WriterRegistrationSet,
        ExpectedOutputSchema,
    ) {
        (
            self.submissions,
            self.root_fetch,
            self.writer_registrations,
            self.expected_output,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::AttemptId;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 9),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id")
    }

    #[test]
    fn view_rejects_duplicate_placement_key() {
        let key = NativeSubmissionKey::new(2, 3, UniqueId::new(5, 7));
        let error = NativeSubmissionEncodingView::new(1, execution_id(), vec![key, key], key)
            .err()
            .expect("duplicate key must be rejected");
        assert!(error.message().contains("repeats a sealed placement key"));
    }
}
