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

use std::fmt;

use crate::dml::model::{CommitOutcome, DmlOperationId, StatementNextAction};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlErrorKind {
    JournalUnavailable,
    JournalCorruption,
    JournalUnresolved,
    Executor,
    Commit,
    CommittedButUnfinalized,
    Admission,
}

#[derive(Debug)]
pub struct DmlError {
    kind: DmlErrorKind,
    message: String,
    operation_id: Option<DmlOperationId>,
    next_action: Option<StatementNextAction>,
    committed_outcome: Option<CommitOutcome>,
}

impl DmlError {
    pub(crate) fn new(kind: DmlErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
            operation_id: None,
            next_action: None,
            committed_outcome: None,
        }
    }

    pub(crate) fn journal_unavailable(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::JournalUnavailable, error)
    }

    pub(crate) fn journal_corruption(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::JournalCorruption, error)
    }

    pub(crate) fn journal_unresolved(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::JournalUnresolved, error)
    }

    pub(crate) fn executor(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Executor, error)
    }

    pub(crate) fn commit(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Commit, error)
    }

    pub(crate) fn with_operation_id(mut self, operation_id: DmlOperationId) -> Self {
        self.operation_id = Some(operation_id);
        self
    }

    pub(crate) fn with_next_action(mut self, next_action: StatementNextAction) -> Self {
        self.next_action = Some(next_action);
        self
    }

    pub(crate) fn committed_but_unfinalized(
        operation_id: DmlOperationId,
        committed_outcome: Option<CommitOutcome>,
        error: impl fmt::Display,
    ) -> Self {
        Self {
            kind: DmlErrorKind::CommittedButUnfinalized,
            message: format!("{error}; do not retry commit"),
            operation_id: Some(operation_id),
            next_action: Some(StatementNextAction::RetryFinalize),
            committed_outcome,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn admission(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Admission, error)
    }

    pub const fn kind(&self) -> DmlErrorKind {
        self.kind
    }

    pub const fn operation_id(&self) -> Option<DmlOperationId> {
        self.operation_id
    }

    pub const fn next_action(&self) -> Option<StatementNextAction> {
        self.next_action
    }

    pub const fn committed_outcome(&self) -> Option<&CommitOutcome> {
        self.committed_outcome.as_ref()
    }
}

impl fmt::Display for DmlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)?;
        if let Some(operation_id) = self.operation_id {
            write!(formatter, " (operation {operation_id})")?;
        }
        if let Some(next_action) = self.next_action {
            write!(formatter, " (next action {next_action:?})")?;
        }
        if let Some(outcome) = &self.committed_outcome {
            write!(
                formatter,
                " (known committed snapshot {})",
                outcome.new_snapshot_id
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for DmlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_kind_and_message() {
        let operation_id = DmlOperationId::new_v7();
        let error = DmlError::journal_unavailable("boom")
            .with_operation_id(operation_id)
            .with_next_action(StatementNextAction::ManualInspect);
        assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
        assert_eq!(error.operation_id(), Some(operation_id));
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        assert_eq!(
            error.to_string(),
            format!(
                "JournalUnavailable: boom (operation {operation_id}) (next action ManualInspect)"
            )
        );
    }
}
