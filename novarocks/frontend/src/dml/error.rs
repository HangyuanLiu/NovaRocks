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

use crate::common::engine_error::EngineError;
use novarocks_parser::Span;
use novarocks_spi::connector::{
    ConnectorWriteReceipt, LakePublicationDisposition, LakePublicationFamily,
    LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationTarget,
    LakePublicationTerminal,
};
use novarocks_sql::analyze_error::AnalyzeError;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

use crate::dml::model::{DmlOperationId, StatementNextAction};

const ADMIT_DELETE_REQUIRES_WHERE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.delete_requires_where"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_DELETE_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.delete_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_UPDATE_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.update_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_MERGE_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.merge_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_INSERT_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.insert_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_CREATE_TABLE_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.create_table_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

/// DML capability descriptors, exported only for the independent manifest tool.
pub const ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    ADMIT_DELETE_REQUIRES_WHERE,
    ADMIT_DELETE_UNSUPPORTED_FORM,
    ADMIT_UPDATE_UNSUPPORTED_FORM,
    ADMIT_MERGE_UNSUPPORTED_FORM,
    ADMIT_INSERT_UNSUPPORTED_FORM,
    ADMIT_CREATE_TABLE_UNSUPPORTED_FORM,
];

/// Capability failures are owned by the frontend DML application, never by the parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmitError {
    DeleteRequiresWhere,
    #[allow(
        dead_code,
        reason = "The manifest keeps this typed capability code while parser validation owns the form rejection."
    )]
    DeleteUnsupportedForm,
    UpdateUnsupportedForm,
    MergeUnsupportedForm,
    InsertUnsupportedForm,
    CreateTableUnsupportedForm,
}

impl AdmitError {
    const fn descriptor(self) -> ErrorCodeDescriptor {
        match self {
            Self::DeleteRequiresWhere => ADMIT_DELETE_REQUIRES_WHERE,
            Self::DeleteUnsupportedForm => ADMIT_DELETE_UNSUPPORTED_FORM,
            Self::UpdateUnsupportedForm => ADMIT_UPDATE_UNSUPPORTED_FORM,
            Self::MergeUnsupportedForm => ADMIT_MERGE_UNSUPPORTED_FORM,
            Self::InsertUnsupportedForm => ADMIT_INSERT_UNSUPPORTED_FORM,
            Self::CreateTableUnsupportedForm => ADMIT_CREATE_TABLE_UNSUPPORTED_FORM,
        }
    }

    pub(crate) fn to_user_error(
        self,
        source: &str,
        span: Span,
        message: impl Into<String>,
    ) -> UserError {
        UserError::from_descriptor(
            self.descriptor(),
            message,
            Some(span.to_user_error_location(source)),
            RetryClass::Never,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlErrorKind {
    JournalUnavailable,
    JournalCorruption,
    JournalUnresolved,
    Executor,
    Commit,
    CommittedButUnfinalized,
    Admission,
    CoordinationContended,
    CoordinationLost,
    CoordinationUnresolved,
}

#[derive(Debug)]
pub struct DmlError {
    kind: DmlErrorKind,
    message: String,
    operation_id: Option<DmlOperationId>,
    next_action: Option<StatementNextAction>,
    committed_receipt: Option<Box<ConnectorWriteReceipt>>,
    publication_terminal: Option<LakePublicationTerminal>,
    user_error: Option<UserError>,
    engine_error: Option<EngineError>,
}

/// DML-local carrier for a SQL analysis error before the frontend client
/// boundary renders it as a [`UserError`].
///
/// The carrier intentionally keeps non-analysis failures as opaque engine
/// text. It must not infer a user-facing code from that text.
#[derive(Debug)]
pub enum DmlExecutionError {
    Engine(String),
    Analyze(AnalyzeError),
}

impl DmlExecutionError {
    pub(crate) fn from_compile(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        match error {
            novarocks_sql::compiler::SqlCompileError::Analyze(error) => Self::Analyze(error),
            error => Self::Engine(error.to_string()),
        }
    }

    pub(crate) fn into_dml_error(self, source: Option<&str>) -> DmlError {
        match self {
            Self::Engine(error) => DmlError::executor(error),
            Self::Analyze(error) => DmlError::admit(error.to_user_error(source)),
        }
    }
}

impl From<String> for DmlExecutionError {
    fn from(error: String) -> Self {
        Self::Engine(error)
    }
}

impl From<&str> for DmlExecutionError {
    fn from(error: &str) -> Self {
        Self::Engine(error.to_string())
    }
}

impl From<crate::query_execution::planning::time_travel::TimeTravelRewriteError>
    for DmlExecutionError
{
    fn from(error: crate::query_execution::planning::time_travel::TimeTravelRewriteError) -> Self {
        match error {
            crate::query_execution::planning::time_travel::TimeTravelRewriteError::Engine(
                error,
            ) => Self::Engine(error),
            crate::query_execution::planning::time_travel::TimeTravelRewriteError::Analyze(
                error,
            ) => Self::Analyze(error),
        }
    }
}

impl std::fmt::Display for DmlExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(error) => formatter.write_str(error),
            Self::Analyze(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DmlExecutionError {}

impl DmlError {
    pub(crate) fn new(kind: DmlErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
            operation_id: None,
            next_action: None,
            committed_receipt: None,
            publication_terminal: None,
            user_error: None,
            engine_error: None,
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

    pub(crate) fn with_publication_context(
        mut self,
        family: LakePublicationFamily,
        catalog: String,
        namespace: String,
        table: String,
        reference: Option<String>,
    ) -> Self {
        let Some(publication_id) = self.operation_id else {
            return self;
        };
        let Ok(target) = LakePublicationTarget::try_new(catalog, namespace, Some(table), reference)
        else {
            return self;
        };
        let disposition = match self.kind {
            DmlErrorKind::CommittedButUnfinalized => LakePublicationDisposition::KnownCommitted,
            DmlErrorKind::CoordinationUnresolved => LakePublicationDisposition::CommitUnknown,
            _ => LakePublicationDisposition::KnownUncommitted,
        };
        self.publication_terminal = Some(LakePublicationTerminal::new(
            LakePublicationMarkerHeader::new(publication_id, family),
            target,
            disposition,
            if disposition.do_not_retry() {
                LakePublicationNextAction::InspectPublishedState
            } else {
                LakePublicationNextAction::RetryStatement
            },
            None,
        ));
        self
    }

    pub(crate) fn committed_but_unfinalized(
        operation_id: DmlOperationId,
        committed_receipt: Option<ConnectorWriteReceipt>,
        error: impl fmt::Display,
    ) -> Self {
        Self {
            kind: DmlErrorKind::CommittedButUnfinalized,
            message: format!("{error}; do not retry commit"),
            operation_id: Some(operation_id),
            next_action: Some(StatementNextAction::RetryFinalize),
            committed_receipt: committed_receipt.map(Box::new),
            publication_terminal: None,
            user_error: None,
            engine_error: None,
        }
    }

    pub(crate) fn committed_outcome_not_durable(
        operation_id: DmlOperationId,
        committed_receipt: ConnectorWriteReceipt,
        error: impl fmt::Display,
    ) -> Self {
        Self {
            kind: DmlErrorKind::CoordinationUnresolved,
            message: format!(
                "provider returned a known-committed outcome but the durable journal write failed: {error}; do not retry commit"
            ),
            operation_id: Some(operation_id),
            next_action: Some(StatementNextAction::ManualInspect),
            committed_receipt: Some(Box::new(committed_receipt)),
            publication_terminal: None,
            user_error: None,
            engine_error: None,
        }
    }

    pub(crate) fn ambiguous_outcome_not_durable(
        operation_id: DmlOperationId,
        error: impl fmt::Display,
    ) -> Self {
        Self {
            kind: DmlErrorKind::CoordinationUnresolved,
            message: format!(
                "provider outcome is ambiguous and could not be recorded as durable terminal truth: {error}; do not retry commit"
            ),
            operation_id: Some(operation_id),
            next_action: Some(StatementNextAction::ManualInspect),
            committed_receipt: None,
            publication_terminal: None,
            user_error: None,
            engine_error: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn admission(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Admission, error)
    }

    /// Carries an admission error across DML boundaries without losing its code or location.
    pub(crate) fn admit(error: UserError) -> Self {
        Self {
            kind: DmlErrorKind::Admission,
            message: error.to_string(),
            operation_id: None,
            next_action: None,
            committed_receipt: None,
            publication_terminal: None,
            user_error: Some(error),
            engine_error: None,
        }
    }

    pub(crate) fn coordination_contended(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::CoordinationContended, error)
    }

    pub(crate) fn coordination_lost(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::CoordinationLost, error)
    }

    pub(crate) fn coordination_unresolved(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::CoordinationUnresolved, error)
    }

    /// Carries an explicit engine-level outcome to the SQL boundary.  DML
    /// must never reconstruct these codes by parsing its own display text.
    pub(crate) fn with_engine_error(mut self, error: EngineError) -> Self {
        self.engine_error = Some(error);
        self
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

    pub fn committed_receipt(&self) -> Option<&ConnectorWriteReceipt> {
        self.committed_receipt.as_deref()
    }

    pub fn user_error(&self) -> Option<&UserError> {
        self.user_error.as_ref()
    }

    pub(crate) fn engine_error(&self) -> Option<&EngineError> {
        self.engine_error.as_ref()
    }

    pub fn publication_terminal(&self) -> Option<&LakePublicationTerminal> {
        self.publication_terminal.as_ref()
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
        if self.committed_receipt.is_some() {
            write!(formatter, " (known committed connector write)")?;
        }
        if let Some(terminal) = &self.publication_terminal {
            let target = terminal.target();
            write!(
                formatter,
                " (lake publication id={} family={} target={}.{}{}{} disposition={:?} next_action={:?} do_not_retry={})",
                terminal.header().publication_id(),
                terminal.header().family(),
                target.catalog(),
                target.namespace(),
                target
                    .table()
                    .map(|table| format!(".{table}"))
                    .unwrap_or_default(),
                target
                    .reference()
                    .map(|reference| format!("@{reference}"))
                    .unwrap_or_default(),
                terminal.disposition(),
                terminal.next_action(),
                terminal.do_not_retry(),
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for DmlError {}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn admit_descriptor_registry_is_unique_and_frontend_owned() {
        let codes = ERROR_CODE_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.code)
            .collect::<HashSet<_>>();

        assert_eq!(codes.len(), ERROR_CODE_DESCRIPTORS.len());
        assert!(
            ERROR_CODE_DESCRIPTORS
                .iter()
                .all(|descriptor| descriptor.phase == ErrorPhase::Admit)
        );
    }

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

    #[test]
    fn admit_error_preserves_the_typed_user_error() {
        let error = AdmitError::DeleteRequiresWhere.to_user_error(
            "DELETE FROM t",
            Span::new(0, 6),
            "DELETE requires a WHERE clause",
        );
        let dml_error = DmlError::admit(error.clone());

        assert_eq!(dml_error.kind(), DmlErrorKind::Admission);
        assert_eq!(dml_error.user_error(), Some(&error));
        assert_eq!(error.code().as_str(), "sql.admit.delete_requires_where");
        assert_eq!(error.phase(), ErrorPhase::Admit);
        assert_eq!(error.location().map(|location| location.column()), Some(1));
    }

    #[test]
    fn publication_context_projects_unknown_without_granting_retry() {
        let publication_id = DmlOperationId::new_v7();
        let error = DmlError::coordination_unresolved("commit response was lost")
            .with_operation_id(publication_id)
            .with_publication_context(
                LakePublicationFamily::DataMutation,
                "ice".to_string(),
                "db".to_string(),
                "t".to_string(),
                Some("audit".to_string()),
            );

        let terminal = error.publication_terminal().expect("terminal projection");
        assert_eq!(terminal.header().publication_id(), publication_id);
        assert_eq!(
            terminal.header().family(),
            LakePublicationFamily::DataMutation
        );
        assert_eq!(terminal.target().catalog(), "ice");
        assert_eq!(terminal.target().namespace(), "db");
        assert_eq!(terminal.target().table(), Some("t"));
        assert_eq!(terminal.target().reference(), Some("audit"));
        assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::CommitUnknown
        );
        assert!(terminal.do_not_retry());
        assert!(error.to_string().contains(&publication_id.to_string()));
    }
}
