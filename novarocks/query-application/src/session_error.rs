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

//! Query-session errors before a protocol adapter maps them to wire codes.

use std::{borrow::Cow, fmt};

use crate::api::StatementExternalEffectDisposition;
use novarocks_spi::connector::{LakePublicationDisposition, LakePublicationTerminal};
use novarocks_user_error::UserError;

/// Render an execution error at the Query Application boundary. Protocol
/// adapters consume this projection without rebuilding the selected input.
pub fn execution_public_message(error: &crate::api::QueryExecutionError) -> Cow<'_, str> {
    match error.execution_failure() {
        Some(input) => Cow::Owned(input.user_error().to_string()),
        None => Cow::Borrowed(error.message()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryServiceErrorKind {
    Parse,
    BadDatabase,
    Unsupported,
    PermissionDenied,
    NoSuchSession,
    Interrupted,
    Timeout,
    InvalidValue,
    Unavailable,
    /// The role-local serving lifecycle has irreversibly closed workload admission.
    FrontendDraining,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QueryServiceErrorBody {
    Message {
        kind: QueryServiceErrorKind,
        message: String,
    },
    User {
        error: UserError,
        message: String,
    },
    Execution {
        input: Box<crate::execution_error::ExecutionFailureInput>,
        user_error: UserError,
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryServiceError {
    failure_cause: Option<novarocks_failure_contract::SharedFailureCause>,
    body: QueryServiceErrorBody,
    publication_terminal: Option<LakePublicationTerminal>,
    external_effect_disposition: Option<StatementExternalEffectDisposition>,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    compile_provenance: Option<novarocks_type_contract::CompileControlProvenance>,
}

impl QueryServiceError {
    pub const FRONTEND_DRAINING_MESSAGE: &'static str =
        "FRONTEND_DRAINING: frontend is draining; retry on another frontend";

    pub fn frontend_draining() -> Self {
        Self::new(
            QueryServiceErrorKind::FrontendDraining,
            Self::FRONTEND_DRAINING_MESSAGE,
        )
    }

    pub fn new(kind: QueryServiceErrorKind, message: impl Into<String>) -> Self {
        Self {
            body: QueryServiceErrorBody::Message {
                kind,
                message: message.into(),
            },
            publication_terminal: None,
            external_effect_disposition: None,
            compile_control: None,
            compile_provenance: None,
            failure_cause: None,
        }
    }

    /// Retains the domain descriptor and rendering without inventing a phase.
    pub fn from_user_error(error: UserError) -> Self {
        let message = error.to_string();
        Self {
            body: QueryServiceErrorBody::User { error, message },
            publication_terminal: None,
            external_effect_disposition: None,
            compile_control: None,
            compile_provenance: None,
            failure_cause: None,
        }
    }

    /// Carries the selected execution fact, safe detail and site as one sum.
    /// User-visible rendering is derived here; it grants no new acceptance.
    pub fn from_execution_failure(input: crate::execution_error::ExecutionFailureInput) -> Self {
        let user_error = input.user_error();
        let message = user_error.to_string();
        Self {
            body: QueryServiceErrorBody::Execution {
                input: Box::new(input),
                user_error,
                message,
            },
            publication_terminal: None,
            external_effect_disposition: None,
            compile_control: None,
            compile_provenance: None,
            failure_cause: None,
        }
    }

    /// Preserve the originating domain and use its frozen fact and safe detail.
    pub fn from_failure_cause(cause: novarocks_failure_contract::SharedFailureCause) -> Self {
        use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite};
        use novarocks_failure_contract::FailureProjection;
        let mut failure = Self::from_execution_failure(ExecutionFailureInput::new(
            cause.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating(cause.detail()),
            ExecutionFailureSite::Coordinator,
        ));
        failure.failure_cause = Some(cause);
        failure
    }
    pub fn failure_cause(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.failure_cause.as_ref()
    }

    pub fn execution_failure(&self) -> Option<&crate::execution_error::ExecutionFailureInput> {
        match &self.body {
            QueryServiceErrorBody::Execution { input, .. } => Some(input.as_ref()),
            QueryServiceErrorBody::Message { .. } | QueryServiceErrorBody::User { .. } => None,
        }
    }

    pub fn with_publication_terminal(
        message: impl Into<String>,
        terminal: LakePublicationTerminal,
    ) -> Self {
        Self {
            body: QueryServiceErrorBody::Message {
                kind: QueryServiceErrorKind::Internal,
                message: message.into(),
            },
            publication_terminal: Some(terminal),
            external_effect_disposition: None,
            compile_control: None,
            compile_provenance: None,
            failure_cause: None,
        }
    }

    /// Keep the selected input and descriptor unchanged while exposing an
    /// independent publication verdict that forbids retry. Provider and target
    /// diagnostics remain in the terminal sidecar.
    pub fn attach_publication_terminal(mut self, terminal: LakePublicationTerminal) -> Self {
        if let QueryServiceErrorBody::Execution {
            user_error,
            message,
            ..
        } = &mut self.body
        {
            // Always render from the frozen descriptor, so attaching a newer
            // publication verdict cannot duplicate a previous public suffix.
            *message = user_error.to_string();
            let disposition = match terminal.disposition() {
                LakePublicationDisposition::KnownUncommitted => None,
                LakePublicationDisposition::CommitUnknown => Some("CommitUnknown"),
                LakePublicationDisposition::KnownCommitted => Some("KnownCommitted"),
            };
            if let Some(disposition) = disposition {
                use std::fmt::Write;
                let header = terminal.header();
                write!(
                    message,
                    " (lake publication id={} family={} disposition={} do_not_retry=true; do not retry statement)",
                    header.publication_id(),
                    header.family().as_str(),
                    disposition,
                )
                .expect("writing to a String cannot fail");
            }
        }
        self.publication_terminal = Some(terminal);
        self
    }

    /// Forward only the original action owner's effect knowledge.
    /// A disposition alone grants no domain identity, receipt, or exit proof.
    /// The selected input, descriptor and phase are retained without rewriting.
    pub fn attach_external_effect_disposition(
        mut self,
        disposition: StatementExternalEffectDisposition,
    ) -> Self {
        // This is one original owner's frozen terminal verdict, not a new
        // observation/update channel. Reapplying it cannot duplicate rendering.
        if self.external_effect_disposition.is_some() {
            return self;
        }
        let message = match &mut self.body {
            QueryServiceErrorBody::Message { message, .. }
            | QueryServiceErrorBody::User { message, .. }
            | QueryServiceErrorBody::Execution { message, .. } => message,
        };
        let label = match disposition {
            StatementExternalEffectDisposition::KnownUncommitted => None,
            StatementExternalEffectDisposition::CommitUnknown => Some("CommitUnknown"),
            StatementExternalEffectDisposition::KnownCommitted => Some("KnownCommitted"),
        };
        if let Some(label) = label {
            use std::fmt::Write;
            write!(
                message,
                " (external effect disposition={label} do_not_retry=true; do not retry statement)"
            )
            .expect("writing to a String cannot fail");
        }
        self.external_effect_disposition = Some(disposition);
        self
    }

    pub fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.external_effect_disposition
    }

    pub fn with_compile_control(
        mut self,
        cause: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_provenance = cause.as_ref().map(|cause| cause.provenance());
        self.compile_control = cause;
        self
    }
    pub fn from_compile_control(error: novarocks_type_contract::CompileControlError) -> Self {
        use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite};
        use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
        use novarocks_type_contract::CompileControlError;

        // A pure compile stop observes its caller, not accepted Query cancellation.
        // Source diagnostics remain in the rich sidecar; only owner-authored safe
        // descriptions enter the client-facing canonical input.
        let detail = match &error {
            CompileControlError::Cancelled => "pure compilation observed a caller stop",
            CompileControlError::DeadlineExceeded => "pure compilation observed a caller deadline",
            CompileControlError::ResourceExhausted => "pure compilation work budget was exhausted",
            CompileControlError::Source(_) => "pure compilation failed in its source domain",
        };
        let mut failure = Self::from_execution_failure(ExecutionFailureInput::new(
            error.failure_fact(),
            SafeDetail::truncating(detail),
            ExecutionFailureSite::Coordinator,
        ));
        failure.compile_provenance = Some(error.provenance());
        failure.compile_control = Some(error);
        failure
    }
    /// Storage-only geometry for the existing execution projection followed
    /// by attach_publication_terminal. The caller pays real target String
    /// lengths separately. No Provider diagnostics or source graphs enter it.
    pub fn bounded_publication_projection_backing_bytes(
        family: novarocks_spi::connector::LakePublicationFamily,
        detail_bytes: usize,
    ) -> Result<usize, novarocks_workload_control::WorkError> {
        use crate::execution_error::{EXECUTION_ERROR_CODE_DESCRIPTORS, ExecutionFailureInput};
        use novarocks_workload_control::WorkError;
        let code = EXECUTION_ERROR_CODE_DESCRIPTORS
            .iter()
            .map(|d| d.code.as_str().len())
            .max()
            .unwrap_or(0);
        // UUID display is exactly 36 bytes. The original caller supplies
        // the publication family; disposition uses the
        // longest existing label. Target text is not rendered in this suffix.
        let suffix = " (lake publication id=".len()
            + 36
            + " family=".len()
            + family.as_str().len()
            + " disposition=KnownCommitted do_not_retry=true; do not retry statement)".len();
        let rendered = code
            .checked_add(3)
            .and_then(|n| n.checked_add(detail_bytes))
            .and_then(|n| n.checked_add(suffix))
            .ok_or(WorkError::ArithmeticOverflow)?;
        // Existing String formatting can double RawVec capacity. Count the
        // old rendering, reset rendering, and one reallocation peak together;
        // the two exact detail copies belong to SafeDetail and UserError.
        rendered
            .max(8)
            .checked_mul(6)
            .and_then(|n| detail_bytes.checked_mul(2).and_then(|d| n.checked_add(d)))
            .and_then(|n| n.checked_add(std::mem::size_of::<ExecutionFailureInput>()))
            .ok_or(WorkError::ArithmeticOverflow)
    }
    /// A historical fact carries its real compiler provenance without keeping
    /// the original Provider tree alive or manufacturing a rich Source error.
    pub fn with_compile_control_provenance(
        mut self,
        provenance: Option<novarocks_type_contract::CompileControlProvenance>,
    ) -> Self {
        self.compile_provenance = provenance;
        self
    }
    pub fn compile_control_provenance(
        &self,
    ) -> Option<novarocks_type_contract::CompileControlProvenance> {
        self.compile_provenance
    }
    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }

    pub fn kind(&self) -> QueryServiceErrorKind {
        match &self.body {
            QueryServiceErrorBody::Message { kind, .. } => *kind,
            QueryServiceErrorBody::User { error, .. }
                if error.phase() != novarocks_user_error::ErrorPhase::Execution =>
            {
                QueryServiceErrorKind::Parse
            }
            QueryServiceErrorBody::User { .. } => QueryServiceErrorKind::Internal,
            QueryServiceErrorBody::Execution { input, .. } => match input.error_kind() {
                crate::api::QueryExecutionErrorKind::Cancelled => {
                    QueryServiceErrorKind::Interrupted
                }
                crate::api::QueryExecutionErrorKind::DeadlineExceeded => {
                    QueryServiceErrorKind::Timeout
                }
                crate::api::QueryExecutionErrorKind::Rejected => QueryServiceErrorKind::Unavailable,
                crate::api::QueryExecutionErrorKind::InvalidRequest
                | crate::api::QueryExecutionErrorKind::Failed => QueryServiceErrorKind::Internal,
            },
        }
    }

    pub fn message(&self) -> &str {
        match &self.body {
            QueryServiceErrorBody::Message { message, .. }
            | QueryServiceErrorBody::User { message, .. }
            | QueryServiceErrorBody::Execution { message, .. } => message,
        }
    }

    pub fn user_error(&self) -> Option<&UserError> {
        match &self.body {
            QueryServiceErrorBody::User { error, .. } => Some(error),
            QueryServiceErrorBody::Execution { user_error, .. } => Some(user_error),
            QueryServiceErrorBody::Message { .. } => None,
        }
    }

    pub fn publication_terminal(&self) -> Option<&LakePublicationTerminal> {
        self.publication_terminal.as_ref()
    }
}

impl fmt::Display for QueryServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for QueryServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failure_cause
            .as_ref()
            .map(|cause| cause as &dyn std::error::Error)
            .or_else(|| {
                self.compile_control
                    .as_ref()
                    .map(|cause| cause as &dyn std::error::Error)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_control_resource_and_source_keep_their_author_facts_and_rich_sidecars() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        for cause in [
            CompileControlError::ResourceExhausted,
            CompileControlError::from_domain(LayoutResourceError::Arithmetic),
        ] {
            let expected = cause.failure_fact();
            let service = QueryServiceError::from_compile_control(cause.clone());
            let input = service.execution_failure().unwrap();
            assert_eq!(input.fact(), expected);
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1041, "HY000")
            );
            assert_eq!(service.compile_control_error(), Some(cause));
            assert_eq!(
                service.user_error().unwrap().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(service.user_error().unwrap().location(), None);
        }
    }

    #[test]
    fn pure_compile_stop_observations_do_not_claim_accepted_query_cancellation() {
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        use novarocks_type_contract::CompileControlError;
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
        ] {
            let expected = cause.failure_fact();
            let service = QueryServiceError::from_compile_control(cause.clone());
            let input = service.execution_failure().unwrap();
            assert_eq!(input.fact(), expected);
            assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1105, "HY000")
            );
            assert_eq!(
                input.error_kind(),
                crate::api::QueryExecutionErrorKind::Failed
            );
            assert_eq!(service.compile_control_error(), Some(cause));
            assert_eq!(
                service.user_error().unwrap().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(service.user_error().unwrap().location(), None);
        }
    }

    #[test]
    fn typed_error_preserves_kind_and_message() {
        let error = QueryServiceError::new(QueryServiceErrorKind::Timeout, "deadline elapsed");
        assert_eq!(error.kind(), QueryServiceErrorKind::Timeout);
        assert_eq!(error.message(), "deadline elapsed");
        assert!(error.user_error().is_none());
    }

    #[test]
    fn frontend_draining_error_uses_the_fixed_retry_message() {
        let error = QueryServiceError::frontend_draining();
        assert_eq!(error.kind(), QueryServiceErrorKind::FrontendDraining);
        assert_eq!(
            error.message(),
            "FRONTEND_DRAINING: frontend is draining; retry on another frontend"
        );
    }

    #[test]
    fn parser_user_error_is_preserved_without_message_classification() {
        let parser_error = novarocks_parser::parse("SHOW")
            .expect_err("incomplete SHOW command must be a parser error")
            .to_user_error("SHOW");
        let error = QueryServiceError::from_user_error(parser_error.clone());

        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
        assert_eq!(error.user_error(), Some(&parser_error));
        assert_eq!(error.message(), parser_error.to_string());
    }
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn service_terminal_preserves_control_without_inventing_query_cancellation() {
        for (control, kind) in [
            (
                CompileControlError::Cancelled,
                QueryServiceErrorKind::Internal,
            ),
            (
                CompileControlError::DeadlineExceeded,
                QueryServiceErrorKind::Internal,
            ),
            (
                CompileControlError::ResourceExhausted,
                QueryServiceErrorKind::Unavailable,
            ),
        ] {
            let error = QueryServiceError::from_compile_control(control.clone());
            use novarocks_execution_contract::failure::FailureProjection;
            assert_eq!(error.kind(), kind);
            let input = error.execution_failure().unwrap();
            assert_eq!(input.fact(), control.failure_fact());
            let expected_errno = if control == CompileControlError::ResourceExhausted {
                1041
            } else {
                1105
            };
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (expected_errno, "HY000")
            );
            assert_eq!(error.compile_control_error(), Some(control.clone()));
            assert_eq!(
                QueryServiceError::new(kind, control.to_string()).compile_control_error(),
                None
            );
        }
    }
}

#[cfg(test)]
mod publication_message_tests {
    use super::*;
    use crate::{
        api::{QueryExecutionError, QueryExecutionErrorKind},
        execution_error::{ExecutionFailureInput, ExecutionFailureSite},
    };
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, LakePublicationFamily, LakePublicationId,
        LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationStatementTag,
        LakePublicationTarget,
    };

    fn selected_input() -> ExecutionFailureInput {
        let source = ConnectorError::new(
            ConnectorErrorKind::Internal,
            "private provider diagnostic must not enter SQL",
        );
        ExecutionFailureInput::new(
            source.failure_fact(),
            SafeDetail::new("writer execution failed").unwrap(),
            ExecutionFailureSite::Coordinator,
        )
    }

    fn publication(disposition: LakePublicationDisposition) -> LakePublicationTerminal {
        LakePublicationTerminal::new(
            LakePublicationMarkerHeader::new(
                LakePublicationId::new_v7(),
                LakePublicationFamily::Write,
            ),
            LakePublicationTarget::try_new(
                "private-catalog".to_string(),
                "private-namespace".to_string(),
                Some("private-table".to_string()),
                Some("private-reference".to_string()),
            )
            .unwrap(),
            disposition,
            if disposition.do_not_retry() {
                LakePublicationNextAction::InspectPublishedState
            } else {
                LakePublicationNextAction::RetryStatement
            },
            Some(LakePublicationStatementTag::try_new("private-tag".to_string()).unwrap()),
        )
    }

    #[test]
    fn known_uncommitted_keeps_the_complete_canonical_message() {
        let input = selected_input();
        let expected_user_error = input.user_error();
        let expected_message = "[query.execution.unexpected] writer execution failed";
        let selected = QueryServiceError::from_execution_failure(input.clone());
        assert_eq!(selected.message(), expected_message);
        let terminal = publication(LakePublicationDisposition::KnownUncommitted);
        let service = selected.attach_publication_terminal(terminal.clone());
        assert_eq!(service.message(), expected_message);
        assert_eq!(service.execution_failure(), Some(&input));
        assert_eq!(service.user_error(), Some(&expected_user_error));
        assert_eq!(service.publication_terminal(), Some(&terminal));
        assert!(!terminal.do_not_retry());
    }

    #[test]
    fn non_retryable_publication_keeps_selected_input_and_descriptor() {
        for (disposition, name) in [
            (LakePublicationDisposition::KnownCommitted, "KnownCommitted"),
            (LakePublicationDisposition::CommitUnknown, "CommitUnknown"),
        ] {
            let input = selected_input();
            let expected_user_error = input.user_error();
            let terminal = publication(disposition);
            let expected_message = format!(
                "[query.execution.unexpected] writer execution failed (lake publication id={} family=write disposition={} do_not_retry=true; do not retry statement)",
                terminal.header().publication_id(),
                name,
            );
            let service = QueryServiceError::from_execution_failure(input.clone())
                .attach_publication_terminal(terminal.clone());
            assert_eq!(service.message(), expected_message);
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(service.user_error(), Some(&expected_user_error));
            assert_eq!(service.publication_terminal(), Some(&terminal));
            assert!(terminal.do_not_retry());
            assert_eq!(service.kind(), QueryServiceErrorKind::Internal);
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1105, "HY000")
            );
            assert_eq!(
                input.descriptor().descriptor.code.as_str(),
                "query.execution.unexpected"
            );
            assert_eq!(input.detail().as_str(), "writer execution failed");
            for private in [
                "private provider diagnostic",
                "private-catalog",
                "private-namespace",
                "private-table",
                "private-reference",
                "private-tag",
            ] {
                assert!(!service.message().contains(private));
            }
            // The public suffix belongs to this verdict, not the immutable
            // execution detail, and reattachment cannot accumulate suffixes.
            let repeated = service.attach_publication_terminal(terminal);
            assert_eq!(repeated.message(), expected_message);
            assert_eq!(repeated.execution_failure(), Some(&input));
        }
    }

    #[test]
    fn legacy_publication_message_and_streaming_projection_keep_their_contracts() {
        let legacy = QueryServiceError::new(QueryServiceErrorKind::Internal, "legacy terminal")
            .attach_publication_terminal(publication(LakePublicationDisposition::CommitUnknown));
        assert_eq!(legacy.message(), "legacy terminal");
        assert!(legacy.execution_failure().is_none());
        let input = selected_input();
        let streaming = QueryExecutionError::from_execution(input.clone());
        assert_eq!(streaming.message(), "writer execution failed");
        assert_eq!(
            execution_public_message(&streaming),
            "[query.execution.unexpected] writer execution failed"
        );
        assert_eq!(streaming.execution_failure(), Some(&input));
        let application = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "application");
        assert!(matches!(
            execution_public_message(&application),
            Cow::Borrowed("application")
        ));
    }
}

#[cfg(test)]
mod external_effect_companion_tests {
    use super::*;
    #[test]
    fn external_effect_neither_reauthors_cause_nor_invents_publication_identity() {
        let original = QueryServiceError::from_compile_control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        );
        let input = original.execution_failure().unwrap().clone();
        for disposition in [
            StatementExternalEffectDisposition::KnownUncommitted,
            StatementExternalEffectDisposition::CommitUnknown,
            StatementExternalEffectDisposition::KnownCommitted,
        ] {
            let service = original
                .clone()
                .attach_external_effect_disposition(disposition);
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(service.user_error(), original.user_error());
            assert_eq!(service.kind(), original.kind());
            assert_eq!(service.external_effect_disposition(), Some(disposition));
            assert_eq!(service.publication_terminal(), None);
            assert!(!service.message().contains("maintenance publication"));
            assert!(!service.message().contains("lake publication"));
            assert_eq!(
                service.message().contains("do not retry statement"),
                disposition.do_not_retry()
            );
            let repeated = service
                .clone()
                .attach_external_effect_disposition(disposition);
            assert_eq!(repeated.message(), service.message());
            assert_eq!(repeated.execution_failure(), Some(&input));
        }
    }
}
