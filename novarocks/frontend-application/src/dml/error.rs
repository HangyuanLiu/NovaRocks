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

use novarocks_query_application::engine_error::EngineErrorCode;
use novarocks_query_application::execution_error::ExecutionFailureInput;
use novarocks_spi::connector::{LakePublicationDisposition, LakePublicationTerminal};
use novarocks_sql::analyze_error::AnalyzeError;
use novarocks_user_error::UserError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlErrorKind {
    Executor,
    Commit,
    CommittedButUnfinalized,
    Admission,
}

#[derive(Debug)]
enum DmlErrorBody {
    Legacy(String),
    Execution(Box<ExecutionFailureInput>),
    Application(novarocks_query_application::session_error::QueryServiceError),
}

#[derive(Debug)]
pub struct DmlError {
    kind: DmlErrorKind,
    body: DmlErrorBody,
    publication_terminal: Option<LakePublicationTerminal>,
    user_error: Option<UserError>,
    engine_error_code: Option<EngineErrorCode>,
    cow_failure: Option<crate::query_execution::dml::mutation_flow::CowFailure>,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    staged_create_preparation: Option<Box<DmlStagedCreateFailure>>,
}

/// DML-local carrier for a SQL analysis error before the frontend client
/// boundary renders it as a [`UserError`].
///
/// COW begin failures keep their original typed cause until the original
/// command worker retires it. Governed application refusals retain their original
/// QueryServiceError. Other failures retain their existing text route;
/// no user-facing code is inferred from that text.
#[derive(Debug)]
pub enum DmlExecutionError {
    Application(novarocks_query_application::session_error::QueryServiceError),
    Execution(ExecutionFailureInput),
    Engine(String),
    Compilation(novarocks_failure_contract::SharedFailureCause),
    Analyze(AnalyzeError),
    Cow(crate::query_execution::dml::mutation_flow::CowFailure),
    Control(novarocks_type_contract::CompileControlError),
}

impl DmlExecutionError {
    pub(crate) fn from_distributed(
        error: crate::query_execution::contract::DistributedQueryError,
    ) -> Self {
        if let Some(input) = error.execution_failure() {
            return Self::Execution(input.clone());
        }
        if let Some(control) = error.compile_control_error() {
            return Self::Control(control);
        }
        Self::Execution(error.into_execution_failure_input())
    }

    pub(crate) fn from_compile(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        match error {
            novarocks_sql::compiler::SqlCompileError::Analyze(error) => match error.control_error()
            {
                Some(control) => Self::Control(control),
                None => Self::Analyze(error),
            },
            novarocks_sql::compiler::SqlCompileError::Cancelled => {
                Self::Control(novarocks_type_contract::CompileControlError::Cancelled)
            }
            novarocks_sql::compiler::SqlCompileError::DeadlineExceeded => {
                Self::Control(novarocks_type_contract::CompileControlError::DeadlineExceeded)
            }
            novarocks_sql::compiler::SqlCompileError::ResourceExhausted => {
                Self::Control(novarocks_type_contract::CompileControlError::ResourceExhausted)
            }
            novarocks_sql::compiler::SqlCompileError::CompilationSource(source) => {
                Self::Compilation(source)
            }
            error => Self::Engine(error.to_string()),
        }
    }

    pub(crate) fn from_encode(error: novarocks_plan_codec::PhysicalEncodeError) -> Self {
        match error {
            novarocks_plan_codec::PhysicalEncodeError::Control(error) => Self::Control(error),
            other => Self::Compilation(novarocks_failure_contract::SharedFailureCause::new(other)),
        }
    }

    pub(crate) fn into_dml_error(self, source: Option<&str>) -> DmlError {
        match self {
            Self::Execution(input) => DmlError::from_execution(input),
            Self::Application(error) => DmlError::from_application(error),
            Self::Engine(error) => DmlError::executor(error),
            Self::Compilation(source) => DmlError::compile_control(
                novarocks_type_contract::CompileControlError::Source(source),
            ),
            Self::Analyze(error) => match error.control_error() {
                Some(error) => DmlError::compile_control(error),
                None => DmlError::admit(error.to_user_error(source)),
            },
            Self::Control(error) => DmlError::compile_control(error),
            Self::Cow(error) => DmlError::cow(error),
        }
    }
}

impl From<novarocks_sql::compiler::SqlCompileError> for DmlExecutionError {
    fn from(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        Self::from_compile(error)
    }
}
impl From<novarocks_plan_codec::PhysicalEncodeError> for DmlExecutionError {
    fn from(error: novarocks_plan_codec::PhysicalEncodeError) -> Self {
        Self::from_encode(error)
    }
}

impl From<crate::query_execution::contract::DistributedQueryError> for DmlExecutionError {
    fn from(error: crate::query_execution::contract::DistributedQueryError) -> Self {
        Self::from_distributed(error)
    }
}

impl From<novarocks_query_application::session_error::QueryServiceError> for DmlExecutionError {
    fn from(error: novarocks_query_application::session_error::QueryServiceError) -> Self {
        Self::Application(error)
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
            Self::Execution(input) => formatter.write_str(input.detail().as_str()),
            Self::Engine(error) => formatter.write_str(error),
            Self::Application(error) => error.fmt(formatter),
            Self::Compilation(source) => source.fmt(formatter),
            Self::Analyze(error) => error.fmt(formatter),
            Self::Cow(error) => error.fmt(formatter),
            Self::Control(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DmlExecutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cow(error) => Some(error),
            Self::Application(error) => Some(error),
            Self::Compilation(source) => Some(source),
            Self::Analyze(source) => Some(source),
            Self::Control(source) => Some(source),
            _ => None,
        }
    }
}
impl From<crate::query_execution::dml::mutation_flow::CowFailure> for DmlExecutionError {
    fn from(error: crate::query_execution::dml::mutation_flow::CowFailure) -> Self {
        Self::Cow(error)
    }
}

/// Positive stage evidence retained independently of final publication.
#[derive(Debug)]
pub struct DmlStagedCreateFailure {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "retained as typed staged-create evidence in DML failure diagnostics"
        )
    )]
    pub receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "retained as typed staged-create evidence in DML failure diagnostics"
        )
    )]
    pub finalization: novarocks_spi::connector::ExternalMutationFinalization,
}

impl DmlError {
    pub(crate) fn from_application(error: novarocks_query_application::session_error::QueryServiceError) -> Self {
        Self {
            kind: DmlErrorKind::Executor,
            body: DmlErrorBody::Application(error),
            publication_terminal: None,
            user_error: None,
            engine_error_code: None,
            cow_failure: None,
            compile_control: None,
            staged_create_preparation: None,
        }
    }

    pub(crate) fn application_error(
        &self,
    ) -> Option<&novarocks_query_application::session_error::QueryServiceError> {
        match &self.body {
            DmlErrorBody::Application(error) => Some(error),
            _ => None,
        }
    }

    pub(crate) fn from_execution(input: ExecutionFailureInput) -> Self {
        Self {
            kind: DmlErrorKind::Executor,
            body: DmlErrorBody::Execution(Box::new(input)),
            publication_terminal: None,
            user_error: None,
            engine_error_code: None,
            cow_failure: None,
            compile_control: None,
            staged_create_preparation: None,
        }
    }

    /// Publication observations cannot replace an already selected Task input.
    pub(crate) fn prefer_selected_execution(self, later: Self) -> Self {
        if matches!(
            &self.body,
            DmlErrorBody::Execution(_) | DmlErrorBody::Application(_)
        ) {
            tracing::warn!(publication_error = %later, "publication observation accompanies the selected execution failure");
            self
        } else {
            later
        }
    }

    pub(crate) fn execution_failure(&self) -> Option<&ExecutionFailureInput> {
        match &self.body {
            DmlErrorBody::Execution(input) => Some(input.as_ref()),
            DmlErrorBody::Application(error) => error.execution_failure(),
            DmlErrorBody::Legacy(_) => None,
        }
    }

    /// Cleanup may enrich local legacy diagnostics, never the selected input.
    pub(crate) fn with_uncommitted_cleanup(
        mut self,
        cleanup: &novarocks_spi::connector::ExternalMutationFinalization,
    ) -> Self {
        match &mut self.body {
            DmlErrorBody::Legacy(message) => {
                *message =
                    crate::connector::mutation::known_uncommitted_message(&*message, cleanup);
            }
            DmlErrorBody::Execution(_) | DmlErrorBody::Application(_) => {
                if let novarocks_spi::connector::ExternalMutationFinalization::Failed(error) =
                    cleanup
                {
                    tracing::warn!(%error, "artifact cleanup failed after the selected execution failure");
                }
            }
        }
        self
    }

    fn cow(error: crate::query_execution::dml::mutation_flow::CowFailure) -> Self {
        Self {
            kind: DmlErrorKind::Executor,
            body: DmlErrorBody::Legacy("copy-on-write begin failed".to_owned()),
            publication_terminal: None,
            user_error: None,
            engine_error_code: None,
            cow_failure: Some(error),
            compile_control: None,
            staged_create_preparation: None,
        }
    }
    pub(crate) fn take_cow_failure(
        &mut self,
    ) -> Option<crate::query_execution::dml::mutation_flow::CowFailure> {
        self.cow_failure.take()
    }

    pub(crate) fn new(kind: DmlErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            body: DmlErrorBody::Legacy(error.to_string()),
            publication_terminal: None,
            user_error: None,
            engine_error_code: None,
            cow_failure: None,
            compile_control: None,
            staged_create_preparation: None,
        }
    }

    pub(crate) fn executor(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Executor, error)
    }

    pub(crate) fn commit(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Commit, error)
    }

    /// Attaches a terminal produced by the statement-local publication state
    /// machine. This additive API deliberately does not infer a disposition
    /// from the broad DML error kind.
    pub(crate) fn with_publication_terminal(mut self, terminal: LakePublicationTerminal) -> Self {
        self.publication_terminal = Some(terminal);
        self
    }

    /// Reports a local finalization failure after the Catalog publication is
    /// already known committed. The explicit terminal remains authoritative
    /// and keeps retry disabled.
    pub(crate) fn known_committed_finalization_failed(
        terminal: LakePublicationTerminal,
        error: impl fmt::Display,
    ) -> Self {
        debug_assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::KnownCommitted,
            "known-committed finalization errors require a committed terminal"
        );
        Self {
            kind: DmlErrorKind::CommittedButUnfinalized,
            body: DmlErrorBody::Legacy(format!(
                "{error}; publication is known committed; do not retry statement"
            )),
            publication_terminal: Some(terminal),
            user_error: None,
            engine_error_code: None,
            cow_failure: None,
            compile_control: None,
            staged_create_preparation: None,
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
            body: DmlErrorBody::Legacy(error.to_string()),
            publication_terminal: None,
            user_error: Some(error),
            engine_error_code: None,
            cow_failure: None,
            compile_control: None,
            staged_create_preparation: None,
        }
    }

    /// Carries an explicit engine-level code to the SQL boundary. DML must
    /// never reconstruct these codes by parsing its own display text.
    pub(crate) fn with_engine_error_code(mut self, code: EngineErrorCode) -> Self {
        self.engine_error_code = Some(code);
        self
    }

    pub(crate) fn with_compile_control(
        mut self,
        cause: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_control = cause;
        self
    }
    pub(crate) fn compile_control(error: novarocks_type_contract::CompileControlError) -> Self {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_query_application::execution_error::ExecutionFailureSite;
        let mut failure = match &error {
            novarocks_type_contract::CompileControlError::Source(source) => {
                Self::from_execution(ExecutionFailureInput::new(
                    source.failure_fact(),
                    novarocks_execution_contract::SafeDetail::truncating(
                        "DML compilation failed in its source domain",
                    ),
                    ExecutionFailureSite::Coordinator,
                ))
            }
            _ => Self::executor(&error),
        };
        failure.compile_control = Some(error);
        failure
    }
    pub(crate) fn compile_control_error(
        &self,
    ) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }

    #[cfg(test)]
    pub const fn kind(&self) -> DmlErrorKind {
        self.kind
    }

    pub fn user_error(&self) -> Option<&UserError> {
        self.user_error.as_ref()
    }

    pub(crate) const fn engine_error_code(&self) -> Option<EngineErrorCode> {
        self.engine_error_code
    }

    pub(crate) fn with_unusable_stage(
        mut self,
        receipt: novarocks_spi::connector::ConnectorStagedCreateReceipt,
        finalization: novarocks_spi::connector::ExternalMutationFinalization,
    ) -> Self {
        self.staged_create_preparation = Some(Box::new(DmlStagedCreateFailure {
            receipt,
            finalization,
        }));
        self
    }

    #[cfg(test)]
    pub fn staged_create_preparation(&self) -> Option<&DmlStagedCreateFailure> {
        self.staged_create_preparation.as_deref()
    }

    pub fn publication_terminal(&self) -> Option<&LakePublicationTerminal> {
        self.publication_terminal.as_ref()
    }
}

impl fmt::Display for DmlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: ", self.kind)?;
        match &self.body {
            DmlErrorBody::Legacy(message) => formatter.write_str(message)?,
            DmlErrorBody::Execution(input) => formatter.write_str(input.detail().as_str())?,
            DmlErrorBody::Application(error) => fmt::Display::fmt(error, formatter)?,
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

impl std::error::Error for DmlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let DmlErrorBody::Application(error) = &self.body {
            return Some(error);
        }
        self.cow_failure
            .as_ref()
            .map(|error| error as &(dyn std::error::Error + 'static))
            .or_else(|| {
                self.compile_control
                    .as_ref()
                    .map(|error| error as &(dyn std::error::Error + 'static))
            })
    }
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn encode_capability_refusal_keeps_its_identity_through_dml_projection() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let original = novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
            "change-stream router".into(),
        );
        let expected = original.failure_fact();
        let error = DmlExecutionError::from_encode(original).into_dml_error(None);
        assert_eq!(error.execution_failure().unwrap().fact(), expected);
        assert_eq!(expected.class(), FailureClass::Unsupported);
        let Some(CompileControlError::Source(source)) = error.compile_control_error() else {
            panic!("original source must survive")
        };
        assert!(
            matches!(source.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>(),
            Some(novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(detail)) if detail == "change-stream router")
        );
        assert_eq!(error.engine_error_code(), None);
    }

    #[test]
    fn compile_and_encode_failures_retain_exact_control_until_dml_terminal() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let compiled =
                DmlExecutionError::from_compile(control.clone().into()).into_dml_error(None);
            let encoded = DmlExecutionError::from_encode(
                novarocks_plan_codec::PhysicalEncodeError::Control(control.clone()),
            )
            .into_dml_error(None);
            let analyzed =
                DmlExecutionError::from_compile(novarocks_sql::compiler::SqlCompileError::Analyze(
                    AnalyzeError::control(control.clone()),
                ))
                .into_dml_error(None);
            for failure in [compiled, encoded, analyzed] {
                assert_eq!(failure.compile_control_error(), Some(control.clone()));
                assert_eq!(failure.kind(), DmlErrorKind::Executor);
                assert!(failure.user_error().is_none());
                assert!(failure.publication_terminal().is_none());
            }
        }
    }

    #[test]
    fn ordinary_error_text_does_not_acquire_control_authority() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let legacy = DmlExecutionError::from(control.to_string()).into_dml_error(None);
            assert_eq!(legacy.compile_control_error(), None);
            let original = novarocks_plan_codec::PhysicalEncodeError::Invalid(control.to_string());
            let expected = original.failure_fact();
            let encoded = DmlExecutionError::from_encode(original).into_dml_error(None);
            assert_eq!(encoded.execution_failure().unwrap().fact(), expected);
            assert_eq!(expected.class(), FailureClass::Invariant);
            assert_ne!(expected, control.failure_fact());
            let Some(CompileControlError::Source(source)) = encoded.compile_control_error() else {
                panic!("retain the actual Invalid author")
            };
            assert!(matches!(
                source.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>(),
                Some(novarocks_plan_codec::PhysicalEncodeError::Invalid(_))
            ));
        }
    }
}
