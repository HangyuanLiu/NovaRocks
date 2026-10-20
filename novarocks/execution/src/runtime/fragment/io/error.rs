use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentIoOperation {
    ExchangeTransmit,
    ResultOpen,
    ResultWrite,
    ResultFinish,
    Lookup,
}

impl fmt::Display for FragmentIoOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::ExchangeTransmit => "exchange transmit",
            Self::ResultOpen => "result open",
            Self::ResultWrite => "result write",
            Self::ResultFinish => "result finish",
            Self::Lookup => "lookup",
        };
        formatter.write_str(label)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentIoErrorKind {
    Unavailable,
    Timeout,
    RemoteRejected,
    InvalidResponse,
    Cancelled,
    Internal,
}

impl fmt::Display for FragmentIoErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Unavailable => "unavailable",
            Self::Timeout => "timeout",
            Self::RemoteRejected => "remote rejected",
            Self::InvalidResponse => "invalid response",
            Self::Cancelled => "cancelled",
            Self::Internal => "internal",
        };
        formatter.write_str(label)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentIoError {
    operation: FragmentIoOperation,
    kind: FragmentIoErrorKind,
    message: String,
    source: Option<crate::runtime::fragment::SharedDomainFailure>,
}

impl FragmentIoError {
    pub fn new(
        operation: FragmentIoOperation,
        kind: FragmentIoErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            operation,
            kind,
            message: message.into(),
            source: None,
        }
    }

    /// Preserve a known producer's original source and neutral fact. The IO
    /// operation/kind remain transport context and cannot reclassify it.
    pub fn from_domain<E>(
        operation: FragmentIoOperation,
        kind: FragmentIoErrorKind,
        cause: E,
    ) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        let source = crate::runtime::fragment::SharedDomainFailure::new(cause);
        Self {
            operation,
            kind,
            message: novarocks_failure_contract::BoundedDetail::truncating_display(&source)
                .into_string(),
            source: Some(source),
        }
    }

    pub const fn operation(&self) -> FragmentIoOperation {
        self.operation
    }

    pub const fn kind(&self) -> FragmentIoErrorKind {
        self.kind
    }
}

impl fmt::Display for FragmentIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} {}: {}",
            self.operation, self.kind, self.message
        )
    }
}

impl std::error::Error for FragmentIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl novarocks_failure_contract::FailureProjection for FragmentIoError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        if let Some(source) = &self.source {
            return source.failure_fact();
        }
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let namespace = match self.operation {
            FragmentIoOperation::ExchangeTransmit => domain::EXCHANGE,
            FragmentIoOperation::ResultOpen
            | FragmentIoOperation::ResultWrite
            | FragmentIoOperation::ResultFinish => domain::RESULT,
            FragmentIoOperation::Lookup => domain::EXECUTION,
        };
        let (class, code) = match self.kind {
            FragmentIoErrorKind::Unavailable => (
                FailureClass::Dependency,
                ExecutionReason::FragmentIoUnavailable,
            ),
            FragmentIoErrorKind::Timeout => {
                (FailureClass::Control, ExecutionReason::FragmentIoTimeout)
            }
            FragmentIoErrorKind::RemoteRejected => (
                FailureClass::Dependency,
                ExecutionReason::FragmentIoRemoteRejected,
            ),
            FragmentIoErrorKind::InvalidResponse => (
                FailureClass::Invariant,
                ExecutionReason::FragmentIoInvalidResponse,
            ),
            FragmentIoErrorKind::Cancelled => {
                (FailureClass::Control, ExecutionReason::FragmentIoCancelled)
            }
            FragmentIoErrorKind::Internal => (
                FailureClass::Unexpected,
                ExecutionReason::FragmentIoInternal,
            ),
        };
        FailureFact::new(class, FailureReason::new(namespace, code.code()))
    }
}
impl From<FragmentIoError> for crate::runtime::fragment::ExecutionFailure {
    fn from(error: FragmentIoError) -> Self {
        Self::from_domain(error)
    }
}
