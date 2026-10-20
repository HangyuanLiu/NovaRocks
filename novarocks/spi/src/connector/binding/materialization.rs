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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::connector::{ConnectorError, ConnectorErrorKind, ConnectorOperationControl};

const MAX_DETAIL_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorMaterializationErrorClass {
    InvalidDefinition,
    Authentication,
    Unavailable,
    Timeout,
    ResourceExhausted,
    Internal,
    /// The caller interrupted this installation with a retained typed cause.
    /// Its neutral failure facts still belong to that original cause.
    Interrupted,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorMaterializationRetryDisposition {
    Transient,
    UntilDefinitionChanges,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MaterializationCause {
    Authored,
    Connector(ConnectorError),
    StopObservation,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorMaterializationError {
    class: ConnectorMaterializationErrorClass,
    disposition: ConnectorMaterializationRetryDisposition,
    detail: Arc<str>,
    cause: MaterializationCause,
}

impl ConnectorMaterializationError {
    pub fn new(
        class: ConnectorMaterializationErrorClass,
        disposition: ConnectorMaterializationRetryDisposition,
        detail: impl AsRef<str>,
    ) -> Self {
        Self {
            class,
            disposition,
            detail: Arc::from(redact_and_bound(detail.as_ref())),
            cause: MaterializationCause::Authored,
        }
    }

    /// Observe cancellation already established by the attempt owner.
    pub fn cancelled() -> Self {
        observed_stop(
            ConnectorMaterializationErrorClass::Cancelled,
            ConnectorMaterializationRetryDisposition::Transient,
            "connector materialization was cancelled",
        )
    }

    pub const fn class(&self) -> ConnectorMaterializationErrorClass {
        self.class
    }

    pub const fn disposition(&self) -> ConnectorMaterializationRetryDisposition {
        self.disposition
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl From<ConnectorError> for ConnectorMaterializationError {
    fn from(error: ConnectorError) -> Self {
        let (class, disposition) = match error.kind() {
            ConnectorErrorKind::PermissionDenied => (
                ConnectorMaterializationErrorClass::Authentication,
                ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
            ),
            ConnectorErrorKind::Unavailable => (
                ConnectorMaterializationErrorClass::Unavailable,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::DeadlineExceeded => (
                ConnectorMaterializationErrorClass::Timeout,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::ResourceExhausted => (
                ConnectorMaterializationErrorClass::ResourceExhausted,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            // Suppression only scopes a future catalog installation. It does
            // not authorize replay of this query/task or of a write operation.
            ConnectorErrorKind::Interrupted => (
                ConnectorMaterializationErrorClass::Interrupted,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::Cancelled => (
                ConnectorMaterializationErrorClass::Cancelled,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::Internal | ConnectorErrorKind::CorruptData => (
                ConnectorMaterializationErrorClass::Internal,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::InvalidRequest
            | ConnectorErrorKind::NotFound
            | ConnectorErrorKind::Unsupported => (
                ConnectorMaterializationErrorClass::InvalidDefinition,
                ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
            ),
        };
        let mut projected = Self::new(class, disposition, error.message());
        projected.cause = MaterializationCause::Connector(error);
        projected
    }
}

impl fmt::Display for ConnectorMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.class, self.detail)
    }
}

// Keep the existing bounded diagnostic surface. Retaining a source must not
// cause Debug to traverse an SDK error and print its private payload.
impl fmt::Debug for ConnectorMaterializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorMaterializationError")
            .field("class", &self.class)
            .field("disposition", &self.disposition)
            .field("detail", &self.detail)
            .finish_non_exhaustive()
    }
}
impl std::error::Error for ConnectorMaterializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            MaterializationCause::Connector(source) => Some(source),
            MaterializationCause::Authored | MaterializationCause::StopObservation => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ConnectorMaterializationError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        if let MaterializationCause::Connector(source) = &self.cause {
            return source.failure_fact();
        }
        let (class, code) = match self.class {
            ConnectorMaterializationErrorClass::InvalidDefinition => (FailureClass::Data, 5000),
            ConnectorMaterializationErrorClass::Authentication => (FailureClass::Dependency, 5001),
            ConnectorMaterializationErrorClass::Unavailable => (FailureClass::Dependency, 5002),
            ConnectorMaterializationErrorClass::Timeout => (FailureClass::Control, 5003),
            ConnectorMaterializationErrorClass::ResourceExhausted => (FailureClass::Resource, 5004),
            ConnectorMaterializationErrorClass::Internal => (FailureClass::Unexpected, 5005),
            ConnectorMaterializationErrorClass::Cancelled => (FailureClass::Control, 5006),
            // Missing the required underlying caller cause is invalid.
            ConnectorMaterializationErrorClass::Interrupted => (FailureClass::Invariant, 5007),
        };
        let reason =
            FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(code).unwrap());
        if matches!(self.cause, MaterializationCause::StopObservation) {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}

/// Per-attempt cancellation and deadline facts. The FE scheduler owns when it
/// is cancelled; providers must check the context before and after I/O.
#[derive(Clone, Debug)]
pub struct MaterializationContext {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
}

impl MaterializationContext {
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn check_active(&self) -> Result<(), ConnectorMaterializationError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(observed_stop(
                ConnectorMaterializationErrorClass::Cancelled,
                ConnectorMaterializationRetryDisposition::Transient,
                "connector materialization was cancelled",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(observed_stop(
                ConnectorMaterializationErrorClass::Timeout,
                ConnectorMaterializationRetryDisposition::Transient,
                "connector materialization deadline elapsed",
            ));
        }
        Ok(())
    }
}

// Only this context observes an already-established stop. A provider's own
// timeout remains its originating failure and retains that provider's facts.
fn observed_stop(
    class: ConnectorMaterializationErrorClass,
    disposition: ConnectorMaterializationRetryDisposition,
    detail: &str,
) -> ConnectorMaterializationError {
    let mut error = ConnectorMaterializationError::new(class, disposition, detail);
    error.cause = MaterializationCause::StopObservation;
    error
}

impl ConnectorOperationControl for MaterializationContext {
    fn check_active(&self) -> Result<(), ConnectorError> {
        MaterializationContext::check_active(self).map_err(|error| {
            let kind = match error.class() {
                ConnectorMaterializationErrorClass::Cancelled => ConnectorErrorKind::Cancelled,
                ConnectorMaterializationErrorClass::Timeout => ConnectorErrorKind::DeadlineExceeded,
                _ => ConnectorErrorKind::Internal,
            };
            ConnectorError::from_domain(kind, error)
        })
    }
}

fn redact_and_bound(detail: &str) -> String {
    let mut result = detail.replace("password=", "password=[REDACTED]");
    result = result.replace("secret=", "secret=[REDACTED]");
    result = result.replace("token=", "token=[REDACTED]");
    if result.len() > MAX_DETAIL_BYTES {
        let mut end = MAX_DETAIL_BYTES;
        while !result.is_char_boundary(end) {
            end -= 1;
        }
        result.truncate(end);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialization_preserves_connector_facts_and_actual_source() {
        use novarocks_failure_contract::{
            FailureClass, FailureEvidence, FailureFact, FailureProjection, FailureReason, domain,
        };
        use std::error::Error;
        #[derive(Debug)]
        struct ProviderLimit;
        impl fmt::Display for ProviderLimit {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("provider-local diagnostic")
            }
        }
        impl Error for ProviderLimit {}
        impl FailureProjection for ProviderLimit {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Resource,
                    FailureReason::new(
                        domain::CONNECTOR,
                        std::num::NonZeroU16::new(65000).unwrap(),
                    ),
                )
            }
        }
        let original = ConnectorError::from_domain(ConnectorErrorKind::Internal, ProviderLimit);
        let fact = original.failure_fact();
        let materialization = ConnectorMaterializationError::from(original);
        assert_eq!(materialization.failure_fact(), fact);
        let connector = materialization
            .source()
            .unwrap()
            .downcast_ref::<ConnectorError>()
            .unwrap();
        assert!(
            connector
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<ProviderLimit>()
        );

        // The attempt's observed deadline is not the provider's I/O timeout.
        let expired =
            MaterializationContext::new(Instant::now() - std::time::Duration::from_secs(1));
        let observed = ConnectorOperationControl::check_active(&expired).unwrap_err();
        assert_eq!(
            observed.failure_fact().evidence(),
            FailureEvidence::StopObservation
        );
        assert!(
            observed
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<ConnectorMaterializationError>()
        );
        let io_timeout = ConnectorMaterializationError::from(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "remote timeout",
        ));
        assert_eq!(
            io_timeout.failure_fact().evidence(),
            FailureEvidence::Originating
        );
    }

    #[test]
    fn caller_interruption_keeps_its_real_cause_across_materialization_and_mutation() {
        use crate::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        use std::error::Error as _;
        let raw = LayoutResourceError::SourceModel;
        let expected = raw.failure_fact();
        let cause = CompileControlError::from_domain(raw);
        let connector = ConnectorError::compile_interruption(cause.clone());
        let materialization = ConnectorMaterializationError::from(connector.clone());
        let mutation = ConnectorMutationFailure::from_connector(connector.clone());
        assert_eq!(expected.class(), FailureClass::Invariant);
        assert_eq!(
            materialization.class(),
            ConnectorMaterializationErrorClass::Interrupted
        );
        assert_eq!(materialization.failure_fact(), expected);
        assert_eq!(mutation.kind(), ConnectorMutationFailureKind::Interrupted);
        assert_eq!(mutation.failure_fact(), expected);
        assert_eq!(mutation.connector_source(), Some(&connector));
        assert_eq!(
            materialization
                .source()
                .unwrap()
                .downcast_ref::<ConnectorError>(),
            Some(&connector)
        );
        assert!(!connector.retryable_before_progress());
    }

    #[test]
    fn materialization_diagnostic_bounds_preserve_utf8() {
        let error = ConnectorMaterializationError::new(
            ConnectorMaterializationErrorClass::Internal,
            ConnectorMaterializationRetryDisposition::Transient,
            format!("{}界", "x".repeat(MAX_DETAIL_BYTES - 1)),
        );
        assert_eq!(error.detail().len(), MAX_DETAIL_BYTES - 1);
    }

    #[test]
    fn operation_control_preserves_materialization_stop_reason() {
        let cancelled =
            MaterializationContext::new(Instant::now() + std::time::Duration::from_secs(5));
        cancelled.cancel();
        assert_eq!(
            ConnectorOperationControl::check_active(&cancelled)
                .expect_err("cancelled materialization must stop")
                .kind(),
            ConnectorErrorKind::Cancelled,
        );

        let expired =
            MaterializationContext::new(Instant::now() - std::time::Duration::from_secs(1));
        assert_eq!(
            ConnectorOperationControl::check_active(&expired)
                .expect_err("expired materialization must stop")
                .kind(),
            ConnectorErrorKind::DeadlineExceeded,
        );
    }

    #[test]
    fn connector_error_mapping_has_a_finite_retry_disposition() {
        let error = ConnectorMaterializationError::from(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "temporary remote failure",
        ));
        assert_eq!(
            error.class(),
            ConnectorMaterializationErrorClass::Unavailable
        );
        assert_eq!(
            error.disposition(),
            ConnectorMaterializationRetryDisposition::Transient
        );
    }

    #[test]
    fn materialization_context_fails_closed_after_cancellation() {
        let context =
            MaterializationContext::new(Instant::now() + std::time::Duration::from_secs(1));
        context.cancel();
        assert_eq!(
            context.check_active().unwrap_err().class(),
            ConnectorMaterializationErrorClass::Cancelled
        );
    }
}
