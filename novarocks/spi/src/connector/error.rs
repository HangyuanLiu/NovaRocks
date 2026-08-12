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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorErrorKind {
    InvalidRequest,
    NotFound,
    PermissionDenied,
    Unsupported,
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    Unavailable,
    CorruptData,
    Internal,
}

/// Typed classification of an external write fence failure.
///
/// A fence failure is a linearization decision, never a transient condition and
/// never a missing capability. It must therefore stay a distinct classification:
/// downgrading it to `CommitUnknown` would invite an unsafe retry, and
/// downgrading it to `Unsupported` would invite an unfenced fallback.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorExternalFenceFailure {
    /// The submitted fence generation is behind the established fence.
    Stale,
    /// Another coordination attempt already established this generation with
    /// different contents, so the submitted authority is superseded.
    Superseded,
    /// A fence or receipt from a different operation or resource was presented.
    ForeignOperation,
    /// No external fence was established before a fenced terminal request.
    NotEstablished,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorError {
    kind: ConnectorErrorKind,
    message: String,
    retryable_before_progress: bool,
    cleanup_context: Option<String>,
    external_fence_failure: Option<ConnectorExternalFenceFailure>,
}

impl ConnectorError {
    pub fn new(kind: ConnectorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable_before_progress: false,
            cleanup_context: None,
            external_fence_failure: None,
        }
    }

    /// Report a typed external write fence failure.
    ///
    /// `InvalidRequest` is the carrier kind on purpose: every kind that a
    /// provider maps to a commit-unknown outcome is excluded, so a fenced
    /// commit conflict can never be laundered into an ambiguous result.
    pub fn external_fence(
        failure: ConnectorExternalFenceFailure,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: ConnectorErrorKind::InvalidRequest,
            message: message.into(),
            retryable_before_progress: false,
            cleanup_context: None,
            external_fence_failure: Some(failure),
        }
    }

    pub const fn kind(&self) -> ConnectorErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// The typed external fence classification, when this error is one.
    pub const fn external_fence_failure(&self) -> Option<ConnectorExternalFenceFailure> {
        self.external_fence_failure
    }

    /// Whether this error is an external write fence failure. Callers must
    /// treat it as a terminal stale/conflict decision.
    pub const fn is_external_fence_failure(&self) -> bool {
        self.external_fence_failure.is_some()
    }

    pub const fn retryable_before_progress(&self) -> bool {
        self.retryable_before_progress
    }

    pub fn with_retryable_before_progress(mut self) -> Self {
        // An external fence failure stays non-retryable: repeating the same
        // superseded authority can only fail again, and a caller that retried
        // it would be attempting an unfenced write.
        if self.external_fence_failure.is_none() {
            self.retryable_before_progress = true;
        }
        self
    }

    pub fn with_cleanup_context(mut self, cleanup_context: impl Into<String>) -> Self {
        self.cleanup_context = Some(cleanup_context.into());
        self
    }
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)?;
        if let Some(cleanup_context) = &self.cleanup_context {
            write!(formatter, " (cleanup: {cleanup_context})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConnectorError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_fence_failures_stay_typed_and_non_retryable() {
        for failure in [
            ConnectorExternalFenceFailure::Stale,
            ConnectorExternalFenceFailure::Superseded,
            ConnectorExternalFenceFailure::ForeignOperation,
            ConnectorExternalFenceFailure::NotEstablished,
        ] {
            let error = ConnectorError::external_fence(failure, "fence conflict")
                .with_retryable_before_progress();
            assert!(error.is_external_fence_failure());
            assert_eq!(error.external_fence_failure(), Some(failure));
            assert!(
                !error.retryable_before_progress(),
                "an external fence failure must never become retryable"
            );
            assert_ne!(
                error.kind(),
                ConnectorErrorKind::Unsupported,
                "an external fence failure must never be downgraded to unsupported"
            );
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        }
    }

    #[test]
    fn ordinary_errors_keep_their_retryable_flag() {
        let error = ConnectorError::new(ConnectorErrorKind::Unavailable, "transient")
            .with_retryable_before_progress();
        assert!(error.retryable_before_progress());
        assert!(!error.is_external_fence_failure());
    }
}
