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

use std::error::Error;
use std::fmt::{Display, Formatter};

use novarocks_failure_contract::SharedFailureCause;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::storage_authority::failure::{AuthorityFailure, AuthorityFailureReason};

pub type FileResult<T> = Result<T, FileError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileErrorKind {
    Invalid,
    Unsupported,
    NotFound,
    AlreadyExists,
    Permission,
    Corrupt,
    ResourceExhausted,
    Transient,
    DeadlineExceeded,
    Cancelled,
    Internal,
}

#[derive(Debug)]
pub struct FileError {
    kind: FileErrorKind,
    message: String,
    source: Option<Box<dyn Error + Send + Sync>>,
    authority_cause: Option<SharedFailureCause>,
}

impl FileError {
    pub fn new(kind: FileErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
            authority_cause: None,
        }
    }

    pub fn with_source(
        kind: FileErrorKind,
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
            authority_cause: None,
        }
    }

    pub(crate) fn authority(
        kind: FileErrorKind,
        reason: AuthorityFailureReason,
        message: impl Into<String>,
    ) -> Self {
        let message = message.into();
        let cause = SharedFailureCause::new(AuthorityFailure::new(reason, message.clone()));
        Self::new(kind, message).with_authority_cause(cause)
    }

    pub(crate) fn with_authority_cause(mut self, cause: SharedFailureCause) -> Self {
        self.authority_cause = Some(cause);
        self
    }

    pub(crate) fn authority_cause(&self) -> Option<&SharedFailureCause> {
        self.authority_cause.as_ref()
    }

    pub fn kind(&self) -> FileErrorKind {
        self.kind
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(FileErrorKind::Invalid, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(FileErrorKind::Unsupported, message)
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(FileErrorKind::Cancelled, message)
    }

    pub fn deadline(message: impl Into<String>) -> Self {
        Self::new(FileErrorKind::DeadlineExceeded, message)
    }
}

impl Display for FileError {
    /// Renders the cause, not only the operation that hit it.
    ///
    /// A storage failure's reason usually lives in the source: the operation
    /// name says "stat file" while the source says the catalog could not be
    /// reached. Dropping it leaves an operator with a classification and no
    /// fact, which is precisely what CAD-1 D12 requires to survive.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl Error for FileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
            .or_else(|| {
                self.authority_cause
                    .as_ref()
                    .map(|cause| cause as &(dyn Error + 'static))
            })
    }
}

/// The connector-neutral taxonomy of a file failure, shared by every provider
/// and by the source operations a file request is registered in.
impl From<&FileError> for ConnectorError {
    fn from(error: &FileError) -> Self {
        let kind = match error.kind() {
            FileErrorKind::Invalid => ConnectorErrorKind::InvalidRequest,
            FileErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
            FileErrorKind::NotFound => ConnectorErrorKind::NotFound,
            FileErrorKind::Permission => ConnectorErrorKind::PermissionDenied,
            FileErrorKind::Corrupt => ConnectorErrorKind::CorruptData,
            FileErrorKind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
            FileErrorKind::Transient => ConnectorErrorKind::Unavailable,
            FileErrorKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
            FileErrorKind::Cancelled => ConnectorErrorKind::Cancelled,
            FileErrorKind::AlreadyExists | FileErrorKind::Internal => ConnectorErrorKind::Internal,
        };
        match error.authority_cause() {
            Some(cause) => ConnectorError::from_domain(kind, cause.clone()),
            None => ConnectorError::new(kind, error.to_string()),
        }
    }
}

impl From<FileError> for ConnectorError {
    fn from(error: FileError) -> Self {
        Self::from(&error)
    }
}
