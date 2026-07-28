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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorError {
    kind: ConnectorErrorKind,
    message: String,
    retryable_before_progress: bool,
    cleanup_context: Option<String>,
}

impl ConnectorError {
    pub fn new(kind: ConnectorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable_before_progress: false,
            cleanup_context: None,
        }
    }

    pub const fn kind(&self) -> ConnectorErrorKind {
        self.kind
    }

    pub const fn retryable_before_progress(&self) -> bool {
        self.retryable_before_progress
    }

    pub fn with_retryable_before_progress(mut self) -> Self {
        self.retryable_before_progress = true;
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
