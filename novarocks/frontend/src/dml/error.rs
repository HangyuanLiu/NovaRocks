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

/// Category of a DML foundation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlErrorKind {
    /// Operation-journal persistence failure (provider/repository).
    Journal,
    /// Coordinated-write execution failure before commit.
    Executor,
    /// Typed commit service failure.
    Commit,
    /// Post-commit finalization failure (metadata already committed).
    Finalize,
    /// Write admission (fencing) rejected the operation.
    Admission,
}

/// A DML foundation error. Wraps lower-layer errors as a message; the original
/// repository/provider error is not surfaced directly (frontend wire mapping is
/// a caller concern).
#[derive(Debug)]
pub struct DmlError {
    kind: DmlErrorKind,
    message: String,
}

impl DmlError {
    pub(crate) fn new(kind: DmlErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    pub(crate) fn journal(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Journal, error)
    }

    pub(crate) fn executor(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Executor, error)
    }

    pub(crate) fn commit(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Commit, error)
    }

    pub(crate) fn finalize(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Finalize, error)
    }

    pub(crate) fn admission(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Admission, error)
    }

    pub const fn kind(&self) -> DmlErrorKind {
        self.kind
    }
}

impl fmt::Display for DmlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for DmlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_kind_and_message() {
        let error = DmlError::journal("boom");
        assert_eq!(error.kind(), DmlErrorKind::Journal);
        assert_eq!(error.to_string(), "Journal: boom");
    }
}
