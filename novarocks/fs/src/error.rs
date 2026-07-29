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

pub type FileResult<T> = Result<T, FileError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileErrorKind {
    Invalid,
    Unsupported,
    NotFound,
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
}

impl FileError {
    pub fn new(kind: FileErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
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
        }
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for FileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}
