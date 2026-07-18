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
use std::fmt;

use crate::protocol::common::error::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};

/// Typed error used by nested native-protobuf leaf decoders before the owning
/// DTO boundary attaches its exact [`FieldPath`].
#[derive(Debug)]
pub(crate) struct NativeFragmentLeafDecodeError(String);

impl NativeFragmentLeafDecodeError {
    pub(crate) fn new(detail: impl Into<String>) -> Self {
        Self(detail.into())
    }

    pub(crate) fn into_native(self, path: FieldPath) -> NativeFragmentDecodeError {
        NativeFragmentDecodeError::invalid_value(path, self.0)
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.0.contains(pattern)
    }
}

impl fmt::Display for NativeFragmentLeafDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for NativeFragmentLeafDecodeError {}

impl From<String> for NativeFragmentLeafDecodeError {
    fn from(detail: String) -> Self {
        Self(detail)
    }
}

impl From<&str> for NativeFragmentLeafDecodeError {
    fn from(detail: &str) -> Self {
        Self(detail.to_string())
    }
}

impl From<NativeFragmentLeafDecodeError> for String {
    fn from(error: NativeFragmentLeafDecodeError) -> Self {
        error.0
    }
}

#[cfg(test)]
impl PartialEq<&str> for NativeFragmentLeafDecodeError {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

#[derive(Debug)]
pub(crate) enum NativeFragmentDecodeError {
    Protocol(ProtocolError),
    Plan(crate::exec::fragment::error::ExecPlanBuildError),
    Binding(crate::exec::fragment::error::FragmentBindingError),
}

impl NativeFragmentDecodeError {
    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.to_string().contains(pattern)
    }

    pub(crate) fn protocol(&self) -> Option<&ProtocolError> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Plan(_) | Self::Binding(_) => None,
        }
    }

    pub(crate) fn missing(path: FieldPath, detail: impl Into<String>) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::MissingField, detail)
    }

    pub(crate) fn invalid_value(path: FieldPath, detail: impl Into<String>) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InvalidValue, detail)
    }

    pub(crate) fn invalid_enum(path: FieldPath, detail: impl Into<String>) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InvalidEnum, detail)
    }

    pub(crate) fn out_of_range(path: FieldPath, detail: impl Into<String>) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::OutOfRange, detail)
    }

    pub(crate) fn inconsistent(path: FieldPath, detail: impl Into<String>) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InconsistentFields, detail)
    }

    pub(crate) fn unsupported(path: FieldPath, detail: impl Into<String>) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::Unsupported, detail)
    }

    pub(crate) fn map_invalid<T, E: fmt::Display>(
        path: FieldPath,
        result: Result<T, E>,
    ) -> Result<T, Self> {
        result.map_err(|detail| Self::invalid_value(path, detail.to_string()))
    }

    fn protocol_error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> Self {
        Self::Protocol(ProtocolError::new(
            ProtocolFamily::Native,
            path,
            kind,
            detail,
        ))
    }
}

#[cfg(test)]
impl PartialEq<&str> for NativeFragmentDecodeError {
    fn eq(&self, other: &&str) -> bool {
        self.to_string().ends_with(other)
    }
}

impl fmt::Display for NativeFragmentDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(f),
            Self::Plan(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
        }
    }
}

impl Error for NativeFragmentDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Plan(error) => Some(error),
            Self::Binding(error) => Some(error),
        }
    }
}

impl From<ProtocolError> for NativeFragmentDecodeError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<crate::exec::fragment::error::ExecPlanBuildError> for NativeFragmentDecodeError {
    fn from(error: crate::exec::fragment::error::ExecPlanBuildError) -> Self {
        Self::Plan(error)
    }
}

impl From<crate::exec::fragment::error::FragmentBindingError> for NativeFragmentDecodeError {
    fn from(error: crate::exec::fragment::error::FragmentBindingError) -> Self {
        Self::Binding(error)
    }
}
