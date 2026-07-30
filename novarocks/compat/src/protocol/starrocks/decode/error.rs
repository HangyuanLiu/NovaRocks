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

use novarocks::exec::fragment::error::{ExecPlanBuildError, FragmentBindingError};
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksDependencyContractError {
    kind: StarRocksDependencyContractErrorKind,
    dependency_id: u64,
    detail: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksDependencyContractErrorKind {
    Missing,
    Extra,
    WrongKind,
}

impl StarRocksDependencyContractError {
    pub(crate) fn new(
        kind: StarRocksDependencyContractErrorKind,
        dependency_id: u64,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            dependency_id,
            detail: detail.into(),
        }
    }

    pub(crate) const fn kind(&self) -> StarRocksDependencyContractErrorKind {
        self.kind
    }

    pub(crate) const fn dependency_id(&self) -> u64 {
        self.dependency_id
    }
}

impl fmt::Display for StarRocksDependencyContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StarRocks dependency contract error ({:?}, id={}): {}",
            self.kind, self.dependency_id, self.detail
        )
    }
}

impl Error for StarRocksDependencyContractError {}

#[derive(Debug)]
#[allow(private_interfaces)]
pub(crate) enum StarRocksFragmentDecodeError {
    Protocol(ProtocolError),
    Plan(ExecPlanBuildError),
    DependencyContract(StarRocksDependencyContractError),
    Binding(FragmentBindingError),
}

impl StarRocksFragmentDecodeError {
    pub(crate) fn protocol(&self) -> Option<&ProtocolError> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Plan(_) | Self::DependencyContract(_) | Self::Binding(_) => None,
        }
    }

    pub(crate) fn missing(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::MissingField, detail)
    }

    pub(crate) fn invalid_value(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InvalidValue, detail)
    }

    pub(crate) fn invalid_enum(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InvalidEnum, detail)
    }

    pub(crate) fn out_of_range(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::OutOfRange, detail)
    }

    pub(crate) fn unsupported(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::Unsupported, detail)
    }

    pub(crate) fn inconsistent(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InconsistentFields, detail)
    }

    fn protocol_error(path: FieldPath, kind: ProtocolErrorKind, detail: impl fmt::Display) -> Self {
        Self::Protocol(ProtocolError::new(
            ProtocolFamily::StarRocks,
            path,
            kind,
            detail.to_string(),
        ))
    }
}

impl fmt::Display for StarRocksFragmentDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(f),
            Self::Plan(error) => error.fmt(f),
            Self::DependencyContract(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
        }
    }
}

impl Error for StarRocksFragmentDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Plan(error) => Some(error),
            Self::DependencyContract(error) => Some(error),
            Self::Binding(error) => Some(error),
        }
    }
}

impl From<ProtocolError> for StarRocksFragmentDecodeError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<ExecPlanBuildError> for StarRocksFragmentDecodeError {
    fn from(error: ExecPlanBuildError) -> Self {
        Self::Plan(error)
    }
}

impl From<StarRocksDependencyContractError> for StarRocksFragmentDecodeError {
    fn from(error: StarRocksDependencyContractError) -> Self {
        Self::DependencyContract(error)
    }
}

impl From<FragmentBindingError> for StarRocksFragmentDecodeError {
    fn from(error: FragmentBindingError) -> Self {
        Self::Binding(error)
    }
}
