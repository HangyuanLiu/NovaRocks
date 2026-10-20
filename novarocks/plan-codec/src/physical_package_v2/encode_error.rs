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

//! Encoding failures shared by the compiled package and its callers.

/// Typed original-control, structural and capability encoding failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalEncodeError {
    Control(novarocks_type_contract::CompileControlError),
    Binding(novarocks_functions::FunctionBindingError),
    Source(novarocks_failure_contract::SharedFailureCause),
    Invalid(String),
    UnsupportedCapability(String),
}
impl std::fmt::Display for PhysicalEncodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::Binding(error) => error.fmt(formatter),
            Self::Source(error) => error.fmt(formatter),
            Self::Invalid(error) => formatter.write_str(error),
            Self::UnsupportedCapability(message) => formatter.write_str(message),
        }
    }
}
impl std::error::Error for PhysicalEncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::Source(source) => Some(source),
            Self::Invalid(_) | Self::UnsupportedCapability(_) => None,
        }
    }
}
impl From<String> for PhysicalEncodeError {
    fn from(error: String) -> Self {
        Self::Invalid(error)
    }
}
impl From<&str> for PhysicalEncodeError {
    fn from(error: &str) -> Self {
        Self::Invalid(error.into())
    }
}
impl From<novarocks_type_contract::CompileControlError> for PhysicalEncodeError {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<novarocks_functions::FunctionBindingError> for PhysicalEncodeError {
    fn from(error: novarocks_functions::FunctionBindingError) -> Self {
        match error {
            novarocks_functions::FunctionBindingError::Control(error) => Self::Control(error),
            other => Self::Binding(other),
        }
    }
}

impl PhysicalEncodeError {
    pub fn from_domain<E>(source: E) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        Self::Source(novarocks_failure_contract::SharedFailureCause::new(source))
    }
}
