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

/// Bounded local failure categories. Values and cryptographic material are
/// intentionally not retained in either `Debug` or `Display` output.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NativeTrustFailureKind {
    InvalidDeploymentId,
    InvalidSharedSecret,
    InvalidCallerSubject,
    MissingAuthorization,
    DuplicateAuthorization,
    MalformedAuthorization,
    TokenTooLarge,
    MalformedToken,
    InvalidJoseHeader,
    InvalidClaims,
    InvalidSignature,
    ExpiredToken,
    InvalidTokenTime,
    TransportConfiguration,
}

impl fmt::Display for NativeTrustFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::InvalidDeploymentId => "invalid deployment id",
            Self::InvalidSharedSecret => "invalid shared secret",
            Self::InvalidCallerSubject => "invalid native caller subject",
            Self::MissingAuthorization => "missing native authorization",
            Self::DuplicateAuthorization => "duplicate native authorization",
            Self::MalformedAuthorization => "malformed native authorization",
            Self::TokenTooLarge => "native authorization token exceeds limit",
            Self::MalformedToken => "malformed native authorization token",
            Self::InvalidJoseHeader => "invalid native authorization JOSE header",
            Self::InvalidClaims => "invalid native authorization claims",
            Self::InvalidSignature => "invalid native authorization signature",
            Self::ExpiredToken => "expired native authorization token",
            Self::InvalidTokenTime => "invalid native authorization token time",
            Self::TransportConfiguration => "invalid native transport configuration",
        };
        formatter.write_str(value)
    }
}

impl std::error::Error for NativeTrustFailureKind {}
