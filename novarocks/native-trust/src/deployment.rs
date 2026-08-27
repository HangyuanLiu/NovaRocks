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

use novarocks_secret::SecretValue;

use crate::NativeTrustFailureKind;

/// Public, exact trust-domain identifier. It is never inferred or normalized.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeploymentId(String);

impl DeploymentId {
    pub fn parse(value: impl Into<String>) -> Result<Self, NativeTrustFailureKind> {
        let value = value.into();
        let bytes = value.as_bytes();
        if !(1..=64).contains(&bytes.len()) || !value.is_ascii() {
            return Err(NativeTrustFailureKind::InvalidDeploymentId);
        }
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if !first.is_ascii_lowercase() && !first.is_ascii_digit()
            || !last.is_ascii_lowercase() && !last.is_ascii_digit()
            || !bytes.iter().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(*byte, b'.' | b'_' | b'-')
            })
        {
            return Err(NativeTrustFailureKind::InvalidDeploymentId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DeploymentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("DeploymentId")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for DeploymentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Validated secret input. This wrapper never exposes its source value; only
/// `NativeTrust` consumes it at the cryptographic construction boundary.
#[derive(Clone)]
pub struct ValidatedSharedSecret(SecretValue);

impl ValidatedSharedSecret {
    pub fn new(value: SecretValue) -> Result<Self, NativeTrustFailureKind> {
        let bytes = value.expose_secret().as_bytes();
        if !(32..=4096).contains(&bytes.len())
            || !bytes.iter().all(|byte| (0x21..=0x7e).contains(byte))
        {
            return Err(NativeTrustFailureKind::InvalidSharedSecret);
        }
        Ok(Self(value))
    }

    pub(crate) fn expose_for_kdf(&self) -> &[u8] {
        self.0.expose_secret().as_bytes()
    }
}

impl fmt::Debug for ValidatedSharedSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ValidatedSharedSecret(REDACTED)")
    }
}
