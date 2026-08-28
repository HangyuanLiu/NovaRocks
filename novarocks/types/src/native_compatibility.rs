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

// Design: ADR-0121 (docs/adr/ADR-0121-native-compatibility-islands-and-ingress-admission.md)
//! Fixed-width identity for one complete Native execution compatibility contract.

use std::fmt;

/// Exact-width validation failure for a Native compatibility identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeCompatibilityIdError {
    InvalidLength { actual: usize },
}

impl fmt::Display for NativeCompatibilityIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { actual } => {
                write!(
                    formatter,
                    "native compatibility id must be 32 bytes, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for NativeCompatibilityIdError {}

/// Immutable exact identity for one Native compatibility island.
///
/// This value intentionally owns neither compatibility material nor admission
/// policy. Those belong to the build/version owner and role applications.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NativeCompatibilityId([u8; 32]);

impl NativeCompatibilityId {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, NativeCompatibilityIdError> {
        let bytes: [u8; 32] =
            bytes
                .try_into()
                .map_err(|_| NativeCompatibilityIdError::InvalidLength {
                    actual: bytes.len(),
                })?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for NativeCompatibilityId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{NativeCompatibilityId, NativeCompatibilityIdError};

    #[test]
    fn displays_exactly_sixty_four_lowercase_hex_characters() {
        let id = NativeCompatibilityId::new([0xab; 32]);

        assert_eq!(id.to_string(), "ab".repeat(32));
        assert_eq!(id.to_string().len(), 64);
    }

    #[test]
    fn rejects_non_exact_width_slices() {
        assert_eq!(
            NativeCompatibilityId::try_from_slice(&[0; 31]),
            Err(NativeCompatibilityIdError::InvalidLength { actual: 31 })
        );
        assert_eq!(
            NativeCompatibilityId::try_from_slice(&[0; 33]),
            Err(NativeCompatibilityIdError::InvalidLength { actual: 33 })
        );
    }
}
