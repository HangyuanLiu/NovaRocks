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

//! Minimal redacted scalar for already-resolved secret values.
//!
//! This crate owns no credential, configuration-source, or runtime semantics.
//! `SecretValue` does not promise memory zeroization.

use std::fmt;

/// An opaque secret scalar that requires an explicit exposure at the consumer boundary.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct SecretValue(String);

impl SecretValue {
    /// Wraps an already-resolved secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns whether this secret has no characters.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Exposes the secret to the concrete consumer that must use it.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue(REDACTED)")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use super::SecretValue;

    #[test]
    fn debug_redacts_the_value() {
        let secret = SecretValue::new("nwt-1-secret-canary");

        assert!(!format!("{secret:?}").contains("nwt-1-secret-canary"));
    }

    #[test]
    fn clone_equality_and_hash_preserve_value_semantics() {
        let secret = SecretValue::new("nwt-1-secret-canary");
        let clone = secret.clone();
        let distinct = SecretValue::new("another-secret");

        assert_eq!(secret, clone);
        assert_ne!(secret, distinct);
        assert_eq!(hash_of(&secret), hash_of(&clone));
        assert_eq!(secret.expose_secret(), "nwt-1-secret-canary");
        assert!(!secret.is_empty());
        assert!(SecretValue::new("").is_empty());
    }

    fn hash_of(value: &SecretValue) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
}
