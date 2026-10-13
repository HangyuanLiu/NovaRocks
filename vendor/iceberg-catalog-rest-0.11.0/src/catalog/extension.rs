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

//! NovaRocks protocol surfaces implemented by the session catalog.

use super::*;

/// A redacted view of one prefix-scoped REST storage credential.
///
/// Credential values remain private. Consumers can inspect only the keys they
/// understand and retrieve a value by its exact key while translating this
/// response into their own closed credential type.
pub struct StorageCredentialDelegation<'a> {
    credential: &'a crate::types::StorageCredential,
}

impl StorageCredentialDelegation<'_> {
    /// Prefix to which this credential applies.
    pub fn prefix(&self) -> &str {
        &self.credential.prefix
    }

    /// Return the value for one known provider configuration key.
    pub fn config_value(&self, key: &str) -> Option<&str> {
        self.credential.config.get(key).map(String::as_str)
    }

    /// Return the available provider configuration key names, never values.
    pub fn config_keys(&self) -> impl Iterator<Item = &str> {
        self.credential.config.keys().map(String::as_str)
    }
}

impl Debug for StorageCredentialDelegation<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageCredentialDelegation")
            .field("prefix", &self.credential.prefix)
            .field(
                "config_keys",
                &self.credential.config.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Storage delegation facts received from one REST table response.
///
/// This is intentionally not serializable and has no accessor that returns a
/// raw configuration map. It is a short-lived handoff from the vendored REST
/// client to the provider-owned parser.
pub struct RestAccessDelegation {
    storage_credentials: Option<Vec<crate::types::StorageCredential>>,
}

impl RestAccessDelegation {
    fn new(storage_credentials: Option<Vec<crate::types::StorageCredential>>) -> Self {
        Self {
            storage_credentials,
        }
    }

    /// Whether the REST response carried the `storage-credentials` member.
    pub fn is_present(&self) -> bool {
        self.storage_credentials.is_some()
    }

    /// Number of prefix-scoped credentials in the response.
    pub fn len(&self) -> usize {
        self.storage_credentials.as_ref().map_or(0, Vec::len)
    }

    /// Whether no prefix-scoped credentials were returned.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over redacted prefix-scoped credential views.
    pub fn credentials(&self) -> impl Iterator<Item = StorageCredentialDelegation<'_>> {
        self.storage_credentials
            .iter()
            .flatten()
            .map(|credential| StorageCredentialDelegation { credential })
    }
}

impl Debug for RestAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestAccessDelegation")
            .field("present", &self.is_present())
            .field("credential_count", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    //! Credential-handoff correctness coverage: exact-key reads preserve the
    //! response while Debug never reveals values. This isolated in-memory test
    //! follows the closed handoff contract and runs with the REST crate suite;
    //! the NovaRocks connector maintainer owns it until IRU-13a replaces R6.
    use super::*;

    #[test]
    fn storage_delegation_exposes_exact_keys_without_debug_secrets() {
        let delegation = RestAccessDelegation::new(Some(vec![crate::types::StorageCredential {
            prefix: "s3://warehouse/data/".into(),
            config: HashMap::from([
                ("s3.access-key-id".into(), "access-canary".into()),
                ("s3.secret-access-key".into(), "secret-canary".into()),
            ]),
        }]));
        assert!(delegation.is_present());
        assert_eq!(delegation.len(), 1);
        let credential = delegation.credentials().next().unwrap();
        assert_eq!(credential.prefix(), "s3://warehouse/data/");
        assert_eq!(
            credential.config_value("s3.access-key-id"),
            Some("access-canary")
        );
        assert_eq!(credential.config_value("unknown"), None);
        assert!(!format!("{delegation:?}").contains("secret-canary"));
        assert!(!format!("{credential:?}").contains("secret-canary"));
        assert!(format!("{credential:?}").contains("s3.secret-access-key"));
        assert!(!RestAccessDelegation::new(None).is_present());
        assert!(RestAccessDelegation::new(Some(vec![])).is_empty());
    }
}
