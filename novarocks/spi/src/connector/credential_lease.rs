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

//! Secret-free query-attempt credential lease metadata.
//!
//! A descriptor is immutable attempt metadata and may be included in the
//! participant-manifest digest. Credential values intentionally do not appear
//! here: the native lifecycle confidential carrier owns their short-lived
//! transport and execution-side installation.

use super::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, StorageAccessDomainId,
    StorageCredentialScopePrefix,
};

pub const MAX_CREDENTIAL_LEASES_PER_QUERY: usize = 64;
pub const MAX_CREDENTIAL_LEASE_PREFIXES: usize = 64;
pub const MAX_CREDENTIAL_LEASE_ID_BYTES: usize = 16;
pub const MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES: usize = 8 * 1024;
pub const MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES: usize = 256 * 1024;

/// Stable, non-secret identity for one query-attempt credential lease.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CredentialLeaseId([u8; MAX_CREDENTIAL_LEASE_ID_BYTES]);

impl CredentialLeaseId {
    pub fn try_from_bytes(
        bytes: [u8; MAX_CREDENTIAL_LEASE_ID_BYTES],
    ) -> Result<Self, ConnectorError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(invalid("credential lease id"));
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; MAX_CREDENTIAL_LEASE_ID_BYTES] {
        &self.0
    }
}

/// Closed provider family accepted by the M2 v1 confidential lease carrier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CredentialLeaseProvider {
    S3,
}

/// Immutable, secret-free lease metadata frozen for one query participant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialLeaseDescriptor {
    lease_id: CredentialLeaseId,
    epoch: u64,
    owner: CatalogHandle,
    provider: CredentialLeaseProvider,
    prefixes: Vec<StorageCredentialScopePrefix>,
    not_after_unix_ms: u64,
    refresh_capable: bool,
    storage_access_domain_id: StorageAccessDomainId,
}

impl CredentialLeaseDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        lease_id: CredentialLeaseId,
        epoch: u64,
        owner: CatalogHandle,
        provider: CredentialLeaseProvider,
        mut prefixes: Vec<StorageCredentialScopePrefix>,
        not_after_unix_ms: u64,
        refresh_capable: bool,
        storage_access_domain_id: StorageAccessDomainId,
    ) -> Result<Self, ConnectorError> {
        if epoch == 0 {
            return Err(invalid("credential lease epoch"));
        }
        if prefixes.is_empty() || prefixes.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
            return Err(exhausted("credential lease prefix set"));
        }
        prefixes.sort();
        if prefixes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("duplicate credential lease prefix"));
        }
        if not_after_unix_ms == 0 {
            return Err(invalid("credential lease expiration"));
        }
        Ok(Self {
            lease_id,
            epoch,
            owner,
            provider,
            prefixes,
            not_after_unix_ms,
            refresh_capable,
            storage_access_domain_id,
        })
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn owner(&self) -> &CatalogHandle {
        &self.owner
    }

    pub const fn provider(&self) -> CredentialLeaseProvider {
        self.provider
    }

    pub fn prefixes(&self) -> &[StorageCredentialScopePrefix] {
        &self.prefixes
    }

    pub const fn not_after_unix_ms(&self) -> u64 {
        self.not_after_unix_ms
    }

    pub const fn refresh_capable(&self) -> bool {
        self.refresh_capable
    }

    pub const fn storage_access_domain_id(&self) -> StorageAccessDomainId {
        self.storage_access_domain_id
    }

    /// Refresh may advance only epoch and expiration. Any owner, provider,
    /// scope, or access-domain change is a new attempt, never a refresh.
    pub fn has_same_refresh_scope(&self, other: &Self) -> bool {
        self.lease_id == other.lease_id
            && self.owner == other.owner
            && self.provider == other.provider
            && self.prefixes == other.prefixes
            && self.refresh_capable == other.refresh_capable
            && self.storage_access_domain_id == other.storage_access_domain_id
    }
}

fn invalid(subject: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!("invalid {subject}"),
    )
}

fn exhausted(subject: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        format!("{subject} exceeds configured bounds"),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        CredentialLeaseDescriptor, CredentialLeaseId, CredentialLeaseProvider,
        MAX_CREDENTIAL_LEASE_PREFIXES,
    };
    use crate::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, StorageAccessDomainId,
        StorageCredentialScopePrefix,
    };

    fn owner() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("warehouse").expect("catalog"),
            CatalogVersion::from_bytes([7; 32]),
        )
    }

    fn prefix(value: &str) -> StorageCredentialScopePrefix {
        StorageCredentialScopePrefix::try_from_normalized(value).expect("prefix")
    }

    fn descriptor(
        epoch: u64,
        prefixes: Vec<StorageCredentialScopePrefix>,
    ) -> CredentialLeaseDescriptor {
        CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            epoch,
            owner(),
            CredentialLeaseProvider::S3,
            prefixes,
            10,
            true,
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("descriptor")
    }

    #[test]
    fn descriptor_canonicalizes_prefixes_and_refresh_scope_excludes_epoch_and_expiry() {
        let first = descriptor(1, vec![prefix("s3://bucket/z"), prefix("s3://bucket/a")]);
        assert_eq!(first.prefixes()[0].as_str(), "s3://bucket/a");
        let second = CredentialLeaseDescriptor::try_new(
            first.lease_id(),
            2,
            owner(),
            CredentialLeaseProvider::S3,
            first.prefixes().to_vec(),
            20,
            true,
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("refresh descriptor");
        assert!(first.has_same_refresh_scope(&second));
    }

    #[test]
    fn descriptor_rejects_empty_duplicate_and_overbound_prefix_sets() {
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                vec![],
                10,
                false,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                vec![prefix("s3://bucket/a"), prefix("s3://bucket/a")],
                10,
                false,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
        let prefixes = (0..=MAX_CREDENTIAL_LEASE_PREFIXES)
            .map(|index| prefix(&format!("s3://bucket/{index}")))
            .collect();
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                prefixes,
                10,
                false,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
    }
}
