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

//! One Worker's installed vended credentials for one query context.
//!
//! Native decoding remains outside this crate.  This owner receives already
//! validated SPI lease pairs, atomically replaces its local table, and is the
//! only place that resolves an attempt-local storage request.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use novarocks_execution_contract::task_execution::status::TaskFailureCategory;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorStorageResolver, CredentialLeaseDescriptor,
    CredentialLeaseId, CredentialLeaseProvider, CredentialLeaseSecretEnvelope,
    ResolvedVendedS3Access, StorageAccessRequest, StorageCredentialScopePrefix,
    VendedCredentialLease,
};

use crate::HostRejection;

struct InstalledLease {
    descriptor: CredentialLeaseDescriptor,
    envelope: CredentialLeaseSecretEnvelope,
}

impl fmt::Debug for InstalledLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledLease")
            .field("lease_id", &self.descriptor.lease_id())
            .field("epoch", &self.descriptor.epoch())
            .field("material", &"[REDACTED]")
            .finish()
    }
}

/// The installed vended credentials of one Worker query context.
#[derive(Default)]
pub struct QueryContextCredentialSlot {
    leases: RwLock<BTreeMap<CredentialLeaseId, InstalledLease>>,
}

impl fmt::Debug for QueryContextCredentialSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let leases = self
            .leases
            .read()
            .unwrap_or_else(|error| error.into_inner());
        formatter
            .debug_struct("QueryContextCredentialSlot")
            .field("lease_count", &leases.len())
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl QueryContextCredentialSlot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces every installed lease with one fully decoded rotation.
    ///
    /// A refusal leaves the prior table intact, so an invalid rotation cannot
    /// turn a still-live context into an empty credential authority.
    pub fn install(&self, material: &[VendedCredentialLease]) -> Result<(), HostRejection> {
        let now = unix_ms();
        let mut installed = BTreeMap::new();
        for lease in material {
            let lease = validate(lease, now)?;
            if installed
                .insert(lease.descriptor.lease_id(), lease)
                .is_some()
            {
                return Err(rejected("credential rotation repeats a lease id"));
            }
        }
        *self
            .leases
            .write()
            .unwrap_or_else(|error| error.into_inner()) = installed;
        Ok(())
    }

    /// Drops every installed lease.  Release and failed-establish unwind are
    /// both idempotent and no other owner can retain the material.
    pub fn clear(&self) {
        self.leases
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }

    /// Resolves one process-local vended S3 target by exact catalog owner,
    /// unexpired lease, and longest matching scope prefix.
    pub fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        let now = unix_ms();
        let leases = self
            .leases
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let mut selected: Option<(&InstalledLease, &StorageCredentialScopePrefix)> = None;
        for lease in leases.values() {
            if lease.descriptor.provider() != CredentialLeaseProvider::S3
                || lease.descriptor.owner() != request.owner()
                || lease.envelope.session_token_expires_at_unix_ms() <= now
            {
                continue;
            }
            for prefix in lease.descriptor.prefixes() {
                if !request.location().starts_with(prefix.as_str()) {
                    continue;
                }
                if selected
                    .is_none_or(|(_, current)| prefix.as_str().len() > current.as_str().len())
                {
                    selected = Some((lease, prefix));
                }
            }
        }
        let (lease, matched_prefix) = selected.ok_or_else(vended_storage_access_denied)?;
        Ok(ResolvedVendedS3Access::new(
            lease.descriptor.storage_access_domain_id(),
            lease.descriptor.lease_id(),
            lease.envelope.epoch(),
            matched_prefix.clone(),
            lease.descriptor.credentials_endpoint().map(Arc::from),
            lease.envelope.session_token_expires_at_unix_ms(),
            lease.envelope.access_key_id().clone(),
            lease.envelope.secret_access_key().clone(),
            lease.envelope.session_token().clone(),
        ))
    }

    pub fn installed_epoch(&self, lease_id: CredentialLeaseId) -> Option<u64> {
        self.leases
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&lease_id)
            .map(|lease| lease.envelope.epoch())
    }

    pub fn installed_lease_count(&self) -> usize {
        self.leases
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }
}

impl ConnectorStorageResolver for QueryContextCredentialSlot {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        Self::resolve_vended_s3(self, request)
    }
}

fn validate(lease: &VendedCredentialLease, now: u64) -> Result<InstalledLease, HostRejection> {
    let descriptor = lease.descriptor();
    let envelope = lease.envelope();
    if !envelope.matches_descriptor(descriptor) {
        return Err(rejected(
            "credential envelope does not match the scope it claims",
        ));
    }
    if descriptor.not_after_unix_ms() <= now || envelope.session_token_expires_at_unix_ms() <= now {
        return Err(rejected("credential lease is already expired"));
    }
    Ok(InstalledLease {
        descriptor: descriptor.clone(),
        envelope: envelope.clone(),
    })
}

fn rejected(detail: &str) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Protocol, detail)
}

fn vended_storage_access_denied() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "vended storage access is unavailable for this query attempt",
    )
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{QueryContextCredentialSlot, unix_ms};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
        CredentialLeaseId, CredentialLeaseProvider, CredentialLeaseSecretEnvelope,
        StorageAccessDomainId, StorageAccessRequest, StorageCredentialScopePrefix,
        VendedCredentialLease,
    };

    const SECRET_SENTINEL: &str = "NOVAROCKS_SECRET_SENTINEL";

    fn owner() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("warehouse").expect("legal instance id"),
            CatalogVersion::from_bytes([7; 32]),
        )
    }

    fn lease_id(seed: u8) -> CredentialLeaseId {
        CredentialLeaseId::try_from_bytes([seed; 16]).expect("legal lease id")
    }

    fn prefix(value: &str) -> StorageCredentialScopePrefix {
        StorageCredentialScopePrefix::try_from_normalized(value).expect("legal prefix")
    }

    fn live() -> u64 {
        unix_ms() + 600_000
    }

    fn lease(
        seed: u8,
        epoch: u64,
        prefixes: &[&str],
        not_after: u64,
        secret: &str,
    ) -> VendedCredentialLease {
        let descriptor = CredentialLeaseDescriptor::try_new(
            lease_id(seed),
            epoch,
            owner(),
            CredentialLeaseProvider::S3,
            prefixes.iter().copied().map(prefix).collect(),
            not_after,
            true,
            None,
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("legal descriptor");
        let envelope = CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
            lease_id(seed),
            epoch,
            "access-key".to_owned(),
            secret.to_owned(),
            "session-token".to_owned(),
            not_after,
        )
        .expect("legal envelope");
        VendedCredentialLease::try_new(descriptor, envelope).expect("matching lease")
    }

    fn request(location: &str) -> StorageAccessRequest {
        StorageAccessRequest::try_new(owner(), location).expect("legal storage request")
    }

    /// Same lease, but with an acquisition address announced.
    fn announcing_lease(seed: u8, prefixes: &[&str], endpoint: &str) -> VendedCredentialLease {
        let not_after = live();
        let descriptor = CredentialLeaseDescriptor::try_new(
            lease_id(seed),
            1,
            owner(),
            CredentialLeaseProvider::S3,
            prefixes.iter().copied().map(prefix).collect(),
            not_after,
            true,
            Some(Arc::from(endpoint)),
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("legal descriptor");
        let envelope = CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
            lease_id(seed),
            1,
            "access-key".to_owned(),
            SECRET_SENTINEL.to_owned(),
            "session-token".to_owned(),
            not_after,
        )
        .expect("legal envelope");
        VendedCredentialLease::try_new(descriptor, envelope).expect("matching lease")
    }

    #[test]
    fn a_selection_carries_the_acquisition_address_of_the_lease_it_selected() {
        // The address reaches the consumer through the selection, not through
        // a second lookup: a node that had to re-derive it could disagree with
        // the lease it is actually holding (CAD-1 D1).
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[
            announcing_lease(1, &["s3://bucket/a"], "https://rest/v1/a/credentials"),
            lease(2, 1, &["s3://bucket/b"], live(), "second"),
        ])
        .expect("install");

        let announcing = slot
            .resolve_vended_s3(&request("s3://bucket/a/x"))
            .expect("selection");
        assert_eq!(
            announcing.credentials_endpoint(),
            Some("https://rest/v1/a/credentials")
        );

        // Absence is the seeded-without-renewal shape, and it must stay
        // distinguishable from the announcing lease installed beside it.
        let silent = slot
            .resolve_vended_s3(&request("s3://bucket/b/x"))
            .expect("selection");
        assert_eq!(silent.credentials_endpoint(), None);
    }

    #[test]
    fn a_rotation_replaces_the_whole_table_in_one_move() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&[
            lease(1, 1, &["s3://bucket/a"], expiry, SECRET_SENTINEL),
            lease(2, 1, &["s3://bucket/b"], expiry, "second"),
        ])
        .expect("initial install");
        slot.install(&[lease(1, 2, &["s3://bucket/a"], expiry, "rotated")])
            .expect("rotation install");
        assert_eq!(slot.installed_lease_count(), 1);
        assert_eq!(slot.installed_epoch(lease_id(1)), Some(2));
        assert_eq!(slot.installed_epoch(lease_id(2)), None);
        assert!(slot.resolve_vended_s3(&request("s3://bucket/b/x")).is_err());
    }

    #[test]
    fn longest_matching_prefix_wins_and_foreign_location_is_refused() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&[
            lease(1, 1, &["s3://bucket"], expiry, "broad"),
            lease(2, 1, &["s3://bucket/deep/nested"], expiry, "narrow"),
        ])
        .expect("install");
        let resolved = slot
            .resolve_vended_s3(&request("s3://bucket/deep/nested/file.parquet"))
            .expect("nested location resolves");
        assert_eq!(resolved.lease_id(), lease_id(2));
        assert_eq!(
            resolved.matched_prefix(),
            &prefix("s3://bucket/deep/nested")
        );
        assert!(
            slot.resolve_vended_s3(&request("s3://other/file.parquet"))
                .is_err()
        );
    }

    #[test]
    fn refused_rotation_leaves_the_installed_epoch_serving() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&[lease(1, 1, &["s3://bucket/a"], expiry, SECRET_SENTINEL)])
            .expect("install");
        let rejection = slot
            .install(&[lease(1, 2, &["s3://bucket/a"], 1, "rotated")])
            .expect_err("expired rotation is refused");
        assert!(rejection.detail().as_str().contains("expired"));
        assert_eq!(slot.installed_epoch(lease_id(1)), Some(1));
        assert!(slot.resolve_vended_s3(&request("s3://bucket/a/x")).is_ok());
    }

    #[test]
    fn material_never_appears_in_any_rendering() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&[lease(1, 1, &["s3://bucket/a"], expiry, SECRET_SENTINEL)])
            .expect("install");
        let rendered = format!("{slot:?}");
        assert!(!rendered.contains(SECRET_SENTINEL));
        assert!(rendered.contains("[REDACTED]"));
        let refused = slot
            .install(&[lease(1, 2, &["s3://bucket/a"], 1, SECRET_SENTINEL)])
            .map(|()| String::new())
            .unwrap_or_else(|rejection| rejection.to_string());
        assert!(!refused.contains(SECRET_SENTINEL));
    }
}
