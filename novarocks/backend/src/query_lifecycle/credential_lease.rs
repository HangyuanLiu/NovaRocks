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

//! Backend-owned confidential credential lease state.
//!
//! Values live only in one query lifecycle entry.  The manifest supplies the
//! immutable descriptor; this module retains the corresponding envelope only
//! after TLS-aware ingress has admitted it.  It deliberately has no global
//! lookup path and every diagnostic is secret-free.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use novarocks_proto_codec::lifecycle::{CredentialLeaseSecretEnvelope, QueryInitRequest};
use novarocks_spi::connector::{CredentialLeaseDescriptor, CredentialLeaseId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CredentialLeaseError {
    Invalid,
    Conflict,
    Terminated,
}

impl CredentialLeaseError {
    pub(crate) const fn detail(self) -> &'static str {
        match self {
            Self::Invalid => "credential lease contribution is invalid",
            Self::Conflict => "credential lease contribution conflicts with lifecycle state",
            Self::Terminated => "query lifecycle is no longer active for credential lease update",
        }
    }
}

/// One committed descriptor/value pair and an optional invisible refresh.
/// The envelope's manual Debug implementation redacts all secret scalars.
struct LeaseSlot {
    descriptor: CredentialLeaseDescriptor,
    committed: CredentialLeaseSecretEnvelope,
    prepared: Option<CredentialLeaseSecretEnvelope>,
    in_flight_users: u32,
}

impl fmt::Debug for LeaseSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseSlot")
            .field("lease_id", &self.descriptor.lease_id())
            .field("committed_epoch", &self.committed.epoch())
            .field(
                "prepared_epoch",
                &self
                    .prepared
                    .as_ref()
                    .map(CredentialLeaseSecretEnvelope::epoch),
            )
            .field("prefix_count", &self.descriptor.prefixes().len())
            .field("in_flight_users", &self.in_flight_users)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

/// Attempt-local secret state.  Tombstones never contain this type: terminal
/// cleanup clears it before a lifecycle entry can be retained or evicted.
#[derive(Default)]
pub(crate) struct QueryCredentialLeases {
    slots: BTreeMap<CredentialLeaseId, LeaseSlot>,
}

impl fmt::Debug for QueryCredentialLeases {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryCredentialLeases")
            .field("lease_count", &self.slots.len())
            .field("leases", &self.slots.values().collect::<Vec<_>>())
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl QueryCredentialLeases {
    pub(crate) fn from_tls_init(request: &QueryInitRequest) -> Result<Self, CredentialLeaseError> {
        let manifest = request
            .manifest()
            .map_err(|_| CredentialLeaseError::Invalid)?;
        let descriptors = manifest
            .credential_lease_descriptors()
            .map_err(|_| CredentialLeaseError::Invalid)?;
        let envelopes = request
            .credential_lease_envelopes()
            .map_err(|_| CredentialLeaseError::Invalid)?;
        Self::from_descriptor_envelopes(descriptors, envelopes)
    }

    fn from_descriptor_envelopes(
        descriptors: Vec<CredentialLeaseDescriptor>,
        envelopes: Vec<CredentialLeaseSecretEnvelope>,
    ) -> Result<Self, CredentialLeaseError> {
        if descriptors.len() != envelopes.len() {
            return Err(CredentialLeaseError::Invalid);
        }
        let now = unix_ms();
        let mut slots = BTreeMap::new();
        for (descriptor, envelope) in descriptors.into_iter().zip(envelopes) {
            if !envelope.matches_descriptor(&descriptor)
                || descriptor.not_after_unix_ms() <= now
                || envelope.session_token_expires_at_unix_ms() <= now
                || slots
                    .insert(
                        descriptor.lease_id(),
                        LeaseSlot {
                            descriptor,
                            committed: envelope,
                            prepared: None,
                            in_flight_users: 0,
                        },
                    )
                    .is_some()
            {
                return Err(CredentialLeaseError::Invalid);
            }
        }
        Ok(Self { slots })
    }

    /// Retry equality compares every scalar without turning any value into a
    /// digest, log field, or metric label.  All caller-visible failures use
    /// the same conflict shape.
    pub(crate) fn same_initial_values(&self, other: &Self) -> bool {
        if self.slots.len() != other.slots.len() {
            return false;
        }
        self.slots.iter().fold(true, |equal, (lease_id, left)| {
            let Some(right) = other.slots.get(lease_id) else {
                return false;
            };
            equal
                & descriptor_eq(&left.descriptor, &right.descriptor)
                & envelope_eq(&left.committed, &right.committed)
        })
    }

    pub(crate) fn prepare(
        &mut self,
        envelope: CredentialLeaseSecretEnvelope,
    ) -> Result<u64, CredentialLeaseError> {
        let now = unix_ms();
        let slot = self
            .slots
            .get_mut(&envelope.lease_id())
            .ok_or(CredentialLeaseError::Conflict)?;
        if !slot.descriptor.refresh_capable() || envelope.session_token_expires_at_unix_ms() <= now
        {
            return Err(CredentialLeaseError::Invalid);
        }
        let expected_epoch = slot
            .committed
            .epoch()
            .checked_add(1)
            .ok_or(CredentialLeaseError::Invalid)?;
        if envelope.epoch() != expected_epoch {
            if slot.prepared.as_ref().is_some_and(|prepared| {
                prepared.epoch() == envelope.epoch() && envelope_eq(prepared, &envelope)
            }) {
                return Ok(envelope.epoch());
            }
            return Err(CredentialLeaseError::Conflict);
        }
        match &slot.prepared {
            None => {
                slot.prepared = Some(envelope);
                Ok(expected_epoch)
            }
            Some(prepared) if envelope_eq(prepared, &envelope) => Ok(expected_epoch),
            Some(_) => Err(CredentialLeaseError::Conflict),
        }
    }

    pub(crate) fn commit(
        &mut self,
        lease_id: CredentialLeaseId,
        epoch: u64,
    ) -> Result<u64, CredentialLeaseError> {
        let slot = self
            .slots
            .get_mut(&lease_id)
            .ok_or(CredentialLeaseError::Conflict)?;
        if slot.committed.epoch() == epoch {
            return Ok(epoch);
        }
        let prepared = slot.prepared.take().ok_or(CredentialLeaseError::Conflict)?;
        if prepared.epoch() != epoch {
            slot.prepared = Some(prepared);
            return Err(CredentialLeaseError::Conflict);
        }
        slot.committed = prepared;
        Ok(epoch)
    }

    pub(crate) fn clear(&mut self) {
        self.slots.clear();
    }

    #[cfg(test)]
    pub(crate) fn committed_epoch(&self, lease_id: CredentialLeaseId) -> Option<u64> {
        self.slots.get(&lease_id).map(|slot| slot.committed.epoch())
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn descriptor_eq(left: &CredentialLeaseDescriptor, right: &CredentialLeaseDescriptor) -> bool {
    left == right
}

fn envelope_eq(
    left: &CredentialLeaseSecretEnvelope,
    right: &CredentialLeaseSecretEnvelope,
) -> bool {
    let metadata_matches = left.lease_id() == right.lease_id()
        && left.epoch() == right.epoch()
        && left.session_token_expires_at_unix_ms() == right.session_token_expires_at_unix_ms();
    let secret_matches = secret_eq(
        left.access_key_id().expose_secret(),
        right.access_key_id().expose_secret(),
    ) & secret_eq(
        left.secret_access_key().expose_secret(),
        right.secret_access_key().expose_secret(),
    ) & secret_eq(
        left.session_token().expose_secret(),
        right.session_token().expose_secret(),
    );
    metadata_matches & secret_matches
}

/// Constant-work over the maximum scalar length.  It is intentionally local:
/// the only observable result is the common conflict outcome.
fn secret_eq(left: &str, right: &str) -> bool {
    let length = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..length {
        let left_byte = left.as_bytes().get(index).copied().unwrap_or(0);
        let right_byte = right.as_bytes().get(index).copied().unwrap_or(0);
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_proto_codec::lifecycle::CredentialLeaseSecretEnvelope;
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseProvider,
        StorageAccessDomainId, StorageCredentialScopePrefix,
    };

    fn descriptor(epoch: u64, expiration: u64) -> CredentialLeaseDescriptor {
        CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([9; 16]).expect("id"),
            epoch,
            CatalogHandle::new(
                ConnectorInstanceId::parse("lease-test").expect("connector"),
                CatalogVersion::from_bytes([4; 32]),
            ),
            CredentialLeaseProvider::S3,
            vec![
                StorageCredentialScopePrefix::try_from_normalized("s3://bucket/a").expect("prefix"),
            ],
            expiration,
            true,
            StorageAccessDomainId::from_bytes([3; 32]),
        )
        .expect("descriptor")
    }

    fn envelope(epoch: u64, expiration: u64, token: &str) -> CredentialLeaseSecretEnvelope {
        CredentialLeaseSecretEnvelope::try_new(
            CredentialLeaseId::try_from_bytes([9; 16]).expect("id"),
            epoch,
            SecretValue::new("access"),
            SecretValue::new("secret"),
            SecretValue::new(token),
            expiration,
        )
        .expect("envelope")
    }

    #[test]
    fn prepare_is_invisible_until_matching_commit() {
        let expiration = unix_ms() + 60_000;
        let mut leases = QueryCredentialLeases::from_descriptor_envelopes(
            vec![descriptor(1, expiration)],
            vec![envelope(1, expiration, "first")],
        )
        .expect("initial lease");
        let id = CredentialLeaseId::try_from_bytes([9; 16]).expect("id");
        assert_eq!(leases.prepare(envelope(2, expiration + 1, "second")), Ok(2));
        assert_eq!(leases.committed_epoch(id), Some(1));
        assert_eq!(leases.commit(id, 2), Ok(2));
        assert_eq!(leases.committed_epoch(id), Some(2));
    }

    #[test]
    fn conflicting_prepare_never_exposes_material() {
        let expiration = unix_ms() + 60_000;
        let mut leases = QueryCredentialLeases::from_descriptor_envelopes(
            vec![descriptor(1, expiration)],
            vec![envelope(1, expiration, "first")],
        )
        .expect("initial lease");
        assert_eq!(leases.prepare(envelope(2, expiration + 1, "next")), Ok(2));
        assert_eq!(
            leases.prepare(envelope(2, expiration + 1, "different")),
            Err(CredentialLeaseError::Conflict)
        );
        assert!(!format!("{leases:?}").contains("different"));
    }

    #[test]
    fn teardown_drops_committed_and_prepared_values() {
        let expiration = unix_ms() + 60_000;
        let id = CredentialLeaseId::try_from_bytes([9; 16]).expect("id");
        let mut leases = QueryCredentialLeases::from_descriptor_envelopes(
            vec![descriptor(1, expiration)],
            vec![envelope(1, expiration, "committed-canary")],
        )
        .expect("initial lease");
        leases
            .prepare(envelope(2, expiration + 1, "prepared-canary"))
            .expect("prepare");
        leases.clear();
        assert_eq!(leases.committed_epoch(id), None);
        let rendered = format!("{leases:?}");
        assert!(!rendered.contains("committed-canary"));
        assert!(!rendered.contains("prepared-canary"));
    }
}
