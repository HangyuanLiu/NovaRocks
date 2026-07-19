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

use std::time::Duration;

use bytes::Bytes;
use uuid::Uuid;

use super::error::CoordinationError;
use crate::{OperationId, VersionToken};

const MAX_RESOURCE_KEY_BYTES: usize = 8 * 1024;
const MAX_HOLDER_ID_BYTES: usize = 256;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceKey(Bytes);

impl ResourceKey {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl TryFrom<Bytes> for ResourceKey {
    type Error = CoordinationError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(CoordinationError::invalid_request(
                "resource key must not be empty",
            ));
        }
        if value.len() > MAX_RESOURCE_KEY_BYTES {
            return Err(CoordinationError::limit_exceeded(
                "resource key exceeds the coordination byte limit",
            ));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HolderId(Bytes);

impl HolderId {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl TryFrom<Bytes> for HolderId {
    type Error = CoordinationError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(CoordinationError::invalid_request(
                "holder id must not be empty",
            ));
        }
        if value.len() > MAX_HOLDER_ID_BYTES {
            return Err(CoordinationError::limit_exceeded(
                "holder id exceeds the coordination byte limit",
            ));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AttemptId(Uuid);

impl AttemptId {
    pub(crate) const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl TryFrom<Uuid> for AttemptId {
    type Error = CoordinationError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        if value.get_version_num() != 7 || value.get_variant() != uuid::Variant::RFC4122 {
            return Err(CoordinationError::invalid_request(
                "lease attempt id must be UUIDv7",
            ));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ControlPlaneIncarnation(u64);

impl ControlPlaneIncarnation {
    pub fn new(value: u64) -> Result<Self, CoordinationError> {
        if value == 0 {
            return Err(CoordinationError::invalid_request(
                "control plane incarnation must be nonzero",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn checked_next(self) -> Result<Self, CoordinationError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(CoordinationError::incarnation_exhausted)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceEpoch(u64);

impl ResourceEpoch {
    pub fn new(value: u64) -> Result<Self, CoordinationError> {
        if value == 0 {
            return Err(CoordinationError::invalid_request(
                "resource epoch must be nonzero",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn checked_next(self) -> Result<Self, CoordinationError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(CoordinationError::epoch_exhausted)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FencingToken {
    cluster_id: String,
    control_plane_incarnation: ControlPlaneIncarnation,
    resource_epoch: ResourceEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseFence {
    pub(crate) resource: ResourceKey,
    pub(crate) holder: HolderId,
    pub(crate) attempt: AttemptId,
    pub(crate) token: FencingToken,
    pub(crate) record_version: VersionToken,
}

impl FencingToken {
    pub fn new(
        cluster_id: impl Into<String>,
        control_plane_incarnation: ControlPlaneIncarnation,
        resource_epoch: ResourceEpoch,
    ) -> Result<Self, CoordinationError> {
        let cluster_id = cluster_id.into();
        if cluster_id.is_empty() {
            return Err(CoordinationError::invalid_request(
                "cluster id must not be empty",
            ));
        }
        Ok(Self {
            cluster_id,
            control_plane_incarnation,
            resource_epoch,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn control_plane_incarnation(&self) -> ControlPlaneIncarnation {
        self.control_plane_incarnation
    }

    pub const fn resource_epoch(&self) -> ResourceEpoch {
        self.resource_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ControlPlaneMode {
    Reconciling,
    WriteOpen,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlaneSnapshot {
    store_id: Uuid,
    cluster_id: String,
    incarnation: ControlPlaneIncarnation,
    mode: ControlPlaneMode,
    last_operation_id: OperationId,
    version: VersionToken,
}

impl ControlPlaneSnapshot {
    pub(crate) fn new(
        store_id: Uuid,
        cluster_id: String,
        incarnation: ControlPlaneIncarnation,
        mode: ControlPlaneMode,
        last_operation_id: OperationId,
        version: VersionToken,
    ) -> Self {
        Self {
            store_id,
            cluster_id,
            incarnation,
            mode,
            last_operation_id,
            version,
        }
    }

    pub const fn incarnation(&self) -> ControlPlaneIncarnation {
        self.incarnation
    }

    pub const fn mode(&self) -> ControlPlaneMode {
        self.mode
    }

    pub(crate) const fn store_id(&self) -> Uuid {
        self.store_id
    }

    pub(crate) fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub(crate) const fn last_operation_id(&self) -> OperationId {
        self.last_operation_id
    }

    pub(crate) fn version(&self) -> &VersionToken {
        &self.version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseObservation {
    token: FencingToken,
    retry_after: Duration,
}

impl LeaseObservation {
    pub(crate) const fn new(token: FencingToken, retry_after: Duration) -> Self {
        Self { token, retry_after }
    }

    pub const fn token(&self) -> &FencingToken {
        &self.token
    }

    pub const fn retry_after(&self) -> Duration {
        self.retry_after
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LeaseCancellationReason {
    FenceLost,
    IncarnationChanged,
    ClockUnsafe,
    Released,
}

#[cfg(test)]
mod tests {
    use uuid::{Uuid, Variant};

    use super::AttemptId;

    #[test]
    fn attempt_id_rejects_v7_uuids_with_non_rfc4122_variants() {
        for variant_byte in [0x00, 0xc0, 0xe0] {
            let mut bytes = [0; 16];
            bytes[6] = 0x70;
            bytes[8] = variant_byte;
            let attempt = Uuid::from_bytes(bytes);

            assert_eq!(attempt.get_version_num(), 7);
            assert_ne!(attempt.get_variant(), Variant::RFC4122);
            assert!(AttemptId::try_from(attempt).is_err());
        }
    }
}
