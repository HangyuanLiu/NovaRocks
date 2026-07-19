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
        if value.get_version_num() != 7 {
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
    incarnation: ControlPlaneIncarnation,
    mode: ControlPlaneMode,
}

impl ControlPlaneSnapshot {
    pub(crate) const fn new(incarnation: ControlPlaneIncarnation, mode: ControlPlaneMode) -> Self {
        Self { incarnation, mode }
    }

    pub const fn incarnation(&self) -> ControlPlaneIncarnation {
        self.incarnation
    }

    pub const fn mode(&self) -> ControlPlaneMode {
        self.mode
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
