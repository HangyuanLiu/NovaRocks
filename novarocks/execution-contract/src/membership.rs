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

//! Transport-neutral backend membership facts.
//!
//! Native adapters validate and project their generated membership messages
//! into these values. Role-local topology owners and query coordination retain
//! only this immutable vocabulary, never a protobuf wrapper.

use std::fmt;

use novarocks_types::{BackendProcessId, NativeCompatibilityId};

use crate::RuntimeEndpoint;
use crate::native_result_support::BoundedRootSupport;

const MAX_DEPLOYMENT_ID_BYTES: usize = 256;
const MAX_BUILD_IDENTITY_BYTES: usize = 256;

/// The only backend liveness states a membership authority may admit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendReportedState {
    Running,
    Draining,
}

/// Immutable process facts announced by a backend and confirmed by heartbeat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendProcessDescriptor {
    process_id: BackendProcessId,
    endpoint: RuntimeEndpoint,
    control_endpoint: RuntimeEndpoint,
    deployment_id: String,
    build_identity: String,
    native_compatibility_id: NativeCompatibilityId,
    preparing_positions: usize,
    bounded_root_support: Option<BoundedRootSupport>,
}

impl BackendProcessDescriptor {
    pub fn try_new(
        process_id: BackendProcessId,
        endpoint: RuntimeEndpoint,
        control_endpoint: RuntimeEndpoint,
        deployment_id: impl Into<String>,
        build_identity: impl Into<String>,
        native_compatibility_id: NativeCompatibilityId,
        preparing_positions: usize,
    ) -> Result<Self, BackendProcessDescriptorError> {
        if endpoint == control_endpoint {
            return Err(BackendProcessDescriptorError::IndependentEndpoints);
        }
        if preparing_positions == 0 {
            return Err(BackendProcessDescriptorError::PreparingPositions);
        }
        let deployment_id = deployment_id.into();
        validate_text(
            &deployment_id,
            MAX_DEPLOYMENT_ID_BYTES,
            BackendProcessDescriptorError::DeploymentId,
        )?;
        let build_identity = build_identity.into();
        validate_text(
            &build_identity,
            MAX_BUILD_IDENTITY_BYTES,
            BackendProcessDescriptorError::BuildIdentity,
        )?;
        Ok(Self {
            process_id,
            endpoint,
            control_endpoint,
            deployment_id,
            build_identity,
            native_compatibility_id,
            preparing_positions,
            bounded_root_support: None,
        })
    }

    /// Optional root support must use this process's already-frozen control
    /// endpoint. Configuring independent endpoints does not advertise support.
    pub fn with_bounded_root_support(
        mut self,
        support: BoundedRootSupport,
    ) -> Result<Self, BackendProcessDescriptorError> {
        if support.control_endpoint() != &self.control_endpoint {
            return Err(BackendProcessDescriptorError::ConflictingControlEndpoint);
        }
        if self
            .bounded_root_support
            .as_ref()
            .is_some_and(|current| current != &support)
        {
            return Err(BackendProcessDescriptorError::ConflictingRootSupport);
        }
        self.bounded_root_support = Some(support);
        Ok(self)
    }
    pub fn bounded_root_support(&self) -> Option<&BoundedRootSupport> {
        self.bounded_root_support.as_ref()
    }
    pub fn require_bounded_root_support(
        &self,
    ) -> Result<&BoundedRootSupport, BackendProcessDescriptorError> {
        self.bounded_root_support
            .as_ref()
            .ok_or(BackendProcessDescriptorError::MissingRootSupport)
    }
    pub const fn process_id(&self) -> BackendProcessId {
        self.process_id
    }

    pub fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    pub fn control_endpoint(&self) -> &RuntimeEndpoint {
        &self.control_endpoint
    }

    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }

    pub fn build_identity(&self) -> &str {
        &self.build_identity
    }

    /// Immutable per-Context preparation positions advertised by this process.
    pub const fn preparing_positions(&self) -> usize {
        self.preparing_positions
    }

    pub const fn native_compatibility_id(&self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendProcessDescriptorError {
    PreparingPositions,
    IndependentEndpoints,
    ConflictingControlEndpoint,
    ConflictingRootSupport,
    MissingRootSupport,
    DeploymentId,
    BuildIdentity,
}

impl fmt::Display for BackendProcessDescriptorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IndependentEndpoints => {
                formatter.write_str("backend control and data endpoints must be distinct")
            }
            Self::ConflictingControlEndpoint => formatter
                .write_str("bounded root support must use the frozen backend control endpoint"),
            Self::ConflictingRootSupport => {
                formatter.write_str("backend root support is immutable")
            }
            Self::MissingRootSupport => {
                formatter.write_str("backend does not advertise bounded root delivery")
            }
            Self::PreparingPositions => {
                formatter.write_str("backend preparation positions must be positive")
            }
            Self::DeploymentId => {
                formatter.write_str("deployment id must be non-empty and at most 256 bytes")
            }
            Self::BuildIdentity => {
                formatter.write_str("build identity must be non-empty and at most 256 bytes")
            }
        }
    }
}

impl std::error::Error for BackendProcessDescriptorError {}

fn validate_text(
    value: &str,
    maximum_bytes: usize,
    error: BackendProcessDescriptorError,
) -> Result<(), BackendProcessDescriptorError> {
    if value.trim().is_empty() || value.len() > maximum_bytes {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BackendProcessDescriptor, BackendProcessDescriptorError};
    use crate::RuntimeEndpoint;
    use novarocks_types::{BackendProcessId, NativeCompatibilityId};

    #[test]
    fn descriptor_rejects_empty_or_oversized_identity_text() {
        let endpoint = RuntimeEndpoint::new("be-0.internal", 9060).expect("endpoint");
        assert_eq!(
            BackendProcessDescriptor::try_new(
                BackendProcessId::new_v7(),
                endpoint.clone(),
                RuntimeEndpoint::new("be-0.internal", 9061).unwrap(),
                "",
                "build",
                NativeCompatibilityId::new([7; 32]),
                4096,
            ),
            Err(BackendProcessDescriptorError::DeploymentId)
        );
        assert_eq!(
            BackendProcessDescriptor::try_new(
                BackendProcessId::new_v7(),
                endpoint,
                RuntimeEndpoint::new("be-0.internal", 9061).unwrap(),
                "deployment",
                "b".repeat(257),
                NativeCompatibilityId::new([7; 32]),
                4096,
            ),
            Err(BackendProcessDescriptorError::BuildIdentity)
        );
    }

    #[test]
    fn root_support_is_explicit_and_immutable_for_one_process() {
        use crate::native_result_support::BoundedRootSupport;
        use novarocks_result_contract::RootProfileId;
        let base = BackendProcessDescriptor::try_new(
            BackendProcessId::new_v7(),
            RuntimeEndpoint::new("be", 9060).unwrap(),
            RuntimeEndpoint::new("be", 9061).unwrap(),
            "deployment",
            "build",
            NativeCompatibilityId::new([7; 32]),
            4096,
        )
        .unwrap();
        assert_eq!(
            base.require_bounded_root_support(),
            Err(BackendProcessDescriptorError::MissingRootSupport)
        );
        assert_eq!(
            base.control_endpoint(),
            &RuntimeEndpoint::new("be", 9061).unwrap()
        );
        assert!(
            base.clone()
                .with_bounded_root_support(BoundedRootSupport::new(
                    base.endpoint().clone(),
                    RootProfileId::V1,
                ))
                .is_err()
        );
        let support =
            BoundedRootSupport::new(RuntimeEndpoint::new("be", 9061).unwrap(), RootProfileId::V1);
        let enabled = base.with_bounded_root_support(support.clone()).unwrap();
        assert_eq!(
            enabled.clone().with_bounded_root_support(support).unwrap(),
            enabled
        );
        assert_eq!(
            enabled.with_bounded_root_support(BoundedRootSupport::new(
                RuntimeEndpoint::new("be", 9062).unwrap(),
                RootProfileId::V1,
            )),
            Err(BackendProcessDescriptorError::ConflictingControlEndpoint)
        );
    }

    #[test]
    fn mandatory_control_endpoint_is_distinct_and_part_of_exact_process_facts() {
        let process = BackendProcessId::new_v7();
        let data = RuntimeEndpoint::new("be", 9060).unwrap();
        assert_eq!(
            BackendProcessDescriptor::try_new(
                process,
                data.clone(),
                data.clone(),
                "deployment",
                "build",
                NativeCompatibilityId::new([7; 32]),
                4096,
            ),
            Err(BackendProcessDescriptorError::IndependentEndpoints),
        );
        let descriptor = |port| {
            BackendProcessDescriptor::try_new(
                process,
                data.clone(),
                RuntimeEndpoint::new("be", port).unwrap(),
                "deployment",
                "build",
                NativeCompatibilityId::new([7; 32]),
                4096,
            )
            .unwrap()
        };
        let first = descriptor(9061);
        let replacement = descriptor(9062);
        assert_eq!(first.process_id(), replacement.process_id());
        assert_eq!(first.endpoint(), replacement.endpoint());
        assert_ne!(first, replacement);
        assert!(first.bounded_root_support().is_none());
        assert_eq!(
            first.with_bounded_root_support(crate::native_result_support::BoundedRootSupport::new(
                RuntimeEndpoint::new("be", 9062).unwrap(),
                novarocks_result_contract::RootProfileId::V1,
            )),
            Err(BackendProcessDescriptorError::ConflictingControlEndpoint),
        );
    }
}
