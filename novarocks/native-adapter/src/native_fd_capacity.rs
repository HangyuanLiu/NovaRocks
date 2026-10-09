// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Read-only verification of the frozen Native operational descriptor baseline.
//!
//! The descriptor envelope is derived from the same admission positions the
//! listeners and dialers enforce. A Backend counts every physical connection
//! position of both classes (handshakes are a subset of those), its two
//! listeners, one accepted-then-refused socket per listener, and one resolver
//! descriptor per DNS position. A Frontend counts its outgoing lane positions,
//! the Backends' announcement connections to its report listener, that
//! listener and its DNS positions. Remaining baseline headroom is arithmetic,
//! not a reservation for connectors, files, scheduler activity, or unrelated
//! process users. This module neither changes a process limit nor acquires a
//! descriptor.

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_proto_codec::native_rpc::NativeEndpointDomain;
use std::io;

use crate::native_transport_admission::AdmissionDimensions;

/// The process limits observed before a Native listener is bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeFileDescriptorCapacityReport {
    /// The finite soft limit, or `u64::MAX` when `soft_unlimited` is true.
    pub soft_limit: u64,
    /// The finite hard limit, or `u64::MAX` when `hard_unlimited` is true.
    pub hard_limit: u64,
    pub soft_unlimited: bool,
    pub hard_unlimited: bool,
    /// The frozen role baseline: 1024 for BE domains and 2048 for FE membership.
    pub required_soft_limit: u64,
    pub backend_data_socket_positions: u64,
    pub backend_control_socket_positions: u64,
    pub backend_listener_positions: u64,
    pub backend_refusal_positions: u64,
    /// Resolver descriptors: one per process DNS position.
    pub dns_resolution_positions: u64,
    pub backend_native_socket_positions: u64,
    /// The BE baseline minus the complete Native socket envelope, not free FDs.
    pub backend_baseline_headroom: u64,
    pub frontend_outgoing_socket_positions: u64,
    pub frontend_membership_socket_positions: u64,
    pub frontend_listener_positions: u64,
    pub frontend_refusal_positions: u64,
    pub frontend_native_socket_positions: u64,
    /// The FE baseline minus the complete Native socket envelope.
    pub frontend_baseline_headroom: u64,
}

/// Query `RLIMIT_NOFILE` without changing it, then check the frozen role baseline.
///
/// Only the supported 64-bit Linux/macOS operational targets are accepted.
/// Failure occurs before listener construction; success does not advertise V1
/// support or reserve descriptors against other users in this process.
pub fn verify_native_file_descriptor_capacity(
    domain: NativeEndpointDomain,
) -> io::Result<NativeFileDescriptorCapacityReport> {
    let (soft, hard) = query_limits()?;
    validate_limits(domain, soft, hard, NativeResultSupportGeometry::V1)
}

/// Verify the role baseline during pure launch preflight, before runtimes or
/// listeners are constructed. Backend Data and Control share one role baseline.
pub fn verify_native_file_descriptor_capacity_for_role(
    role: novarocks_types::ClusterRole,
) -> io::Result<NativeFileDescriptorCapacityReport> {
    verify_native_file_descriptor_capacity(match role {
        novarocks_types::ClusterRole::Fe => NativeEndpointDomain::FrontendMembership,
        novarocks_types::ClusterRole::Be => NativeEndpointDomain::BackendData,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Limit {
    Finite(u64),
    Unlimited,
}

impl Limit {
    fn value(self) -> u64 {
        match self {
            Self::Finite(value) => value,
            Self::Unlimited => u64::MAX,
        }
    }
}

// Keep the conversion generic so a platform's rlim_t width cannot silently
// truncate. The infinity sentinel is recognized before conversion.
fn convert_limit<T: Copy + PartialEq + TryInto<u64>>(raw: T, infinity: T) -> io::Result<Limit> {
    if raw == infinity {
        return Ok(Limit::Unlimited);
    }
    raw.try_into()
        .map(Limit::Finite)
        .map_err(|_| io::ErrorKind::InvalidData.into())
}

#[cfg(all(
    target_pointer_width = "64",
    any(target_os = "linux", target_os = "macos")
))]
fn query_limits() -> io::Result<(Limit, Limit)> {
    let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: getrlimit receives valid writable storage for one rlimit and
    // initializes both members on success. No storage is read on failure.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful getrlimit call initialized this entire value.
    let limits = unsafe { limits.assume_init() };
    Ok((
        convert_limit(limits.rlim_cur, libc::RLIM_INFINITY)?,
        convert_limit(limits.rlim_max, libc::RLIM_INFINITY)?,
    ))
}

#[cfg(not(all(
    target_pointer_width = "64",
    any(target_os = "linux", target_os = "macos")
)))]
fn query_limits() -> io::Result<(Limit, Limit)> {
    unsupported_target()
}

#[cfg(any(
    test,
    not(all(
        target_pointer_width = "64",
        any(target_os = "linux", target_os = "macos")
    ))
))]
fn unsupported_target<T>() -> io::Result<T> {
    Err(io::ErrorKind::Unsupported.into())
}

fn add(a: u64, b: u64) -> io::Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}

const BACKEND_LISTENERS: u64 = 2;
/// One accepted socket per listener may be open while it is being refused.
const BACKEND_REFUSALS: u64 = 2;
const FRONTEND_LISTENERS: u64 = 1;
const FRONTEND_REFUSALS: u64 = 1;

fn dns_positions() -> io::Result<u64> {
    u64::try_from(novarocks_native_trust::NATIVE_DNS_RESOLUTION_POSITIONS)
        .map_err(|_| io::ErrorKind::InvalidInput.into())
}

fn counts(dimensions: AdmissionDimensions) -> io::Result<(u64, u64)> {
    let convert = |value: usize| -> io::Result<u64> {
        u64::try_from(value).map_err(|_| io::ErrorKind::InvalidInput.into())
    };
    Ok((
        convert(dimensions.data_positions)?,
        convert(dimensions.control_positions)?,
    ))
}

/// Every Native descriptor a Backend can hold: its Data and Control physical
/// positions, listeners, refusal transients and resolver descriptors.
pub fn backend_socket_positions(g: &NativeResultSupportGeometry) -> io::Result<u64> {
    let (data, control) = counts(AdmissionDimensions::backend(g)?)?;
    add(
        add(data, control)?,
        add(add(BACKEND_LISTENERS, BACKEND_REFUSALS)?, dns_positions()?)?,
    )
}

/// The Backends' announcement connections to one Frontend's report listener,
/// with their connecting and closing headroom.
fn frontend_membership_positions(g: &NativeResultSupportGeometry) -> io::Result<u64> {
    u64::try_from(AdmissionDimensions::frontend_membership(g)?.0)
        .map_err(|_| io::ErrorKind::InvalidInput.into())
}

/// Every Native descriptor a Frontend can hold: its outgoing lane positions,
/// the announcement connections it serves, its listener and resolver
/// descriptors.
pub fn frontend_socket_positions(g: &NativeResultSupportGeometry) -> io::Result<u64> {
    let (data, control) = counts(AdmissionDimensions::frontend(g)?)?;
    add(
        add(data, control)?,
        add(
            frontend_membership_positions(g)?,
            add(
                add(FRONTEND_LISTENERS, FRONTEND_REFUSALS)?,
                dns_positions()?,
            )?,
        )?,
    )
}

fn validate_limits(
    domain: NativeEndpointDomain,
    soft: Limit,
    hard: Limit,
    g: NativeResultSupportGeometry,
) -> io::Result<NativeFileDescriptorCapacityReport> {
    if matches!((soft, hard), (Limit::Unlimited, Limit::Finite(_))) || soft.value() > hard.value() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let required_soft_limit = match domain {
        NativeEndpointDomain::BackendData | NativeEndpointDomain::BackendControl => {
            g.backend_minimum_open_file_limit
        }
        NativeEndpointDomain::FrontendMembership => g.frontend_minimum_open_file_limit,
    };
    if required_soft_limit == 0 || soft.value() < required_soft_limit {
        return Err(io::ErrorKind::InvalidInput.into());
    }

    // The same admission positions the listeners and dialers enforce.
    let (backend_data_socket_positions, backend_control_socket_positions) =
        counts(AdmissionDimensions::backend(&g)?)?;
    let backend_native_socket_positions = backend_socket_positions(&g)?;
    let backend_baseline_headroom = g
        .backend_minimum_open_file_limit
        .checked_sub(backend_native_socket_positions)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let (frontend_data, frontend_control) = counts(AdmissionDimensions::frontend(&g)?)?;
    let frontend_native_socket_positions = frontend_socket_positions(&g)?;
    let frontend_baseline_headroom = g
        .frontend_minimum_open_file_limit
        .checked_sub(frontend_native_socket_positions)
        .ok_or(io::ErrorKind::InvalidInput)?;

    Ok(NativeFileDescriptorCapacityReport {
        soft_limit: soft.value(),
        hard_limit: hard.value(),
        soft_unlimited: soft == Limit::Unlimited,
        hard_unlimited: hard == Limit::Unlimited,
        required_soft_limit,
        backend_data_socket_positions,
        backend_control_socket_positions,
        backend_listener_positions: BACKEND_LISTENERS,
        backend_refusal_positions: BACKEND_REFUSALS,
        dns_resolution_positions: dns_positions()?,
        backend_native_socket_positions,
        backend_baseline_headroom,
        frontend_outgoing_socket_positions: add(frontend_data, frontend_control)?,
        frontend_membership_socket_positions: frontend_membership_positions(&g)?,
        frontend_listener_positions: FRONTEND_LISTENERS,
        frontend_refusal_positions: FRONTEND_REFUSALS,
        frontend_native_socket_positions,
        frontend_baseline_headroom,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(
        domain: NativeEndpointDomain,
        soft: u64,
        g: NativeResultSupportGeometry,
    ) -> io::Result<NativeFileDescriptorCapacityReport> {
        validate_limits(domain, Limit::Finite(soft), Limit::Unlimited, g)
    }

    #[test]
    fn frozen_backend_envelope_preserves_control_and_operational_headroom() {
        for domain in [
            NativeEndpointDomain::BackendData,
            NativeEndpointDomain::BackendControl,
        ] {
            let report = check(domain, 1024, NativeResultSupportGeometry::V1).unwrap();
            assert_eq!(report.required_soft_limit, 1024);
            assert_eq!(report.backend_data_socket_positions, 518);
            assert_eq!(report.backend_control_socket_positions, 20);
            assert_eq!(report.backend_listener_positions, 2);
            assert_eq!(report.backend_refusal_positions, 2);
            assert_eq!(report.frontend_refusal_positions, 1);
            assert_eq!(report.dns_resolution_positions, 4);
            assert_eq!(report.backend_native_socket_positions, 546);
            assert_eq!(report.backend_baseline_headroom, 478);
            // 32 BEs * (10 data + 3 lanes * 2 tails) + 32 * (1 control + 2 tails).
            assert_eq!(report.frontend_outgoing_socket_positions, 512 + 96);
            assert_eq!(report.frontend_membership_socket_positions, 96);
            assert_eq!(
                report.frontend_native_socket_positions,
                608 + 96 + 1 + 1 + 4
            );
            assert_eq!(report.frontend_baseline_headroom, 2048 - 710);
            assert_eq!(report.soft_limit, 1024);
            assert!(!report.soft_unlimited);
            assert_eq!(report.hard_limit, u64::MAX);
            assert!(report.hard_unlimited);
        }
    }

    #[test]
    fn role_baselines_refuse_low_limits_and_accept_the_exact_boundary() {
        for (domain, minimum) in [
            (NativeEndpointDomain::BackendData, 1024),
            (NativeEndpointDomain::BackendControl, 1024),
            (NativeEndpointDomain::FrontendMembership, 2048),
        ] {
            assert_eq!(
                check(domain, minimum - 1, NativeResultSupportGeometry::V1)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                check(domain, minimum, NativeResultSupportGeometry::V1)
                    .unwrap()
                    .required_soft_limit,
                minimum
            );
        }
    }

    #[test]
    fn infinity_is_explicit_and_finite_conversion_cannot_truncate() {
        assert_eq!(convert_limit(7_u32, u32::MAX).unwrap(), Limit::Finite(7));
        assert_eq!(convert_limit(u64::MAX, u64::MAX).unwrap(), Limit::Unlimited);
        assert_eq!(
            convert_limit(u128::MAX, u128::MAX).unwrap(),
            Limit::Unlimited
        );
        assert_eq!(
            convert_limit(u128::from(u64::MAX) + 1, u128::MAX)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        let report = validate_limits(
            NativeEndpointDomain::FrontendMembership,
            Limit::Unlimited,
            Limit::Unlimited,
            NativeResultSupportGeometry::V1,
        )
        .unwrap();
        assert!(report.soft_unlimited && report.hard_unlimited);
        assert_eq!(report.soft_limit, u64::MAX);
    }

    #[test]
    fn impossible_soft_and_hard_pairs_are_not_accepted() {
        for (soft, hard) in [
            (Limit::Finite(1024), Limit::Finite(1023)),
            (Limit::Unlimited, Limit::Finite(u64::MAX)),
        ] {
            assert_eq!(
                validate_limits(
                    NativeEndpointDomain::BackendData,
                    soft,
                    hard,
                    NativeResultSupportGeometry::V1,
                )
                .unwrap_err()
                .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn peer_and_control_geometry_remain_distinct() {
        let mut control = NativeResultSupportGeometry::V1;
        control.transport_control_handshake_positions = 9;
        let report = check(NativeEndpointDomain::BackendControl, 1024, control).unwrap();
        assert_eq!(report.backend_data_socket_positions, 518);
        assert_eq!(report.backend_control_socket_positions, 21);
        assert_eq!(report.backend_baseline_headroom, 477);

        let mut peer = NativeResultSupportGeometry::V1;
        peer.transport_maximum_live_backends = 33;
        let report = check(NativeEndpointDomain::BackendData, 1024, peer).unwrap();
        assert_eq!(report.backend_data_socket_positions, 532);
        assert_eq!(report.backend_control_socket_positions, 20);
        assert_eq!(report.backend_native_socket_positions, 560);
        assert_eq!(report.backend_baseline_headroom, 464);
    }

    #[test]
    fn overflowing_or_underfunded_geometry_is_rejected() {
        let mut multiply = NativeResultSupportGeometry::V1;
        multiply.transport_maximum_live_backends = u64::MAX;
        let mut addition = NativeResultSupportGeometry::V1;
        addition.transport_connections_per_frontend_backend_result = u64::MAX;
        let mut headroom = NativeResultSupportGeometry::V1;
        headroom.backend_minimum_open_file_limit = 545;
        let mut frontend_headroom = NativeResultSupportGeometry::V1;
        frontend_headroom.frontend_minimum_open_file_limit = 708;
        for g in [multiply, addition, headroom, frontend_headroom] {
            assert_eq!(
                check(NativeEndpointDomain::BackendData, u64::MAX, g)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn unsupported_operational_target_has_an_explicit_error() {
        assert_eq!(
            unsupported_target::<NativeFileDescriptorCapacityReport>()
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
}
