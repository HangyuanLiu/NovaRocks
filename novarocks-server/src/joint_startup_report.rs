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

//! A checked startup report over original first-party owner projections.
//!
//! This grants no capacity. Unknown projections and transport coefficients
//! remain explicit; successful number checks are not a complete byte gate.

use anyhow::{Context, Result, anyhow, ensure};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_mysql_adapter::{MysqlStartupParameters, frozen_mysql_startup_parameters};
use novarocks_native_adapter::native_transport_geometry::{
    NativeTransportGeometryReport, validate_native_transport_geometry,
};
use novarocks_query_application::api::LocalResultBound;
use novarocks_workload_control::{ResultCapacityConfig, WorkloadConfig};

use crate::sdk_listing_profile::ValidatedListingStartup;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenOwnerProjection {
    MysqlSessionAndProtocolTailMaximumCoexistence,
    LocalConstructionMaximumCoexistence,
    InternalDomainMaximumCoexistence,
    NativeRootCallerCoexistenceAndAttemptMultiplicity,
}

/// Scalar configuration facts, not admission rights or physical-exit evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JointStartupReport {
    pub(crate) computation_positions: u64,
    pub(crate) result_class_positions: [u64; 4],
    pub(crate) result_class_all_objects_bytes: [u64; 4],
    pub(crate) first_party_result_window_declarations_bytes: u64,
    pub(crate) client_compute_plus_short_tail_positions: u64,
    pub(crate) required_short_tail_positions: u64,
    pub(crate) result_lane_stream_positions_per_frontend_backend: u64,
    /// Arithmetic inventory only; NOT a simultaneous root requirement/limit.
    pub(crate) uncomposed_native_capable_window_slots: u64,
    /// No invented per-window or old-attempt multiplier fills this field.
    pub(crate) complete_native_root_peak_per_frontend_backend: Option<u64>,
    pub(crate) local_source_rows: u64,
    pub(crate) local_source_columns: u64,
    pub(crate) local_source_logical_collector_bytes: u64,
    pub(crate) sdk_listing: ValidatedListingStartup,
    pub(crate) sdk_owned_listing_entries: u64,
    pub(crate) native: NativeTransportGeometryReport,
    pub(crate) mysql: MysqlStartupParameters,
    pub(crate) mysql_connection_positions: u64,
    /// Input-budget declarations, not an actual-allocation observation.
    pub(crate) mysql_connection_input_declarations_bytes: u64,
    /// None is unknown, never zero. SDK buffers are not part of this sum.
    pub(crate) result_plus_native_declared_bytes: Option<u64>,
    /// Partial declaration sum only; unknown Native bytes propagate unchanged.
    pub(crate) result_input_plus_native_declared_bytes: Option<u64>,
    pub(crate) open_owner_projections: [OpenOwnerProjection; 4],
}

pub(crate) fn validate_current(workload: &WorkloadConfig) -> Result<JointStartupReport> {
    let sdk = crate::sdk_listing_profile::validated_current()?;
    validate_parts(
        workload,
        ResultCapacityConfig::V1,
        &NativeResultSupportGeometry::V1,
        LocalResultBound::V1,
        sdk,
        frozen_mysql_startup_parameters().context("read original MySQL startup parameters")?,
    )
}

fn as_u64(value: usize, name: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("joint startup {name} exceeds u64"))
}

fn add(a: u64, b: u64, name: &'static str) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| anyhow!("joint startup {name} addition overflows"))
}

fn multiply(a: u64, b: u64, name: &'static str) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| anyhow!("joint startup {name} product overflows"))
}

fn equal(actual: u64, declared: u64, name: &'static str) -> Result<()> {
    ensure!(
        actual == declared,
        "joint startup {name} differs between original owners"
    );
    Ok(())
}

fn join_optional_bytes(result: u64, native: Option<u64>) -> Result<Option<u64>> {
    native
        .map(|native| add(result, native, "result/native declaration"))
        .transpose()
}

fn validate_parts(
    workload: &WorkloadConfig,
    result: ResultCapacityConfig,
    geometry: &NativeResultSupportGeometry,
    local: LocalResultBound,
    sdk_listing: ValidatedListingStartup,
    mysql: MysqlStartupParameters,
) -> Result<JointStartupReport> {
    workload
        .validate()
        .context("validate original frontend workload policy")?;
    let result = result
        .validate()
        .context("validate original result window profile")?;
    let native = validate_native_transport_geometry(geometry)
        .context("validate original Native transport geometry")?;
    mysql
        .validate()
        .context("validate original MySQL startup parameters")?;
    let ordinary_positions = as_u64(mysql.ordinary_positions, "ordinary connection positions")?;
    let control_positions = as_u64(mysql.control_positions, "control connection positions")?;
    equal(
        ordinary_positions,
        geometry.frontend_ordinary_connections,
        "ordinary connection positions",
    )?;
    equal(
        control_positions,
        geometry.frontend_control_connections,
        "control connection positions",
    )?;
    ensure!(
        mysql.control.limits.command_bytes == mysql.control.limits.diagnostic_bytes,
        "joint startup Control command bound differs from its original diagnostic bound"
    );
    ensure!(
        mysql.closing_object_bytes <= result.all_objects_bytes[3],
        "joint startup Closing writer declaration exceeds its original Closing window"
    );
    let mysql_connection_positions = add(
        ordinary_positions,
        control_positions,
        "MySQL connection positions",
    )?;
    let mysql_connection_input_declarations_bytes = add(
        multiply(
            ordinary_positions,
            as_u64(
                mysql.ordinary.limits.connection_input_bytes,
                "ordinary input budget",
            )?,
            "ordinary input budgets",
        )?,
        multiply(
            control_positions,
            as_u64(
                mysql.control.limits.connection_input_bytes,
                "control input budget",
            )?,
            "control input budgets",
        )?,
        "connection input budgets",
    )?;

    let compute = as_u64(
        result.client_compute_positions,
        "client computation positions",
    )?;
    let short_tail = as_u64(
        result.client_short_tail_positions,
        "client short-tail positions",
    )?;
    let positions = [
        as_u64(result.positions[0], "Client positions")?,
        as_u64(result.positions[1], "Local positions")?,
        as_u64(result.positions[2], "Internal positions")?,
        as_u64(result.positions[3], "Closing positions")?,
    ];
    let computation_positions = as_u64(workload.query_concurrency_limit, "query concurrency")?;
    ensure!(
        computation_positions <= compute,
        "joint startup result windows do not cover frontend computation admission"
    );
    for (actual, declared, name) in [
        (
            compute,
            geometry.frontend_client_compute_positions,
            "client compute positions",
        ),
        (
            short_tail,
            geometry.frontend_short_tail_positions,
            "short-tail positions",
        ),
        (
            positions[0],
            geometry.frontend_client_window_positions,
            "Client window positions",
        ),
        (
            positions[1],
            geometry.frontend_local_positions,
            "Local window positions",
        ),
        (
            positions[2],
            geometry.frontend_internal_positions,
            "Internal window positions",
        ),
        (
            positions[3],
            geometry.frontend_closing_positions,
            "Closing window positions",
        ),
        (
            result.supported_cancel_burst,
            geometry.frontend_supported_cancel_burst,
            "cancel burst",
        ),
        (
            result.sustained_cancels_per_second,
            geometry.frontend_sustained_cancels_per_second,
            "cancel rate",
        ),
        (
            result.short_tail_exit_millis,
            geometry.transport_short_tail_exit_deadline_ms,
            "short-tail exit deadline",
        ),
        (
            result.all_objects_bytes[0],
            geometry.frontend_window_all_objects_bytes,
            "Client all-objects bytes",
        ),
        (
            result.all_objects_bytes[3],
            geometry.frontend_closing_all_objects_bytes,
            "Closing all-objects bytes",
        ),
    ] {
        equal(actual, declared, name)?;
    }
    let required_short_tail_positions = add(
        add(
            multiply(
                result.sustained_cancels_per_second,
                result.short_tail_exit_millis,
                "short-tail rate/time",
            )?,
            999,
            "short-tail ceiling",
        )? / 1000,
        result.supported_cancel_burst,
        "short-tail burst",
    )?;
    ensure!(
        required_short_tail_positions <= short_tail,
        "joint startup short-tail coverage is insufficient"
    );
    let client_compute_plus_short_tail_positions =
        add(compute, short_tail, "Client compute/tail positions")?;
    ensure!(
        client_compute_plus_short_tail_positions <= positions[0],
        "joint startup Client window coverage is insufficient"
    );
    let result_lane_stream_positions_per_frontend_backend = multiply(
        geometry.transport_connections_per_frontend_backend_result,
        geometry.transport_streams_per_connection,
        "result lane stream positions",
    )?;
    // Each known native-capable class fits individually. Summing the classes
    // does not establish simultaneous occupancy: they share query permits and
    // may own nested or old attempts. Caller composition remains open below.
    ensure!(
        positions[0] <= result_lane_stream_positions_per_frontend_backend,
        "joint startup result lane cannot carry Client windows"
    );
    ensure!(
        positions[2] <= result_lane_stream_positions_per_frontend_backend,
        "joint startup result lane cannot carry Internal windows"
    );
    let uncomposed_native_capable_window_slots = add(
        positions[0],
        positions[2],
        "uncomposed native-capable slots",
    )?;
    let mut first_party_result_window_declarations_bytes = 0;
    for (positions, bytes) in positions.into_iter().zip(result.all_objects_bytes) {
        first_party_result_window_declarations_bytes = add(
            first_party_result_window_declarations_bytes,
            multiply(positions, bytes, "result window declarations")?,
            "result window declarations",
        )?;
    }
    let local_source_rows = as_u64(local.rows, "Local source rows")?;
    let local_source_columns = as_u64(local.columns, "Local source columns")?;
    let local_source_logical_collector_bytes =
        as_u64(local.bytes, "Local logical collector bytes")?;
    ensure!(
        local_source_rows > 0
            && local_source_columns > 0
            && local_source_logical_collector_bytes > 0,
        "joint startup Local source limits must be nonzero"
    );
    // Necessary coverage only. The private Local construction coexistence
    // envelope is not exported by its actual owner, so it stays OPEN.
    ensure!(
        local_source_logical_collector_bytes <= result.all_objects_bytes[1],
        "joint startup Local window is smaller than its logical collector"
    );
    let sdk_owned_listing_entries = as_u64(
        sdk_listing.owned_bound().entries,
        "owned SDK listing entries",
    )?;
    let result_plus_native_declared_bytes = join_optional_bytes(
        first_party_result_window_declarations_bytes,
        native
            .frontend
            .total_bytes()
            .context("calculate Native complete envelope")?,
    )?;
    let result_input_plus_native_declared_bytes = join_optional_bytes(
        add(
            first_party_result_window_declarations_bytes,
            mysql_connection_input_declarations_bytes,
            "result/input declarations",
        )?,
        native
            .frontend
            .total_bytes()
            .context("calculate Native complete envelope with input declarations")?,
    )?;
    Ok(JointStartupReport {
        computation_positions,
        result_class_positions: positions,
        result_class_all_objects_bytes: result.all_objects_bytes,
        first_party_result_window_declarations_bytes,
        client_compute_plus_short_tail_positions,
        required_short_tail_positions,
        result_lane_stream_positions_per_frontend_backend,
        uncomposed_native_capable_window_slots,
        complete_native_root_peak_per_frontend_backend: None,
        local_source_rows,
        local_source_columns,
        local_source_logical_collector_bytes,
        sdk_listing,
        sdk_owned_listing_entries,
        native,
        mysql,
        mysql_connection_positions,
        mysql_connection_input_declarations_bytes,
        result_plus_native_declared_bytes,
        result_input_plus_native_declared_bytes,
        open_owner_projections: [
            OpenOwnerProjection::MysqlSessionAndProtocolTailMaximumCoexistence,
            OpenOwnerProjection::LocalConstructionMaximumCoexistence,
            OpenOwnerProjection::InternalDomainMaximumCoexistence,
            OpenOwnerProjection::NativeRootCallerCoexistenceAndAttemptMultiplicity,
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(
        result: ResultCapacityConfig,
        geometry: NativeResultSupportGeometry,
    ) -> Result<JointStartupReport> {
        validate_parts(
            &WorkloadConfig::default(),
            result,
            &geometry,
            LocalResultBound::V1,
            crate::sdk_listing_profile::validated_current()?,
            frozen_mysql_startup_parameters()?,
        )
    }

    #[test]
    fn actual_owner_projection_numbers_are_checked_without_claiming_complete_envelope() {
        let report = validate_current(&WorkloadConfig::default()).expect("original owners agree");
        assert_eq!(report.result_class_positions, [320, 16, 4, 64]);
        assert_eq!(
            report.first_party_result_window_declarations_bytes,
            8704 * 1024 * 1024
        );
        assert_eq!(report.required_short_tail_positions, 64);
        assert_eq!(report.client_compute_plus_short_tail_positions, 320);
        assert_eq!(
            report.result_lane_stream_positions_per_frontend_backend,
            512
        );
        assert_eq!(report.uncomposed_native_capable_window_slots, 324);
        assert_eq!(report.complete_native_root_peak_per_frontend_backend, None);
        assert_eq!(report.result_plus_native_declared_bytes, None);
        assert_eq!(report.open_owner_projections.len(), 4);
        assert_eq!(report.mysql_connection_positions, 544);
        assert_eq!(
            report.mysql_connection_input_declarations_bytes,
            1088 * 1024 * 1024
        );
        assert_eq!(report.mysql, frozen_mysql_startup_parameters().unwrap());
        assert_eq!(report.result_input_plus_native_declared_bytes, None);
        assert_eq!(report.sdk_owned_listing_entries, 65_536);
        assert!(report.native.frontend.structural_bytes > 0);
    }

    #[test]
    fn excess_compute_is_refused_but_lower_actual_config_is_not_a_profile_drift() {
        let mut workload = WorkloadConfig::default();
        workload.query_concurrency_limit = 257;
        assert!(validate_current(&workload).is_err());
        workload.query_concurrency_limit = 16;
        let report = validate_current(&workload).expect("smaller workload remains covered");
        assert_eq!(report.computation_positions, 16);
        assert_eq!(report.result_class_positions[0], 320);
    }

    #[test]
    fn every_cross_owner_window_or_tail_drift_is_refused() {
        for change in [
            (|g: &mut NativeResultSupportGeometry| g.frontend_client_compute_positions += 1)
                as fn(&mut NativeResultSupportGeometry),
            |g| g.frontend_short_tail_positions += 1,
            |g| g.frontend_client_window_positions += 1,
            |g| g.frontend_local_positions += 1,
            |g| g.frontend_internal_positions += 1,
            |g| g.frontend_closing_positions += 1,
            |g| g.frontend_supported_cancel_burst += 1,
            |g| g.frontend_sustained_cancels_per_second += 1,
            |g| g.transport_short_tail_exit_deadline_ms += 1,
            |g| g.frontend_window_all_objects_bytes += 1,
            |g| g.frontend_closing_all_objects_bytes += 1,
        ] {
            let mut g = NativeResultSupportGeometry::V1;
            change(&mut g);
            assert!(check(ResultCapacityConfig::V1, g).is_err());
        }
    }

    #[test]
    fn original_workload_result_and_transport_error_sources_survive_context() {
        let mut workload = WorkloadConfig::default();
        workload.waiting_limit = 0;
        let error = validate_current(&workload).expect_err("invalid original workload");
        assert!(
            error
                .downcast_ref::<novarocks_workload_control::WorkError>()
                .is_some()
        );
        let mut result = ResultCapacityConfig::V1;
        result.all_objects_bytes[2] = u64::MAX;
        let error =
            check(result, NativeResultSupportGeometry::V1).expect_err("result product overflow");
        assert!(
            error
                .downcast_ref::<novarocks_workload_control::WorkError>()
                .is_some()
        );
        let mut g = NativeResultSupportGeometry::V1;
        g.transport_streams_per_connection = u64::MAX;
        let error = check(ResultCapacityConfig::V1, g).expect_err("invalid transport count");
        assert!(error.downcast_ref::<std::io::Error>().is_some());
    }

    #[test]
    fn local_logical_collector_cannot_exceed_its_prebuilt_window() {
        let mut local = LocalResultBound::V1;
        local.bytes = usize::try_from(ResultCapacityConfig::V1.all_objects_bytes[1]).unwrap() + 1;
        assert!(
            validate_parts(
                &WorkloadConfig::default(),
                ResultCapacityConfig::V1,
                &NativeResultSupportGeometry::V1,
                local,
                crate::sdk_listing_profile::validated_current().unwrap(),
                frozen_mysql_startup_parameters().unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn arithmetic_overflow_and_unknown_transport_never_become_zero() {
        assert!(add(u64::MAX, 1, "test").is_err());
        assert!(multiply(u64::MAX, 2, "test").is_err());
        assert_eq!(join_optional_bytes(u64::MAX, None).unwrap(), None);
        assert!(join_optional_bytes(u64::MAX, Some(1)).is_err());
        assert_eq!(join_optional_bytes(7, Some(9)).unwrap(), Some(16));
    }
    fn with_mysql(mysql: MysqlStartupParameters) -> Result<JointStartupReport> {
        validate_parts(
            &WorkloadConfig::default(),
            ResultCapacityConfig::V1,
            &NativeResultSupportGeometry::V1,
            LocalResultBound::V1,
            crate::sdk_listing_profile::validated_current()?,
            mysql,
        )
    }

    #[test]
    fn actual_mysql_positions_and_control_command_policy_cannot_drift_from_native_declarations() {
        let mut p = frozen_mysql_startup_parameters().unwrap();
        p.ordinary_positions -= 1;
        assert!(with_mysql(p).is_err());
        p = frozen_mysql_startup_parameters().unwrap();
        p.control_positions += 1;
        assert!(with_mysql(p).is_err());
        p = frozen_mysql_startup_parameters().unwrap();
        p.control.limits.command_bytes = p.ordinary.limits.command_bytes;
        assert!(with_mysql(p).is_err());
    }

    #[test]
    fn closing_writer_bytes_must_be_covered_without_using_clock_duration_as_capacity() {
        let mut p = frozen_mysql_startup_parameters().unwrap();
        p.closing_object_bytes += 1;
        assert!(with_mysql(p).is_err());
        let report = with_mysql(frozen_mysql_startup_parameters().unwrap()).unwrap();
        assert_eq!(
            report.mysql.closing_deadline,
            std::time::Duration::from_secs(5)
        );
        assert_eq!(
            report.mysql.active_write_deadline,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(report.complete_native_root_peak_per_frontend_backend, None);
    }

    #[test]
    fn mysql_library_error_source_and_checked_clock_refusal_are_retained() {
        let mut p = frozen_mysql_startup_parameters().unwrap();
        p.ordinary.limits.metadata_bytes += 1;
        let error = with_mysql(p).expect_err("original library refusal");
        let original = error
            .downcast_ref::<std::io::Error>()
            .expect("original IO type retained");
        assert_eq!(original.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(original.to_string(), "invalid MySQL protocol limits");
        p = frozen_mysql_startup_parameters().unwrap();
        p.control.auth_timeout = std::time::Duration::ZERO;
        assert!(
            with_mysql(p)
                .unwrap_err()
                .downcast_ref::<std::io::Error>()
                .is_some()
        );
    }
}
