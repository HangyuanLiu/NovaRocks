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

//! Read-only startup parameters of the original MySQL owners.
//!
//! Values here grant no rights and cannot configure a listener or statement.
//! The production caller and this getter use the same pure input policy.

use std::{io, time::Duration};

use crate::connection_registry::{MysqlConnectionClass, frozen_connection_positions};
use crate::query_application_shim::{MysqlInputPolicy, mysql_input_policy};
use crate::relay_result_writer::{ACTIVE_WRITE_DEADLINE, CLOSING_DEADLINE, CLOSING_OBJECT_BYTES};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlInputLimits {
    pub row_bytes: usize,
    pub metadata_bytes: usize,
    pub command_bytes: usize,
    pub auth_bytes: usize,
    pub diagnostic_bytes: usize,
    pub long_data_bytes: usize,
    pub prepared_statements: usize,
    pub prepared_parameters: usize,
    pub long_data_entries: usize,
    pub connection_input_bytes: usize,
    pub columns: usize,
    pub coalescing_bytes: usize,
}

impl MysqlInputLimits {
    fn from_original(limits: opensrv_mysql::ProtocolLimits) -> Self {
        Self {
            row_bytes: limits.row_bytes,
            metadata_bytes: limits.metadata_bytes,
            command_bytes: limits.command_bytes,
            auth_bytes: limits.auth_bytes,
            diagnostic_bytes: limits.diagnostic_bytes,
            long_data_bytes: limits.long_data_bytes,
            prepared_statements: limits.prepared_statements,
            prepared_parameters: limits.prepared_parameters,
            long_data_entries: limits.long_data_entries,
            connection_input_bytes: limits.connection_input_bytes,
            columns: limits.columns,
            coalescing_bytes: limits.coalescing_bytes,
        }
    }

    fn validate(self) -> io::Result<()> {
        // Use the original library validator and retain its actual error.
        opensrv_mysql::ProtocolLimits {
            row_bytes: self.row_bytes,
            metadata_bytes: self.metadata_bytes,
            command_bytes: self.command_bytes,
            auth_bytes: self.auth_bytes,
            diagnostic_bytes: self.diagnostic_bytes,
            long_data_bytes: self.long_data_bytes,
            prepared_statements: self.prepared_statements,
            prepared_parameters: self.prepared_parameters,
            long_data_entries: self.long_data_entries,
            connection_input_bytes: self.connection_input_bytes,
            columns: self.columns,
            coalescing_bytes: self.coalescing_bytes,
        }
        .validate()
        .map(|_| ())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlInputStartupParameters {
    pub limits: MysqlInputLimits,
    pub auth_timeout: Duration,
    pub command_timeout: Duration,
    pub response_write_timeout: Duration,
}
impl MysqlInputStartupParameters {
    fn from_original(policy: MysqlInputPolicy) -> Self {
        Self {
            limits: MysqlInputLimits::from_original(policy.limits),
            auth_timeout: policy.auth_timeout,
            command_timeout: policy.command_timeout,
            response_write_timeout: policy.response_write_timeout,
        }
    }
    fn validate(self) -> io::Result<()> {
        self.limits.validate()?;
        for duration in [
            self.auth_timeout,
            self.command_timeout,
            self.response_write_timeout,
        ] {
            exact_positive_milliseconds(duration)?;
        }
        Ok(())
    }
}

/// Copyable configuration facts; no listener, IO, grant, token or authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlStartupParameters {
    pub ordinary_positions: usize,
    pub control_positions: usize,
    pub ordinary: MysqlInputStartupParameters,
    pub control: MysqlInputStartupParameters,
    /// The original result writer clock, not the command-read clock.
    pub active_write_deadline: Duration,
    /// The independent original Closing writer clock.
    pub closing_deadline: Duration,
    pub closing_object_bytes: u64,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn exact_positive_milliseconds(duration: Duration) -> io::Result<u64> {
    if duration.is_zero() || duration.subsec_nanos() % 1_000_000 != 0 {
        return Err(invalid(
            "MySQL startup duration must be positive exact milliseconds",
        ));
    }
    u64::try_from(duration.as_millis())
        .map_err(|_| invalid("MySQL startup duration milliseconds overflow"))
}

impl MysqlStartupParameters {
    /// Validate copied facts without granting or changing production settings.
    pub fn validate(self) -> io::Result<()> {
        if self.ordinary_positions == 0
            || self.control_positions == 0
            || self.closing_object_bytes == 0
        {
            return Err(invalid(
                "MySQL startup positions and Closing bytes must be nonzero",
            ));
        }
        self.ordinary_positions
            .checked_add(self.control_positions)
            .ok_or_else(|| invalid("MySQL startup connection positions overflow"))?;
        self.ordinary.validate()?;
        self.control.validate()?;
        exact_positive_milliseconds(self.active_write_deadline)?;
        exact_positive_milliseconds(self.closing_deadline)?;
        Ok(())
    }
}

pub fn frozen_mysql_startup_parameters() -> io::Result<MysqlStartupParameters> {
    let [ordinary_positions, control_positions] = frozen_connection_positions();
    let parameters = MysqlStartupParameters {
        ordinary_positions,
        control_positions,
        ordinary: MysqlInputStartupParameters::from_original(mysql_input_policy(
            MysqlConnectionClass::Ordinary,
        )),
        control: MysqlInputStartupParameters::from_original(mysql_input_policy(
            MysqlConnectionClass::Control,
        )),
        active_write_deadline: ACTIVE_WRITE_DEADLINE,
        closing_deadline: CLOSING_DEADLINE,
        closing_object_bytes: CLOSING_OBJECT_BYTES,
    };
    parameters.validate()?;
    Ok(parameters)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_original_callers() {
        let projection = frozen_mysql_startup_parameters().unwrap();
        assert_eq!(
            projection.ordinary,
            MysqlInputStartupParameters::from_original(mysql_input_policy(
                MysqlConnectionClass::Ordinary
            ))
        );
        assert_eq!(
            projection.control,
            MysqlInputStartupParameters::from_original(mysql_input_policy(
                MysqlConnectionClass::Control
            ))
        );
        assert_eq!(
            [projection.ordinary_positions, projection.control_positions],
            frozen_connection_positions()
        );
        assert_eq!(projection.active_write_deadline, ACTIVE_WRITE_DEADLINE);
        assert_eq!(projection.closing_deadline, CLOSING_DEADLINE);
        assert_eq!(projection.closing_object_bytes, CLOSING_OBJECT_BYTES);
    }

    #[test]
    fn default_caller_and_public_projection_use_original_settings() {
        assert_original_callers();
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    #[test]
    fn feature_caller_keeps_the_same_original_limits_and_clock_values() {
        // The feature replaces only the writer wrapper before the shared
        // input-policy call. This is config equivalence, not actual IO proof.
        assert_original_callers();
    }

    #[test]
    fn frozen_caps_and_clock_durations_remain_exact_without_changing_start_points() {
        let p = frozen_mysql_startup_parameters().unwrap();
        assert_eq!([p.ordinary_positions, p.control_positions], [512, 32]);
        let limits = p.ordinary.limits;
        assert_eq!(
            [
                limits.row_bytes,
                limits.metadata_bytes,
                limits.command_bytes,
                limits.auth_bytes,
                limits.diagnostic_bytes,
                limits.long_data_bytes,
                limits.prepared_statements,
                limits.prepared_parameters,
                limits.long_data_entries,
                limits.connection_input_bytes,
                limits.columns,
                limits.coalescing_bytes
            ],
            [
                64 * 1024 * 1024,
                512 * 1024,
                1024 * 1024,
                64 * 1024,
                16 * 1024,
                1024 * 1024,
                64,
                4096,
                4096,
                2 * 1024 * 1024,
                4096,
                64 * 1024
            ]
        );
        let mut expected_control = limits;
        expected_control.command_bytes = 16 * 1024;
        assert_eq!(p.control.limits, expected_control);
        for input in [p.ordinary, p.control] {
            assert_eq!(input.auth_timeout, Duration::from_secs(10));
            assert_eq!(input.command_timeout, Duration::from_secs(10));
            assert_eq!(input.response_write_timeout, Duration::from_secs(30));
        }
        assert_eq!(p.active_write_deadline, Duration::from_secs(30));
        assert_eq!(p.closing_deadline, Duration::from_secs(5));
        assert_eq!(p.closing_object_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn original_protocol_limit_error_is_returned_without_reformatting() {
        let mut p = frozen_mysql_startup_parameters().unwrap();
        p.ordinary.limits.metadata_bytes = 512 * 1024 + 1;
        let error = p.validate().expect_err("original library limit refusal");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "invalid MySQL protocol limits");
    }

    #[test]
    fn zero_fractional_and_overflowing_durations_or_positions_are_refused() {
        for deadline in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::MAX,
            Duration::from_secs(u64::MAX),
        ] {
            let mut p = frozen_mysql_startup_parameters().unwrap();
            p.active_write_deadline = deadline;
            assert!(p.validate().is_err());
            p = frozen_mysql_startup_parameters().unwrap();
            p.ordinary.auth_timeout = deadline;
            assert!(p.validate().is_err());
        }
        let mut p = frozen_mysql_startup_parameters().unwrap();
        p.ordinary_positions = usize::MAX;
        assert!(p.validate().is_err());
        let error = exact_positive_milliseconds(Duration::from_secs(u64::MAX))
            .expect_err("whole-millisecond conversion must be checked");
        assert_eq!(
            error.to_string(),
            "MySQL startup duration milliseconds overflow"
        );
        for change in [
            (|p: &mut MysqlStartupParameters| p.ordinary_positions = 0)
                as fn(&mut MysqlStartupParameters),
            |p| p.control_positions = 0,
            |p| p.closing_object_bytes = 0,
        ] {
            let mut p = frozen_mysql_startup_parameters().unwrap();
            change(&mut p);
            assert!(p.validate().is_err());
        }
    }
}
