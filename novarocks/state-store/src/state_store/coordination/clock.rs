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

use super::CoordinationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClockHealth {
    Healthy,
    Unsafe,
    Unknown,
}

pub trait LeaseClock: Send + Sync {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError>;
    fn monotonic_time_millis(&self) -> u64;
    fn health(&self) -> ClockHealth;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseSettings {
    lease_duration: Duration,
    renew_interval: Duration,
    max_clock_skew: Duration,
    observation_window: Duration,
    pub(crate) lease_duration_ms: u64,
    pub(crate) renew_interval_ms: u64,
    pub(crate) max_clock_skew_ms: u64,
    pub(crate) observation_window_ms: u64,
}

impl LeaseSettings {
    pub fn new(
        lease_duration: Duration,
        renew_interval: Duration,
        max_clock_skew: Duration,
        observation_window: Duration,
    ) -> Result<Self, CoordinationError> {
        let lease_duration_ms = validated_millis("lease duration", lease_duration)?;
        let renew_interval_ms = validated_millis("renew interval", renew_interval)?;
        let max_clock_skew_ms = validated_millis("maximum clock skew", max_clock_skew)?;
        let observation_window_ms =
            validated_millis("lease observation window", observation_window)?;
        if renew_interval_ms >= lease_duration_ms {
            return Err(CoordinationError::invalid_request(
                "lease renew interval must be shorter than lease duration",
            ));
        }
        lease_duration_ms
            .checked_add(max_clock_skew_ms)
            .ok_or_else(|| {
                CoordinationError::invalid_request(
                    "lease duration plus clock skew exceeds the millisecond range",
                )
            })?;
        Ok(Self {
            lease_duration,
            renew_interval,
            max_clock_skew,
            observation_window,
            lease_duration_ms,
            renew_interval_ms,
            max_clock_skew_ms,
            observation_window_ms,
        })
    }

    pub const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub const fn renew_interval(&self) -> Duration {
        self.renew_interval
    }

    pub const fn max_clock_skew(&self) -> Duration {
        self.max_clock_skew
    }

    pub const fn observation_window(&self) -> Duration {
        self.observation_window
    }
}

fn validated_millis(name: &'static str, duration: Duration) -> Result<u64, CoordinationError> {
    if duration.is_zero() {
        return Err(CoordinationError::invalid_request(match name {
            "lease duration" => "lease duration must be nonzero",
            "renew interval" => "lease renew interval must be nonzero",
            "maximum clock skew" => "maximum clock skew must be nonzero",
            _ => "lease observation window must be nonzero",
        }));
    }
    u64::try_from(duration.as_millis()).map_err(|_| {
        CoordinationError::invalid_request("lease duration exceeds the millisecond range")
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::LeaseSettings;
    use crate::coordination::CoordinationErrorKind;

    #[test]
    fn settings_reject_zero_ordering_conversion_and_addition_overflow() {
        let one = Duration::from_millis(1);
        let two = Duration::from_millis(2);
        for settings in [
            LeaseSettings::new(Duration::ZERO, one, one, one),
            LeaseSettings::new(two, Duration::ZERO, one, one),
            LeaseSettings::new(two, one, Duration::ZERO, one),
            LeaseSettings::new(two, one, one, Duration::ZERO),
            LeaseSettings::new(two, two, one, one),
            LeaseSettings::new(Duration::from_secs(u64::MAX), one, one, one),
            LeaseSettings::new(Duration::from_millis(u64::MAX), one, one, one),
        ] {
            assert_eq!(
                settings.expect_err("settings must fail closed").kind(),
                CoordinationErrorKind::InvalidRequest
            );
        }
    }

    #[test]
    fn settings_preserve_validated_durations() {
        let settings = LeaseSettings::new(
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::from_secs(2),
            Duration::from_secs(3),
        )
        .expect("valid settings");

        assert_eq!(settings.lease_duration(), Duration::from_secs(30));
        assert_eq!(settings.renew_interval(), Duration::from_secs(10));
        assert_eq!(settings.max_clock_skew(), Duration::from_secs(2));
        assert_eq!(settings.observation_window(), Duration::from_secs(3));
    }
}
