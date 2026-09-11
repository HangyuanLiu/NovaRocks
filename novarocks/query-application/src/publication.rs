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

//! Query-admission policy for lake-publication attempts.

use std::time::{Duration, Instant};

/// Startup-frozen temporal boundary shared by every lake publication attempt.
// Design: ADR-0110 (docs/adr/ADR-0110-lake-publication-crash-only-contract.md)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LakePublicationRuntimePolicy {
    max_attempt_duration: Duration,
    safe_gc_age: Duration,
    max_clock_skew: Duration,
    listing_visibility_delay: Duration,
    scheduler_margin: Duration,
}

impl LakePublicationRuntimePolicy {
    pub fn try_new(
        max_attempt_duration: Duration,
        safe_gc_age: Duration,
        max_clock_skew: Duration,
        listing_visibility_delay: Duration,
        scheduler_margin: Duration,
    ) -> Result<Self, String> {
        let components = [
            ("max_attempt_duration", max_attempt_duration),
            ("safe_gc_age", safe_gc_age),
            ("max_clock_skew", max_clock_skew),
            ("listing_visibility_delay", listing_visibility_delay),
            ("scheduler_margin", scheduler_margin),
        ];
        for (name, duration) in components {
            if duration.is_zero() {
                return Err(format!("lake publication {name} must be greater than zero"));
            }
        }
        let minimum_safe_gc_age = max_attempt_duration
            .checked_add(max_clock_skew)
            .and_then(|value| value.checked_add(listing_visibility_delay))
            .and_then(|value| value.checked_add(scheduler_margin))
            .ok_or_else(|| "lake publication safe GC age calculation overflows".to_string())?;
        if safe_gc_age <= minimum_safe_gc_age {
            return Err(
                "lake publication safe GC age must exceed max attempt duration plus clock skew, listing visibility delay, and scheduler margin"
                    .to_string(),
            );
        }
        Ok(Self {
            max_attempt_duration,
            safe_gc_age,
            max_clock_skew,
            listing_visibility_delay,
            scheduler_margin,
        })
    }

    pub const fn max_attempt_duration(self) -> Duration {
        self.max_attempt_duration
    }

    pub const fn safe_gc_age(self) -> Duration {
        self.safe_gc_age
    }

    pub const fn max_clock_skew(self) -> Duration {
        self.max_clock_skew
    }

    pub const fn listing_visibility_delay(self) -> Duration {
        self.listing_visibility_delay
    }

    pub const fn scheduler_margin(self) -> Duration {
        self.scheduler_margin
    }

    /// Clamp a mutating statement's session deadline to the shared attempt
    /// maximum. Read-only statements must not call this method.
    pub fn admit_deadline(
        self,
        now: Instant,
        existing_deadline: Option<Instant>,
    ) -> Result<Instant, String> {
        let policy_deadline = now
            .checked_add(self.max_attempt_duration)
            .ok_or_else(|| "lake publication deadline exceeds monotonic clock range".to_string())?;
        Ok(existing_deadline
            .map(|deadline| deadline.min(policy_deadline))
            .unwrap_or(policy_deadline))
    }
}

#[cfg(test)]
mod tests {
    use super::LakePublicationRuntimePolicy;
    use std::time::{Duration, Instant};

    #[test]
    fn requires_a_strict_gc_age_and_clamps_deadlines() {
        let policy = LakePublicationRuntimePolicy::try_new(
            Duration::from_secs(30),
            Duration::from_secs(36),
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .expect("strictly safe policy");
        let now = Instant::now();
        assert_eq!(
            policy.admit_deadline(now, None).unwrap(),
            now + Duration::from_secs(30)
        );
        assert_eq!(
            policy
                .admit_deadline(now, Some(now + Duration::from_secs(5)))
                .unwrap(),
            now + Duration::from_secs(5)
        );
        assert!(
            LakePublicationRuntimePolicy::try_new(
                Duration::from_secs(30),
                Duration::from_secs(35),
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(2),
            )
            .is_err()
        );
    }
}
