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

//! Function-domain projections. Codes describe function facts, never SQL or
//! recovery decisions. Control observation is not accepted client cancellation.

use crate::WorkError;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};
use std::num::NonZeroU16;

impl FailureProjection for WorkError {
    fn failure_fact(&self) -> FailureFact {
        let (class, code, observed_stop) = match self {
            Self::InvalidConfig(_) => (FailureClass::Invariant, 1, false),
            Self::Closed => (FailureClass::Control, 2, true),
            Self::NotReady => (FailureClass::Control, 3, false),
            Self::Released => (FailureClass::Control, 4, true),
            Self::ForeignAuthority => (FailureClass::Invariant, 5, false),
            Self::Cancelled(_) => (FailureClass::Control, 6, true),
            Self::Capacity(_) => (FailureClass::Resource, 7, false),
            Self::CapacityWaitTimeout => (FailureClass::Resource, 8, false),
            Self::AlreadyAdmitted => (FailureClass::Invariant, 9, false),
            Self::AlreadyWaitingForResource(_) => (FailureClass::Invariant, 10, false),
            Self::Conflict => (FailureClass::Invariant, 11, false),
            Self::OwnerStillPresent => (FailureClass::Invariant, 12, false),
            Self::ArithmeticOverflow => (FailureClass::Resource, 13, false),
        };
        let reason = FailureReason::new(domain::WORKLOAD, NonZeroU16::new(code).unwrap());
        if observed_stop {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capacity_and_wait_expiry_keep_unknown_measurements_without_claiming_backend_loss() {
        for error in [
            WorkError::Capacity("positions"),
            WorkError::CapacityWaitTimeout,
            WorkError::ArithmeticOverflow,
        ] {
            let fact = error.failure_fact();
            assert_eq!(fact.class(), FailureClass::Resource);
            assert_eq!(
                fact.resource(),
                None,
                "no measured size is available in this refusal"
            );
            assert_eq!(fact.reason().domain(), domain::WORKLOAD.get());
        }
        assert_ne!(
            WorkError::Capacity("positions").failure_fact().reason(),
            WorkError::CapacityWaitTimeout.failure_fact().reason()
        );
    }
    #[test]
    fn observing_the_existing_stop_is_not_a_new_originating_cause() {
        assert_eq!(
            WorkError::Released.failure_fact().evidence(),
            novarocks_failure_contract::FailureEvidence::StopObservation
        );
        assert_eq!(
            WorkError::ForeignAuthority.failure_fact().class(),
            FailureClass::Invariant
        );
    }
}
