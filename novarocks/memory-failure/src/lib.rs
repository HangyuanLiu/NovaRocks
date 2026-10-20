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

//! Memory-domain boundary projections over the dependency-free capacity core.
//!
//! Views borrow original causes and introduce no owner, capacity, allocation,
//! retry decision or authority. Applications retain the rich core cause and
//! delegate only its neutral projection here. Opaque account IDs grant no
//! knowledge of application scope.

use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, ResourceDimension, ResourceFacts,
    ResourceScope, domain,
};
use novarocks_memory::{CapacityError, ConstraintKind, Refusal, ShortageReceipt};
use std::num::NonZeroU16;

/// A borrowed projection, not a new failure occurrence or a capacity grant.
#[derive(Clone, Copy, Debug)]
pub struct CapacityFailureProjection<'a>(&'a CapacityError);
impl<'a> CapacityFailureProjection<'a> {
    pub const fn new(cause: &'a CapacityError) -> Self {
        Self(cause)
    }
}
/// Projects the original refusal without asserting that a receipt is fresh.
#[derive(Clone, Copy, Debug)]
pub struct ShortageFailureProjection<'a>(&'a ShortageReceipt);
impl<'a> ShortageFailureProjection<'a> {
    pub const fn new(receipt: &'a ShortageReceipt) -> Self {
        Self(receipt)
    }
}

fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(
            domain::MEMORY,
            NonZeroU16::new(code).expect("nonzero reason"),
        ),
    )
}
fn refusal_facts(refusal: &Refusal) -> ResourceFacts {
    ResourceFacts {
        request_scope: None,
        constraint_scope: (refusal.constraint_kind == ConstraintKind::ProcessCapacity)
            .then_some(ResourceScope::Process),
        dimension: ResourceDimension::Bytes,
        requested: Some(refusal.requested),
        required_growth: Some(refusal.required_growth),
        limit: Some(refusal.limit),
        live: None,
        committed: Some(refusal.committed),
    }
}
impl FailureProjection for CapacityFailureProjection<'_> {
    fn failure_fact(&self) -> FailureFact {
        match self.0 {
            CapacityError::QueryLimit(refusal) => {
                fact(FailureClass::Resource, 1).with_resource(refusal_facts(refusal))
            }
            CapacityError::ImpossibleRequest(refusal) => {
                fact(FailureClass::Resource, 2).with_resource(refusal_facts(refusal))
            }
            CapacityError::ShortageCandidate(refusal) => {
                fact(FailureClass::Resource, 3).with_resource(refusal_facts(refusal))
            }
            CapacityError::Closed { .. } => fact(FailureClass::Control, 4),
            CapacityError::MetadataExhausted { limit, .. } => fact(FailureClass::Resource, 5)
                .with_resource(ResourceFacts {
                    request_scope: None,
                    constraint_scope: Some(ResourceScope::Process),
                    dimension: ResourceDimension::Items,
                    requested: None,
                    required_growth: None,
                    limit: Some(*limit),
                    live: None,
                    committed: None,
                }),
            CapacityError::Invalid { .. } => fact(FailureClass::Invariant, 6),
        }
    }
}
impl FailureProjection for ShortageFailureProjection<'_> {
    fn failure_fact(&self) -> FailureFact {
        fact(FailureClass::Resource, 7).with_resource(refusal_facts(&self.0.refusal))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_limit_preserves_known_limit_without_inventing_live_usage() {
        let cause = CapacityError::MetadataExhausted {
            registry: novarocks_memory::MetadataRegistryLabel::Accounts,
            limit: 7,
        };
        let projected = CapacityFailureProjection::new(&cause).failure_fact();
        let resource = projected.resource().unwrap();
        assert_eq!(resource.limit, Some(7));
        assert_eq!(resource.requested, None);
        assert_eq!(resource.committed, None);
        assert_eq!(resource.live, None);
    }
    #[test]
    fn invalid_source_diagnostic_does_not_become_a_capacity_refusal() {
        let cause = CapacityError::Invalid {
            detail: "process ran out of memory",
        };
        let projected = CapacityFailureProjection::new(&cause).failure_fact();
        assert_eq!(projected.class(), FailureClass::Invariant);
        assert_eq!(projected.reason().code(), 6);
    }
    #[test]
    fn refusal_measurements_survive_projection_without_guessing_account_scope() {
        use novarocks_memory::{AccountId, PolicyVersion};
        let refusal = Refusal {
            request_account: AccountId::new(1),
            constraint_account: AccountId::new(2),
            constraint_kind: ConstraintKind::AccountPolicy,
            dimension: None,
            requested: 17,
            required_growth: 9,
            ledger_revision: 3,
            limit: 20,
            committed: 11,
            policy_revision: PolicyVersion::new(4),
            capacity_revision: 5,
        };
        let cause = CapacityError::QueryLimit(refusal);
        let fact = CapacityFailureProjection::new(&cause).failure_fact();
        let resource = fact.resource().unwrap();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(resource.request_scope, None);
        assert_eq!(resource.constraint_scope, None);
        assert_eq!(resource.requested, Some(17));
        assert_eq!(resource.required_growth, Some(9));
        assert_eq!(resource.limit, Some(20));
        assert_eq!(resource.committed, Some(11));
        assert_eq!(resource.live, None);
    }
}
