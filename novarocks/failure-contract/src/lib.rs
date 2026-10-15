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

//! Domain-owned, transport-neutral failure facts.
//!
//! Producers project their own reasons. This crate knows neither runtime error
//! types nor Task lifecycle, recovery policy, SQL codes, or Arrow carriers.

use std::{fmt, num::NonZeroU16};

/// Semantic category, independent of the component that produced the error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum FailureClass {
    Data,
    Dependency,
    Resource,
    Representation,
    Invariant,
    Unexpected,
    Control,
}

/// Stable producer namespace and producer-owned reason. Zero is never a reason.
/// Domain owners define constants; boundary codecs preserve both integers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct FailureReason {
    domain: NonZeroU16,
    code: NonZeroU16,
}

impl FailureReason {
    pub const fn new(domain: NonZeroU16, code: NonZeroU16) -> Self {
        Self { domain, code }
    }
    pub const fn domain(self) -> u16 {
        self.domain.get()
    }
    pub const fn code(self) -> u16 {
        self.code.get()
    }
}

impl fmt::Display for FailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.domain, self.code)
    }
}

/// Stable namespaces. A new reason is defined by its owning domain, not here.
pub mod domain {
    use std::num::NonZeroU16;
    pub const KERNEL: NonZeroU16 = NonZeroU16::new(1).unwrap();
    pub const CONSTRUCTION: NonZeroU16 = NonZeroU16::new(2).unwrap();
    pub const MEMORY: NonZeroU16 = NonZeroU16::new(3).unwrap();
    pub const CONNECTOR: NonZeroU16 = NonZeroU16::new(4).unwrap();
    pub const EXCHANGE: NonZeroU16 = NonZeroU16::new(5).unwrap();
    pub const RESULT: NonZeroU16 = NonZeroU16::new(6).unwrap();
    pub const EXECUTION: NonZeroU16 = NonZeroU16::new(7).unwrap();
    pub const METADATA: NonZeroU16 = NonZeroU16::new(8).unwrap();
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ResourceScope {
    Request,
    Operator,
    Driver,
    Task,
    Query,
    Process,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ResourceDimension {
    Bytes,
    Items,
    Positions,
}

/// Unknown measurements remain None; zero remains an actual observed zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceFacts {
    pub request_scope: Option<ResourceScope>,
    pub constraint_scope: Option<ResourceScope>,
    pub dimension: ResourceDimension,
    pub requested: Option<u64>,
    pub limit: Option<u64>,
    pub live: Option<u64>,
    pub committed: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepresentationBoundary {
    MinimumUnit,
    SingleOutput,
}

/// A domain's stable layout identifier, distinct from an error reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepresentationLayout {
    pub domain: NonZeroU16,
    pub code: NonZeroU16,
}

/// Layout identity belongs to the producing domain. The contract carries its
/// stable reason and numerical boundary without importing a value crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepresentationFacts {
    pub boundary: RepresentationBoundary,
    pub layout: RepresentationLayout,
    pub required: Option<u64>,
    pub representable_prefix: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailureFact {
    class: FailureClass,
    reason: FailureReason,
    resource: Option<ResourceFacts>,
    representation: Option<RepresentationFacts>,
}

impl FailureFact {
    pub const fn new(class: FailureClass, reason: FailureReason) -> Self {
        Self {
            class,
            reason,
            resource: None,
            representation: None,
        }
    }
    pub const fn with_resource(mut self, resource: ResourceFacts) -> Self {
        self.resource = Some(resource);
        self
    }
    pub const fn with_representation(mut self, representation: RepresentationFacts) -> Self {
        self.representation = Some(representation);
        self
    }
    pub const fn class(self) -> FailureClass {
        self.class
    }
    pub const fn reason(self) -> FailureReason {
        self.reason
    }
    pub const fn resource(self) -> Option<ResourceFacts> {
        self.resource
    }
    pub const fn representation(self) -> Option<RepresentationFacts> {
        self.representation
    }
}

/// Implemented where the reason is known. Wrappers delegate this projection;
/// they never classify another domain's concrete error type or its Display.
pub trait FailureProjection {
    fn failure_fact(&self) -> FailureFact;
}

impl<T: FailureProjection + ?Sized> FailureProjection for &T {
    fn failure_fact(&self) -> FailureFact {
        (**self).failure_fact()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // A domain can project a new cause without adding a runtime dependency or
    // extending a central match. Its own identifiers are the independent oracle.
    struct LocalCapacityRefusal;
    impl FailureProjection for LocalCapacityRefusal {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::MEMORY, NonZeroU16::new(41).unwrap()),
            )
            .with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Operator),
                constraint_scope: Some(ResourceScope::Query),
                dimension: ResourceDimension::Bytes,
                requested: Some(64),
                limit: Some(32),
                live: None,
                committed: Some(0),
            })
        }
    }
    #[test]
    fn domain_projection_preserves_reason_and_unknown_measurements() {
        let fact = LocalCapacityRefusal.failure_fact();
        assert_eq!((fact.reason().domain(), fact.reason().code()), (3, 41));
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!((resource.requested, resource.limit), (Some(64), Some(32)));
        assert_eq!((resource.live, resource.committed), (None, Some(0)));
    }
    #[test]
    fn representation_distinguishes_a_value_from_a_whole_output_boundary() {
        let layout = RepresentationLayout {
            domain: domain::CONSTRUCTION,
            code: NonZeroU16::new(2).unwrap(),
        };
        let one = RepresentationFacts {
            boundary: RepresentationBoundary::MinimumUnit,
            layout,
            required: Some(10),
            representable_prefix: Some(0),
        };
        let batch = RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout,
            required: Some(10),
            representable_prefix: Some(3),
        };
        assert_ne!(one, batch);
    }
}
