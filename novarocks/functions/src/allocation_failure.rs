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

//! Typed collection refusals retain the original error and only facts exposed
//! by that container. Allocator callback errors do not identify a quota owner.
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, ResourceDimension, ResourceFacts, domain,
};
use std::{alloc::Layout, error::Error, fmt, num::NonZeroU16};

/// One Rust array backing block's byte extent, without a quota or row prefix.
/// Producers opt into this geometry only at their actual layout boundary.
pub(crate) const RUST_ARRAY_BACKING_LAYOUT: NonZeroU16 = NonZeroU16::new(80).unwrap();

/// Project a producer-proven failure of one contiguous Rust array backing.
/// The producer supplies its own reason and proves the API boundary; budget
/// arithmetic and actual allocator refusals do not use this projection.
pub fn rust_array_backing_failure_fact(
    reason: FailureReason,
    required: u128,
    boundary: novarocks_failure_contract::RepresentationBoundary,
) -> FailureFact {
    use novarocks_failure_contract::{RepresentationFacts, RepresentationLayout};
    FailureFact::new(FailureClass::Representation, reason).with_representation(
        RepresentationFacts {
            boundary,
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code: RUST_ARRAY_BACKING_LAYOUT,
            },
            required: u64::try_from(required).ok(),
            extent_limit: Some(isize::MAX as u64),
            representable_prefix: None,
        },
    )
}

mod sealed {
    pub trait Sealed {}
}
/// Only actual allocation/container refusal carriers implement this trait.
/// Control, data, and diagnostic strings cannot enter this resource path.
pub trait AllocationRefusalSource: sealed::Sealed + fmt::Debug + Send + Sync + 'static {
    fn capacity_overflow(&self) -> bool;
    fn requested_bytes(&self) -> Option<usize>;
    fn original_error(&self) -> Option<&(dyn Error + 'static)>;
    /// None means the standard container does not expose its failure kind.
    fn allocator_refusal(&self) -> Option<bool>;
}
impl sealed::Sealed for allocator_api2::collections::TryReserveError {}
impl AllocationRefusalSource for allocator_api2::collections::TryReserveError {
    fn capacity_overflow(&self) -> bool {
        matches!(
            self.kind(),
            allocator_api2::collections::TryReserveErrorKind::CapacityOverflow
        )
    }
    fn requested_bytes(&self) -> Option<usize> {
        match self.kind() {
            allocator_api2::collections::TryReserveErrorKind::AllocError { layout, .. } => {
                Some(layout.size())
            }
            _ => None,
        }
    }
    fn original_error(&self) -> Option<&(dyn Error + 'static)> {
        Some(self)
    }
    fn allocator_refusal(&self) -> Option<bool> {
        Some(!self.capacity_overflow())
    }
}
impl sealed::Sealed for hashbrown::TryReserveError {}
impl AllocationRefusalSource for hashbrown::TryReserveError {
    fn capacity_overflow(&self) -> bool {
        matches!(self, Self::CapacityOverflow)
    }
    fn requested_bytes(&self) -> Option<usize> {
        match self {
            Self::AllocError { layout } => Some(layout.size()),
            _ => None,
        }
    }
    // hashbrown exposes this typed enum without implementing std::error::Error.
    fn original_error(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
    fn allocator_refusal(&self) -> Option<bool> {
        Some(!self.capacity_overflow())
    }
}
impl sealed::Sealed for std::collections::TryReserveError {}
impl AllocationRefusalSource for std::collections::TryReserveError {
    fn capacity_overflow(&self) -> bool {
        false
    }
    fn requested_bytes(&self) -> Option<usize> {
        None
    }
    fn original_error(&self) -> Option<&(dyn Error + 'static)> {
        Some(self)
    }
    fn allocator_refusal(&self) -> Option<bool> {
        None
    }
}
/// The direct allocator caller still owns the exact failed Layout.
#[derive(Debug)]
pub struct AllocatorRequestRefusal {
    layout: Layout,
    source: allocator_api2::alloc::AllocError,
}
impl AllocatorRequestRefusal {
    pub fn new(layout: Layout, source: allocator_api2::alloc::AllocError) -> Self {
        Self { layout, source }
    }
}
impl sealed::Sealed for AllocatorRequestRefusal {}
impl AllocationRefusalSource for AllocatorRequestRefusal {
    fn capacity_overflow(&self) -> bool {
        false
    }
    fn requested_bytes(&self) -> Option<usize> {
        Some(self.layout.size())
    }
    fn original_error(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
    fn allocator_refusal(&self) -> Option<bool> {
        Some(true)
    }
}
#[derive(Debug)]
pub struct AllocationRefusal<E: AllocationRefusalSource> {
    operation: BoundedDetail,
    source: E,
}
impl<E: AllocationRefusalSource> AllocationRefusal<E> {
    pub fn new(operation: &str, source: E) -> Self {
        Self {
            operation: BoundedDetail::truncating(operation),
            source,
        }
    }
    pub fn original(&self) -> &E {
        &self.source
    }
}
impl<E: AllocationRefusalSource> fmt::Display for AllocationRefusal<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: collection reservation refused: {:?}",
            self.operation.as_str(),
            self.source
        )
    }
}
impl<E: AllocationRefusalSource> Error for AllocationRefusal<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.original_error()
    }
}
impl<E: AllocationRefusalSource> FailureProjection for AllocationRefusal<E> {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(
                domain::KERNEL,
                NonZeroU16::new(if self.source.capacity_overflow() {
                    168
                } else {
                    169
                })
                .unwrap(),
            ),
        )
        .with_resource(ResourceFacts {
            request_scope: None,
            constraint_scope: None,
            dimension: ResourceDimension::Bytes,
            requested: self
                .source
                .requested_bytes()
                .and_then(|n| u64::try_from(n).ok()),
            required_growth: None,
            limit: None,
            live: None,
            committed: None,
        })
    }
}
impl<E: AllocationRefusalSource> OuterFailureProjection for AllocationRefusal<E> {}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator_api2::{
        alloc::{AllocError, Allocator},
        vec::Vec as StateVec,
    };
    use std::ptr::NonNull;
    #[derive(Clone, Debug)]
    struct Reject;
    // SAFETY: this allocator never produces a live block.
    unsafe impl Allocator for Reject {
        fn allocate(&self, _: Layout) -> Result<NonNull<[u8]>, AllocError> {
            Err(AllocError)
        }
        unsafe fn deallocate(&self, _: NonNull<u8>, _: Layout) {
            panic!("no block was allocated")
        }
    }
    #[test]
    fn actual_vector_refusal_preserves_layout_and_original_error() {
        let mut values = StateVec::<u64, _>::new_in(Reject);
        let original = values.try_reserve_exact(3).unwrap_err();
        let error = AllocationRefusal::new("cancel 403 decimal overflow", original.clone());
        assert_eq!(error.original(), &original);
        assert!(
            error
                .source()
                .unwrap()
                .is::<allocator_api2::collections::TryReserveError>()
        );
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().code(), 169);
        let resource = fact.resource().unwrap();
        assert_eq!(resource.requested, Some(24));
        assert_eq!(
            (resource.limit, resource.committed, resource.live),
            (None, None, None)
        );
        assert!(values.is_empty());
        assert_eq!(values.capacity(), 0);
    }
    #[test]
    fn capacity_overflow_is_distinct_and_does_not_invent_a_layout() {
        let mut values = StateVec::<u64, _>::new_in(Reject);
        let original = values.try_reserve_exact(usize::MAX).unwrap_err();
        assert_eq!(original.allocator_refusal(), Some(false));
        let error = AllocationRefusal::new("reserve state", original);
        assert_eq!(error.failure_fact().reason().code(), 168);
        assert_eq!(error.failure_fact().resource().unwrap().requested, None);
    }
    #[test]
    fn hash_table_refusal_retains_typed_source_and_exact_request() {
        let mut values = hashbrown::HashSet::<u64, _, _>::with_hasher_in(
            hashbrown::hash_map::DefaultHashBuilder::default(),
            Reject,
        );
        let original = values.try_reserve(5).unwrap_err();
        let requested = original.requested_bytes().unwrap();
        let error = AllocationRefusal::new("reserve hash buckets", original.clone());
        assert_eq!(error.original(), &original);
        assert_eq!(
            error.failure_fact().resource().unwrap().requested,
            Some(requested as u64)
        );
        assert!(error.source().is_none());
        assert!(values.is_empty());
    }
    #[test]
    fn opaque_standard_error_and_direct_layout_keep_their_evidence_boundary() {
        let original = Vec::<u64>::new().try_reserve(usize::MAX).unwrap_err();
        assert_eq!(original.allocator_refusal(), None);
        let error = AllocationRefusal::new("reserve standard vector", original);
        assert_eq!(error.failure_fact().resource().unwrap().requested, None);
        assert!(
            error
                .source()
                .unwrap()
                .is::<std::collections::TryReserveError>()
        );
        let error = AllocationRefusal::new(
            "allocate state block",
            AllocatorRequestRefusal::new(Layout::from_size_align(513, 16).unwrap(), AllocError),
        );
        assert_eq!(
            error.failure_fact().resource().unwrap().requested,
            Some(513)
        );
        assert!(error.source().unwrap().is::<AllocError>());
    }
}
