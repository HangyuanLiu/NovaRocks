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

//! Actual nominal storage for ordered keys and canonical contribution bytes.
//! Vector Layouts and the immutable shared holder have separate authors. No
//! opaque capacity charge is added for an already-paid vector.

use std::{fmt, ops::Deref, sync::Arc};

use novarocks_functions::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelEvaluationControl, KernelFailure,
    MAX_UNOBSERVED_KERNEL_WORK, SourceBackingOwner, source_custody_prefix_facts,
};

use super::contribution::{
    ControlledContributionCodecError, OrderedScalarRef, OrderedTupleRef, RuntimeOrderContract,
    borrowed_ordered_bound_encoded_len, visit_borrowed_ordered_bound,
};
use crate::exec::expr::agg::{AggregateAllocator, AggregateVec};
use crate::exec::operators::compiled_aggregate::expression_allocation_host;
use crate::runtime::mem_tracker::MemTracker;

struct ByteBacking {
    bytes: AggregateVec<u8>,
}

#[derive(Clone)]
enum ByteStorage {
    Legacy(Arc<[u8]>),
    Owned(SourceBackingOwner<ByteBacking>),
}

/// Borrow bytes or clone the complete carrier. The owned branch never exposes
/// a detachable vector/Arc that could outlive its original paying host.
#[derive(Clone)]
pub struct CanonicalContributionBytes(ByteStorage);

impl CanonicalContributionBytes {
    pub fn as_slice(&self) -> &[u8] {
        match &self.0 {
            ByteStorage::Legacy(bytes) => bytes,
            ByteStorage::Owned(owner) => &owner.borrowed().bytes,
        }
    }

    /// Preserve the original local-ingress copy for legacy payloads. Newly
    /// owned payloads instead share the same paid immutable backing.
    pub fn clone_for_local_ingress(&self) -> Self {
        match &self.0 {
            ByteStorage::Legacy(bytes) => Self::from(Arc::<[u8]>::from(bytes.as_ref())),
            ByteStorage::Owned(_) => self.clone(),
        }
    }
}
impl From<Arc<[u8]>> for CanonicalContributionBytes {
    fn from(bytes: Arc<[u8]>) -> Self {
        Self(ByteStorage::Legacy(bytes))
    }
}
impl From<Vec<u8>> for CanonicalContributionBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl From<Box<[u8]>> for CanonicalContributionBytes {
    fn from(bytes: Box<[u8]>) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl From<&[u8]> for CanonicalContributionBytes {
    fn from(bytes: &[u8]) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl<const N: usize> From<[u8; N]> for CanonicalContributionBytes {
    fn from(bytes: [u8; N]) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl<const N: usize> From<&[u8; N]> for CanonicalContributionBytes {
    fn from(bytes: &[u8; N]) -> Self {
        Self::from(bytes.as_slice())
    }
}
impl AsRef<[u8]> for CanonicalContributionBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl Deref for CanonicalContributionBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl fmt::Debug for CanonicalContributionBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}
impl PartialEq for CanonicalContributionBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for CanonicalContributionBytes {}

/// A NULL key has no payload, and is distinct from an owned empty UTF8 key.
#[derive(Clone)]
pub(crate) struct OwnedUtf8OrderKey(Option<SourceBackingOwner<ByteBacking>>);
impl fmt::Debug for OwnedUtf8OrderKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.borrowed().fmt(f)
    }
}
impl PartialEq for OwnedUtf8OrderKey {
    fn eq(&self, other: &Self) -> bool {
        match (self.borrowed(), other.borrowed()) {
            (OrderedTupleRef::Single(a), OrderedTupleRef::Single(b)) => a == b,
            _ => unreachable!("owned UTF8 keys always have exactly one borrowed scalar"),
        }
    }
}
impl Eq for OwnedUtf8OrderKey {}
impl OwnedUtf8OrderKey {
    pub(crate) fn borrowed(&self) -> OrderedTupleRef<'_> {
        OrderedTupleRef::Single(self.0.as_ref().map(|owner| {
            // SAFETY: this private backing is only made by copy_utf8 below,
            // which copies an entire valid str without mutation after publish.
            let text = unsafe { std::str::from_utf8_unchecked(&owner.borrowed().bytes) };
            OrderedScalarRef::Utf8(text)
        }))
    }
}

/// Both real authors use the SAME supplied tracker. No default tracker or
/// foreign host can be paired with these vectors. The existing adapter Arc
/// retains its existing formal scaffold qualification, not a new payload grant.
#[derive(Clone)]
pub(crate) struct OrderedTopNAllocationHost {
    allocator: AggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
}
impl OrderedTopNAllocationHost {
    pub(crate) fn new(tracker: Arc<MemTracker>) -> Self {
        Self {
            allocator: AggregateAllocator::new_nominal(Arc::clone(&tracker)),
            host: expression_allocation_host(tracker),
        }
    }

    pub(crate) fn vector_allocator(&self) -> AggregateAllocator {
        self.allocator.clone()
    }

    fn reserve_prefix(&self) -> Result<OpaqueReservation, KernelFailure> {
        let authority = OpaqueRetainedCharge::try_new(Arc::clone(&self.host))?;
        authority.reserve_operation(source_custody_prefix_facts().requests().bytes())
    }

    fn retain_bytes<E: From<KernelFailure>>(
        &self,
        control: &dyn KernelEvaluationControl,
        build: impl FnOnce(
            &AggregateAllocator,
            &mut EvaluationCheckpoints<'_>,
        ) -> Result<AggregateVec<u8>, E>,
    ) -> Result<SourceBackingOwner<ByteBacking>, E> {
        // Fund the original cold-holder prefix before any control callback.
        let _prefix = self.reserve_prefix()?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let bytes = build(&self.allocator, &mut work)?;
        let owner = SourceBackingOwner::try_from_owned_direct(
            ByteBacking { bytes },
            Arc::clone(&self.host),
            &mut work,
        )?;
        work.finish()?;
        Ok(owner)
    }

    pub(crate) fn copy_utf8(
        &self,
        text: Option<&str>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<OwnedUtf8OrderKey, KernelFailure> {
        let Some(text) = text else {
            let _prefix = self.reserve_prefix()?;
            control.checkpoint(0)?;
            return Ok(OwnedUtf8OrderKey(None));
        };
        let owner = self.retain_bytes(control, |allocator, work| {
            work.flush()?;
            let mut bytes = AggregateVec::new_in(allocator.clone());
            bytes
                .try_reserve_exact(text.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for chunk in text.as_bytes().chunks(MAX_UNOBSERVED_KERNEL_WORK as usize) {
                control.checkpoint(chunk.len() as u32)?;
                bytes.extend_from_slice(chunk);
            }
            work.flush()?;
            Ok::<_, KernelFailure>(bytes)
        })?;
        Ok(OwnedUtf8OrderKey(Some(owner)))
    }

    pub(crate) fn encode_ordered_bound(
        &self,
        tuple: OrderedTupleRef<'_>,
        contract: &RuntimeOrderContract,
        cap: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<CanonicalContributionBytes, ControlledContributionCodecError> {
        let owner = self.retain_bytes(control, |allocator, work| {
            // Counting itself observes the caller's control, so it belongs
            // inside this same paid prefix transaction, before vector creation.
            let len = borrowed_ordered_bound_encoded_len(tuple, contract, control)?;
            if len > cap {
                return Err(
                    super::contribution::ContributionCodecError::EncodedSizeExceeded.into(),
                );
            }
            work.flush()?;
            let mut bytes = AggregateVec::new_in(allocator.clone());
            bytes
                .try_reserve_exact(len)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            // All output remains private until the complete visitor succeeds.
            // It supplies bounded slices and preserves the first sink/control cause.
            visit_borrowed_ordered_bound(tuple, contract, cap, control, |chunk| {
                if chunk.len() > bytes.capacity() - bytes.len() {
                    return Err(super::contribution::ContributionCodecError::LengthOverflow.into());
                }
                bytes.extend_from_slice(chunk);
                Ok(())
            })?;
            work.flush()?;
            Ok::<_, ControlledContributionCodecError>(bytes)
        })?;
        Ok(CanonicalContributionBytes(ByteStorage::Owned(owner)))
    }

    pub(crate) fn ordered_contribution(
        &self,
        tuple: OrderedTupleRef<'_>,
        contract: &RuntimeOrderContract,
        cap: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<super::RuntimeFilterContribution, ControlledContributionCodecError> {
        let bytes = self.encode_ordered_bound(tuple, contract, cap, control)?;
        Ok(super::RuntimeFilterContribution::from_shared(
            super::RuntimeFilterContributionKind::OrderedBound,
            contract.digest(),
            bytes,
        ))
    }
}

#[cfg(test)]
#[path = "owned_tests.rs"]
mod tests;
