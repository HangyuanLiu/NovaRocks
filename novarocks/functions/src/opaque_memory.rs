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

//! Explicit host authority over opaque library operation backing.
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelFailure};
use std::sync::Arc;

/// A host reservation is not a replacement allocation or a private wallet.
/// The actual host owns admission and releases exactly this reservation.
pub trait OpaqueAllocationHost: Send + Sync {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure>;
    fn release_opaque(&self, bytes: usize);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpaqueRetainedUnderestimate {
    pub reserved: usize,
    pub growth: usize,
}
/// The original retained transfer arithmetic. The legacy shell formats its
/// original String, while pure adapters classify the invariant as Internal.
pub fn reconcile_opaque_retained(
    current: &mut usize,
    reservation: &mut usize,
    new_bytes: usize,
    mut release: impl FnMut(usize),
) -> Result<(), OpaqueRetainedUnderestimate> {
    if new_bytes >= *current {
        let growth = new_bytes - *current;
        if growth > *reservation {
            return Err(OpaqueRetainedUnderestimate {
                reserved: *reservation,
                growth,
            });
        }
        *reservation -= growth;
    } else {
        release(*current - new_bytes);
    }
    *current = new_bytes;
    Ok(())
}

/// Inline charge tied to the original allocator's explicit opaque host.
/// Arc clones retain the existing authority; no metadata block is fabricated.
pub struct OpaqueRetainedCharge {
    host: Arc<dyn AggregateStateAllocator>,
    bytes: usize,
}
impl OpaqueRetainedCharge {
    pub fn try_new(host: Arc<dyn AggregateStateAllocator>) -> Result<Self, KernelFailure> {
        if host.opaque_allocation_host().is_none() {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "opaque aggregate requires an actual host reservation capability",
            )));
        }
        Ok(Self { host, bytes: 0 })
    }
    fn authority(&self) -> &dyn OpaqueAllocationHost {
        // The immutable host capability was checked before publication.
        self.host
            .opaque_allocation_host()
            .expect("opaque host capability remains installed")
    }
    /// The caller has already destroyed all owned opaque backing. Repeated
    /// cleanup and last Drop therefore cannot release this charge twice.
    pub fn release_retained(&mut self) {
        self.authority().release_opaque(self.bytes);
        self.bytes = 0;
    }
    pub const fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn reserve_operation(&self, bytes: usize) -> Result<OpaqueReservation, KernelFailure> {
        self.authority().reserve_opaque(bytes)?;
        Ok(OpaqueReservation {
            host: Arc::clone(&self.host),
            bytes,
        })
    }
    pub fn reconcile_under_reservation(
        &mut self,
        new_bytes: usize,
        reservation: &mut OpaqueReservation,
    ) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(&self.host, &reservation.host) {
            return Err(KernelFailure::Internal(KernelDiagnostic::new(
                "opaque retained transfer uses another host authority",
            )));
        }
        let authority = self
            .host
            .opaque_allocation_host()
            .expect("opaque host capability remains installed");
        reconcile_opaque_retained(
            &mut self.bytes,
            &mut reservation.bytes,
            new_bytes,
            |bytes| authority.release_opaque(bytes),
        )
        .map_err(|_| {
            KernelFailure::Internal(KernelDiagnostic::new(
                "opaque operation preflight underestimated retained growth",
            ))
        })
    }
}
impl Drop for OpaqueRetainedCharge {
    fn drop(&mut self) {
        self.authority().release_opaque(self.bytes);
    }
}
/// A unique live host reservation. Drop releases only its untransferred part.
pub struct OpaqueReservation {
    host: Arc<dyn AggregateStateAllocator>,
    bytes: usize,
}
impl OpaqueReservation {
    /// Borrow the actual reservation authority without making a zero-byte grant.
    pub(crate) fn belongs_to_allocator(
        &self,
        allocator: &crate::aggregate_host_allocator::HostAggregateAllocator,
    ) -> bool {
        allocator.has_host_authority(&self.host)
    }
    pub const fn remaining_bytes(&self) -> usize {
        self.bytes
    }
}
impl Drop for OpaqueReservation {
    fn drop(&mut self) {
        self.host
            .opaque_allocation_host()
            .expect("opaque host capability remains installed")
            .release_opaque(self.bytes);
    }
}
