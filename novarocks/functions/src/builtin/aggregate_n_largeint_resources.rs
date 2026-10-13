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
//! External admission for reached N decimal codec calls. Library invoices are
//! deliberately uncalibrated policy estimates, not private byte upper bounds.
//! Returned String backing has an independent OWN scope. Caller diagnostics,
//! allocator metadata, invocation Data and escaped output remain caller-owned.

use super::aggregate_n_core::NCodecResources;
use crate::KernelFailure;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{ScalarStateError, ScalarWork};
use crate::kernel_control::{internal, invalid};
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};

/// Versioned engineering policy over public calls and reached input bytes.
/// No calibration or internal allocation proof is implied by these invoices.
pub(super) struct NCodecBudgetPolicyV1;
impl NCodecBudgetPolicyV1 {
    const FORMAT_INVOICE: usize = 8192;
    const PARSE_BASE: usize = 4096;
    const PARSE_PER_BYTE: usize = 16;

    fn parse_invoice(text_bytes: usize) -> Result<usize, KernelFailure> {
        text_bytes
            .checked_mul(Self::PARSE_PER_BYTE)
            .and_then(|bytes| bytes.checked_add(Self::PARSE_BASE))
            .ok_or(KernelFailure::ResourceExhausted)
    }
}

// Default signed-i256 ToString writes an optional sign (first capacity8),
// then one magnitude (final capacity<=78): requests/overlap<=86. This OWN
// writer fact is independent of num-bigint's internal initial capacity.
const FORMAT_STRING_PEAK: usize = 86;
const FORMAT_STRING_CAPACITY: usize = 78;

pub(super) struct NCodecLease {
    // Body text is destroyed before this inline lease. Keep the OWN loan until
    // after the library loan; neither field creates new host authority backing.
    _library: OpaqueReservation,
    _returned_string: Option<OpaqueReservation>,
}

pub(super) struct BoundNCodecResources<'a> {
    pub(super) charge: &'a OpaqueRetainedCharge,
    pub(super) allocator: &'a HostAggregateAllocator,
}
impl BoundNCodecResources<'_> {
    fn reserve(&self, bytes: usize) -> Result<OpaqueReservation, KernelFailure> {
        let loan = self.charge.reserve_operation(bytes)?;
        if !loan.belongs_to_allocator(self.allocator) {
            return Err(invalid(
                "min_n/max_n codec reservation uses another host authority",
            ));
        }
        Ok(loan)
    }
}
fn flush(work: &mut ScalarWork<'_, '_>) -> Result<(), KernelFailure> {
    match work.flush() {
        Ok(()) => Ok(()),
        Err(ScalarStateError::Kernel(cause)) => Err(cause),
        Err(_) => Err(internal(
            "min_n/max_n codec flush returned a non-control failure",
        )),
    }
}
impl NCodecResources for BoundNCodecResources<'_> {
    type Lease = NCodecLease;

    fn before_decimal256_format(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self::Lease, KernelFailure> {
        flush(work)?;
        let returned_string = self.reserve(FORMAT_STRING_PEAK)?;
        let library = self.reserve(NCodecBudgetPolicyV1::FORMAT_INVOICE)?;
        Ok(NCodecLease {
            _library: library,
            _returned_string: Some(returned_string),
        })
    }
    fn after_decimal256_format(
        &mut self,
        _text: &str,
        capacity: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        // This drift assertion cannot replace admission before the call.
        if capacity > FORMAT_STRING_CAPACITY {
            return Err(internal(
                "min_n/max_n decimal256 formatter exceeded its owned String extent",
            ));
        }
        flush(work)
    }
    fn before_decimal256_parse(
        &mut self,
        text_bytes: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self::Lease, KernelFailure> {
        flush(work)?;
        let bytes = NCodecBudgetPolicyV1::parse_invoice(text_bytes)?;
        let library = self.reserve(bytes)?;
        Ok(NCodecLease {
            _library: library,
            _returned_string: None,
        })
    }
    fn after_decimal256_parse(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        flush(work)
    }
}

#[cfg(test)]
mod tests;
