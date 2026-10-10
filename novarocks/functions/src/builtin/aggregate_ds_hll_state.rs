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

//! The original aggregate retained-state operation sequence over an explicit host.
use crate::builtin::aggregate_ds_hll_failure::{
    DsHllFailureSink, DsHllInputContext, DsHllInputDetail, DsHllInputRecipe,
};
use crate::datasketches_hll::{HllHandle, HllTargetType};
use crate::datasketches_hll_failure::HllObservation;
#[derive(Clone, Copy, Debug)]
pub enum DsHllRetainedOperation {
    Create,
    Payload,
    Update,
    Merge,
}
impl DsHllRetainedOperation {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Create => "reserve ds_hll handle creation",
            Self::Payload => "reserve ds_hll payload initialization",
            Self::Update => "reserve ds_hll update",
            Self::Merge => "reserve ds_hll merge",
        }
    }
}
/// A lease comes from the actual host. Its original implementation owns
/// admission, rollback and the transfer to retained bytes.
pub trait DsHllRetainedPort<F: DsHllFailureSink> {
    fn payload_error_headroom(
        &self,
        preflight: crate::datasketches_hll::HllPayloadPreflight,
    ) -> usize;
    type Reservation;
    fn reserve(
        &self,
        bytes: usize,
        operation: DsHllRetainedOperation,
        sink: &mut F,
    ) -> Result<Self::Reservation, F::Error>;
    fn reconcile(
        &mut self,
        bytes: usize,
        reservation: &mut Self::Reservation,
        sink: &mut F,
    ) -> Result<(), F::Error>;
}
pub fn hll_heap_bytes(current: usize) -> usize {
    current.saturating_sub(std::mem::size_of::<HllHandle>())
}
pub fn ensure_handle<'a, F: DsHllFailureSink, R: DsHllRetainedPort<F>>(
    handle: &'a mut Option<HllHandle>,
    charge: &mut R,
    log_k: u8,
    target: HllTargetType,
    sink: &mut F,
) -> Result<&'a mut HllHandle, F::Error> {
    if handle.is_none() {
        let preflight = HllHandle::new_allocation_preflight_with_failure(log_k, target, sink)?;
        let mut reservation = charge.reserve(
            preflight.bounds().operation_peak_bytes,
            DsHllRetainedOperation::Create,
            sink,
        )?;
        let (new_handle, outcome) =
            HllHandle::new_under_reservation_with_failure(&preflight, &reservation, sink)?;
        charge.reconcile(
            hll_heap_bytes(outcome.current_bytes),
            &mut reservation,
            sink,
        )?;
        *handle = Some(new_handle);
        // Retained transfer precedes the first post-mutation control observation.
        sink.observe(HllObservation::OpaqueBoundary)?;
    }
    Ok(handle.as_mut().expect("ds_hll handle initialized"))
}
pub fn ensure_handle_from_payload<'a, F: DsHllFailureSink, R: DsHllRetainedPort<F>>(
    handle: &'a mut Option<HllHandle>,
    charge: &mut R,
    payload: &[u8],
    sink: &mut F,
) -> Result<&'a mut HllHandle, F::Error> {
    if handle.is_none() {
        let preflight = HllHandle::from_payload_allocation_preflight_with_failure(payload, sink)?;
        let mut reservation = charge.reserve(
            preflight.bounds().operation_peak_bytes + charge.payload_error_headroom(preflight),
            DsHllRetainedOperation::Payload,
            sink,
        )?;
        let (new_handle, outcome) = HllHandle::from_payload_under_reservation_with_failure(
            payload,
            &preflight,
            &reservation,
            sink,
        )?;
        charge.reconcile(
            hll_heap_bytes(outcome.current_bytes),
            &mut reservation,
            sink,
        )?;
        *handle = Some(new_handle);
        sink.observe(HllObservation::OpaqueBoundary)?;
    }
    Ok(handle.as_mut().expect("ds_hll handle initialized"))
}
pub fn update_hash<F: DsHllFailureSink, R: DsHllRetainedPort<F>>(
    handle: &mut Option<HllHandle>,
    charge: &mut R,
    hash: u64,
    sink: &mut F,
) -> Result<(), F::Error> {
    let handle = handle.as_mut().ok_or_else(|| {
        sink.input(DsHllInputRecipe {
            context: DsHllInputContext::CountDistinct,
            detail: DsHllInputDetail::Uninitialized,
        })
    })?;
    let preflight = handle.update_hash_allocation_preflight_with_failure(sink)?;
    let mut reservation = charge.reserve(
        preflight.bounds().additional_headroom_bytes(),
        DsHllRetainedOperation::Update,
        sink,
    )?;
    let outcome =
        handle.update_hash_under_reservation_with_failure(hash, &preflight, &reservation, sink)?;
    charge.reconcile(
        hll_heap_bytes(outcome.current_bytes),
        &mut reservation,
        sink,
    )?;
    sink.observe(HllObservation::OpaqueBoundary)?;
    Ok(())
}
pub fn merge_payload<F: DsHllFailureSink, R: DsHllRetainedPort<F>>(
    handle: &mut Option<HllHandle>,
    charge: &mut R,
    payload: &[u8],
    sink: &mut F,
) -> Result<(), F::Error> {
    if handle.is_none() {
        ensure_handle_from_payload(handle, charge, payload, sink)?;
        return Ok(());
    }
    let handle = handle.as_mut().expect("ds_hll handle initialized");
    let preflight = handle.merge_payload_allocation_preflight_with_failure(payload, sink)?;
    let mut reservation = charge.reserve(
        preflight.bounds().additional_headroom_bytes() + charge.payload_error_headroom(preflight),
        DsHllRetainedOperation::Merge,
        sink,
    )?;
    let outcome = handle.merge_payload_under_reservation_with_failure(
        payload,
        &preflight,
        &reservation,
        sink,
    )?;
    charge.reconcile(
        hll_heap_bytes(outcome.current_bytes),
        &mut reservation,
        sink,
    )?;
    sink.observe(HllObservation::OpaqueBoundary)?;
    Ok(())
}
